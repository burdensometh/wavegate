package rollout

import (
	"slices"
	"time"

	"wavegate/discovery"
	"wavegate/traffic"
	"wavegate/v1"
)

func (e *Engine) advance(now time.Time, state v1.ReleaseState, live v1.TrafficBinding, snapshot discovery.Snapshot) error {
	spec := state.Spec
	if spec.Abort || state.Status.Phase == v1.RollingBack {
		return e.rollback(now, state, live, snapshot, "AbortRequested")
	}
	if state.Status.Verdict != nil && state.Status.Verdict.Result == v1.Fail {
		return e.rollback(now, state, live, snapshot, state.Status.Verdict.Reason)
	}
	if snapshot.Generation > e.generations[spec.UID] {
		if state.Window != nil {
			state.Window = nil
			state.Status.Verdict = nil
			setCondition(&state.Status, now, "DiscoveryReady", false, "WatchUnavailable")
			setCondition(&state.Status, now, "WindowReady", false, "WindowInvalidated")
			if err := e.commit(now, state, "WindowInvalidated", "WatchUnavailable", state.Reservation); err != nil {
				return err
			}
		}
		e.generations[spec.UID] = snapshot.Generation
		if !snapshot.Synced || state.Binding != nil && state.Binding.Rule.CandidateWeight > 0 {
			return nil
		}
	}
	if !snapshot.Synced || snapshot.Version < state.Status.DiscoveryVersion {
		return e.condition(now, state, "DiscoveryReady", false, "WatchUnavailable")
	}
	state.Status.DiscoveryVersion = snapshot.Version
	if setCondition(&state.Status, now, "DiscoveryReady", true, "WatchSynchronized") {
		return e.commit(now, state, "ConditionChanged", "WatchSynchronized", state.Reservation)
	}
	rule := traffic.Render(spec, snapshot, spec.Stable, spec.Candidate, 0)
	if state.Binding == nil {
		return e.admit(now, state, live, rule)
	}
	ready := len(rule.StableTargets) > 0 && len(rule.CandidateTargets) >= spec.MinReady
	if !ready && live.Rule.CandidateWeight > 0 {
		return e.rollback(now, state, live, snapshot, "EndpointsBelowMinimum")
	}
	if spec.Paused {
		if state.Status.Phase == v1.Paused {
			return nil
		}
		state.Window, state.Status.Verdict = nil, nil
		state.Status.Phase = v1.Paused
		setCondition(&state.Status, now, "Progressing", false, "ManuallyPaused")
		setCondition(&state.Status, now, "WindowReady", false, "WindowInvalidated")
		return e.commit(now, state, "ReleasePaused", "ManuallyPaused", state.Reservation)
	}
	if !ready {
		reason := "CandidateBelowMinimum"
		if len(rule.StableTargets) == 0 {
			reason = "StableUnavailable"
		}
		e.keepDrain(now, live.Rule, &rule)
		if !traffic.SameRoutes(live.Rule, rule) || state.Status.Phase == v1.Paused {
			return e.prepare(now, state, live, v1.Initialize, -1, rule, reason)
		}
		return e.condition(now, state, "Progressing", false, reason)
	}
	if live.Rule.CandidateWeight == 0 {
		rule.CandidateWeight = spec.Steps[0]
		e.keepDrain(now, live.Rule, &rule)
		return e.prepare(now, state, live, v1.WeightStep, 0, rule, "EndpointsReady")
	}
	rule.CandidateWeight = live.Rule.CandidateWeight
	e.keepDrain(now, live.Rule, &rule)
	if state.Window == nil || !traffic.SameRoutes(live.Rule, rule) {
		return e.prepare(now, state, live, v1.RefreshStep, state.Status.Step, rule, "NewObservationWindow")
	}
	if state.Status.Verdict == nil {
		return e.evaluate(now, state)
	}
	nextStep := state.Status.Step + 1
	if nextStep < len(spec.Steps) {
		rule.CandidateWeight = spec.Steps[nextStep]
		return e.prepare(now, state, live, v1.WeightStep, nextStep, rule, "WindowPassed")
	}
	rule = traffic.Render(spec, snapshot, spec.Candidate, "", 0)
	if e.drain > 0 {
		rule = traffic.WithDrain(rule, live.Rule, now.Add(e.drain))
	}
	e.keepDrain(now, live.Rule, &rule)
	return e.prepare(now, state, live, v1.Promote, state.Status.Step, rule, "FinalWindowPassed")
}

func (e *Engine) admit(now time.Time, state v1.ReleaseState, live v1.TrafficBinding, rule v1.Rule) error {
	if state.Reservation == nil {
		reservation, reason := e.book.Plan(state.Spec, e.journal.Sequence()+1, now)
		if reason != "" {
			return e.condition(now, state, "Admitted", false, reason)
		}
		if live.ResourceVersion > 0 && (live.Rule.Stable != state.Spec.Stable || live.Rule.CandidateWeight != 0) {
			return e.condition(now, state, "Admitted", false, "StableBindingMismatch")
		}
		state.Reservation = &reservation
		setCondition(&state.Status, now, "Admitted", true, "ReservationGranted")
		return e.commit(now, state, "ReservationGranted", "NamespaceBudgetAvailable", nil)
	}
	e.keepDrain(now, live.Rule, &rule)
	return e.prepare(now, state, live, v1.Initialize, -1, rule, "InstallStableBinding")
}

func (e *Engine) evaluate(now time.Time, state v1.ReleaseState) error {
	verdict, err := e.windows.Verdict(*state.Window, state.Spec.Window, now)
	if err != nil {
		return err
	}
	if verdict.Result == v1.Pending {
		return e.condition(now, state, "WindowReady", false, verdict.Reason)
	}
	state.Status.Verdict = &verdict
	setCondition(&state.Status, now, "WindowReady", verdict.Result == v1.Pass, verdict.Reason)
	return e.commit(now, state, "WindowEvaluated", verdict.Reason, state.Reservation)
}

func (e *Engine) keepDrain(now time.Time, previous v1.Rule, rule *v1.Rule) {
	for _, target := range previous.DrainTargets {
		active := e.drainInUse != nil && e.drainInUse(previous.Service, target)
		if !now.Before(previous.DrainUntil) && !active {
			continue
		}
		if !slices.Contains(rule.StableTargets, target) && !slices.Contains(rule.CandidateTargets, target) &&
			!slices.Contains(rule.DrainTargets, target) {
			rule.DrainTargets = append(rule.DrainTargets, target)
			if rule.DrainUntil.Before(previous.DrainUntil) {
				rule.DrainUntil = previous.DrainUntil
			}
		}
	}
}
