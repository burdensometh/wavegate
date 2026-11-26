package rollout

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"wavegate/discovery"
	"wavegate/traffic"
	"wavegate/v1"
)

func (e *Engine) prepare(now time.Time, state v1.ReleaseState, live v1.TrafficBinding, kind string, step int, rule v1.Rule, reason string) error {
	id, err := v1.Digest(struct {
		UID      string
		Sequence uint64
	}{state.Spec.UID, e.journal.Sequence() + 1})
	if err != nil {
		return err
	}
	state.Intent = &v1.Intent{Operation: id, ExpectedVersion: live.ResourceVersion, Kind: kind, Step: step, Rule: rule}
	if kind == v1.RollBack {
		state.Status.Phase = v1.RollingBack
		state.Window = nil
		setCondition(&state.Status, now, "Progressing", false, reason)
	}
	return e.commit(now, state, "TrafficPrepared", reason, state.Reservation)
}

func (e *Engine) execute(ctx context.Context, now time.Time, state v1.ReleaseState, live v1.TrafficBinding, snapshot discovery.Snapshot) error {
	intent := *state.Intent
	if live.Operation == intent.Operation {
		if live.OwnerUID != state.Spec.UID || !traffic.SameRule(live.Rule, intent.Rule) || live.ResourceVersion <= intent.ExpectedVersion {
			return fmt.Errorf("%w: live receipt does not match prepared traffic operation", v1.ErrConflict)
		}
		return e.complete(now, state, live)
	}
	if live.ResourceVersion != intent.ExpectedVersion {
		if state.Status.Phase == v1.Aborted || state.Status.Phase == v1.Promoted {
			return e.cancel(now, state, "BindingOwnerChanged")
		}
		return fmt.Errorf("%w: prepared binding precondition changed for %s", v1.ErrConflict, state.Spec.Service)
	}
	if state.Spec.Abort && intent.Kind != v1.RollBack && intent.Kind != v1.SyncStable {
		return e.cancel(now, state, "AbortRequested")
	}
	needsReady := v1.ChangesCandidate(intent.Kind)
	if needsReady && (state.Spec.Paused || !snapshot.Synced || snapshot.Version < intent.Rule.DiscoveryVersion ||
		snapshot.Generation > e.generations[state.Spec.UID]) {
		return e.cancel(now, state, "TrafficPreconditionsChanged")
	}
	var binding v1.TrafficBinding
	apply := func() error {
		var err error
		binding, err = e.plane.Apply(ctx, traffic.ApplyRequest{OwnerUID: state.Spec.UID, Intent: intent, At: now})
		return err
	}
	err := e.specs.WithVersion(state.Spec, func() error {
		if needsReady {
			return e.fence.WithReadyRule(intent.Rule, apply)
		}
		return apply()
	})
	if errors.Is(err, v1.ErrExpired) && needsReady {
		observed, _, observeErr := e.plane.Observe(state.Spec.Service)
		if observeErr != nil {
			return observeErr
		}
		if observed.ResourceVersion == intent.ExpectedVersion {
			return e.cancel(now, state, "EndpointGenerationChanged")
		}
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.complete(now, state, binding)
}

func (e *Engine) cancel(now time.Time, state v1.ReleaseState, reason string) error {
	kind := state.Intent.Kind
	state.Intent = nil
	if v1.ChangesCandidate(kind) && (reason == "TrafficPreconditionsChanged" || reason == "EndpointGenerationChanged") {
		state.Window, state.Status.Verdict = nil, nil
		setCondition(&state.Status, now, "WindowReady", false, "WindowInvalidated")
	}
	return e.commit(now, state, "TrafficCancelled", reason, state.Reservation)
}

func (e *Engine) complete(now time.Time, state v1.ReleaseState, binding v1.TrafficBinding) error {
	intent := *state.Intent
	if err := traffic.ValidateBinding(binding); err != nil {
		return err
	}
	if binding.Operation != intent.Operation || binding.OwnerUID != state.Spec.UID ||
		binding.ResourceVersion <= intent.ExpectedVersion || !traffic.SameRule(binding.Rule, intent.Rule) {
		return fmt.Errorf("%w: apply receipt does not match prepared traffic operation", v1.ErrConflict)
	}
	previous := state.Reservation
	state.Intent = nil
	state.Binding = &binding
	state.Status.BindingVersion = binding.ResourceVersion
	state.Status.DiscoveryVersion = max(state.Status.DiscoveryVersion, binding.Rule.DiscoveryVersion)
	state.Status.Step = intent.Step
	switch intent.Kind {
	case v1.Initialize:
		state.Status.Phase = v1.Waiting
		setCondition(&state.Status, now, "Progressing", false, "StableBindingInstalled")
	case v1.WeightStep, v1.RefreshStep:
		state.Status.Phase = v1.Progressing
		state.Status.Verdict = nil
		start := binding.AppliedAt
		state.Window = &v1.WindowKey{Service: state.Spec.Service, BindingVersion: binding.ResourceVersion, Start: start, End: start.Add(state.Spec.Window.Duration())}
		setCondition(&state.Status, now, "Progressing", true, "CandidateServing")
		setCondition(&state.Status, now, "WindowReady", false, "WindowOpen")
	case v1.RollBack:
		state.Status.Phase = v1.Aborted
		state.Reservation = nil
		setCondition(&state.Status, now, "Admitted", false, "ReservationReleased")
		setCondition(&state.Status, now, "Completed", true, "StableRestored")
	case v1.Promote:
		state.Status.Phase = v1.Promoted
		state.Window = nil
		state.Reservation = nil
		setCondition(&state.Status, now, "Admitted", false, "ReservationReleased")
		setCondition(&state.Status, now, "Progressing", false, "CandidatePromoted")
		setCondition(&state.Status, now, "Completed", true, "CandidatePromoted")
	case v1.SyncStable:
	}
	return e.commit(now, state, "TrafficCommitted", intent.Kind, previous)
}

func (e *Engine) rollback(now time.Time, state v1.ReleaseState, live v1.TrafficBinding, snapshot discovery.Snapshot, reason string) error {
	if state.Binding == nil {
		previous := state.Reservation
		state.Status.Phase, state.Status.Step = v1.Aborted, -1
		state.Reservation = nil
		setCondition(&state.Status, now, "Progressing", false, reason)
		setCondition(&state.Status, now, "Admitted", false, "ReservationReleased")
		setCondition(&state.Status, now, "Completed", true, "NoCandidateTraffic")
		return e.commit(now, state, "ReleaseAborted", reason, previous)
	}
	rule := traffic.Render(state.Spec, snapshot, state.Spec.Stable, "", 0)
	if !snapshot.Synced || snapshot.Version < state.Status.DiscoveryVersion {
		rule = v1.Rule{
			Service: state.Spec.Service, Stable: state.Spec.Stable, DiscoveryVersion: live.Rule.DiscoveryVersion,
			StableTargets: live.Rule.StableTargets, CandidateTargets: []v1.Target{},
			DomainRoutes: zeroDomainRoutes(live.Rule.DomainRoutes),
		}
	}
	e.keepDrain(now, live.Rule, &rule)
	return e.prepare(now, state, live, v1.RollBack, -1, rule, reason)
}

// zeroDomainRoutes keeps the rule's domain routes but pins every domain to its
// stable revision, so a stale rollback still stays inside the declared domains.
func zeroDomainRoutes(routes []v1.DomainRoute) []v1.DomainRoute {
	zeroed := slices.Clone(routes)
	for index := range zeroed {
		zeroed[index].CandidateWeight = 0
	}
	return zeroed
}

func (e *Engine) terminal(now time.Time, state v1.ReleaseState, live v1.TrafficBinding, snapshot discovery.Snapshot) error {
	if state.Binding == nil || live.OwnerUID != state.Spec.UID {
		return nil
	}
	if live.ResourceVersion != state.Binding.ResourceVersion || live.Operation != state.Binding.Operation ||
		!traffic.SameRule(live.Rule, state.Binding.Rule) {
		return fmt.Errorf("%w: terminal binding diverged for %s", v1.ErrConflict, state.Spec.Service)
	}
	rule := live.Rule
	if snapshot.Synced && snapshot.Version >= state.Status.DiscoveryVersion {
		rule = traffic.Render(state.Spec, snapshot, live.Rule.Stable, "", 0)
	} else {
		rule.DrainTargets = nil
		rule.DrainUntil = time.Time{}
	}
	e.keepDrain(now, live.Rule, &rule)
	if traffic.SameRoutes(live.Rule, rule) {
		return nil
	}
	return e.prepare(now, state, live, v1.SyncStable, state.Status.Step, rule, "ReconcileStableEndpoints")
}
