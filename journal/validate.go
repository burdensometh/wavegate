package journal

import (
	"reflect"

	"wavegate/metrics"
	"wavegate/quota"
	"wavegate/traffic"
	"wavegate/v1"
)

func validateRecord(r v1.DecisionRecord) error {
	if err := v1.CheckVersion(r.APIVersion); err != nil {
		return err
	}
	if r.Sequence == 0 || r.At.IsZero() || !v1.ValidToken(r.Kind) || !v1.ValidToken(r.Reason) {
		return v1.Invalid("decision sequence, time, kind or reason")
	}
	if (r.Sequence == 1 && r.PreviousDigest != "") || (r.Sequence > 1 && !v1.ValidDigest(r.PreviousDigest)) {
		return v1.Invalid("decision previous digest")
	}
	digest, err := digestRecord(r)
	if err != nil {
		return err
	}
	if digest != r.Digest {
		return v1.Invalid("decision digest")
	}
	return validateState(r.State, r.Sequence)
}

func validateState(state v1.ReleaseState, sequence uint64) error {
	spec, status := state.Spec, state.Status
	if err := spec.Validate(); err != nil {
		return err
	}
	if err := v1.CheckVersion(status.APIVersion); err != nil {
		return err
	}
	if status.Service != spec.Service || status.ReleaseUID != spec.UID || status.ObservedVersion != spec.ResourceVersion ||
		status.ObservedVersion == 0 || status.DecisionSequence != sequence || !v1.KnownPhase(status.Phase) ||
		status.Step < -1 || status.Step >= len(spec.Steps) {
		return v1.Invalid("decision status identity or phase")
	}
	conditions := map[string]bool{}
	for _, condition := range status.Conditions {
		if !v1.ValidToken(condition.Type) || !v1.ValidToken(condition.Reason) || condition.LastTransitionTime.IsZero() || conditions[condition.Type] {
			return v1.Invalid("decision condition")
		}
		conditions[condition.Type] = true
	}
	if state.Binding == nil {
		if status.BindingVersion != 0 {
			return v1.Invalid("status references missing traffic binding")
		}
	} else {
		binding := *state.Binding
		if err := traffic.ValidateBinding(binding); err != nil {
			return err
		}
		if binding.OwnerUID != spec.UID || binding.Rule.Service != spec.Service || status.BindingVersion != binding.ResourceVersion {
			return v1.Invalid("decision binding does not match release or status")
		}
		if binding.Rule.DiscoveryVersion > status.DiscoveryVersion {
			return v1.Invalid("binding exceeds the recorded discovery watermark")
		}
	}
	if state.Reservation != nil {
		if err := quota.Validate(*state.Reservation); err != nil {
			return err
		}
		reservation := state.Reservation
		if reservation.Service != spec.Service || reservation.ReleaseUID != spec.UID || reservation.Units != spec.MinReady ||
			reservation.ResourceVersion > sequence || v1.Terminal(status.Phase) {
			return v1.Invalid("decision reservation does not match release or phase")
		}
	}
	if state.Window != nil {
		if err := state.Window.Validate(); err != nil {
			return err
		}
		if state.Binding == nil || status.BindingVersion != state.Window.BindingVersion || state.Window.Service != spec.Service ||
			!state.Window.Start.Equal(state.Binding.AppliedAt) || state.Window.End.Sub(state.Window.Start) != spec.Window.Duration() ||
			state.Binding.Rule.CandidateWeight == 0 || status.Phase != v1.Progressing {
			return v1.Invalid("decision window does not match binding or phase")
		}
	}
	if status.Verdict != nil {
		if err := metrics.CheckVerdict(*status.Verdict); err != nil {
			return err
		}
		if status.Verdict.Result == v1.Pending || status.Verdict.Key.Service != spec.Service ||
			status.Verdict.Key.BindingVersion > status.BindingVersion ||
			(state.Window != nil && status.Verdict.Key != *state.Window) {
			return v1.Invalid("decision verdict does not match release or window")
		}
	}
	if err := validatePhase(state); err != nil {
		return err
	}
	if state.Intent != nil {
		return validateIntent(state)
	}
	return nil
}

func validatePhase(state v1.ReleaseState) error {
	spec, status := state.Spec, state.Status
	switch status.Phase {
	case v1.Waiting:
		if status.Step != -1 || (state.Binding != nil && (state.Binding.Rule.Stable != spec.Stable || state.Binding.Rule.CandidateWeight != 0)) {
			return v1.Invalid("waiting release must use stable-only traffic")
		}
	case v1.Progressing, v1.Paused, v1.RollingBack:
		if state.Binding == nil || state.Reservation == nil || state.Binding.Rule.Stable != spec.Stable {
			return v1.Invalid("active release requires its original stable binding and reservation")
		}
		if status.Step >= 0 && (state.Binding.Rule.Candidate != spec.Candidate ||
			state.Binding.Rule.CandidateWeight != spec.Steps[status.Step]) {
			return v1.Invalid("active binding does not match the fixed step")
		}
		if status.Step < 0 && state.Binding.Rule.CandidateWeight != 0 {
			return v1.Invalid("weighted binding has no matching step")
		}
		if status.Phase == v1.Progressing && status.Step < 0 {
			return v1.Invalid("progressing release has no active step")
		}
	case v1.Aborted:
		if status.Step != -1 || (state.Binding != nil && (state.Binding.Rule.Stable != spec.Stable || state.Binding.Rule.CandidateWeight != 0)) {
			return v1.Invalid("aborted release still serves the candidate")
		}
	case v1.Promoted:
		if status.Step != len(spec.Steps)-1 || state.Binding == nil || state.Binding.Rule.Stable != spec.Candidate ||
			state.Binding.Rule.CandidateWeight != 0 || status.Verdict == nil || status.Verdict.Result != v1.Pass {
			return v1.Invalid("promoted release lacks a final successful gate and stable-only binding")
		}
	}
	return nil
}

func validateIntent(state v1.ReleaseState) error {
	intent, spec, status := state.Intent, state.Spec, state.Status
	if !v1.ValidToken(intent.Operation) || intent.Rule.Service != spec.Service ||
		(state.Binding != nil && intent.ExpectedVersion != status.BindingVersion) {
		return v1.Invalid("decision intent identity")
	}
	if err := intent.Rule.Validate(); err != nil {
		return err
	}
	if state.Reservation == nil && intent.Kind != v1.SyncStable {
		return v1.Invalid("decision intent requires reservation")
	}
	switch intent.Kind {
	case v1.Initialize:
		if intent.Rule.Stable == spec.Stable && intent.Rule.Candidate == spec.Candidate && intent.Rule.CandidateWeight == 0 &&
			intent.Step == -1 && (status.Phase == v1.Waiting || status.Phase == v1.Paused) {
			return nil
		}
	case v1.RollBack:
		if intent.Rule.Stable == spec.Stable && intent.Rule.Candidate == "" && intent.Step == -1 && status.Phase == v1.RollingBack {
			return nil
		}
	case v1.Promote:
		if intent.Rule.Stable == spec.Candidate && intent.Rule.Candidate == "" && intent.Step == status.Step &&
			status.Step == len(spec.Steps)-1 && status.Verdict != nil && status.Verdict.Result == v1.Pass &&
			len(intent.Rule.StableTargets) >= spec.MinReady {
			return nil
		}
	case v1.WeightStep, v1.RefreshStep:
		if intent.Step >= 0 && intent.Step < len(spec.Steps) && intent.Rule.Stable == spec.Stable &&
			intent.Rule.Candidate == spec.Candidate && intent.Rule.CandidateWeight == spec.Steps[intent.Step] &&
			len(intent.Rule.CandidateTargets) >= spec.MinReady && !v1.Terminal(status.Phase) {
			switch intent.Kind {
			case v1.RefreshStep:
				if intent.Step == status.Step {
					return nil
				}
			case v1.WeightStep:
				if intent.Step == status.Step+1 && (intent.Step == 0 || status.Verdict != nil && status.Verdict.Result == v1.Pass) {
					return nil
				}
			}
		}
	case v1.SyncStable:
		if state.Binding != nil && v1.Terminal(status.Phase) && intent.Rule.Stable == state.Binding.Rule.Stable &&
			intent.Rule.CandidateWeight == 0 && intent.Step == status.Step {
			return nil
		}
	}
	return v1.Invalid("decision intent is inconsistent with phase or step")
}

func validateTransition(previous, current v1.DecisionRecord) error {
	state := current.State
	if previous.Sequence == 0 {
		if current.Kind != "ReleaseObserved" || state.Reservation != nil || state.Binding != nil || state.Intent != nil ||
			state.Status.Phase != v1.Waiting {
			return v1.Invalid("first decision must observe the release")
		}
		return nil
	}
	old := previous.State
	if current.At.Before(previous.At) || state.Spec.ResourceVersion < old.Spec.ResourceVersion ||
		!v1.SameReleaseDefinition(state.Spec, old.Spec) || (old.Spec.Abort && !state.Spec.Abort) ||
		state.Status.BindingVersion < old.Status.BindingVersion || state.Status.DiscoveryVersion < old.Status.DiscoveryVersion {
		return v1.Invalid("decision moved backwards or changed immutable identity")
	}
	if state.Spec.ResourceVersion == old.Spec.ResourceVersion && !reflect.DeepEqual(state.Spec, old.Spec) {
		return v1.Invalid("same spec resource version refers to different desired state")
	}
	if v1.Terminal(old.Status.Phase) && old.Status.Phase != state.Status.Phase {
		return v1.Invalid("terminal release changed phase")
	}
	if state.Status.BindingVersion > old.Status.BindingVersion {
		if old.Intent == nil || state.Intent != nil || state.Binding == nil ||
			state.Binding.Operation != old.Intent.Operation || !traffic.SameRule(state.Binding.Rule, old.Intent.Rule) {
			return v1.Invalid("changed binding lacks matching prepared operation")
		}
	} else if !reflect.DeepEqual(state.Binding, old.Binding) {
		return v1.Invalid("unchanged binding version has different content")
	}
	if (old.Spec.ResourceVersion != state.Spec.ResourceVersion) != (current.Kind == "DesiredObserved") {
		return v1.Invalid("desired version change lacks a matching observation")
	}
	switch {
	case old.Reservation == nil && state.Reservation != nil:
		if current.Kind != "ReservationGranted" || state.Reservation.ResourceVersion != current.Sequence {
			return v1.Invalid("reservation was acquired outside an admission record")
		}
	case old.Reservation != nil && state.Reservation == nil:
		if !v1.Terminal(state.Status.Phase) {
			return v1.Invalid("active release relinquished its reservation")
		}
	case old.Reservation != nil && *old.Reservation != *state.Reservation:
		return v1.Invalid("live reservation changed identity")
	}
	return validateDecisionKind(old, current)
}

func validateDecisionKind(old v1.ReleaseState, record v1.DecisionRecord) error {
	state := record.State
	if state.Status.BindingVersion != old.Status.BindingVersion && record.Kind != "TrafficCommitted" {
		return v1.Invalid("binding version changed outside a traffic commit")
	}
	if !reflect.DeepEqual(state.Intent, old.Intent) &&
		(record.Kind != "TrafficPrepared" && record.Kind != "TrafficCancelled" && record.Kind != "TrafficCommitted") {
		return v1.Invalid("intent was modified outside a traffic decision")
	}
	switch record.Kind {
	case "DesiredObserved", "ConditionChanged":
		if state.Status.Phase == old.Status.Phase {
			return nil
		}
	case "ReservationGranted":
		if old.Reservation == nil && state.Reservation != nil && state.Status.Phase == v1.Waiting {
			return nil
		}
	case "TrafficPrepared":
		if old.Intent == nil && state.Intent != nil {
			return nil
		}
	case "TrafficCancelled":
		if old.Intent != nil && state.Intent == nil && state.Status.Phase == old.Status.Phase {
			return nil
		}
	case "TrafficCommitted":
		if old.Intent != nil && state.Intent == nil && state.Status.BindingVersion > old.Status.BindingVersion &&
			record.Reason == old.Intent.Kind && state.Status.Step == old.Intent.Step {
			return nil
		}
	case "WindowInvalidated":
		if old.Window != nil && state.Window == nil && state.Status.Verdict == nil && state.Status.Phase == old.Status.Phase {
			return nil
		}
	case "WindowEvaluated":
		if old.Window != nil && reflect.DeepEqual(old.Window, state.Window) && old.Status.Verdict == nil && state.Status.Verdict != nil {
			return nil
		}
	case "ReleasePaused":
		if state.Spec.Paused && state.Window == nil && state.Status.Phase == v1.Paused {
			return nil
		}
	case "ReleaseAborted":
		if old.Binding == nil && state.Binding == nil && state.Spec.Abort && state.Status.Phase == v1.Aborted {
			return nil
		}
	}
	return v1.Invalid("decision does not match its state transition")
}
