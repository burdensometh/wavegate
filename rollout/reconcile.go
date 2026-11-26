package rollout

import (
	"wavegate/traffic"
	"wavegate/v1"
)

// BindingFact classifies how a service's live traffic binding relates to one
// release's durable journal state. The read-only startup inspection and the
// reconcile loop both derive their view of actual traffic from this single
// classification, so the same snapshot yields the same facts in either entry.
type BindingFact int

const (
	// BindingCurrent: the live binding is exactly the committed decision.
	BindingCurrent BindingFact = iota
	// BindingReceipt: the live binding is the applied receipt of the prepared intent.
	BindingReceipt
	// BindingPrepared: a prepared intent still finds its apply precondition in
	// the live binding.
	BindingPrepared
	// BindingForeign: no live binding exists, or another release owns it.
	BindingForeign
	// BindingConflict: the live binding diverged from the durable record
	// outside any commit boundary.
	BindingConflict
)

// SameBinding reports whether the live binding is exactly the binding a
// committed decision recorded.
func SameBinding(recorded *v1.TrafficBinding, live v1.TrafficBinding) bool {
	return recorded != nil && recorded.ResourceVersion == live.ResourceVersion && recorded.Operation == live.Operation &&
		recorded.OwnerUID == live.OwnerUID && recorded.AppliedAt.Equal(live.AppliedAt) && traffic.SameRule(recorded.Rule, live.Rule)
}

// MatchesIntent reports whether the live binding is the applied receipt of the
// release's prepared intent.
func MatchesIntent(state v1.ReleaseState, live v1.TrafficBinding) bool {
	intent := state.Intent
	return intent != nil && live.OwnerUID == state.Spec.UID && live.Operation == intent.Operation &&
		live.ResourceVersion > intent.ExpectedVersion && traffic.SameRule(live.Rule, intent.Rule)
}

// ReconcileBinding classifies the live binding of a service against the
// durable state of one release. A zero live binding describes a service with
// no installed traffic.
func ReconcileBinding(state v1.ReleaseState, live v1.TrafficBinding) BindingFact {
	switch {
	case MatchesIntent(state, live):
		return BindingReceipt
	case SameBinding(state.Binding, live):
		if state.Intent != nil {
			return BindingPrepared
		}
		return BindingCurrent
	case state.Intent != nil && state.Binding == nil && live.ResourceVersion == state.Intent.ExpectedVersion:
		// The release never committed a binding: its first apply still finds
		// either an empty service or the stable-only binding a terminal
		// predecessor left behind for a legitimate takeover.
		return BindingPrepared
	case live.OwnerUID != state.Spec.UID:
		return BindingForeign
	default:
		return BindingConflict
	}
}
