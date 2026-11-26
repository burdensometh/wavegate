package rollout

import (
	"testing"
	"time"

	"wavegate/v1"
)

var reconcileClock = time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)

func reconcileSpec() v1.ReleaseSpec {
	return v1.ReleaseSpec{
		APIVersion: v1.Version, Service: v1.Service{Namespace: "shop", Name: "cart"},
		Name: "rollout", UID: "rel-1", ResourceVersion: 2, Stable: "v1", Candidate: "v2", Steps: []int{25, 50},
		Window: v1.WindowPolicy{Seconds: 1, MinSamples: 2}, MinReady: 1,
	}
}

func reconcileRule(candidate string, weight int) v1.Rule {
	rule := v1.Rule{
		Service: v1.Service{Namespace: "shop", Name: "cart"}, Stable: "v1", Candidate: candidate,
		CandidateWeight: weight, DiscoveryVersion: 7,
		StableTargets: []v1.Target{{Node: "node-a", Name: "v1", UID: "uid-stable"}},
	}
	if candidate != "" {
		rule.CandidateTargets = []v1.Target{{Node: "node-a", Name: "v2", UID: "uid-candidate"}}
	}
	return rule
}

func reconcileBinding(version uint64, operation, owner string, rule v1.Rule, at time.Time) v1.TrafficBinding {
	return v1.TrafficBinding{
		APIVersion: v1.Version, OwnerUID: owner, ResourceVersion: version,
		Operation: operation, AppliedAt: at, Rule: rule,
	}
}

func TestReconcileBindingFacts(t *testing.T) {
	spec := reconcileSpec()
	committed := reconcileBinding(3, "op-1", spec.UID, reconcileRule("v2", 25), reconcileClock)
	receiptRule := reconcileRule("v2", 50)
	intent := &v1.Intent{Operation: "op-2", ExpectedVersion: 3, Kind: v1.WeightStep, Step: 1, Rule: receiptRule}
	receipt := reconcileBinding(4, "op-2", spec.UID, receiptRule, reconcileClock.Add(time.Second))

	state := func(binding *v1.TrafficBinding, prepared *v1.Intent) v1.ReleaseState {
		return v1.ReleaseState{Spec: spec, Binding: binding, Intent: prepared}
	}
	committedState := state(&committed, nil)
	preparedState := state(&committed, intent)

	cases := []struct {
		name  string
		state v1.ReleaseState
		live  v1.TrafficBinding
		want  BindingFact
	}{
		{"committed binding is live", committedState, committed, BindingCurrent},
		{"committed binding with forged apply time", committedState,
			reconcileBinding(3, "op-1", spec.UID, committed.Rule, reconcileClock.Add(time.Hour)), BindingConflict},
		{"committed binding replaced by another owner", committedState,
			reconcileBinding(4, "op-9", "rel-2", committed.Rule, reconcileClock), BindingForeign},
		{"committed binding absent", committedState, v1.TrafficBinding{}, BindingForeign},
		{"committed binding bumped outside a decision", committedState,
			reconcileBinding(4, "op-1", spec.UID, committed.Rule, reconcileClock), BindingConflict},
		{"never bound and still empty", state(nil, nil), v1.TrafficBinding{}, BindingForeign},
		{"never bound with foreign live binding", state(nil, nil),
			reconcileBinding(2, "op-9", "rel-2", committed.Rule, reconcileClock), BindingForeign},
		{"never bound but live binding claims this owner", state(nil, nil), committed, BindingConflict},
		{"prepared intent holds its committed precondition", preparedState, committed, BindingPrepared},
		{"prepared intent already applied", preparedState, receipt, BindingReceipt},
		{"first apply finds an empty service", state(nil, &v1.Intent{
			Operation: "op-3", ExpectedVersion: 0, Kind: v1.Initialize, Step: -1, Rule: reconcileRule("", 0),
		}), v1.TrafficBinding{}, BindingPrepared},
		{"first apply finds a terminal predecessor binding", state(nil, &v1.Intent{
			Operation: "op-3", ExpectedVersion: 5, Kind: v1.Initialize, Step: -1, Rule: reconcileRule("", 0),
		}), reconcileBinding(5, "op-0", "rel-0", reconcileRule("", 0), reconcileClock), BindingPrepared},
		{"prepared intent loses the binding to another owner", preparedState,
			reconcileBinding(4, "op-9", "rel-2", committed.Rule, reconcileClock), BindingForeign},
		{"prepared intent loses the binding version", preparedState,
			reconcileBinding(4, "op-1", spec.UID, committed.Rule, reconcileClock), BindingConflict},
		{"receipt with a rewound version", preparedState,
			reconcileBinding(3, "op-2", spec.UID, receiptRule, reconcileClock), BindingConflict},
		{"receipt with a forged rule", preparedState,
			reconcileBinding(4, "op-2", spec.UID, reconcileRule("v2", 40), reconcileClock), BindingConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReconcileBinding(tc.state, tc.live); got != tc.want {
				t.Fatalf("ReconcileBinding = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSameBindingRequiresExactReceipt(t *testing.T) {
	committed := reconcileBinding(3, "op-1", "rel-1", reconcileRule("v2", 25), reconcileClock)
	if SameBinding(nil, committed) {
		t.Fatal("nil recorded binding cannot match a live binding")
	}
	if !SameBinding(&committed, committed) {
		t.Fatal("identical bindings must match")
	}
	drifted := committed
	drifted.AppliedAt = committed.AppliedAt.Add(time.Second)
	if SameBinding(&committed, drifted) {
		t.Fatal("apply time drift must diverge the committed binding fact")
	}
}

func TestMatchesIntentRequiresAppliedReceipt(t *testing.T) {
	spec := reconcileSpec()
	rule := reconcileRule("v2", 50)
	state := v1.ReleaseState{
		Spec:   spec,
		Intent: &v1.Intent{Operation: "op-2", ExpectedVersion: 3, Kind: v1.WeightStep, Step: 1, Rule: rule},
	}
	if MatchesIntent(v1.ReleaseState{Spec: spec}, reconcileBinding(4, "op-2", spec.UID, rule, reconcileClock)) {
		t.Fatal("no prepared intent means no receipt")
	}
	if !MatchesIntent(state, reconcileBinding(4, "op-2", spec.UID, rule, reconcileClock)) {
		t.Fatal("applied receipt must match its prepared intent")
	}
	if MatchesIntent(state, reconcileBinding(4, "op-2", "rel-2", rule, reconcileClock)) {
		t.Fatal("receipt owned by another release must not match")
	}
	if MatchesIntent(state, reconcileBinding(3, "op-2", spec.UID, rule, reconcileClock)) {
		t.Fatal("receipt at the expected version never advanced")
	}
}
