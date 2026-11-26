package server

import (
	"context"
	"fmt"
	"reflect"

	"wavegate/rollout"
	"wavegate/v1"
)

type PendingCommit struct {
	Service         v1.Service `json:"service"`
	ReleaseUID      string     `json:"releaseUid"`
	Operation       string     `json:"operation"`
	Kind            string     `json:"kind"`
	ExpectedVersion uint64     `json:"expectedVersion"`
	LiveVersion     uint64     `json:"liveVersion"`
	Applied         bool       `json:"applied"`
}

type RecoveryReport struct {
	APIVersion      string          `json:"apiVersion"`
	EndpointVersion uint64          `json:"endpointVersion"`
	EndpointCount   int             `json:"endpointCount"`
	JournalSequence uint64          `json:"journalSequence"`
	ReleaseCount    int             `json:"releaseCount"`
	BindingCount    int             `json:"bindingCount"`
	SampleVersion   uint64          `json:"sampleVersion"`
	SealedWindows   int             `json:"sealedWindows"`
	Pending         []PendingCommit `json:"pending"`
	StatusRepublish []string        `json:"statusRepublish"`
}

// inspectRecovery runs with exclusive process ownership before starting the
// runner. Independent v1 documents can legitimately lag at the commit cuts.
func (a *App) inspectRecovery() (RecoveryReport, error) {
	endpoints, err := a.Catalog.List(context.Background())
	if err != nil {
		return RecoveryReport{}, err
	}
	releases, err := a.Specs.List()
	if err != nil {
		return RecoveryReport{}, err
	}
	states, err := a.Journal.States()
	if err != nil {
		return RecoveryReport{}, err
	}
	bindings, err := a.Plane.Bindings()
	if err != nil {
		return RecoveryReport{}, err
	}
	samples, sealed, err := a.Metrics.Inventory()
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{
		APIVersion: v1.Version, EndpointVersion: endpoints.ResourceVersion, EndpointCount: len(endpoints.Items),
		JournalSequence: a.Journal.Sequence(), ReleaseCount: len(releases.Items), BindingCount: len(bindings),
		SampleVersion: samples, SealedWindows: sealed, Pending: []PendingCommit{}, StatusRepublish: []string{},
	}
	desired := make(map[string]rollout.ReleaseView, len(releases.Items))
	for _, view := range releases.Items {
		desired[view.Spec.UID] = view
	}
	byUID := make(map[string]v1.ReleaseState, len(states))
	byService := make(map[v1.Service]v1.TrafficBinding, len(bindings))
	for _, binding := range bindings {
		if binding.Rule.DiscoveryVersion > endpoints.ResourceVersion {
			return RecoveryReport{}, recoveryError(binding.Rule.Service, "traffic references future endpoint inventory")
		}
		byService[binding.Rule.Service] = binding
	}
	for _, state := range states {
		spec := state.Spec
		view, exists := desired[spec.UID]
		if !exists || !v1.SameReleaseDefinition(view.Spec, spec) || view.Spec.ResourceVersion < spec.ResourceVersion ||
			(view.Spec.ResourceVersion == spec.ResourceVersion && !reflect.DeepEqual(view.Spec, spec)) ||
			(spec.Abort && !view.Spec.Abort) {
			return RecoveryReport{}, recoveryError(spec.Service, "journal desired state does not match the release inventory")
		}
		if state.Status.DiscoveryVersion > endpoints.ResourceVersion {
			return RecoveryReport{}, recoveryError(spec.Service, "decision references future endpoint inventory")
		}
		if err := checkPublished(view, state.Status); err != nil {
			return RecoveryReport{}, err
		}
		if view.Status == nil || view.Status.DecisionSequence < state.Status.DecisionSequence {
			report.StatusRepublish = append(report.StatusRepublish, spec.UID)
		}
		if verdict := state.Status.Verdict; verdict != nil {
			if err := a.Metrics.CheckSealed(*verdict); err != nil {
				return RecoveryReport{}, fmt.Errorf("%s metrics recovery: %w", spec.Service, err)
			}
		}
		byUID[spec.UID] = state
		live := byService[spec.Service]
		pending, err := checkReleaseBinding(state, live)
		if err != nil {
			return RecoveryReport{}, err
		}
		if pending != nil {
			report.Pending = append(report.Pending, *pending)
		}
	}
	for _, view := range releases.Items {
		if _, known := byUID[view.Spec.UID]; !known && view.Status != nil {
			return RecoveryReport{}, recoveryError(view.Spec.Service, "published status is missing its journal decision")
		}
	}
	for _, live := range bindings {
		state, exists := byUID[live.OwnerUID]
		if !exists || state.Spec.Service != live.Rule.Service ||
			(!rollout.SameBinding(state.Binding, live) && !rollout.MatchesIntent(state, live)) {
			return RecoveryReport{}, recoveryError(live.Rule.Service, "live binding has no durable decision or apply intent")
		}
	}
	return report, nil
}

func recoveryError(service v1.Service, reason string) error {
	return fmt.Errorf("%w: %s: %s", v1.ErrConflict, service, reason)
}

func checkPublished(view rollout.ReleaseView, status v1.ReleaseStatus) error {
	published := view.Status
	if published == nil {
		return nil
	}
	if published.DecisionSequence > status.DecisionSequence || published.ObservedVersion > status.ObservedVersion ||
		(published.DecisionSequence == status.DecisionSequence && !reflect.DeepEqual(*published, status)) {
		return recoveryError(status.Service, "published status diverges from the journal")
	}
	return nil
}

func checkReleaseBinding(state v1.ReleaseState, live v1.TrafficBinding) (*PendingCommit, error) {
	spec, status := state.Spec, state.Status
	intent := state.Intent
	active := !v1.Terminal(status.Phase)
	fact := rollout.ReconcileBinding(state, live)
	diverged := fact == rollout.BindingForeign || fact == rollout.BindingConflict
	if active && state.Binding != nil && diverged {
		return nil, recoveryError(spec.Service, "active release has lost its recorded binding")
	}
	if intent == nil {
		return nil, nil
	}
	applied := fact == rollout.BindingReceipt
	if active && !applied && diverged {
		return nil, recoveryError(spec.Service, "prepared intent has lost its traffic precondition")
	}
	return &PendingCommit{
		Service: spec.Service, ReleaseUID: spec.UID, Operation: intent.Operation, Kind: intent.Kind,
		ExpectedVersion: intent.ExpectedVersion, LiveVersion: live.ResourceVersion, Applied: applied,
	}, nil
}
