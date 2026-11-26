package journal

import (
	"errors"
	"testing"
	"time"

	"wavegate/v1"
)

func TestValidateIntentAcceptsBothOperationRepresentations(t *testing.T) {
	base := releaseFixture()
	service := base.Spec.Service
	at := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	rule := v1.Rule{Service: service, Stable: "alpha", Candidate: "beta",
		StableTargets:    []v1.Target{{Node: "a", Name: "pod", UID: "stable"}},
		CandidateTargets: []v1.Target{{Node: "b", Name: "pod", UID: "canary"}}, DiscoveryVersion: 1}
	reservation := v1.NamespaceReservation{
		APIVersion: v1.Version, Service: service, ReleaseUID: "release-1", ResourceVersion: 3,
		Units: 1, AcquiredAt: at,
	}
	withIntent := func(intent *v1.Intent) v1.ReleaseState {
		state := base
		state.Status.DecisionSequence = 3
		state.Reservation = &reservation
		state.Status.Phase = v1.Waiting
		state.Intent = intent
		return state
	}

	t.Run("legacy bare operation", func(t *testing.T) {
		state := withIntent(&v1.Intent{Operation: "bare-token", ExpectedVersion: 0, Kind: v1.Initialize, Step: -1, Rule: rule})
		if err := validateState(state, 3); err != nil {
			t.Fatalf("legacy intent rejected: %v", err)
		}
	})

	t.Run("composite operation", func(t *testing.T) {
		state := withIntent(&v1.Intent{
			Operation:       "external-flow",
			Identity:        &v1.OperationIdentity{Service: service, OwnerUID: "release-1", External: "external-flow"},
			ExpectedVersion: 0, Kind: v1.Initialize, Step: -1, Rule: rule,
		})
		if err := validateState(state, 3); err != nil {
			t.Fatalf("composite intent rejected: %v", err)
		}
	})

	t.Run("identity disagreeing with operation", func(t *testing.T) {
		state := withIntent(&v1.Intent{
			Operation:       "external-flow",
			Identity:        &v1.OperationIdentity{Service: service, OwnerUID: "release-1", External: "different"},
			ExpectedVersion: 0, Kind: v1.Initialize, Step: -1, Rule: rule,
		})
		if err := validateState(state, 3); !errors.Is(err, v1.ErrInvalid) {
			t.Fatalf("inconsistent identity accepted: %v", err)
		}
	})

	t.Run("identity scoped to another service", func(t *testing.T) {
		state := withIntent(&v1.Intent{
			Operation: "external-flow",
			Identity: &v1.OperationIdentity{
				Service: v1.Service{Namespace: "other", Name: "api"}, OwnerUID: "release-1", External: "external-flow",
			},
			ExpectedVersion: 0, Kind: v1.Initialize, Step: -1, Rule: rule,
		})
		if err := validateState(state, 3); !errors.Is(err, v1.ErrInvalid) {
			t.Fatalf("foreign-service identity accepted: %v", err)
		}
	})
}
