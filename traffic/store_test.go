package traffic

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"wavegate/discovery"
	"wavegate/v1"
)

func TestBindingCASAndReceiptSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "traffic.v1.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	service := v1.Service{Namespace: "shop", Name: "checkout"}
	base := v1.Rule{Service: service, Stable: "old", Candidate: "new", StableTargets: []v1.Target{{Node: "a", Name: "pod", UID: "stable"}},
		CandidateTargets: []v1.Target{{Node: "b", Name: "pod", UID: "canary"}}, DiscoveryVersion: 25}
	first := ApplyRequest{OwnerUID: "release", At: time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC),
		Intent: v1.Intent{Operation: "stable-only", Rule: base}}
	initial, err := store.Apply(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	promoting := first
	promoting.Intent.Operation = "first-weight"
	promoting.Intent.ExpectedVersion = initial.ResourceVersion
	promoting.Intent.Rule.CandidateWeight = 20
	weighted, err := store.Apply(ctx, promoting)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	promoting.At = promoting.At.Add(time.Second)
	replayed, err := reopened.Apply(ctx, promoting)
	if err != nil || !replayed.AppliedAt.Equal(weighted.AppliedAt) || replayed.ResourceVersion != weighted.ResourceVersion {
		t.Fatalf("idempotent apply = %+v %v", replayed, err)
	}
	if _, err := reopened.Apply(ctx, first); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("stale operation overwrote current binding: %v", err)
	}
	foreign := first
	foreign.OwnerUID = "new-owner"
	foreign.Intent.Operation = "foreign-apply"
	foreign.Intent.ExpectedVersion = weighted.ResourceVersion
	if _, err := reopened.Apply(ctx, foreign); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("foreign owner replaced weighted routes: %v", err)
	}
	replayed.Rule.StableTargets[0].UID = "modified"
	observed, found, err := reopened.Observe(service)
	if err != nil || !found || observed.Rule.StableTargets[0].UID != "stable" {
		t.Fatalf("observe aliased exposed target: %+v %v", observed, err)
	}
}

func TestCompositeOperationIdentityScopesReplayToServiceAndOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "traffic.v1.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	serviceA := v1.Service{Namespace: "shop", Name: "checkout"}
	serviceB := v1.Service{Namespace: "shop", Name: "catalog"}
	ruleA := v1.Rule{Service: serviceA, Stable: "old", Candidate: "new",
		StableTargets:    []v1.Target{{Node: "a", Name: "pod", UID: "stable-a"}},
		CandidateTargets: []v1.Target{{Node: "b", Name: "pod", UID: "canary-a"}}, DiscoveryVersion: 25}
	ruleB := v1.Rule{Service: serviceB, Stable: "old", Candidate: "new",
		StableTargets:    []v1.Target{{Node: "a", Name: "pod", UID: "stable-b"}},
		CandidateTargets: []v1.Target{{Node: "b", Name: "pod", UID: "canary-b"}}, DiscoveryVersion: 25}
	at := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)

	// The same external flow string is minted independently by two services.
	identityA := &v1.OperationIdentity{Service: serviceA, OwnerUID: "owner-a", External: "shared-flow"}
	identityB := &v1.OperationIdentity{Service: serviceB, OwnerUID: "owner-b", External: "shared-flow"}
	applyA := ApplyRequest{OwnerUID: "owner-a", At: at, Intent: v1.Intent{
		Operation: "shared-flow", Identity: identityA, Rule: ruleA,
	}}
	applyB := ApplyRequest{OwnerUID: "owner-b", At: at, Intent: v1.Intent{
		Operation: "shared-flow", Identity: identityB, Rule: ruleB,
	}}
	bindingA, err := store.Apply(ctx, applyA)
	if err != nil {
		t.Fatal(err)
	}
	bindingB, err := store.Apply(ctx, applyB)
	if err != nil {
		t.Fatalf("same external flow on another service must not conflict: %v", err)
	}
	if bindingA.ResourceVersion == bindingB.ResourceVersion || bindingA.Identity == nil || bindingB.Identity == nil {
		t.Fatalf("cross-service applies collapsed into one binding: %+v %+v", bindingA, bindingB)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Retrying A's exact operation after reopen must acknowledge A's binding once.
	replayed, err := reopened.Apply(ctx, applyA)
	if err != nil || replayed.ResourceVersion != bindingA.ResourceVersion ||
		!replayed.AppliedAt.Equal(bindingA.AppliedAt) || replayed.Identity == nil ||
		*replayed.Identity != *identityA {
		t.Fatalf("composite replay = %+v %v", replayed, err)
	}

	// A foreign owner reusing A's external flow on A's service is not an
	// acknowledgement: the composite identities differ, so the store refuses
	// rather than treating the foreign apply as A's already-applied operation.
	intruder := applyA
	intruder.OwnerUID = "intruder"
	intruder.Intent.Identity = &v1.OperationIdentity{Service: serviceA, OwnerUID: "intruder", External: "shared-flow"}
	intruder.Intent.ExpectedVersion = bindingA.ResourceVersion
	if _, err := reopened.Apply(ctx, intruder); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("foreign owner reused the external flow: %v", err)
	}

	// An identity whose external flow disagrees with the operation token is invalid.
	inconsistent := applyA
	inconsistent.Intent.Identity = &v1.OperationIdentity{Service: serviceA, OwnerUID: "owner-a", External: "different-flow"}
	if _, err := reopened.Apply(ctx, inconsistent); !errors.Is(err, v1.ErrInvalid) {
		t.Fatalf("inconsistent identity accepted: %v", err)
	}
}

func TestLegacyAndCompositeRepresentationsDoNotCrossAcknowledge(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "traffic.v1.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	service := v1.Service{Namespace: "shop", Name: "checkout"}
	rule := v1.Rule{Service: service, Stable: "old", Candidate: "new",
		StableTargets:    []v1.Target{{Node: "a", Name: "pod", UID: "stable"}},
		CandidateTargets: []v1.Target{{Node: "b", Name: "pod", UID: "canary"}}, DiscoveryVersion: 25}
	at := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)

	// A legacy v1 operation carries no composite identity.
	legacy := ApplyRequest{OwnerUID: "owner-1", At: at, Intent: v1.Intent{
		Operation: "bare-token", Rule: rule,
	}}
	oldBinding, err := store.Apply(ctx, legacy)
	if err != nil || oldBinding.Identity != nil {
		t.Fatalf("legacy apply = %+v %v", oldBinding, err)
	}

	// A new composite operation reusing the legacy bare token must not be
	// acknowledged as the legacy result, nor create a second binding under the
	// same external flow: the mixed representation is refused outright.
	upgraded := legacy
	upgraded.Intent.Identity = &v1.OperationIdentity{Service: service, OwnerUID: "owner-1", External: "bare-token"}
	upgraded.Intent.ExpectedVersion = oldBinding.ResourceVersion
	upgraded.At = at.Add(time.Second)
	if _, err := store.Apply(ctx, upgraded); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("mixed representations sharing a flow were not refused: %v", err)
	}

	// Reopening keeps the legacy representation intact and its replay stays idempotent.
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	legacyReplay := legacy
	if got, err := reopened.Apply(ctx, legacyReplay); err != nil || got.ResourceVersion != oldBinding.ResourceVersion {
		t.Fatalf("legacy replay after reopen = %+v %v", got, err)
	}

	// A distinct composite external flow on the same service applies normally.
	next := legacy
	next.Intent.Operation = "composite-flow"
	next.Intent.Identity = &v1.OperationIdentity{Service: service, OwnerUID: "owner-1", External: "composite-flow"}
	next.Intent.ExpectedVersion = oldBinding.ResourceVersion
	next.At = at.Add(2 * time.Second)
	newBinding, err := reopened.Apply(ctx, next)
	if err != nil || newBinding.ResourceVersion != oldBinding.ResourceVersion+1 || newBinding.Identity == nil {
		t.Fatalf("fresh composite operation = %+v %v", newBinding, err)
	}
	if got, err := reopened.Apply(ctx, next); err != nil || got.ResourceVersion != newBinding.ResourceVersion {
		t.Fatalf("composite replay after reopen = %+v %v", got, err)
	}
}

func BenchmarkRenderRule(b *testing.B) {
	service := v1.Service{Namespace: "shop", Name: "checkout"}
	spec := v1.ReleaseSpec{Service: service}
	snapshot := discovery.Snapshot{Version: 4500, Synced: true, Ready: make([]v1.ReplicaObservation, 0, 4500)}
	for i := 0; i < 4500; i++ {
		revision := "stable"
		if i%3 == 0 {
			revision = "canary"
		}
		endpoint := v1.ReplicaObservation{
			Service: service, Revision: revision, Node: fmt.Sprintf("node-%d", i%80),
			Name: fmt.Sprintf("pod-%d", i), UID: fmt.Sprintf("uid-%d", i),
		}
		snapshot.Ready = append(snapshot.Ready, endpoint)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rule := Render(spec, snapshot, "stable", "canary", 20)
		if len(rule.StableTargets) != 3000 || len(rule.CandidateTargets) != 1500 {
			b.Fatal("rule dropped ready targets")
		}
	}
}
