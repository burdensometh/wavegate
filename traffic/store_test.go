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
