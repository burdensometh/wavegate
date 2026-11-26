package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"wavegate/v1"
)

func phaseEndpoint(node, name, uid, phase string) v1.ReplicaObservation {
	endpoint := testEndpoint(node, name, uid)
	endpoint.APIVersion = v1.VersionV2
	endpoint.Phase = phase
	endpoint.Ready = phase == v1.PhaseServing
	return endpoint
}

func TestV1BooleanAndPhaseAgree(t *testing.T) {
	cases := []struct {
		name  string
		input v1.ReplicaObservation
		want  bool
	}{
		{"v1 ready true", v1.ReplicaObservation{APIVersion: v1.Version, Ready: true}, true},
		{"v1 ready false", v1.ReplicaObservation{APIVersion: v1.Version, Ready: false}, false},
		{"v2 serving", phaseEndpoint("node-a", "pod", "uid", v1.PhaseServing), true},
		{"v2 unknown", phaseEndpoint("node-a", "pod", "uid", v1.PhaseUnknown), false},
		{"v2 terminating", phaseEndpoint("node-a", "pod", "uid", v1.PhaseTerminating), false},
	}
	for _, tc := range cases {
		if got := tc.input.Serving(); got != tc.want {
			t.Errorf("%s: Serving()=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestPhaseWinsOverLegacyBoolean(t *testing.T) {
	// A newer source may deliver an explicit phase through the v1 document.
	// The phase must be authoritative even when the legacy boolean disagrees.
	serving := v1.ReplicaObservation{APIVersion: v1.Version, Ready: false, Phase: v1.PhaseServing}
	if !serving.Serving() {
		t.Fatal("explicit Serving phase must override ready=false")
	}
	terminating := v1.ReplicaObservation{APIVersion: v1.Version, Ready: true, Phase: v1.PhaseTerminating}
	if terminating.Serving() {
		t.Fatal("explicit Terminating phase must override ready=true")
	}
	if !terminating.Terminating() {
		t.Fatal("terminating endpoint not reported")
	}
}

func TestTerminatingEndpointDoesNotDisplaceNewerUID(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	live := phaseEndpoint("node-a", "pod", "new-uid", v1.PhaseServing)
	if _, err := catalog.Put(live); err != nil {
		t.Fatal(err)
	}
	ending := phaseEndpoint("node-a", "pod", "old-uid", v1.PhaseTerminating)
	if _, err := catalog.Put(ending); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("terminating observation replaced a live UID: %v", err)
	}
	list, err := catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].UID != "new-uid" {
		t.Fatalf("inventory after rejected terminating write = %+v", list.Items)
	}
}

func TestTerminatingEndpointIsNotServed(t *testing.T) {
	index := NewIndex()
	serving := phaseEndpoint("node-a", "pod", "keep", v1.PhaseServing)
	ending := phaseEndpoint("node-b", "pod", "drain", v1.PhaseTerminating)
	unknown := phaseEndpoint("node-c", "pod", "unknown", v1.PhaseUnknown)
	serving.ResourceVersion, ending.ResourceVersion, unknown.ResourceVersion = 3, 4, 5
	list := v1.EndpointList{APIVersion: v1.VersionV2, ResourceVersion: 5, Items: []v1.ReplicaObservation{serving, ending, unknown}}
	if err := index.Replace(list); err != nil {
		t.Fatal(err)
	}
	snapshot := index.Snapshot(serving.Service)
	if len(snapshot.Ready) != 1 || snapshot.Ready[0].UID != "keep" {
		t.Fatalf("snapshot served non-serving endpoints: %+v", snapshot.Ready)
	}
	// A terminating observation for an old UID must not evict the live one.
	evict := phaseEndpoint("node-a", "pod", "old", v1.PhaseTerminating)
	evict.ResourceVersion = 6
	if err := index.Apply(v1.EndpointEvent{APIVersion: v1.VersionV2, Type: v1.Modified, ResourceVersion: 6, Object: evict}); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("index applied a terminating overwrite: %v", err)
	}
	if got := index.Snapshot(serving.Service); len(got.Ready) != 1 || got.Ready[0].UID != "keep" {
		t.Fatalf("snapshot after rejected overwrite = %+v", got.Ready)
	}
}

func TestRestartKeepsPhaseInterpretation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	serving := phaseEndpoint("node-a", "pod", "keep", v1.PhaseServing)
	ending := phaseEndpoint("node-b", "pod", "drain", v1.PhaseTerminating)
	unknown := phaseEndpoint("node-c", "pod", "unknown", v1.PhaseUnknown)
	for _, endpoint := range []v1.ReplicaObservation{serving, ending, unknown} {
		if _, err := catalog.Put(endpoint); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := OpenCatalog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	list, err := reopened.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	index := NewIndex()
	if err := index.Replace(list); err != nil {
		t.Fatal(err)
	}
	snapshot := index.Snapshot(serving.Service)
	if len(snapshot.Ready) != 1 || snapshot.Ready[0].UID != "keep" {
		t.Fatalf("restart changed readiness interpretation: %+v", snapshot.Ready)
	}
	// The identity ledger still recognizes the retired UIDs so they cannot be
	// reused after a restart.
	for _, uid := range []string{"drain", "unknown"} {
		reused := phaseEndpoint("node-z", "pod", uid, v1.PhaseServing)
		if _, err := reopened.Put(reused); !errors.Is(err, v1.ErrConflict) {
			t.Fatalf("reused UID %s after restart: %v", uid, err)
		}
	}
}

func TestV2EndpointRequiresExplicitPhase(t *testing.T) {
	endpoint := testEndpoint("node-a", "pod", "uid")
	endpoint.APIVersion = v1.VersionV2
	if err := endpoint.Validate(); err == nil {
		t.Fatal("v2 endpoint without a phase accepted")
	}
	endpoint.Phase = "Ready"
	if err := endpoint.Validate(); err == nil {
		t.Fatal("unknown phase accepted")
	}
	endpoint.Phase = v1.PhaseServing
	if err := endpoint.Validate(); err != nil {
		t.Fatalf("valid v2 endpoint rejected: %v", err)
	}
}

func TestInterleavedSourcesRespectPerUIDOrdering(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	// A v1 source reports a live endpoint for the old UID.
	legacy := testEndpoint("node-a", "pod", "old-uid")
	legacy.Ready = true
	first, err := catalog.Put(legacy)
	if err != nil {
		t.Fatal(err)
	}
	// A v2 source supersedes it with a new UID that is serving.
	modernInput := phaseEndpoint("node-a", "pod", "new-uid", v1.PhaseServing)
	modernInput.ResourceVersion = first.ResourceVersion
	modern, err := catalog.Put(modernInput)
	if err != nil {
		t.Fatal(err)
	}
	if modern.UID != "new-uid" {
		t.Fatalf("new UID not adopted: %+v", modern)
	}
	// The old UID is retired and a stale observation cannot resurrect it, even
	// though the key now belongs to the new UID.
	stale := testEndpoint("node-a", "pod", "old-uid")
	if _, err := catalog.Put(stale); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("retired UID resurrected: %v", err)
	}
	// A terminating observation of the retired UID also cannot take the slot.
	endingOld := phaseEndpoint("node-a", "pod", "old-uid", v1.PhaseTerminating)
	endingOld.ResourceVersion = modern.ResourceVersion
	if _, err := catalog.Put(endingOld); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("terminating retired UID took the slot: %v", err)
	}
	// The new UID can itself move to terminating with its current version.
	goingDown := phaseEndpoint("node-a", "pod", "new-uid", v1.PhaseTerminating)
	goingDown.ResourceVersion = modern.ResourceVersion
	if _, err := catalog.Put(goingDown); err != nil {
		t.Fatal(err)
	}
	// A fresh serving observation of the new UID is still allowed.
	back := phaseEndpoint("node-a", "pod", "new-uid", v1.PhaseServing)
	back.ResourceVersion = modern.ResourceVersion + 1
	if _, err := catalog.Put(back); err != nil {
		t.Fatal(err)
	}
	list, err := catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].UID != "new-uid" || !list.Items[0].Serving() {
		t.Fatalf("final inventory = %+v", list.Items)
	}
	_ = first
}
