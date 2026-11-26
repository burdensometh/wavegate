package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"wavegate/v1"
)

func TestV1BooleanCatalogRoundTripsUnchanged(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	ready := testEndpoint("node-a", "pod", "ready-uid")
	notReady := testEndpoint("node-b", "pod", "pending-uid")
	notReady.Ready = false
	for _, endpoint := range []v1.ReplicaObservation{ready, notReady} {
		stored, err := catalog.Put(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		// A v1 observation keeps the plain boolean and never gains a phase.
		if stored.Phase != "" || stored.APIVersion != v1.Version {
			t.Fatalf("v1 observation rewritten: %+v", stored)
		}
	}
	list, err := catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if list.APIVersion != v1.Version {
		t.Fatalf("v1 list envelope changed: %q", list.APIVersion)
	}
	for _, endpoint := range list.Items {
		if endpoint.Phase != "" {
			t.Fatalf("v1 list item invented a phase: %+v", endpoint)
		}
	}
	index := NewIndex()
	if err := index.Replace(list); err != nil {
		t.Fatal(err)
	}
	snapshot := index.Snapshot(ready.Service)
	if len(snapshot.Ready) != 1 || snapshot.Ready[0].UID != "ready-uid" {
		t.Fatalf("v1 boolean readiness changed: %+v", snapshot.Ready)
	}
}

func TestV1DeleteEventReplaysUnchanged(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := catalog.Put(testEndpoint("node-a", "pod", "gone"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Delete(endpoint.Service, endpoint.Node, endpoint.Name, endpoint.UID, endpoint.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	// Replay from after the ADDED event so the next event is the DELETE.
	stream, err := catalog.Watch(ctx, endpoint.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != v1.Deleted || event.APIVersion != v1.Version || event.Object.Phase != "" ||
		event.Object.UID != "gone" || event.ResourceVersion != endpoint.ResourceVersion+1 {
		t.Fatalf("delete replay changed: %+v", event)
	}
}

func TestEachUIDAdoptsOnlyNewerObservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	first, err := catalog.Put(phaseEndpoint("node-a", "pod", "uid-a", v1.PhaseServing))
	if err != nil {
		t.Fatal(err)
	}
	// A stale write carrying the old resource version is rejected, so a UID
	// never adopts an older observation.
	ending := phaseEndpoint("node-a", "pod", "uid-a", v1.PhaseTerminating)
	if _, err := catalog.Put(ending); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("stale observation adopted: %v", err)
	}
	// The newer observation with the current version is accepted.
	ending.ResourceVersion = first.ResourceVersion
	updated, err := catalog.Put(ending)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.Terminating() || updated.Serving() {
		t.Fatalf("newer phase not adopted: %+v", updated)
	}
}

func TestValidateCatalogRejectsPhaseInHistoryMismatch(t *testing.T) {
	// A persisted catalog whose current inventory disagrees with the replay
	// phase must be rejected, matching watch-reader expectations.
	doc := catalogDocument{
		APIVersion: v1.Version, Version: 1,
		Items:   map[string]v1.ReplicaObservation{},
		UIDKeys: map[string]string{"uid": "shop/api/node-a/pod"},
		History: []v1.EndpointEvent{},
	}
	serving := phaseEndpoint("node-a", "pod", "uid", v1.PhaseServing)
	serving.ResourceVersion = 1
	serving.Normalized()
	doc.Items[serving.Key()] = serving
	doc.History = append(doc.History, v1.EndpointEvent{APIVersion: v1.VersionV2, Type: v1.Added, ResourceVersion: 1, Object: serving})
	if err := validateCatalog(doc); err != nil {
		t.Fatalf("consistent catalog rejected: %v", err)
	}
	drifted := serving
	drifted.Phase = v1.PhaseTerminating
	drifted.Normalized()
	doc.History[0].Object = drifted
	if err := validateCatalog(doc); !errors.Is(err, v1.ErrInvalid) {
		t.Fatalf("drifted catalog accepted: %v", err)
	}
}
