package discovery

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"wavegate/v1"
)

func testEndpoint(node, name, uid string) v1.ReplicaObservation {
	return v1.ReplicaObservation{
		APIVersion: v1.Version, Service: v1.Service{Namespace: "shop", Name: "api"},
		Node: node, Name: name, UID: uid, Revision: "r1", Address: "http://127.0.0.1:8080", Ready: true,
	}
}

func TestRelistRemovesDeletedEndpointAndPreventsUIDReuse(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoints.v1.json")
	catalog, err := OpenCatalog(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	old, err := catalog.Put(testEndpoint("node-a", "pod", "old"))
	if err != nil {
		t.Fatal(err)
	}
	list, err := catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	index := NewIndex()
	if err := index.Replace(list); err != nil {
		t.Fatal(err)
	}
	index.Disconnect()
	removed, err := catalog.Delete(old.Service, old.Node, old.Name, old.UID, old.ResourceVersion)
	if err != nil || removed.ResourceVersion != 2 {
		t.Fatalf("delete: %+v %v", removed, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := catalog.Put(testEndpoint("node-b", fmt.Sprintf("extra-%d", i), fmt.Sprintf("uid-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := catalog.Watch(ctx, list.ResourceVersion); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("stale watch returned %v", err)
	}
	reopened, err := OpenCatalog(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Put(testEndpoint("node-a", "pod", old.UID)); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("reused retired UID: %v", err)
	}
	replacement, err := reopened.Put(testEndpoint("node-a", "pod", "replacement"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Delete(old.Service, old.Node, old.Name, old.UID, replacement.ResourceVersion); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("stale delete accepted: %v", err)
	}
	relisted, err := reopened.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Replace(relisted); err != nil {
		t.Fatal(err)
	}
	snapshot := index.Snapshot(old.Service)
	if !snapshot.Synced || snapshot.Generation != 1 || len(snapshot.Ready) != 3 || snapshot.Ready[0].UID != replacement.UID {
		t.Fatalf("relist = %+v", snapshot)
	}
}

func TestIndexWatchGapDoesNotMutateWatermark(t *testing.T) {
	index := NewIndex()
	endpoint := testEndpoint("node-a", "pod", "original")
	endpoint.ResourceVersion = 1
	if err := index.Replace(v1.EndpointList{APIVersion: v1.Version, ResourceVersion: 1, Items: []v1.ReplicaObservation{endpoint}}); err != nil {
		t.Fatal(err)
	}
	endpoint.ResourceVersion = 3
	gap := v1.EndpointEvent{APIVersion: v1.Version, Type: v1.Deleted, ResourceVersion: 3, Object: endpoint}
	if err := index.Apply(gap); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("gap accepted: %v", err)
	}
	snapshot := index.Snapshot(endpoint.Service)
	if snapshot.Version != 1 || len(snapshot.Ready) != 1 {
		t.Fatalf("gap mutated index: %+v", snapshot)
	}
}

func BenchmarkWatchRelist(b *testing.B) {
	const count = 6000
	list := v1.EndpointList{APIVersion: v1.Version, ResourceVersion: count, Items: make([]v1.ReplicaObservation, 0, count)}
	for i := 0; i < count; i++ {
		endpoint := testEndpoint(fmt.Sprintf("node-%d", i%80), fmt.Sprintf("pod-%d", i), fmt.Sprintf("uid-%d", i))
		endpoint.Service = v1.Service{Namespace: fmt.Sprintf("team-%d", i%20), Name: fmt.Sprintf("service-%d", i%137)}
		endpoint.ResourceVersion = uint64(i + 1)
		endpoint.Ready = i%4 != 0
		list.Items = append(list.Items, endpoint)
	}
	index := NewIndex()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Disconnect()
		if err := index.Replace(list); err != nil {
			b.Fatal(err)
		}
		snapshot := index.Snapshot(list.Items[1].Service)
		if snapshot.Version != list.ResourceVersion || len(snapshot.Ready) == 0 {
			b.Fatal("invalid relist inventory")
		}
	}
}
