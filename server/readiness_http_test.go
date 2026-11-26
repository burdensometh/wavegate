package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"wavegate/discovery"
	"wavegate/v1"
)

func putEndpointHTTP(t *testing.T, d *daemon, endpoint v1.ReplicaObservation) *http.Response {
	t.Helper()
	data, err := json.Marshal(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPut, d.origin+"/v1/endpoints", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := d.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestFakeAPIRejectsTerminatingOverwriteOfNewerUID(t *testing.T) {
	d := startDaemon(t)
	spec := testRelease()
	upstream := backend(t, simple)

	newer := v1.ReplicaObservation{
		APIVersion: v1.VersionV2, Service: spec.Service, Node: "node-a", Name: "stable",
		Revision: spec.Stable, UID: "newer-uid", Address: upstream.URL, Phase: v1.PhaseServing,
		Ready: true,
	}
	if response := putEndpointHTTP(t, d, newer); response.StatusCode != http.StatusOK {
		t.Fatalf("create serving endpoint: %d", response.StatusCode)
	}
	older := v1.ReplicaObservation{
		APIVersion: v1.VersionV2, Service: spec.Service, Node: "node-a", Name: "stable",
		Revision: spec.Stable, UID: "older-uid", Address: upstream.URL, Phase: v1.PhaseTerminating,
		Ready: false, ResourceVersion: 1,
	}
	if response := putEndpointHTTP(t, d, older); response.StatusCode != http.StatusConflict {
		t.Fatalf("terminating overwrite accepted: %d", response.StatusCode)
	}
	// The newer UID is still the one the fake API lists.
	list, err := d.app.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].UID != "newer-uid" || !list.Items[0].Serving() {
		t.Fatalf("inventory after rejected overwrite = %+v", list.Items)
	}
}

func TestRestartInterpretationConsistentAcrossSurfaces(t *testing.T) {
	d := startDaemon(t)
	spec := testRelease()
	upstream := backend(t, simple)

	// A terminating stable endpoint and an unknown candidate endpoint.
	stable := v1.ReplicaObservation{
		APIVersion: v1.VersionV2, Service: spec.Service, Node: "node-a", Name: "stable",
		Revision: spec.Stable, UID: "stable-uid", Address: upstream.URL, Phase: v1.PhaseTerminating,
		Ready: false,
	}
	candidate := v1.ReplicaObservation{
		APIVersion: v1.VersionV2, Service: spec.Service, Node: "node-a", Name: "candidate",
		Revision: spec.Candidate, UID: "candidate-uid", Address: upstream.URL, Phase: v1.PhaseUnknown,
		Ready: false,
	}
	if response := putEndpointHTTP(t, d, stable); response.StatusCode != http.StatusOK {
		t.Fatalf("create terminating: %d", response.StatusCode)
	}
	if response := putEndpointHTTP(t, d, candidate); response.StatusCode != http.StatusOK {
		t.Fatalf("create unknown: %d", response.StatusCode)
	}
	// Directory view: the persisted catalog keeps the phases.
	persisted, err := d.app.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	phases := map[string]string{}
	for _, endpoint := range persisted.Items {
		phases[endpoint.UID] = endpoint.Phase
	}
	if phases["stable-uid"] != v1.PhaseTerminating || phases["candidate-uid"] != v1.PhaseUnknown {
		t.Fatalf("persisted phases = %+v", phases)
	}
	// Watch reader view: replay yields the same phases.
	stream, err := d.app.Catalog.Watch(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	replayed := map[string]string{}
	for i := 0; i < len(persisted.Items); i++ {
		event, err := stream.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		replayed[event.Object.UID] = event.Object.Phase
	}
	if replayed["stable-uid"] != v1.PhaseTerminating || replayed["candidate-uid"] != v1.PhaseUnknown {
		t.Fatalf("replayed phases = %+v", replayed)
	}
	// Routing view: neither non-serving endpoint is in the ready snapshot.
	if snapshot := d.app.Index.Snapshot(spec.Service); len(snapshot.Ready) != 0 {
		t.Fatalf("non-serving endpoints projected as ready: %+v", snapshot.Ready)
	}
	// Reopen the catalog from disk and confirm the same interpretation.
	reopened, err := discovery.OpenCatalog(filepath.Join(d.app.cfg.StateDirectory, "endpoints.v1.json"), d.app.cfg.EndpointHistory)
	if err != nil {
		t.Fatal(err)
	}
	list, err := reopened.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range list.Items {
		if endpoint.Serving() {
			t.Fatalf("endpoint served after restart: %+v", endpoint)
		}
	}
}
