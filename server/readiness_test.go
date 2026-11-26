package server

import (
	"errors"
	"net/http"
	"testing"

	"wavegate/v1"
)

func putPhase(t *testing.T, app *App, service v1.Service, node, name, revision, uid, phase, address string) (v1.ReplicaObservation, error) {
	t.Helper()
	return putPhaseVersion(t, app, service, node, name, revision, uid, phase, address, 0)
}

func putPhaseVersion(t *testing.T, app *App, service v1.Service, node, name, revision, uid, phase, address string, version uint64) (v1.ReplicaObservation, error) {
	t.Helper()
	endpoint := v1.ReplicaObservation{
		APIVersion: v1.VersionV2, Service: service, Node: node, Name: name,
		Revision: revision, UID: uid, Address: address, Phase: phase,
		Ready: phase == v1.PhaseServing, ResourceVersion: version,
	}
	return app.Catalog.Put(endpoint)
}

func TestTerminatingEndpointStopsNewTrafficAndProtectsNewerUID(t *testing.T) {
	upstream := backend(t, simple)
	d := startDaemon(t)
	spec := testRelease()

	serving, err := putPhase(t, d.app, spec.Service, "node-a", "stable", spec.Stable, "stable-uid", v1.PhaseServing, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	startRelease(t, d, spec)
	waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool { return len(binding.Rule.StableTargets) == 1 })

	// While the live endpoint is serving, a terminating observation for an
	// older UID at the same key must not displace it.
	stale := v1.ReplicaObservation{
		APIVersion: v1.VersionV2, Service: spec.Service, Node: "node-a", Name: "stable",
		Revision: spec.Stable, UID: "older-uid", Address: upstream.URL, Phase: v1.PhaseTerminating,
		ResourceVersion: serving.ResourceVersion,
	}
	if _, err := d.app.Catalog.Put(stale); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("terminating stale UID overwrote live endpoint: %v", err)
	}

	// The stable endpoint turns terminating. New requests must fail closed
	// until the binding is re-rendered; the terminating node never serves.
	ending := serving.Normalized()
	ending.Phase = v1.PhaseTerminating
	ending.Ready = false
	stored, err := d.app.Catalog.Put(ending)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		header, code, err := d.request(spec, "/during-termination")
		if err != nil {
			t.Fatal(err)
		}
		if header.Get("X-Wavegate-Endpoint") == "stable-uid" {
			t.Fatalf("terminating endpoint served a new request: %d", code)
		}
		if code != http.StatusServiceUnavailable {
			t.Fatalf("terminating endpoint returned %d", code)
		}
	}

	// A fresh serving observation at a newer UID replaces the terminating one.
	replacement, err := putPhaseVersion(t, d.app, spec.Service, "node-a", "stable", spec.Stable, "replacement-uid", v1.PhaseServing, upstream.URL, stored.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.UID != "replacement-uid" {
		t.Fatalf("replacement not stored: %+v", replacement)
	}
}
