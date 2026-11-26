package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/fault"
	"wavegate/traffic"
	"wavegate/v1"
)

func syncIndex(t *testing.T, app *App) {
	t.Helper()
	list, err := app.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
}

func reconcileUntil(t *testing.T, app *App, spec v1.ReleaseSpec, check func(v1.ReleaseState) bool) v1.ReleaseState {
	t.Helper()
	var state v1.ReleaseState
	for i := 0; i < 30; i++ {
		if err := app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		next, exists, err := app.Journal.State(spec.UID)
		if err != nil || !exists {
			t.Fatalf("state: %v %v", next, err)
		}
		state = next
		if check(state) {
			return state
		}
	}
	t.Fatalf("release never reached the expected state: phase=%s step=%d", state.Status.Phase, state.Status.Step)
	return state
}

func catalogItem(t *testing.T, app *App, service v1.Service, node, name string) v1.ReplicaObservation {
	t.Helper()
	list, err := app.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range list.Items {
		if endpoint.Service == service && endpoint.Node == node && endpoint.Name == name {
			return endpoint
		}
	}
	t.Fatalf("endpoint %s/%s/%s not found in catalog", service, node, name)
	return v1.ReplicaObservation{}
}

func putAndSync(t *testing.T, app *App, service v1.Service, revision, uid, address string, version uint64) v1.ReplicaObservation {
	t.Helper()
	observed := putEndpoint(t, app, service, revision, uid, address, version)
	syncIndex(t, app)
	return observed
}

func pausedApp(t *testing.T) (*App, *clock.Manual, v1.ReleaseSpec, string, string) {
	t.Helper()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	clk := clock.NewManual(restartTime())
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	upstream := httptest.NewServer(http.HandlerFunc(simple))
	t.Cleanup(upstream.Close)
	spec := testRelease()
	putAndSync(t, app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	putAndSync(t, app, spec.Service, spec.Candidate, "candidate-uid-1", upstream.URL, 0)
	view, err := app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec = view.Spec
	reconcileUntil(t, app, spec, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Progressing && state.Status.Step == 0 &&
			state.Binding != nil && state.Binding.Rule.CandidateWeight == spec.Steps[0]
	})
	spec.Paused = true
	view, err = app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	paused := view.Spec
	reconcileUntil(t, app, paused, func(state v1.ReleaseState) bool { return state.Status.Phase == v1.Paused })
	return app, clk, paused, "candidate-uid-1", upstream.URL
}

// A paused release must rebind to a recreated candidate replica without
// changing its step or weight, and must not open a promotion window.
func TestPausedReleaseRebindsRecreatedCandidate(t *testing.T) {
	app, clk, spec, oldUID, upstream := pausedApp(t)
	before, _, err := app.Plane.Observe(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	if before.Rule.CandidateTargets[0].UID != oldUID {
		t.Fatalf("test setup missed the original candidate UID: %+v", before.Rule)
	}

	old := catalogItem(t, app, spec.Service, "node-a", spec.Candidate)
	if old.UID != oldUID {
		t.Fatalf("catalog returned unexpected candidate UID %s", old.UID)
	}
	if _, err := app.Catalog.Delete(spec.Service, old.Node, old.Name, oldUID, old.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	replacement := putAndSync(t, app, spec.Service, spec.Candidate, "candidate-uid-2", upstream, 0)

	state := reconcileUntil(t, app, spec, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Paused && state.Intent == nil &&
			state.Binding != nil && state.Binding.ResourceVersion > before.ResourceVersion
	})
	binding := state.Binding
	if binding.Rule.CandidateWeight != spec.Steps[0] {
		t.Fatalf("paused refresh changed the weight from %d to %d", spec.Steps[0], binding.Rule.CandidateWeight)
	}
	if state.Status.Step != 0 {
		t.Fatalf("paused refresh changed the step from 0 to %d", state.Status.Step)
	}
	if state.Window != nil || state.Status.Verdict != nil {
		t.Fatalf("paused refresh opened a promotion window or verdict: %+v", state)
	}
	if len(binding.Rule.CandidateTargets) != 1 || binding.Rule.CandidateTargets[0].UID != replacement.UID {
		t.Fatalf("paused binding does not target the recreated replica: %+v", binding.Rule.CandidateTargets)
	}
	for _, target := range binding.Rule.CandidateTargets {
		if target.UID == oldUID {
			t.Fatalf("retired UID %s remains in the refreshed rule", oldUID)
		}
	}

	// Advancing time past a full window must not produce any verdict while paused.
	clk.Advance(spec.Window.Duration() + time.Second)
	if err := app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	state, _, err = app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Window != nil || state.Status.Verdict != nil || state.Status.Phase != v1.Paused {
		t.Fatalf("pause opened an observation window over time: %+v", state.Status)
	}

	// Resuming starts a fresh window at the same step and weight.
	resumed := spec
	resumed.Paused = false
	view, err := app.Specs.Put(resumed)
	if err != nil {
		t.Fatal(err)
	}
	resumed = view.Spec
	resumedState := reconcileUntil(t, app, resumed, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Progressing && state.Window != nil && state.Intent == nil
	})
	if resumedState.Status.Step != 0 || resumedState.Binding.Rule.CandidateWeight != spec.Steps[0] {
		t.Fatalf("resume changed step/weight: step=%d weight=%d", resumedState.Status.Step,
			resumedState.Binding.Rule.CandidateWeight)
	}
	if resumedState.Binding.Rule.CandidateTargets[0].UID != replacement.UID {
		t.Fatalf("resume did not keep the recreated candidate target")
	}
}

// Repeated reconciles with an unchanged inventory must not churn the binding.
// Requests routed during the pause must reach the recreated candidate and
// never the retired UID, while the weight split stays at the committed step.
func TestPausedRefreshRoutesRequestsToRecreatedCandidate(t *testing.T) {
	upstream := backend(t, simple)
	d := startDaemon(t)
	spec := testRelease()
	putEndpoint(t, d.app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	putEndpoint(t, d.app, spec.Service, spec.Candidate, "candidate-uid-1", upstream.URL, 0)
	view, err := d.app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec = view.Spec
	waitBinding(t, d, spec, func(b v1.TrafficBinding) bool { return b.Rule.CandidateWeight == spec.Steps[0] })

	spec.Paused = true
	view, err = d.app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec = view.Spec
	eventual(t, "paused phase", func() bool {
		state, ok, err := d.app.Journal.State(spec.UID)
		return err == nil && ok && state.Status.Phase == v1.Paused
	})

	old := catalogItem(t, d.app, spec.Service, "node-a", spec.Candidate)
	// Add the replacement on a distinct node before retiring the old replica
	// so the live watch never observes an inventory below MinReady mid-swap.
	added, err := d.app.Catalog.Put(v1.ReplicaObservation{
		APIVersion: v1.Version, Service: spec.Service, Revision: spec.Candidate, UID: "candidate-uid-2",
		Address: upstream.URL, Node: "node-b", Name: spec.Candidate, Ready: true, ResourceVersion: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitBinding(t, d, spec, func(b v1.TrafficBinding) bool {
		for _, target := range b.Rule.CandidateTargets {
			if target.UID == added.UID {
				return true
			}
		}
		return false
	})
	if _, err := d.app.Catalog.Delete(spec.Service, old.Node, old.Name, "candidate-uid-1", old.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	refreshed := waitBinding(t, d, spec, func(b v1.TrafficBinding) bool {
		return b.Rule.CandidateWeight == spec.Steps[0] && len(b.Rule.CandidateTargets) == 1 &&
			b.Rule.CandidateTargets[0].UID == "candidate-uid-2"
	})
	state, _, err := d.app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Window != nil || state.Status.Verdict != nil || state.Status.Step != 0 {
		t.Fatalf("paused refresh altered invariants: %+v", state)
	}
	if refreshed.Rule.CandidateWeight != spec.Steps[0] {
		t.Fatalf("paused routing weight changed: %d", refreshed.Rule.CandidateWeight)
	}

	endpoints := map[string]int{}
	var failures int
	for i := 0; i < 40; i++ {
		header, code, err := d.request(spec, "/paused-traffic")
		if err != nil {
			t.Fatal(err)
		}
		if code == http.StatusServiceUnavailable {
			failures++
			continue
		}
		if code != http.StatusOK {
			t.Fatalf("paused request failed with %d", code)
		}
		uid := header.Get("X-Wavegate-Endpoint")
		if uid == "candidate-uid-1" {
			t.Fatalf("retired candidate UID served traffic during the pause")
		}
		endpoints[uid]++
	}
	if failures != 0 {
		t.Fatalf("recreated candidate caused %d fail-closed requests during the pause", failures)
	}
	if endpoints["candidate-uid-2"] == 0 {
		t.Fatalf("no request reached the recreated candidate: %+v", endpoints)
	}
	state, _, err = d.app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Window != nil || state.Status.Verdict != nil {
		t.Fatalf("serving traffic during the pause opened a window: %+v", state)
	}
}

func TestPausedReleaseHoldsBindingWhenEndpointsUnchanged(t *testing.T) {
	app, _, spec, oldUID, _ := pausedApp(t)
	before, _, err := app.Plane.Observe(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); err != nil {
			t.Fatal(err)
		}
	}
	live, _, err := app.Plane.Observe(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	if live.ResourceVersion != before.ResourceVersion || live.Operation != before.Operation {
		t.Fatalf("idle paused release churned its binding: %d -> %d", before.ResourceVersion, live.ResourceVersion)
	}
	if live.Rule.CandidateTargets[0].UID != oldUID {
		t.Fatalf("idle paused refresh replaced the candidate UID")
	}
	state, _, err := app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent != nil || state.Window != nil {
		t.Fatalf("idle pause prepared an intent or window: %+v", state)
	}
}

// A paused release whose candidate drops below MinReady must fail closed and
// roll back rather than keep serving stale candidate UIDs.
func TestPausedReleaseBelowMinimumRollsBack(t *testing.T) {
	app, _, spec, oldUID, _ := pausedApp(t)
	old := catalogItem(t, app, spec.Service, "node-a", spec.Candidate)
	if _, err := app.Catalog.Delete(spec.Service, old.Node, old.Name, oldUID, old.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	syncIndex(t, app)
	state := reconcileUntil(t, app, spec, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Aborted
	})
	if state.Binding == nil || state.Binding.Rule.CandidateWeight != 0 || state.Reservation != nil {
		t.Fatalf("paused release did not release candidate traffic and quota: %+v", state)
	}
}

// If the refresh applies durably but its receipt is lost, the restarted
// controller must recognize the live binding and complete it once.
func TestPausedRefreshReceiptLossReconcilesOnce(t *testing.T) {
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	var dropped *fault.Plane
	app, err := Open(cfg, Options{
		Clock:      clock.NewManual(restartTime()),
		WrapPlane: func(plane traffic.Plane) traffic.Plane {
			dropped = &fault.Plane{Plane: plane}
			return dropped
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(simple))
	t.Cleanup(upstream.Close)
	spec := testRelease()
	putAndSync(t, app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	putAndSync(t, app, spec.Service, spec.Candidate, "candidate-uid-1", upstream.URL, 0)
	view, err := app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec = view.Spec
	reconcileUntil(t, app, spec, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Progressing && state.Status.Step == 0
	})
	spec.Paused = true
	view, err = app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	paused := view.Spec
	reconcileUntil(t, app, paused, func(state v1.ReleaseState) bool { return state.Status.Phase == v1.Paused })

	old := catalogItem(t, app, spec.Service, "node-a", spec.Candidate)
	if _, err := app.Catalog.Delete(spec.Service, old.Node, old.Name, "candidate-uid-1", old.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	putAndSync(t, app, spec.Service, spec.Candidate, "candidate-uid-2", upstream.URL, 0)

	prepared := reconcileUntil(t, app, paused, func(state v1.ReleaseState) bool {
		return state.Intent != nil && state.Intent.Kind == v1.RefreshPaused
	})
	dropped.DropNextReceipt()
	err = app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name)
	if !errors.Is(err, fault.ErrLostReceipt) {
		t.Fatalf("expected a lost receipt, got %v", err)
	}
	live, exists, err := app.Plane.Observe(spec.Service)
	if err != nil || !exists {
		t.Fatalf("live binding missing after unacknowledged apply: %v", err)
	}
	if live.Operation != prepared.Intent.Operation {
		t.Fatalf("live operation %s does not match prepared %s", live.Operation, prepared.Intent.Operation)
	}

	// Reconcile again without a restart: the committed binding is recognized.
	if err := app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	state, _, err := app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent != nil || state.Binding == nil ||
		state.Binding.ResourceVersion != live.ResourceVersion || state.Binding.Operation != live.Operation {
		t.Fatalf("receipt recovery did not confirm the prepared binding: %+v", state)
	}
	if state.Status.Phase != v1.Paused || state.Window != nil || state.Status.Step != 0 {
		t.Fatalf("recovery altered pause invariants: phase=%s step=%d window=%+v",
			state.Status.Phase, state.Status.Step, state.Window)
	}

	// A further reconcile must not prepare or apply another switch.
	if err := app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	again, _, err := app.Plane.Observe(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResourceVersion != live.ResourceVersion || again.Operation != live.Operation {
		t.Fatalf("recovery switched the binding a second time: %d -> %d", live.ResourceVersion, again.ResourceVersion)
	}
	state, _, err = app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent != nil {
		t.Fatalf("duplicate refresh intent prepared after recovery: %+v", state.Intent)
	}
}

// A full process restart with an unacknowledged paused refresh must confirm
// the new binding from the traffic plane rather than re-applying it.
func TestPausedRefreshSurvivesRestart(t *testing.T) {
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	stateDir := cfg.StateDirectory
	var dropped *fault.Plane
	app, err := Open(cfg, Options{
		Clock:      clock.NewManual(restartTime()),
		WrapPlane: func(plane traffic.Plane) traffic.Plane {
			dropped = &fault.Plane{Plane: plane}
			return dropped
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(simple))
	t.Cleanup(upstream.Close)
	spec := testRelease()
	putAndSync(t, app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	putAndSync(t, app, spec.Service, spec.Candidate, "candidate-uid-1", upstream.URL, 0)
	view, err := app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec = view.Spec
	reconcileUntil(t, app, spec, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Progressing && state.Status.Step == 0
	})
	spec.Paused = true
	view, err = app.Specs.Put(spec)
	if err != nil {
		t.Fatal(err)
	}
	paused := view.Spec
	reconcileUntil(t, app, paused, func(state v1.ReleaseState) bool { return state.Status.Phase == v1.Paused })

	old := catalogItem(t, app, spec.Service, "node-a", spec.Candidate)
	if _, err := app.Catalog.Delete(spec.Service, old.Node, old.Name, "candidate-uid-1", old.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	putAndSync(t, app, spec.Service, spec.Candidate, "candidate-uid-2", upstream.URL, 0)
	prepared := reconcileUntil(t, app, paused, func(state v1.ReleaseState) bool {
		return state.Intent != nil && state.Intent.Kind == v1.RefreshPaused
	})
	dropped.DropNextReceipt()
	if err := app.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); !errors.Is(err, fault.ErrLostReceipt) {
		t.Fatalf("expected a lost receipt, got %v", err)
	}
	live, _, err := app.Plane.Observe(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}

	reopenCfg := config.Default()
	reopenCfg.StateDirectory = stateDir
	reopenCfg.DefaultQuota = cfg.DefaultQuota
	reopenCfg.Quotas = cfg.Quotas
	reopened, err := Open(reopenCfg, Options{
		Clock: clock.NewManual(restartTime().Add(time.Second)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	syncIndex(t, reopened)
	pending := reopened.Recovery.Pending
	if len(pending) != 1 || !pending[0].Applied || pending[0].Kind != v1.RefreshPaused {
		t.Fatalf("recovery inspection missed the paused refresh: %+v", pending)
	}
	if err := reopened.Engine.Reconcile(context.Background(), spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	state, _, err := reopened.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent != nil || state.Binding == nil ||
		state.Binding.Operation != prepared.Intent.Operation || state.Binding.ResourceVersion != live.ResourceVersion {
		t.Fatalf("restart did not confirm the original binding: %+v", state)
	}
	if state.Status.Phase != v1.Paused || state.Window != nil || state.Status.Step != 0 ||
		state.Binding.Rule.CandidateWeight != spec.Steps[0] {
		t.Fatalf("restart broke the paused step/weight/window invariants: %+v", state)
	}
	if got := state.Binding.Rule.CandidateTargets; len(got) != 1 || got[0].UID != "candidate-uid-2" {
		t.Fatalf("restart binding lost the recreated candidate: %+v", got)
	}
}
