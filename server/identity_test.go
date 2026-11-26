package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/journal"
	"wavegate/traffic"
	"wavegate/v1"
)

func newManualApp(t *testing.T) *App {
	t.Helper()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

func installEndpoints(t *testing.T, app *App, spec v1.ReleaseSpec, suffix string) {
	t.Helper()
	putEndpoint(t, app, spec.Service, spec.Stable, "stable-uid-"+suffix, "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, spec.Service, spec.Candidate, "candidate-uid-"+suffix, "http://127.0.0.1:49161", 0)
	list, err := app.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
}

// reconcileUntil drives the engine until the predicate holds, failing after a
// bounded number of attempts. It mirrors the demo's manual-clock loop.
func reconcileUntil(t *testing.T, app *App, spec v1.ReleaseSpec, want string, ready func(v1.ReleaseState) bool) v1.ReleaseState {
	t.Helper()
	ctx := context.Background()
	var state v1.ReleaseState
	for i := 0; i < 30; i++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatalf("%s reconcile: %v", spec.Key(), err)
		}
		next, found, err := app.Journal.State(spec.UID)
		if err != nil || !found {
			t.Fatalf("%s state: %v", spec.Key(), err)
		}
		state = next
		if ready(state) {
			return state
		}
	}
	t.Fatalf("%s did not reach %s; phase=%s step=%d", spec.Key(), want, state.Status.Phase, state.Status.Step)
	return state
}

func TestNewBindingsCarryCompositeOperationIdentity(t *testing.T) {
	app := newManualApp(t)
	spec := testRelease()
	installEndpoints(t, app, spec, "single")
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	state := reconcileUntil(t, app, spec, "weighted step", func(s v1.ReleaseState) bool {
		return s.Binding != nil && s.Binding.Rule.CandidateWeight > 0 && s.Intent == nil
	})
	binding := state.Binding
	if binding.Identity == nil {
		t.Fatalf("new binding has no composite identity: %+v", binding)
	}
	if binding.Identity.Service != spec.Service || binding.Identity.OwnerUID != spec.UID ||
		binding.Identity.External != binding.Operation {
		t.Fatalf("binding identity inconsistent: %+v", binding.Identity)
	}
	live, found, err := app.Plane.Observe(spec.Service)
	if err != nil || !found || live.Identity == nil || *live.Identity != *binding.Identity {
		t.Fatalf("live binding diverges from decision identity: %+v %v", live, err)
	}
	receipt, err := app.Journal.Receipt(spec.UID, restartTime().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.VerifyReceipt(receipt); err != nil {
		t.Fatalf("v2 receipt does not verify: %v", err)
	}
}

func TestSameExternalFlowOnDifferentServicesDoesNotCrossAcknowledge(t *testing.T) {
	app := newManualApp(t)
	first := testRelease()
	second := testRelease()
	second.Name, second.UID, second.Service.Name = "peer-rollout", "rollout-2", "peer"
	installEndpoints(t, app, first, "first")
	installEndpoints(t, app, second, "second")
	for _, spec := range []v1.ReleaseSpec{first, second} {
		if _, err := app.Specs.Put(spec); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	// Prepare both releases' stable-only intents. The first reconcile only
	// records ReleaseObserved; reservation and intent preparation follow.
	for _, spec := range []v1.ReleaseSpec{first, second} {
		reconcileUntil(t, app, spec, "prepared intent", func(s v1.ReleaseState) bool {
			return s.Intent != nil
		})
	}
	firstState, found, err := app.Journal.State(first.UID)
	if err != nil || !found || firstState.Intent == nil {
		t.Fatalf("first release not prepared: %+v %v", firstState, err)
	}
	secondState, found, err := app.Journal.State(second.UID)
	if err != nil || !found || secondState.Intent == nil {
		t.Fatalf("peer release not prepared: %+v %v", secondState, err)
	}

	// Apply the first release's prepared operation normally.
	firstBinding, err := app.Plane.Apply(ctx, traffic.ApplyRequest{
		OwnerUID: first.UID, Intent: *firstState.Intent, At: restartTime(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// The peer presents the same external flow token inside its own composite
	// identity (its service and owner). It must apply independently and must
	// never be acknowledged as the first service's binding.
	peerIntent := *secondState.Intent
	peerIntent.Operation = firstState.Intent.Operation
	peerIntent.Identity = &v1.OperationIdentity{
		Service: second.Service, OwnerUID: second.UID, External: firstState.Intent.Operation,
	}
	peerBinding, err := app.Plane.Apply(ctx, traffic.ApplyRequest{
		OwnerUID: second.UID, Intent: peerIntent, At: restartTime().Add(time.Second),
	})
	if err != nil {
		t.Fatalf("shared external flow on another service must apply independently: %v", err)
	}
	if peerBinding.ResourceVersion == firstBinding.ResourceVersion ||
		peerBinding.Identity == nil || peerBinding.Identity.Service != second.Service ||
		peerBinding.Operation != firstBinding.Operation {
		t.Fatalf("cross-service apply acknowledged the wrong binding: %+v", peerBinding)
	}

	// Replaying the first release's exact prepared intent acknowledges its own
	// binding exactly once, even though the same token now exists on the peer.
	replay, err := app.Plane.Apply(ctx, traffic.ApplyRequest{
		OwnerUID: first.UID, Intent: *firstState.Intent, At: restartTime().Add(2 * time.Second),
	})
	if err != nil || replay.ResourceVersion != firstBinding.ResourceVersion ||
		!replay.AppliedAt.Equal(firstBinding.AppliedAt) {
		t.Fatalf("same-service replay was not idempotent: %+v %v", replay, err)
	}
}

func TestUpgradeReopenConfirmsOldOperationOnlyOnce(t *testing.T) {
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	spec := testRelease()
	installEndpoints(t, app, spec, "legacy")
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	// Drive to a stable-only binding through the upgraded engine.
	state := reconcileUntil(t, app, spec, "stable binding", func(s v1.ReleaseState) bool {
		return s.Binding != nil && s.Intent == nil
	})
	originalVersion := state.Binding.ResourceVersion

	// Stage a genuine pre-upgrade state: rewrite both the traffic binding and
	// the journal decision that references it as legacy v1 records (bare
	// operation tokens, no composite identity), recomputing all digests so the
	// documents remain self-consistent as an old binary would have written them.
	live, found, err := app.Plane.Observe(spec.Service)
	if err != nil || !found {
		t.Fatalf("stable binding missing: %v", err)
	}
	legacyOperation := live.Operation
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stageLegacyState(cfg.StateDirectory, spec.Service, live); err != nil {
		t.Fatal(err)
	}

	// Reopen with the upgraded controller: it must accept the legacy binding,
	// reconcile against it, and never re-apply or reject it. The in-memory
	// discovery index starts empty in a new process, so rehydrate it from the
	// durable catalog exactly as a real restart does.
	clk := clock.NewManual(restartTime().Add(time.Second))
	reopened, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatalf("reopen over legacy binding failed: %v", err)
	}
	defer reopened.Close()
	list, err := reopened.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	observed, found, err := reopened.Plane.Observe(spec.Service)
	if err != nil || !found || observed.Identity != nil || observed.Operation != legacyOperation {
		t.Fatalf("legacy binding altered on reopen: %+v %v", observed, err)
	}
	ctx := context.Background()
	// A first reconcile with the discovery view rehydrated must leave the
	// legacy stable-only binding exactly in place: it cannot re-apply the old
	// operation or mint a new binding while the phase is still Waiting.
	if err := reopened.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatalf("reconcile over legacy binding: %v", err)
	}
	stillLegacy, _, err := reopened.Plane.Observe(spec.Service)
	if err != nil {
		t.Fatal(err)
	}
	if stillLegacy.ResourceVersion != observed.ResourceVersion || stillLegacy.Identity != nil ||
		stillLegacy.Operation != legacyOperation {
		t.Fatalf("reopen re-applied the legacy operation: %+v", stillLegacy)
	}

	// With candidate endpoints ready, normal progression installs the weighted
	// step as a fresh operation carrying a v2 composite identity.
	clk.Advance(time.Second)
	upgradedState := reconcileUntil(t, reopened, spec, "weighted step", func(s v1.ReleaseState) bool {
		return s.Binding != nil && s.Binding.Rule.CandidateWeight > 0 && s.Intent == nil
	})
	upgraded := upgradedState.Binding
	if upgraded.Identity == nil || upgraded.Identity.Service != spec.Service ||
		upgraded.Identity.OwnerUID != spec.UID || upgraded.Identity.External != upgraded.Operation {
		t.Fatalf("post-upgrade binding lacks a valid composite identity: %+v", upgraded)
	}
	if upgraded.ResourceVersion == originalVersion {
		t.Fatalf("post-upgrade operation reused the legacy binding version")
	}

	// The receipt spans both representations and still verifies, and its
	// decisions contain evidence of each form.
	receipt, err := reopened.Journal.Receipt(spec.UID, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.VerifyReceipt(receipt); err != nil {
		t.Fatalf("receipt spanning both representations failed: %v", err)
	}
	var sawLegacy, sawComposite bool
	for _, record := range receipt.Decisions {
		if record.State.Binding == nil {
			continue
		}
		if record.State.Binding.Identity == nil && record.State.Binding.Operation == legacyOperation {
			sawLegacy = true
		}
		if record.State.Binding.Identity != nil {
			sawComposite = true
		}
	}
	if !sawLegacy || !sawComposite {
		t.Fatalf("receipt does not explain both representations: legacy=%v composite=%v", sawLegacy, sawComposite)
	}
}

// stageLegacyState rewrites a state directory so it looks exactly like data an
// older binary produced: the traffic binding and the journal records that
// reference it carry a bare operation token with no composite identity. The
// token itself is preserved because the prepared intent and the applied
// binding must share it under the v1 rule. Every digest is recomputed so the
// decision chain and checkpoint stay verifiable.
func stageLegacyState(stateDir string, service v1.Service, binding v1.TrafficBinding) error {
	legacyBinding := binding
	legacyBinding.Identity = nil

	trafficPath := filepath.Join(stateDir, "traffic.v1.json")
	trafficDoc := struct {
		APIVersion string                       `json:"apiVersion"`
		Version    uint64                       `json:"resourceVersion"`
		Bindings   map[string]v1.TrafficBinding `json:"bindings"`
	}{
		APIVersion: v1.Version, Version: legacyBinding.ResourceVersion,
		Bindings: map[string]v1.TrafficBinding{service.String(): legacyBinding},
	}
	if err := writeIndented(trafficPath, trafficDoc); err != nil {
		return err
	}

	decisionsPath := filepath.Join(stateDir, "decisions.v1.json")
	raw, err := os.ReadFile(decisionsPath)
	if err != nil {
		return err
	}
	var doc struct {
		APIVersion string              `json:"apiVersion"`
		Checkpoint json.RawMessage     `json:"checkpoint"`
		Records    []v1.DecisionRecord `json:"records"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	for index := range doc.Records {
		record := &doc.Records[index]
		// An old binary writes neither composite intents nor composite bindings.
		if record.State.Intent != nil {
			record.State.Intent.Identity = nil
		}
		if record.State.Binding != nil && record.State.Binding.ResourceVersion == binding.ResourceVersion {
			record.State.Binding.Identity = nil
		}
	}
	// Recompute the chain: each record's digest covers its PreviousDigest, so
	// relink first, then digest in sequence order.
	var previousDigest string
	for index := range doc.Records {
		doc.Records[index].PreviousDigest = previousDigest
		doc.Records[index].Digest = ""
		digest, err := v1.Digest(doc.Records[index])
		if err != nil {
			return err
		}
		doc.Records[index].Digest = digest
		previousDigest = digest
	}
	return writeIndented(decisionsPath, doc)
}

func writeIndented(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
