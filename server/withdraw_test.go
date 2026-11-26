package server

import (
	"context"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/journal"
	"wavegate/v1"
)

// oneSided policy: the candidate side receives no samples before the deadline.
func oneSidedRelease() v1.ReleaseSpec {
	spec := testRelease()
	spec.UID = "rollout-withdraw"
	spec.Name = "withdraw-rollout"
	spec.Window = v1.WindowPolicy{Seconds: 2, GraceSeconds: 5, MinSamples: 2, MaxErrorBPS: 1000, MaxErrorDeltaBPS: 1000, MaxMeanLatencyDeltaMS: 1000}
	return spec
}

// A candidate window that never gets its candidate side is withdrawn safely at
// the bounded deadline: the reservation is released and the release aborts with
// the insufficiency reason instead of an unqualified metric failure.
func TestOneSidedWindowIsWithdrawnAfterGrace(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	clk := clock.NewManual(restartTime())
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	spec := oneSidedRelease()
	putEndpoint(t, app, spec.Service, spec.Stable, "stable-uid", "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, spec.Service, spec.Candidate, "candidate-uid", "http://127.0.0.1:49161", 0)
	syncIndex(t, app)
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	// Drive to the first weighted step.
	state := reconcileUntil(t, ctx, app, spec, func(s v1.ReleaseState) bool {
		return s.Status.Phase == v1.Progressing && s.Window != nil && s.Intent == nil
	})
	window := *state.Window
	// Only the stable side reports; the candidate side stays empty.
	stableOnly := []v1.Sample{
		windowSample(window, "s0", v1.StableRole, window.Start),
		windowSample(window, "s1", v1.StableRole, window.Start.Add(time.Second)),
	}
	for _, sample := range stableOnly {
		if added, err := app.Metrics.Add(sample); err != nil || !added {
			t.Fatalf("add %s: %v %v", sample.ID, err, added)
		}
	}
	// Just past the window but inside the grace: still pending, safe.
	clk.Advance(window.End.Sub(clk.Now()) + time.Second)
	if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	mid, _, err := app.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if mid.Status.Verdict != nil || mid.Status.Phase != v1.Progressing {
		t.Fatalf("withdrew before the deadline: %+v", mid)
	}
	if !condition(mid, "WindowReady", "AwaitingLateSamples") {
		t.Fatalf("grace state not recorded: %+v", mid.Status.Conditions)
	}
	// Reach the bounded deadline without the candidate side.
	deadline := window.End.Add(spec.Window.Grace())
	clk.Advance(deadline.Sub(clk.Now()))
	state = reconcileUntil(t, ctx, app, spec, func(s v1.ReleaseState) bool {
		return v1.Terminal(s.Status.Phase)
	})
	if state.Status.Phase != v1.Aborted {
		t.Fatalf("one-sided window did not withdraw: %+v", state)
	}
	if state.Status.Verdict == nil || state.Status.Verdict.Result != v1.Withdrawn ||
		state.Status.Verdict.Reason != "InsufficientSamples" {
		t.Fatalf("withdrawal verdict = %+v", state.Status.Verdict)
	}
	if state.Reservation != nil {
		t.Fatalf("withdrawal did not release the reservation: %+v", state.Reservation)
	}
	if usage := app.Engine.Usage(spec.Service.Namespace); usage.Releases != 0 {
		t.Fatalf("quota still reserved after withdrawal: %+v", usage)
	}
	// The receipt distinguishes withdrawal from a metric failure.
	receipt, err := app.Journal.Receipt(spec.UID, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.VerifyReceipt(receipt); err != nil {
		t.Fatalf("receipt does not verify: %v", err)
	}
	last := receipt.Decisions[len(receipt.Decisions)-1]
	if last.Kind != "TrafficCommitted" || last.Reason != v1.RollBack || last.State.Status.Phase != v1.Aborted {
		t.Fatalf("unexpected terminal decision: %+v", last)
	}
	if !receiptHasReason(receipt, "InsufficientSamples") {
		t.Fatalf("receipt lacks the insufficiency reason: %+v", receipt.Decisions)
	}
}

// Late samples that carry an in-window timestamp and complete the pair decide
// the window by the original thresholds, even though they arrive after end.
func TestLateSampleCompletesPairBeforeDeadline(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	clk := clock.NewManual(restartTime())
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	spec := oneSidedRelease()
	putEndpoint(t, app, spec.Service, spec.Stable, "stable-uid", "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, spec.Service, spec.Candidate, "candidate-uid", "http://127.0.0.1:49161", 0)
	syncIndex(t, app)
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	state := reconcileUntil(t, ctx, app, spec, func(s v1.ReleaseState) bool {
		return s.Status.Phase == v1.Progressing && s.Window != nil && s.Intent == nil
	})
	window := *state.Window
	clk.Advance(window.End.Sub(clk.Now()) + time.Second)
	// The candidate samples arrive late but were observed inside the window.
	for _, sample := range []v1.Sample{
		windowSample(window, "s0", v1.StableRole, window.Start),
		windowSample(window, "s1", v1.StableRole, window.Start.Add(time.Second)),
		windowSample(window, "c0", v1.CandidateRole, window.Start.Add(1500*time.Millisecond)),
		windowSample(window, "c1", v1.CandidateRole, window.End.Add(-time.Second)),
	} {
		if added, err := app.Metrics.Add(sample); err != nil || !added {
			t.Fatalf("add %s: %v %v", sample.ID, err, added)
		}
	}
	state = reconcileUntil(t, ctx, app, spec, func(s v1.ReleaseState) bool {
		return s.Status.Phase == v1.Promoted
	})
	if state.Status.Verdict.Result != v1.Pass {
		t.Fatalf("late pair must pass by the original thresholds: %+v", state.Status.Verdict)
	}
}

// After a withdrawal the terminal state is durable: reopening the same state
// directory preserves the Aborted phase and the insufficiency reason, and the
// retried reconcile does not resurrect the candidate.
func TestWithdrawnReleaseSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StateDirectory = dir
	clk := clock.NewManual(restartTime())
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	spec := oneSidedRelease()
	putEndpoint(t, app, spec.Service, spec.Stable, "stable-uid", "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, spec.Service, spec.Candidate, "candidate-uid", "http://127.0.0.1:49161", 0)
	syncIndex(t, app)
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	state := reconcileUntil(t, ctx, app, spec, func(s v1.ReleaseState) bool {
		return s.Status.Phase == v1.Progressing && s.Window != nil && s.Intent == nil
	})
	window := *state.Window
	for _, sample := range []v1.Sample{
		windowSample(window, "s0", v1.StableRole, window.Start),
		windowSample(window, "s1", v1.StableRole, window.Start.Add(time.Second)),
	} {
		if _, err := app.Metrics.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
	deadline := window.End.Add(spec.Window.Grace())
	clk.Advance(deadline.Sub(clk.Now()))
	reconcileUntil(t, ctx, app, spec, func(s v1.ReleaseState) bool { return v1.Terminal(s.Status.Phase) })
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg, Options{Clock: clock.NewManual(deadline.Add(time.Hour))})
	if err != nil {
		t.Fatalf("reopen after withdrawal: %v", err)
	}
	defer reopened.Close()
	if len(reopened.Recovery.Pending) != 0 {
		t.Fatalf("withdrawal left a pending apply: %+v", reopened.Recovery.Pending)
	}
	before, _, err := reopened.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	after, _, err := reopened.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status.Phase != v1.Aborted || after.Status.Verdict == nil ||
		after.Status.Verdict.Result != v1.Withdrawn || after.Status.Verdict.Reason != "InsufficientSamples" {
		t.Fatalf("restart lost the withdrawal: %+v", after)
	}
	if after.Status.DecisionSequence != before.Status.DecisionSequence {
		t.Fatalf("retrying a terminal withdrawal wrote new decisions: before=%d after=%d",
			before.Status.DecisionSequence, after.Status.DecisionSequence)
	}
}

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

func reconcileUntil(t *testing.T, ctx context.Context, app *App, spec v1.ReleaseSpec, done func(v1.ReleaseState) bool) v1.ReleaseState {
	t.Helper()
	var state v1.ReleaseState
	for i := 0; i < 20; i++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		next, exists, err := app.Journal.State(spec.UID)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			state = next
			if done(state) {
				return state
			}
		}
	}
	t.Fatalf("release did not converge: phase=%s step=%d", state.Status.Phase, state.Status.Step)
	return state
}

func windowSample(window v1.WindowKey, id, role string, at time.Time) v1.Sample {
	return v1.Sample{
		APIVersion: v1.Version, Service: window.Service, BindingVersion: window.BindingVersion,
		ID: id, Role: role, At: at, LatencyMS: 10,
	}
}

func condition(state v1.ReleaseState, kind, reason string) bool {
	for _, item := range state.Status.Conditions {
		if item.Type == kind && item.Reason == reason {
			return true
		}
	}
	return false
}

func receiptHasReason(receipt journal.Receipt, reason string) bool {
	for _, record := range receipt.Decisions {
		if record.Reason == reason {
			return true
		}
	}
	return false
}
