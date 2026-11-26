package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/fault"
	"wavegate/metrics"
	"wavegate/rollout"
	"wavegate/traffic"
	"wavegate/v1"
)

func TestInterruptedControllerRecoversLiveOperation(t *testing.T) {
	const key = "WAVEGATE_CHILD_STATE"
	if state := os.Getenv(key); state != "" {
		runInterruptedChild(t, state)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stateDir := t.TempDir()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInterruptedControllerRecoversLiveOperation$")
	child.Env = append(os.Environ(), key+"="+stateDir)
	child.Stderr = os.Stderr
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(output)
	applied := false
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		if line == "binding committed and receipt withheld" {
			applied = true
			break
		}
	}
	if err := child.Process.Kill(); err != nil && applied {
		t.Errorf("interrupt controller: %v", err)
	}
	waitError := child.Wait()
	if !applied || waitError == nil {
		t.Fatalf("child did not stop at intended failure boundary: %v, %s", waitError, strings.Join(lines, "\n"))
	}
	cfg := config.Default()
	cfg.StateDirectory = stateDir
	clk := clock.NewManual(restartTime())
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	pending := app.Recovery.Pending
	if len(pending) != 1 || !pending[0].Applied || pending[0].Kind != v1.WeightStep {
		t.Fatalf("recovery inspection missed the committed operation: %+v", pending)
	}
	spec := testRelease()
	if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	got, exists, err := app.Journal.State(spec.UID)
	if err != nil || !exists || got.Intent != nil || got.Window == nil ||
		got.Binding.ResourceVersion != pending[0].LiveVersion || got.Binding.Operation != pending[0].Operation {
		t.Fatalf("recovery committed a different binding: %+v %v", got, err)
	}
	if usage := app.Engine.Usage(spec.Service.Namespace); usage.Releases != 1 {
		t.Fatalf("replayed reservation = %+v", usage)
	}
}

func restartTime() time.Time { return time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC) }

func runInterruptedChild(t *testing.T, stateDir string) {
	cfg := config.Default()
	cfg.StateDirectory = stateDir
	var dropped *fault.Plane
	app, err := Open(cfg, Options{
		Clock: clock.NewManual(restartTime()),
		WrapPlane: func(plane traffic.Plane) traffic.Plane {
			dropped = &fault.Plane{Plane: plane}
			return dropped
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	spec := testRelease()
	ctx := context.Background()
	putEndpoint(t, app, spec.Service, spec.Stable, "stable-uid", "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, spec.Service, spec.Candidate, "candidate-uid", "http://127.0.0.1:49161", 0)
	list, err := app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 12; step++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatal(err)
		}
		state, _, err := app.Journal.State(spec.UID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Intent == nil || state.Intent.Kind != v1.WeightStep {
			continue
		}
		dropped.DropNextReceipt()
		err = app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name)
		if !errors.Is(err, fault.ErrLostReceipt) {
			t.Fatalf("unexpected apply outcome: %v", err)
		}
		fmt.Println("binding committed and receipt withheld")
		time.Sleep(15 * time.Second)
		t.Fatal("controller was never interrupted")
	}
	t.Fatal("weighted intent was never prepared")
}

func TestInterruptedControllerRecoversSealedWindow(t *testing.T) {
	const key = "WAVEGATE_SEAL_CHILD_STATE"
	if state := os.Getenv(key); state != "" {
		runSealedChild(t, state)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stateDir := t.TempDir()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInterruptedControllerRecoversSealedWindow$")
	child.Env = append(os.Environ(), key+"="+stateDir)
	child.Stderr = os.Stderr
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(output)
	sealed := false
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		if line == "window sealed and decision withheld" {
			sealed = true
			break
		}
	}
	if err := child.Process.Kill(); err != nil && sealed {
		t.Errorf("interrupt controller: %v", err)
	}
	waitError := child.Wait()
	if !sealed || waitError == nil {
		t.Fatalf("child did not stop at the seal boundary: %v, %s", waitError, strings.Join(lines, "\n"))
	}
	cfg := config.Default()
	cfg.StateDirectory = stateDir
	clk := clock.NewManual(restartTime().Add(time.Minute))
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	spec := testRelease()
	if app.Recovery.SealedWindows != 1 || len(app.Recovery.Pending) != 0 {
		t.Fatalf("recovery inspection = %+v", app.Recovery)
	}
	state, exists, err := app.Journal.State(spec.UID)
	if err != nil || !exists || state.Window == nil || state.Status.Verdict != nil || state.Intent != nil {
		t.Fatalf("crash boundary left %+v, %v", state, err)
	}
	preCrashBinding := state.Binding.ResourceVersion
	_, window, err := app.Metrics.Evidence(spec.Service, state.Window.BindingVersion)
	if err != nil || window == nil {
		t.Fatalf("sealed window evidence: %+v, %v", window, err)
	}
	sealedVerdict := window.Verdict
	if sealedVerdict.SampleVersion != app.Recovery.SampleVersion {
		t.Fatalf("seal used a later sample version: %+v vs inventory %d", sealedVerdict, app.Recovery.SampleVersion)
	}
	for _, record := range decisions(t, app) {
		if record.Kind == "WindowEvaluated" || record.State.Status.Verdict != nil {
			t.Fatalf("verdict decision survived the crash: %+v", record)
		}
	}
	// A late arrival for the sealed binding must be refused: the restart may
	// not fold later inputs into the seal.
	late := v1.Sample{
		APIVersion: v1.Version, Service: spec.Service, BindingVersion: state.Window.BindingVersion,
		ID: "late-arrival", Role: v1.CandidateRole, At: state.Window.Start.Add(500 * time.Millisecond), LatencyMS: 10,
	}
	if _, err := app.Metrics.Add(late); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("late sample entered the sealed window: %v", err)
	}
	// An unrelated sample advances the sample high water mark, so any
	// recomputation during recovery produces a detectably different verdict.
	unrelated := v1.Sample{
		APIVersion: v1.Version, Service: v1.Service{Namespace: "other", Name: "app"}, BindingVersion: 7,
		ID: "unrelated", Role: v1.StableRole, At: restartTime(), LatencyMS: 10,
	}
	if added, err := app.Metrics.Add(unrelated); err != nil || !added {
		t.Fatalf("unrelated sample: %v, %v", added, err)
	}
	list, err := app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	var recovered v1.ReleaseState
	promoted := false
	for step := 0; step < 12 && !promoted; step++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatal(err)
		}
		state, exists, err := app.Journal.State(spec.UID)
		if err != nil || !exists {
			t.Fatalf("recovered state: %v, %v", exists, err)
		}
		recovered = state
		promoted = state.Status.Phase == v1.Promoted
	}
	if !promoted || recovered.Status.Verdict == nil {
		t.Fatalf("recovery did not finish the release: %+v", recovered.Status)
	}
	if *recovered.Status.Verdict != sealedVerdict {
		t.Fatalf("recovery recomputed the verdict: sealed %+v, committed %+v", sealedVerdict, *recovered.Status.Verdict)
	}
	binding, exists, err := app.Plane.Observe(spec.Service)
	if err != nil || !exists || binding.Rule.Stable != spec.Candidate || binding.Rule.CandidateWeight != 0 {
		t.Fatalf("recovered live binding = %+v, %v", binding, err)
	}
	if binding.ResourceVersion != preCrashBinding+1 {
		t.Fatalf("recovery applied traffic more than once: pre-crash %d, live %d", preCrashBinding, binding.ResourceVersion)
	}
	var evaluated, promotions int
	for _, record := range decisions(t, app) {
		switch {
		case record.Kind == "WindowEvaluated":
			evaluated++
		case record.Kind == "TrafficCommitted" && record.Reason == v1.Promote:
			promotions++
		}
		if verdict := record.State.Status.Verdict; verdict != nil && *verdict != sealedVerdict {
			t.Fatalf("decision %d carries a recomputed verdict: %+v", record.Sequence, *verdict)
		}
	}
	if evaluated != 1 || promotions != 1 {
		t.Fatalf("recovery left %d verdict decisions and %d promotions", evaluated, promotions)
	}
	sequence := app.Journal.Sequence()
	for step := 0; step < 3; step++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatal(err)
		}
	}
	if app.Journal.Sequence() != sequence {
		t.Fatalf("steady state appended decisions: %d -> %d", sequence, app.Journal.Sequence())
	}
	if current, _, err := app.Plane.Observe(spec.Service); err != nil || current.ResourceVersion != binding.ResourceVersion {
		t.Fatalf("steady state reapplied traffic: %+v, %v", current, err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Recovery.SealedWindows != 1 || len(reopened.Recovery.Pending) != 0 {
		t.Fatalf("post-recovery inspection = %+v", reopened.Recovery)
	}
}

func decisions(t *testing.T, app *App) []v1.DecisionRecord {
	t.Helper()
	page, err := app.Journal.Events(0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return page.Records
}

func runSealedChild(t *testing.T, stateDir string) {
	cfg := config.Default()
	cfg.StateDirectory = stateDir
	clk := clock.NewManual(restartTime())
	var windows *fault.Windows
	app, err := Open(cfg, Options{
		Clock: clk,
		WrapWindows: func(store *metrics.Store) rollout.Windows {
			windows = fault.NewWindows(store)
			return windows
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	spec := testRelease()
	ctx := context.Background()
	putEndpoint(t, app, spec.Service, spec.Stable, "stable-uid", "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, spec.Service, spec.Candidate, "candidate-uid", "http://127.0.0.1:49161", 0)
	list, err := app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	var window v1.WindowKey
	for step := 0; step < 12 && window.BindingVersion == 0; step++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatal(err)
		}
		state, exists, err := app.Journal.State(spec.UID)
		if err != nil || !exists {
			t.Fatalf("weighted state: %v, %v", exists, err)
		}
		if state.Window != nil && state.Intent == nil {
			window = *state.Window
		}
	}
	if window.BindingVersion == 0 {
		t.Fatal("weighted window was never established")
	}
	seed := []v1.Sample{
		{APIVersion: v1.Version, Service: spec.Service, BindingVersion: window.BindingVersion, ID: "stable-0", Role: v1.StableRole, At: window.Start.Add(100 * time.Millisecond), LatencyMS: 10},
		{APIVersion: v1.Version, Service: spec.Service, BindingVersion: window.BindingVersion, ID: "stable-1", Role: v1.StableRole, At: window.Start.Add(200 * time.Millisecond), LatencyMS: 10},
		{APIVersion: v1.Version, Service: spec.Service, BindingVersion: window.BindingVersion, ID: "candidate-0", Role: v1.CandidateRole, At: window.Start.Add(300 * time.Millisecond), LatencyMS: 10},
		{APIVersion: v1.Version, Service: spec.Service, BindingVersion: window.BindingVersion, ID: "candidate-1", Role: v1.CandidateRole, At: window.Start.Add(400 * time.Millisecond), LatencyMS: 10},
	}
	for _, sample := range seed {
		if added, err := app.Metrics.Add(sample); err != nil || !added {
			t.Fatalf("seed sample %s: %v, %v", sample.ID, added, err)
		}
	}
	clk.Advance(2 * time.Second)
	windows.TrapAfterNextSeal(func(v1.WindowVerdict) {
		fmt.Println("window sealed and decision withheld")
		time.Sleep(15 * time.Second)
		t.Fatal("controller was never interrupted")
	})
	if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	t.Fatal("window was never sealed")
}
