package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/v1"
)

// A terminal release keeps its durable record while a successor legitimately
// takes over the service binding. The offline inspection and the reconcile
// loop must derive the same facts from that snapshot, and the inspection must
// not write.
func TestTerminalReleaseTakeoverConsistentAcrossEntries(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	clk := clock.NewManual(restartTime())
	app, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	primary := testRelease()
	putEndpoint(t, app, primary.Service, primary.Stable, "stable-uid", "http://127.0.0.1:49160", 0)
	putEndpoint(t, app, primary.Service, primary.Candidate, "candidate-uid", "http://127.0.0.1:49161", 0)
	list, err := app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Specs.Put(primary); err != nil {
		t.Fatal(err)
	}
	driveToPhase(t, app, clk, primary, v1.Promoted)
	live, exists, err := app.Plane.Observe(primary.Service)
	if err != nil || !exists || live.OwnerUID != primary.UID || live.Rule.Stable != primary.Candidate {
		t.Fatalf("promoted binding = %+v, %v, %v", live, exists, err)
	}

	successor := primary
	successor.Name, successor.UID = "successor", "rollout-2"
	successor.Stable, successor.Candidate = primary.Candidate, "v3"
	successor.ResourceVersion = 0
	if _, err := app.Specs.Put(successor); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		state, exists, err := app.Journal.State(successor.UID)
		if err != nil {
			t.Fatal(err)
		}
		if exists && state.Binding != nil && state.Intent == nil {
			break
		}
		if err := app.Engine.Reconcile(ctx, successor.Service.Namespace, successor.Name); err != nil {
			t.Fatalf("successor reconcile: %v", err)
		}
		if i == 39 {
			t.Fatalf("successor never committed its binding: %+v", state)
		}
	}
	live, _, err = app.Plane.Observe(primary.Service)
	if err != nil || live.OwnerUID != successor.UID || live.Rule.Stable != primary.Candidate {
		t.Fatalf("successor takeover = %+v, %v", live, err)
	}
	state, _, err := app.Journal.State(successor.UID)
	if err != nil || state.Binding == nil || state.Binding.ResourceVersion != live.ResourceVersion {
		t.Fatalf("successor committed binding = %+v, %v", state.Binding, err)
	}
	if err := app.Engine.Reconcile(ctx, primary.Service.Namespace, primary.Name); err != nil {
		t.Fatalf("terminal release rejects the takeover: %v", err)
	}

	sequence := app.Journal.Sequence()
	before := readStateFiles(t, cfg.StateDirectory)
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg, Options{Clock: clk})
	if err != nil {
		t.Fatalf("offline inspection rejected the takeover snapshot: %v", err)
	}
	report := reopened.Recovery
	if len(report.Pending) != 0 || len(report.StatusRepublish) != 0 || report.BindingCount != 1 || report.ReleaseCount != 2 {
		t.Fatalf("unexpected recovery report after takeover: %+v", report)
	}
	if reopened.Journal.Sequence() != sequence {
		t.Fatalf("inspection committed decisions: %d -> %d", sequence, reopened.Journal.Sequence())
	}
	after := readStateFiles(t, cfg.StateDirectory)
	for name, content := range before {
		if string(content) != string(after[name]) {
			t.Fatalf("inspection rewrote %s", name)
		}
	}
	for _, spec := range []v1.ReleaseSpec{primary, successor} {
		if err := reopened.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatalf("reconcile %s after inspection: %v", spec.Key(), err)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func driveToPhase(t *testing.T, app *App, clk *clock.Manual, spec v1.ReleaseSpec, phase string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		state, exists, err := app.Journal.State(spec.UID)
		if err != nil {
			t.Fatal(err)
		}
		if exists && state.Status.Phase == phase && state.Intent == nil {
			return
		}
		if exists && state.Intent == nil && state.Window != nil && state.Status.Verdict == nil {
			for _, role := range []string{v1.StableRole, v1.CandidateRole} {
				for k := 0; k < 2; k++ {
					sample := v1.Sample{
						APIVersion: v1.Version, Service: spec.Service, BindingVersion: state.Window.BindingVersion,
						ID: fmt.Sprintf("%s-%d-%d", role, i, k), Role: role, At: state.Window.Start, LatencyMS: 10,
					}
					if added, err := app.Metrics.Add(sample); err != nil || !added {
						t.Fatalf("add %s sample: %v, %v", role, added, err)
					}
				}
			}
			clk.Advance(2 * time.Second)
		}
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatalf("%s reconcile toward %s: %v", spec.Key(), phase, err)
		}
	}
	state, _, _ := app.Journal.State(spec.UID)
	t.Fatalf("%s never reached %s; phase=%s step=%d", spec.Key(), phase, state.Status.Phase, state.Status.Step)
}

func readStateFiles(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	contents := make(map[string][]byte)
	for _, name := range []string{"endpoints", "releases", "traffic", "samples", "decisions"} {
		file := name + ".v1.json"
		content, err := os.ReadFile(filepath.Join(directory, file))
		if err != nil {
			t.Fatal(err)
		}
		contents[file] = content
	}
	return contents
}
