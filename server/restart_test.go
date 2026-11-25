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
