package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/rollout"
	"wavegate/v1"
)

type stoppable struct {
	app     *App
	origin  string
	client  *http.Client
	cancel  context.CancelFunc
	stopped chan struct{}
	err     error
}

// startStoppable runs a daemon whose planned stop the test triggers itself.
// The stopped channel closes when Serve returns; err holds its result.
func startStoppable(t *testing.T, configure func(*config.Config)) *stoppable {
	t.Helper()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	cfg.EndpointHistory, cfg.DrainMS, cfg.ReconcileMS, cfg.WatchRetryMS, cfg.UpstreamTimeoutMS = 2, 500, 10, 20, 10000
	if configure != nil {
		configure(&cfg)
	}
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = app.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &stoppable{
		app: app, origin: "http://" + listener.Addr().String(), client: &http.Client{Timeout: 15 * time.Second},
		cancel: cancel, stopped: make(chan struct{}),
	}
	go func() {
		s.err = app.Serve(ctx, listener, func(err error) { t.Log(err) })
		close(s.stopped)
	}()
	t.Cleanup(func() {
		s.waitStopped(t)
		s.client.CloseIdleConnections()
		if err := app.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func (s *stoppable) waitStopped(t *testing.T) error {
	t.Helper()
	s.cancel()
	select {
	case <-s.stopped:
		return s.err
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit")
		return nil
	}
}

func (s *stoppable) get(t *testing.T, path string) (http.Header, int, error) {
	t.Helper()
	response, err := s.client.Get(s.origin + path)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return response.Header, response.StatusCode, err
}

type activityDoc struct {
	Draining bool `json:"draining"`
	Requests []struct {
		Target   v1.Target `json:"target"`
		InFlight int       `json:"inFlight"`
	} `json:"requests"`
}

func (s *stoppable) activity(t *testing.T, spec v1.ReleaseSpec) activityDoc {
	t.Helper()
	response, err := s.client.Get(s.origin + "/v1/traffic/" + spec.Service.String() + "/activity")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Fatalf("activity returned %d: %s", response.StatusCode, body)
	}
	var doc activityDoc
	if err := json.NewDecoder(response.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func (s *stoppable) waitBinding(t *testing.T, spec v1.ReleaseSpec, check func(v1.TrafficBinding) bool) v1.TrafficBinding {
	t.Helper()
	var result v1.TrafficBinding
	eventual(t, "traffic binding for "+spec.Key(), func() bool {
		binding, ok, err := s.app.Plane.Observe(spec.Service)
		result = binding
		return err == nil && ok && check(binding)
	})
	return result
}

func TestPlannedStopDrainsRegisteredResponse(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	upstream := backend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(entered)
			select {
			case <-unblock:
			case <-r.Context().Done():
			}
		}
		simple(w, r)
	})
	defer once.Do(func() { close(unblock) })
	s := startStoppable(t, func(cfg *config.Config) { cfg.ShutdownDrainMS = 10000 })
	spec := testRelease()
	putEndpoint(t, s.app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	if _, err := s.app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	s.waitBinding(t, spec, func(binding v1.TrafficBinding) bool { return len(binding.Rule.StableTargets) == 1 })
	result := make(chan error, 1)
	go func() {
		header, code, err := s.get(t, "/route/"+spec.Service.String()+"/hold")
		if err == nil && (code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "stable-uid") {
			err = fmt.Errorf("in-flight request returned %d from %s", code, header.Get("X-Wavegate-Endpoint"))
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		t.Fatal("registered request never reached its endpoint")
	}
	if doc := s.activity(t, spec); doc.Draining || len(doc.Requests) != 1 || doc.Requests[0].InFlight != 1 {
		t.Fatalf("activity before stop = %+v", doc)
	}
	s.cancel()
	eventual(t, "observable drain on the activity endpoint", func() bool {
		return s.activity(t, spec).Draining
	})
	if _, code, err := s.get(t, "/route/"+spec.Service.String()+"/fast"); err != nil || code != http.StatusServiceUnavailable {
		t.Fatalf("new proxy request during drain = %d, %v", code, err)
	}
	if doc := s.activity(t, spec); len(doc.Requests) != 1 || doc.Requests[0].InFlight != 1 {
		t.Fatalf("drain dropped the registered request: %+v", doc)
	}
	once.Do(func() { close(unblock) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("registered response did not complete within the drain window")
	}
	if err := s.waitStopped(t); err != nil {
		t.Fatalf("clean planned stop returned: %v", err)
	}
}

func TestPlannedStopDeadlineEndsResidualResponse(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	upstream := backend(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		<-r.Context().Done()
		simple(w, r)
	})
	s := startStoppable(t, func(cfg *config.Config) {
		cfg.ShutdownDrainMS = 300
		cfg.UpstreamTimeoutMS = 30000
	})
	spec := testRelease()
	putEndpoint(t, s.app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	if _, err := s.app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	s.waitBinding(t, spec, func(binding v1.TrafficBinding) bool { return len(binding.Rule.StableTargets) == 1 })
	result := make(chan error, 1)
	go func() {
		_, code, err := s.get(t, "/route/"+spec.Service.String()+"/stuck")
		if err == nil && code != http.StatusBadGateway {
			err = fmt.Errorf("residual request ended with %d", code)
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		t.Fatal("residual request never reached its endpoint")
	}
	s.cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("residual connection survived the drain deadline")
	}
	err := s.waitStopped(t)
	if err == nil || !strings.Contains(err.Error(), "planned stop") {
		t.Fatalf("stop past the drain deadline returned %v", err)
	}
}

func TestPlannedStopLeavesPersistedIntentForSingleResume(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	spec := testRelease()
	app, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
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
	var prepared v1.ReleaseState
	for step := 0; step < 10 && prepared.Intent == nil; step++ {
		if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
			t.Fatal(err)
		}
		if prepared, _, err = app.Journal.State(spec.UID); err != nil {
			t.Fatal(err)
		}
	}
	if prepared.Intent == nil {
		t.Fatal("no traffic operation was ever prepared")
	}
	operation := prepared.Intent.Operation
	sequence := app.Journal.Sequence()
	app.Engine.Stop()
	if err := app.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); !errors.Is(err, rollout.ErrStopping) {
		t.Fatalf("reconcile after stop = %v", err)
	}
	if got := app.Journal.Sequence(); got != sequence {
		t.Fatalf("stop committed new decisions: %d -> %d", sequence, got)
	}
	state, _, err := app.Journal.State(spec.UID)
	if err != nil || state.Intent == nil || state.Intent.Operation != operation {
		t.Fatalf("stop lost the persisted operation: %+v %v", state, err)
	}
	if _, exists, err := app.Plane.Observe(spec.Service); err != nil || exists {
		t.Fatalf("stop applied traffic: %v %v", exists, err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg, Options{Clock: clock.NewManual(restartTime())})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pending := reopened.Recovery.Pending
	if len(pending) != 1 || pending[0].Applied || pending[0].Operation != operation {
		t.Fatalf("recovery did not surface the persisted operation: %+v", pending)
	}
	list, err = reopened.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	resumed, _, err := reopened.Journal.State(spec.UID)
	if err != nil {
		t.Fatal(err)
	}
	live, exists, err := reopened.Plane.Observe(spec.Service)
	if err != nil || !exists {
		t.Fatalf("resume applied no binding: %v %v", exists, err)
	}
	if resumed.Intent != nil || resumed.Binding == nil || resumed.Binding.Operation != operation ||
		live.ResourceVersion != resumed.Binding.ResourceVersion {
		t.Fatalf("resume committed a different operation: %+v live=%+v", resumed, live)
	}
	commits := func() int {
		page, err := reopened.Journal.Events(0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, record := range page.Records {
			if record.Kind == "TrafficCommitted" && record.State.Binding != nil && record.State.Binding.Operation == operation {
				count++
			}
		}
		return count
	}
	if got := commits(); got != 1 {
		t.Fatalf("persisted operation resumed %d times", got)
	}
	if err := reopened.Engine.Reconcile(ctx, spec.Service.Namespace, spec.Name); err != nil {
		t.Fatal(err)
	}
	if got := commits(); got != 1 {
		t.Fatalf("persisted operation resumed again on the next reconcile: %d", got)
	}
}
