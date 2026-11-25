package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/discovery"
	"wavegate/fault"
	"wavegate/v1"
)

type daemon struct {
	app    *App
	clk    *clock.Manual
	source *fault.Source
	origin string
	client *http.Client
}

func startDaemon(t *testing.T) *daemon {
	t.Helper()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	cfg.EndpointHistory, cfg.DrainMS, cfg.ReconcileMS, cfg.WatchRetryMS, cfg.UpstreamTimeoutMS = 2, 500, 10, 20, 10000
	clk := clock.NewManual(time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC))
	ready := make(chan *fault.Source, 1)
	app, err := Open(cfg, Options{Clock: clk, WrapSource: func(source discovery.Source) discovery.Source {
		wrapped := fault.NewSource(source)
		ready <- wrapped
		return wrapped
	}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = app.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	terminated := make(chan error, 1)
	go func() { terminated <- app.Serve(ctx, listener, func(err error) { t.Log(err) }) }()
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-terminated:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(6 * time.Second):
			t.Error("daemon did not exit")
		}
		client.CloseIdleConnections()
		if err := app.Close(); err != nil {
			t.Error(err)
		}
	})
	var wrapped *fault.Source
	select {
	case wrapped = <-ready:
	case <-time.After(4 * time.Second):
		t.Fatal("daemon did not open its watch")
	}
	return &daemon{app: app, clk: clk, source: wrapped, origin: "http://" + listener.Addr().String(), client: client}
}

func eventual(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}

func testRelease() v1.ReleaseSpec {
	return v1.ReleaseSpec{
		APIVersion: v1.Version, Service: v1.Service{Namespace: "shop", Name: "cart"},
		Name: "rollout", UID: "rollout-1", Stable: "stable", Candidate: "candidate", Steps: []int{25},
		Window:   v1.WindowPolicy{Seconds: 1, MinSamples: 2, MaxErrorBPS: 1000, MaxErrorDeltaBPS: 1000, MaxMeanLatencyDeltaMS: 1000},
		MinReady: 1,
	}
}

func putEndpoint(t *testing.T, app *App, service v1.Service, revision, uid, address string, version uint64) v1.ReplicaObservation {
	t.Helper()
	observed, err := app.Catalog.Put(v1.ReplicaObservation{
		APIVersion: v1.Version, Service: service, Revision: revision, UID: uid, Address: address,
		Node: "node-a", Name: revision, Ready: true, ResourceVersion: version,
	})
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func startRelease(t *testing.T, d *daemon, spec v1.ReleaseSpec) {
	t.Helper()
	if _, err := d.app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
}

func waitBinding(t *testing.T, d *daemon, spec v1.ReleaseSpec, check func(v1.TrafficBinding) bool) v1.TrafficBinding {
	t.Helper()
	var result v1.TrafficBinding
	eventual(t, "traffic binding for "+spec.Key(), func() bool {
		binding, ok, err := d.app.Plane.Observe(spec.Service)
		result = binding
		return err == nil && ok && check(binding)
	})
	return result
}

func (d *daemon) request(spec v1.ReleaseSpec, path string) (http.Header, int, error) {
	response, err := d.client.Get(d.origin + "/route/" + spec.Service.String() + path)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return response.Header, response.StatusCode, err
}

func backend(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return s
}

func simple(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func TestDaemonWatchFencesStaleUIDBeforeRelist(t *testing.T) {
	upstream := backend(t, simple)
	d := startDaemon(t)
	spec := testRelease()
	putEndpoint(t, d.app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	old := putEndpoint(t, d.app, spec.Service, spec.Candidate, "old-canary", upstream.URL, 0)
	startRelease(t, d, spec)
	first := waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool { return binding.Rule.CandidateWeight == 25 })
	d.source.Pause(true)
	replacement := putEndpoint(t, d.app, spec.Service, spec.Candidate, "new-canary", upstream.URL, old.ResourceVersion)
	var failed int
	for i := 0; i < 20; i++ {
		header, code, err := d.request(spec, "/known")
		if err != nil || header.Get("X-Wavegate-Endpoint") == old.UID || header.Get("X-Wavegate-Endpoint") == replacement.UID {
			t.Fatalf("stale or unobserved target served %s, %d: %v", header, code, err)
		}
		if code == http.StatusServiceUnavailable {
			failed++
		}
	}
	if failed != 5 {
		t.Fatalf("expected 5 failed closed candidate requests, got %d", failed)
	}
	other := v1.Service{Namespace: "other", Name: "app"}
	for i := 0; i < 3; i++ {
		putEndpoint(t, d.app, other, fmt.Sprintf("replica-%d", i), fmt.Sprintf("other-%d", i), upstream.URL, 0)
	}
	d.source.Disconnect()
	refreshed := waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool {
		return binding.ResourceVersion > first.ResourceVersion && len(binding.Rule.CandidateTargets) == 1 &&
			binding.Rule.CandidateTargets[0].UID == replacement.UID
	})
	d.source.Pause(false)
	if refreshed.Rule.CandidateWeight != 25 || d.app.Index.Snapshot(spec.Service).Generation == 0 {
		t.Fatalf("relist changed the step or skipped a watch generation: %+v", refreshed)
	}
	var candidate int
	for i := 0; i < 20; i++ {
		header, code, err := d.request(spec, "/known")
		if err != nil || code != http.StatusOK || header.Get("X-Wavegate-Endpoint") == old.UID {
			t.Fatalf("route after relist = %s %d %v", header, code, err)
		}
		if header.Get("X-Wavegate-Endpoint") == replacement.UID {
			candidate++
		}
	}
	if candidate != 5 {
		t.Fatalf("replacement received %d candidate requests", candidate)
	}
}

func TestPromotionDrainsInFlightRequestAfterMinimumInterval(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	old := backend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(entered)
			select {
			case <-unblock:
			case <-r.Context().Done():
			}
		}
		simple(w, r)
	})
	fresh := backend(t, simple)
	d := startDaemon(t)
	defer once.Do(func() { close(unblock) })
	spec := testRelease()
	putEndpoint(t, d.app, spec.Service, spec.Stable, "stable-uid", old.URL, 0)
	startRelease(t, d, spec)
	waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool { return len(binding.Rule.StableTargets) == 1 })
	result := make(chan error, 1)
	go func() {
		header, code, err := d.request(spec, "/hold")
		if err == nil && (code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "stable-uid") {
			err = fmt.Errorf("in-flight request returned %d from %s", code, header.Get("X-Wavegate-Endpoint"))
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		t.Fatal("stable request never reached its endpoint")
	}
	putEndpoint(t, d.app, spec.Service, spec.Candidate, "new-uid", fresh.URL, 0)
	waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool { return binding.Rule.CandidateWeight == 25 })
	for i := 0; i < 20; i++ {
		if _, code, err := d.request(spec, "/fast"); err != nil || code != http.StatusOK {
			t.Fatalf("steady request: %d %v", code, err)
		}
	}
	d.clk.Advance(time.Second)
	promoted := waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool {
		return binding.Rule.Stable == spec.Candidate
	})
	if len(promoted.Rule.DrainTargets) != 1 || promoted.Rule.DrainTargets[0].UID != "stable-uid" {
		t.Fatalf("promoted without a drain target: %+v", promoted)
	}
	d.clk.Advance(time.Second)
	header, code, err := d.request(spec, "/post")
	if err != nil || code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "new-uid" {
		t.Fatalf("new requests must use promoted endpoints: %s %d %v", header, code, err)
	}
	if current, _, err := d.app.Plane.Observe(spec.Service); err != nil || len(current.Rule.DrainTargets) != 1 {
		t.Fatalf("drain dropped the active old request: %+v %v", current, err)
	}
	once.Do(func() { close(unblock) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("stable request did not finish")
	}
	waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool {
		return binding.ResourceVersion > promoted.ResourceVersion && len(binding.Rule.DrainTargets) == 0
	})
}
