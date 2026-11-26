package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"wavegate/clock"
	"wavegate/config"
	"wavegate/v1"
)

type domainHarness struct {
	app     *App
	api     *httptest.Server
	clk     *clock.Manual
	cfg     config.Config
	service v1.Service
	client  *http.Client
	spec    v1.ReleaseSpec
}

func newDomainHarness(t *testing.T, spec v1.ReleaseSpec) *domainHarness {
	t.Helper()
	cfg := config.Default()
	cfg.StateDirectory = t.TempDir()
	clk := clock.NewManual(time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC))
	h := &domainHarness{clk: clk, service: spec.Service, spec: spec, cfg: cfg}
	h.open(t)
	t.Cleanup(func() {
		h.close()
	})
	if _, err := h.app.Specs.Put(spec); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *domainHarness) open(t *testing.T) {
	t.Helper()
	app, err := Open(h.cfg, Options{Clock: h.clk})
	if err != nil {
		t.Fatal(err)
	}
	h.app, h.api, h.client = app, httptest.NewServer(app.Handler()), nil
	h.client = h.api.Client()
}

func (h *domainHarness) close() {
	if h.api != nil {
		h.api.Close()
		h.api = nil
	}
	if h.app != nil {
		_ = h.app.Close()
		h.app = nil
	}
}

// restart closes the controller and reopens it against the same state
// directory, exercising journal replay of any domain-scoped decision chain.
func (h *domainHarness) restart(t *testing.T) {
	t.Helper()
	h.close()
	h.open(t)
}

func (h *domainHarness) put(t *testing.T, node, name, revision, uid string, ready bool, address string) v1.ReplicaObservation {
	t.Helper()
	version := uint64(0)
	live, err := h.app.Catalog.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range live.Items {
		if endpoint.Service == h.service && endpoint.Node == node && endpoint.Name == name && endpoint.UID == uid {
			version = endpoint.ResourceVersion
		}
	}
	observed, err := h.app.Catalog.Put(v1.ReplicaObservation{
		APIVersion: v1.Version, Service: h.service, Revision: revision, UID: uid, Address: address,
		Node: node, Name: name, Ready: ready, ResourceVersion: version,
	})
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func (h *domainHarness) sync(t *testing.T, ctx context.Context) {
	t.Helper()
	list, err := h.app.Catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.app.Index.Replace(list); err != nil {
		t.Fatal(err)
	}
}

func (h *domainHarness) until(t *testing.T, ctx context.Context, want func(v1.TrafficBinding) bool) v1.TrafficBinding {
	t.Helper()
	for i := 0; i < 40; i++ {
		if err := h.app.Engine.Reconcile(ctx, h.spec.Service.Namespace, h.spec.Name); err != nil {
			t.Fatal(err)
		}
		binding, ok, err := h.app.Plane.Observe(h.service)
		if err != nil {
			t.Fatal(err)
		}
		if ok && want(binding) {
			return binding
		}
	}
	t.Fatal("domain harness never reached the wanted binding")
	return v1.TrafficBinding{}
}

func (h *domainHarness) route(t *testing.T, domain, path string) (http.Header, int) {
	t.Helper()
	url := h.api.URL + "/route/" + h.service.String() + path
	if domain != "" {
		url += "?domain=" + domain
	}
	response, err := h.client.Get(url)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.Header, response.StatusCode
}

func domainSpec() v1.ReleaseSpec {
	return v1.ReleaseSpec{
		APIVersion: v1.Version, Service: v1.Service{Namespace: "shop", Name: "geo"}, Name: "geo-rollout", UID: "geo-1",
		Stable: "blue-v1", Candidate: "blue-v2", Steps: []int{10},
		Window:   v1.WindowPolicy{Seconds: 1, MinSamples: 2, MaxErrorBPS: 1000, MaxErrorDeltaBPS: 1000, MaxMeanLatencyDeltaMS: 1000},
		MinReady: 1,
		Domains:  []v1.DomainSpec{{Name: "east", Nodes: []string{"node-east"}}, {Name: "west", Nodes: []string{"node-west"}}},
	}
}

func routeWeightOf(t *testing.T, rule v1.Rule, domain string) int {
	t.Helper()
	for _, route := range rule.DomainRoutes {
		if route.Name == domain {
			return route.CandidateWeight
		}
	}
	t.Fatalf("rule has no route for %q: %+v", domain, rule.DomainRoutes)
	return 0
}

// routeWeightOf2 avoids t.Fatal inside a wait predicate.
func routeWeightOf2(rule v1.Rule, domain string) int {
	for _, route := range rule.DomainRoutes {
		if route.Name == domain {
			return route.CandidateWeight
		}
	}
	return -1
}

func hasCandidateUID(rule v1.Rule, uid string) bool {
	for _, target := range rule.CandidateTargets {
		if target.UID == uid {
			return true
		}
	}
	return false
}

// TestDomainScopedRolloutServesPerDomainCandidatesAndFallsBack covers a service
// with two node domains: each domain delivers the current step from its own
// ready candidates, a domain without candidates keeps serving its own stable
// revision, and requests only ever reach targets inside their own domain.
func TestDomainScopedRolloutServesPerDomainCandidatesAndFallsBack(t *testing.T) {
	ctx := context.Background()
	upstream := backend(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := newDomainHarness(t, domainSpec())
	h.put(t, "node-east", "stable", "blue-v1", "east-stable", true, upstream.URL)
	h.put(t, "node-east", "canary", "blue-v2", "east-canary", true, upstream.URL)
	h.put(t, "node-west", "stable", "blue-v1", "west-stable", true, upstream.URL)
	h.put(t, "node-west", "canary", "blue-v2", "west-canary", false, upstream.URL)
	h.sync(t, ctx)
	binding := h.until(t, ctx, func(binding v1.TrafficBinding) bool {
		return binding.Rule.CandidateWeight == 10 && len(binding.Rule.DomainRoutes) == 2
	})
	if got := routeWeightOf(t, binding.Rule, "east"); got != 10 {
		t.Fatalf("east route weight = %d, want 10", got)
	}
	if got := routeWeightOf(t, binding.Rule, "west"); got != 0 {
		t.Fatalf("west route weight = %d, want stable fallback", got)
	}
	var eastCandidate int
	for i := 0; i < 100; i++ {
		header, code := h.route(t, "east", "/probe")
		if code != http.StatusOK || header.Get("X-Wavegate-Domain") != "east" {
			t.Fatalf("east request = %d domain=%q", code, header.Get("X-Wavegate-Domain"))
		}
		if header.Get("X-Wavegate-Role") == v1.CandidateRole {
			eastCandidate++
			if header.Get("X-Wavegate-Endpoint") != "east-canary" {
				t.Fatalf("east candidate reached %q", header.Get("X-Wavegate-Endpoint"))
			}
		} else if header.Get("X-Wavegate-Endpoint") != "east-stable" {
			t.Fatalf("east stable reached %q", header.Get("X-Wavegate-Endpoint"))
		}
	}
	for i := 0; i < 100; i++ {
		header, code := h.route(t, "west", "/probe")
		if code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "west-stable" {
			t.Fatalf("west left its stable fallback: %d %q", code, header.Get("X-Wavegate-Endpoint"))
		}
		if header.Get("X-Wavegate-Role") == v1.CandidateRole {
			t.Fatalf("west served a candidate without a ready candidate replica")
		}
	}
	if eastCandidate != 10 {
		t.Fatalf("east delivered %d candidate requests, want 10", eastCandidate)
	}
	if _, code := h.route(t, "north", "/probe"); code != http.StatusNotFound {
		t.Fatalf("unrouted domain returned %d, want 404", code)
	}

	// Make west's candidate ready: west now joins the same step while east
	// keeps the weight it already served.
	h.put(t, "node-west", "canary", "blue-v2", "west-canary", true, upstream.URL)
	h.sync(t, ctx)
	both := h.until(t, ctx, func(binding v1.TrafficBinding) bool {
		return routeWeightOf2(binding.Rule, "east") == 10 && routeWeightOf2(binding.Rule, "west") == 10
	})
	if got := routeWeightOf(t, both.Rule, "east"); got != 10 {
		t.Fatalf("east weight dropped to %d", got)
	}
	if got := routeWeightOf(t, both.Rule, "west"); got != 10 {
		t.Fatalf("west weight = %d after its candidate became ready", got)
	}
	if both.Rule.CandidateWeight != 10 {
		t.Fatalf("rule headline weight = %d", both.Rule.CandidateWeight)
	}
}

// TestDomainScopedRolloutPromotesAcrossBothDomains drives a domain-scoped
// release through its step, observation window and promotion, proving the
// journal's phase and intent validators accept per-domain weighted rules and
// that a promoted service turns every domain into that domain's stable version.
func TestDomainScopedRolloutPromotesAcrossBothDomains(t *testing.T) {
	ctx := context.Background()
	upstream := backend(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := newDomainHarness(t, domainSpec())
	h.put(t, "node-east", "stable", "blue-v1", "east-stable", true, upstream.URL)
	h.put(t, "node-east", "canary", "blue-v2", "east-canary", true, upstream.URL)
	h.put(t, "node-west", "stable", "blue-v1", "west-stable", true, upstream.URL)
	h.put(t, "node-west", "canary", "blue-v2", "west-canary", true, upstream.URL)
	h.sync(t, ctx)
	weighted := h.until(t, ctx, func(binding v1.TrafficBinding) bool {
		return binding.Rule.CandidateWeight == 10 && len(binding.Rule.CandidateTargets) == 2
	})
	// Record the current step's samples and let the window close.
	h.samples(t, weighted, 4)
	h.clk.Advance(2 * time.Second)
	state := h.untilState(t, ctx, func(state v1.ReleaseState) bool {
		return state.Status.Verdict != nil && state.Status.Verdict.Result == v1.Pass
	})
	if state.Spec.UID != h.spec.UID || state.Binding == nil {
		t.Fatalf("missing release state: %+v", state)
	}
	state = h.untilState(t, ctx, func(state v1.ReleaseState) bool {
		return state.Status.Phase == v1.Promoted
	})
	// Reopen the controller: the journal must replay a decision chain whose
	// rules carry per-domain routes, and the promoted binding must survive.
	h.restart(t)
	h.sync(t, ctx)
	recovered := h.state(t, ctx)
	if recovered.Status.Phase != v1.Promoted || recovered.Binding == nil ||
		recovered.Binding.Rule.Stable != h.spec.Candidate {
		t.Fatalf("restart did not recover the promoted domain binding: %+v", recovered)
	}
	if state.Binding.Rule.Stable != h.spec.Candidate {
		t.Fatalf("promotion kept stable revision %q", state.Binding.Rule.Stable)
	}
	for _, route := range state.Binding.Rule.DomainRoutes {
		if route.CandidateWeight != 0 {
			t.Fatalf("promoted domain %s still carries weight %d", route.Name, route.CandidateWeight)
		}
	}
	for i := 0; i < 40; i++ {
		header, code := h.route(t, "east", "/probe")
		if code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "east-canary" {
			t.Fatalf("east promoted request = %d %q", code, header.Get("X-Wavegate-Endpoint"))
		}
	}
	for i := 0; i < 40; i++ {
		header, code := h.route(t, "west", "/probe")
		if code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "west-canary" {
			t.Fatalf("west promoted request = %d %q", code, header.Get("X-Wavegate-Endpoint"))
		}
	}
}

func (h *domainHarness) samples(t *testing.T, binding v1.TrafficBinding, each int) {
	t.Helper()
	at := h.clk.Now()
	for i := 0; i < each; i++ {
		for _, role := range []string{v1.StableRole, v1.CandidateRole} {
			if _, err := h.app.Metrics.Add(v1.Sample{
				APIVersion: v1.Version, Service: h.service, BindingVersion: binding.ResourceVersion,
				ID: role + "-" + strconv.Itoa(i), Role: role, At: at.Add(time.Duration(i) * time.Millisecond),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func (h *domainHarness) state(t *testing.T, ctx context.Context) v1.ReleaseState {
	t.Helper()
	state, ok, err := h.app.Journal.State(h.spec.UID)
	if err != nil || !ok {
		t.Fatalf("journal state: %v", err)
	}
	return state
}

func (h *domainHarness) untilState(t *testing.T, ctx context.Context, want func(v1.ReleaseState) bool) v1.ReleaseState {
	t.Helper()
	var state v1.ReleaseState
	for i := 0; i < 80; i++ {
		if err := h.app.Engine.Reconcile(ctx, h.spec.Service.Namespace, h.spec.Name); err != nil {
			t.Fatal(err)
		}
		state = h.state(t, ctx)
		if want(state) {
			return state
		}
	}
	t.Fatal("domain harness never reached the wanted state")
	return state
}

// TestLegacyServiceIgnoresDomainParameter pins the compatibility contract: a
// release without declared domains keeps serving the whole service even if a
// caller sends a domain parameter, exactly as the original configuration did.
func TestLegacyServiceIgnoresDomainParameter(t *testing.T) {
	upstream := backend(t, simple)
	d := startDaemon(t)
	spec := testRelease()
	putEndpoint(t, d.app, spec.Service, spec.Stable, "stable-uid", upstream.URL, 0)
	startRelease(t, d, spec)
	waitBinding(t, d, spec, func(binding v1.TrafficBinding) bool { return len(binding.Rule.StableTargets) == 1 })
	header, code, err := d.request(spec, "/probe?domain=east")
	if err != nil || code != http.StatusOK || header.Get("X-Wavegate-Endpoint") != "stable-uid" {
		t.Fatalf("legacy domain parameter = %d %q %v", code, header.Get("X-Wavegate-Endpoint"), err)
	}
}

// after one domain rebuilds a replica: the rebuilt domain resumes with the new
// UID and the untouched domain keeps the same step weight.
func TestSingleDomainRebuildKeepsOtherDomainWeight(t *testing.T) {
	ctx := context.Background()
	upstream := backend(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := newDomainHarness(t, domainSpec())
	h.put(t, "node-east", "stable", "blue-v1", "east-stable", true, upstream.URL)
	h.put(t, "node-east", "canary", "blue-v2", "east-canary", true, upstream.URL)
	h.put(t, "node-west", "stable", "blue-v1", "west-stable", true, upstream.URL)
	oldWest := h.put(t, "node-west", "canary", "blue-v2", "west-canary", true, upstream.URL)
	h.sync(t, ctx)
	first := h.until(t, ctx, func(binding v1.TrafficBinding) bool {
		return binding.Rule.CandidateWeight == 10 && len(binding.Rule.CandidateTargets) == 2
	})

	// Rebuild only the west candidate: retire the old UID and add a fresh one
	// under the same pod name and revision. East's UID and weight stay put.
	if _, err := h.app.Catalog.Delete(h.service, "node-west", "canary", oldWest.UID, oldWest.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	h.put(t, "node-west", "canary", "blue-v2", "west-canary-2", true, upstream.URL)
	h.sync(t, ctx)
	refreshed := h.until(t, ctx, func(binding v1.TrafficBinding) bool {
		return binding.ResourceVersion > first.ResourceVersion && hasCandidateUID(binding.Rule, "west-canary-2")
	})
	if got := routeWeightOf(t, refreshed.Rule, "east"); got != 10 {
		t.Fatalf("east weight across a west rebuild = %d, want 10", got)
	}
	if got := routeWeightOf(t, refreshed.Rule, "west"); got != 10 {
		t.Fatalf("west weight after its rebuild = %d, want 10", got)
	}
	if !hasCandidateUID(refreshed.Rule, "east-canary") || hasCandidateUID(refreshed.Rule, "west-canary") {
		t.Fatalf("rebuild did not produce a complete new rule generation: %+v", refreshed.Rule.CandidateTargets)
	}
	if refreshed.Rule.DiscoveryVersion <= first.Rule.DiscoveryVersion {
		t.Fatalf("rebuild kept a stale discovery version: %d", refreshed.Rule.DiscoveryVersion)
	}
	for i := 0; i < 100; i++ {
		header, code := h.route(t, "west", "/probe")
		if code != http.StatusOK || header.Get("X-Wavegate-Endpoint") == "west-canary" {
			t.Fatalf("west still reached the retired replica: %d %q", code, header.Get("X-Wavegate-Endpoint"))
		}
	}
}
