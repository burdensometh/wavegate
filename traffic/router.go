package traffic

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"wavegate/clock"
	"wavegate/v1"
)

type SampleWriter interface {
	Add(v1.Sample) (bool, error)
}

type counterKey struct {
	service v1.Service
	domain  string
}

type counter struct {
	version   uint64
	request   uint64
	stable    uint64
	candidate uint64
}

type Router struct {
	plane     RoutePlane
	resolver  Resolver
	tracker   *DrainTracker
	samples   SampleWriter
	clock     clock.Clock
	transport http.RoundTripper
	instance  string
	report    func(error)
	mu        sync.Mutex
	requestID atomic.Uint64
	counters  map[counterKey]counter
}

func NewRouter(plane RoutePlane, resolver Resolver, tracker *DrainTracker, samples SampleWriter, clk clock.Clock, transport http.RoundTripper, instance string, report func(error)) *Router {
	return &Router{
		plane: plane, resolver: resolver, tracker: tracker, samples: samples, clock: clk, transport: transport, instance: instance,
		report: report, counters: make(map[counterKey]counter),
	}
}

// selectRole walks the candidate-weight buckets for the request's own domain.
// Counters are per domain so each domain independently delivers the step's
// proportion over its own traffic.
func (r *Router) selectRole(binding v1.TrafficBinding, domain string, weight int) (role string, index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := counterKey{service: binding.Rule.Service, domain: domain}
	c := r.counters[key]
	isOld := c.version > binding.ResourceVersion
	if c.version != binding.ResourceVersion {
		c = counter{version: binding.ResourceVersion}
		if isOld {
			c.request = r.requestID.Load()
		}
	}
	nextBucket := (c.request%100 + 1) * uint64(weight) / 100
	baseBucket := (c.request % 100) * uint64(weight) / 100
	c.request++
	if nextBucket > baseBucket {
		role, index = v1.CandidateRole, c.candidate
		c.candidate++
	} else {
		role, index = v1.StableRole, c.stable
		c.stable++
	}
	if !isOld {
		r.counters[key] = c
	}
	return
}

// requestDomain identifies which node domain a request belongs to. Requests
// pin their domain through the domain query parameter or header.
func requestDomain(req *http.Request) (string, error) {
	domain := req.URL.Query().Get("domain")
	if domain == "" {
		domain = req.Header.Get("X-Wavegate-Domain")
	}
	if domain == "" {
		return "", nil
	}
	if !v1.ValidLabel(domain) {
		return "", v1.Invalid("request domain must be a DNS label")
	}
	return domain, nil
}

func (r *Router) ServeService(w http.ResponseWriter, req *http.Request, service v1.Service) {
	domain, err := requestDomain(req)
	if err != nil {
		http.Error(w, "request domain must be a DNS label", http.StatusBadRequest)
		return
	}
	request, err := r.admit(service, domain)
	if err != nil {
		routeUnavailable(w, err)
		return
	}
	if request.done != nil {
		defer request.done()
	}
	binding, role, revision := request.binding, request.role, request.revision
	requestID := r.requestID.Add(1)
	w.Header().Set("X-Wavegate-Binding", strconv.FormatUint(binding.ResourceVersion, 10))
	if request.domain != "" {
		w.Header().Set("X-Wavegate-Domain", request.domain)
	}
	w.Header().Set("X-Wavegate-Role", role)
	w.Header().Set("X-Wavegate-Revision", revision)
	at := r.clock.Now()
	weighted := HasCandidateTraffic(binding.Rule)
	record := func(failed bool) {
		if r.samples == nil || !weighted {
			return
		}
		latency := r.clock.Now().Sub(at).Milliseconds()
		latency = max(0, min(latency, 600000))
		_, err := r.samples.Add(v1.Sample{
			APIVersion: v1.Version, Service: service, BindingVersion: binding.ResourceVersion,
			ID: fmt.Sprintf("route-%s-%d", r.instance, requestID), Role: role, At: at,
			LatencyMS: latency, Failed: failed,
		})
		if err != nil && r.report != nil && !errors.Is(err, v1.ErrExpired) {
			r.report(err)
		}
	}
	endpoint := request.endpoint
	if endpoint.UID == "" {
		record(true)
		http.Error(w, "no live endpoint for selected role", http.StatusServiceUnavailable)
		return
	}
	address, err := url.Parse(endpoint.Address)
	if err != nil {
		record(true)
		http.Error(w, "invalid upstream", http.StatusBadGateway)
		return
	}
	w.Header().Set("X-Wavegate-Endpoint", endpoint.UID)
	handler := &httputil.ReverseProxy{
		Transport: r.transport,
		Rewrite: func(upstream *httputil.ProxyRequest) {
			upstream.SetURL(address)
			upstream.Out.Header.Del("X-Wavegate-Binding")
			upstream.Out.Header.Del("X-Wavegate-Domain")
			upstream.Out.Header.Del("X-Wavegate-Role")
			upstream.Out.Header.Del("X-Wavegate-Revision")
			upstream.Out.Header.Del("X-Wavegate-Endpoint")
		},
		ModifyResponse: func(response *http.Response) error {
			record(response.StatusCode >= 500)
			for _, name := range []string{"X-Wavegate-Binding", "X-Wavegate-Domain", "X-Wavegate-Role", "X-Wavegate-Revision", "X-Wavegate-Endpoint"} {
				response.Header.Del(name)
			}
			return nil
		},
		ErrorHandler: func(out http.ResponseWriter, _ *http.Request, _ error) {
			record(true)
			http.Error(out, "upstream unavailable", http.StatusBadGateway)
		},
	}
	handler.ServeHTTP(w, req)
}
