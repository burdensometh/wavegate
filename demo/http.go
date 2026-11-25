package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"

	"wavegate/discovery"
	"wavegate/rollout"
	"wavegate/v1"
)

func call[T any](ctx context.Context, api *httptest.Server, method, path string, body any) (T, error) {
	var result T
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return result, err
		}
		input = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, api.URL+path, input)
	if err != nil {
		return result, err
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := api.Client().Do(req)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return result, fmt.Errorf("%s %s returned HTTP %d: %s", method, path, response.StatusCode, message)
	}
	return result, v1.Decode(response.Body, &result)
}

func (s *scenario) sync(ctx context.Context) error {
	source := discovery.HTTPSource{Origin: s.api.URL, Client: s.api.Client()}
	list, err := source.List(ctx)
	if err != nil {
		return err
	}
	return s.app.Index.Replace(list)
}

func (s *scenario) activateEndpoints(ctx context.Context, fixture Fixture) error {
	for _, endpoint := range fixture.Endpoints {
		code := http.StatusOK
		if slices.Contains(fixture.FailingEndpoints, endpoint.UID) {
			code = http.StatusServiceUnavailable
		}
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Wavegate-Probe-Node", endpoint.Node)
			w.WriteHeader(code)
			_, _ = io.WriteString(w, endpoint.UID+" "+r.URL.Path+"\n")
		}))
		s.backends = append(s.backends, backend)
		endpoint.Address = backend.URL
		if _, err := call[v1.ReplicaObservation](ctx, s.api, http.MethodPut, "/v1/endpoints", endpoint); err != nil {
			return err
		}
	}
	return s.sync(ctx)
}

func (s *scenario) put(ctx context.Context, spec v1.ReleaseSpec) (v1.ReleaseSpec, error) {
	view, err := call[rollout.ReleaseView](ctx, s.api, http.MethodPut, "/v1/releases", spec)
	return view.Spec, err
}

func (s *scenario) get(ctx context.Context, spec v1.ReleaseSpec) (rollout.ReleaseView, error) {
	return call[rollout.ReleaseView](ctx, s.api, http.MethodGet, "/v1/releases/"+spec.Key(), nil)
}

func (s *scenario) traffic(ctx context.Context, spec v1.ReleaseSpec) (v1.TrafficBinding, error) {
	return call[v1.TrafficBinding](ctx, s.api, http.MethodGet, "/v1/traffic/"+spec.Service.String(), nil)
}

type Probe struct {
	BindingVersion uint64         `json:"bindingVersion"`
	Stable         int            `json:"stable"`
	Candidate      int            `json:"candidate"`
	Failed         int            `json:"failed"`
	Nodes          map[string]int `json:"nodes"`
	Endpoints      map[string]int `json:"endpoints"`
}

func (s *scenario) probe(ctx context.Context, spec v1.ReleaseSpec, expectedWeight, expectedFailures int) (Probe, error) {
	current, err := s.traffic(ctx, spec)
	if err != nil {
		return Probe{}, err
	}
	probe := Probe{BindingVersion: current.ResourceVersion, Nodes: map[string]int{}, Endpoints: map[string]int{}}
	version := strconv.FormatUint(current.ResourceVersion, 10)
	for i := 0; i < 100; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.api.URL+"/route/"+spec.Service.String()+"/probe", nil)
		if err != nil {
			return probe, err
		}
		response, err := s.api.Client().Do(req)
		if err != nil {
			return probe, err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		response.Body.Close()
		if readErr != nil {
			return probe, readErr
		}
		endpoint := response.Header.Get("X-Wavegate-Endpoint")
		node := response.Header.Get("X-Wavegate-Probe-Node")
		if response.Header.Get("X-Wavegate-Binding") != version || endpoint == "" || node == "" || string(body) != endpoint+" /probe\n" {
			return probe, fmt.Errorf("request %d did not reach the expected endpoint at binding %s: %s", i, version, body)
		}
		probe.Nodes[node]++
		probe.Endpoints[endpoint]++
		switch response.Header.Get("X-Wavegate-Role") {
		case v1.StableRole:
			probe.Stable++
		case v1.CandidateRole:
			probe.Candidate++
		default:
			return probe, fmt.Errorf("request %d had an unexpected route role", i)
		}
		if response.StatusCode >= 500 {
			probe.Failed++
		}
	}
	if probe.Candidate != expectedWeight || probe.Stable != 100-expectedWeight || probe.Failed != expectedFailures {
		return probe, fmt.Errorf("probe expected %d candidate and %d failures, received %+v", expectedWeight, expectedFailures, probe)
	}
	return probe, nil
}
