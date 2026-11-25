package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"wavegate/metrics"
	"wavegate/v1"
)

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /v1/endpoints", a.endpoints)
	mux.HandleFunc("PUT /v1/endpoints", a.putEndpoint)
	mux.HandleFunc("DELETE /v1/endpoints/{namespace}/{service}/{node}/{name}", a.deleteEndpoint)
	mux.HandleFunc("GET /v1/releases", a.releases)
	mux.HandleFunc("PUT /v1/releases", a.putRelease)
	mux.HandleFunc("GET /v1/releases/{namespace}/{name}", a.release)
	mux.HandleFunc("GET /v1/releases/{namespace}/{name}/receipt", a.receipt)
	mux.HandleFunc("GET /v1/decisions", a.decisions)
	mux.HandleFunc("GET /v1/decisions/watch", a.watchDecisions)
	mux.HandleFunc("GET /v1/namespaces/{namespace}/quota", a.usage)
	mux.HandleFunc("GET /v1/traffic/{namespace}/{service}", a.binding)
	mux.HandleFunc("GET /v1/traffic/{namespace}/{service}/activity", a.activity)
	mux.HandleFunc("GET /v1/samples/{namespace}/{service}/{binding}", a.sampleEvidence)
	mux.HandleFunc("POST /v1/samples", a.addSample)
	mux.HandleFunc("/route/{namespace}/{service}/{path...}", a.route)
	return mux
}

type problem struct {
	APIVersion string `json:"apiVersion"`
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Checkpoint uint64 `json:"checkpointSequence,omitempty"`
}

func reply(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func response(w http.ResponseWriter, value any, err error) {
	if err != nil {
		respondError(w, err)
		return
	}
	reply(w, http.StatusOK, value)
}

func respondError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, v1.ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, v1.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, v1.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, v1.ErrExpired):
		code = http.StatusGone
	case errors.Is(err, v1.ErrUnavailable), errors.Is(err, context.Canceled):
		code = http.StatusServiceUnavailable
	}
	message := err.Error()
	if code == http.StatusInternalServerError {
		message = "internal state operation failed"
	}
	reply(w, code, problem{APIVersion: v1.Version, Code: code, Message: message})
}

func decode(w http.ResponseWriter, r *http.Request, result any) bool {
	err := v1.Decode(http.MaxBytesReader(w, r.Body, v1.MaxBody), result)
	if err != nil {
		respondError(w, v1.Invalid(err.Error()))
		return false
	}
	return true
}

func number(value string, fallback uint64) (uint64, error) {
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, v1.Invalid("invalid resource version or cursor")
	}
	return n, nil
}

func servicePath(r *http.Request) (v1.Service, error) {
	service := v1.Service{Namespace: r.PathValue("namespace"), Name: r.PathValue("service")}
	return service, service.Validate()
}

func (a *App) health(w http.ResponseWriter, _ *http.Request) {
	snapshot := a.Index.Snapshot(v1.Service{})
	code := http.StatusServiceUnavailable
	if snapshot.Synced {
		code = http.StatusOK
	}
	reply(w, code, struct {
		APIVersion      string `json:"apiVersion"`
		Synced          bool   `json:"synced"`
		ResourceVersion uint64 `json:"resourceVersion"`
	}{v1.Version, snapshot.Synced, snapshot.Version})
}

func (a *App) endpoints(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("watch") {
	case "":
		list, err := a.Catalog.List(r.Context())
		response(w, list, err)
	case "1":
		a.watch(w, r)
	default:
		respondError(w, v1.Invalid("watch must be 1 when supplied"))
	}
}

func (a *App) watch(w http.ResponseWriter, r *http.Request) {
	cursor, err := number(r.URL.Query().Get("resourceVersion"), 0)
	if err != nil {
		respondError(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(w, v1.ErrUnavailable)
		return
	}
	stream, err := a.Catalog.Watch(r.Context(), cursor)
	if err != nil {
		respondError(w, err)
		return
	}
	defer stream.Close()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	encoder := json.NewEncoder(w)
	for {
		event, err := stream.Next(r.Context())
		if err != nil || encoder.Encode(event) != nil {
			return
		}
		flusher.Flush()
	}
}

func (a *App) putEndpoint(w http.ResponseWriter, r *http.Request) {
	var endpoint v1.ReplicaObservation
	if decode(w, r, &endpoint) {
		persisted, err := a.Catalog.Put(endpoint)
		response(w, persisted, err)
	}
}

func (a *App) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	service, err := servicePath(r)
	if err != nil {
		respondError(w, err)
		return
	}
	version, err := number(r.URL.Query().Get("resourceVersion"), 0)
	if err != nil {
		respondError(w, err)
		return
	}
	node, name, uid := r.PathValue("node"), r.PathValue("name"), r.URL.Query().Get("uid")
	if !v1.ValidLabel(node) || !v1.ValidLabel(name) || !v1.ValidToken(uid) || version == 0 {
		respondError(w, v1.Invalid("delete requires endpoint identity and resource version"))
		return
	}
	event, err := a.Catalog.Delete(service, node, name, uid, version)
	response(w, event, err)
}

func (a *App) releases(w http.ResponseWriter, _ *http.Request) {
	list, err := a.Specs.List()
	response(w, list, err)
}

func (a *App) putRelease(w http.ResponseWriter, r *http.Request) {
	var spec v1.ReleaseSpec
	if decode(w, r, &spec) {
		view, err := a.Specs.Put(spec)
		response(w, view, err)
	}
}

func (a *App) release(w http.ResponseWriter, r *http.Request) {
	view, err := a.Specs.Get(r.PathValue("namespace"), r.PathValue("name"))
	response(w, view, err)
}

func (a *App) receipt(w http.ResponseWriter, r *http.Request) {
	view, err := a.Specs.Get(r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		respondError(w, err)
		return
	}
	doc, err := a.Journal.Receipt(view.Spec.UID, a.Clock.Now())
	response(w, doc, err)
}

func (a *App) decisions(w http.ResponseWriter, r *http.Request) {
	after, err := number(r.URL.Query().Get("after"), 0)
	if err != nil {
		respondError(w, err)
		return
	}
	limit, err := number(r.URL.Query().Get("limit"), 100)
	if err != nil || limit == 0 || limit > 1000 {
		respondError(w, v1.Invalid("decision page limit"))
		return
	}
	page, err := a.Journal.Events(after, int(limit))
	if errors.Is(err, v1.ErrExpired) {
		reply(w, http.StatusGone, problem{
			APIVersion: v1.Version, Code: http.StatusGone, Message: "decision cursor compacted", Checkpoint: page.CheckpointSequence,
		})
		return
	}
	response(w, page, err)
}

func (a *App) usage(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	if !v1.ValidLabel(namespace) {
		respondError(w, v1.Invalid("namespace"))
		return
	}
	reply(w, http.StatusOK, a.Engine.Usage(namespace))
}

func (a *App) binding(w http.ResponseWriter, r *http.Request) {
	service, err := servicePath(r)
	if err != nil {
		respondError(w, err)
		return
	}
	binding, exists, err := a.Plane.Observe(service)
	if err == nil && !exists {
		err = v1.ErrNotFound
	}
	response(w, binding, err)
}

func (a *App) addSample(w http.ResponseWriter, r *http.Request) {
	var sample v1.Sample
	if !decode(w, r, &sample) {
		return
	}
	if err := sample.Validate(); err != nil {
		respondError(w, err)
		return
	}
	binding, found, err := a.Plane.Observe(sample.Service)
	if err != nil {
		respondError(w, err)
		return
	}
	if !found || sample.BindingVersion != binding.ResourceVersion || binding.Rule.CandidateWeight == 0 ||
		sample.At.Before(binding.AppliedAt) || sample.At.After(a.Clock.Now()) {
		respondError(w, v1.Invalid("sample requires the current weighted binding and an elapsed timestamp"))
		return
	}
	added, err := a.Metrics.Add(sample)
	response(w, struct {
		APIVersion string `json:"apiVersion"`
		Added      bool   `json:"added"`
	}{v1.Version, added}, err)
}

func (a *App) sampleEvidence(w http.ResponseWriter, r *http.Request) {
	service, err := servicePath(r)
	if err != nil {
		respondError(w, err)
		return
	}
	version, err := number(r.PathValue("binding"), 0)
	if err != nil || version == 0 {
		respondError(w, v1.Invalid("binding version"))
		return
	}
	samples, sealed, err := a.Metrics.Evidence(service, version)
	response(w, metrics.EvidenceDocument{APIVersion: v1.Version, Samples: samples, Sealed: sealed}, err)
}

func (a *App) route(w http.ResponseWriter, r *http.Request) {
	service, err := servicePath(r)
	if err != nil {
		respondError(w, err)
		return
	}
	// The fake API accepts controlled endpoint fixtures. Only localhost
	// origins are accepted by v1 validation for downstream connections.
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.UpstreamTimeout())
	defer cancel()
	request := r.Clone(ctx)
	request.URL.Path = "/" + r.PathValue("path")
	request.URL.RawPath = ""
	a.router.ServeService(w, request, service)
}
