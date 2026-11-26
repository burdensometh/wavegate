package server

import (
	"net/http"

	"wavegate/traffic"
	"wavegate/v1"
)

type activityView struct {
	APIVersion             string                    `json:"apiVersion"`
	Service                v1.Service                `json:"service"`
	ObservedBindingVersion uint64                    `json:"observedBindingVersion"`
	Draining               bool                      `json:"draining"`
	Requests               []traffic.RequestActivity `json:"requests"`
}

func (a *App) activity(w http.ResponseWriter, r *http.Request) {
	service, err := servicePath(r)
	if err != nil {
		respondError(w, err)
		return
	}
	binding, exists, err := a.Plane.Observe(service)
	if err == nil && !exists {
		err = v1.ErrNotFound
	}
	if err != nil {
		respondError(w, err)
		return
	}
	reply(w, http.StatusOK, activityView{
		APIVersion: v1.Version, Service: service, ObservedBindingVersion: binding.ResourceVersion,
		Draining: a.Drain.Draining(), Requests: a.Drain.Activity(service),
	})
}
