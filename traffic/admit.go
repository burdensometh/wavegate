package traffic

import (
	"errors"
	"net/http"

	"wavegate/v1"
)

type RoutePlane interface {
	Observe(v1.Service) (v1.TrafficBinding, bool, error)
	WithCurrent(v1.Service, uint64, func()) error
}

type Resolver interface {
	WithResolvedTargets(v1.Service, string, []v1.Target, func([]v1.ReplicaObservation) error) error
}

type admission struct {
	binding  v1.TrafficBinding
	role     string
	revision string
	endpoint v1.ReplicaObservation
	done     func()
}

func (r *Router) admit(service v1.Service) (admission, error) {
	for attempt := 0; attempt < 4; attempt++ {
		binding, exists, err := r.plane.Observe(service)
		if err != nil {
			return admission{}, err
		}
		if !exists {
			return admission{}, v1.ErrNotFound
		}
		next := admission{binding: binding}
		role, index := r.selectRole(binding)
		next.role = role
		targets := binding.Rule.StableTargets
		next.revision = binding.Rule.Stable
		if next.role == v1.CandidateRole {
			next.revision, targets = binding.Rule.Candidate, binding.Rule.CandidateTargets
		}
		err = r.resolver.WithResolvedTargets(service, next.revision, targets, func(live []v1.ReplicaObservation) error {
			return r.plane.WithCurrent(service, binding.ResourceVersion, func() {
				if len(live) == 0 {
					return
				}
				next.endpoint = live[index%uint64(len(live))]
				target := v1.Target{Node: next.endpoint.Node, Name: next.endpoint.Name, UID: next.endpoint.UID}
				next.done = r.tracker.Begin(service, target, binding.ResourceVersion)
			})
		})
		if errors.Is(err, v1.ErrConflict) {
			continue
		}
		return next, err
	}
	return admission{}, v1.ErrUnavailable
}

func routeUnavailable(w http.ResponseWriter, err error) {
	if errors.Is(err, v1.ErrNotFound) {
		http.Error(w, "traffic binding not found", http.StatusNotFound)
		return
	}
	http.Error(w, "traffic binding unavailable", http.StatusServiceUnavailable)
}
