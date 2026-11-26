package traffic

import (
	"errors"
	"net/http"

	"wavegate/v1"
)

var errDomainUnrouted = errors.New("request domain has no route for this service")

type RoutePlane interface {
	Observe(v1.Service) (v1.TrafficBinding, bool, error)
	WithCurrent(v1.Service, uint64, func()) error
}

type Resolver interface {
	WithResolvedTargets(v1.Service, string, []v1.Target, func([]v1.ReplicaObservation) error) error
}

type admission struct {
	binding  v1.TrafficBinding
	domain   string
	role     string
	revision string
	endpoint v1.ReplicaObservation
	done     func()
}

func (r *Router) admit(service v1.Service, domain string) (admission, error) {
	for attempt := 0; attempt < 4; attempt++ {
		binding, exists, err := r.plane.Observe(service)
		if err != nil {
			return admission{}, err
		}
		if !exists {
			return admission{}, v1.ErrNotFound
		}
		rule := binding.Rule
		weight := rule.CandidateWeight
		effective := domain
		if len(rule.DomainRoutes) > 0 {
			routed, ok := RouteWeight(rule, domain)
			if !ok {
				return admission{}, errDomainUnrouted
			}
			weight = routed
		} else {
			// A legacy whole-service release has no domains; a caller's domain
			// parameter is ignored so the original rollout is reproduced exactly.
			effective = ""
		}
		next := admission{binding: binding, domain: effective}
		role, index := r.selectRole(binding, effective, weight)
		next.role = role
		targets := DomainTargets(rule.StableTargets, effective)
		next.revision = rule.Stable
		if next.role == v1.CandidateRole {
			next.revision, targets = rule.Candidate, DomainTargets(rule.CandidateTargets, effective)
		}
		err = r.resolver.WithResolvedTargets(service, next.revision, targets, func(live []v1.ReplicaObservation) error {
			return r.plane.WithCurrent(service, binding.ResourceVersion, func() {
				if len(live) == 0 {
					return
				}
				next.endpoint = live[index%uint64(len(live))]
				target := v1.Target{Node: next.endpoint.Node, Name: next.endpoint.Name, UID: next.endpoint.UID, Domain: effective}
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
	switch {
	case errors.Is(err, errDomainUnrouted):
		http.Error(w, "request domain has no route", http.StatusNotFound)
	case errors.Is(err, v1.ErrNotFound):
		http.Error(w, "traffic binding not found", http.StatusNotFound)
	default:
		http.Error(w, "traffic binding unavailable", http.StatusServiceUnavailable)
	}
}
