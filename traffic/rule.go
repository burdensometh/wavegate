package traffic

import (
	"cmp"
	"slices"
	"time"

	"wavegate/discovery"
	"wavegate/v1"
)

// Render builds the routes a service serves at the given candidate weight.
// When the release declares node domains, each domain keeps its own stable and
// candidate targets and receives the requested weight only from its own ready
// candidates. A domain without enough ready candidates is pinned to weight zero
// so its requests keep falling back to that domain's stable revision.
func Render(spec v1.ReleaseSpec, snapshot discovery.Snapshot, stable, candidate string, weight int) v1.Rule {
	rule := v1.Rule{
		Service: spec.Service, Stable: stable, Candidate: candidate, CandidateWeight: weight,
		DiscoveryVersion: snapshot.Version, StableTargets: []v1.Target{}, CandidateTargets: []v1.Target{},
	}
	if len(spec.Domains) == 0 {
		for _, endpoint := range snapshot.Ready {
			if endpoint.Service != spec.Service {
				continue
			}
			target := v1.Target{Node: endpoint.Node, Name: endpoint.Name, UID: endpoint.UID}
			switch endpoint.Revision {
			case stable:
				rule.StableTargets = append(rule.StableTargets, target)
			case candidate:
				if candidate != "" {
					rule.CandidateTargets = append(rule.CandidateTargets, target)
				}
			}
		}
		slices.SortFunc(rule.StableTargets, compareTarget)
		slices.SortFunc(rule.CandidateTargets, compareTarget)
		return rule
	}
	for _, endpoint := range snapshot.Ready {
		if endpoint.Service != spec.Service {
			continue
		}
		domain := spec.DomainForNode(endpoint.Node)
		if domain == "" {
			continue
		}
		target := v1.Target{Node: endpoint.Node, Name: endpoint.Name, UID: endpoint.UID, Domain: domain}
		switch endpoint.Revision {
		case stable:
			rule.StableTargets = append(rule.StableTargets, target)
		case candidate:
			if candidate != "" {
				rule.CandidateTargets = append(rule.CandidateTargets, target)
			}
		}
	}
	slices.SortFunc(rule.StableTargets, compareTarget)
	slices.SortFunc(rule.CandidateTargets, compareTarget)
	ready := DomainReadiness(spec.Domains, spec.MinReady, rule.StableTargets, rule.CandidateTargets)
	rule.DomainRoutes = make([]v1.DomainRoute, 0, len(spec.Domains))
	servedWeight := 0
	for _, domain := range spec.Domains {
		routeWeight := 0
		if weight > 0 && ready[domain.Name] {
			routeWeight = weight
			servedWeight = weight
		}
		rule.DomainRoutes = append(rule.DomainRoutes, v1.DomainRoute{Name: domain.Name, CandidateWeight: routeWeight})
	}
	// The rule's headline weight is what the ready domains actually serve. A
	// step with no ready domain is pinned to zero so persisted rules stay
	// consistent with their routes.
	rule.CandidateWeight = servedWeight
	// Rules persist their routes sorted by name; a release may declare its
	// domains in any order.
	slices.SortFunc(rule.DomainRoutes, func(a, b v1.DomainRoute) int { return cmp.Compare(a.Name, b.Name) })
	return rule
}

// DomainReadiness reports, for each declared domain, whether that domain has a
// stable target and at least minReady ready candidate targets.
func DomainReadiness(domains []v1.DomainSpec, minReady int, stable, candidate []v1.Target) map[string]bool {
	ready := make(map[string]bool, len(domains))
	for _, domain := range domains {
		ready[domain.Name] = false
	}
	hasStable := make(map[string]bool, len(domains))
	candidateCount := make(map[string]int, len(domains))
	for _, target := range stable {
		hasStable[target.Domain] = true
	}
	for _, target := range candidate {
		candidateCount[target.Domain]++
	}
	for name := range ready {
		ready[name] = hasStable[name] && candidateCount[name] >= minReady
	}
	return ready
}

// AnyDomainReady reports whether a domain-scoped rule can serve this step from
// at least one domain's own ready candidates.
func AnyDomainReady(rule v1.Rule, minReady int) bool {
	return rule.DomainReady(minReady)
}

// Ready reports whether a release can be weighted at its current candidates.
// Legacy services need the aggregate candidate group to reach minReady; a
// domain-scoped service only needs one of its domains to be ready.
func Ready(spec v1.ReleaseSpec, rule v1.Rule) bool {
	if len(rule.StableTargets) == 0 {
		return false
	}
	if len(spec.Domains) == 0 {
		return len(rule.CandidateTargets) >= spec.MinReady
	}
	return AnyDomainReady(rule, spec.MinReady)
}

// HasCandidateTraffic reports whether the rule serves any candidate weight,
// either across the whole service or within one of its node domains.
func HasCandidateTraffic(rule v1.Rule) bool {
	return rule.HasCandidateTraffic()
}

// AllDomainsReady reports whether every declared domain has its own stable
// targets and enough ready candidate targets. Legacy releases have no domains
// and are always "ready" here.
func AllDomainsReady(spec v1.ReleaseSpec, rule v1.Rule) bool {
	if len(spec.Domains) == 0 {
		return true
	}
	stableByDomain := make(map[string]bool, len(spec.Domains))
	candidateByDomain := make(map[string]int, len(spec.Domains))
	for _, target := range rule.StableTargets {
		stableByDomain[target.Domain] = true
	}
	for _, target := range rule.CandidateTargets {
		candidateByDomain[target.Domain]++
	}
	for _, domain := range spec.Domains {
		if !stableByDomain[domain.Name] || candidateByDomain[domain.Name] < spec.MinReady {
			return false
		}
	}
	return true
}

// StableCoversDomains reports whether the stable target group carries at least
// one target in every declared domain. A domain-scoped promotion only switches
// the service over once every domain has its own stable replica to serve.
func StableCoversDomains(domains []v1.DomainSpec, stable []v1.Target) bool {
	present := make(map[string]bool, len(domains))
	for _, target := range stable {
		present[target.Domain] = true
	}
	for _, domain := range domains {
		if !present[domain.Name] {
			return false
		}
	}
	return true
}

// RouteWeight returns the candidate weight a request's domain serves. Domains
// absent from the rule have no route.
func RouteWeight(rule v1.Rule, domain string) (int, bool) {
	for _, route := range rule.DomainRoutes {
		if route.Name == domain {
			return route.CandidateWeight, true
		}
	}
	return 0, false
}

// DomainTargets filters a target group down to the request's own domain. Legacy
// targets carry no domain, so such requests only ever see undomained targets.
func DomainTargets(targets []v1.Target, domain string) []v1.Target {
	selected := make([]v1.Target, 0, len(targets))
	for _, target := range targets {
		if target.Domain == domain {
			selected = append(selected, target)
		}
	}
	return selected
}

func compareTarget(a, b v1.Target) int {
	switch {
	case a.Key() < b.Key():
		return -1
	case a.Key() > b.Key():
		return 1
	case a.Domain < b.Domain:
		return -1
	case a.Domain > b.Domain:
		return 1
	case a.UID < b.UID:
		return -1
	case a.UID > b.UID:
		return 1
	default:
		return 0
	}
}

func WithDrain(rule, previous v1.Rule, until time.Time) v1.Rule {
	if until.IsZero() {
		return rule
	}
	for _, target := range previous.StableTargets {
		if !slices.Contains(rule.StableTargets, target) && !slices.Contains(rule.CandidateTargets, target) {
			rule.DrainTargets = append(rule.DrainTargets, target)
		}
	}
	if len(rule.DrainTargets) > 0 {
		rule.DrainUntil = until.UTC()
	}
	return rule
}

func SameRoutes(a, b v1.Rule) bool {
	return a.Service == b.Service && a.Stable == b.Stable && a.Candidate == b.Candidate &&
		a.CandidateWeight == b.CandidateWeight && slices.Equal(a.StableTargets, b.StableTargets) &&
		slices.Equal(a.CandidateTargets, b.CandidateTargets) && slices.Equal(a.DomainRoutes, b.DomainRoutes) &&
		slices.Equal(a.DrainTargets, b.DrainTargets) && a.DrainUntil.Equal(b.DrainUntil)
}

func SameRule(a, b v1.Rule) bool {
	return a.DiscoveryVersion == b.DiscoveryVersion && SameRoutes(a, b)
}

func cloneRule(rule v1.Rule) v1.Rule {
	rule.StableTargets = slices.Clone(rule.StableTargets)
	rule.CandidateTargets = slices.Clone(rule.CandidateTargets)
	rule.DomainRoutes = slices.Clone(rule.DomainRoutes)
	rule.DrainTargets = slices.Clone(rule.DrainTargets)
	return rule
}
