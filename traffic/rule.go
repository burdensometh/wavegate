package traffic

import (
	"slices"
	"time"

	"wavegate/discovery"
	"wavegate/v1"
)

func Render(spec v1.ReleaseSpec, snapshot discovery.Snapshot, stable, candidate string, weight int) v1.Rule {
	rule := v1.Rule{
		Service: spec.Service, Stable: stable, Candidate: candidate, CandidateWeight: weight,
		DiscoveryVersion: snapshot.Version, StableTargets: []v1.Target{}, CandidateTargets: []v1.Target{},
	}
	for _, endpoint := range snapshot.Ready {
		if endpoint.Service != spec.Service {
			continue
		}
		// snapshot.Ready already holds only endpoints whose readiness phase
		// permits new traffic; unknown or terminating endpoints never appear.
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

func compareTarget(a, b v1.Target) int {
	switch {
	case a.Key() < b.Key():
		return -1
	case a.Key() > b.Key():
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
		slices.Equal(a.CandidateTargets, b.CandidateTargets) && slices.Equal(a.DrainTargets, b.DrainTargets) &&
		a.DrainUntil.Equal(b.DrainUntil)
}

func SameRule(a, b v1.Rule) bool {
	return a.DiscoveryVersion == b.DiscoveryVersion && SameRoutes(a, b)
}

func cloneRule(rule v1.Rule) v1.Rule {
	rule.StableTargets = slices.Clone(rule.StableTargets)
	rule.CandidateTargets = slices.Clone(rule.CandidateTargets)
	rule.DrainTargets = slices.Clone(rule.DrainTargets)
	return rule
}
