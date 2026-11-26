package v1

import (
	"testing"
)

func TestReleaseSpecDomainsRejectOverlappingNodesAndDuplicates(t *testing.T) {
	base := ReleaseSpec{
		APIVersion: Version, Service: Service{Namespace: "shop", Name: "cart"}, Name: "rollout", UID: "r1",
		Stable: "blue-v1", Candidate: "blue-v2", Steps: []int{10}, MinReady: 1,
		Window: WindowPolicy{Seconds: 1, MinSamples: 1},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("legacy spec rejected: %v", err)
	}
	overlap := base
	overlap.Domains = []DomainSpec{{Name: "a", Nodes: []string{"node-a"}}, {Name: "b", Nodes: []string{"node-a"}}}
	if err := overlap.Validate(); !errorsIs(err, ErrInvalid) {
		t.Fatalf("overlapping node domains accepted: %v", err)
	}
	duplicate := base
	duplicate.Domains = []DomainSpec{{Name: "a", Nodes: []string{"node-a"}}, {Name: "a", Nodes: []string{"node-b"}}}
	if err := duplicate.Validate(); !errorsIs(err, ErrInvalid) {
		t.Fatalf("duplicate domain names accepted: %v", err)
	}
	empty := base
	empty.Domains = []DomainSpec{{Name: "a", Nodes: nil}}
	if err := empty.Validate(); !errorsIs(err, ErrInvalid) {
		t.Fatalf("empty domain accepted: %v", err)
	}
	valid := base
	valid.Domains = []DomainSpec{{Name: "a", Nodes: []string{"node-a"}}, {Name: "b", Nodes: []string{"node-b"}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid domains rejected: %v", err)
	}
	if valid.DomainForNode("node-a") != "a" || valid.DomainForNode("node-z") != "" {
		t.Fatalf("unexpected node-to-domain mapping")
	}
}

func TestRuleDomainRoutesMustAgreeOnWeightAndTargets(t *testing.T) {
	service := Service{Namespace: "shop", Name: "cart"}
	stableA := Target{Node: "node-a", Name: "stable", UID: "a-stable", Domain: "a"}
	candidateA := Target{Node: "node-a", Name: "canary", UID: "a-canary", Domain: "a"}
	stableB := Target{Node: "node-b", Name: "stable", UID: "b-stable", Domain: "b"}
	base := Rule{
		Service: service, Stable: "blue-v1", Candidate: "blue-v2", CandidateWeight: 10,
		StableTargets: []Target{stableA, stableB}, CandidateTargets: []Target{candidateA},
		DomainRoutes: []DomainRoute{{Name: "a", CandidateWeight: 10}, {Name: "b", CandidateWeight: 0}},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid domain rule rejected: %v", err)
	}
	bWeighted := base
	bWeighted.DomainRoutes = []DomainRoute{{Name: "a", CandidateWeight: 10}, {Name: "b", CandidateWeight: 10}}
	if err := bWeighted.Validate(); err == nil {
		t.Fatalf("domain b weighted without its own candidate accepted")
	}
	noDomains := base
	noDomains.DomainRoutes = nil
	if err := noDomains.Validate(); err == nil {
		t.Fatalf("target with a domain accepted without domain routes")
	}
	badHeadline := base
	badHeadline.CandidateWeight = 40
	if err := badHeadline.Validate(); err == nil {
		t.Fatalf("headline weight diverging from routes accepted")
	}
	targetInUndeclared := base
	targetInUndeclared.CandidateTargets = []Target{{Node: "node-c", Name: "canary", UID: "c-canary", Domain: "c"}}
	if err := targetInUndeclared.Validate(); err == nil {
		t.Fatalf("target in undeclared domain accepted")
	}
}

func TestTargetKeepsDomainInRuleIdentity(t *testing.T) {
	service := Service{Namespace: "shop", Name: "cart"}
	root := Rule{Service: service, Stable: "blue-v1", CandidateWeight: 0, StableTargets: []Target{{Node: "node-a", Name: "stable", UID: "a"}}}
	if err := root.Validate(); err != nil {
		t.Fatalf("legacy target rejected: %v", err)
	}
	if !root.HasCandidateTraffic() && root.StepWeight() != 0 {
		t.Fatalf("legacy rule reports candidate traffic")
	}
	routed := root
	routed.Candidate = "blue-v2"
	routed.DomainRoutes = []DomainRoute{{Name: "a", CandidateWeight: 5}}
	routed.StableTargets = []Target{{Node: "node-a", Name: "stable", UID: "a", Domain: "a"}}
	routed.CandidateTargets = []Target{{Node: "node-a", Name: "canary", UID: "c", Domain: "a"}}
	routed.CandidateWeight = 5
	if err := routed.Validate(); err != nil {
		t.Fatalf("domain weighted rule rejected: %v", err)
	}
	if !routed.HasCandidateTraffic() || routed.StepWeight() != 5 || !routed.DomainReady(1) {
		t.Fatalf("domain rule helpers disagree: %+v", routed)
	}
}

func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}
