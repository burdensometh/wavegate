package traffic

import (
	"strconv"
	"testing"

	"wavegate/discovery"
	"wavegate/v1"
)

func domainSnapshot(service v1.Service) discovery.Snapshot {
	ready := []v1.ReplicaObservation{
		{Service: service, Node: "node-a", Name: "stable", UID: "a-stable", Revision: "blue-v1", Ready: true},
		{Service: service, Node: "node-a", Name: "canary", UID: "a-canary", Revision: "blue-v2", Ready: true},
		{Service: service, Node: "node-b", Name: "stable", UID: "b-stable", Revision: "blue-v1", Ready: true},
		{Service: service, Node: "node-b", Name: "canary", UID: "b-canary", Revision: "blue-v2", Ready: true},
	}
	return discovery.Snapshot{Version: 9, Synced: true, Ready: ready}
}

func domainSpec() v1.ReleaseSpec {
	return v1.ReleaseSpec{
		Service: v1.Service{Namespace: "shop", Name: "geo"}, Stable: "blue-v1", Candidate: "blue-v2",
		Steps: []int{10}, MinReady: 1,
		Domains: []v1.DomainSpec{{Name: "a", Nodes: []string{"node-a"}}, {Name: "b", Nodes: []string{"node-b"}}},
	}
}

func TestRenderDomainRoutesFollowEachDomainsCandidates(t *testing.T) {
	spec := domainSpec()
	rule := Render(spec, domainSnapshot(spec.Service), spec.Stable, spec.Candidate, 10)
	if err := rule.Validate(); err != nil {
		t.Fatalf("rendered domain rule is invalid: %v", err)
	}
	if rule.CandidateWeight != 10 {
		t.Fatalf("headline weight = %d, want 10", rule.CandidateWeight)
	}
	for _, route := range rule.DomainRoutes {
		if route.CandidateWeight != 10 {
			t.Fatalf("domain %s weight = %d, want 10", route.Name, route.CandidateWeight)
		}
	}
	if got, ok := RouteWeight(rule, "a"); !ok || got != 10 {
		t.Fatalf("route weight a = %d %v", got, ok)
	}
	if _, ok := RouteWeight(rule, "z"); ok {
		t.Fatalf("undeclared domain somehow routed")
	}
	if targets := DomainTargets(rule.StableTargets, "a"); len(targets) != 1 || targets[0].UID != "a-stable" {
		t.Fatalf("domain a stable targets = %+v", targets)
	}
	if targets := DomainTargets(rule.CandidateTargets, "b"); len(targets) != 1 || targets[0].UID != "b-canary" {
		t.Fatalf("domain b candidate targets = %+v", targets)
	}
}

func TestRenderPinsDomainWithoutCandidatesToStable(t *testing.T) {
	spec := domainSpec()
	snapshot := domainSnapshot(spec.Service)
	// Drop domain b's candidate: b must render at zero and keep its stable.
	snapshot.Ready = snapshot.Ready[:3]
	rule := Render(spec, snapshot, spec.Stable, spec.Candidate, 10)
	if err := rule.Validate(); err != nil {
		t.Fatalf("pinned rule is invalid: %v", err)
	}
	if rule.CandidateWeight != 10 {
		t.Fatalf("headline weight = %d, want 10 from domain a", rule.CandidateWeight)
	}
	if got, _ := RouteWeight(rule, "a"); got != 10 {
		t.Fatalf("domain a weight = %d", got)
	}
	if got, _ := RouteWeight(rule, "b"); got != 0 {
		t.Fatalf("domain b weight = %d, want 0", got)
	}
	for _, target := range rule.CandidateTargets {
		if target.Domain == "b" {
			t.Fatalf("domain b has a candidate target without a ready replica")
		}
	}
	if !Ready(spec, rule) {
		t.Fatalf("at least domain a should be ready")
	}
	if AllDomainsReady(spec, rule) {
		t.Fatalf("promotion must wait while domain b has no candidates")
	}
}

func TestRenderDomainRuleWithNoReadyCandidateIsStableOnly(t *testing.T) {
	spec := domainSpec()
	snapshot := domainSnapshot(spec.Service)
	snapshot.Ready = snapshot.Ready[:1] // only domain a stable
	rule := Render(spec, snapshot, spec.Stable, spec.Candidate, 10)
	if rule.CandidateWeight != 0 || len(rule.CandidateTargets) != 0 {
		t.Fatalf("no candidate should render stable only: %+v", rule)
	}
	if err := rule.Validate(); err != nil {
		t.Fatalf("stable-only domain rule invalid: %v", err)
	}
	if Ready(spec, rule) {
		t.Fatalf("stable-only domain rule reported ready")
	}
}

func TestRenderSortsDeclaredDomainsIntoAValidRule(t *testing.T) {
	spec := domainSpec()
	// Declare the domains out of name order; the persisted rule must still
	// sort its routes so validation accepts it.
	spec.Domains = []v1.DomainSpec{{Name: "b", Nodes: []string{"node-b"}}, {Name: "a", Nodes: []string{"node-a"}}}
	rule := Render(spec, domainSnapshot(spec.Service), spec.Stable, spec.Candidate, 10)
	if err := rule.Validate(); err != nil {
		t.Fatalf("unsorted declaration rendered an invalid rule: %v", err)
	}
	if rule.DomainRoutes[0].Name != "a" || rule.DomainRoutes[1].Name != "b" {
		t.Fatalf("routes not sorted by name: %+v", rule.DomainRoutes)
	}
}

func TestRenderPromotionCarriesDomains(t *testing.T) {
	spec := domainSpec()
	rule := Render(spec, domainSnapshot(spec.Service), spec.Candidate, "", 0)
	if err := rule.Validate(); err != nil {
		t.Fatalf("promoted domain rule invalid: %v", err)
	}
	if rule.HasCandidateTraffic() || rule.CandidateWeight != 0 {
		t.Fatalf("promoted rule still serves a candidate: %+v", rule)
	}
	if len(rule.StableTargets) != 2 {
		t.Fatalf("promoted rule stable targets = %+v", rule.StableTargets)
	}
	for _, route := range rule.DomainRoutes {
		if route.CandidateWeight != 0 {
			t.Fatalf("promoted route %s carries weight %d", route.Name, route.CandidateWeight)
		}
	}
}

func BenchmarkRenderDomainRule(b *testing.B) {
	service := v1.Service{Namespace: "shop", Name: "geo"}
	spec := domainSpec()
	spec.Domains = []v1.DomainSpec{{Name: "a", Nodes: []string{"node-a"}}, {Name: "b", Nodes: []string{"node-b"}}}
	snapshot := discovery.Snapshot{Version: 4500, Synced: true, Ready: make([]v1.ReplicaObservation, 0, 4500)}
	for i := 0; i < 4500; i++ {
		revision := "blue-v1"
		if i%3 == 0 {
			revision = "blue-v2"
		}
		node := "node-a"
		if i%2 == 0 {
			node = "node-b"
		}
		snapshot.Ready = append(snapshot.Ready, v1.ReplicaObservation{
			Service: service, Revision: revision, Node: node, Name: "pod-" + strconv.Itoa(i), UID: "uid-" + strconv.Itoa(i), Ready: true,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rule := Render(spec, snapshot, "blue-v1", "blue-v2", 20)
		if len(rule.DomainRoutes) != 2 || len(rule.StableTargets) == 0 {
			b.Fatal("domain render dropped targets")
		}
	}
}
