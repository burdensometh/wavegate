package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"wavegate/v1"
)

func domainEndpoint(service v1.Service, node, name, revision, uid string) v1.ReplicaObservation {
	return v1.ReplicaObservation{
		APIVersion: v1.Version, Service: service, Node: node, Name: name, Revision: revision, UID: uid,
		Address: "http://127.0.0.1:8080", Ready: true,
	}
}

func TestReadyRuleFencesFullGenerationAcrossDomainRebuild(t *testing.T) {
	ctx := context.Background()
	catalog, err := OpenCatalog(filepath.Join(t.TempDir(), "endpoints.v1.json"), 32)
	if err != nil {
		t.Fatal(err)
	}
	service := v1.Service{Namespace: "shop", Name: "geo"}
	catalog.put(t, domainEndpoint(service, "node-east", "stable", "blue-v1", "east-stable"))
	catalog.put(t, domainEndpoint(service, "node-east", "canary", "blue-v2", "east-canary"))
	catalog.put(t, domainEndpoint(service, "node-west", "stable", "blue-v1", "west-stable"))
	oldWest := catalog.put(t, domainEndpoint(service, "node-west", "canary", "blue-v2", "west-canary"))
	// Rebuild west's canary under a new UID, matching a replica recreation.
	if _, err := catalog.Delete(service, "node-west", "canary", oldWest.UID, oldWest.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	catalog.put(t, domainEndpoint(service, "node-west", "canary", "blue-v2", "west-canary-2"))

	list, err := catalog.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rule := v1.Rule{
		Service: service, Stable: "blue-v1", Candidate: "blue-v2", CandidateWeight: 10,
		DiscoveryVersion: list.ResourceVersion,
		StableTargets: []v1.Target{
			{Node: "node-east", Name: "stable", UID: "east-stable", Domain: "east"},
			{Node: "node-west", Name: "stable", UID: "west-stable", Domain: "west"},
		},
		CandidateTargets: []v1.Target{
			{Node: "node-east", Name: "canary", UID: "east-canary", Domain: "east"},
			{Node: "node-west", Name: "canary", UID: "west-canary-2", Domain: "west"},
		},
		DomainRoutes: []v1.DomainRoute{{Name: "east", CandidateWeight: 10}, {Name: "west", CandidateWeight: 10}},
	}
	if err := rule.Validate(); err != nil {
		t.Fatalf("rule invalid: %v", err)
	}
	applied := false
	if err := catalog.WithReadyRule(rule, func() error { applied = true; return nil }); err != nil {
		t.Fatalf("full new generation rejected: %v", err)
	}
	if !applied {
		t.Fatal("fence did not run its mutation")
	}
	// The retired west canary must no longer satisfy the fence.
	stale := rule
	stale.CandidateTargets[1].UID = "west-canary"
	if err := catalog.WithReadyRule(stale, func() error {
		t.Fatal("stale rule reached the mutation")
		return nil
	}); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("retired UID passed the fence: %v", err)
	}
}

func (c *Catalog) put(t *testing.T, endpoint v1.ReplicaObservation) v1.ReplicaObservation {
	t.Helper()
	stored, err := c.Put(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}
