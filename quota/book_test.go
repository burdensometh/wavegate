package quota

import (
	"testing"
	"time"

	"wavegate/v1"
)

func sharedSpec(namespace, name, uid string, units int) v1.ReleaseSpec {
	return v1.ReleaseSpec{
		APIVersion: v1.Version, Service: v1.Service{Namespace: namespace, Name: name},
		Name: name, UID: uid, Stable: "stable", Candidate: "candidate",
		Steps: []int{50}, MinReady: units,
		Window: v1.WindowPolicy{Seconds: 1, MinSamples: 1, MaxErrorBPS: 100},
	}
}

func planInstall(t *testing.T, book *Book, spec v1.ReleaseSpec, version uint64) v1.NamespaceReservation {
	t.Helper()
	reservation, reason := book.Plan(spec, version, time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC))
	if reason != "" {
		t.Fatalf("%s plan denied with %s", spec.Service, reason)
	}
	if err := book.Install(reservation); err != nil {
		t.Fatalf("%s install: %v", spec.Service, err)
	}
	return reservation
}

func TestSharedPoolCapsAcrossNamespaces(t *testing.T) {
	shared := Shared{CandidateUnits: 3, Namespaces: []string{"ns-a", "ns-b"}}
	limits := map[string]Limit{"ns-a": {Releases: 1, CandidateUnits: 5}}
	book, err := New(Limit{Releases: 10, CandidateUnits: 10}, limits, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	one := planInstall(t, book, sharedSpec("ns-a", "one", "uid-1", 2), 1)

	// The per-namespace release limit binds even though the pool has room.
	if _, reason := book.Plan(sharedSpec("ns-a", "two", "uid-2", 1), 2, time.Now()); reason != "NamespaceBudget" {
		t.Fatalf("ns-a/two reason = %q, want NamespaceBudget", reason)
	}
	planInstall(t, book, sharedSpec("ns-b", "three", "uid-3", 1), 3)

	// The pool is now full (3/3): a member in the other namespace waits.
	if _, reason := book.Plan(sharedSpec("ns-b", "four", "uid-4", 1), 4, time.Now()); reason != "SharedBudget" {
		t.Fatalf("ns-b/four reason = %q, want SharedBudget", reason)
	}
	// A non-member namespace never draws from the pool.
	planInstall(t, book, sharedSpec("ns-c", "five", "uid-5", 4), 5)

	// Only the exact live reservation returns its share.
	stale := one
	stale.Units++
	if err := book.Release(stale); err == nil {
		t.Fatal("released a reservation that does not match the live one")
	}
	if err := book.Release(one); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	if usage := book.Usage("ns-b").Shared; usage == nil || usage.CandidateUnits != 1 {
		t.Fatalf("shared usage after release = %+v", usage)
	}
	planInstall(t, book, sharedSpec("ns-b", "four", "uid-4", 1), 6)
	planInstall(t, book, sharedSpec("ns-a", "two", "uid-2", 1), 7)
}

func TestSharedUsageReportedToMembers(t *testing.T) {
	shared := Shared{CandidateUnits: 5, Namespaces: []string{"ns-a", "ns-b"}}
	book, err := New(Limit{Releases: 10, CandidateUnits: 10}, nil, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	planInstall(t, book, sharedSpec("ns-b", "two", "uid-2", 1), 1)
	planInstall(t, book, sharedSpec("ns-a", "one", "uid-1", 2), 2)
	planInstall(t, book, sharedSpec("ns-c", "three", "uid-3", 4), 3)

	usage := book.Usage("ns-a")
	if usage.Shared == nil || usage.Shared.Limit != 5 || usage.Shared.CandidateUnits != 3 {
		t.Fatalf("member shared view = %+v", usage.Shared)
	}
	if len(usage.Shared.Reservations) != 2 ||
		usage.Shared.Reservations[0].Service != (v1.Service{Namespace: "ns-a", Name: "one"}) ||
		usage.Shared.Reservations[1].Service != (v1.Service{Namespace: "ns-b", Name: "two"}) {
		t.Fatalf("shared reservations not sorted across namespaces: %+v", usage.Shared.Reservations)
	}
	if usage.CandidateUnits != 2 || usage.Releases != 1 {
		t.Fatalf("namespace usage = %+v", usage)
	}
	if outsider := book.Usage("ns-c"); outsider.Shared != nil {
		t.Fatalf("non-member saw a shared view: %+v", outsider.Shared)
	}
}

func TestSharedUsageRecoveredFromReservations(t *testing.T) {
	shared := Shared{CandidateUnits: 2, Namespaces: []string{"ns-a", "ns-b"}}
	book, err := New(Limit{Releases: 10, CandidateUnits: 10}, nil, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	kept := planInstall(t, book, sharedSpec("ns-a", "one", "uid-1", 2), 1)

	// A restart rebuilds the book from the journaled reservations only.
	restored, err := New(Limit{Releases: 10, CandidateUnits: 10}, nil, shared, []v1.NamespaceReservation{kept})
	if err != nil {
		t.Fatal(err)
	}
	if usage := restored.Usage("ns-b").Shared; usage == nil || usage.CandidateUnits != 2 || usage.Limit != 2 {
		t.Fatalf("recovered shared usage = %+v", usage)
	}
	if _, reason := restored.Plan(sharedSpec("ns-b", "two", "uid-2", 1), 2, time.Now()); reason != "SharedBudget" {
		t.Fatalf("recovered book plan reason = %q, want SharedBudget", reason)
	}
}

func TestSharedValidation(t *testing.T) {
	cases := map[string]Shared{
		"units without members": {CandidateUnits: 1},
		"members without units": {Namespaces: []string{"ns-a"}},
		"duplicate namespace":   {CandidateUnits: 2, Namespaces: []string{"ns-a", "ns-a"}},
		"invalid namespace":     {CandidateUnits: 2, Namespaces: []string{"Bad_Name"}},
		"units out of range":    {CandidateUnits: 1000001, Namespaces: []string{"ns-a"}},
	}
	for name, shared := range cases {
		if err := shared.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, shared)
		}
		if _, err := New(Limit{Releases: 1, CandidateUnits: 1}, nil, shared, nil); err == nil {
			t.Errorf("%s: book accepted %+v", name, shared)
		}
	}
	disabled := Shared{}
	if err := disabled.Validate(); err != nil {
		t.Fatalf("disabled shared quota: %v", err)
	}
}
