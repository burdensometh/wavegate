package metrics

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"wavegate/v1"
)

// TestIncrementalMatchesFullEvaluation feeds the same stream through the
// incremental store path and the reference Evaluate function after every poll,
// exercising half-open boundaries, duplicate ids, conflicting ids, late
// arrivals, and samples outside the binding identity. The two must agree
// record by record at every point in time.
func TestIncrementalMatchesFullEvaluation(t *testing.T) {
	key, policy := windowFixture()
	store, err := Open(filepath.Join(t.TempDir(), "samples.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(20261126))
	var stream []v1.Sample
	nextID := 0
	makeSample := func() v1.Sample {
		role := v1.StableRole
		if rng.Intn(4) == 0 {
			role = v1.CandidateRole
		}
		// Spread across the boundary: some before start, some at/after end.
		at := key.Start.Add(time.Duration(rng.Intn(int(2*policy.Duration())) - int(policy.Duration())))
		nextID++
		return sample(key, fmt.Sprintf("s-%05d", nextID), role, at, int64(rng.Intn(40)), rng.Intn(8) == 0)
	}
	openNow := key.End.Add(-time.Nanosecond)
	for round := 0; round < 60; round++ {
		for batch := 0; batch < 5; batch++ {
			value := makeSample()
			added, err := store.Add(value)
			if err != nil {
				t.Fatalf("round %d add %s: %v", round, value.ID, err)
			}
			if added {
				stream = append(stream, value)
			}
			// Every fifth new sample is delivered a second time: an exact
			// duplicate must be ignored and a mutated one must conflict.
			if batch == 4 && added {
				if repeated, err := store.Add(value); err != nil || repeated {
					t.Fatalf("duplicate replay: %v %v", repeated, err)
				}
				mutated := value
				mutated.LatencyMS++
				if _, err := store.Add(mutated); err == nil {
					t.Fatal("conflicting duplicate id was accepted")
				}
			}
		}
		// Samples for a different binding identity must never enter the window.
		other := makeSample()
		other.BindingVersion++
		if _, err := store.Add(other); err != nil {
			t.Fatal(err)
		}
		// Poll while the window stays open so the accumulator remains live.
		got, err := store.Verdict(key, policy, openNow)
		if err != nil {
			t.Fatalf("round %d verdict: %v", round, err)
		}
		want, err := Evaluate(key, policy, store.doc.Version, stream, openNow)
		if err != nil {
			t.Fatalf("round %d reference: %v", round, err)
		}
		if got != want {
			t.Fatalf("round %d open poll:\n incremental %+v\n full        %+v", round, got, want)
		}
	}
	// Closing the window seals it. The sealed verdict must equal a fresh full
	// evaluation over the same raw samples, including the closed-window reason.
	want, err := Evaluate(key, policy, store.doc.Version, stream, key.End)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := store.Verdict(key, policy, key.End)
	if err != nil {
		t.Fatal(err)
	}
	if sealed != want {
		t.Fatalf("sealed verdict:\n incremental %+v\n full        %+v", sealed, want)
	}
}

// TestIncrementalBoundaryAndLateSamples pins the exact [start,end) semantics
// and late-arrival handling on the incremental path against Evaluate.
func TestIncrementalBoundaryAndLateSamples(t *testing.T) {
	key, policy := windowFixture()
	store, err := Open(filepath.Join(t.TempDir(), "samples.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	values := []v1.Sample{
		sample(key, "at-start", v1.StableRole, key.Start, 10, false),
		sample(key, "at-start-2", v1.StableRole, key.Start.Add(time.Millisecond), 12, false),
		sample(key, "before-start", v1.StableRole, key.Start.Add(-time.Nanosecond), 10, false),
		sample(key, "at-end", v1.CandidateRole, key.End, 10, false),
		sample(key, "before-end", v1.CandidateRole, key.End.Add(-time.Nanosecond), 10, false),
		sample(key, "late-open", v1.CandidateRole, key.Start, 10, false),
	}
	// Poll while open with no samples, then stream arrivals one at a time with
	// a poll between each, including a late sample while the window is open.
	if _, err := store.Verdict(key, policy, key.Start); err != nil {
		t.Fatal(err)
	}
	var included []v1.Sample
	for index, value := range values {
		if added, err := store.Add(value); err != nil || !added {
			t.Fatalf("add %s: %v %v", value.ID, added, err)
		}
		included = append(included, value)
		now := key.End.Add(-time.Nanosecond)
		got, err := store.Verdict(key, policy, now)
		if err != nil {
			t.Fatal(err)
		}
		want, err := Evaluate(key, policy, uint64(index+1), included, now)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("open poll %d: %+v != %+v", index, got, want)
		}
	}
	// Closing seals the window; subsequent late samples are rejected and the
	// sealed verdict stays identical after a restart.
	sealed, err := store.Verdict(key, policy, key.End)
	if err != nil || sealed.Result == v1.Pending {
		t.Fatalf("seal: %+v %v", sealed, err)
	}
	reopened, err := Open(store.path)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.Verdict(key, policy, key.End.Add(time.Hour))
	if err != nil || again != sealed {
		t.Fatalf("restart changed sealed verdict: %+v %v", again, err)
	}
	late := sample(key, "after-seal", v1.StableRole, key.Start, 10, false)
	if _, err := reopened.Add(late); err == nil {
		t.Fatal("late sample accepted into a sealed binding")
	}
}

// TestIncrementalSealAndRestartConsistency drives a window to a terminal
// verdict through repeated polling and proves the sealed document survives a
// restart with identical measurements, digest, reason, and sample version.
func TestIncrementalSealAndRestartConsistency(t *testing.T) {
	key, policy := windowFixture()
	policy.MinSamples = 50
	store, err := Open(filepath.Join(t.TempDir(), "samples.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	const total = 300
	for i := 0; i < total; i++ {
		role := v1.StableRole
		if i%3 == 0 {
			role = v1.CandidateRole
		}
		value := sample(key, fmt.Sprintf("bulk-%05d", i), role,
			key.Start.Add(time.Duration(i%9000)*time.Millisecond), int64(i%25), i%37 == 0)
		if _, err := store.Add(value); err != nil {
			t.Fatal(err)
		}
		// Poll repeatedly while the window stays open: each poll must remain
		// consistent with a fresh full evaluation of the same raw samples.
		if i%50 == 0 {
			got, err := store.Verdict(key, policy, key.End.Add(-time.Second))
			if err != nil {
				t.Fatal(err)
			}
			bucket := store.doc.Buckets[bucketKey(key.Service, key.BindingVersion)]
			want, err := Evaluate(key, policy, store.doc.Version, materialize(bucket, store.doc.Version), key.End.Add(-time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("open poll at %d diverged", i)
			}
		}
	}
	sealed, err := store.Verdict(key, policy, key.End)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.Result == v1.Pending {
		t.Fatalf("window did not seal: %+v", sealed)
	}
	reopened, err := Open(store.path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopened.Verdict(key, policy, key.End.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if replayed != sealed {
		t.Fatalf("restart divergence:\n before %+v\n after  %+v", sealed, replayed)
	}
	samples, window, err := reopened.Evidence(key.Service, key.BindingVersion)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyEvidence(EvidenceDocument{APIVersion: v1.Version, Samples: samples, Sealed: window}); err != nil {
		t.Fatalf("evidence no longer reconstructs the seal: %v", err)
	}
}
