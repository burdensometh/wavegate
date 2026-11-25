package metrics

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"wavegate/v1"
)

func windowFixture() (v1.WindowKey, v1.WindowPolicy) {
	start := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	key := v1.WindowKey{Service: v1.Service{Namespace: "shop", Name: "cart"}, BindingVersion: 8, Start: start, End: start.Add(10 * time.Second)}
	policy := v1.WindowPolicy{Seconds: 10, MinSamples: 2, MaxErrorBPS: 5000, MaxErrorDeltaBPS: 5000, MaxMeanLatencyDeltaMS: 20}
	return key, policy
}

func sample(key v1.WindowKey, id, role string, at time.Time, ms int64, failed bool) v1.Sample {
	return v1.Sample{
		APIVersion: v1.Version, Service: key.Service, BindingVersion: key.BindingVersion,
		ID: id, Role: role, At: at, LatencyMS: ms, Failed: failed,
	}
}

func TestWindowBoundariesAndSealedReplay(t *testing.T) {
	key, policy := windowFixture()
	path := filepath.Join(t.TempDir(), "samples.v1.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Verdict(key, policy, key.End.Add(-time.Nanosecond))
	if err != nil || got.Result != v1.Pending || got.Reason != "WindowOpen" {
		t.Fatalf("open = %+v, %v", got, err)
	}
	got, err = store.Verdict(key, policy, key.End)
	if err != nil || got.Reason != "InsufficientSamples" {
		t.Fatalf("closed with insufficient samples = %+v, %v", got, err)
	}
	values := []v1.Sample{
		sample(key, "s0", v1.StableRole, key.Start, 10, false),
		sample(key, "s1", v1.StableRole, key.Start.Add(time.Second), 10, false),
		sample(key, "c0", v1.CandidateRole, key.End.Add(-time.Nanosecond), 25, true),
		sample(key, "c1", v1.CandidateRole, key.Start, 35, false),
		sample(key, "skip", v1.CandidateRole, key.End, 600000, true),
	}
	for _, value := range values {
		if added, err := store.Add(value); err != nil || !added {
			t.Fatalf("add %s: %v %v", value.ID, added, err)
		}
	}
	if added, err := store.Add(values[0]); err != nil || added {
		t.Fatalf("idempotence: %v %v", added, err)
	}
	collision := values[0]
	collision.LatencyMS++
	if _, err := store.Add(collision); !errors.Is(err, v1.ErrConflict) {
		t.Fatalf("conflicting sample: %v", err)
	}
	got, err = store.Verdict(key, policy, key.End)
	if err != nil || got.Result != v1.Pass || got.Candidate.Count != 2 || got.Stable.Count != 2 {
		t.Fatalf("threshold equality must pass: %+v %v", got, err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopened.Verdict(key, policy, key.End.Add(time.Minute))
	if err != nil || got != replayed {
		t.Fatalf("replay = %+v, %v", replayed, err)
	}
	if _, err := reopened.Add(sample(key, "late", v1.StableRole, key.Start, 10, false)); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("sealed mutation: %v", err)
	}
	samples, window, err := reopened.Evidence(key.Service, key.BindingVersion)
	if err != nil {
		t.Fatal(err)
	}
	document := EvidenceDocument{APIVersion: v1.Version, Samples: samples, Sealed: window}
	if len(samples) != 4 || VerifyEvidence(document) != nil {
		t.Fatalf("invalid evidence: %+v", document)
	}
	document.Samples[0].LatencyMS++
	if err := VerifyEvidence(document); err == nil {
		t.Fatal("tampered samples verified")
	}
}

func TestWindowRejectsUnrelatedSamplesAndUsesExactDelta(t *testing.T) {
	key, policy := windowFixture()
	policy.MaxErrorBPS = 10000
	policy.MaxErrorDeltaBPS = 3333
	other := key
	other.BindingVersion++
	input := []v1.Sample{
		sample(key, "s0", v1.StableRole, key.Start, 5, false),
		sample(key, "s1", v1.StableRole, key.Start, 5, false),
		sample(key, "c0", v1.CandidateRole, key.Start, 5, true),
		sample(key, "c1", v1.CandidateRole, key.Start, 5, false),
		sample(key, "c2", v1.CandidateRole, key.Start, 5, false),
		sample(other, "c0", v1.CandidateRole, key.Start, 5, false),
	}
	verdict, err := Evaluate(key, policy, uint64(len(input)), input, key.End)
	if err != nil || verdict.Result != v1.Fail || verdict.Reason != "CandidateErrorDelta" ||
		verdict.Candidate.Count != 3 || verdict.Candidate.Failed != 1 {
		t.Fatalf("exact delta = %+v, %v", verdict, err)
	}
}

func BenchmarkAggregateWindow(b *testing.B) {
	key, policy := windowFixture()
	policy.MinSamples = 1000
	input := make([]v1.Sample, 10000)
	for i := range input {
		role := v1.StableRole
		if i%4 == 0 {
			role = v1.CandidateRole
		}
		at := key.Start.Add(time.Duration(i%10000) * time.Millisecond)
		input[i] = sample(key, fmt.Sprintf("sample-%06d", i), role, at, int64(i%35), false)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verdict, err := Evaluate(key, policy, uint64(len(input)), input, key.End)
		if err != nil || verdict.Result != v1.Pass || verdict.Candidate.Count != 2500 {
			b.Fatalf("aggregate: %+v %v", verdict, err)
		}
	}
}
