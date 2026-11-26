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
	policy := v1.WindowPolicy{Seconds: 10, GraceSeconds: 30, MinSamples: 2, MaxErrorBPS: 5000, MaxErrorDeltaBPS: 5000, MaxMeanLatencyDeltaMS: 20}
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
	if err != nil || got.Result != v1.Pending || got.Reason != "AwaitingLateSamples" {
		t.Fatalf("closed but still waiting for late samples = %+v, %v", got, err)
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

// A window that is missing one side keeps waiting only until the bounded
// deadline. Late samples with an in-window timestamp still decide it; if the
// pair completes before the deadline the normal thresholds apply.
func TestLateSamplesCompleteWithinBoundBeforeDeadline(t *testing.T) {
	key, policy := windowFixture()
	oneSided := []v1.Sample{
		sample(key, "s0", v1.StableRole, key.Start, 10, false),
		sample(key, "s1", v1.StableRole, key.Start.Add(time.Second), 10, false),
	}
	verdict, err := Evaluate(key, policy, 2, oneSided, key.End.Add(time.Second))
	if err != nil || verdict.Result != v1.Pending || verdict.Reason != "AwaitingLateSamples" {
		t.Fatalf("mid-grace = %+v, %v", verdict, err)
	}
	// A sample that carries an in-window timestamp but is ingested after the
	// window closed still belongs to [start,end) and settles the pair.
	late := append(append([]v1.Sample{}, oneSided...),
		sample(key, "c0", v1.CandidateRole, key.Start.Add(5*time.Second), 10, false),
		sample(key, "c1", v1.CandidateRole, key.Start.Add(6*time.Second), 10, false),
	)
	verdict, err = Evaluate(key, policy, 4, late, key.End.Add(time.Duration(policy.GraceSeconds-1)*time.Second))
	if err != nil || verdict.Result != v1.Pass || verdict.Reason != "WithinPolicy" {
		t.Fatalf("late pair must pass by the normal thresholds = %+v, %v", verdict, err)
	}
}

// At the deadline an incomplete pair is withdrawn, not failed: the receipt can
// distinguish an insufficient-sample withdrawal from a metric failure.
func TestMissingSideIsWithdrawnAtDeadlineNotFailed(t *testing.T) {
	key, policy := windowFixture()
	oneSided := []v1.Sample{
		sample(key, "s0", v1.StableRole, key.Start, 10, false),
		sample(key, "s1", v1.StableRole, key.Start.Add(time.Second), 10, false),
	}
	deadline := key.End.Add(policy.Grace())
	verdict, err := Evaluate(key, policy, 2, oneSided, deadline.Add(-time.Nanosecond))
	if err != nil || verdict.Result != v1.Pending || verdict.Reason != "AwaitingLateSamples" {
		t.Fatalf("before deadline = %+v, %v", verdict, err)
	}
	withdrawn, err := Evaluate(key, policy, 2, oneSided, deadline)
	if err != nil || withdrawn.Result != v1.Withdrawn || withdrawn.Reason != "InsufficientSamples" {
		t.Fatalf("at deadline = %+v, %v", withdrawn, err)
	}
	if err := CheckVerdict(withdrawn); err != nil {
		t.Fatalf("withdrawn verdict rejected: %v", err)
	}
	// A metric failure carries a threshold reason and must remain distinguishable.
	failed, err := Evaluate(key, policy, 4, []v1.Sample{
		sample(key, "s0", v1.StableRole, key.Start, 5, false),
		sample(key, "s1", v1.StableRole, key.Start, 5, false),
		sample(key, "c0", v1.CandidateRole, key.Start, 5, true),
		sample(key, "c1", v1.CandidateRole, key.Start, 5, true),
	}, deadline)
	if err != nil || failed.Result != v1.Fail || failed.Reason == withdrawn.Reason {
		t.Fatalf("metric failure must be distinct from withdrawal = %+v, %v", failed, err)
	}
}

// The deadline comes from the durable window plus policy, so reopening the
// store (a restart) resumes the same cutoff instead of granting a new grace.
func TestRestartKeepsOriginalDeadline(t *testing.T) {
	key, policy := windowFixture()
	path := filepath.Join(t.TempDir(), "samples.v1.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []v1.Sample{
		sample(key, "s0", v1.StableRole, key.Start, 10, false),
		sample(key, "s1", v1.StableRole, key.Start.Add(time.Second), 10, false),
	} {
		if added, err := store.Add(value); err != nil || !added {
			t.Fatalf("add %s: %v %v", value.ID, added, err)
		}
	}
	deadline := key.End.Add(policy.Grace())
	if got, err := store.Verdict(key, policy, deadline.Add(-time.Nanosecond)); err != nil || got.Result != v1.Pending {
		t.Fatalf("first process before deadline = %+v, %v", got, err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Verdict(key, policy, deadline.Add(-time.Nanosecond)); err != nil || got.Result != v1.Pending {
		t.Fatalf("restart extended the deadline = %+v, %v", got, err)
	}
	got, err := reopened.Verdict(key, policy, deadline)
	if err != nil || got.Result != v1.Withdrawn || got.Reason != "InsufficientSamples" {
		t.Fatalf("restart ignored the original deadline = %+v, %v", got, err)
	}
	// The withdrawal is sealed durably and replays from its samples.
	again, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := again.Verdict(key, policy, deadline.Add(time.Hour))
	if err != nil || replayed != got {
		t.Fatalf("sealed withdrawal did not replay = %+v, %v", replayed, err)
	}
	samples, window, err := again.Evidence(key.Service, key.BindingVersion)
	if err != nil || window == nil {
		t.Fatalf("withdrawal evidence = %+v %v", window, err)
	}
	if err := VerifyEvidence(EvidenceDocument{APIVersion: v1.Version, Samples: samples, Sealed: window}); err != nil {
		t.Fatalf("withdrawal evidence does not verify: %v", err)
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
