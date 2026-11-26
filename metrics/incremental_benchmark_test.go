package metrics

import (
	"fmt"
	"testing"
	"time"

	"wavegate/v1"
)

// benchmarkOrder builds the in-memory append order for ten thousand samples of
// one binding, the same shape as BenchmarkAggregateWindow but expressed in the
// versioned form the incremental path consumes.
func benchmarkOrder(key v1.WindowKey) []storedSample {
	order := make([]storedSample, 10000)
	for i := range order {
		role := v1.StableRole
		if i%4 == 0 {
			role = v1.CandidateRole
		}
		value := sample(key, fmt.Sprintf("sample-%06d", i), role,
			key.Start.Add(time.Duration(i%10000)*time.Millisecond), int64(i%35), false)
		order[i] = storedSample{Version: uint64(i + 1), Sample: value}
	}
	return order
}

// BenchmarkFullWindowPoll is the baseline the task targets: every controller
// poll runs a complete evaluation, touching all ten thousand samples even when
// nothing changed.
func BenchmarkFullWindowPoll(b *testing.B) {
	key, policy := windowFixture()
	policy.MinSamples = 1000
	order := benchmarkOrder(key)
	all := make([]v1.Sample, len(order))
	for i, entry := range order {
		all[i] = entry.Sample
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verdict, err := Evaluate(key, policy, uint64(len(order)), all, key.End)
		if err != nil || verdict.Result != v1.Pass || verdict.Candidate.Count != 2500 {
			b.Fatalf("full poll: %+v %v", verdict, err)
		}
	}
}

// BenchmarkIncrementalIdlePoll measures the dominant controller pattern:
// repeated polls against an unchanged ten-thousand-sample bucket. No records
// are inspected and the cached digest, measurements, and verdict are reused,
// so an idle poll touches none of the accumulated samples.
func BenchmarkIncrementalIdlePoll(b *testing.B) {
	key, policy := windowFixture()
	policy.MinSamples = 1000
	order := benchmarkOrder(key)
	accumulator := newWindowAccumulator(key, policy)
	if _, err := accumulator.absorb(order); err != nil {
		b.Fatal(err)
	}
	now := key.End.Add(-time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inspected, err := accumulator.absorb(order)
		if err != nil || inspected != 0 {
			b.Fatalf("idle poll inspected %d: %v", inspected, err)
		}
		verdict, err := accumulator.verdict(uint64(len(order)), now)
		if err != nil || verdict.Result != v1.Pending {
			b.Fatalf("idle poll: %+v %v", verdict, err)
		}
	}
}

// BenchmarkIncrementalWindowPoll is the optimized path: ten thousand samples
// are already accumulated, and each round only eight genuinely new in-window
// arrivals are appended to the bucket tail. The window logic inspects just
// those eight records; accumulated measurements and the selected set are
// reused, with only the digest recomputed for the changed set. After measuring
// a round, the eight additions are rolled back (the ten-thousand-sample
// inventory is never copied) so the loop stays in steady state.
func BenchmarkIncrementalWindowPoll(b *testing.B) {
	key, policy := windowFixture()
	policy.MinSamples = 1000
	order := benchmarkOrder(key)
	accumulator := newWindowAccumulator(key, policy)
	if _, err := accumulator.absorb(order); err != nil {
		b.Fatal(err)
	}
	snapshot := struct {
		through   uint64
		stable    v1.Measurement
		candidate v1.Measurement
		selected  int
		dirty     bool
		digest    string
	}{
		through:   accumulator.through,
		stable:    accumulator.stable,
		candidate: accumulator.candidate,
		selected:  len(accumulator.selected),
		dirty:     accumulator.dirty,
		digest:    accumulator.digest,
	}
	now := key.End.Add(-time.Second) // keep the window open across rounds
	const additions = 8
	addedIDs := make([]string, 0, additions)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := len(order)
		addedIDs = addedIDs[:0]
		for j := 0; j < additions; j++ {
			index := base + j
			value := sample(key, fmt.Sprintf("sample-%08d-%06d", i, j), v1.StableRole,
				key.Start.Add(time.Duration(index%10000)*time.Millisecond), int64(index%35), false)
			order = append(order, storedSample{Version: uint64(index + 1), Sample: value})
			addedIDs = append(addedIDs, value.ID)
		}
		inspected, err := accumulator.absorb(order)
		if err != nil {
			b.Fatal(err)
		}
		if inspected != additions {
			b.Fatalf("round %d inspected %d, want only the %d new samples", i, inspected, additions)
		}
		verdict, err := accumulator.verdict(uint64(len(order)), now)
		if err != nil || verdict.Result != v1.Pending {
			b.Fatalf("incremental poll: %+v %v", verdict, err)
		}
		// Roll back only this round's eight additions.
		for _, id := range addedIDs {
			delete(accumulator.seen, id)
		}
		accumulator.selected = accumulator.selected[:snapshot.selected]
		accumulator.through = snapshot.through
		accumulator.stable = snapshot.stable
		accumulator.candidate = snapshot.candidate
		accumulator.dirty = snapshot.dirty
		accumulator.digest = snapshot.digest
		order = order[:base]
	}
}

// TestIncrementalHotspotUnderOneThird counts the records the window logic
// inspects per polling round with ten thousand accumulated samples and eight
// new arrivals per round. The incremental path must stay below one third of
// the full-scan baseline every round and on average.
func TestIncrementalHotspotUnderOneThird(t *testing.T) {
	key, policy := windowFixture()
	policy.MinSamples = 1000
	order := benchmarkOrder(key)
	accumulator := newWindowAccumulator(key, policy)
	if inspected, err := accumulator.absorb(order); err != nil || inspected != 10000 {
		t.Fatalf("initial build inspected %d: %v", inspected, err)
	}
	now := key.End.Add(-time.Second)
	const rounds, additions = 50, 8
	var totalFull, totalIncremental int
	for round := 0; round < rounds; round++ {
		for j := 0; j < additions; j++ {
			index := len(order)
			value := sample(key, fmt.Sprintf("extra-%08d", index), v1.StableRole,
				key.Start.Add(time.Duration(index%10000)*time.Millisecond), int64(index%35), false)
			order = append(order, storedSample{Version: uint64(index + 1), Sample: value})
		}
		inspected, err := accumulator.absorb(order)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := accumulator.verdict(uint64(len(order)), now); err != nil {
			t.Fatal(err)
		}
		fullVisits := len(order)
		totalFull += fullVisits
		totalIncremental += inspected
		if inspected*3 >= fullVisits {
			t.Fatalf("round %d hotspot %d is not under one third of full scan %d", round, inspected, fullVisits)
		}
	}
	ratio := float64(totalIncremental) / float64(totalFull)
	t.Logf("10k samples, %d additions/round over %d rounds: full visits=%d incremental visits=%d ratio=%.4f (limit 0.3333)",
		additions, rounds, totalFull, totalIncremental, ratio)
	if ratio >= 1.0/3.0 {
		t.Fatalf("average hotspot ratio %.4f is not under one third", ratio)
	}
}
