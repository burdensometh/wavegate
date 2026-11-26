package metrics

import (
	"cmp"
	"slices"
	"time"

	"wavegate/v1"
)

// windowAccumulator keeps the live state of one unsealed binding's window so
// repeated controller polls only inspect samples added since the previous
// poll. The accumulator is purely in-memory: the persisted document keeps the
// complete sample inventory, and reopening a store rebuilds accumulators from
// that inventory. Every per-record decision goes through considerSample, the
// same predicate used by Evaluate, so half-open [start,end) boundaries,
// duplicates, conflicting ids, and late arrivals are handled identically to a
// full evaluation.
type windowAccumulator struct {
	key       v1.WindowKey
	policy    v1.WindowPolicy
	seen      map[string]v1.Sample
	selected  []v1.Sample
	stable    v1.Measurement
	candidate v1.Measurement
	// through is the highest sample resourceVersion already absorbed.
	through uint64
	// digest caches the digest of selected and is recomputed only when the
	// selected set changed during an absorb.
	dirty  bool
	digest string
}

func newWindowAccumulator(key v1.WindowKey, policy v1.WindowPolicy) *windowAccumulator {
	return &windowAccumulator{
		key:    key,
		policy: policy,
		seen:   make(map[string]v1.Sample),
		dirty:  true,
	}
}

// absorb folds every entry in order with resourceVersion greater than through.
// Entries outside the window still advance through, matching a full scan that
// visits them but filters them out. It returns the number of records actually
// inspected, which is the incremental hotspot size.
func (a *windowAccumulator) absorb(order []storedSample) (int, error) {
	start, _ := slices.BinarySearchFunc(order, a.through+1, func(entry storedSample, target uint64) int {
		if entry.Version < target {
			return -1
		}
		return 1
	})
	inspected := 0
	changed := false
	for i := start; i < len(order); i++ {
		entry := order[i]
		if entry.Version <= a.through {
			continue
		}
		inspected++
		accepted, normalized, err := considerSample(a.key, a.seen, entry.Sample)
		if err != nil {
			return inspected, err
		}
		a.through = entry.Version
		if !accepted {
			continue
		}
		if len(a.selected) >= MaxBucket {
			return inspected, v1.Invalid("window sample limit")
		}
		a.seen[normalized.ID] = normalized
		// Append unsorted, exactly as Evaluate does; ordering happens once when
		// the digest is recomputed, not once per inserted record.
		a.selected = append(a.selected, normalized)
		measurement := &a.stable
		if normalized.Role == v1.CandidateRole {
			measurement = &a.candidate
		}
		measurement.Count++
		if normalized.Failed {
			measurement.Failed++
		}
		measurement.TotalLatencyMS += normalized.LatencyMS
		changed = true
	}
	// Only new in-window samples change the selected set and its digest; a poll
	// that merely re-inspects duplicate or out-of-window records reuses the
	// cached digest, so idle polling allocates nothing.
	a.dirty = a.dirty || changed
	return inspected, nil
}

// verdict produces the same WindowVerdict as Evaluate would at now.
func (a *windowAccumulator) verdict(sampleVersion uint64, now time.Time) (v1.WindowVerdict, error) {
	if a.dirty {
		// Same ordering and digest input as the full evaluator, so the sealed
		// SampleDigest is identical record for record.
		slices.SortFunc(a.selected, func(x, y v1.Sample) int { return cmp.Compare(x.ID, y.ID) })
		digest, err := v1.Digest(a.selected)
		if err != nil {
			return v1.WindowVerdict{}, err
		}
		a.digest = digest
		a.dirty = false
	}
	result := v1.WindowVerdict{
		APIVersion:    v1.Version,
		Key:           a.key,
		SampleVersion: sampleVersion,
		SampleDigest:  a.digest,
		Stable:        a.stable,
		Candidate:     a.candidate,
		Result:        v1.Pending,
		Reason:        "WindowOpen",
	}
	if now.Before(a.key.End) {
		return result, nil
	}
	if result.Candidate.Count < a.policy.MinSamples || result.Stable.Count < a.policy.MinSamples {
		result.Reason = "InsufficientSamples"
		return result, nil
	}
	result.Result, result.Reason = compare(result.Stable, result.Candidate, a.policy)
	return result, nil
}
