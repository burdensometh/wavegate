package metrics

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"

	"wavegate/v1"
)

// Evaluate uses [start,end) and the exact live binding identity. Once time has
// elapsed, insufficient samples keep the same window open to late arrivals.
func Evaluate(key v1.WindowKey, policy v1.WindowPolicy, version uint64, all []v1.Sample, now time.Time) (v1.WindowVerdict, error) {
	if err := key.Validate(); err != nil {
		return v1.WindowVerdict{}, err
	}
	if err := policy.Validate(); err != nil {
		return v1.WindowVerdict{}, err
	}
	if policy.Duration() != key.End.Sub(key.Start) {
		return v1.WindowVerdict{}, v1.Invalid("window and policy durations disagree")
	}
	selected := make([]v1.Sample, 0)
	seen := make(map[string]v1.Sample)
	result := v1.WindowVerdict{
		APIVersion: v1.Version, Key: key, SampleVersion: version, Result: v1.Pending, Reason: "WindowOpen",
	}
	for _, sample := range all {
		accepted, normalized, err := considerSample(key, seen, sample)
		if err != nil {
			return v1.WindowVerdict{}, err
		}
		if !accepted {
			continue
		}
		if len(selected) >= MaxBucket {
			return v1.WindowVerdict{}, v1.Invalid("window sample limit")
		}
		selected = append(selected, normalized)
		applyMeasurement(&result, normalized)
	}
	slices.SortFunc(selected, func(a, b v1.Sample) int { return cmp.Compare(a.ID, b.ID) })
	digest, err := v1.Digest(selected)
	if err != nil {
		return v1.WindowVerdict{}, err
	}
	result.SampleDigest = digest
	if now.Before(key.End) {
		return result, nil
	}
	if result.Candidate.Count < policy.MinSamples || result.Stable.Count < policy.MinSamples {
		result.Reason = "InsufficientSamples"
		return result, nil
	}
	result.Result, result.Reason = compare(result.Stable, result.Candidate, policy)
	return result, nil
}

// considerSample applies the exact per-record window rules shared by full and
// incremental evaluation: the half-open [start,end) boundary, identity
// filtering, validation, UTC normalization, and duplicate/conflict detection.
// accepted is false when the sample falls outside the window or repeats an
// already-seen identical id. A repeated id with distinct values is a conflict.
func considerSample(key v1.WindowKey, seen map[string]v1.Sample, sample v1.Sample) (accepted bool, normalized v1.Sample, err error) {
	if sample.Service != key.Service || sample.BindingVersion != key.BindingVersion ||
		sample.At.Before(key.Start) || !sample.At.Before(key.End) {
		return false, v1.Sample{}, nil
	}
	if err := sample.Validate(); err != nil {
		return false, v1.Sample{}, err
	}
	sample.At = sample.At.UTC()
	if previous, duplicate := seen[sample.ID]; duplicate {
		if previous != sample {
			return false, v1.Sample{}, fmt.Errorf("%w: sample id %s has distinct values", v1.ErrConflict, sample.ID)
		}
		return false, v1.Sample{}, nil
	}
	return true, sample, nil
}

func applyMeasurement(result *v1.WindowVerdict, sample v1.Sample) {
	measurement := &result.Stable
	if sample.Role == v1.CandidateRole {
		measurement = &result.Candidate
	}
	measurement.Count++
	if sample.Failed {
		measurement.Failed++
	}
	measurement.TotalLatencyMS += sample.LatencyMS
}

func compare(stable, candidate v1.Measurement, policy v1.WindowPolicy) (string, string) {
	stableCount, candidateCount := int64(stable.Count), int64(candidate.Count)
	stableFailed, candidateFailed := int64(stable.Failed), int64(candidate.Failed)
	denominator := stableCount * candidateCount
	if candidateFailed*10000 > int64(policy.MaxErrorBPS)*candidateCount {
		return v1.Fail, "CandidateErrorRate"
	}
	if (candidateFailed*stableCount-stableFailed*candidateCount)*10000 >
		int64(policy.MaxErrorDeltaBPS)*denominator {
		return v1.Fail, "CandidateErrorDelta"
	}
	if candidate.TotalLatencyMS*stableCount-stable.TotalLatencyMS*candidateCount >
		policy.MaxMeanLatencyDeltaMS*denominator {
		return v1.Fail, "CandidateLatencyDelta"
	}
	return v1.Pass, "WithinPolicy"
}

func CheckVerdict(verdict v1.WindowVerdict) error {
	if err := v1.CheckVersion(verdict.APIVersion); err != nil {
		return err
	}
	if err := verdict.Key.Validate(); err != nil {
		return err
	}
	for _, group := range []v1.Measurement{verdict.Stable, verdict.Candidate} {
		if group.Count < 0 || group.Count > MaxBucket || group.Failed < 0 || group.Failed > group.Count ||
			group.TotalLatencyMS < 0 || group.TotalLatencyMS > int64(group.Count)*600000 {
			return v1.Invalid("invalid window measurement")
		}
	}
	if verdict.Stable.Count > math.MaxInt-verdict.Candidate.Count ||
		verdict.Stable.Count+verdict.Candidate.Count > MaxBucket ||
		uint64(verdict.Stable.Count+verdict.Candidate.Count) > verdict.SampleVersion {
		return v1.Invalid("invalid total window count")
	}
	if !v1.ValidDigest(verdict.SampleDigest) {
		return v1.Invalid("window digest")
	}
	switch verdict.Result {
	case v1.Pass:
		if verdict.Reason == "WithinPolicy" && verdict.Candidate.Count > 0 && verdict.Stable.Count > 0 {
			return nil
		}
	case v1.Fail:
		switch verdict.Reason {
		case "CandidateErrorRate", "CandidateErrorDelta", "CandidateLatencyDelta":
			if verdict.Candidate.Count > 0 && verdict.Stable.Count > 0 {
				return nil
			}
		}
	case v1.Pending:
		if verdict.Reason == "WindowOpen" || verdict.Reason == "InsufficientSamples" {
			return nil
		}
	}
	return v1.Invalid("invalid window result or reason")
}
