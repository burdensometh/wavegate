package metrics

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"

	"wavegate/v1"
)

// Evaluate uses [start,end) and the exact live binding identity. Samples that
// arrive after end but carry an in-window timestamp still count: the policy's
// bounded grace period delays the verdict so one-sided windows can complete.
// Before the deadline a complete pair is judged by the normal thresholds; at
// the deadline it is withdrawn safely with an explicit insufficiency reason.
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
		if sample.Service != key.Service || sample.BindingVersion != key.BindingVersion ||
			sample.At.Before(key.Start) || !sample.At.Before(key.End) {
			continue
		}
		if err := sample.Validate(); err != nil {
			return v1.WindowVerdict{}, err
		}
		sample.At = sample.At.UTC()
		if previous, duplicate := seen[sample.ID]; duplicate {
			if previous != sample {
				return v1.WindowVerdict{}, fmt.Errorf("%w: sample id %s has distinct values", v1.ErrConflict, sample.ID)
			}
			continue
		}
		if len(selected) >= MaxBucket {
			return v1.WindowVerdict{}, v1.Invalid("window sample limit")
		}
		selected = append(selected, sample)
		seen[sample.ID] = sample
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
		// The wait for late samples is bounded by the policy grace period. The
		// deadline is derived from the durable window key and policy, so a
		// restart resumes the same cutoff instead of extending it.
		if now.Before(key.End.Add(policy.Grace())) {
			result.Reason = "AwaitingLateSamples"
			return result, nil
		}
		result.Result, result.Reason = v1.Withdrawn, "InsufficientSamples"
		return result, nil
	}
	result.Result, result.Reason = compare(result.Stable, result.Candidate, policy)
	return result, nil
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
		if verdict.Reason == "WindowOpen" || verdict.Reason == "AwaitingLateSamples" {
			return nil
		}
	case v1.Withdrawn:
		// Withdrawal is only ever the bounded-grace outcome for a window that
		// never received both sides, so it is distinct from a metric failure.
		if verdict.Reason == "InsufficientSamples" {
			return nil
		}
	}
	return v1.Invalid("invalid window result or reason")
}
