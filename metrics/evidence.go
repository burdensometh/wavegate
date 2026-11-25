package metrics

import (
	"wavegate/v1"
)

type EvidenceDocument struct {
	APIVersion string        `json:"apiVersion"`
	Samples    []v1.Sample   `json:"samples"`
	Sealed     *StoredWindow `json:"sealed,omitempty"`
}

func VerifyEvidence(document EvidenceDocument) error {
	if err := v1.CheckVersion(document.APIVersion); err != nil {
		return err
	}
	if document.Sealed == nil {
		return v1.Invalid("evidence has no sealed window")
	}
	window := document.Sealed
	verdict := window.Verdict
	if err := CheckVerdict(verdict); err != nil {
		return err
	}
	if verdict.Result == v1.Pending || window.SealedAt.Before(verdict.Key.End) {
		return v1.Invalid("evidence window is unsealed")
	}
	seen := make(map[string]bool, len(document.Samples))
	for _, sample := range document.Samples {
		if err := sample.Validate(); err != nil {
			return err
		}
		if sample.Service != verdict.Key.Service || sample.BindingVersion != verdict.Key.BindingVersion || seen[sample.ID] ||
			sample.At.Before(verdict.Key.Start) || !sample.At.Before(verdict.Key.End) {
			return v1.Invalid("sample evidence identity or duplicate")
		}
		seen[sample.ID] = true
	}
	if uint64(len(document.Samples)) > verdict.SampleVersion {
		return v1.Invalid("sample evidence exceeds verdict high water mark")
	}
	actual, err := Evaluate(verdict.Key, window.Policy, verdict.SampleVersion, document.Samples, window.SealedAt)
	if err != nil {
		return err
	}
	if actual != verdict {
		return v1.Invalid("sample evidence does not reconstruct the sealed verdict")
	}
	return nil
}
