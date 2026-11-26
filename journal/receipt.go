package journal

import (
	"fmt"
	"time"

	"wavegate/v1"
)

type Receipt struct {
	APIVersion         string              `json:"apiVersion"`
	ReleaseUID         string              `json:"releaseUid"`
	Service            v1.Service          `json:"service"`
	CheckpointSequence uint64              `json:"checkpointSequence"`
	CheckpointHead     string              `json:"checkpointHead,omitempty"`
	CheckpointDigest   string              `json:"checkpointDigest"`
	Base               *v1.DecisionRecord  `json:"base,omitempty"`
	Decisions          []v1.DecisionRecord `json:"decisions"`
	LastDecision       uint64              `json:"lastDecision"`
	GeneratedAt        time.Time           `json:"generatedAt"`
	Digest             string              `json:"digest"`
}

func receiptDigest(receipt Receipt) (string, error) {
	receipt.Digest = ""
	return v1.Digest(receipt)
}

func (s *Store) Receipt(uid string, at time.Time) (Receipt, error) {
	receipt, _, err := s.receipt(uid, at)
	return receipt, err
}

// receipt builds the release receipt from the per-release retained index and
// reports how many retained decision records were examined to produce it.
func (s *Store) receipt(uid string, at time.Time) (Receipt, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.uncertain != nil {
		return Receipt{}, 0, s.uncertain
	}
	current, exists := s.states[uid]
	if !exists {
		return Receipt{}, 0, v1.ErrNotFound
	}
	base := s.doc.Checkpoint
	result := Receipt{
		APIVersion: v1.Version, ReleaseUID: uid, Service: current.State.Spec.Service,
		CheckpointSequence: base.Sequence, CheckpointHead: base.Head, CheckpointDigest: base.Digest,
		Decisions: []v1.DecisionRecord{}, LastDecision: current.Sequence, GeneratedAt: at.UTC(),
	}
	if record, found := base.States[uid]; found {
		result.Base = &record
	}
	accessed := 0
	for _, sequence := range s.retained[uid] {
		result.Decisions = append(result.Decisions, s.doc.Records[int(sequence-base.Sequence)-1])
		accessed++
	}
	digest, err := receiptDigest(result)
	if err != nil {
		return Receipt{}, 0, err
	}
	result.Digest = digest
	copied, err := v1.Copy(result)
	if err != nil {
		return Receipt{}, 0, err
	}
	return copied, accessed, nil
}

func VerifyReceipt(receipt Receipt) error {
	if err := v1.CheckVersion(receipt.APIVersion); err != nil {
		return err
	}
	if err := receipt.Service.Validate(); err != nil {
		return err
	}
	if !v1.ValidToken(receipt.ReleaseUID) || !v1.ValidDigest(receipt.CheckpointDigest) || receipt.GeneratedAt.IsZero() ||
		(receipt.CheckpointSequence == 0 && receipt.CheckpointHead != "") ||
		(receipt.CheckpointSequence > 0 && !v1.ValidDigest(receipt.CheckpointHead)) {
		return v1.Invalid("receipt header")
	}
	digest, err := receiptDigest(receipt)
	if err != nil {
		return err
	}
	if digest != receipt.Digest {
		return v1.Invalid("receipt digest")
	}
	var previous v1.DecisionRecord
	checkIdentity := func(record v1.DecisionRecord) error {
		if record.State.Spec.UID != receipt.ReleaseUID || record.State.Spec.Service != receipt.Service {
			return v1.Invalid("receipt record identity")
		}
		return validateRecord(record)
	}
	if receipt.Base != nil {
		previous = *receipt.Base
		if previous.Sequence > receipt.CheckpointSequence {
			return v1.Invalid("receipt base beyond checkpoint")
		}
		if err := checkIdentity(previous); err != nil {
			return fmt.Errorf("receipt base: %w", err)
		}
	}
	for _, record := range receipt.Decisions {
		if record.Sequence <= max(previous.Sequence, receipt.CheckpointSequence) ||
			(previous.Sequence > 0 && record.Sequence == previous.Sequence+1 && record.PreviousDigest != previous.Digest) {
			return v1.Invalid("receipt decision order or contiguous link")
		}
		if err := checkIdentity(record); err != nil {
			return err
		}
		if err := validateTransition(previous, record); err != nil {
			return err
		}
		previous = record
	}
	if previous.Sequence == 0 || previous.Sequence != receipt.LastDecision {
		return v1.Invalid("receipt last decision")
	}
	return nil
}
