package journal

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"wavegate/internal/statefile"
	"wavegate/v1"
)

const sparseLogUID = "release-sparse"

// buildSparseLog persists a decision log of retain+folded records where
// sparseLogUID records appear every 512 sequences among records from many
// other releases, folds the first folded records into the checkpoint, and
// returns the reopened store. The sparse release ends up with one record in
// the checkpoint base and a handful of records spread across the retained
// segment.
func buildSparseLog(tb testing.TB, retain, folded int) *Store {
	tb.Helper()
	total := retain + folded
	at := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)
	states := map[string]v1.ReleaseState{}
	records := make([]v1.DecisionRecord, 0, total)
	head := ""
	other := 0
	for sequence := uint64(1); sequence <= uint64(total); sequence++ {
		uid := sparseLogUID
		if (sequence-1)%512 != 0 {
			uid = fmt.Sprintf("release-other-%03d", other%67)
			other++
		}
		state, found := states[uid]
		if !found {
			service := v1.Service{Namespace: "shop", Name: "api"}
			state = v1.ReleaseState{
				Spec: v1.ReleaseSpec{APIVersion: v1.Version, Service: service, Name: fmt.Sprintf("r%03d", len(states)), UID: uid,
					ResourceVersion: 1, Stable: "alpha", Candidate: "beta", Steps: []int{10},
					Window: v1.WindowPolicy{Seconds: 1, MinSamples: 1}, MinReady: 1},
				Status: v1.ReleaseStatus{APIVersion: v1.Version, Service: service, ReleaseUID: uid, ObservedVersion: 1, Phase: v1.Waiting, Step: -1},
			}
			states[uid] = state
		}
		kind := "ConditionChanged"
		if !found {
			kind = "ReleaseObserved"
		}
		state.Status.DecisionSequence = sequence
		record := v1.DecisionRecord{
			APIVersion: v1.Version, Sequence: sequence, At: at.Add(time.Duration(sequence) * time.Second),
			Kind: kind, Reason: "ObservedCut", State: state, PreviousDigest: head,
		}
		digest, err := digestRecord(record)
		if err != nil {
			tb.Fatal(err)
		}
		record.Digest = digest
		head = digest
		records = append(records, record)
	}
	base, err := fold(checkpoint{States: make(map[string]v1.DecisionRecord)}, records[:folded])
	if err != nil {
		tb.Fatal(err)
	}
	doc := document{APIVersion: v1.Version, Checkpoint: base, Records: records[folded:]}
	path := filepath.Join(tb.TempDir(), "decisions.v1.json")
	if _, err := statefile.Write(path, doc); err != nil {
		tb.Fatal(err)
	}
	store, err := Open(path, retain)
	if err != nil {
		tb.Fatal(err)
	}
	return store
}

func uidState(uid string) v1.ReleaseState {
	service := v1.Service{Namespace: "shop", Name: "api"}
	return v1.ReleaseState{
		Spec: v1.ReleaseSpec{APIVersion: v1.Version, Service: service, Name: uid, UID: uid,
			ResourceVersion: 1, Stable: "alpha", Candidate: "beta", Steps: []int{10},
			Window: v1.WindowPolicy{Seconds: 1, MinSamples: 1}, MinReady: 1},
		Status: v1.ReleaseStatus{APIVersion: v1.Version, Service: service, ReleaseUID: uid, ObservedVersion: 1, Phase: v1.Waiting, Step: -1},
	}
}

// Compaction lands exactly on the sparse release's previous record: its base
// sits at the checkpoint boundary and its first retained record links to it.
// The receipt must carry the same evidence before and after a restart.
func TestReceiptCompactionBoundarySurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.v1.json")
	store, err := Open(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	commit := func(store *Store, expected uint64, uid, kind string) {
		t.Helper()
		if _, err := store.Commit(expected, at.Add(time.Duration(expected+1)*time.Minute), kind, "ObservedCut", uidState(uid)); err != nil {
			t.Fatal(err)
		}
	}
	commit(store, 0, "release-other-1", "ReleaseObserved")
	commit(store, 1, sparseLogUID, "ReleaseObserved")
	commit(store, 2, sparseLogUID, "ConditionChanged")
	commit(store, 3, sparseLogUID, "ConditionChanged")
	commit(store, 4, "release-other-1", "ConditionChanged")
	commit(store, 5, "release-other-2", "ReleaseObserved")
	commit(store, 6, "release-other-2", "ConditionChanged")
	if sequence := store.CheckpointSequence(); sequence != 3 {
		t.Fatalf("checkpoint should advance to the sparse record at 3, got %d", sequence)
	}
	receipt, accessed, err := store.receipt(sparseLogUID, at)
	if err != nil {
		t.Fatal(err)
	}
	if accessed != 1 || receipt.Base == nil || receipt.Base.Sequence != 3 || receipt.CheckpointSequence != 3 ||
		receipt.Base.Digest != receipt.CheckpointHead || len(receipt.Decisions) != 1 || receipt.Decisions[0].Sequence != 4 ||
		receipt.Decisions[0].PreviousDigest != receipt.Base.Digest || receipt.LastDecision != 4 {
		t.Fatalf("boundary receipt: accessed=%d %+v", accessed, receipt)
	}
	if err := VerifyReceipt(receipt); err != nil {
		t.Fatalf("verify boundary receipt: %v", err)
	}
	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, accessed, err := reopened.receipt(sparseLogUID, at)
	if err != nil {
		t.Fatal(err)
	}
	if accessed != 1 || afterRestart.Digest != receipt.Digest || !reflect.DeepEqual(receipt, afterRestart) {
		t.Fatalf("restart changed receipt evidence: accessed=%d %+v vs %+v", accessed, receipt, afterRestart)
	}
	if err := VerifyReceipt(afterRestart); err != nil {
		t.Fatalf("verify restarted receipt: %v", err)
	}
	// Compact past the sparse release's last record: the receipt becomes
	// checkpoint-base only and must still verify identically across a restart.
	commit(reopened, 7, "release-other-1", "ConditionChanged")
	if sequence := reopened.CheckpointSequence(); sequence != 4 {
		t.Fatalf("checkpoint should advance past the sparse record to 4, got %d", sequence)
	}
	baseOnly, accessed, err := reopened.receipt(sparseLogUID, at)
	if err != nil {
		t.Fatal(err)
	}
	if accessed != 0 || baseOnly.Base == nil || baseOnly.Base.Sequence != 4 || baseOnly.CheckpointSequence != 4 ||
		len(baseOnly.Decisions) != 0 || baseOnly.Decisions == nil || baseOnly.LastDecision != 4 {
		t.Fatalf("base-only receipt: accessed=%d %+v", accessed, baseOnly)
	}
	if err := VerifyReceipt(baseOnly); err != nil {
		t.Fatalf("verify base-only receipt: %v", err)
	}
	reopenedAgain, err := Open(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, accessed, err = reopenedAgain.receipt(sparseLogUID, at)
	if err != nil {
		t.Fatal(err)
	}
	if accessed != 0 || afterRestart.Digest != baseOnly.Digest || !reflect.DeepEqual(baseOnly, afterRestart) {
		t.Fatalf("restart changed base-only receipt evidence: accessed=%d %+v vs %+v", accessed, baseOnly, afterRestart)
	}
}

// The indexed receipt must be byte-identical to an independent full scan of
// the retained segment, for every release, after every commit and a restart.
func TestReceiptIndexMatchesFullScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.v1.json")
	store, err := Open(path, 6)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	uids := make([]string, 9)
	for i := range uids {
		uids[i] = fmt.Sprintf("release-%d", i)
	}
	seen := map[string]bool{}
	check := func(store *Store) {
		t.Helper()
		for _, uid := range uids {
			if !seen[uid] {
				continue
			}
			receipt, accessed, err := store.receipt(uid, at)
			if err != nil {
				t.Fatalf("receipt %s: %v", uid, err)
			}
			if err := VerifyReceipt(receipt); err != nil {
				t.Fatalf("verify %s: %v", uid, err)
			}
			expected := Receipt{
				APIVersion: v1.Version, ReleaseUID: uid, Service: receipt.Service,
				CheckpointSequence: store.doc.Checkpoint.Sequence, CheckpointHead: store.doc.Checkpoint.Head,
				CheckpointDigest: store.doc.Checkpoint.Digest,
				Decisions:        []v1.DecisionRecord{}, LastDecision: store.states[uid].Sequence, GeneratedAt: at.UTC(),
			}
			if record, found := store.doc.Checkpoint.States[uid]; found {
				expected.Base = &record
			}
			for _, record := range store.doc.Records {
				if record.State.Spec.UID == uid {
					expected.Decisions = append(expected.Decisions, record)
				}
			}
			digest, err := receiptDigest(expected)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Digest != digest || accessed != len(expected.Decisions) || receipt.Decisions == nil {
				t.Fatalf("indexed receipt for %s diverges from full scan: accessed=%d want=%d", uid, accessed, len(expected.Decisions))
			}
		}
	}
	for i := 0; i < 60; i++ {
		uid := uids[(i*7+3)%len(uids)]
		kind := "ConditionChanged"
		if !seen[uid] {
			kind = "ReleaseObserved"
		}
		seen[uid] = true
		if _, err := store.Commit(uint64(i), at.Add(time.Duration(i+1)*time.Minute), kind, "ObservedCut", uidState(uid)); err != nil {
			t.Fatal(err)
		}
		check(store)
	}
	reopened, err := Open(path, 6)
	if err != nil {
		t.Fatal(err)
	}
	check(reopened)
}

func BenchmarkReceiptSparseRelease(b *testing.B) {
	store := buildSparseLog(b, 4096, 256)
	at := time.Date(2026, 11, 26, 1, 0, 0, 0, time.UTC)
	receipt, accessed, err := store.receipt(sparseLogUID, at)
	if err != nil {
		b.Fatal(err)
	}
	if err := VerifyReceipt(receipt); err != nil {
		b.Fatalf("verify sparse receipt: %v", err)
	}
	if receipt.Base == nil || len(receipt.Decisions) != 8 || accessed != len(receipt.Decisions) {
		b.Fatalf("sparse receipt base=%v decisions=%d accessed=%d", receipt.Base != nil, len(receipt.Decisions), accessed)
	}
	retained := len(store.doc.Records)
	b.Logf("retained log records: %d, records examined per receipt: %d", retained, accessed)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(accessed), "records/op")
	b.ReportMetric(float64(retained), "log-records")
	for i := 0; i < b.N; i++ {
		receipt, accessed, err := store.receipt(sparseLogUID, at)
		if err != nil || accessed != 8 || len(receipt.Decisions) != 8 {
			b.Fatalf("receipt: accessed=%d decisions=%d err=%v", accessed, len(receipt.Decisions), err)
		}
	}
}
