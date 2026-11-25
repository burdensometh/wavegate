package journal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"wavegate/v1"
)

func releaseFixture() v1.ReleaseState {
	service := v1.Service{Namespace: "shop", Name: "api"}
	return v1.ReleaseState{
		Spec: v1.ReleaseSpec{APIVersion: v1.Version, Service: service, Name: "main", UID: "release-1", ResourceVersion: 1,
			Stable: "alpha", Candidate: "beta", Steps: []int{10}, Window: v1.WindowPolicy{Seconds: 1, MinSamples: 1}, MinReady: 1},
		Status: v1.ReleaseStatus{APIVersion: v1.Version, Service: service, ReleaseUID: "release-1", ObservedVersion: 1, Phase: v1.Waiting, Step: -1},
	}
}

func TestCompactionRetainsRecoverableStateAndCursorFences(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.v1.json")
	store, err := Open(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	initial := releaseFixture()
	at := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	if _, err := store.Commit(0, at, "ReleaseObserved", "ObservedCut", initial); err != nil {
		t.Fatal(err)
	}
	cursor, err := store.Watch(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i < 6; i++ {
		if _, err := store.Commit(i, at.Add(time.Duration(i)*time.Second), "ConditionChanged", "ObservedCut", initial); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cursor.Next(context.Background()); !errors.Is(err, v1.ErrExpired) {
		t.Fatalf("lagged watch should expire: %v", err)
	}
	reopened, err := Open(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if page, err := reopened.Events(0, 10); !errors.Is(err, v1.ErrExpired) || page.CheckpointSequence != 4 {
		t.Fatalf("expired page: %+v %v", page, err)
	}
	page, err := reopened.Events(4, 10)
	if err != nil || page.Through != 6 || len(page.Records) != 2 {
		t.Fatalf("retained page: %+v %v", page, err)
	}
	receipt, err := reopened.Receipt(initial.Spec.UID, at)
	if err != nil {
		t.Fatal(err)
	}
	if e := VerifyReceipt(receipt); e != nil || receipt.LastDecision != 6 || receipt.Base == nil {
		t.Fatalf("receipt = %+v: %v", receipt, e)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content = bytes.Replace(content, []byte("ObservedCut"), []byte("ReplayedCut"), 1)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, 2); !errors.Is(err, v1.ErrInvalid) {
		t.Fatalf("changed checkpoint accepted: %v", err)
	}
}
