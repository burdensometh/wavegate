package demo

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"wavegate/v1"
)

func TestDemoProgressAbortResumeAndIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out := filepath.Join(t.TempDir(), "run")
	if err := Run(ctx, "../samples/v1/demo-input.json", out, io.Discard); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(out, "report.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var report report
	if err := v1.Decode(f, &report); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"progress/promoted": false, "abort/metrics-rollback": false, "abort/manual-rollback": false,
		"resume/recovered-unacknowledged-apply": false, "isolation/queued-and-admitted": false,
	}
	for _, observation := range report.Observations {
		key := observation.Flow + "/" + observation.Phase
		if _, wanted := want[key]; wanted {
			want[key] = true
		}
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("demo omitted %s", key)
		}
	}
}
