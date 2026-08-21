package soak

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/verify"
)

func TestShortRunValidatesHarnessWithoutClaimingLongSoak(t *testing.T) {
	var output bytes.Buffer
	summary, err := Run(context.Background(), Options{
		Dir:                 t.TempDir() + "/store",
		Duration:            150 * time.Millisecond,
		SampleInterval:      25 * time.Millisecond,
		MaintenanceInterval: 50 * time.Millisecond,
		MaintenanceBatches:  8,
		LiveRecords:         32,
		BatchMutations:      4,
		ValueBytes:          128,
		Seed:                7,
		SegmentSize:         1 << 20,
		GitCommit:           "test",
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.CompletedNaturally || !summary.VerifiedClean || summary.Batches == 0 || summary.Mutations == 0 || summary.Samples < 2 {
		t.Fatalf("summary=%+v", summary)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != int(summary.Samples)+2 {
		t.Fatalf("JSONL lines=%d samples=%d", len(lines), summary.Samples)
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid JSONL %q: %v", line, err)
		}
	}
}

func TestRunRefusesExistingStorePath(t *testing.T) {
	dir := t.TempDir()
	_, err := Run(context.Background(), Options{Dir: dir, Duration: time.Second, GitCommit: "test"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("existing path accepted")
	}
}

func TestCanceledRunClosesStoreAndLeavesVerifiableEvidence(t *testing.T) {
	dir := t.TempDir() + "/store"
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := Run(ctx, Options{Dir: dir, Duration: time.Hour, SampleInterval: 10 * time.Millisecond, MaintenanceInterval: time.Hour, MaintenanceBatches: 1 << 20, LiveRecords: 32, BatchMutations: 4, ValueBytes: 128, SegmentSize: 1 << 20, GitCommit: "test"}, &output)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run error=%v", err)
	}
	report, verifyErr := verify.Run(context.Background(), dir)
	if verifyErr != nil || !report.Clean {
		t.Fatalf("verify=%+v error=%v", report, verifyErr)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	var failure Failure
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &failure); err != nil || failure.Type != "failure" || failure.Error == "" || failure.CompletedNaturally {
		t.Fatalf("failure=%+v decode=%v", failure, err)
	}
}
