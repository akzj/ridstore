package soak

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akzj/ridstore"
)

func TestShortRunValidatesHarnessWithoutClaimingLongSoak(t *testing.T) {
	var output bytes.Buffer
	summary, err := Run(context.Background(), testOptions(t), &output)
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
	var terminal map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &terminal); err != nil || terminal["type"] != "summary" || terminal["completed_naturally"] != true || terminal["verified_clean"] != true {
		t.Fatalf("terminal=%v decode=%v", terminal, err)
	}
}

func TestRunRefusesExistingStoreAndWritesFailure(t *testing.T) {
	var output bytes.Buffer
	opts := testOptions(t)
	opts.Dir = t.TempDir()
	_, err := Run(context.Background(), opts, &output)
	if !errors.Is(err, ridstore.ErrAlreadyExists) {
		t.Fatalf("error=%v", err)
	}
	if !strings.Contains(output.String(), `"type":"failure"`) {
		t.Fatalf("report=%q", output.String())
	}
}

func TestCanceledRunClosesStoreAndLeavesExactEvidence(t *testing.T) {
	opts := testOptions(t)
	opts.Duration = time.Hour
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	_, err := Run(ctx, opts, &output)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	report, verifyErr := ridstore.Verify(context.Background(), ridstore.VerifyConfig{Dir: opts.Dir})
	if verifyErr != nil || report.Stage != ridstore.VerifyStageExact {
		t.Fatalf("verify=%+v error=%v", report, verifyErr)
	}
	if !strings.Contains(output.String(), `"type":"failure"`) || strings.Contains(output.String(), `"completed_naturally":true`) {
		t.Fatalf("report=%q", output.String())
	}
}

func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{Dir: t.TempDir() + "/store", Duration: 200 * time.Millisecond, SampleInterval: 25 * time.Millisecond, MaintenanceInterval: 50 * time.Millisecond, MaintenanceBatches: 8, LiveRecords: 32, BatchMutations: 4, ValueBytes: 128, Seed: 7, SegmentSize: 1 << 20, GitCommit: "test"}
}
