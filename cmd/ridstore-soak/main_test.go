package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRunRefusesExistingReportWithoutChangingIt(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.jsonl")
	if err := os.WriteFile(report, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"--dir", filepath.Join(t.TempDir(), "store"), "--report", report, "--git-commit", "test"}, &stderr)
	if status != 1 {
		t.Fatalf("status=%d stderr=%q", status, stderr.String())
	}
	got, err := os.ReadFile(report)
	if err != nil || string(got) != "keep\n" {
		t.Fatalf("report=%q error=%v", got, err)
	}
}

func TestRunRequiresPaths(t *testing.T) {
	var stderr bytes.Buffer
	if status := run(context.Background(), nil, &stderr); status != 2 {
		t.Fatalf("status=%d stderr=%q", status, stderr.String())
	}
}
