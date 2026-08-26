package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	publicCrashHelperEnv = "RIDSTORE_PUBLIC_CRASH_HELPER"
	publicCrashRootEnv   = "RIDSTORE_PUBLIC_CRASH_ROOT"
	publicCrashResultEnv = "RIDSTORE_PUBLIC_CRASH_RESULT"
	publicCrashPhaseEnv  = "RIDSTORE_PUBLIC_CRASH_PHASE"
)

type publicCrashResult struct {
	id        ID
	batchID   BatchID
	commitSeq CommitSeq
}

func TestPublicRecoveryAcrossProcessExit(t *testing.T) {
	for _, phase := range []string{"uncommitted", "checkpoint-open", "committed", "checkpoint-committed"} {
		t.Run(phase, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "store")
			resultPath := filepath.Join(parent, "result")
			command := exec.Command(os.Args[0], "-test.run=^TestPublicCrashHelper$")
			command.Env = append(os.Environ(),
				publicCrashHelperEnv+"=1",
				publicCrashRootEnv+"="+root,
				publicCrashResultEnv+"="+resultPath,
				publicCrashPhaseEnv+"="+phase,
			)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("helper: %v\n%s", err, output)
			}
			crashed := readPublicCrashResult(t, resultPath)
			config := testCreateConfig(root)
			store, err := Open(context.Background(), OpenConfig{Dir: root, Runtime: config.Runtime})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			record, getErr := store.Get(context.Background(), crashed.id)
			status, statusErr := store.Status(context.Background(), crashed.batchID)
			switch phase {
			case "uncommitted", "checkpoint-open":
				if !errors.Is(getErr, ErrNotFound) {
					t.Fatalf("phase=%s record=%+v get err=%v", phase, record, getErr)
				}
				if statusErr != nil || status.State != BatchStateAborted || status.CommitSeq != 0 {
					t.Fatalf("phase=%s status=%+v err=%v", phase, status, statusErr)
				}
			case "committed":
				if getErr != nil || string(record.Value) != phase {
					t.Fatalf("phase=%s record=%q err=%v", phase, record.Value, getErr)
				}
				if statusErr != nil || status.State != BatchStateCommitted || status.CommitSeq != crashed.commitSeq {
					t.Fatalf("phase=%s status=%+v err=%v", phase, status, statusErr)
				}
			case "checkpoint-committed":
				if getErr != nil || string(record.Value) != phase {
					t.Fatalf("phase=%s record=%q err=%v", phase, record.Value, getErr)
				}
				if !errors.Is(statusErr, ErrStatusExpired) {
					t.Fatalf("phase=%s status=%+v err=%v", phase, status, statusErr)
				}
			default:
				t.Fatalf("unknown phase %q", phase)
			}
		})
	}
}

func TestPublicCrashHelper(t *testing.T) {
	if os.Getenv(publicCrashHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	phase := os.Getenv(publicCrashPhaseEnv)
	config := testCreateConfig(os.Getenv(publicCrashRootEnv))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(ctx, []byte(phase))
	if err != nil {
		t.Fatal(err)
	}
	result := publicCrashResult{id: id, batchID: batch.ID()}
	switch phase {
	case "uncommitted":
	case "checkpoint-open":
		if err := store.Checkpoint(ctx); err != nil {
			t.Fatal(err)
		}
	case "committed", "checkpoint-committed":
		commit, err := batch.Commit(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result.commitSeq = commit.CommitSeq
		if phase == "checkpoint-committed" {
			if err := store.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
		}
	default:
		t.Fatalf("unknown phase %q", phase)
	}
	writePublicCrashResult(t, os.Getenv(publicCrashResultEnv), result)
	os.Exit(0)
}

func writePublicCrashResult(t *testing.T, path string, result publicCrashResult) {
	t.Helper()
	value := fmt.Sprintf("%d %d %d\n", result.id, result.batchID, result.commitSeq)
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readPublicCrashResult(t *testing.T, path string) publicCrashResult {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result publicCrashResult
	if count, err := fmt.Sscanf(string(value), "%d %d %d", &result.id, &result.batchID, &result.commitSeq); err != nil || count != 3 {
		t.Fatalf("result=%q count=%d err=%v", value, count, err)
	}
	return result
}
