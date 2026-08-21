package backup_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/backup"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/verify"
)

const (
	offlineCrashChildEnv    = "RIDSTORE_OFFLINE_CRASH_CHILD"
	offlineCrashKindEnv     = "RIDSTORE_OFFLINE_CRASH_KIND"
	offlineCrashSourceEnv   = "RIDSTORE_OFFLINE_CRASH_SOURCE"
	offlineCrashArtifactEnv = "RIDSTORE_OFFLINE_CRASH_ARTIFACT"
	offlineCrashDestEnv     = "RIDSTORE_OFFLINE_CRASH_DEST"
	offlineCrashPointEnv    = "RIDSTORE_OFFLINE_CRASH_POINT"
)

func TestBackupProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		backup.PointBackupPrepared, backup.PointBackupFilesCopied, backup.PointBackupPayloadVerified,
		backup.PointBackupMetadataSynced, backup.PointBackupMarkerRemoved, backup.PointBackupPublished,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			source, _, _ := createStoreWithRecord(t, context.Background())
			artifact := filepath.Join(t.TempDir(), "backup")
			killOfflineChild(t, "backup", source, artifact, "", point)
			report, err := verify.Run(context.Background(), source)
			if err != nil || !report.Clean {
				t.Fatalf("source verify=%+v error=%v", report, err)
			}
			published := point == backup.PointBackupMarkerRemoved || point == backup.PointBackupPublished
			_, err = backup.Inspect(context.Background(), artifact)
			if published && err != nil {
				t.Fatalf("published artifact: %v", err)
			}
			if !published && !errors.Is(err, base.ErrRecoveryRequired) {
				t.Fatalf("incomplete artifact error=%v", err)
			}
		})
	}
}

func TestRestoreProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		backup.PointRestorePrepared, backup.PointRestoreFilesCopied, backup.PointRestoreUUIDRewritten,
		backup.PointRestorePayloadVerified, backup.PointRestorePayloadPublished, backup.PointRestoreLayoutVerified,
		backup.PointRestoreMarkerRemoved, backup.PointRestorePublished,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			_, artifact, id, want := createBackupWithRecord(t, context.Background())
			destination := filepath.Join(t.TempDir(), "restored")
			killOfflineChild(t, "restore", "", artifact, destination, point)
			if _, err := backup.Inspect(context.Background(), artifact); err != nil {
				t.Fatalf("source artifact changed: %v", err)
			}
			published := point == backup.PointRestoreMarkerRemoved || point == backup.PointRestorePublished
			if !published {
				if _, err := ridstore.Open(testConfig(destination)); !errors.Is(err, base.ErrRecoveryRequired) {
					t.Fatalf("incomplete restore opened: %v", err)
				}
				if _, err := os.Lstat(filepath.Join(destination, initialize.RestoringMarkerFileName)); err != nil {
					t.Fatalf("RESTORING missing: %v", err)
				}
				return
			}
			store, err := ridstore.Open(testConfig(destination))
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.GetRecord(context.Background(), id)
			if err != nil || string(got.Value) != string(want.Value) || got.Revision != want.Revision {
				t.Fatalf("record=%+v want=%+v error=%v", got, want, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func killOfflineChild(t *testing.T, kind, source, artifact, destination string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestOfflineCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		offlineCrashChildEnv+"=1", offlineCrashKindEnv+"="+kind,
		offlineCrashSourceEnv+"="+source, offlineCrashArtifactEnv+"="+artifact,
		offlineCrashDestEnv+"="+destination, offlineCrashPointEnv+"="+string(point),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
			return
		}
		ready <- ""
	}()
	want := "RIDSTORE_FAILPOINT_READY " + string(point)
	select {
	case line := <-ready:
		if line != want {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("child did not reach failpoint: line=%q stderr=%s", line, stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("timeout waiting for %s: stderr=%s", point, stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("SIGKILL child exited successfully")
	}
}

func TestOfflineCrashChild(t *testing.T) {
	if os.Getenv(offlineCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	target := failpoint.Point(os.Getenv(offlineCrashPointEnv))
	hook := failpoint.Func(func(point failpoint.Point) error {
		if point != target {
			return nil
		}
		fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
		_ = os.Stdout.Sync()
		select {}
	})
	var err error
	switch os.Getenv(offlineCrashKindEnv) {
	case "backup":
		_, err = backup.CreateWithOptions(context.Background(), os.Getenv(offlineCrashSourceEnv), os.Getenv(offlineCrashArtifactEnv), backup.CreateOptions{Hook: hook})
	case "restore":
		_, err = backup.Restore(context.Background(), os.Getenv(offlineCrashArtifactEnv), os.Getenv(offlineCrashDestEnv), backup.RestoreOptions{Hook: hook})
	default:
		t.Fatal("unknown offline crash kind")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("offline failpoint %s was not reached", target)
}
