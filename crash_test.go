package ridstore

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

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/commit"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/manifest"
)

const (
	crashChildEnv       = "RIDSTORE_INITIALIZE_CRASH_CHILD"
	crashDirEnv         = "RIDSTORE_INITIALIZE_CRASH_DIR"
	crashPointEnv       = "RIDSTORE_INITIALIZE_CRASH_POINT"
	commitCrashChildEnv = "RIDSTORE_COMMIT_CRASH_CHILD"
	commitCrashDirEnv   = "RIDSTORE_COMMIT_CRASH_DIR"
	commitCrashPointEnv = "RIDSTORE_COMMIT_CRASH_POINT"
)

func TestCommitProcessCrashMatrix(t *testing.T) {
	tests := []struct {
		point         failpoint.Point
		mustCommitted bool
		allowEither   bool
	}{
		{appendlog.PointPutWritten, false, false},
		{appendlog.PointCommitPartWritten, false, false},
		{appendlog.PointCommitSealWritten, false, true},
		{appendlog.PointCommitSynced, true, false},
		{commit.PointMappingPublished, true, false},
		{commit.PointResultReady, true, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(string(tc.point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			killCommitChildAt(t, dir, tc.point)
			store, err := Open(smallTestConfig(dir))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer store.Close()
			value, getErr := store.Get(context.Background(), 1)
			status, statusErr := store.Status(context.Background(), 1)
			committed := getErr == nil
			if committed && (string(value) != "value" || statusErr != nil || status.State != BatchStateCommitted || status.CommitSeq != 1) {
				t.Fatalf("committed value=%q status=%+v statusErr=%v", value, status, statusErr)
			}
			if !committed && (!errors.Is(getErr, ErrNotFound) || statusErr != nil || status.State != BatchStateAborted) {
				t.Fatalf("uncommitted getErr=%v status=%+v statusErr=%v", getErr, status, statusErr)
			}
			if tc.mustCommitted && !committed {
				t.Fatal("durable boundary recovered as uncommitted")
			}
			if !tc.mustCommitted && !tc.allowEither && committed {
				t.Fatal("pre-Seal boundary recovered as committed")
			}
		})
	}
}

func killCommitChildAt(t *testing.T, dir string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommitCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), commitCrashChildEnv+"=1", commitCrashDirEnv+"="+dir, commitCrashPointEnv+"="+string(point))
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
	case <-time.After(10 * time.Second):
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

func TestCommitCrashChild(t *testing.T) {
	if os.Getenv(commitCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	dir := os.Getenv(commitCrashDirEnv)
	target := failpoint.Point(os.Getenv(commitCrashPointEnv))
	hook := failpoint.Func(func(point failpoint.Point) error {
		if point != target {
			return nil
		}
		fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
		_ = os.Stdout.Sync()
		select {}
	})
	store, err := createWithOptions(smallTestConfig(dir), initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put(context.Background(), id, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("failpoint %s was not reached", target)
}

func TestInitializationProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		initialize.PointMarkerWritten, initialize.PointMarkerFileSynced, initialize.PointMarkerRenamed, initialize.PointMarkerDirSynced,
		initialize.PointDirectoriesCreated, initialize.PointDirectoriesSynced,
		initialize.PointDataHeaderWritten, initialize.PointDataHeaderSynced, initialize.PointDataDirectorySynced,
		initialize.PointMapHeaderWritten, initialize.PointMapHeaderSynced, initialize.PointMapDirectorySynced,
		"manifest.manifest-written", "manifest.manifest-file-synced", "manifest.manifest-renamed", "manifest.manifest-dir-synced",
		"manifest.current-written", "manifest.current-file-synced", "manifest.current-renamed", "manifest.root-dir-synced",
		initialize.PointMarkerRemoved, initialize.PointFinalDirSynced,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			killInitializeChildAt(t, dir, point)
			before, err := initializationUUID(dir)
			if err != nil {
				t.Fatalf("read pre-recovery UUID: %v", err)
			}
			store, err := Open(Config{Dir: dir})
			if err != nil {
				t.Fatalf("fresh-process recovery: %v", err)
			}
			if store.manifest.StoreUUID != before || store.manifest.Generation != 1 {
				t.Fatalf("manifest UUID=%x generation=%d, pre-recovery UUID=%x", store.manifest.StoreUUID, store.manifest.Generation, before)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func killInitializeChildAt(t *testing.T, dir string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestInitializationCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), crashChildEnv+"=1", crashDirEnv+"="+dir, crashPointEnv+"="+string(point))
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
	select {
	case line := <-ready:
		if line != "RIDSTORE_FAILPOINT_READY "+string(point) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("child did not reach failpoint: line=%q stderr=%s", line, stderr.String())
		}
	case <-time.After(10 * time.Second):
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

func TestInitializationCrashChild(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	dir := os.Getenv(crashDirEnv)
	target := failpoint.Point(os.Getenv(crashPointEnv))
	hook := failpoint.Func(func(point failpoint.Point) error {
		if point != target {
			return nil
		}
		fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
		_ = os.Stdout.Sync()
		select {}
	})
	store, err := createWithOptions(Config{Dir: dir}, initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	// A missing target must fail the helper instead of silently proving a normal Close.
	_ = store
	t.Fatalf("failpoint %s was not reached", target)
}

func initializationUUID(dir string) (base.StoreUUID, error) {
	for _, name := range []string{initialize.MarkerFileName, ".INITIALIZING.tmp"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err == nil {
			marker, decodeErr := storeformat.DecodeInitializingMarker(data)
			if decodeErr != nil {
				return base.StoreUUID{}, decodeErr
			}
			return marker.StoreUUID, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return base.StoreUUID{}, err
		}
	}
	m, err := manifest.LoadCurrent(dir)
	if err != nil {
		return base.StoreUUID{}, err
	}
	return m.StoreUUID, nil
}
