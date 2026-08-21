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
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/commit"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/maintenance"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/mapping/radix"
	"github.com/akzj/ridstore/internal/rotation"
	"github.com/akzj/ridstore/internal/verify"
)

const (
	crashChildEnv           = "RIDSTORE_INITIALIZE_CRASH_CHILD"
	crashDirEnv             = "RIDSTORE_INITIALIZE_CRASH_DIR"
	crashPointEnv           = "RIDSTORE_INITIALIZE_CRASH_POINT"
	commitCrashChildEnv     = "RIDSTORE_COMMIT_CRASH_CHILD"
	commitCrashDirEnv       = "RIDSTORE_COMMIT_CRASH_DIR"
	commitCrashPointEnv     = "RIDSTORE_COMMIT_CRASH_POINT"
	rotationCrashChildEnv   = "RIDSTORE_ROTATION_CRASH_CHILD"
	rotationCrashDirEnv     = "RIDSTORE_ROTATION_CRASH_DIR"
	rotationCrashPointEnv   = "RIDSTORE_ROTATION_CRASH_POINT"
	checkpointCrashChildEnv = "RIDSTORE_CHECKPOINT_CRASH_CHILD"
	checkpointCrashDirEnv   = "RIDSTORE_CHECKPOINT_CRASH_DIR"
	checkpointCrashPointEnv = "RIDSTORE_CHECKPOINT_CRASH_POINT"
	dataGCCrashChildEnv     = "RIDSTORE_DATA_GC_CRASH_CHILD"
	dataGCCrashDirEnv       = "RIDSTORE_DATA_GC_CRASH_DIR"
	dataGCCrashPointEnv     = "RIDSTORE_DATA_GC_CRASH_POINT"
	dataGCForceMapRotateEnv = "RIDSTORE_DATA_GC_FORCE_MAP_ROTATION"
	reserveCrashChildEnv    = "RIDSTORE_RESERVE_CRASH_CHILD"
	reserveCrashDirEnv      = "RIDSTORE_RESERVE_CRASH_DIR"
	reserveCrashKindEnv     = "RIDSTORE_RESERVE_CRASH_KIND"
	reserveCrashPointEnv    = "RIDSTORE_RESERVE_CRASH_POINT"
	abortCrashChildEnv      = "RIDSTORE_ABORT_CRASH_CHILD"
	abortCrashDirEnv        = "RIDSTORE_ABORT_CRASH_DIR"
	abortCrashPointEnv      = "RIDSTORE_ABORT_CRASH_POINT"
)

const reserveIssuedPoint failpoint.Point = "allocator.id-issued"
const abortReturnedPoint failpoint.Point = "batch.abort-returned"

func TestAbortProcessCrashMatrix(t *testing.T) {
	for _, point := range []failpoint.Point{
		appendlog.PointAbortPrepared,
		appendlog.PointAbortWritten,
		abortReturnedPoint,
	} {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			killAbortChildAt(t, dir, point)
			store, err := Open(smallTestConfig(dir))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if _, err := store.Get(context.Background(), 1); !errors.Is(err, ErrNotFound) {
				t.Fatalf("aborted Put became visible: %v", err)
			}
			status, err := store.Status(context.Background(), 1)
			if err != nil || status.State != BatchStateAborted || status.CommitSeq != 0 {
				t.Fatalf("old status=%+v error=%v", status, err)
			}
			batch, err := store.Begin(context.Background())
			if err != nil || batch.ID() != 5 {
				t.Fatalf("new batch=%v error=%v", batchIDOf(batch), err)
			}
			id, err := batch.Allocate(context.Background())
			if err != nil || id != 5 {
				t.Fatalf("new record ID=%d error=%v", id, err)
			}
			if err := batch.Abort(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAbortPreWriteFailureReleasesResourcesAndRemainsRecoverable(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *Batch) error
		want error
	}{
		{
			name: "cancelled-context",
			run: func(_ context.Context, batch *Batch) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return batch.Abort(ctx)
			},
			want: context.Canceled,
		},
		{
			name: "prepared-hook-error",
			run:  func(ctx context.Context, batch *Batch) error { return batch.Abort(ctx) },
			want: errAbortPrepared,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			var armed atomic.Bool
			hook := failpoint.Func(func(point failpoint.Point) error {
				if armed.Load() && point == appendlog.PointAbortPrepared {
					return errAbortPrepared
				}
				return nil
			})
			store, err := createWithOptions(smallTestConfig(dir), initialize.Options{Hook: hook})
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			id, err := batch.Allocate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Put(context.Background(), id, []byte("orphan")); err != nil {
				t.Fatal(err)
			}
			segmentID := store.segments.Active().SegmentID()
			if refs := store.segments.OpenBatchRefs(segmentID); refs != 1 {
				t.Fatalf("open refs before abort=%d", refs)
			}
			armed.Store(true)
			if err := tc.run(context.Background(), batch); !errors.Is(err, tc.want) {
				t.Fatalf("abort error=%v want=%v", err, tc.want)
			}
			armed.Store(false)
			if refs := store.segments.OpenBatchRefs(segmentID); refs != 0 {
				t.Fatalf("open refs after abort=%d", refs)
			}
			status, err := store.Status(context.Background(), batch.ID())
			if err != nil || status.State != BatchStateAborted {
				t.Fatalf("status=%+v error=%v", status, err)
			}

			live, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			liveID, err := live.Allocate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := live.Put(context.Background(), liveID, []byte("live")); err != nil {
				t.Fatal(err)
			}
			if _, err := live.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = Open(smallTestConfig(dir))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Get(context.Background(), id); !errors.Is(err, ErrNotFound) {
				t.Fatalf("orphan get error=%v", err)
			}
			if value, err := store.Get(context.Background(), liveID); err != nil || string(value) != "live" {
				t.Fatalf("live value=%q error=%v", value, err)
			}
			status, err = store.Status(context.Background(), batch.ID())
			if err != nil || status.State != BatchStateAborted {
				t.Fatalf("recovered status=%+v error=%v", status, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

var errAbortPrepared = errors.New("abort prepared test error")

func batchIDOf(batch *Batch) BatchID {
	if batch == nil {
		return 0
	}
	return batch.ID()
}

func killAbortChildAt(t *testing.T, dir string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAbortCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), abortCrashChildEnv+"=1", abortCrashDirEnv+"="+dir, abortCrashPointEnv+"="+string(point))
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

func TestAbortCrashChild(t *testing.T) {
	if os.Getenv(abortCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	target := failpoint.Point(os.Getenv(abortCrashPointEnv))
	var armed atomic.Bool
	hook := failpoint.Func(func(point failpoint.Point) error {
		if armed.Load() && point == target {
			blockAtCrashPoint(point)
		}
		return nil
	})
	store, err := createWithOptions(smallTestConfig(os.Getenv(abortCrashDirEnv)), initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(context.Background(), id, []byte("must-stay-invisible")); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if err := batch.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target == abortReturnedPoint {
		blockAtCrashPoint(target)
	}
	t.Fatalf("abort failpoint %s was not reached", target)
}

func TestReserveProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		appendlog.PointReservePrepared,
		appendlog.PointReserveWritten,
		appendlog.PointReserveSynced,
		reserveIssuedPoint,
	}
	for _, kind := range []string{"record", "batch"} {
		kind := kind
		for _, point := range points {
			point := point
			t.Run(kind+"/"+string(point), func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				killReserveChildAt(t, dir, kind, point)
				store, err := Open(smallTestConfig(dir))
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}

				batch, err := store.Begin(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				got := uint64(batch.ID())
				if kind == "record" {
					id, allocateErr := batch.Allocate(context.Background())
					if allocateErr != nil {
						t.Fatal(allocateErr)
					}
					got = uint64(id)
				}
				assertRecoveredReserveID(t, point, got)

				// Record reserve cases always issued BatchID 1 before arming the
				// record allocator failpoint. The explicit issued case does so for
				// both allocator kinds. Recovery must preserve that old identity as
				// Aborted while the new Batch remains a distinct Open identity.
				if kind == "record" || point == reserveIssuedPoint {
					status, statusErr := store.Status(context.Background(), 1)
					if statusErr != nil || status.State != BatchStateAborted {
						t.Fatalf("old batch status=%+v error=%v", status, statusErr)
					}
					newStatus, statusErr := store.Status(context.Background(), batch.ID())
					if statusErr != nil || newStatus.State != BatchStateOpen {
						t.Fatalf("new batch status=%+v error=%v", newStatus, statusErr)
					}
				}
				if err := batch.Abort(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestReserveMonotonicAcrossRotationCheckpointAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var maxBatch BatchID
	var maxRecord ID
	for i := 1; i <= 240; i++ {
		batch, beginErr := store.Begin(context.Background())
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		id, allocateErr := batch.Allocate(context.Background())
		if allocateErr != nil {
			t.Fatal(allocateErr)
		}
		if batch.ID() <= maxBatch || id <= maxRecord {
			t.Fatalf("non-monotonic allocation batch=%d after=%d record=%d after=%d", batch.ID(), maxBatch, id, maxRecord)
		}
		maxBatch, maxRecord = batch.ID(), id
		if err := batch.Abort(context.Background()); err != nil {
			t.Fatal(err)
		}
		if i%40 == 0 {
			if err := store.Checkpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(store.catalog.Snapshot().SealedDataSegments) == 0 {
		t.Fatal("workload did not cross a Data Segment rotation")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if batch.ID() <= maxBatch || id <= maxRecord {
		t.Fatalf("recovery reused ID batch=%d previous=%d record=%d previous=%d", batch.ID(), maxBatch, id, maxRecord)
	}
	if err := batch.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveredReserveID(t *testing.T, point failpoint.Point, got uint64) {
	t.Helper()
	switch point {
	case appendlog.PointReservePrepared:
		if got != 1 {
			t.Fatalf("pre-write recovered ID=%d, want 1", got)
		}
	case appendlog.PointReserveWritten:
		if got != 1 && got != 5 {
			t.Fatalf("pre-sync recovered ID=%d, want old 1 or durable 5", got)
		}
	case appendlog.PointReserveSynced, reserveIssuedPoint:
		if got != 5 {
			t.Fatalf("durable reserve recovered ID=%d, want 5", got)
		}
	default:
		t.Fatalf("unknown reserve point %q", point)
	}
}

func killReserveChildAt(t *testing.T, dir, kind string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestReserveCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		reserveCrashChildEnv+"=1", reserveCrashDirEnv+"="+dir,
		reserveCrashKindEnv+"="+kind, reserveCrashPointEnv+"="+string(point),
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
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("timeout waiting for %s/%s: stderr=%s", kind, point, stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("SIGKILL child exited successfully")
	}
}

func TestReserveCrashChild(t *testing.T) {
	if os.Getenv(reserveCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	kind := os.Getenv(reserveCrashKindEnv)
	target := failpoint.Point(os.Getenv(reserveCrashPointEnv))
	var armed atomic.Bool
	hook := failpoint.Func(func(point failpoint.Point) error {
		if !armed.Load() || point != target {
			return nil
		}
		blockAtCrashPoint(point)
		return nil
	})
	store, err := createWithOptions(smallTestConfig(os.Getenv(reserveCrashDirEnv)), initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	if kind == "batch" {
		armed.Store(true)
		batch, beginErr := store.Begin(context.Background())
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if target == reserveIssuedPoint {
			blockAtCrashPoint(target)
		}
		_ = batch
	} else if kind == "record" {
		batch, beginErr := store.Begin(context.Background())
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		armed.Store(true)
		if _, allocateErr := batch.Allocate(context.Background()); allocateErr != nil {
			t.Fatal(allocateErr)
		}
		if target == reserveIssuedPoint {
			blockAtCrashPoint(target)
		}
	} else {
		t.Fatalf("unknown reserve kind %q", kind)
	}
	t.Fatalf("reserve failpoint %s was not reached", target)
}

func blockAtCrashPoint(point failpoint.Point) {
	fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
	_ = os.Stdout.Sync()
	select {}
}

func TestDataGCProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		appendlog.PointRelocationPartWritten, appendlog.PointRelocationSealWritten,
		appendlog.PointRelocationSynced, commit.PointRelocationPublished,
		pointDataGCPrepared, pointDataGCCopying, pointDataGCRelocations, pointDataGCCheckpoint,
		pointDataGCRetired, pointDataGCManifestRemoved, pointDataGCTrashed, pointDataGCDeleted,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			killDataGCChildAt(t, dir, point)
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, err := Open(cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer store.Close()
			record, err := store.GetRecord(context.Background(), 1)
			if err != nil || string(record.Value) != "stable" || record.Revision != 1 {
				t.Fatalf("record=%+v error=%v", record, err)
			}
			if _, found, err := maintenance.Load(dir); err != nil || found {
				t.Fatalf("maintenance journal found=%v error=%v", found, err)
			}
		})
	}
}

func TestDataGCNestedMappingRotationProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		radix.PointRotationPrepared, radix.PointRotationOldSealed,
		radix.PointRotationNewCreated, radix.PointRotationManifestInstalled,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			killDataGCChildAt(t, dir, point, true)
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, err := Open(cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer store.Close()
			record, err := store.GetRecord(context.Background(), 1)
			if err != nil || string(record.Value) != "stable" || record.Revision != 1 {
				t.Fatalf("record=%+v error=%v", record, err)
			}
			if _, found, err := maintenance.Load(dir); err != nil || found {
				t.Fatalf("maintenance journal found=%v error=%v", found, err)
			}
		})
	}
}

func killDataGCChildAt(t *testing.T, dir string, point failpoint.Point, forceMapRotation ...bool) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDataGCCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), dataGCCrashChildEnv+"=1", dataGCCrashDirEnv+"="+dir, dataGCCrashPointEnv+"="+string(point))
	if len(forceMapRotation) != 0 && forceMapRotation[0] {
		cmd.Env = append(cmd.Env, dataGCForceMapRotateEnv+"=1")
	}
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
	case <-time.After(15 * time.Second):
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

func TestDataGCCrashChild(t *testing.T) {
	if os.Getenv(dataGCCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	dir := os.Getenv(dataGCCrashDirEnv)
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	target := failpoint.Point(os.Getenv(dataGCCrashPointEnv))
	var armed atomic.Bool
	hook := failpoint.Func(func(point failpoint.Point) error {
		if !armed.Load() || point != target {
			return nil
		}
		fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
		_ = os.Stdout.Sync()
		select {}
	})
	store, err := createWithOptions(cfg, initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	populateDataGCStore(t, store)
	if os.Getenv(dataGCForceMapRotateEnv) == "1" {
		fillActiveMappingForNestedDataGC(t, store, cfg)
	}
	armed.Store(true)
	if _, err := store.CompactData(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("data GC failpoint %s was not reached", target)
}

func TestCheckpointProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		radix.PointRotationPrepared, radix.PointRotationOldSealed, radix.PointRotationNewCreated, radix.PointRotationManifestInstalled,
		pointCheckpointMappingSynced, pointCheckpointManifestInstalled, pointCheckpointRuntimePublished,
		"manifest.manifest-written", "manifest.manifest-file-synced", "manifest.manifest-renamed", "manifest.manifest-dir-synced",
		"manifest.current-written", "manifest.current-file-synced", "manifest.current-renamed", "manifest.root-dir-synced",
		radix.PointMappingGCPrepared, radix.PointMappingGCCopying, radix.PointMappingGCCopied, radix.PointMappingGCFilesDurable,
		radix.PointMappingGCManifestInstalled, radix.PointMappingGCRuntimeInstalled, radix.PointMappingGCTrashed,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			killCheckpointChildAt(t, dir, point)
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			cfg.MaxOpenBatches = 128
			store, err := Open(cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			for i := 1; i <= 80; i++ {
				id := ID(uint64(i) << 20)
				value, err := store.Get(context.Background(), id)
				if err != nil || string(value) != fmt.Sprintf("sparse-%d", i) {
					t.Fatalf("id=%d value=%q error=%v", id, value, err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			report, err := verify.Run(context.Background(), dir)
			if err != nil || !report.Clean {
				t.Fatalf("post-recovery verify=%+v error=%v", report, err)
			}
		})
	}
}

func killCheckpointChildAt(t *testing.T, dir string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), checkpointCrashChildEnv+"=1", checkpointCrashDirEnv+"="+dir, checkpointCrashPointEnv+"="+string(point))
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
	case <-time.After(15 * time.Second):
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

func TestCheckpointCrashChild(t *testing.T) {
	if os.Getenv(checkpointCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	target := failpoint.Point(os.Getenv(checkpointCrashPointEnv))
	var armed atomic.Bool
	hook := failpoint.Func(func(point failpoint.Point) error {
		if !armed.Load() || point != target {
			return nil
		}
		fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
		_ = os.Stdout.Sync()
		select {}
	})
	cfg := smallTestConfig(os.Getenv(checkpointCrashDirEnv))
	cfg.SegmentSize = 16 << 10
	cfg.MaxOpenBatches = 128
	store, err := createWithOptions(cfg, initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 80; i++ {
		batch, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id := ID(uint64(i) << 20)
		if err := batch.Put(context.Background(), id, []byte(fmt.Sprintf("sparse-%d", i))); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	armed.Store(true)
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target == radix.PointMappingGCPrepared || target == radix.PointMappingGCCopying || target == radix.PointMappingGCCopied || target == radix.PointMappingGCFilesDurable ||
		target == radix.PointMappingGCManifestInstalled || target == radix.PointMappingGCRuntimeInstalled || target == radix.PointMappingGCTrashed {
		if err := store.CompactMapping(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("checkpoint failpoint %s was not reached", target)
}

func TestRotationProcessCrashMatrix(t *testing.T) {
	points := []failpoint.Point{
		rotation.PointPrepared, rotation.PointOldSealed, rotation.PointNewCreated, rotation.PointManifestInstalled,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			killRotationChildAt(t, dir, point)
			cfg := smallTestConfig(dir)
			cfg.SegmentSize = 16 << 10
			store, err := Open(cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer store.Close()
			b, err := store.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			id, err := b.Allocate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Put(context.Background(), id, []byte("after-recovery")); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			if value, err := store.Get(context.Background(), id); err != nil || string(value) != "after-recovery" {
				t.Fatalf("value=%q error=%v", value, err)
			}
		})
	}
}

func killRotationChildAt(t *testing.T, dir string, point failpoint.Point) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRotationCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), rotationCrashChildEnv+"=1", rotationCrashDirEnv+"="+dir, rotationCrashPointEnv+"="+string(point))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	want := "RIDSTORE_FAILPOINT_READY " + string(point)
	scanner := bufio.NewScanner(stdout)
	ready := make(chan string, 1)
	go func() {
		if scanner.Scan() {
			ready <- scanner.Text()
			return
		}
		ready <- ""
	}()
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

func TestRotationCrashChild(t *testing.T) {
	if os.Getenv(rotationCrashChildEnv) != "1" {
		t.Skip("subprocess helper")
	}
	target := failpoint.Point(os.Getenv(rotationCrashPointEnv))
	hook := failpoint.Func(func(point failpoint.Point) error {
		if point != target {
			return nil
		}
		fmt.Printf("RIDSTORE_FAILPOINT_READY %s\n", point)
		_ = os.Stdout.Sync()
		select {}
	})
	cfg := smallTestConfig(os.Getenv(rotationCrashDirEnv))
	cfg.SegmentSize = 16 << 10
	store, err := createWithOptions(cfg, initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		b, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(context.Background(), id, bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("rotation failpoint %s was not reached", target)
}

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
