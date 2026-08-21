package ridstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/rotation"
	"github.com/akzj/ridstore/internal/verify"
)

func TestRotationJournalSyscallErrorsRecoverAndRemoveOrphanTemp(t *testing.T) {
	points := []failpoint.Point{
		rotation.PointBeforeJournalTempRemove,
		rotation.PointBeforeJournalWrite,
		rotation.PointBeforeJournalSync,
		rotation.PointBeforeJournalRename,
		rotation.PointBeforeJournalDirSync,
		rotation.PointBeforeJournalRemove,
		rotation.PointBeforeJournalRemoveTemp,
		rotation.PointBeforeJournalRemoveDirSync,
	}
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range points {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				hook := failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				})
				exerciseRotationFaultRecovery(t, hook, cause.err)
			})
		}
	}
}

func TestRotationJournalDirSyncErrorRecoversEveryPublishedPhase(t *testing.T) {
	for phase := int32(1); phase <= 5; phase++ {
		phase := phase
		t.Run(string(rune('0'+phase)), func(t *testing.T) {
			var calls atomic.Int32
			hook := failpoint.Func(func(got failpoint.Point) error {
				if got == rotation.PointBeforeJournalDirSync && calls.Add(1) == phase {
					return syscall.EIO
				}
				return nil
			})
			exerciseRotationFaultRecovery(t, hook, syscall.EIO)
		})
	}
}

func exerciseRotationFaultRecovery(t *testing.T, hook failpoint.Hook, cause error) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, err := createWithOptions(cfg, initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	var operationErr error
	for i := 0; i < 100 && operationErr == nil; i++ {
		batch, err := store.Begin(context.Background())
		if err != nil {
			operationErr = err
			break
		}
		id, err := batch.Allocate(context.Background())
		if err == nil {
			err = batch.Put(context.Background(), id, bytes.Repeat([]byte{'x'}, 512))
		}
		if err == nil {
			_, err = batch.Commit(context.Background())
		}
		operationErr = err
	}
	if !errors.Is(operationErr, cause) {
		t.Fatalf("rotation operation error=%v", operationErr)
	}
	if _, err := store.Begin(context.Background()); !errors.Is(err, ErrReadOnly) || !errors.Is(err, cause) {
		t.Fatalf("write after rotation error=%v", err)
	}
	_ = store.Close()

	store, err = Open(cfg)
	if err != nil {
		t.Fatalf("fresh Open: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "journal", ".ROTATION.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan rotation temp remains: %v", err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Allocate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(context.Background(), id, []byte("after-recovery")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := verify.Run(context.Background(), dir)
	if err != nil || !report.Clean {
		t.Fatalf("verify=%+v error=%v", report, err)
	}
}

func TestRotationRecoveryRejectsNonRegularOrphanTemp(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "journal", ".ROTATION.tmp")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error=%v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "keep" {
		t.Fatalf("target=%q error=%v", data, err)
	}
}
