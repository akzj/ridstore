package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/mapping/radix"
	"github.com/akzj/ridstore/internal/verify"
)

func TestActiveMappingTailRepairSyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{radix.PointBeforeTailTruncate, radix.PointBeforeTailSync}
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range points {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				cfg := smallTestConfig(dir)
				store, err := Create(cfg)
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
				if err := batch.Put(context.Background(), id, []byte("tail-repair")); err != nil {
					t.Fatal(err)
				}
				committed, err := batch.Commit(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Checkpoint(context.Background()); err != nil {
					t.Fatal(err)
				}
				activeID := store.catalog.Snapshot().ActiveMapSegmentID
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "mapping", fmt.Sprintf("MAP-%08d.active", activeID))
				file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write([]byte("incomplete-tail")); err != nil {
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				hook := failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				})
				opened, err := openWithHook(cfg, hook)
				if opened != nil {
					_ = opened.Close()
					t.Fatal("faulted Open returned a Store")
				}
				if !errors.Is(err, cause.err) {
					t.Fatalf("Open error=%v", err)
				}
				recovered, err := Open(cfg)
				if err != nil {
					t.Fatalf("retry Open: %v", err)
				}
				record, err := recovered.GetRecord(context.Background(), id)
				if err != nil || record.Revision != Revision(committed.BatchID) || string(record.Value) != "tail-repair" {
					t.Fatalf("record=%+v error=%v", record, err)
				}
				if err := recovered.Close(); err != nil {
					t.Fatal(err)
				}
				report, err := verify.Run(context.Background(), dir)
				if err != nil || !report.Clean {
					t.Fatalf("verify=%+v error=%v", report, err)
				}
			})
		}
	}
}

func TestActiveMappingTailRepairDoesNotTruncateReferencedNode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	store, err := Create(cfg)
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
	if err := batch.Put(context.Background(), id, []byte("referenced-tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest := store.catalog.Snapshot()
	root := manifest.MappingRoot
	if root == 0 || root.SegmentID() != manifest.ActiveMapSegmentID {
		t.Fatalf("root=%x active=%d", root, manifest.ActiveMapSegmentID)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "mapping", fmt.Sprintf("MAP-%08d.active", manifest.ActiveMapSegmentID))
	brokenSize := int64(root.Offset()) + 8
	if err := os.Truncate(path, brokenSize); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error=%v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != brokenSize {
		t.Fatalf("corrupt referenced tail changed size=%d want=%d", info.Size(), brokenSize)
	}
}
