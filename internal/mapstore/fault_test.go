package mapstore

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/recordlog"
)

func TestCheckpointWriterFaultsPoisonRuntimeAndFreshOpenRecovers(t *testing.T) {
	causes := []error{syscall.EIO, syscall.ENOSPC, syscall.EACCES}
	points := []FaultPoint{FaultBeforeAppendWrite, FaultBeforeSync}
	for _, point := range points {
		for _, cause := range causes {
			t.Run(string(point)+"/"+cause.Error(), func(t *testing.T) {
				root := t.TempDir()
				state := initialState()
				if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
					t.Fatal(err)
				}
				catalog := &staticCatalog{state: state}
				store, err := OpenWithFaultHook(root, catalog, func(got FaultPoint) error {
					if got == point {
						return cause
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				var slots [NodeSlots]uint64
				slots[1] = mustRecordValue(t, 1)
				if point == FaultBeforeAppendWrite {
					_, err = store.AppendLeaf(0, 1, testLeafRefs(slots))
				} else {
					if _, err = store.AppendLeaf(0, 1, testLeafRefs(slots)); err == nil {
						err = store.Sync()
					}
				}
				if !errors.Is(err, ErrPoisoned) || !errors.Is(err, cause) {
					t.Fatalf("fault err=%v", err)
				}
				if _, err := store.AppendLeaf(0, 2, testLeafRefs(slots)); !errors.Is(err, ErrPoisoned) {
					t.Fatalf("append after fault err=%v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(root, catalog)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := reopened.AppendLeaf(0, 2, testLeafRefs(slots)); err != nil {
					t.Fatal(err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestActiveTailRepairFaultsRemainRetryable(t *testing.T) {
	for _, point := range []FaultPoint{FaultBeforeTailTruncate, FaultBeforeTailSync} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			state := initialState()
			if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
				t.Fatal(err)
			}
			catalog := &staticCatalog{state: state}
			store, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			var slots [NodeSlots]uint64
			slots[1] = mustRecordValue(t, 1)
			if _, err := store.AppendLeaf(0, 1, testLeafRefs(slots)); err != nil {
				t.Fatal(err)
			}
			if err := store.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, mappingDirectory, activeName(1))
			valid, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte{1, 2, 3, 4}); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("tail repair failure")
			if _, err := OpenWithFaultHook(root, catalog, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("open err=%v", err)
			}
			reopened, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(path)
			if err != nil || after.Size() != valid.Size() {
				t.Fatalf("size=%d want=%d err=%v", after.Size(), valid.Size(), err)
			}
		})
	}
}

func mustRecordValue(t *testing.T, segment recordlog.SegmentID) uint64 {
	t.Helper()
	value, err := recordlog.NewVAddr(segment, recordlog.SegmentHeaderSize, 64)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(value)
}
