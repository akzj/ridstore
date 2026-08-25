package bootstrap

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestInitializationResumesAtEveryDurablePhase(t *testing.T) {
	points := []FaultPoint{
		FaultBeforeMarkerWrite,
		FaultBeforeMarkerSync,
		FaultBeforeMarkerRename,
		FaultBeforeMarkerDirSync,
		FaultBeforeDataSegment,
		FaultBeforeMapSegment,
		FaultBeforeManifest,
		FaultBeforeMarkerRemove,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			injected := errors.New("injected initialization failure")
			_, err := Initialize(root, testHardLimits(), func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("initial create err=%v", err)
			}
			manifest, err := Initialize(root, testHardLimits(), nil)
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			loaded, err := storecatalog.Load(root)
			if err != nil || !reflect.DeepEqual(loaded, manifest) {
				t.Fatalf("loaded=%+v manifest=%+v err=%v", loaded, manifest, err)
			}
			if err := RequireReady(root); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFinalDirectorySyncFailureLeavesOpenableStore(t *testing.T) {
	root := t.TempDir()
	injected := errors.New("injected final directory sync failure")
	_, err := Initialize(root, testHardLimits(), func(point FaultPoint) error {
		if point == FaultBeforeFinalDirSync {
			return injected
		}
		return nil
	})
	if !errors.Is(err, injected) {
		t.Fatalf("create err=%v", err)
	}
	if err := RequireReady(root); err != nil {
		t.Fatal(err)
	}
	if _, err := storecatalog.Load(root); err != nil {
		t.Fatal(err)
	}
}

func TestInitializationKeepsIdentityAndRejectsConfigChange(t *testing.T) {
	root := t.TempDir()
	stop := errors.New("stop after marker")
	_, err := Initialize(root, testHardLimits(), func(point FaultPoint) error {
		if point == FaultBeforeDataSegment {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("create err=%v", err)
	}
	marker, found, err := loadMarker(root)
	if err != nil || !found {
		t.Fatalf("marker found=%v err=%v", found, err)
	}
	different := testHardLimits()
	different.MaxValueSize--
	if _, err := Initialize(root, different, nil); !errors.Is(err, base.ErrConfigMismatch) {
		t.Fatalf("config mismatch err=%v", err)
	}
	resumed, err := Initialize(root, testHardLimits(), nil)
	if err != nil || resumed.StoreUUID != marker.StoreUUID || resumed.RecordLogID != marker.RecordLogID {
		t.Fatalf("resumed=%+v marker=%+v err=%v", resumed, marker, err)
	}
	if _, err := Initialize(root, testHardLimits(), nil); !errors.Is(err, base.ErrAlreadyExists) {
		t.Fatalf("duplicate create err=%v", err)
	}
}

func TestInitializationDiscardsUnpublishedCreatingFiles(t *testing.T) {
	root := t.TempDir()
	stop := errors.New("stop after marker")
	_, err := Initialize(root, testHardLimits(), func(point FaultPoint) error {
		if point == FaultBeforeDataSegment {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "records"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "records", "record-0000000001.creating"), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "mapping-v2"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "mapping-v2", "map-0000000001.creating"), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Initialize(root, testHardLimits(), nil); err != nil {
		t.Fatal(err)
	}
}

func testHardLimits() storecatalog.HardLimits {
	return storecatalog.HardLimits{
		SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
		MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
		MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
	}
}
