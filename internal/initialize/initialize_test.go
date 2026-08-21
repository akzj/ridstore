package initialize

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func testHardLimits() storeformat.HardLimits {
	return storeformat.HardLimits{
		SegmentSize: 256 << 20, MaxValueSize: 64 << 20, MaxBatchBytes: 256 << 20,
		MaxBatchMutations: 1_000_000, MaxBatchConditions: 1_000_000, MaxOpenBatches: 1024,
		IDReserveSize: 1 << 20, BatchIDReserveSize: 1 << 16,
	}
}

func TestEveryInitializationFailpointIsRetryable(t *testing.T) {
	points := []failpoint.Point{
		PointMarkerWritten, PointMarkerFileSynced, PointMarkerRenamed, PointMarkerDirSynced,
		PointDirectoriesCreated, PointDirectoriesSynced,
		PointDataHeaderWritten, PointDataHeaderSynced, PointDataDirectorySynced,
		PointMapHeaderWritten, PointMapHeaderSynced, PointMapDirectorySynced,
		"manifest.manifest-written", "manifest.manifest-file-synced", "manifest.manifest-renamed", "manifest.manifest-dir-synced",
		"manifest.current-written", "manifest.current-file-synced", "manifest.current-renamed", "manifest.root-dir-synced",
		PointMarkerRemoved, PointFinalDirSynced,
	}
	stop := errors.New("injected stop")
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			hook := failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					return stop
				}
				return nil
			})
			if _, err := CreateWithOptions(dir, testHardLimits(), Options{Hook: hook}); !errors.Is(err, stop) {
				t.Fatalf("injected error=%v", err)
			}
			m, err := Open(dir)
			if err != nil {
				t.Fatalf("recovery: %v", err)
			}
			if m.StoreUUID == (base.StoreUUID{}) || m.Generation != 1 {
				t.Fatalf("manifest=%+v", m)
			}
		})
	}
}

func TestResumePreparedMarkerKeepsUUID(t *testing.T) {
	dir := t.TempDir()
	marker := storeformat.InitializingMarker{StoreUUID: base.StoreUUID{7, 8, 9}, HardLimits: testHardLimits(), Phase: storeformat.InitializingPrepared}
	if err := installMarker(dir, marker, nil); err != nil {
		t.Fatal(err)
	}
	m, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.StoreUUID != marker.StoreUUID || m.HardLimits != marker.HardLimits {
		t.Fatalf("manifest=%+v marker=%+v", m, marker)
	}
	if _, err := os.Stat(filepath.Join(dir, MarkerFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker remains: %v", err)
	}
}

func TestRecoverValidMarkerTemp(t *testing.T) {
	dir := t.TempDir()
	marker := storeformat.InitializingMarker{StoreUUID: base.StoreUUID{1}, HardLimits: testHardLimits(), Phase: storeformat.InitializingPrepared}
	data, err := storeformat.EncodeInitializingMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeExclusiveSynced(filepath.Join(dir, markerTempFileName), data, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	m, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.StoreUUID != marker.StoreUUID {
		t.Fatalf("UUID=%x want=%x", m.StoreUUID, marker.StoreUUID)
	}
}

func TestDurablePhaseMissingDirectoryFailsClosed(t *testing.T) {
	dir := t.TempDir()
	marker := storeformat.InitializingMarker{StoreUUID: base.StoreUUID{1}, HardLimits: testHardLimits(), Phase: storeformat.InitializingDirectoriesDurable}
	if err := installMarker(dir, marker, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestCreateResumeConfigMismatch(t *testing.T) {
	dir := t.TempDir()
	marker := storeformat.InitializingMarker{StoreUUID: base.StoreUUID{1}, HardLimits: testHardLimits(), Phase: storeformat.InitializingPrepared}
	if err := installMarker(dir, marker, nil); err != nil {
		t.Fatal(err)
	}
	different := marker.HardLimits
	different.MaxValueSize--
	if _, err := Create(dir, different); !errors.Is(err, base.ErrConfigMismatch) {
		t.Fatalf("error=%v", err)
	}
	got, found, err := loadRecoverableMarker(dir, nil)
	if err != nil || !found || !reflect.DeepEqual(got, marker) {
		t.Fatalf("marker=%+v found=%v error=%v", got, found, err)
	}
}
