package initialize

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type initializeCause struct {
	name string
	err  error
}

func initializeCauses() []initializeCause {
	return []initializeCause{
		{"EIO", syscall.EIO},
		{"ENOSPC", syscall.ENOSPC},
		{"EACCES", syscall.EACCES},
	}
}

func initializeOneShot(point failpoint.Point, cause error) failpoint.Hook {
	fired := false
	return failpoint.Func(func(got failpoint.Point) error {
		if got == point && !fired {
			fired = true
			return cause
		}
		return nil
	})
}

func assertInitialized(t *testing.T, dir string, wantHard storeformat.HardLimits) {
	t.Helper()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("open recovered store: %v", err)
	}
	if m.Generation != 1 || m.StoreUUID == (base.StoreUUID{}) || m.HardLimits != wantHard {
		t.Fatalf("manifest=%+v", m)
	}
	for _, name := range []string{MarkerFileName, markerTempFileName} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("initialization artifact remains %s: %v", name, err)
		}
	}
}

func TestInitializationSyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{
		PointBeforeMarkerTempRemove,
		PointBeforeMarkerWrite,
		PointBeforeMarkerFileSync,
		PointBeforeMarkerRename,
		PointBeforeMarkerDirSync,
		PointBeforeDirectoryCreate,
		PointBeforeDirectoriesSync,
		PointBeforeDataHeaderWrite,
		PointBeforeDataHeaderFileSync,
		PointBeforeDataDirectorySync,
		PointBeforeMapHeaderWrite,
		PointBeforeMapHeaderFileSync,
		PointBeforeMapDirectorySync,
		PointBeforeFinalMarkerRemove,
		PointBeforeFinalTempRemove,
		PointBeforeFinalDirSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range initializeCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir := t.TempDir()
				hard := testHardLimits()
				_, err := CreateWithOptions(dir, hard, Options{Hook: initializeOneShot(point, cause.err)})
				if !errors.Is(err, cause.err) {
					t.Fatalf("create error=%v want cause=%v", err, cause.err)
				}

				if _, err := Open(dir); errors.Is(err, base.ErrNotInitialized) {
					if _, err := Create(dir, hard); err != nil {
						t.Fatalf("retry create: %v", err)
					}
				} else if err != nil {
					t.Fatalf("retry open: %v", err)
				}
				assertInitialized(t, dir, hard)
			})
		}
	}
}

func TestValidMarkerTempRecoverySyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{
		PointBeforeMarkerFileSync,
		PointBeforeMarkerRename,
		PointBeforeMarkerDirSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range initializeCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir := t.TempDir()
				hard := testHardLimits()
				marker := storeformat.InitializingMarker{
					StoreUUID:  base.StoreUUID{1, 2, 3},
					HardLimits: hard,
					Phase:      storeformat.InitializingPrepared,
				}
				data, err := storeformat.EncodeInitializingMarker(marker)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, markerTempFileName), data, 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := OpenWithOptions(dir, Options{Hook: initializeOneShot(point, cause.err)}); !errors.Is(err, cause.err) {
					t.Fatalf("recovery error=%v want cause=%v", err, cause.err)
				}
				m, err := Open(dir)
				if err != nil {
					t.Fatalf("retry: %v", err)
				}
				if m.StoreUUID != marker.StoreUUID || m.HardLimits != hard {
					t.Fatalf("manifest=%+v marker=%+v", m, marker)
				}
				assertInitialized(t, dir, hard)
			})
		}
	}
}

func TestInvalidMarkerTempCleanupSyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{PointBeforeMarkerRecoveryRemove, PointBeforeMarkerRecoverySync}
	for _, point := range points {
		point := point
		for _, cause := range initializeCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir := t.TempDir()
				hard := testHardLimits()
				if err := os.WriteFile(filepath.Join(dir, markerTempFileName), []byte("partial"), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := OpenWithOptions(dir, Options{Hook: initializeOneShot(point, cause.err)}); !errors.Is(err, cause.err) {
					t.Fatalf("cleanup error=%v want cause=%v", err, cause.err)
				}
				if _, err := Create(dir, hard); err != nil {
					t.Fatalf("retry create: %v", err)
				}
				assertInitialized(t, dir, hard)
			})
		}
	}
}

func TestPartialInitialSegmentCleanupSyscallErrorsAreRetryable(t *testing.T) {
	for _, cause := range initializeCauses() {
		cause := cause
		t.Run(cause.name, func(t *testing.T) {
			dir := t.TempDir()
			hard := testHardLimits()
			if _, err := CreateWithOptions(dir, hard, Options{Hook: initializeOneShot(PointBeforeDataHeaderWrite, syscall.EIO)}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("prepare partial segment: %v", err)
			}
			if _, err := OpenWithOptions(dir, Options{Hook: initializeOneShot(PointBeforeInitialSegmentRemove, cause.err)}); !errors.Is(err, cause.err) {
				t.Fatalf("cleanup error=%v want cause=%v", err, cause.err)
			}
			assertInitialized(t, dir, hard)
		})
	}
}

func TestMarkerFreeOpenRetriesFinalDirectorySync(t *testing.T) {
	dir := t.TempDir()
	hard := testHardLimits()
	if _, err := CreateWithOptions(dir, hard, Options{Hook: initializeOneShot(PointBeforeFinalDirSync, syscall.EIO)}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("create error=%v", err)
	}
	if _, err := OpenWithOptions(dir, Options{Hook: initializeOneShot(PointBeforeFinalDirSync, syscall.EIO)}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("open did not retry root sync: %v", err)
	}
	assertInitialized(t, dir, hard)
}
