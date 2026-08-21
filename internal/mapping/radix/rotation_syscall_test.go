package radix

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/catalog"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/maintenance"
)

func TestMappingRotationSyscallErrorsRecover(t *testing.T) {
	points := mappingRotationSyscallPoints()
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range points {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir, manifest := nestedRotationFixture(t)
				manager, err := catalog.New(dir, manifest)
				if err != nil {
					t.Fatal(err)
				}
				store, err := openNodeStore(dir, manifest, manager)
				if err != nil {
					t.Fatal(err)
				}
				fillMappingSegmentForRotation(t, store)
				store.setHook(failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				}))
				if _, err := store.append(denseLeafBuild()); !errors.Is(err, cause.err) {
					t.Fatalf("append error=%v", err)
				}
				_ = store.Close()
				current, err := initialize.Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				recovered, err := RecoverMappingRotation(dir, current)
				if err != nil {
					t.Fatalf("RecoverMappingRotation: %v", err)
				}
				if point == PointBeforeRotationActiveSync {
					if recovered.ActiveMapSegmentID != 1 || len(recovered.SealedMappingSegments) != 0 {
						t.Fatalf("pre-journal recovered=%+v", recovered)
					}
				} else if recovered.ActiveMapSegmentID != 2 || !hasMapSummary(recovered.SealedMappingSegments, 1) {
					t.Fatalf("recovered=%+v", recovered)
				}
				assertRecoveredMappingRotation(t, dir, recovered)
			})
		}
	}
}

func TestNestedMappingRotationSyscallErrorsRecover(t *testing.T) {
	for _, point := range mappingRotationSyscallPoints() {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir, manifest := nestedRotationFixture(t)
			manager, err := catalog.New(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			store, err := openNodeStore(dir, manifest, manager)
			if err != nil {
				t.Fatal(err)
			}
			fillMappingSegmentForRotation(t, store)
			parent := installDataGCParent(t, dir, manifest)
			store.setHook(failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					return syscall.EIO
				}
				return nil
			}))
			if _, err := store.append(denseLeafBuild()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("append error=%v", err)
			}
			_ = store.Close()
			current, err := initialize.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := RecoverMappingRotation(dir, current)
			if err != nil {
				t.Fatalf("RecoverMappingRotation: %v", err)
			}
			journal, found, err := maintenance.Load(dir)
			if err != nil || !found || journal.OperationID != parent.OperationID || journal.Phase != 3 {
				t.Fatalf("journal=%+v found=%v error=%v", journal, found, err)
			}
			if point == PointBeforeRotationActiveSync {
				if recovered.ActiveMapSegmentID != 1 {
					t.Fatalf("pre-journal recovered=%+v", recovered)
				}
				if _, found := journalMappingRef(journal.SourceFiles, 1); found {
					t.Fatalf("unexpected Mapping ref: %+v", journal.SourceFiles)
				}
			} else {
				if recovered.ActiveMapSegmentID != 2 || !hasMapSummary(recovered.SealedMappingSegments, 1) {
					t.Fatalf("recovered=%+v", recovered)
				}
				if _, found := journalMappingRef(journal.SourceFiles, 1); !found {
					t.Fatalf("missing Mapping ref: %+v", journal.SourceFiles)
				}
			}
			assertRecoveredMappingFiles(t, dir, recovered)
		})
	}
}

func TestMappingRotationPropagatesMaintenanceJournalHook(t *testing.T) {
	points := []failpoint.Point{maintenance.PointBeforeWrite, maintenance.PointBeforeRemove}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir, manifest := nestedRotationFixture(t)
			manager, err := catalog.New(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			store, err := openNodeStore(dir, manifest, manager)
			if err != nil {
				t.Fatal(err)
			}
			fillMappingSegmentForRotation(t, store)
			store.setHook(failpoint.Func(func(got failpoint.Point) error {
				if got == point {
					return syscall.EIO
				}
				return nil
			}))
			if _, err := store.append(denseLeafBuild()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("append error=%v", err)
			}
			_ = store.Close()
			current, err := initialize.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := RecoverMappingRotation(dir, current)
			if err != nil {
				t.Fatal(err)
			}
			assertRecoveredMappingRotation(t, dir, recovered)
		})
	}
}

func TestMappingRotationRecoverySyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{
		PointBeforeRotationTruncate,
		PointBeforeRotationFooterWrite,
		PointBeforeRotationFooterSync,
		PointBeforeRotationRename,
		PointBeforeRotationDirSync,
		PointBeforeRotationHeaderWrite,
		PointBeforeRotationHeaderSync,
		PointBeforeRotationCreateSync,
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
				dir, current := preparedMappingRotation(t)
				hook := failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				})
				if _, err := RecoverMappingRotationWithHook(dir, current, hook); !errors.Is(err, cause.err) {
					t.Fatalf("recovery error=%v", err)
				}
				current, err := initialize.Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				recovered, err := RecoverMappingRotation(dir, current)
				if err != nil {
					t.Fatalf("retry recovery: %v", err)
				}
				if recovered.ActiveMapSegmentID != 2 || !hasMapSummary(recovered.SealedMappingSegments, 1) {
					t.Fatalf("recovered=%+v", recovered)
				}
				assertRecoveredMappingRotation(t, dir, recovered)
			})
		}
	}
}

func TestMappingRotationRecoveryPartialDestinationRemoveErrorsAreRetryable(t *testing.T) {
	for _, cause := range []error{syscall.EIO, syscall.ENOSPC, syscall.EACCES} {
		cause := cause
		t.Run(cause.Error(), func(t *testing.T) {
			dir, current := preparedMappingRotation(t)
			partial := filepath.Join(dir, "mapping", activeMapFileName(2))
			if err := os.WriteFile(partial, []byte("partial"), 0o600); err != nil {
				t.Fatal(err)
			}
			hook := failpoint.Func(func(got failpoint.Point) error {
				if got == PointBeforeRotationRemove {
					return cause
				}
				return nil
			})
			if _, err := RecoverMappingRotationWithHook(dir, current, hook); !errors.Is(err, cause) {
				t.Fatalf("recovery error=%v", err)
			}
			current, err := initialize.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := RecoverMappingRotation(dir, current)
			if err != nil {
				t.Fatal(err)
			}
			assertRecoveredMappingRotation(t, dir, recovered)
		})
	}
}

func TestMappingRotationRecoveryResyncsExistingFiles(t *testing.T) {
	cases := []struct {
		name          string
		runtimePoint  failpoint.Point
		recoveryPoint failpoint.Point
	}{
		{"sealed", PointBeforeRotationDirSync, PointBeforeRotationFooterSync},
		{"new-active", PointBeforeRotationCreateSync, PointBeforeRotationHeaderSync},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			dir, manifest := nestedRotationFixture(t)
			manager, err := catalog.New(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			store, err := openNodeStore(dir, manifest, manager)
			if err != nil {
				t.Fatal(err)
			}
			fillMappingSegmentForRotation(t, store)
			store.setHook(failpoint.Func(func(got failpoint.Point) error {
				if got == test.runtimePoint {
					return syscall.EBUSY
				}
				return nil
			}))
			if _, err := store.append(denseLeafBuild()); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("append error=%v", err)
			}
			_ = store.Close()
			current, err := initialize.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			hook := failpoint.Func(func(got failpoint.Point) error {
				if got == test.recoveryPoint {
					return syscall.EIO
				}
				return nil
			})
			if _, err := RecoverMappingRotationWithHook(dir, current, hook); !errors.Is(err, syscall.EIO) {
				t.Fatalf("recovery did not resync existing file: %v", err)
			}
			current, err = initialize.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := RecoverMappingRotation(dir, current)
			if err != nil {
				t.Fatal(err)
			}
			assertRecoveredMappingRotation(t, dir, recovered)
		})
	}
}

func preparedMappingRotation(t *testing.T) (string, storeformat.Manifest) {
	t.Helper()
	dir, manifest := nestedRotationFixture(t)
	manager, err := catalog.New(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	store, err := openNodeStore(dir, manifest, manager)
	if err != nil {
		t.Fatal(err)
	}
	fillMappingSegmentForRotation(t, store)
	store.setHook(failpoint.Func(func(got failpoint.Point) error {
		if got == PointRotationPrepared {
			return syscall.EBUSY
		}
		return nil
	}))
	if _, err := store.append(denseLeafBuild()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("prepare rotation error=%v", err)
	}
	_ = store.Close()
	current, err := initialize.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir, current
}

func mappingRotationSyscallPoints() []failpoint.Point {
	return []failpoint.Point{
		PointBeforeRotationActiveSync,
		PointBeforeRotationFooterWrite,
		PointBeforeRotationFooterSync,
		PointBeforeRotationRename,
		PointBeforeRotationDirSync,
		PointBeforeRotationHeaderWrite,
		PointBeforeRotationHeaderSync,
		PointBeforeRotationCreateSync,
	}
}

func assertRecoveredMappingRotation(t *testing.T, dir string, manifest storeformat.Manifest) {
	t.Helper()
	if journal, found, err := maintenance.Load(dir); err != nil || found {
		t.Fatalf("journal=%+v found=%v error=%v", journal, found, err)
	}
	assertRecoveredMappingFiles(t, dir, manifest)
}

func assertRecoveredMappingFiles(t *testing.T, dir string, manifest storeformat.Manifest) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, "mapping", activeMapFileName(manifest.ActiveMapSegmentID))); err != nil {
		t.Fatalf("active Mapping file: %v", err)
	}
	for _, summary := range manifest.SealedMappingSegments {
		if _, err := os.Stat(filepath.Join(dir, "mapping", sealedMapFileName(base.MapSegmentID(summary.FileID)))); err != nil {
			t.Fatalf("sealed Mapping file %d: %v", summary.FileID, err)
		}
	}
	manager, err := catalog.New(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	store, err := openNodeStore(dir, manifest, manager)
	if err != nil {
		t.Fatalf("open recovered node store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
