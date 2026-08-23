package radix

import (
	"context"
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
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/mapping/api"
)

func TestMappingGCSyscallErrorsRecover(t *testing.T) {
	points := []failpoint.Point{
		PointBeforeMappingGCHeaderWrite,
		PointBeforeMappingGCNodeWrite,
		PointBeforeMappingGCFileSync,
		PointBeforeMappingGCTempDirSync,
		PointBeforeMappingGCPublishRename,
		PointBeforeMappingGCPublishDirSync,
		PointBeforeMappingGCTrashRename,
		PointBeforeMappingGCTrashMappingDirSync,
		PointBeforeMappingGCTrashPublishDirSync,
		PointBeforeMappingGCTrashDelete,
		PointBeforeMappingGCTrashDeleteDirSync,
	}
	for _, point := range points {
		point := point
		for _, cause := range mappingGCSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir, mapping, _, id, want := mappingGCSyscallFixture(t, false, nil)
				mapping.SetHook(errorAtMappingGCPoint(point, cause.err))
				if _, err := mapping.Compact(context.Background()); !errors.Is(err, cause.err) {
					t.Fatalf("Compact error=%v", err)
				}
				_ = mapping.Close()
				assertMappingGCRecovery(t, dir, id, want)
			})
		}
	}
}

func TestMappingGCSealedWriterErrorsRecover(t *testing.T) {
	for _, point := range []failpoint.Point{PointBeforeMappingGCFooterWrite, PointBeforeMappingGCFileSync} {
		point := point
		for _, cause := range mappingGCSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir, mapping, _, id, want := mappingGCSyscallFixture(t, true, nil)
				mapping.SetHook(errorAtMappingGCPoint(point, cause.err))
				if _, err := mapping.Compact(context.Background()); !errors.Is(err, cause.err) {
					t.Fatalf("Compact error=%v", err)
				}
				_ = mapping.Close()
				assertMappingGCRecovery(t, dir, id, want)
			})
		}
	}
}

func TestMappingGCPartialMultiFileOperationsRecover(t *testing.T) {
	points := []failpoint.Point{
		PointBeforeMappingGCPublishRename,
		PointBeforeMappingGCTrashRename,
		PointBeforeMappingGCTrashDelete,
	}
	for _, point := range points {
		point := point
		for _, cause := range mappingGCSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir, mapping, _, id, want := mappingGCSyscallFixture(t, true, nil)
				calls := 0
				mapping.SetHook(failpoint.Func(func(got failpoint.Point) error {
					if got != point {
						return nil
					}
					calls++
					if calls == 2 {
						return cause.err
					}
					return nil
				}))
				if _, err := mapping.Compact(context.Background()); !errors.Is(err, cause.err) {
					t.Fatalf("Compact error=%v calls=%d", err, calls)
				}
				if calls != 2 {
					t.Fatalf("syscall calls=%d want=2", calls)
				}
				_ = mapping.Close()
				assertMappingGCRecovery(t, dir, id, want)
			})
		}
	}
}

func TestMappingGCCleanupErrorsAreReturnedAndRecoverable(t *testing.T) {
	for _, point := range []failpoint.Point{PointBeforeMappingGCCleanupRemove, PointBeforeMappingGCCleanupDirSync} {
		point := point
		for _, cause := range mappingGCSyscallCauses() {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				dir, mapping, _, id, want := mappingGCSyscallFixture(t, false, nil)
				mapping.SetHook(failpoint.Func(func(got failpoint.Point) error {
					switch got {
					case PointMappingGCCopying:
						return syscall.EBUSY
					case point:
						return cause.err
					default:
						return nil
					}
				}))
				_, err := mapping.Compact(context.Background())
				if !errors.Is(err, syscall.EBUSY) || !errors.Is(err, cause.err) {
					t.Fatalf("Compact error=%v", err)
				}
				_ = mapping.Close()
				assertMappingGCRecovery(t, dir, id, want)
			})
		}
	}
}

func TestMappingGCRecoverySyscallErrorsAreRetryable(t *testing.T) {
	cases := []struct {
		name    string
		prepare failpoint.Point
		points  []failpoint.Point
	}{
		{
			name:    "rollback-before-manifest",
			prepare: PointMappingGCFilesDurable,
			points: []failpoint.Point{
				PointBeforeMappingGCCleanupRemove,
				PointBeforeMappingGCCleanupDirSync,
			},
		},
		{
			name:    "finish-after-manifest",
			prepare: PointMappingGCManifestInstalled,
			points: []failpoint.Point{
				PointBeforeMappingGCTrashRename,
				PointBeforeMappingGCTrashMappingDirSync,
				PointBeforeMappingGCTrashPublishDirSync,
				PointBeforeMappingGCTrashDelete,
				PointBeforeMappingGCTrashDeleteDirSync,
			},
		},
	}
	for _, test := range cases {
		test := test
		for _, point := range test.points {
			point := point
			for _, cause := range mappingGCSyscallCauses() {
				cause := cause
				t.Run(test.name+"/"+string(point)+"/"+cause.name, func(t *testing.T) {
					dir, mapping, _, id, want := mappingGCSyscallFixture(t, false, nil)
					prepareHook := errorAtMappingGCPoint(test.prepare, syscall.EBUSY)
					if test.name == "rollback-before-manifest" {
						prepareHook = failpoint.Func(func(got failpoint.Point) error {
							switch got {
							case test.prepare:
								return syscall.EBUSY
							case PointBeforeMappingGCCleanupDirSync:
								return syscall.EAGAIN
							default:
								return nil
							}
						})
					}
					mapping.SetHook(prepareHook)
					if _, err := mapping.Compact(context.Background()); !errors.Is(err, syscall.EBUSY) {
						t.Fatalf("prepare Compact error=%v", err)
					}
					_ = mapping.Close()
					current, err := initialize.Open(dir)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := RecoverMappingRotationWithHook(dir, current, errorAtMappingGCPoint(point, cause.err)); !errors.Is(err, cause.err) {
						t.Fatalf("recovery error=%v", err)
					}
					assertMappingGCRecovery(t, dir, id, want)
				})
			}
		}
	}
}

func TestMappingGCManifestPublicationErrorDoesNotDeletePublishedFiles(t *testing.T) {
	armed := false
	hook := failpoint.Func(func(point failpoint.Point) error {
		if armed && point == manifest.PointBeforeRootDirSync {
			return syscall.EIO
		}
		return nil
	})
	dir, mapping, old, id, want := mappingGCSyscallFixture(t, false, hook)
	armed = true
	if _, err := mapping.Compact(context.Background()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Compact error=%v", err)
	}
	current, err := initialize.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if current.MaintenanceGeneration <= old.MaintenanceGeneration || current.ActiveMapSegmentID == old.ActiveMapSegmentID {
		t.Fatalf("CURRENT did not publish compacted generation: old=%+v current=%+v", old, current)
	}
	if _, err := os.Stat(filepath.Join(dir, "mapping", activeMapFileName(current.ActiveMapSegmentID))); err != nil {
		t.Fatalf("published Mapping file was cleaned: %v", err)
	}
	_ = mapping.Close()
	assertMappingGCRecovery(t, dir, id, want)
}

func TestMappingGCPreInstallerConflictCleansNewFiles(t *testing.T) {
	dir, mapping, old, _, _ := mappingGCSyscallFixture(t, false, nil)
	sourceRefs, _, err := mapping.store.mappingGCSourceRefs(old)
	if err != nil {
		t.Fatal(err)
	}
	var oldActive storeformat.JournalFileRef
	for _, ref := range sourceRefs {
		if ref.FileID == uint32(old.ActiveMapSegmentID) {
			oldActive = ref
			break
		}
	}
	changed := false
	mapping.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point != PointMappingGCFilesDurable || changed {
			return nil
		}
		changed = true
		_, err := mapping.store.catalog.Install(0, func(next *storeformat.Manifest) error {
			next.SealedMappingSegments = append(next.SealedMappingSegments, storeformat.FileSummary{
				FileID: oldActive.FileID, ValidEnd: oldActive.ValidEnd, FirstSeq: oldActive.FirstSeq, LastSeq: oldActive.LastSeq,
			})
			next.ActiveMapSegmentID = next.NextMapSegmentID
			next.NextMapSegmentID++
			return nil
		})
		return err
	}))
	if _, err := mapping.Compact(context.Background()); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("Compact error=%v", err)
	}
	if !changed {
		t.Fatal("catalog baseline was not changed")
	}
	if _, err := mapping.store.catalog.Install(0, func(next *storeformat.Manifest) error {
		next.SealedMappingSegments = append([]storeformat.FileSummary(nil), old.SealedMappingSegments...)
		next.ActiveMapSegmentID = old.ActiveMapSegmentID
		next.NextMapSegmentID = old.NextMapSegmentID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := maintenance.Load(dir); err != nil || found {
		t.Fatalf("maintenance journal found=%v error=%v", found, err)
	}
	temps, err := filepath.Glob(filepath.Join(dir, "mapping", ".MAP-GC-*.tmp"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("temporary files=%v error=%v", temps, err)
	}
	if err := mapping.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMappingGCPropagatesMaintenanceJournalHook(t *testing.T) {
	for _, point := range []failpoint.Point{maintenance.PointBeforeWrite, maintenance.PointBeforeRemove} {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir, mapping, _, id, want := mappingGCSyscallFixture(t, false, nil)
			mapping.SetHook(errorAtMappingGCPoint(point, syscall.EIO))
			if _, err := mapping.Compact(context.Background()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Compact error=%v", err)
			}
			_ = mapping.Close()
			assertMappingGCRecovery(t, dir, id, want)
		})
	}
}

type mappingGCCause struct {
	name string
	err  error
}

func mappingGCSyscallCauses() []mappingGCCause {
	return []mappingGCCause{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
}

func errorAtMappingGCPoint(point failpoint.Point, cause error) failpoint.Hook {
	return failpoint.Func(func(got failpoint.Point) error {
		if got == point {
			return cause
		}
		return nil
	})
}

func mappingGCSyscallFixture(t *testing.T, forceSealedOutput bool, hook failpoint.Hook) (string, *Mapping, storeformat.Manifest, base.ID, base.VAddr) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	segmentSize := uint64(1 << 20)
	entries := 1
	if forceSealedOutput {
		segmentSize = 16 << 10
		entries = 96
	}
	hard := storeformat.HardLimits{
		SegmentSize: segmentSize, MaxValueSize: 1024, MaxBatchBytes: 1 << 20,
		MaxBatchMutations: 128, MaxBatchConditions: 64, MaxOpenBatches: 64,
		IDReserveSize: 64, BatchIDReserveSize: 64,
	}
	initial, err := initialize.Create(dir, hard)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := catalog.NewWithHook(dir, initial, hook)
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := OpenWithHook(dir, initial, 64<<10, hook, manager)
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]api.Change, 0, entries)
	for i := 0; i < entries; i++ {
		id := base.ID(uint64(i+1) << 20)
		addr, err := base.NewVAddr(1, base.FirstContentOffset+uint32(i*8))
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, api.Change{RecordID: id, NewAddr: addr})
	}
	if _, err := mapping.Apply(1, api.ApplyUserCommit, changes); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := mapping.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	root, err := mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	installed, err := manager.Install(0, func(next *storeformat.Manifest) error {
		next.MappingRoot = root
		next.CoveredCommitSeq = 1
		next.StatsCoveredCommitSeq = 1
		next.NextCommitSeq = 2
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	return dir, mapping, installed, changes[0].RecordID, changes[0].NewAddr
}

func assertMappingGCRecovery(t *testing.T, dir string, id base.ID, want base.VAddr) {
	t.Helper()
	current, err := initialize.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverMappingRotation(dir, current)
	if err != nil {
		t.Fatalf("retry recovery: %v", err)
	}
	if _, found, err := maintenance.Load(dir); err != nil || found {
		t.Fatalf("maintenance journal found=%v error=%v", found, err)
	}
	opened, err := OpenReadOnly(dir, recovered, 64<<10)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	got, found, lookupErr := opened.Lookup(id)
	closeErr := opened.Close()
	if lookupErr != nil || !found || got != want || closeErr != nil {
		t.Fatalf("lookup addr=%x found=%v lookup_error=%v close_error=%v", got, found, lookupErr, closeErr)
	}
	for _, directory := range []string{"mapping", "trash"} {
		entries, err := os.ReadDir(filepath.Join(dir, directory))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if directory == "trash" || filepath.Ext(entry.Name()) == ".tmp" {
				t.Fatalf("residual %s/%s", directory, entry.Name())
			}
		}
	}
}
