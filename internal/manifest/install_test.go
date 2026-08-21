package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func validManifest(t *testing.T, generation uint64) storeformat.Manifest {
	t.Helper()
	replay, err := base.NewLogPos(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return storeformat.Manifest{
		Generation: generation,
		StoreUUID:  base.StoreUUID{1, 2, 3, 4},
		HardLimits: storeformat.HardLimits{
			SegmentSize: 256 << 20, MaxValueSize: 64 << 20, MaxBatchBytes: 256 << 20,
			MaxBatchMutations: 1_000_000, MaxBatchConditions: 1_000_000,
			MaxOpenBatches: 1024, IDReserveSize: 1 << 20, BatchIDReserveSize: 1 << 16,
		},
		NextDataSegmentID: 2, NextMapSegmentID: 2,
		ActiveDataSegmentID: 1, ActiveMapSegmentID: 1,
		ReplayStart: replay, ReservedIDHighExclusive: 1,
		ReservedBatchIDHighExclusive: 1, IssuedBatchIDHighExclusiveAtCut: 1,
		NextFrameSeq: 1, NextCommitSeq: 1,
	}
}

func newStoreDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ManifestDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestInstallAndLoadCurrent(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	m := validManifest(t, 1)
	installer := Installer{Dir: dir}
	if err := installer.Install(m); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCurrent(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("got=%+v want=%+v", got, m)
	}
	if err := installer.Install(m); err != nil {
		t.Fatalf("idempotent install: %v", err)
	}
}

func TestInstallCrashStepsAreRetryable(t *testing.T) {
	t.Parallel()
	steps := []Step{
		StepManifestWritten, StepManifestFileSynced, StepManifestRenamed,
		StepManifestDirSynced, StepCurrentWritten, StepCurrentFileSynced,
		StepCurrentRenamed, StepRootDirSynced,
	}
	stop := errors.New("stop")
	for _, step := range steps {
		step := step
		t.Run(string(step), func(t *testing.T) {
			dir := newStoreDir(t)
			m := validManifest(t, 1)
			installer := Installer{Dir: dir, Hook: func(got Step) error {
				if got == step {
					return stop
				}
				return nil
			}}
			if err := installer.Install(m); !errors.Is(err, stop) {
				t.Fatalf("error=%v", err)
			}
			if err := (Installer{Dir: dir}).Install(m); err != nil {
				t.Fatalf("retry: %v", err)
			}
			got, err := LoadCurrent(dir)
			if err != nil || got.Generation != 1 {
				t.Fatalf("load=%+v error=%v", got, err)
			}
		})
	}
}

func TestInstallSyscallErrorsAreClassifiedAndRetryable(t *testing.T) {
	tests := []struct {
		point             failpoint.Point
		injected          error
		visibleGeneration uint64
	}{
		{PointBeforeManifestWrite, syscall.ENOSPC, 1},
		{PointBeforeManifestFileSync, syscall.EIO, 1},
		{PointBeforeManifestRename, syscall.EACCES, 1},
		{PointBeforeManifestDirSync, syscall.EIO, 1},
		{PointBeforeCurrentWrite, syscall.ENOSPC, 1},
		{PointBeforeCurrentFileSync, syscall.EIO, 1},
		{PointBeforeCurrentRename, syscall.EACCES, 1},
		{PointBeforeRootDirSync, syscall.EIO, 2},
	}
	for _, test := range tests {
		test := test
		t.Run(string(test.point), func(t *testing.T) {
			dir := newStoreDir(t)
			if err := (Installer{Dir: dir}).Install(validManifest(t, 1)); err != nil {
				t.Fatal(err)
			}
			installer := Installer{Dir: dir, FailpointHook: failpoint.Func(func(point failpoint.Point) error {
				if point == test.point {
					return test.injected
				}
				return nil
			})}
			if err := installer.Install(validManifest(t, 2)); !errors.Is(err, test.injected) {
				t.Fatalf("install error=%v want=%v", err, test.injected)
			}
			visible, err := LoadCurrent(dir)
			if err != nil || visible.Generation != test.visibleGeneration {
				t.Fatalf("visible generation=%d want=%d error=%v", visible.Generation, test.visibleGeneration, err)
			}
			if err := (Installer{Dir: dir}).Install(validManifest(t, 2)); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if err := CleanupInterruptedInstall(dir); err != nil {
				t.Fatal(err)
			}
			current, err := LoadCurrent(dir)
			if err != nil || current.Generation != 2 {
				t.Fatalf("current=%+v error=%v", current, err)
			}
			for _, path := range []string{
				filepath.Join(dir, ".CURRENT.tmp"),
				filepath.Join(dir, ManifestDirName, ManifestFileName(2)+".tmp"),
			} {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("temporary artifact remains %s: %v", path, err)
				}
			}
		})
	}
}

func TestCleanupInterruptedInstallRemovesOnlyUnpublishedTemps(t *testing.T) {
	t.Parallel()
	stop := errors.New("stop")
	for _, test := range []struct {
		step Step
		temp string
	}{
		{StepManifestFileSynced, filepath.Join(ManifestDirName, ManifestFileName(2)+".tmp")},
		{StepManifestRenamed, filepath.Join(ManifestDirName, ManifestFileName(2))},
		{StepCurrentFileSynced, ".CURRENT.tmp"},
	} {
		test := test
		t.Run(string(test.step), func(t *testing.T) {
			dir := newStoreDir(t)
			if err := (Installer{Dir: dir}).Install(validManifest(t, 1)); err != nil {
				t.Fatal(err)
			}
			installer := Installer{Dir: dir, Hook: func(step Step) error {
				if step == test.step {
					return stop
				}
				return nil
			}}
			if err := installer.Install(validManifest(t, 2)); !errors.Is(err, stop) {
				t.Fatalf("install error=%v", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, test.temp)); err != nil {
				t.Fatalf("temp missing before cleanup: %v", err)
			}
			if err := CleanupInterruptedInstall(dir); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(dir, test.temp)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temp remains after cleanup: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, ManifestDirName, ManifestFileName(2))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unpublished final Manifest remains: %v", err)
			}
			current, err := LoadCurrent(dir)
			if err != nil || current.Generation != 1 {
				t.Fatalf("current=%+v error=%v", current, err)
			}
			next := validManifest(t, 2)
			next.NextFrameSeq = 9
			if err := (Installer{Dir: dir}).Install(next); err != nil {
				t.Fatalf("new generation after cleanup: %v", err)
			}
			if err := CleanupInterruptedInstall(dir); err != nil {
				t.Fatal(err)
			}
			for generation := uint64(1); generation <= 2; generation++ {
				if _, err := os.Lstat(filepath.Join(dir, ManifestDirName, ManifestFileName(generation))); err != nil {
					t.Fatalf("published generation %d removed: %v", generation, err)
				}
			}
		})
	}
}

func TestCleanupInterruptedInstallRejectsSymlinkTemp(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	if err := (Installer{Dir: dir}).Install(validManifest(t, 1)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, CurrentFileName), filepath.Join(dir, ".CURRENT.tmp")); err != nil {
		t.Fatal(err)
	}
	if err := CleanupInterruptedInstall(dir); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestInstallRejectsGenerationContentConflict(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	m := validManifest(t, 1)
	installer := Installer{Dir: dir}
	if err := installer.Install(m); err != nil {
		t.Fatal(err)
	}
	m.NextFrameSeq = 2
	if err := installer.Install(m); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestInstallRejectsGenerationRollback(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	installer := Installer{Dir: dir}
	if err := installer.Install(validManifest(t, 1)); err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(validManifest(t, 2)); err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(validManifest(t, 1)); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("rollback error=%v", err)
	}
}

func TestInstallRejectsGenerationGapAndUUIDMismatch(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	installer := Installer{Dir: dir}
	if err := installer.Install(validManifest(t, 1)); err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(validManifest(t, 3)); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("gap error=%v", err)
	}
	different := validManifest(t, 2)
	different.StoreUUID = base.StoreUUID{9}
	if err := installer.Install(different); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("UUID error=%v", err)
	}
}

func TestReadCurrentRejectsPath(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	if err := os.WriteFile(filepath.Join(dir, CurrentFileName), []byte("../MANIFEST-00000000000000000001\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCurrentName(dir); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestReadCurrentRejectsSymlink(t *testing.T) {
	t.Parallel()
	dir := newStoreDir(t)
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte(ManifestFileName(1)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, CurrentFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCurrentName(dir); err == nil {
		t.Fatal("symlink CURRENT unexpectedly accepted")
	}
}
