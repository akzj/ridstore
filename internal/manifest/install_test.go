package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
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
