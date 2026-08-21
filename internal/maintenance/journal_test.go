package maintenance

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

func TestInstallLoadRemove(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "journal"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := storeformat.MaintenanceJournal{
		Generation: 2, StoreUUID: base.StoreUUID{1}, OperationID: [16]byte{1},
		OperationType: storeformat.MaintenanceDataGC, Phase: 1, OldManifestGeneration: 1,
		SourceFiles: []storeformat.JournalFileRef{{Kind: storeformat.FileKindData, State: storeformat.FileStateSealed, FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 2}},
	}
	if err := Install(root, journal); err != nil {
		t.Fatal(err)
	}
	got, found, err := Load(root)
	if err != nil || !found || got.Generation != journal.Generation || got.OperationID != journal.OperationID {
		t.Fatalf("journal=%+v found=%v error=%v", got, found, err)
	}
	if err := Remove(root); err != nil {
		t.Fatal(err)
	}
	if _, found, err := Load(root); err != nil || found {
		t.Fatalf("found=%v error=%v", found, err)
	}
}

func TestInstallRejectsDifferentConcurrentOperation(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "journal"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal := storeformat.MaintenanceJournal{
		Generation: 2, StoreUUID: base.StoreUUID{1}, OperationID: [16]byte{1},
		OperationType: storeformat.MaintenanceDataGC, Phase: 1, OldManifestGeneration: 1,
	}
	if err := Install(root, journal); err != nil {
		t.Fatal(err)
	}
	other := journal
	other.OperationID = [16]byte{2}
	if err := Install(root, other); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("error=%v", err)
	}
}

func TestJournalSyscallErrorsPreserveCause(t *testing.T) {
	installPoints := []failpoint.Point{
		PointBeforeTempRemove, PointBeforeWrite, PointBeforeFileSync,
		PointBeforeRename, PointBeforeDirSync,
	}
	removePoints := []failpoint.Point{
		PointBeforeRemove, PointBeforeRemoveTemp, PointBeforeRemoveDirSync,
	}
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range installPoints {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				root, journal := newJournalFixture(t)
				hook := failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				})
				if err := InstallWithHook(root, journal, hook); !errors.Is(err, cause.err) {
					t.Fatalf("Install error=%v", err)
				}
				if err := Remove(root); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	for _, point := range removePoints {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				root, journal := newJournalFixture(t)
				if err := Install(root, journal); err != nil {
					t.Fatal(err)
				}
				hook := failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				})
				if err := RemoveWithHook(root, hook); !errors.Is(err, cause.err) {
					t.Fatalf("Remove error=%v", err)
				}
				if err := Remove(root); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestLoadRemovesRegularOrphanTempAndRejectsSymlink(t *testing.T) {
	t.Run("regular", func(t *testing.T) {
		root, _ := newJournalFixture(t)
		temp := filepath.Join(root, "journal", ".MAINTENANCE.tmp")
		if err := os.WriteFile(temp, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, found, err := Load(root); err != nil || found {
			t.Fatalf("found=%v error=%v", found, err)
		}
		if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan temp remains: %v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		root, _ := newJournalFixture(t)
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(root, "journal", ".MAINTENANCE.tmp")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := Load(root); !errors.Is(err, base.ErrCorrupt) {
			t.Fatalf("Load error=%v", err)
		}
		data, err := os.ReadFile(target)
		if err != nil || string(data) != "keep" {
			t.Fatalf("target=%q error=%v", data, err)
		}
	})
}

func newJournalFixture(t *testing.T) (string, storeformat.MaintenanceJournal) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "journal"), 0o700); err != nil {
		t.Fatal(err)
	}
	return root, storeformat.MaintenanceJournal{
		Generation: 2, StoreUUID: base.StoreUUID{1}, OperationID: [16]byte{1},
		OperationType: storeformat.MaintenanceDataGC, Phase: 1, OldManifestGeneration: 1,
	}
}
