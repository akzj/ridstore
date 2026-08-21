package maintenance

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
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
