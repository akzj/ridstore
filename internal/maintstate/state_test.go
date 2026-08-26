package maintstate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func testState(t *testing.T) State {
	t.Helper()
	first, _ := recordlog.NewVAddr(2, 64, 64)
	last, _ := recordlog.NewVAddr(2, 128, 64)
	replay, _ := recordlog.NewLogPos(3, 64)
	return State{
		Operation: DataRetire, StoreUUID: storecatalog.StoreUUID{1}, LogID: recordlog.LogID{2},
		BaseGeneration: 7, CoveredCommitSeq: model.CommitSeq(11), ReplayStart: replay,
		Source: recordlog.SegmentSummary{SegmentID: 2, ValidEnd: 192, RecordCount: 2, FirstAddr: first, LastAddr: last},
	}
}

func TestCodecRoundTripAndRejectsCorruption(t *testing.T) {
	want := testState(t)
	encoded, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	encoded[80] ^= 1
	if _, err := Decode(encoded[:]); err == nil {
		t.Fatal("corrupt state accepted")
	}
}

func TestInstallLoadRemove(t *testing.T) {
	root := t.TempDir()
	want := testState(t)
	if err := Install(root, want); err != nil {
		t.Fatal(err)
	}
	if err := Install(root, want); err == nil {
		t.Fatal("second active operation accepted")
	}
	got, found, err := Load(root)
	if err != nil || !found || got != want {
		t.Fatalf("got=%+v found=%v err=%v", got, found, err)
	}
	if err := Remove(root); err != nil {
		t.Fatal(err)
	}
	if _, found, err := Load(root); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if _, err := os.Stat(Path(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal remains: %v", err)
	}
}

func TestLoadRejectsSymlinkTemp(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "journal")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(dir, tempName)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(root); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}
