package mapgcstate

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
)

func TestStateRoundTrip(t *testing.T) {
	want := testState(t)
	encoded, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
	encoded[len(encoded)-1] ^= 0xff
	if _, err := Decode(encoded); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt decode err=%v", err)
	}
}

func TestInstallLoadRemove(t *testing.T) {
	root := t.TempDir()
	want := testState(t)
	if err := Install(root, want, nil); err != nil {
		t.Fatal(err)
	}
	if found, err := RecoveryArtifacts(root); err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	got, found, err := Load(root)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v found=%v err=%v", got, found, err)
	}
	if err := Remove(root, nil); err != nil {
		t.Fatal(err)
	}
	if found, err := RecoveryArtifacts(root); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestLoadCleansUnpublishedTemp(t *testing.T) {
	root := t.TempDir()
	injected := errors.New("stop before publish")
	err := Install(root, testState(t), func(point FaultPoint) error {
		if point == FaultBeforePublishRename {
			return injected
		}
		return nil
	})
	if !errors.Is(err, injected) {
		t.Fatalf("install err=%v", err)
	}
	if _, found, err := Load(root); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "journal", tempName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp err=%v", err)
	}
}

func TestDecodeRejectsNonContiguousNewGeneration(t *testing.T) {
	state := testState(t)
	state.New.Active++
	state.New.Next++
	if _, err := Encode(state); !errors.Is(err, ErrInvalid) {
		t.Fatalf("encode err=%v", err)
	}
}

func TestInstallFaultBoundariesRemainRecoverable(t *testing.T) {
	injected := errors.New("injected")
	for _, point := range []FaultPoint{
		FaultBeforeTempCreate, FaultBeforeWrite, FaultBeforeFileSync, FaultBeforeFileClose,
		FaultBeforePublishRename, FaultBeforeJournalDirSync,
	} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			err := Install(root, testState(t), func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("install err=%v", err)
			}
			state, found, err := Load(root)
			if err != nil {
				t.Fatal(err)
			}
			if point == FaultBeforeJournalDirSync {
				if !found || !reflect.DeepEqual(state, testState(t)) {
					t.Fatalf("state=%+v found=%v", state, found)
				}
			} else if found {
				t.Fatalf("unpublished marker became visible: %+v", state)
			}
		})
	}
}

func TestRemoveFaultKeepsPublishedMarker(t *testing.T) {
	root := t.TempDir()
	if err := Install(root, testState(t), nil); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected")
	if err := Remove(root, func(point FaultPoint) error {
		if point == FaultBeforeMarkerRemove {
			return injected
		}
		return nil
	}); !errors.Is(err, injected) {
		t.Fatalf("remove err=%v", err)
	}
	if _, found, err := Load(root); err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func testState(t *testing.T) State {
	t.Helper()
	oldRoot, err := model.NewMapAddr(1, mapstore.SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	newRoot, err := model.NewMapAddr(3, mapstore.SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	return State{
		StoreID: [16]byte{1}, BaseGeneration: 9, SegmentSize: 8192, Covered: 7,
		Old: FileSet{
			Sealed: []mapstore.SegmentRef{{SegmentID: 1, ValidEnd: 128}},
			Active: 2, Next: 3, Root: oldRoot,
		},
		New: FileSet{
			Sealed: []mapstore.SegmentRef{{SegmentID: 3, ValidEnd: 128}},
			Active: 4, Next: 5, Root: newRoot,
		},
	}
}
