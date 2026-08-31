package compactionstate

import (
	"errors"
	"os"
	"testing"

	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestStateRoundTripAndPhaseUpdate(t *testing.T) {
	root := t.TempDir()
	state := testState()
	if err := Install(root, state); err != nil {
		t.Fatal(err)
	}
	if err := Install(root, state); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second install err=%v", err)
	}
	loaded, found, err := Load(root)
	if err != nil || !found || loaded.Phase != PhaseReserved || len(loaded.OutputIDs) != 1 {
		t.Fatalf("loaded=%+v found=%v err=%v", loaded, found, err)
	}
	state.Phase = PhaseOutputsPublished
	state.Outputs = []recordlog.SegmentSummary{{SegmentID: recordlog.SegmentID(^uint32(0)), ValidEnd: recordlog.SegmentHeaderSize}}
	if err := Update(root, state); err != nil {
		t.Fatal(err)
	}
	loaded, found, err = Load(root)
	if err != nil || !found || loaded.Phase != PhaseOutputsPublished || len(loaded.Outputs) != 1 {
		t.Fatalf("loaded=%+v found=%v err=%v", loaded, found, err)
	}
	if err := Remove(root); err != nil {
		t.Fatal(err)
	}
	if _, found, err := Load(root); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestLoadRejectsCorruptState(t *testing.T) {
	root := t.TempDir()
	if err := Install(root, testState()); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(Path(root))
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if err := os.WriteFile(Path(root), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(root); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("load err=%v", err)
	}
}

func TestInstallRejectsImpossibleState(t *testing.T) {
	for _, mutate := range []func(*State){
		func(state *State) { state.OutputIDs = nil },
		func(state *State) { state.Inputs = append(state.Inputs, state.Inputs[0]) },
		func(state *State) { state.OutputIDs[0] = 1 },
		func(state *State) {
			state.Outputs = []recordlog.SegmentSummary{{SegmentID: state.OutputIDs[0], ValidEnd: recordlog.SegmentHeaderSize}}
		},
		func(state *State) {
			state.Phase = PhaseOutputsPublished
			state.Outputs = []recordlog.SegmentSummary{{SegmentID: state.OutputIDs[0] - 1, ValidEnd: recordlog.SegmentHeaderSize}}
		},
	} {
		state := testState()
		mutate(&state)
		if err := Install(t.TempDir(), state); !errors.Is(err, ErrInvalid) {
			t.Fatalf("state=%+v err=%v", state, err)
		}
	}
}

func testState() State {
	return State{
		Phase: PhaseReserved, StoreUUID: storecatalog.StoreUUID{1}, LogID: recordlog.LogID{2}, BaseGeneration: 3,
		Inputs:    []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: recordlog.SegmentHeaderSize}},
		OutputIDs: []recordlog.SegmentID{recordlog.SegmentID(^uint32(0))},
	}
}
