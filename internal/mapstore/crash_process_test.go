package mapstore

import (
	"os"
	"os/exec"
	"testing"

	"github.com/akzj/ridstore/internal/model"
)

const (
	mapCrashHelperEnv = "RIDSTORE_MAPSTORE_CRASH_HELPER"
	mapCrashRootEnv   = "RIDSTORE_MAPSTORE_CRASH_ROOT"
	mapCrashPhaseEnv  = "RIDSTORE_MAPSTORE_CRASH_PHASE"
)

func TestRotationRecoveryAcrossProcessExit(t *testing.T) {
	for _, phase := range []string{"journal", "sealed", "new-active"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestMapStoreRotationCrashHelper$")
			command.Env = append(os.Environ(), mapCrashHelperEnv+"=1", mapCrashRootEnv+"="+root, mapCrashPhaseEnv+"="+phase)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("helper: %v\n%s", err, output)
			}
			catalog := &staticCatalog{state: initialState()}
			store, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if catalog.state.Generation != 2 || catalog.state.ActiveSegment != 2 || len(catalog.state.SealedSegments) != 1 {
				t.Fatalf("catalog=%+v", catalog.state)
			}
			first, err := model.NewMapAddr(1, SegmentHeaderSize)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Read(first); err != nil {
				t.Fatal(err)
			}
			assertRotationFiles(t, root, 2)
		})
	}
}

func TestMapStoreRotationCrashHelper(t *testing.T) {
	if os.Getenv(mapCrashHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	root := os.Getenv(mapCrashRootEnv)
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(1, 0, 1, denseSlots(t)); err != nil {
		t.Fatal(err)
	}
	journal := rotationJournal{
		BaseGeneration: state.Generation, StoreID: state.StoreID, SegmentSize: state.SegmentSize,
		Old: store.active.summary, NewActive: 2, NextSegment: 3,
	}
	if err := installRotationJournal(root, journal, nil); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(mapCrashPhaseEnv) == "journal" {
		return
	}
	sealed, err := sealActive(root, store.active, journal.Old, nil)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv(mapCrashPhaseEnv) == "sealed" {
		return
	}
	if err := sealed.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := createActive(root, state.headerFor(2), nil); err != nil {
		t.Fatal(err)
	}
}
