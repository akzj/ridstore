package mapstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/model"
)

func TestRotationJournalRoundTrip(t *testing.T) {
	want := rotationJournal{
		BaseGeneration: 4, StoreID: testStoreID(), SegmentSize: 8192,
		Old:       SegmentSummary{SegmentID: 1, ValidEnd: 4224, FirstSeq: 1, LastSeq: 1, NodeCount: 1},
		NewActive: 2, NextSegment: 3,
	}
	encoded, err := encodeRotationJournal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRotationJournal(encoded[:])
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	encoded[90] ^= 1
	if _, err := decodeRotationJournal(encoded[:]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}

func TestOpenCompletesPreparedRotation(t *testing.T) {
	for _, footerBytes := range []int{0, 10, int(SegmentFooterSize)} {
		t.Run(fmt.Sprintf("footer-bytes-%d", footerBytes), func(t *testing.T) {
			root := t.TempDir()
			state := initialState()
			if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
				t.Fatal(err)
			}
			catalog := &staticCatalog{state: state}
			store, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			var slots [NodeSlots]uint64
			for index := range slots {
				slots[index], _ = mapValue(index)
			}
			if _, err := store.Append(1, 0, 1, slots); err != nil {
				t.Fatal(err)
			}
			journal := rotationJournal{
				BaseGeneration: state.Generation, StoreID: state.StoreID, SegmentSize: state.SegmentSize,
				Old: store.active.summary, NewActive: 2, NextSegment: 3,
			}
			if err := installRotationJournal(root, journal, nil); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if footerBytes != 0 {
				footer, _ := EncodeSegmentFooter(journal.Old.footer())
				path := filepath.Join(root, mappingDirectory, activeName(1))
				file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write(footer[:footerBytes]); err != nil {
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if catalog.state.ActiveSegment != 2 || len(catalog.state.SealedSegments) != 1 {
				t.Fatalf("state=%+v", catalog.state)
			}
			if _, err := os.Stat(rotationPath(root)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("journal err=%v", err)
			}
			if _, err := os.Stat(filepath.Join(root, mappingDirectory, sealedName(1))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenAcceptsCatalogCommittedRotationAndRemovesJournal(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	var slots [NodeSlots]uint64
	for index := range slots {
		slots[index], _ = mapValue(index)
	}
	if _, err := store.Append(1, 0, 1, slots); err != nil {
		t.Fatal(err)
	}
	journal := rotationJournal{
		BaseGeneration: state.Generation, StoreID: state.StoreID, SegmentSize: state.SegmentSize,
		Old: store.active.summary, NewActive: 2, NextSegment: 3,
	}
	if err := installRotationJournal(root, journal, nil); err != nil {
		t.Fatal(err)
	}
	sealed, err := sealActive(root, store.active, journal.Old, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.file.Close(); err != nil {
		t.Fatal(err)
	}
	active, err := createActive(root, state.headerFor(2), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := active.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.InstallMapStoreRotation(1, SegmentRef{SegmentID: 1, ValidEnd: journal.Old.ValidEnd}, 2, 3); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rotationPath(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal err=%v", err)
	}
}

func TestRuntimeRotationFaultsConvergeFromDurableEvidence(t *testing.T) {
	tests := []struct {
		point       FaultPoint
		journalSeen bool
	}{
		{FaultBeforeJournalWrite, false},
		{FaultBeforeJournalSync, false},
		{FaultBeforeJournalRename, false},
		{FaultBeforeJournalDirSync, true},
		{FaultBeforeFooterWrite, true},
		{FaultBeforeFooterSync, true},
		{FaultBeforeSealRename, true},
		{FaultBeforeSealDirSync, true},
		{FaultBeforeHeaderWrite, true},
		{FaultBeforeHeaderSync, true},
		{FaultBeforeCreateRename, true},
		{FaultBeforeCreateDirSync, true},
		{FaultBeforeJournalRemove, true},
		{FaultBeforeCleanupDirSync, true},
	}
	for _, test := range tests {
		t.Run(string(test.point), func(t *testing.T) {
			root := t.TempDir()
			state := initialState()
			if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
				t.Fatal(err)
			}
			catalog := &staticCatalog{state: state}
			injected := errors.New("injected rotation failure")
			store, err := OpenWithFaultHook(root, catalog, func(point FaultPoint) error {
				if point == test.point {
					return injected
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			slots := denseSlots(t)
			first, err := store.Append(1, 0, 1, slots)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(1, 1, 2, slots); !errors.Is(err, ErrPoisoned) || !errors.Is(err, injected) {
				t.Fatalf("rotation err=%v", err)
			}
			_ = store.Close()

			reopened, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			wantActive := model.MapSegmentID(1)
			wantGeneration := uint64(1)
			wantSealed := 0
			if test.journalSeen {
				wantActive = 2
				wantGeneration = 2
				wantSealed = 1
			}
			if catalog.state.ActiveSegment != wantActive || catalog.state.Generation != wantGeneration || len(catalog.state.SealedSegments) != wantSealed {
				t.Fatalf("catalog=%+v", catalog.state)
			}
			if _, err := reopened.Read(first); err != nil {
				t.Fatal(err)
			}
			assertRotationFiles(t, root, wantActive)
		})
	}
}

func TestCatalogFailureDuringRotationIsRetriedByOpen(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("catalog install failure")
	catalog := &staticCatalog{state: state, installErr: injected}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	slots := denseSlots(t)
	first, err := store.Append(1, 0, 1, slots)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(1, 1, 2, slots); !errors.Is(err, ErrPoisoned) || !errors.Is(err, injected) {
		t.Fatalf("rotation err=%v", err)
	}
	_ = store.Close()

	reopened, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if catalog.state.Generation != 2 || catalog.state.ActiveSegment != 2 || len(catalog.state.SealedSegments) != 1 {
		t.Fatalf("catalog=%+v", catalog.state)
	}
	if _, err := reopened.Read(first); err != nil {
		t.Fatal(err)
	}
	assertRotationFiles(t, root, 2)
}

func TestRotationRecoveryFailureIsRetryable(t *testing.T) {
	points := []FaultPoint{
		FaultBeforeFooterWrite,
		FaultBeforeFooterSync,
		FaultBeforeSealRename,
		FaultBeforeSealDirSync,
		FaultBeforeHeaderWrite,
		FaultBeforeHeaderSync,
		FaultBeforeCreateRename,
		FaultBeforeCreateDirSync,
		FaultBeforeJournalRemove,
		FaultBeforeCleanupDirSync,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			root, catalog, first := prepareRotationJournal(t)
			injected := errors.New("injected recovery failure")
			if _, err := OpenWithFaultHook(root, catalog, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("first recovery err=%v", err)
			}
			reopened, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if catalog.state.Generation != 2 || catalog.state.ActiveSegment != 2 || len(catalog.state.SealedSegments) != 1 {
				t.Fatalf("catalog=%+v", catalog.state)
			}
			if _, err := reopened.Read(first); err != nil {
				t.Fatal(err)
			}
			assertRotationFiles(t, root, 2)
		})
	}
}

func TestUnpublishedRotationTempCleanupIsRetryable(t *testing.T) {
	for _, point := range []FaultPoint{FaultBeforeJournalRemove, FaultBeforeCleanupDirSync} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
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
			stopPublish := errors.New("leave journal temp")
			if err := installRotationJournal(root, journal, func(got FaultPoint) error {
				if got == FaultBeforeJournalRename {
					return stopPublish
				}
				return nil
			}); !errors.Is(err, stopPublish) {
				t.Fatalf("install err=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("cleanup failure")
			if _, err := OpenWithFaultHook(root, catalog, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("cleanup err=%v", err)
			}
			reopened, err := Open(root, catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if catalog.state.Generation != 1 || catalog.state.ActiveSegment != 1 || len(catalog.state.SealedSegments) != 0 {
				t.Fatalf("catalog=%+v", catalog.state)
			}
			assertRotationFiles(t, root, 1)
		})
	}
}

func prepareRotationJournal(t *testing.T) (string, *staticCatalog, model.MapAddr) {
	t.Helper()
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Append(1, 0, 1, denseSlots(t))
	if err != nil {
		t.Fatal(err)
	}
	journal := rotationJournal{
		BaseGeneration: state.Generation, StoreID: state.StoreID, SegmentSize: state.SegmentSize,
		Old: store.active.summary, NewActive: 2, NextSegment: 3,
	}
	if err := installRotationJournal(root, journal, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return root, catalog, first
}

func denseSlots(t *testing.T) [NodeSlots]uint64 {
	t.Helper()
	var slots [NodeSlots]uint64
	for index := range slots {
		value, err := mapValue(index)
		if err != nil {
			t.Fatal(err)
		}
		slots[index] = value
	}
	return slots
}

func assertRotationFiles(t *testing.T, root string, active model.MapSegmentID) {
	t.Helper()
	dir := filepath.Join(root, mappingDirectory)
	if _, err := os.Stat(rotationPath(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rotation journal err=%v", err)
	}
	if _, err := os.Stat(rotationTempPath(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rotation temp err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, activeName(active))); err != nil {
		t.Fatal(err)
	}
	if active == 1 {
		if _, err := os.Stat(filepath.Join(dir, sealedName(1))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected sealed file err=%v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, activeName(2))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected next active err=%v", err)
		}
		return
	}
	if _, err := os.Stat(filepath.Join(dir, sealedName(1))); err != nil {
		t.Fatal(err)
	}
}
