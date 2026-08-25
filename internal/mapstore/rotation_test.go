package mapstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
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
			if err := installRotationJournal(root, journal); err != nil {
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
	if err := installRotationJournal(root, journal); err != nil {
		t.Fatal(err)
	}
	sealed, err := sealActive(root, store.active, journal.Old)
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.file.Close(); err != nil {
		t.Fatal(err)
	}
	active, err := createActive(root, state.headerFor(2))
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
