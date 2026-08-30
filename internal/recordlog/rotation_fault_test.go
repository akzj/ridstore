package recordlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestRuntimeRotationFaultsConvergeFromDurableEvidence(t *testing.T) {
	tests := []struct {
		point       FaultPoint
		journalSeen bool
	}{
		{FaultBeforeJournalWrite, false},
		{FaultBeforeJournalSync, false},
		{FaultBeforeJournalRename, false},
		{FaultBeforeJournalDirSync, true},
		{FaultBeforeFooterWrite, false},
		{FaultBeforeFooterSync, false},
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
			state := initialCatalog(512, 256)
			if err := CreateInitialSegment(root, state.LogID, state.SegmentSize); err != nil {
				t.Fatal(err)
			}
			catalog := &memoryCatalog{state: state}
			injected := errors.New("injected rotation failure")
			log, err := OpenWithFaultHook(root, testLogConfig(), catalog, func(point FaultPoint) error {
				if point == test.point {
					return injected
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			first, err := log.Append(context.Background(), make([]byte, 200), false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := log.Append(context.Background(), make([]byte, 200), true); !errors.Is(err, ErrPoisoned) || !errors.Is(err, injected) {
				t.Fatalf("rotation err=%v", err)
			}
			_ = log.Close()

			reopened, err := Open(root, testLogConfig(), catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			wantActive, wantGeneration, wantSealed := SegmentID(1), uint64(1), 0
			if test.journalSeen {
				wantActive, wantGeneration, wantSealed = 2, 2, 1
			}
			got := catalog.SnapshotRecordLog()
			if got.ActiveSegmentID != wantActive || got.NextSegmentID != wantActive+1 || got.Generation != wantGeneration || len(got.SealedSegments) != wantSealed {
				t.Fatalf("catalog=%+v", got)
			}
			if value, err := reopened.Read(context.Background(), first.Addr); err != nil || len(value) != 200 {
				t.Fatalf("read old len=%d err=%v", len(value), err)
			}
			assertRecordRotationFiles(t, root, wantActive)
		})
	}
}

func TestRotationSyncsPreparedFooterBeforePublishingJournal(t *testing.T) {
	root := t.TempDir()
	state := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, state.LogID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("stop before journal write")
	var order []FaultPoint
	log, err := OpenWithFaultHook(root, testLogConfig(), &memoryCatalog{state: state}, func(point FaultPoint) error {
		if point == FaultBeforeDataSync || point == FaultBeforeFooterSync || point == FaultBeforeJournalWrite {
			order = append(order, point)
		}
		if point == FaultBeforeJournalWrite {
			return injected
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), false); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); !errors.Is(err, injected) {
		t.Fatalf("rotation err=%v", err)
	}
	_ = log.Close()
	want := []FaultPoint{FaultBeforeFooterSync, FaultBeforeJournalWrite}
	if !slices.Equal(order, want) {
		t.Fatalf("fault order=%v want=%v", order, want)
	}
}

func TestCatalogFailureDuringRotationIsRetriedByOpen(t *testing.T) {
	root := t.TempDir()
	state := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, state.LogID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("catalog install failure")
	catalog := &memoryCatalog{state: state, installErr: injected}
	log, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	first, err := log.Append(context.Background(), make([]byte, 200), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); !errors.Is(err, ErrPoisoned) || !errors.Is(err, injected) {
		t.Fatalf("rotation err=%v", err)
	}
	_ = log.Close()

	reopened, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got := catalog.SnapshotRecordLog()
	if got.Generation != 2 || got.ActiveSegmentID != 2 || got.NextSegmentID != 3 || len(got.SealedSegments) != 1 {
		t.Fatalf("catalog=%+v", got)
	}
	if _, err := reopened.Read(context.Background(), first.Addr); err != nil {
		t.Fatal(err)
	}
	assertRecordRotationFiles(t, root, 2)
}

func TestRotationRecoveryFailureIsRetryable(t *testing.T) {
	points := []FaultPoint{
		FaultBeforeTailTruncate,
		FaultBeforeTailSync,
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
			root, catalog, first := prepareRecordRotationJournal(t)
			injected := errors.New("injected recovery failure")
			if _, err := OpenWithFaultHook(root, testLogConfig(), catalog, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("first recovery err=%v", err)
			}
			reopened, err := Open(root, testLogConfig(), catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got := catalog.SnapshotRecordLog()
			if got.Generation != 2 || got.ActiveSegmentID != 2 || got.NextSegmentID != 3 || len(got.SealedSegments) != 1 {
				t.Fatalf("catalog=%+v", got)
			}
			if _, err := reopened.Read(context.Background(), first.Addr); err != nil {
				t.Fatal(err)
			}
			assertRecordRotationFiles(t, root, 2)
		})
	}
}

func TestUnpublishedRotationTempCleanupIsRetryable(t *testing.T) {
	for _, point := range []FaultPoint{FaultBeforeJournalRemove, FaultBeforeCleanupDirSync} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			state := initialCatalog(512, 256)
			if err := CreateInitialSegment(root, state.LogID, state.SegmentSize); err != nil {
				t.Fatal(err)
			}
			catalog := &memoryCatalog{state: state}
			journal := rotationJournal{
				BaseGeneration: 1, LogID: state.LogID, SegmentSize: state.SegmentSize,
				Old: SegmentSummary{SegmentID: 1, ValidEnd: SegmentHeaderSize}, NewActive: 2, NextSegmentID: 3,
			}
			stopPublish := errors.New("leave journal temp")
			if err := installRotationJournal(root, journal, osFileBackend{}, func(got FaultPoint) error {
				if got == FaultBeforeJournalRename {
					return stopPublish
				}
				return nil
			}); !errors.Is(err, stopPublish) {
				t.Fatalf("install err=%v", err)
			}
			injected := errors.New("cleanup failure")
			if _, err := OpenWithFaultHook(root, testLogConfig(), catalog, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("cleanup err=%v", err)
			}
			reopened, err := Open(root, testLogConfig(), catalog)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertRecordRotationFiles(t, root, 1)
		})
	}
}

func prepareRecordLog(t *testing.T) (string, *memoryCatalog, *Log) {
	t.Helper()
	root := t.TempDir()
	state := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, state.LogID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: state}
	log, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	return root, catalog, log
}

func prepareRecordRotationJournal(t *testing.T) (string, *memoryCatalog, AppendResult) {
	t.Helper()
	root, catalog, log := prepareRecordLog(t)
	first, err := log.Append(context.Background(), make([]byte, 200), true)
	if err != nil {
		t.Fatal(err)
	}
	journal := rotationJournal{
		BaseGeneration: 1, LogID: catalog.state.LogID, SegmentSize: catalog.state.SegmentSize,
		Old: SegmentSummary{
			SegmentID: 1, ValidEnd: first.End.Offset, RecordCount: 1,
			FirstAddr: first.Addr, LastAddr: first.Addr,
		},
		NewActive: 2, NextSegmentID: 3,
	}
	if err := installRotationJournal(root, journal, osFileBackend{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	footer, err := EncodeSegmentFooter(SegmentFooter{
		SegmentID: 1, DataEnd: journal.Old.ValidEnd, FirstAddr: journal.Old.FirstAddr,
		LastAddr: journal.Old.LastAddr, RecordCount: journal.Old.RecordCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(recordsPath(root), activeSegmentName(1)), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(footer[:11], int64(journal.Old.ValidEnd)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return root, catalog, first
}

func assertRecordRotationFiles(t *testing.T, root string, active SegmentID) {
	t.Helper()
	journalEntries, err := os.ReadDir(journalDirectory(root))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if len(journalEntries) != 0 {
		t.Fatalf("journal artifacts=%v", entryNames(journalEntries))
	}
	dir := recordsPath(root)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := entryNames(entries)
	want := []string{activeSegmentName(1)}
	if active == 1 {
		if !slices.Equal(got, want) {
			t.Fatalf("record files=%v want=%v", got, want)
		}
	} else {
		want = []string{activeSegmentName(2), sealedSegmentName(1)}
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("record files=%v want=%v", got, want)
		}
	}
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i := range entries {
		names[i] = entries[i].Name()
	}
	slices.Sort(names)
	return names
}
