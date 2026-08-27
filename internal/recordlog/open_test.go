package recordlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type memoryCatalog struct {
	mu         sync.Mutex
	state      CatalogSnapshot
	installErr error
}

func (c *memoryCatalog) SnapshotRecordLog() CatalogSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.Clone()
}

func (c *memoryCatalog) InstallRecordLogRotation(expect uint64, sealed SegmentSummary, newActive, next SegmentID) (CatalogSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.installErr != nil {
		err := c.installErr
		c.installErr = nil
		return CatalogSnapshot{}, err
	}
	if expect != c.state.Generation {
		return CatalogSnapshot{}, errors.New("generation conflict")
	}
	if sealed.SegmentID != c.state.ActiveSegmentID || newActive != c.state.NextSegmentID || next != newActive+1 {
		return CatalogSnapshot{}, ErrInvalidConfig
	}
	c.state.Generation++
	c.state.SealedSegments = append(c.state.SealedSegments, sealed)
	c.state.ActiveSegmentID = newActive
	c.state.NextSegmentID = next
	return c.state.Clone(), nil
}

func (c *memoryCatalog) RemoveRecordLogSegment(expect uint64, sealed SegmentSummary) (CatalogSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if expect != c.state.Generation {
		return CatalogSnapshot{}, errors.New("generation conflict")
	}
	index := -1
	for i, item := range c.state.SealedSegments {
		if item == sealed {
			index = i
			break
		}
	}
	if index < 0 {
		return CatalogSnapshot{}, ErrSegmentMissing
	}
	c.state.SealedSegments = append(c.state.SealedSegments[:index:index], c.state.SealedSegments[index+1:]...)
	c.state.Generation++
	return c.state.Clone(), nil
}

func initialCatalog(segmentSize, maxPayload uint32) CatalogSnapshot {
	return CatalogSnapshot{
		Generation: 1, LogID: LogID{9, 8, 7, 6}, SegmentSize: segmentSize, MaxPayloadBytes: maxPayload,
		ActiveSegmentID: 1, NextSegmentID: 2,
	}
}

func TestOpenUsesCatalogAndDurablyRotates(t *testing.T) {
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
	first, err := log.Append(context.Background(), make([]byte, 200), false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := log.Append(context.Background(), make([]byte, 200), true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Addr.SegmentID() != 1 || second.Addr.SegmentID() != 2 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	installed := catalog.SnapshotRecordLog()
	if installed.Generation != 2 || installed.ActiveSegmentID != 2 || len(installed.SealedSegments) != 1 {
		t.Fatalf("catalog=%+v", installed)
	}
	var scanned []string
	if err := log.ScanSegment(context.Background(), 1, func(result AppendResult, payload []byte) error {
		if result.Addr.SegmentID() != 1 {
			t.Fatalf("scanned addr=%v", result.Addr)
		}
		scanned = append(scanned, string(payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(scanned) != 1 || len(scanned[0]) != 200 {
		t.Fatalf("scanned=%d", len(scanned))
	}
	if err := log.ScanSegment(context.Background(), 2, func(AppendResult, []byte) error { return nil }); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("scan active err=%v", err)
	}
	if _, err := os.Stat(rotationJournalPath(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal remains: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Read(context.Background(), first.Addr); err != nil || len(got) != 200 {
		t.Fatalf("read old len=%d err=%v", len(got), err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestScanSegmentWaitsForPublishedRotation(t *testing.T) {
	log, registry, sealed, active, want := catalogPublishedRotation(t)
	result := make(chan error, 1)
	go func() {
		result <- log.ScanSegment(context.Background(), 1, func(got AppendResult, payload []byte) error {
			if got != want || string(payload) != "before rotation" {
				t.Errorf("got=%+v payload=%q", got, payload)
			}
			return nil
		})
	}()
	select {
	case err := <-result:
		t.Fatalf("ScanSegment returned before Registry publication: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := registry.publishRotation(1, sealed, active); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ScanSegment did not observe Registry publication")
	}
	if err := registry.close(); err != nil {
		t.Fatal(err)
	}
}

func TestScanSegmentWaitForRotationHonorsCancellation(t *testing.T) {
	log, registry, sealed, active, _ := catalogPublishedRotation(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- log.ScanSegment(ctx, 1, func(AppendResult, []byte) error { return nil })
	}()
	select {
	case err := <-result:
		t.Fatalf("ScanSegment returned before cancellation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ScanSegment err=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ScanSegment ignored cancellation")
	}
	if err := registry.publishRotation(1, sealed, active); err != nil {
		t.Fatal(err)
	}
	if err := registry.close(); err != nil {
		t.Fatal(err)
	}
}

func catalogPublishedRotation(t *testing.T) (*Log, *segmentRegistry, *sealedSegment, *activeSegment, AppendResult) {
	t.Helper()
	root := t.TempDir()
	state := initialCatalog(16<<10, 8<<10)
	old, err := createActiveSegment(root, state.headerFor(1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := appendTestRecords(t, old, []byte("before rotation"))[0]
	registry, err := newSegmentRegistry(old, nil)
	if err != nil {
		t.Fatal(err)
	}
	sealed, summary, err := old.seal()
	if err != nil {
		t.Fatal(err)
	}
	active, err := createActiveSegment(root, state.headerFor(2), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: state}
	if _, err := catalog.InstallRecordLogRotation(state.Generation, summary, 2, 3); err != nil {
		t.Fatal(err)
	}
	return &Log{catalog: catalog, registry: registry}, registry, sealed, active, want
}

func TestRecoverPreparedRotationWithPartialFooter(t *testing.T) {
	root := t.TempDir()
	state := initialCatalog(1024, 512)
	active, err := createActiveSegment(root, state.headerFor(1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, active, []byte("durable prefix"))
	if err := active.sync(); err != nil {
		t.Fatal(err)
	}
	summary := active.summary()
	journal := rotationJournal{
		BaseGeneration: 1, LogID: state.LogID, SegmentSize: state.SegmentSize,
		Old: summary, NewActive: 2, NextSegmentID: 3,
	}
	if err := installRotationJournal(root, journal, osFileBackend{}, nil); err != nil {
		t.Fatal(err)
	}
	footer, err := EncodeSegmentFooter(SegmentFooter{SegmentID: 1, DataEnd: summary.ValidEnd, FirstAddr: summary.FirstAddr, LastAddr: summary.LastAddr, RecordCount: summary.RecordCount})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := active.file.WriteAt(footer[:11], int64(summary.ValidEnd)); err != nil {
		t.Fatal(err)
	}
	if err := active.close(); err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: state}
	recovered, err := recoverRotation(root, catalog, state, osFileBackend{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Generation != 2 || recovered.ActiveSegmentID != 2 || len(recovered.SealedSegments) != 1 || recovered.SealedSegments[0] != summary {
		t.Fatalf("recovered=%+v", recovered)
	}
	if _, err := os.Stat(filepath.Join(recordsPath(root), sealedSegmentName(1))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(recordsPath(root), activeSegmentName(2))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rotationJournalPath(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal remains: %v", err)
	}
}

func TestRetireRemovesCatalogBeforePhysicalFile(t *testing.T) {
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
	if _, err := log.Append(context.Background(), make([]byte, 200), false); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); err != nil {
		t.Fatal(err)
	}
	generation := catalog.SnapshotRecordLog().Generation
	if err := log.RetireSegment(context.Background(), 1, generation); err != nil {
		t.Fatal(err)
	}
	if _, ok := catalog.SnapshotRecordLog().sealedSummary(1); ok {
		t.Fatal("retired segment remains in catalog")
	}
	if _, err := os.Stat(filepath.Join(recordsPath(root), sealedSegmentName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sealed file remains: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupRetiredSegmentIsIdempotentAcrossRestartStates(t *testing.T) {
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
	if _, err := log.Append(context.Background(), make([]byte, 200), false); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := CleanupRetiredSegment(root, 1, 9); err != nil {
		t.Fatal(err)
	}
	if err := CleanupRetiredSegment(root, 1, 9); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(recordsPath(root), sealedSegmentName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sealed file remains: %v", err)
	}
}

func TestRetiredSegmentCleanupFaultsConvergeOnRetry(t *testing.T) {
	points := []FaultPoint{
		FaultBeforeTrashRootSync,
		FaultBeforeRetireRename,
		FaultBeforeRecordsDirSync,
		FaultBeforeTrashDirSync,
		FaultBeforeTrashRemove,
		FaultBeforeTrashFinalSync,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			root := prepareSealedRetirementFile(t)
			injected := errors.New("injected retirement failure")
			err := CleanupRetiredSegmentWithFaultHook(root, 1, 9, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("cleanup err=%v", err)
			}
			if err := CleanupRetiredSegment(root, 1, 9); err != nil {
				t.Fatalf("retry cleanup: %v", err)
			}
			if _, err := os.Stat(filepath.Join(recordsPath(root), sealedSegmentName(1))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("sealed file remains: %v", err)
			}
			trash := filepath.Join(root, "trash", sealedSegmentName(1)+".g9.trash")
			if _, err := os.Stat(trash); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("trash file remains: %v", err)
			}
		})
	}
}

func TestRetireFaultAfterCatalogRemovalPoisonsLog(t *testing.T) {
	root := t.TempDir()
	state := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, state.LogID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: state}
	injected := errors.New("injected retire failure")
	log, err := OpenWithFaultHook(root, testLogConfig(), catalog, func(got FaultPoint) error {
		if got == FaultBeforeRetireRename {
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
	if _, err := log.Append(context.Background(), make([]byte, 200), true); err != nil {
		t.Fatal(err)
	}
	generation := catalog.SnapshotRecordLog().Generation
	if err := log.RetireSegment(context.Background(), 1, generation); !errors.Is(err, ErrPoisoned) || !errors.Is(err, injected) {
		t.Fatalf("retire err=%v", err)
	}
	if _, ok := catalog.SnapshotRecordLog().sealedSummary(1); ok {
		t.Fatal("Catalog removal did not precede physical cleanup")
	}
	if _, err := log.Append(context.Background(), []byte("rejected"), true); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append err=%v", err)
	}
	_ = log.Close()
	if err := CleanupRetiredSegment(root, 1, catalog.SnapshotRecordLog().Generation); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupRetiredSegmentRejectsSymlinkTrashDirectory(t *testing.T) {
	root := prepareSealedRetirementFile(t)
	if err := os.Symlink("records", filepath.Join(root, "trash")); err != nil {
		t.Fatal(err)
	}
	if err := CleanupRetiredSegment(root, 1, 9); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cleanup err=%v", err)
	}
}

func prepareSealedRetirementFile(t *testing.T) string {
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
	if _, err := log.Append(context.Background(), make([]byte, 200), false); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestRotationJournalRejectsCorruption(t *testing.T) {
	state := initialCatalog(1024, 512)
	addr, err := NewVAddr(1, SegmentHeaderSize, 64)
	if err != nil {
		t.Fatal(err)
	}
	journal := rotationJournal{
		BaseGeneration: 1, LogID: state.LogID, SegmentSize: state.SegmentSize,
		Old:       SegmentSummary{SegmentID: 1, ValidEnd: SegmentHeaderSize + 64, RecordCount: 1, FirstAddr: addr, LastAddr: addr},
		NewActive: 2, NextSegmentID: 3,
	}
	encoded, err := encodeRotationJournal(journal)
	if err != nil {
		t.Fatal(err)
	}
	encoded[64] ^= 0xff
	if _, err := decodeRotationJournal(encoded[:]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}
