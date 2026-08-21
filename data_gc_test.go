package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/maintenance"
	"github.com/akzj/ridstore/internal/mapping/radix"
)

func TestDataGCCandidatesRequireCheckpointSafeSealedSegment(t *testing.T) {
	replay, _ := base.NewLogPos(3, storeformat.SegmentHeaderSize)
	manifest := storeformat.Manifest{
		ReplayStart: replay,
		SealedDataSegments: []storeformat.FileSummary{
			{FileID: 1, ValidEnd: 12 << 10},
			{FileID: 2, ValidEnd: 12 << 10},
			{FileID: 3, ValidEnd: 12 << 10},
		},
		SegmentStats: []storeformat.SegmentStatsEntry{
			{SegmentID: 1, ExactLiveBytes: 6 << 10},
			{SegmentID: 2, ExactLiveBytes: 2 << 10},
		},
	}
	candidates := dataGCCandidates(manifest)
	if len(candidates) != 2 || candidates[0].FileID != 2 || candidates[1].FileID != 1 {
		t.Fatalf("candidates=%+v", candidates)
	}
}

func TestDataGCRelocatesLiveRecordsAndRecoveryReplaysCAS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stableID, err := first.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	churnID, err := first.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, stableID, []byte("stable")); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, churnID, make([]byte, 900)); err != nil {
		t.Fatal(err)
	}
	firstResult, err := first.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; len(store.catalog.Snapshot().SealedDataSegments) == 0 && i < 64; i++ {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		value := make([]byte, 900)
		value[0] = byte(i + 1)
		if err := batch.Put(ctx, churnID, value); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.catalog.Snapshot().SealedDataSegments) == 0 {
		t.Fatal("test did not rotate a data segment")
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetRecord(ctx, stableID)
	if err != nil || before.Revision != Revision(firstResult.BatchID) || string(before.Value) != "stable" {
		t.Fatalf("before=%+v error=%v", before, err)
	}
	session, err := store.beginDataGC()
	if err != nil {
		t.Fatal(err)
	}
	if err := session.relocate(ctx); err != nil {
		_ = session.cancel()
		t.Fatal(err)
	}
	if session.relocated == 0 || session.lastCommit == 0 || session.copiedBytes == 0 {
		t.Fatalf("session=%+v", session)
	}
	after, err := store.GetRecord(ctx, stableID)
	if err != nil || after.Revision != before.Revision || string(after.Value) != "stable" {
		t.Fatalf("after=%+v error=%v", after, err)
	}
	if err := session.cancel(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recovered, err := store.GetRecord(ctx, stableID)
	if err != nil || recovered.Revision != before.Revision || string(recovered.Value) != "stable" {
		t.Fatalf("recovered=%+v error=%v", recovered, err)
	}
}

func TestBeginDataGCReportsNoSafeCandidate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := Create(smallTestConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.beginDataGC(); !errors.Is(err, base.ErrNotFound) {
		t.Fatalf("error=%v", err)
	}
}

func TestCompactDataWaitsForReaderDeletesSourceAndReopens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, stableID, revision := prepareDataGCStore(t, cfg)
	manifest := store.catalog.Snapshot()
	if len(manifest.SealedDataSegments) == 0 {
		t.Fatal("missing sealed source")
	}
	sourceID := base.DataSegmentID(manifest.SealedDataSegments[0].FileID)
	pin, err := store.segments.Acquire(sourceID)
	if err != nil {
		t.Fatal(err)
	}
	retired := make(chan struct{})
	store.hook = failpoint.Func(func(point failpoint.Point) error {
		if point == pointDataGCRetired {
			select {
			case <-retired:
			default:
				close(retired)
			}
		}
		return nil
	})
	type compactResult struct {
		result DataGCResult
		err    error
	}
	done := make(chan compactResult, 1)
	go func() {
		result, err := store.CompactData(context.Background())
		done <- compactResult{result: result, err: err}
	}()
	select {
	case <-retired:
	case <-time.After(5 * time.Second):
		t.Fatal("data GC did not reach retired phase")
	}
	select {
	case got := <-done:
		t.Fatalf("compact returned while reader pinned: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := os.Stat(dataGCSealedPath(dir, sourceID)); err != nil {
		t.Fatalf("source disappeared before reader release: %v", err)
	}
	if err := pin.Release(); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.result.SourceSegmentID != sourceID || got.result.SourceBytes == 0 {
		t.Fatalf("result=%+v error=%v", got.result, got.err)
	}
	if _, err := os.Stat(dataGCSealedPath(dir, sourceID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source still exists: %v", err)
	}
	for _, summary := range store.catalog.Snapshot().SealedDataSegments {
		if base.DataSegmentID(summary.FileID) == sourceID {
			t.Fatalf("source remains in manifest: %+v", summary)
		}
	}
	record, err := store.GetRecord(context.Background(), stableID)
	if err != nil || record.Revision != revision || string(record.Value) != "stable" {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	store.hook = nil
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record, err = store.GetRecord(context.Background(), stableID)
	if err != nil || record.Revision != revision || string(record.Value) != "stable" {
		t.Fatalf("reopened record=%+v error=%v", record, err)
	}
}

func TestCompactDataCheckpointCanRotateMappingInsideParentJournal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, stableID, revision := prepareDataGCStore(t, cfg)
	defer store.Close()
	ctx := context.Background()
	fillActiveMappingForNestedDataGC(t, store, cfg)
	rotations := 0
	store.mapping.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == radix.PointRotationPrepared {
			rotations++
		}
		return nil
	}))
	result, err := store.CompactData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceSegmentID == 0 || rotations == 0 {
		t.Fatalf("result=%+v nested rotations=%d", result, rotations)
	}
	record, err := store.GetRecord(ctx, stableID)
	if err != nil || record.Revision != revision || string(record.Value) != "stable" {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	if _, found, err := maintenance.Load(dir); err != nil || found {
		t.Fatalf("journal found=%v error=%v", found, err)
	}
}

func fillActiveMappingForNestedDataGC(t *testing.T, store *Store, cfg Config) {
	t.Helper()
	ctx := context.Background()
	filler, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fillerID, err := filler.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := filler.Put(ctx, fillerID, []byte("fill")); err != nil {
		t.Fatal(err)
	}
	if _, err := filler.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if err := store.Checkpoint(ctx); err != nil {
			t.Fatal(err)
		}
		manifest := store.catalog.Snapshot()
		path := filepath.Join(store.config.Dir, "mapping", fmt.Sprintf("MAP-%08d.active", manifest.ActiveMapSegmentID))
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > cfg.SegmentSize-int64(storeformat.SegmentFooterSize)-1200 {
			break
		}
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Put(ctx, fillerID, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if i == 63 {
			t.Fatal("failed to fill active mapping segment")
		}
	}
}

func prepareDataGCStore(t *testing.T, cfg Config) (*Store, ID, Revision) {
	t.Helper()
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stableID, err := first.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	churnID, err := first.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, stableID, []byte("stable")); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, churnID, make([]byte, 900)); err != nil {
		t.Fatal(err)
	}
	committed, err := first.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; len(store.catalog.Snapshot().SealedDataSegments) == 0 && i < 64; i++ {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		value := make([]byte, 900)
		value[0] = byte(i + 1)
		if err := batch.Put(ctx, churnID, value); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.catalog.Snapshot().SealedDataSegments) == 0 {
		t.Fatal("test did not rotate data")
	}
	return store, stableID, Revision(committed.BatchID)
}
