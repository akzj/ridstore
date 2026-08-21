package ridstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
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
