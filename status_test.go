package ridstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/initialize"
)

func TestStatusCacheEvictsResolvedAndPinsUnknown(t *testing.T) {
	store := &Store{
		config:   Config{StatusRetention: 2},
		statuses: make(map[BatchID]statusEntry),
	}
	store.addStatusLocked(BatchStatus{BatchID: 1, State: BatchStateCommitUnknown})
	for id := BatchID(2); id <= 5; id++ {
		store.addStatusLocked(BatchStatus{BatchID: id, State: BatchStateAborted})
	}
	if len(store.statuses) != 3 || store.resolvedStatusCount != 2 {
		t.Fatalf("statuses=%v resolved=%d", store.statuses, store.resolvedStatusCount)
	}
	if _, ok := store.statuses[1]; !ok {
		t.Fatal("CommitUnknown was evicted")
	}
	if _, ok := store.statuses[2]; ok {
		t.Fatal("old resolved status was retained")
	}
}

func TestCommitUnknownIsPinnedAndBlocksCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 1
	cfg.StatusRetention = 1
	cause := errors.New("injected seal failure")
	hook := failpoint.Func(func(point failpoint.Point) error {
		if point == appendlog.PointCommitSealWritten {
			return cause
		}
		return nil
	})
	store, err := createWithOptions(cfg, initialize.Options{Hook: hook})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(context.Background(), 1, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, cause) {
		t.Fatalf("Commit error=%v", err)
	}
	status, err := store.Status(context.Background(), batch.ID())
	if err != nil || status.State != BatchStateCommitUnknown {
		t.Fatalf("status=%+v error=%v", status, err)
	}
	if err := store.Checkpoint(context.Background()); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Checkpoint error=%v", err)
	}
}

func TestStatusRetentionBackpressuresAndCheckpointAdvancesCut(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 1
	cfg.StatusRetention = 4
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var first BatchID
	for i := 0; i < 12; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		batch, beginErr := store.Begin(ctx)
		cancel()
		if beginErr != nil {
			t.Fatalf("Begin %d: %v", i, beginErr)
		}
		if i == 0 {
			first = batch.ID()
		}
		if err := batch.Abort(context.Background()); err != nil {
			t.Fatalf("Abort %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for store.MaintenanceStatus().CheckpointPending && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	store.mu.Lock()
	statusCount := len(store.statuses)
	replayCount := store.replayStatusCountLocked()
	store.mu.Unlock()
	if statusCount > cfg.StatusRetention || replayCount > uint64(cfg.StatusRetention) {
		t.Fatalf("statuses=%d replay=%d limit=%d", statusCount, replayCount, cfg.StatusRetention)
	}
	if _, err := store.Status(context.Background(), first); !errors.Is(err, ErrStatusExpired) {
		t.Fatalf("first status error=%v", err)
	}
}

func TestStatusRetentionMustCoverOpenBatches(t *testing.T) {
	cfg := smallTestConfig(filepath.Join(t.TempDir(), "store"))
	cfg.StatusRetention = cfg.MaxOpenBatches - 1
	if _, err := Create(cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Create error=%v", err)
	}
}

func TestCloseBroadcastsToAllBlockedBegins(t *testing.T) {
	cfg := smallTestConfig(filepath.Join(t.TempDir(), "store"))
	cfg.MaxOpenBatches = 1
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	const waiters = 8
	results := make(chan error, waiters)
	for range waiters {
		go func() {
			_, err := store.Begin(context.Background())
			results <- err
		}()
	}
	time.Sleep(10 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for range waiters {
		select {
		case err := <-results:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("blocked Begin error=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked Begin was not broadcast on Close")
		}
	}
}
