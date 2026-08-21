package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateOpenLockAndConfig(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := Create(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrLocked) {
		t.Fatalf("concurrent Open error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close error=%v", err)
	}
	if _, err := Create(Config{Dir: dir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second Create error=%v", err)
	}
	reopened, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.config.SegmentSize != 256*mib || reopened.manifest.Generation != 1 {
		t.Fatalf("config=%+v manifest=%+v", reopened.config, reopened.manifest)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir, SegmentSize: 128 * mib}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("mismatched Open error=%v", err)
	}
}

func smallTestConfig(dir string) Config {
	return Config{
		Dir: dir, SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
		MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
		IDReserveSize: 4, BatchIDReserveSize: 4,
		GCBatchBytes: 4096, GCBatchMutations: 16,
	}
}

func TestPublicCommitGetStatusAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.ID() != 1 {
		t.Fatalf("batch ID=%d", b.ID())
	}
	id, err := b.Allocate(context.Background())
	if err != nil || id != 1 {
		t.Fatalf("id=%d error=%v", id, err)
	}
	value := []byte("value")
	if err := b.Put(context.Background(), id, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	commitResult, err := b.Commit(context.Background())
	if err != nil || commitResult.BatchID != 1 || commitResult.CommitSeq != 1 {
		t.Fatalf("commit=%+v error=%v", commitResult, err)
	}
	record, err := store.GetRecord(context.Background(), id)
	if err != nil || string(record.Value) != "value" || record.Revision != 1 {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	status, err := store.Status(context.Background(), b.ID())
	if err != nil || status.State != BatchStateCommitted || status.CommitSeq != 1 {
		t.Fatalf("status=%+v error=%v", status, err)
	}
	if _, err := store.Status(context.Background(), 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unissued status error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	record, err = store.GetRecord(context.Background(), id)
	if err != nil || string(record.Value) != "value" || record.Revision != 1 {
		t.Fatalf("recovered record=%+v error=%v", record, err)
	}
	status, err = store.Status(context.Background(), 1)
	if err != nil || status.State != BatchStateCommitted {
		t.Fatalf("recovered status=%+v error=%v", status, err)
	}
	status, err = store.Status(context.Background(), 2)
	if err != nil || status.State != BatchStateAborted {
		t.Fatalf("skipped reserve status=%+v error=%v", status, err)
	}
	b, err = store.Begin(context.Background())
	if err != nil || b.ID() != 5 {
		t.Fatalf("recovered batch ID=%d error=%v", b.ID(), err)
	}
	nextID, err := b.Allocate(context.Background())
	if err != nil || nextID != 5 {
		t.Fatalf("recovered ID=%d error=%v", nextID, err)
	}
	if err := b.Delete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted get error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Get(context.Background(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recovered delete error=%v", err)
	}
}

func TestSegmentRotationPreservesReadsAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.SegmentSize = 16 << 10
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[ID]string)
	for i := 0; i < 24; i++ {
		b, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		value := fmt.Sprintf("value-%02d-%s", i, strings.Repeat("x", 512))
		if err := b.Put(context.Background(), id, []byte(value)); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
		values[id] = value
	}
	if current := store.rotation.Current(); current.Generation <= 1 || len(current.SealedDataSegments) == 0 {
		t.Fatalf("rotation did not advance manifest: %+v", current)
	}
	for id, want := range values {
		got, err := store.Get(context.Background(), id)
		if err != nil || string(got) != want {
			t.Fatalf("before reopen id=%d error=%v", id, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for id, want := range values {
		got, err := store.Get(context.Background(), id)
		if err != nil || string(got) != want {
			t.Fatalf("after reopen id=%d error=%v", id, err)
		}
	}
}

func TestPublicConditionalConflictAndOpenBatchBackpressure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 1
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b1, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := store.Begin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backpressure error=%v", err)
	}
	if err := b1.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	b2, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	if err := b2.Put(context.Background(), 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := b2.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	b3, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b3.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	if err := b3.Put(context.Background(), 1, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if _, err := b3.Commit(context.Background()); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	value, err := store.Get(context.Background(), 1)
	if err != nil || string(value) != "first" {
		t.Fatalf("value=%q error=%v", value, err)
	}
}

func TestCloseWakesBlockedBeginAndAbortsOpenBatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	cfg := smallTestConfig(dir)
	cfg.MaxOpenBatches = 1
	store, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.Begin(context.Background())
		result <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked Begin error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Begin was not released by Close")
	}
}

func TestOpenDoesNotCreate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open created directory: %v", err)
	}
}

func TestCreateRejectsNonEmptyAndSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "foreign"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(Config{Dir: dir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("non-empty Create error=%v", err)
	}
	link := filepath.Join(t.TempDir(), "store-link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(Config{Dir: link}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("symlink Create error=%v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []Config{
		{Dir: t.TempDir(), SegmentSize: -1},
		{Dir: t.TempDir(), SegmentSize: 8 << 20, MaxValueSize: 16 << 20},
		{Dir: t.TempDir(), DeltaSoftLimitBytes: 2, DeltaHardLimitBytes: 1},
		{Dir: t.TempDir(), MaxBatchBytes: 1 << 20, GCBatchBytes: 2 << 20},
		{Dir: t.TempDir(), MaxGroupDelay: -1},
	}
	for i, cfg := range cases {
		if _, _, err := normalizeCreateConfig(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error=%v", i, err)
		}
	}
}

func TestConfigAllowsExactFourGiBSegment(t *testing.T) {
	cfg, hard, err := normalizeCreateConfig(Config{Dir: t.TempDir(), SegmentSize: int64(1) << 32})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SegmentSize != int64(1)<<32 || hard.SegmentSize != uint64(1)<<32 {
		t.Fatalf("config=%d hard=%d", cfg.SegmentSize, hard.SegmentSize)
	}
}
