package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/transaction"
)

type memoryLog struct {
	mu      sync.Mutex
	offset  uint32
	records map[recordlog.VAddr][]byte
	closed  bool
}

func (l *memoryLog) Append(_ context.Context, payload []byte, _ bool) (recordlog.AppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return recordlog.AppendResult{}, recordlog.ErrClosed
	}
	physical, err := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	if l.offset == 0 {
		l.offset = recordlog.SegmentHeaderSize
	}
	addr, err := recordlog.NewVAddr(1, l.offset, physical)
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	l.offset += physical
	l.records[addr] = append([]byte(nil), payload...)
	return recordlog.NewAppendResult(addr, physical)
}

func (l *memoryLog) Read(_ context.Context, addr recordlog.VAddr) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	payload, ok := l.records[addr]
	if !ok {
		return nil, recordlog.ErrInvalidVAddr
	}
	return append([]byte(nil), payload...), nil
}

func (l *memoryLog) Status() recordlog.Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	return recordlog.Status{Closed: l.closed}
}

func (l *memoryLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return recordlog.ErrClosed
	}
	l.closed = true
	return nil
}

func newStore(t *testing.T, maxOpen int) *Store {
	t.Helper()
	log := &memoryLog{records: make(map[recordlog.VAddr][]byte)}
	ids, err := idalloc.New(idalloc.RecordID, 16, 1, log)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := idalloc.New(idalloc.BatchID, 16, 1, log)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(log, mapping.NewEmpty(), ids, batches, Config{
		Batch:          transaction.Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16},
		Commit:         coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 1 << 20},
		MaxOpenBatches: maxOpen,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return store
}

func TestStoreCreateUpdateConflictDelete(t *testing.T) {
	store := newStore(t, 4)
	created, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := created.Create(context.Background(), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "one" || record.Revision != 1 {
		t.Fatalf("record=%+v err=%v", record, err)
	}

	conflict, _ := store.Begin(context.Background())
	if err := conflict.Update(context.Background(), id, 99, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := conflict.Commit(context.Background()); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("conflict err=%v", err)
	}

	updated, _ := store.Begin(context.Background())
	if err := updated.Update(context.Background(), id, record.Revision, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if _, err := updated.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err = store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "two" || record.Revision != 3 {
		t.Fatalf("record=%+v err=%v", record, err)
	}

	deleted, _ := store.Begin(context.Background())
	if err := deleted.DeleteIfRevision(id, record.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := deleted.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), id); !errors.Is(err, base.ErrNotFound) {
		t.Fatalf("get deleted err=%v", err)
	}
}

func TestOpenBatchLimitBackpressuresAndReleases(t *testing.T) {
	store := newStore(t, 1)
	first, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := store.Begin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("begin err=%v", err)
	}
	if err := first.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
}
