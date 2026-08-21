package allocator

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type fakeWriter struct {
	mu       sync.Mutex
	requests []storeformat.ReservePayload
	types    []storeformat.FrameType
	err      error
}

func (w *fakeWriter) AppendReserve(_ context.Context, typ storeformat.FrameType, payload storeformat.ReservePayload) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.types = append(w.types, typ)
	w.requests = append(w.requests, payload)
	return nil
}

func TestRecordAndBatchReserve(t *testing.T) {
	for _, tc := range []struct {
		kind Kind
		typ  storeformat.FrameType
	}{{RecordID, storeformat.FrameTypeIDReserve}, {BatchID, storeformat.FrameTypeBatchIDReserve}} {
		writer := &fakeWriter{}
		allocator, err := New(tc.kind, 4, 1, writer)
		if err != nil {
			t.Fatal(err)
		}
		for want := uint64(1); want <= 5; want++ {
			got, err := allocator.Allocate(context.Background())
			if err != nil || got != want {
				t.Fatalf("kind=%d got=%d want=%d error=%v", tc.kind, got, want, err)
			}
		}
		if allocator.DurableHigh() != 9 || len(writer.requests) != 2 || writer.types[0] != tc.typ {
			t.Fatalf("kind=%d high=%d requests=%+v types=%v", tc.kind, allocator.DurableHigh(), writer.requests, writer.types)
		}
		if writer.requests[0] != (storeformat.ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 5, Generation: 1}) {
			t.Fatalf("first reserve=%+v", writer.requests[0])
		}
	}
}

func TestRecoverySkipsDurableRange(t *testing.T) {
	writer := &fakeWriter{}
	allocator, err := New(RecordID, 4, 9, writer)
	if err != nil {
		t.Fatal(err)
	}
	got, err := allocator.Allocate(context.Background())
	if err != nil || got != 9 {
		t.Fatalf("got=%d error=%v", got, err)
	}
	if len(writer.requests) != 1 || writer.requests[0].PreviousHighExclusive != 9 || writer.requests[0].Generation != 3 {
		t.Fatalf("requests=%+v", writer.requests)
	}
	high, err := AdvanceRecovered(RecordID, 4, 9, storeformat.ReservePayload{PreviousHighExclusive: 9, NewHighExclusive: 13, Generation: 3})
	if err != nil || high != 13 {
		t.Fatalf("high=%d error=%v", high, err)
	}
}

func TestReserveFailureAndCancellationDoNotIssue(t *testing.T) {
	wantErr := errors.New("fsync failed")
	writer := &fakeWriter{err: wantErr}
	allocator, err := New(RecordID, 4, 1, writer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := allocator.Allocate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("reserve error=%v", err)
	}
	if allocator.DurableHigh() != 1 {
		t.Fatalf("high=%d", allocator.DurableHigh())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := allocator.Allocate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}

func TestConcurrentAllocationUnique(t *testing.T) {
	allocator, err := New(RecordID, 16, 1, &fakeWriter{})
	if err != nil {
		t.Fatal(err)
	}
	const count = 1000
	ids := make(chan uint64, count)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < count/10; j++ {
				id, err := allocator.Allocate(context.Background())
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)
	all := make([]int, 0, count)
	for id := range ids {
		all = append(all, int(id))
	}
	sort.Ints(all)
	for i, id := range all {
		if id != i+1 {
			t.Fatalf("ids[%d]=%d", i, id)
		}
	}
}

func TestRejectsBrokenRecoveryChainAndConfig(t *testing.T) {
	if _, err := New(RecordID, 4, 8, &fakeWriter{}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("config error=%v", err)
	}
	if _, err := AdvanceRecovered(RecordID, 4, 9, storeformat.ReservePayload{PreviousHighExclusive: 5, NewHighExclusive: 9, Generation: 2}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("chain error=%v", err)
	}
}

func TestRecordAndBatchAllocatorRejectUint64Wrap(t *testing.T) {
	for _, kind := range []Kind{RecordID, BatchID} {
		allocator, err := New(kind, 2, math.MaxUint64, &fakeWriter{})
		if err != nil {
			t.Fatalf("kind=%d new: %v", kind, err)
		}
		if _, err := allocator.Allocate(context.Background()); !errors.Is(err, base.ErrIDExhausted) {
			t.Fatalf("kind=%d allocate error=%v", kind, err)
		}
		if allocator.DurableHigh() != math.MaxUint64 {
			t.Fatalf("kind=%d durable high=%d", kind, allocator.DurableHigh())
		}
	}
}
