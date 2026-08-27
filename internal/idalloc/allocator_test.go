package idalloc

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type appendCall struct {
	payload []byte
	sync    bool
}

type fakeLog struct {
	mu    sync.Mutex
	calls []appendCall
	err   error
}

func (l *fakeLog) Append(_ context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return recordlog.AppendResult{}, l.err
	}
	l.calls = append(l.calls, appendCall{payload: append([]byte(nil), payload...), sync: syncWrite})
	addr, _ := recordlog.NewVAddr(1, uint32(64+(len(l.calls)-1)*64), 64)
	return recordlog.NewAppendResult(addr, 64)
}

func TestRecordAndBatchReserveAreDurableBeforeIssue(t *testing.T) {
	for _, tc := range []struct {
		kind Kind
		typ  recordcodec.RecordType
	}{{RecordID, recordcodec.RecordTypeIDReserve}, {BatchID, recordcodec.RecordTypeBatchIDReserve}} {
		log := &fakeLog{}
		allocator, err := New(tc.kind, 4, 1, log)
		if err != nil {
			t.Fatal(err)
		}
		for want := uint64(1); want <= 5; want++ {
			got, err := allocator.Allocate(context.Background())
			if err != nil || got != want {
				t.Fatalf("kind=%d got=%d want=%d err=%v", tc.kind, got, want, err)
			}
		}
		if allocator.DurableHigh() != 9 || allocator.IssuedHigh() != 6 || len(log.calls) != 2 {
			t.Fatalf("kind=%d durable=%d issued=%d calls=%d", tc.kind, allocator.DurableHigh(), allocator.IssuedHigh(), len(log.calls))
		}
		for index, call := range log.calls {
			if !call.sync {
				t.Fatalf("reserve %d was not synchronous", index)
			}
			record, err := recordcodec.DecodeReserve(call.payload, tc.typ)
			want := uint64(5 + index*4)
			if err != nil || record.HighExclusive != want {
				t.Fatalf("record=%+v want=%d err=%v", record, want, err)
			}
		}
	}
}

func TestAllocatorCanUseOnlyConsumedPrefix(t *testing.T) {
	log := &fakeLog{}
	allocator, err := New(RecordID, 4, 1, log)
	if err != nil {
		t.Fatal(err)
	}
	if allocator.CanUse(0) || allocator.CanUse(1) {
		t.Fatal("unissued IDs were accepted")
	}
	id, err := allocator.Allocate(context.Background())
	if err != nil || id != 1 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	if !allocator.CanUse(id) || allocator.CanUse(2) {
		t.Fatalf("issued=%v next=%v", allocator.CanUse(id), allocator.CanUse(2))
	}
	recovered, err := New(RecordID, 4, allocator.DurableHigh(), log)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.CanUse(3) || recovered.CanUse(5) {
		t.Fatalf("recovered prefix=%v future=%v", recovered.CanUse(3), recovered.CanUse(5))
	}
}

func TestRecoveryStartsAfterWholeDurableRange(t *testing.T) {
	log := &fakeLog{}
	allocator, err := New(RecordID, 4, 9, log)
	if err != nil {
		t.Fatal(err)
	}
	got, err := allocator.Allocate(context.Background())
	if err != nil || got != 9 {
		t.Fatalf("got=%d err=%v", got, err)
	}
	record, err := recordcodec.DecodeReserve(log.calls[0].payload, recordcodec.RecordTypeIDReserve)
	if err != nil || record.HighExclusive != 13 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	high, err := AdvanceRecovered(RecordID, 4, 9, recordcodec.ReserveRecord{HighExclusive: 13})
	if err != nil || high != 13 {
		t.Fatalf("high=%d err=%v", high, err)
	}
}

func TestFailureAndCancellationDoNotIssue(t *testing.T) {
	wantErr := errors.New("fsync failed")
	log := &fakeLog{err: wantErr}
	allocator, err := New(RecordID, 4, 1, log)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := allocator.Allocate(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("reserve err=%v", err)
	}
	if allocator.DurableHigh() != 1 {
		t.Fatalf("high=%d", allocator.DurableHigh())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := allocator.Allocate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
}

func TestConcurrentAllocationIsUnique(t *testing.T) {
	allocator, err := New(RecordID, 16, 1, &fakeLog{})
	if err != nil {
		t.Fatal(err)
	}
	const count = 1000
	ids := make(chan uint64, count)
	var group sync.WaitGroup
	for range 10 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range count / 10 {
				id, err := allocator.Allocate(context.Background())
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	group.Wait()
	close(ids)
	all := make([]int, 0, count)
	for id := range ids {
		all = append(all, int(id))
	}
	sort.Ints(all)
	for index, id := range all {
		if id != index+1 {
			t.Fatalf("ids[%d]=%d", index, id)
		}
	}
}

func TestRejectsBrokenRecoveryChainAndExhaustion(t *testing.T) {
	if _, err := New(RecordID, 4, 8, &fakeLog{}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("config err=%v", err)
	}
	if _, err := AdvanceRecovered(RecordID, 4, 9, recordcodec.ReserveRecord{HighExclusive: 17}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("chain err=%v", err)
	}
	for _, kind := range []Kind{RecordID, BatchID} {
		allocator, err := New(kind, 2, math.MaxUint64, &fakeLog{})
		if err != nil {
			t.Fatalf("kind=%d new: %v", kind, err)
		}
		if _, err := allocator.Allocate(context.Background()); !errors.Is(err, base.ErrIDExhausted) {
			t.Fatalf("kind=%d allocate err=%v", kind, err)
		}
	}
}
