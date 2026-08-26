package engine

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestRelocateSegmentCopiesLivePutAndPreservesOrigin(t *testing.T) {
	store, source, id, oldAddr, origin := relocationFixture(t)

	result, err := store.RelocateSegment(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.LiveCandidates == 0 || result.Applied == 0 || result.Applied != result.CopiedRecords || result.Skipped != 0 {
		t.Fatalf("result=%+v", result)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || record.Addr == oldAddr || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	payload, err := store.log.Read(context.Background(), record.Addr)
	if err != nil {
		t.Fatal(err)
	}
	put, err := recordcodec.DecodePut(payload, store.limits.MaxValueSize)
	if err != nil || put.OriginBatchID != origin || put.RecordID != id {
		t.Fatalf("put=%+v err=%v", put, err)
	}
}

func TestConcurrentUserUpdateWinsOverSegmentRelocation(t *testing.T) {
	store, source, id, oldAddr, _ := relocationFixture(t)
	underlying := store.log
	underlyingMaintenance := store.maintenance
	blocked := &blockingCopyLog{
		Log: underlying, maintenanceLog: underlyingMaintenance, target: id, value: []byte("source-value"),
		reached: make(chan struct{}), release: make(chan struct{}),
	}
	store.log = blocked
	store.maintenance = blocked

	type relocationAnswer struct {
		result SegmentRelocationResult
		err    error
	}
	done := make(chan relocationAnswer, 1)
	go func() {
		result, err := store.RelocateSegment(context.Background(), source)
		done <- relocationAnswer{result: result, err: err}
	}()
	<-blocked.reached

	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.CompareAndPut(context.Background(), id, oldAddr, []byte("user-wins")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.result.Skipped == 0 {
		t.Fatalf("result=%+v", got.result)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "user-wins" || record.Addr == oldAddr {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

type blockingCopyLog struct {
	Log
	maintenanceLog
	target  model.ID
	value   []byte
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingCopyLog) Append(ctx context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	result, err := l.Log.Append(ctx, payload, syncWrite)
	if err != nil || syncWrite {
		return result, err
	}
	put, decodeErr := recordcodec.DecodePut(payload, 1<<20)
	if decodeErr == nil && put.RecordID == l.target && bytes.Equal(put.Value, l.value) {
		l.once.Do(func() {
			close(l.reached)
			<-l.release
		})
	}
	return result, err
}

func relocationFixture(t *testing.T) (*Store, recordlog.SegmentID, model.ID, recordlog.VAddr, model.BatchID) {
	t.Helper()
	config := testCreateConfig()
	config.HardLimits.SegmentSize = 8192
	config.HardLimits.MaxValueSize = 512
	config.HardLimits.MaxBatchBytes = 2048
	config.HardLimits.MaxBatchMutations = 4
	config.HardLimits.MaxRecordLogPayload = 1024
	config.Runtime.RecordLog.BufferBytes = 2048
	config.Runtime.Commit.MaxGroupPayload = 1024
	store, err := Create(context.Background(), t.TempDir(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil && !errors.Is(err, recordlog.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})

	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	origin := batch.ID()
	id, err := batch.Create(context.Background(), []byte("source-value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	source := record.Addr.SegmentID()
	for store.catalog.Snapshot().ActiveDataSegmentID == source {
		filler, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Create(context.Background(), bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := filler.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return store, source, id, record.Addr, origin
}
