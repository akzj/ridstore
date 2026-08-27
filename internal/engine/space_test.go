package engine

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestSpaceGateAccountsConcurrentReservations(t *testing.T) {
	now := time.Unix(1, 0)
	calls := 0
	gate := newSpaceGate("/store", 100, time.Minute, func(string) (uint64, error) {
		calls++
		return 200, nil
	})
	gate.now = func() time.Time { return now }
	first, err := gate.reserve(context.Background(), 80)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.reserve(context.Background(), 21); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("oversubscribed reserve err=%v", err)
	}
	first.complete(true)
	now = now.Add(2 * time.Minute)
	second, err := gate.reserve(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	second.complete(true)
	if calls != 2 {
		t.Fatalf("statfs calls=%d", calls)
	}
	snapshot := gate.snapshot()
	if snapshot.rejections != 1 || snapshot.checkErrors != 0 || snapshot.minimum != 100 || snapshot.available != 180 || snapshot.stopped {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestSpaceGateMetricsRecordCheckFailure(t *testing.T) {
	injected := errors.New("statfs failed")
	gate := newSpaceGate("/store", 100, time.Minute, func(string) (uint64, error) { return 0, injected })
	if _, err := gate.reserve(context.Background(), 1); !errors.Is(err, injected) {
		t.Fatalf("reserve err=%v", err)
	}
	snapshot := gate.snapshot()
	if snapshot.checkErrors != 1 || !snapshot.stopped {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestSpaceGateLetsGCKeepOnlyItsLowerHeadroom(t *testing.T) {
	gate := newSpaceGate("/store", 100, time.Minute, func(string) (uint64, error) { return 80, nil })
	if _, err := gate.reserve(context.Background(), 1); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("user reserve err=%v", err)
	}
	reservation, err := gate.reserveWithMinimum(context.Background(), 60, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	reservation.complete(true)
	if snapshot := gate.snapshot(); snapshot.available != 20 || snapshot.rejections != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestSpaceGateInvalidatesEstimateAfterAppendFailure(t *testing.T) {
	available := []uint64{200, 150}
	gate := newSpaceGate("/store", 100, time.Minute, func(string) (uint64, error) {
		value := available[0]
		available = available[1:]
		return value, nil
	})
	first, err := gate.reserve(context.Background(), 80)
	if err != nil {
		t.Fatal(err)
	}
	first.complete(false)
	second, err := gate.reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	second.complete(true)
}

func TestSpaceGateRefreshesInvalidEstimateWithOutstandingReservation(t *testing.T) {
	available := []uint64{220, 200}
	gate := newSpaceGate("/store", 100, time.Minute, func(string) (uint64, error) {
		value := available[0]
		available = available[1:]
		return value, nil
	})
	first, err := gate.reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := gate.reserve(context.Background(), 40)
	if err != nil {
		t.Fatal(err)
	}
	failed.complete(false)
	third, err := gate.reserve(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	third.complete(true)
	first.complete(true)
	if len(available) != 0 {
		t.Fatalf("statfs samples remaining=%d", len(available))
	}
}

func TestSpaceGateRejectsReservationOverflow(t *testing.T) {
	if fitsBelow(math.MaxUint64, 1, math.MaxUint64, 0) {
		t.Fatal("overflowing reservation accepted")
	}
}

func TestSpaceAppenderGatesOnlyUserPutRecords(t *testing.T) {
	next := &captureAppender{}
	gate := newSpaceGate("/store", 100, time.Minute, func(string) (uint64, error) { return 100, nil })
	appender := &spaceAppender{next: next, gate: gate}
	put, err := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 1, RecordID: 1, Value: []byte("value")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appender.Append(context.Background(), put, false); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("put err=%v", err)
	}
	abort, err := recordcodec.EncodeAbort(recordcodec.AbortRecord{BatchID: 1, Reason: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appender.Append(context.Background(), abort, false); err != nil {
		t.Fatalf("control append err=%v", err)
	}
	if next.calls != 1 {
		t.Fatalf("underlying calls=%d", next.calls)
	}
}

func TestSpaceAppenderRejectsIncompleteWiring(t *testing.T) {
	if _, err := (&spaceAppender{}).Append(context.Background(), nil, false); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("append err=%v", err)
	}
}

type captureAppender struct{ calls int }

func (a *captureAppender) Append(context.Context, []byte, bool) (recordlog.AppendResult, error) {
	a.calls++
	return recordlog.AppendResult{}, nil
}
