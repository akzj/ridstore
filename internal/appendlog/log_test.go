package appendlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

func newActive(t *testing.T, segmentSize uint64) (*segment.ActiveData, base.StoreUUID) {
	t.Helper()
	root := t.TempDir()
	uuid := base.StoreUUID{1, 2, 3}
	if err := os.Mkdir(filepath.Join(root, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{Kind: storeformat.SegmentKindData, StoreUUID: uuid, FileID: 1, FirstSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", segment.ActiveDataFileName(1)), header[:], 0o600); err != nil {
		t.Fatal(err)
	}
	active, err := segment.OpenActiveData(root, uuid, 1, segmentSize, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return active, uuid
}

func TestAppendPutReserveAbortAndCommit(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	putAddr, putSeq, _, err := log.AppendPut(context.Background(), 7, 1, []byte("value"))
	if err != nil || putSeq != 1 {
		t.Fatalf("put addr=%x seq=%d error=%v", putAddr, putSeq, err)
	}
	if err := log.AppendReserve(context.Background(), storeformat.FrameTypeIDReserve, storeformat.ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 5, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	prepared := batch.Prepared{BatchID: 7, LogicalPayloadBytes: 5, Mutations: []batch.Mutation{
		{RecordID: 1, Operation: batch.Put, Addr: putAddr, ValueBytes: 5},
		{RecordID: 2, Operation: batch.Delete},
	}}
	result, err := log.AppendCommit(prepared, 1)
	if err != nil || !result.SealStarted || result.SealFrameSeq != 4 {
		t.Fatalf("commit result=%+v error=%v", result, err)
	}
	if err := log.AppendAbort(context.Background(), 8, storeformat.BatchAbortPayload{Reason: storeformat.AbortReasonCaller}); err != nil {
		t.Fatal(err)
	}
	if log.NextFrameSeq() != 6 {
		t.Fatalf("next frame seq=%d", log.NextFrameSeq())
	}
	var parts []storeformat.Frame
	var seal storeformat.Frame
	if err := active.Scan(func(_ base.VAddr, frame storeformat.Frame) error {
		switch frame.Type {
		case storeformat.FrameTypeCommitPart:
			parts = append(parts, frame)
		case storeformat.FrameTypeCommitSeal:
			seal = frame
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	decoded, err := storeformat.ValidateDescriptorFrames(storeformat.DescriptorCommit, parts, seal, 10)
	if err != nil || decoded.Seal.CommitSeq != 1 || len(decoded.Entries) != 2 || decoded.Entries[0].NewVAddr != putAddr {
		t.Fatalf("descriptor=%+v error=%v", decoded, err)
	}
}

func TestCommitPreflightNoSpaceWritesNothing(t *testing.T) {
	segmentSize := uint64(storeformat.SegmentHeaderSize + storeformat.SegmentFooterSize + storeformat.FrameHeaderSize)
	active, _ := newActive(t, segmentSize)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	before := active.End()
	result, err := log.AppendCommit(batch.Prepared{BatchID: 1}, 1)
	if !errors.Is(err, segment.ErrFull) || result.SealStarted {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if active.End() != before || log.NextFrameSeq() != 1 {
		t.Fatalf("end=%d before=%d next=%d", active.End(), before, log.NextFrameSeq())
	}
}

func TestAppendLogRejectsCancellationAndInvalidReserve(t *testing.T) {
	active, _ := newActive(t, 1<<20)
	defer active.Close()
	log, err := New(active, 1, 1024, 64)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := log.AppendPut(ctx, 1, 1, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if err := log.AppendReserve(context.Background(), storeformat.FrameTypePutRecord, storeformat.ReservePayload{}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("reserve error=%v", err)
	}
	if log.NextFrameSeq() != 1 {
		t.Fatalf("next=%d", log.NextFrameSeq())
	}
}
