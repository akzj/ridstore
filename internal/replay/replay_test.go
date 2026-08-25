package replay

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type logRecord struct {
	result  recordlog.AppendResult
	payload []byte
}

type fakeLog struct {
	records    []logRecord
	byAddr     map[recordlog.VAddr][]byte
	nextOffset uint32
}

func (l *fakeLog) add(t *testing.T, payload []byte) recordlog.VAddr {
	t.Helper()
	physical, err := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	offset := l.nextOffset
	if offset == 0 {
		offset = recordlog.SegmentHeaderSize
	}
	if len(l.records) != 0 && l.nextOffset == 0 {
		offset = l.records[len(l.records)-1].result.End.Offset
	}
	addr, err := recordlog.NewVAddr(1, offset, physical)
	if err != nil {
		t.Fatal(err)
	}
	result, _ := recordlog.NewAppendResult(addr, physical)
	l.nextOffset = result.End.Offset
	copyPayload := append([]byte(nil), payload...)
	l.records = append(l.records, logRecord{result: result, payload: copyPayload})
	l.byAddr[addr] = copyPayload
	return addr
}

func (l *fakeLog) Scan(_ context.Context, from recordlog.LogPos, visit func(recordlog.AppendResult, []byte) error) error {
	for _, record := range l.records {
		position := recordlog.LogPos{SegmentID: record.result.Addr.SegmentID(), Offset: record.result.Addr.Offset()}
		if position.Compare(from) >= 0 {
			if err := visit(record.result, append([]byte(nil), record.payload...)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *fakeLog) Read(_ context.Context, addr recordlog.VAddr) ([]byte, error) {
	payload, ok := l.byAddr[addr]
	if !ok {
		return nil, recordlog.ErrInvalidVAddr
	}
	return append([]byte(nil), payload...), nil
}

func replayConfig() Config {
	return Config{MaxValueSize: 1024, MaxRecordPayload: 4096, MaxGroupDescriptors: 16, MaxGroupMutations: 64, IDReserveSize: 4, BatchIDReserveSize: 4}
}

func TestRecoverRebuildsMappingAllocatorsAndStatuses(t *testing.T) {
	log := &fakeLog{byAddr: make(map[recordlog.VAddr][]byte)}
	idReserve, _ := recordcodec.EncodeReserve(recordcodec.RecordTypeIDReserve, recordcodec.ReserveRecord{HighExclusive: 9})
	log.add(t, idReserve)
	batchReserve, _ := recordcodec.EncodeReserve(recordcodec.RecordTypeBatchIDReserve, recordcodec.ReserveRecord{HighExclusive: 9})
	log.add(t, batchReserve)
	orphan, _ := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 1, RecordID: 1, Value: []byte("orphan")}, 1024)
	log.add(t, orphan)
	put, _ := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 2, RecordID: 2, Value: []byte("value")}, 1024)
	putAddr := log.add(t, put)
	commit, _ := recordcodec.EncodeCommitGroup(recordcodec.CommitGroup{Descriptors: []recordcodec.Descriptor{{
		Kind: recordcodec.DescriptorUserCommit, BatchID: 2, CommitSeq: 1, LogicalPayloadBytes: 5,
		Mutations: []recordcodec.Mutation{{RecordID: 2, NewAddr: putAddr, Operation: recordcodec.OperationPut}},
	}}}, 4096)
	log.add(t, commit)
	abort, _ := recordcodec.EncodeAbort(recordcodec.AbortRecord{BatchID: 3, Reason: 1})
	log.add(t, abort)

	start, _ := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	result, err := Recover(context.Background(), log, Checkpoint{
		Mapping: mapping.Snapshot{}, ReplayStart: start, ReservedIDHigh: 5, ReservedBatchIDHigh: 5,
		OpenBatchIDs: []model.BatchID{2, 3},
	}, replayConfig())
	if err != nil {
		t.Fatal(err)
	}
	entry, exists, _ := result.Mapping.Lookup(2)
	if !exists || entry.Addr != putAddr || entry.Revision != 2 || result.NextCommitSeq != 2 || result.ReservedIDHigh != 9 || result.ReservedBatchIDHigh != 9 {
		t.Fatalf("entry=%+v exists=%v result=%+v", entry, exists, result)
	}
	if _, exists, _ := result.Mapping.Lookup(1); exists {
		t.Fatal("orphan put became visible")
	}
	if result.Statuses[2] != (BatchStatus{State: BatchCommitted, CommitSeq: 1}) || result.Statuses[3].State != BatchAborted {
		t.Fatalf("statuses=%+v", result.Statuses)
	}
}

func TestRecoverRejectsCommitSequenceGapAndWrongPutIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		seq  model.CommitSeq
		id   model.ID
	}{{"sequence gap", 2, 1}, {"wrong put identity", 1, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			log := &fakeLog{byAddr: make(map[recordlog.VAddr][]byte)}
			reserve, _ := recordcodec.EncodeReserve(recordcodec.RecordTypeIDReserve, recordcodec.ReserveRecord{HighExclusive: 5})
			log.add(t, reserve)
			batchReserve, _ := recordcodec.EncodeReserve(recordcodec.RecordTypeBatchIDReserve, recordcodec.ReserveRecord{HighExclusive: 5})
			log.add(t, batchReserve)
			put, _ := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 1, RecordID: 1, Value: []byte("x")}, 1024)
			addr := log.add(t, put)
			commit, _ := recordcodec.EncodeCommitGroup(recordcodec.CommitGroup{Descriptors: []recordcodec.Descriptor{{
				Kind: recordcodec.DescriptorUserCommit, BatchID: 1, CommitSeq: tc.seq, LogicalPayloadBytes: 1,
				Mutations: []recordcodec.Mutation{{RecordID: tc.id, NewAddr: addr, Operation: recordcodec.OperationPut}},
			}}}, 4096)
			log.add(t, commit)
			start, _ := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
			_, err := Recover(context.Background(), log, Checkpoint{Mapping: mapping.Snapshot{}, ReplayStart: start, ReservedIDHigh: 1, ReservedBatchIDHigh: 1}, replayConfig())
			if !errors.Is(err, base.ErrCorrupt) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestRecoverRelocationPreservesRevisionAndUsesAddressCAS(t *testing.T) {
	log := &fakeLog{byAddr: make(map[recordlog.VAddr][]byte), nextOffset: 256}
	oldPayload, _ := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 2, RecordID: 1, Value: []byte("value")}, 1024)
	oldPhysical, _ := recordlog.PhysicalRecordSize(uint64(len(oldPayload)))
	oldAddr, _ := recordlog.NewVAddr(1, recordlog.SegmentHeaderSize, oldPhysical)
	log.byAddr[oldAddr] = oldPayload
	newAddr := log.add(t, oldPayload)
	stalePayload, _ := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 2, RecordID: 2, Value: []byte("value")}, 1024)
	staleOldAddr, _ := recordlog.NewVAddr(1, 128, oldPhysical)
	log.byAddr[staleOldAddr] = stalePayload
	staleAddr, _ := recordlog.NewVAddr(1, 192, oldPhysical)
	log.byAddr[staleAddr] = stalePayload
	group, _ := recordcodec.EncodeCommitGroup(recordcodec.CommitGroup{Descriptors: []recordcodec.Descriptor{{
		Kind: recordcodec.DescriptorRelocation, BatchID: 3, CommitSeq: 5, LogicalPayloadBytes: 10,
		Mutations: []recordcodec.Mutation{
			{RecordID: 1, NewAddr: newAddr, ExpectedOldAddr: oldAddr, Operation: recordcodec.OperationRelocate},
			{RecordID: 2, NewAddr: staleAddr, ExpectedOldAddr: staleOldAddr, Operation: recordcodec.OperationRelocate},
		},
	}}}, 4096)
	log.add(t, group)
	start, _ := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	result, err := Recover(context.Background(), log, Checkpoint{
		Mapping:     mapping.Snapshot{CoveredCommitSeq: 4, Entries: map[model.ID]mapping.Entry{1: {Addr: oldAddr, Revision: 2}}},
		ReplayStart: start, ReservedIDHigh: 5, ReservedBatchIDHigh: 5,
	}, replayConfig())
	if err != nil {
		t.Fatal(err)
	}
	entry, exists, _ := result.Mapping.Lookup(1)
	if !exists || entry.Addr != newAddr || entry.Revision != 2 || result.NextCommitSeq != 6 {
		t.Fatalf("entry=%+v exists=%v next=%d", entry, exists, result.NextCommitSeq)
	}
}
