package segmentstats

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type mappingEntry struct {
	id   model.ID
	addr recordlog.VAddr
}

type fakeMapping []mappingEntry

func (m fakeMapping) Walk(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	for _, entry := range m {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(entry.id, entry.addr); err != nil {
			return err
		}
	}
	return nil
}

type inspectedRecord struct {
	header recordlog.RecordHeader
	prefix []byte
}

type fakeInspector map[recordlog.VAddr]inspectedRecord

func (f fakeInspector) Inspect(_ context.Context, addr recordlog.VAddr, prefixBytes uint32) (recordlog.RecordHeader, []byte, error) {
	record, ok := f[addr]
	if !ok || uint32(len(record.prefix)) != prefixBytes {
		return recordlog.RecordHeader{}, nil, errors.New("missing record")
	}
	return record.header, append([]byte(nil), record.prefix...), nil
}

func addPut(t *testing.T, records fakeInspector, segment recordlog.SegmentID, offset uint32, id model.ID, valueBytes int) recordlog.VAddr {
	t.Helper()
	payload, err := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: model.BatchID(id + 100), RecordID: id, Value: make([]byte, valueBytes)}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	physical, _ := recordlog.PhysicalRecordSize(uint64(len(payload)))
	addr, err := recordlog.NewVAddr(segment, offset, physical)
	if err != nil {
		t.Fatal(err)
	}
	records[addr] = inspectedRecord{
		header: recordlog.RecordHeader{PhysicalSize: physical, PayloadSize: uint32(len(payload)), Addr: addr},
		prefix: append([]byte(nil), payload[:recordcodec.PutHeaderSize]...),
	}
	return addr
}

func TestBuildExactSealedStats(t *testing.T) {
	records := make(fakeInspector)
	a := addPut(t, records, 1, 64, 1, 7)
	b := addPut(t, records, 1, 128, 2, 80)
	c := addPut(t, records, 2, 64, 3, 1)
	active := addPut(t, records, 3, 64, 4, 900)
	stats, err := Build(context.Background(), fakeMapping{{1, a}, {2, b}, {3, c}, {4, active}}, records, FileSet{
		Active: 3,
		Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}, {SegmentID: 2, ValidEnd: 512}},
	}, 1024, 2)
	if err != nil {
		t.Fatal(err)
	}
	aSize, _ := recordlog.PhysicalRecordSize(uint64(recordcodec.PutHeaderSize + 7))
	bSize, _ := recordlog.PhysicalRecordSize(uint64(recordcodec.PutHeaderSize + 80))
	cSize, _ := recordlog.PhysicalRecordSize(uint64(recordcodec.PutHeaderSize + 1))
	if len(stats) != 2 || stats[0].SegmentID != 1 || stats[0].LiveRecords != 2 || stats[0].LiveBytes != uint64(aSize+bSize) ||
		stats[1].SegmentID != 2 || stats[1].LiveRecords != 1 || stats[1].LiveBytes != uint64(cSize) {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestBuildRejectsWrongIdentityUnknownSegmentAndBudget(t *testing.T) {
	for _, test := range []struct {
		name       string
		mapping    fakeMapping
		files      FileSet
		maxEntries uint64
	}{
		{name: "wrong identity", maxEntries: 1},
		{name: "unknown segment", maxEntries: 1},
		{name: "budget", maxEntries: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := make(fakeInspector)
			a := addPut(t, records, 1, 64, 1, 1)
			b := addPut(t, records, 2, 64, 2, 1)
			files := FileSet{Active: 3, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}, {SegmentID: 2, ValidEnd: 512}}}
			mapping := fakeMapping{{1, a}}
			switch test.name {
			case "wrong identity":
				mapping[0].id = 9
			case "unknown segment":
				unknown := addPut(t, records, 4, 64, 1, 1)
				mapping[0].addr = unknown
			case "budget":
				mapping = append(mapping, mappingEntry{2, b})
			}
			_, err := Build(context.Background(), mapping, records, files, 1024, test.maxEntries)
			if test.name == "budget" {
				if !errors.Is(err, base.ErrOverflow) {
					t.Fatalf("err=%v", err)
				}
			} else if !errors.Is(err, base.ErrCorrupt) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
