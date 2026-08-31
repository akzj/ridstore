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
	id  model.ID
	ref recordlog.RecordRef
}

type fakeMapping []mappingEntry

func (m fakeMapping) WalkRefs(ctx context.Context, visit func(model.ID, recordlog.RecordRef) error) error {
	for _, entry := range m {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(entry.id, entry.ref); err != nil {
			return err
		}
	}
	return nil
}

type fakeRecords map[recordlog.VAddr]uint32

func addPut(t *testing.T, records fakeRecords, segment recordlog.SegmentID, offset uint32, id model.ID, valueBytes int) recordlog.VAddr {
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
	records[addr] = physical
	return addr
}

func recordRef(records fakeRecords, addr recordlog.VAddr) recordlog.RecordRef {
	return recordlog.RecordRef{Addr: addr, PhysicalSize: records[addr]}
}

func TestBuildExactSealedStats(t *testing.T) {
	records := make(fakeRecords)
	a := addPut(t, records, 1, 64, 1, 7)
	b := addPut(t, records, 1, 128, 2, 80)
	c := addPut(t, records, 2, 64, 3, 1)
	active := addPut(t, records, 3, 64, 4, 900)
	stats, err := Build(context.Background(), fakeMapping{{1, recordRef(records, a)}, {2, recordRef(records, b)}, {3, recordRef(records, c)}, {4, recordRef(records, active)}}, FileSet{
		Active: 3,
		Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}, {SegmentID: 2, ValidEnd: 512}},
	}, 2)
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
		{name: "invalid ref", maxEntries: 1},
		{name: "unknown segment", maxEntries: 1},
		{name: "budget", maxEntries: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := make(fakeRecords)
			a := addPut(t, records, 1, 64, 1, 1)
			b := addPut(t, records, 2, 64, 2, 1)
			files := FileSet{Active: 3, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}, {SegmentID: 2, ValidEnd: 512}}}
			mapping := fakeMapping{{1, recordRef(records, a)}}
			switch test.name {
			case "invalid ref":
				mapping[0].ref.PhysicalSize = 65
			case "unknown segment":
				unknown := addPut(t, records, 4, 64, 1, 1)
				mapping[0].ref = recordRef(records, unknown)
			case "budget":
				mapping = append(mapping, mappingEntry{2, recordRef(records, b)})
			}
			_, err := Build(context.Background(), mapping, files, test.maxEntries)
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

func TestBuildUsesRecordRefWithoutInspect(t *testing.T) {
	records := make(fakeRecords)
	addr := addPut(t, records, 1, 64, 7, 80)
	physical, _ := recordlog.PhysicalRecordSize(uint64(recordcodec.PutHeaderSize + 80))
	stats, err := Build(context.Background(), fakeMapping{{7, recordRef(records, addr)}}, FileSet{
		Active: 2, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}},
	}, 1)
	if err != nil || len(stats) != 1 || stats[0].LiveBytes != uint64(physical) {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestBuildRejectsInvalidRecordRef(t *testing.T) {
	addr, err := recordlog.NewVAddr(1, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Build(context.Background(), fakeMapping{{7, recordlog.RecordRef{Addr: addr, PhysicalSize: 128}}}, FileSet{
		Active: 2, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}},
	}, 1)
	if !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}
