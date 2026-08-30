package segmentstats

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/recordmeta"
	"github.com/akzj/ridstore/internal/storecatalog"
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

type mappingChange struct {
	id                   model.ID
	oldRef, newRef       recordlog.RecordRef
	oldExists, newExists bool
}

type fakeIncremental struct {
	changes []mappingChange
	entries map[model.ID]recordlog.RecordRef
}

func (m fakeIncremental) WalkChanges(ctx context.Context, visit func(model.ID, recordlog.RecordRef, bool, recordlog.RecordRef, bool) error) error {
	for _, change := range m.changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(change.id, change.oldRef, change.oldExists, change.newRef, change.newExists); err != nil {
			return err
		}
	}
	return nil
}

func (m fakeIncremental) LookupRef(id model.ID) (recordlog.RecordRef, bool, error) {
	ref, ok := m.entries[id]
	return ref, ok, nil
}

type inspectedRecord struct {
	header  recordlog.RecordMetadata
	prefix  []byte
	payload []byte
}

type fakeInspector map[recordlog.VAddr]inspectedRecord

func (f fakeInspector) Inspect(_ context.Context, addr recordlog.VAddr, prefixBytes uint32) (recordlog.RecordMetadata, []byte, error) {
	record, ok := f[addr]
	if !ok || uint32(len(record.prefix)) != prefixBytes {
		return recordlog.RecordMetadata{}, nil, errors.New("missing record")
	}
	return record.header, append([]byte(nil), record.prefix...), nil
}

func (f fakeInspector) ScanSegment(ctx context.Context, id recordlog.SegmentID, visit func(recordlog.AppendResult, []byte) error) error {
	for addr, record := range f {
		if addr.SegmentID() != id {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := recordlog.NewAppendResult(addr, record.header.PhysicalSize)
		if err != nil {
			return err
		}
		if err := visit(result, append([]byte(nil), record.payload...)); err != nil {
			return err
		}
	}
	return nil
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
		header:  recordlog.RecordMetadata{PhysicalSize: physical, PayloadSize: uint32(len(payload)), Addr: addr},
		prefix:  append([]byte(nil), payload[:recordcodec.PutHeaderSize]...),
		payload: append([]byte(nil), payload...),
	}
	return addr
}

func recordRef(records fakeInspector, addr recordlog.VAddr) recordlog.RecordRef {
	return recordlog.RecordRef{Addr: addr, PhysicalSize: records[addr].header.PhysicalSize}
}

func TestBuildExactSealedStats(t *testing.T) {
	records := make(fakeInspector)
	a := addPut(t, records, 1, 64, 1, 7)
	b := addPut(t, records, 1, 128, 2, 80)
	c := addPut(t, records, 2, 64, 3, 1)
	active := addPut(t, records, 3, 64, 4, 900)
	stats, err := Build(context.Background(), fakeMapping{{1, recordRef(records, a)}, {2, recordRef(records, b)}, {3, recordRef(records, c)}, {4, recordRef(records, active)}}, records, nil, FileSet{
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
		{name: "invalid ref", maxEntries: 1},
		{name: "unknown segment", maxEntries: 1},
		{name: "budget", maxEntries: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := make(fakeInspector)
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
			_, err := Build(context.Background(), mapping, records, nil, files, 1024, test.maxEntries)
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

func TestBuildUsesValidatedCachedMetadataWithoutInspect(t *testing.T) {
	records := make(fakeInspector)
	addr := addPut(t, records, 1, 64, 7, 80)
	physical, _ := recordlog.PhysicalRecordSize(uint64(recordcodec.PutHeaderSize + 80))
	cache := recordmeta.New(64)
	cache.Remember(addr, 7, physical)
	stats, err := Build(context.Background(), fakeMapping{{7, recordRef(records, addr)}}, fakeInspector{}, cache, FileSet{
		Active: 2, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}},
	}, 1024, 1)
	if err != nil || len(stats) != 1 || stats[0].LiveBytes != uint64(physical) || cache.Stats().Hits != 0 {
		t.Fatalf("stats=%+v cache=%+v err=%v", stats, cache.Stats(), err)
	}
}

func TestBuildRejectsInvalidRecordRef(t *testing.T) {
	addr, err := recordlog.NewVAddr(1, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Build(context.Background(), fakeMapping{{7, recordlog.RecordRef{Addr: addr, PhysicalSize: 128}}}, fakeInspector{}, nil, FileSet{
		Active: 2, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}},
	}, 1024, 1)
	if !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}

func TestBuildIncrementalAppliesOnlyFoldedChanges(t *testing.T) {
	records := make(fakeInspector)
	old := addPut(t, records, 1, 64, 1, 1)
	unchanged := addPut(t, records, 1, 128, 2, 80)
	moved := addPut(t, records, 2, 64, 1, 7)
	created := addPut(t, records, 2, 128, 3, 1)
	active := addPut(t, records, 3, 64, 4, 1)
	oldSize := records[old].header.PhysicalSize
	unchangedSize := records[unchanged].header.PhysicalSize
	movedSize := records[moved].header.PhysicalSize
	createdSize := records[created].header.PhysicalSize
	cache := recordmeta.New(16)
	for id, addr := range map[model.ID]recordlog.VAddr{1: old, 3: created, 4: active} {
		cache.Remember(addr, id, records[addr].header.PhysicalSize)
	}
	cache.Remember(moved, 1, movedSize)
	current := fakeIncremental{changes: []mappingChange{
		{id: 1, oldRef: recordRef(records, old), oldExists: true, newRef: recordRef(records, moved), newExists: true},
		{id: 3, newRef: recordRef(records, created), newExists: true},
		{id: 4, newRef: recordRef(records, active), newExists: true},
	}, entries: map[model.ID]recordlog.RecordRef{1: recordRef(records, moved), 2: recordRef(records, unchanged), 3: recordRef(records, created), 4: recordRef(records, active)}}
	stats, err := BuildIncremental(context.Background(), current, fakeInspector{}, fakeInspector{}, cache,
		[]storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: uint64(oldSize + unchangedSize), LiveRecords: 2}}, 3, FileSet{
			Active: 3, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}, {SegmentID: 2, ValidEnd: 512}},
		}, 1024, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 || stats[0] != (storecatalog.SegmentStats{SegmentID: 1, LiveBytes: uint64(unchangedSize), LiveRecords: 1}) ||
		stats[1] != (storecatalog.SegmentStats{SegmentID: 2, LiveBytes: uint64(movedSize + createdSize), LiveRecords: 2}) {
		t.Fatalf("stats=%+v", stats)
	}
	if got := cache.Stats().Hits; got != 0 {
		t.Fatalf("cache hits=%d", got)
	}
}

func TestBuildIncrementalRejectsBaseUnderflow(t *testing.T) {
	records := make(fakeInspector)
	old := addPut(t, records, 1, 64, 1, 80)
	current := fakeIncremental{changes: []mappingChange{{id: 1, oldRef: recordRef(records, old), oldExists: true}}}
	_, err := BuildIncremental(context.Background(), current, records, records, nil,
		[]storecatalog.SegmentStats{{SegmentID: 1, LiveBytes: 64, LiveRecords: 1}},
		2, FileSet{Active: 2, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}}}, 1024, 1)
	if !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}

func TestBuildIncrementalScansOnlyFormerActiveAfterRotation(t *testing.T) {
	records := make(fakeInspector)
	unchanged := addPut(t, records, 1, 64, 1, 1)
	old := addPut(t, records, 1, 128, 2, 1)
	moved := addPut(t, records, 2, 64, 2, 80)
	created := addPut(t, records, 2, 192, 3, 7)
	current := fakeIncremental{
		changes: []mappingChange{
			{id: 2, oldRef: recordRef(records, old), oldExists: true, newRef: recordRef(records, moved), newExists: true},
			{id: 3, newRef: recordRef(records, created), newExists: true},
		},
		entries: map[model.ID]recordlog.RecordRef{1: recordRef(records, unchanged), 2: recordRef(records, moved), 3: recordRef(records, created)},
	}
	stats, err := BuildIncremental(context.Background(), current, records, records, nil, nil, 1, FileSet{
		Active: 3, Sealed: []recordlog.SegmentSummary{{SegmentID: 1, ValidEnd: 512}, {SegmentID: 2, ValidEnd: 512}},
	}, 1024, 2)
	if err != nil {
		t.Fatal(err)
	}
	unchangedSize := records[unchanged].header.PhysicalSize
	movedSize := records[moved].header.PhysicalSize
	createdSize := records[created].header.PhysicalSize
	if len(stats) != 2 || stats[0] != (storecatalog.SegmentStats{SegmentID: 1, LiveBytes: uint64(unchangedSize), LiveRecords: 1}) ||
		stats[1] != (storecatalog.SegmentStats{SegmentID: 2, LiveBytes: uint64(movedSize + createdSize), LiveRecords: 2}) {
		t.Fatalf("stats=%+v", stats)
	}
}
