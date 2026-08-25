package recordlog

import "testing"

func FuzzDecodeRecord(f *testing.F) {
	physical, _ := PhysicalRecordSize(3)
	addr, _ := NewVAddr(1, SegmentHeaderSize, physical)
	seed, _ := EncodeRecord(addr, []byte("abc"))
	f.Add(seed)
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _, _ = DecodeRecord(value)
	})
}

func FuzzDecodeSegmentHeader(f *testing.F) {
	seed, _ := EncodeSegmentHeader(SegmentHeader{LogID: testLogID(), SegmentID: 1, SegmentSize: 1 << 20})
	f.Add(seed[:])
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodeSegmentHeader(value)
	})
}

func FuzzDecodeSegmentFooter(f *testing.F) {
	seed, _ := EncodeSegmentFooter(SegmentFooter{SegmentID: 1, DataEnd: SegmentHeaderSize})
	f.Add(seed[:])
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodeSegmentFooter(value)
	})
}

func FuzzDecodeRotationJournal(f *testing.F) {
	addr, _ := NewVAddr(1, SegmentHeaderSize, 64)
	seed, _ := encodeRotationJournal(rotationJournal{
		BaseGeneration: 1, LogID: LogID{1}, SegmentSize: 1024,
		Old:       SegmentSummary{SegmentID: 1, ValidEnd: SegmentHeaderSize + 64, RecordCount: 1, FirstAddr: addr, LastAddr: addr},
		NewActive: 2, NextSegmentID: 3,
	})
	f.Add(seed[:])
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeRotationJournal(data)
	})
}
