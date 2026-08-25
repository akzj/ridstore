package recordlog

import (
	"errors"
	"math"
	"testing"
)

func TestVAddrSizeTagsAndEnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		size uint32
		tag  uint8
		hint uint32
	}{
		{32, 0, 64}, {64, 0, 64}, {72, 1, 128}, {128, 1, 128},
		{136, 2, 256}, {512, 3, 512}, {1024, 4, 1024},
		{2048, 5, 2048}, {4096, 6, 4096}, {128 << 10, 6, 4096},
	}
	for _, tt := range tests {
		addr, err := NewVAddr(7, 4096, tt.size)
		if err != nil {
			t.Fatalf("size %d: %v", tt.size, err)
		}
		if addr.SegmentID() != 7 || addr.Offset() != 4096 || addr.SizeTag() != tt.tag {
			t.Fatalf("size %d: addr=%x segment=%d offset=%d tag=%d", tt.size, uint64(addr), addr.SegmentID(), addr.Offset(), addr.SizeTag())
		}
		hint, err := addr.ReadHint()
		if err != nil || hint != tt.hint || !addr.MatchesPhysicalSize(tt.size) {
			t.Fatalf("size %d: hint=%d match=%v err=%v", tt.size, hint, addr.MatchesPhysicalSize(tt.size), err)
		}
		end, err := addr.End(tt.size)
		if err != nil || end != (LogPos{SegmentID: 7, Offset: 4096 + tt.size}) {
			t.Fatalf("size %d: end=%+v err=%v", tt.size, end, err)
		}
	}
}

func TestVAddrRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		segment SegmentID
		offset  uint32
		size    uint32
	}{
		{0, 64, 64}, {1, 56, 64}, {1, 65, 64}, {1, 64, 31}, {1, 64, 65},
	} {
		if _, err := NewVAddr(test.segment, test.offset, test.size); !errors.Is(err, ErrInvalidVAddr) {
			t.Fatalf("NewVAddr(%d,%d,%d) err=%v", test.segment, test.offset, test.size, err)
		}
	}
	reserved := uint64(1)<<32 | uint64(SegmentHeaderSize) | uint64(reservedSizeTag)
	if _, err := ParseVAddr(reserved); !errors.Is(err, ErrInvalidVAddr) {
		t.Fatalf("reserved tag err=%v", err)
	}
	addr, _ := NewVAddr(1, math.MaxUint32&^7, 64)
	if _, err := addr.End(64); !errors.Is(err, ErrInvalidVAddr) {
		t.Fatalf("overflowing end err=%v", err)
	}
}

func TestLogPosRoundTripAndCompare(t *testing.T) {
	t.Parallel()
	p, err := NewLogPos(9, 8192)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseLogPos(p.Uint64())
	if err != nil || decoded != p {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	if p.Compare(LogPos{SegmentID: 9, Offset: 8200}) >= 0 || p.Compare(LogPos{SegmentID: 8, Offset: 9000}) <= 0 || p.Compare(p) != 0 {
		t.Fatal("unexpected LogPos ordering")
	}
	if _, err := NewLogPos(1, 65); !errors.Is(err, ErrInvalidLogPos) {
		t.Fatalf("unaligned position err=%v", err)
	}
}
