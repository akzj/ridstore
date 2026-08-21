package base

import (
	"errors"
	"math"
	"testing"
)

func TestPhysicalAddressTypes(t *testing.T) {
	t.Parallel()

	vaddr, err := NewVAddr(7, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := vaddr.SegmentID(), DataSegmentID(7); got != want {
		t.Fatalf("segment ID = %d, want %d", got, want)
	}
	if got, want := vaddr.Offset(), uint32(8192); got != want {
		t.Fatalf("offset = %d, want %d", got, want)
	}

	if _, err := ParseVAddr(uint64(vaddr)); err != nil {
		t.Fatalf("parse valid VAddr: %v", err)
	}
	if _, err := NewMapAddr(7, 8192); err != nil {
		t.Fatalf("new MapAddr: %v", err)
	}
	if _, err := NewLogPos(7, 8192); err != nil {
		t.Fatalf("new LogPos: %v", err)
	}
}

func TestPhysicalAddressRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		fileID DataSegmentID
		offset uint32
	}{
		{name: "zero file ID", fileID: 0, offset: 4096},
		{name: "header", fileID: 1, offset: 4088},
		{name: "unaligned", fileID: 1, offset: 4097},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewVAddr(tt.fileID, tt.offset); !errors.Is(err, ErrInvalidAddress) {
				t.Fatalf("error = %v, want ErrInvalidAddress", err)
			}
		})
	}

	if _, err := ParseVAddr(0); !errors.Is(err, ErrInvalidAddress) {
		t.Fatalf("zero parse error = %v, want ErrInvalidAddress", err)
	}
}

func TestPhysicalAddressBoundary(t *testing.T) {
	t.Parallel()

	const maxAlignedOffset = uint32(math.MaxUint32 &^ 7)
	vaddr, err := NewVAddr(DataSegmentID(math.MaxUint32), maxAlignedOffset)
	if err != nil {
		t.Fatal(err)
	}
	if vaddr.SegmentID() != DataSegmentID(math.MaxUint32) || vaddr.Offset() != maxAlignedOffset {
		t.Fatalf("boundary address decoded incorrectly: segment=%d offset=%d", vaddr.SegmentID(), vaddr.Offset())
	}
}

func TestCheckedArithmetic(t *testing.T) {
	t.Parallel()

	if got, err := AddUint32(1, 2); err != nil || got != 3 {
		t.Fatalf("AddUint32 = %d, %v", got, err)
	}
	if _, err := AddUint32(math.MaxUint32, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("AddUint32 overflow error = %v", err)
	}
	if _, err := AddUint64(math.MaxUint64, 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("AddUint64 overflow error = %v", err)
	}
	if _, err := MulUint64(math.MaxUint64, 2); !errors.Is(err, ErrOverflow) {
		t.Fatalf("MulUint64 overflow error = %v", err)
	}
	if got, err := Align8(65); err != nil || got != 72 {
		t.Fatalf("Align8 = %d, %v", got, err)
	}
	if _, err := Align8(math.MaxUint64); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Align8 overflow error = %v", err)
	}
}
