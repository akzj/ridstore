package model

import (
	"errors"
	"testing"
)

func TestMapAddrRoundTrip(t *testing.T) {
	t.Parallel()
	addr, err := NewMapAddr(7, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if addr.SegmentID() != 7 || addr.Offset() != 4096 || !addr.Valid() {
		t.Fatalf("addr=%x", uint64(addr))
	}
	if parsed, err := ParseMapAddr(uint64(addr)); err != nil || parsed != addr {
		t.Fatalf("parsed=%x err=%v", uint64(parsed), err)
	}
	if _, err := NewMapAddr(0, 64); !errors.Is(err, ErrInvalidMapAddr) {
		t.Fatalf("zero segment err=%v", err)
	}
	if _, err := NewMapAddr(1, 65); !errors.Is(err, ErrInvalidMapAddr) {
		t.Fatalf("unaligned offset err=%v", err)
	}
}
