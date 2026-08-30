package mapstore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func dataAddr(t *testing.T, segment recordlog.SegmentID, offset uint32) uint64 {
	t.Helper()
	addr, err := recordlog.NewVAddr(segment, offset, 64)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(addr)
}

func mapAddr(t *testing.T, segment model.MapSegmentID, offset uint32) uint64 {
	t.Helper()
	addr, err := model.NewMapAddr(segment, offset)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(addr)
}

func TestSparseNodeRoundTrip(t *testing.T) {
	build := NodeBuild{Level: 0, NodeSeq: 9, Prefix: 3, CoveredCommitSeq: 7}
	build.Slots[0] = dataAddr(t, 1, 64)
	build.Slots[511] = dataAddr(t, 2, 128)
	build.Sizes[0], build.Sizes[511] = 64, 64
	encoded, err := EncodeNode(build)
	if err != nil {
		t.Fatal(err)
	}
	node, size, err := DecodeNode(encoded, uint32(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	if size != uint32(len(encoded)) || node.Encoding != EncodingSparse || node.EntryCount != 2 || node.Prefix != 3 {
		t.Fatalf("node=%+v size=%d", node, size)
	}
	for slot, want := range map[uint16]uint64{0: build.Slots[0], 511: build.Slots[511]} {
		if got, ok := node.Lookup(slot); !ok || got != want {
			t.Fatalf("slot %d got=%d ok=%v", slot, got, ok)
		}
	}
	if expanded := node.Slots(); expanded != build.Slots {
		t.Fatal("expanded sparse slots differ")
	}
	if _, ok := node.Lookup(1); ok {
		t.Fatal("empty sparse slot reported present")
	}
}

func TestNodeEncodingGolden(t *testing.T) {
	build := NodeBuild{Level: 0, NodeSeq: 9, Prefix: 3, CoveredCommitSeq: 7}
	build.Slots[0] = dataAddr(t, 1, 64)
	build.Slots[511] = dataAddr(t, 2, 128)
	build.Sizes[0], build.Sizes[511] = 64, 64
	encoded, err := EncodeNode(build)
	if err != nil {
		t.Fatal(err)
	}
	const want = "e1d280cfcf24f0be39eb3e27c616703f8770d5d2699007fcaaa1d77594eb488a"
	if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != want {
		t.Fatalf("golden digest=%s", got)
	}
}

func TestDenseInternalNodeRoundTrip(t *testing.T) {
	build := NodeBuild{Level: 2, NodeSeq: 4, Prefix: 1, CoveredCommitSeq: 12}
	for index := 0; index < int(SparseDenseBoundary); index++ {
		build.Slots[index] = mapAddr(t, 3, uint32(64+index*8))
	}
	encoded, err := EncodeNode(build)
	if err != nil {
		t.Fatal(err)
	}
	node, _, err := DecodeNode(encoded, uint32(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	if node.Encoding != EncodingDense || len(node.Values) != int(NodeSlots) {
		t.Fatalf("encoding=%d values=%d", node.Encoding, len(node.Values))
	}
	if got, ok := node.Lookup(503); !ok || got != build.Slots[503] {
		t.Fatalf("got=%d ok=%v", got, ok)
	}
}

func TestNodeRejectsInvalidIdentityAndAddresses(t *testing.T) {
	for _, build := range []NodeBuild{
		{Level: 8, NodeSeq: 1, CoveredCommitSeq: 1, Slots: [NodeSlots]uint64{1}},
		{Level: 7, NodeSeq: 1, CoveredCommitSeq: 1, Slots: func() (slots [NodeSlots]uint64) { slots[2] = mapAddr(t, 1, 64); return }()},
		{Level: 0, NodeSeq: 1, CoveredCommitSeq: 1, Slots: [NodeSlots]uint64{1}},
	} {
		if _, err := EncodeNode(build); !errors.Is(err, ErrInvalid) {
			t.Fatalf("build=%+v err=%v", build, err)
		}
	}
}

func TestNodeDecodeRejectsCorruptionAndBoundaryCrossing(t *testing.T) {
	build := NodeBuild{Level: 0, NodeSeq: 1, CoveredCommitSeq: 1}
	build.Slots[4] = dataAddr(t, 1, 64)
	build.Sizes[4] = 64
	encoded, err := EncodeNode(build)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeNode(encoded, uint32(len(encoded)-8)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("boundary err=%v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if _, _, err := DecodeNode(corrupt, uint32(len(corrupt))); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("payload err=%v", err)
	}
	corrupt = append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint64(corrupt[56:64], 1)
	binary.LittleEndian.PutUint32(corrupt[48:52], headerChecksum(corrupt[:NodeHeaderSize]))
	if _, _, err := DecodeNode(corrupt, uint32(len(corrupt))); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reserved err=%v", err)
	}
}

func FuzzDecodeNode(f *testing.F) {
	build := NodeBuild{Level: 0, NodeSeq: 1, CoveredCommitSeq: 1}
	addr, _ := recordlog.NewVAddr(1, 64, 64)
	build.Slots[1] = uint64(addr)
	build.Sizes[1] = 64
	seed, _ := EncodeNode(build)
	f.Add(seed, uint32(len(seed)))
	f.Fuzz(func(t *testing.T, value []byte, remaining uint32) {
		_, _, _ = DecodeNode(value, remaining)
	})
}
