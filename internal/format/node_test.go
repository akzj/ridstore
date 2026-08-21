package format

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

func TestMappingNodeSparseDenseRoundTrip(t *testing.T) {
	t.Parallel()

	addr1, _ := base.NewVAddr(1, 4096)
	addr2, _ := base.NewVAddr(2, 8192)
	var sparse MappingNodeBuild
	sparse.Level, sparse.NodeSeq, sparse.Prefix = 0, 1, 17
	sparse.CoveredCommitSeq = 3
	sparse.Slots[1], sparse.Slots[511] = uint64(addr1), uint64(addr2)
	encoded, err := EncodeMappingNode(sparse)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, encoded, "afbd6bb61235e3de0356c60b8bde0c6177725a90140b472040878cb2df4c54ed")
	if len(encoded) != 144 {
		t.Fatalf("sparse size=%d", len(encoded))
	}
	node, consumed, err := DecodeMappingNode(encoded, uint64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(encoded) || node.Encoding != NodeEncodingSparseBitmap || node.EntryCount != 2 {
		t.Fatalf("unexpected sparse node: %+v", node)
	}
	if got, ok := node.Lookup(511); !ok || got != uint64(addr2) {
		t.Fatalf("lookup=%x,%v", got, ok)
	}
	if _, ok := node.Lookup(2); ok {
		t.Fatal("unexpected sparse hit")
	}

	mapAddr, _ := base.NewMapAddr(3, 4096)
	var dense MappingNodeBuild
	dense.Level, dense.Encoding, dense.NodeSeq = 1, NodeEncodingDense512, 2
	for i := range dense.Slots {
		dense.Slots[i] = uint64(mapAddr)
	}
	denseBytes, err := EncodeMappingNode(dense)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, denseBytes, "c067d96eafcdc5ce27cb7a531a0e0eba2da0e6062088432326db408bd13b0537")
	if len(denseBytes) != 4160 {
		t.Fatalf("dense size=%d", len(denseBytes))
	}
	denseNode, _, err := DecodeMappingNode(denseBytes, uint64(len(denseBytes)))
	if err != nil || denseNode.Encoding != NodeEncodingDense512 || denseNode.EntryCount != 512 {
		t.Fatalf("dense node=%+v error=%v", denseNode, err)
	}
}

func TestMappingNodeEncodingThresholdAndValidation(t *testing.T) {
	t.Parallel()

	addr, _ := base.NewVAddr(1, 4096)
	var node MappingNodeBuild
	node.Level, node.NodeSeq = 0, 1
	for i := 0; i < 503; i++ {
		node.Slots[i] = uint64(addr)
	}
	sparse, err := EncodeMappingNode(node)
	if err != nil || len(sparse) != 64+64+503*8 {
		t.Fatalf("503 encoding size=%d error=%v", len(sparse), err)
	}
	node.Slots[503] = uint64(addr)
	dense, err := EncodeMappingNode(node)
	if err != nil || len(dense) != 4160 {
		t.Fatalf("504 encoding size=%d error=%v", len(dense), err)
	}

	var top MappingNodeBuild
	top.Level, top.NodeSeq = 7, 1
	top.Slots[2] = uint64(addr)
	if _, err := EncodeMappingNode(top); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("top slot error=%v", err)
	}
	node.Prefix = uint64(1) << 55
	if _, err := EncodeMappingNode(node); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("prefix error=%v", err)
	}
}

func TestMappingNodeRejectsCorruption(t *testing.T) {
	t.Parallel()

	addr, _ := base.NewVAddr(1, 4096)
	var build MappingNodeBuild
	build.Level, build.NodeSeq, build.Slots[1] = 0, 1, uint64(addr)
	encoded, _ := EncodeMappingNode(build)
	encoded[MappingNodeHeaderSize+8] ^= 1
	if _, _, err := DecodeMappingNode(encoded, uint64(len(encoded))); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func FuzzDecodeMappingNode(f *testing.F) {
	addr, _ := base.NewVAddr(1, 4096)
	var build MappingNodeBuild
	build.Level, build.NodeSeq, build.Slots[1] = 0, 1, uint64(addr)
	seed, _ := EncodeMappingNode(build)
	f.Add(seed)
	f.Add([]byte("short"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeMappingNode(data, uint64(len(data)))
	})
}
