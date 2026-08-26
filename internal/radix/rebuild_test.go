package radix

import (
	"context"
	"math"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestRebuildBuilderStreamsCompleteTree(t *testing.T) {
	store := newMemoryNodeStore()
	builder, err := NewRebuildBuilder(store, 9, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	ids := []model.ID{1, 511, 512, 1 << 24, 1 << 48, model.ID(uint64(1) << 63), model.ID(math.MaxUint64)}
	want := make(map[model.ID]recordlog.VAddr, len(ids))
	for index, id := range ids {
		addr := testDataAddr(t, recordlog.SegmentID(index+1), 64)
		want[id] = addr
		if err := builder.Add(id, addr); err != nil {
			t.Fatal(err)
		}
	}
	tree, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if tree.Covered() != 9 || tree.Root() == 0 {
		t.Fatalf("covered=%d root=%v", tree.Covered(), tree.Root())
	}
	seen := 0
	if err := tree.Walk(context.Background(), func(id model.ID, addr recordlog.VAddr) error {
		if addr != want[id] {
			t.Fatalf("id=%d addr=%v want=%v", id, addr, want[id])
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(want) {
		t.Fatalf("seen=%d want=%d", seen, len(want))
	}
	for level, count := range store.appends {
		if count == 0 {
			t.Fatalf("level %d was not emitted", level)
		}
	}
}

func TestRebuildBuilderRejectsInvalidOrderAndReuse(t *testing.T) {
	store := newMemoryNodeStore()
	builder, err := NewRebuildBuilder(store, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	addr := testDataAddr(t, 1, 64)
	if err := builder.Add(2, addr); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(1, addr); err != ErrInvalid {
		t.Fatalf("out-of-order err=%v", err)
	}
	if _, err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(3, addr); err != ErrInvalid {
		t.Fatalf("add after finish err=%v", err)
	}
	if _, err := builder.Finish(); err != ErrInvalid {
		t.Fatalf("second finish err=%v", err)
	}
}

func TestRebuildBuilderAllowsEmptyZeroCommitTree(t *testing.T) {
	store := newMemoryNodeStore()
	builder, err := NewRebuildBuilder(store, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if tree.Root() != 0 || tree.Covered() != 0 {
		t.Fatalf("root=%v covered=%d", tree.Root(), tree.Covered())
	}
}
