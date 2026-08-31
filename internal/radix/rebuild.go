package radix

import (
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

// RebuildBuilder constructs a complete radix tree from a strictly increasing
// ID stream. It starts from an empty root and retains only one leaf plus one
// accumulator per upper level, so auxiliary memory is independent of the
// number of live IDs.
type RebuildBuilder struct {
	tree      *Tree
	hierarchy streamingBuilder
	leaf      rebuildLeaf
	last      model.ID
	entries   uint64
	finished  bool
}

type rebuildLeaf struct {
	active bool
	prefix uint64
	refs   [mapstore.NodeSlots]recordlog.RecordRef
}

func NewRebuildBuilder(store NodeStore, covered model.CommitSeq, cacheBytes uint64) (*RebuildBuilder, error) {
	if store == nil || cacheBytes == 0 {
		return nil, ErrInvalid
	}
	tree, err := open(store, store, 0, covered, cacheBytes)
	if err != nil {
		return nil, err
	}
	return &RebuildBuilder{tree: tree, hierarchy: streamingBuilder{tree: tree, covered: covered}}, nil
}

func (b *RebuildBuilder) Add(id model.ID, ref recordlog.RecordRef) error {
	if b == nil || b.finished || b.tree == nil || b.hierarchy.covered == 0 || id == 0 || !ref.Valid() || id <= b.last {
		return ErrInvalid
	}
	prefix := nodePrefix(id, 0)
	if b.leaf.active && b.leaf.prefix != prefix {
		if err := b.flushLeaf(); err != nil {
			return err
		}
	}
	if !b.leaf.active {
		b.leaf = rebuildLeaf{active: true, prefix: prefix}
	}
	b.leaf.refs[nodeSlot(id, 0)] = ref
	b.last = id
	b.entries++
	return nil
}

func (b *RebuildBuilder) Finish() (*Tree, error) {
	if b == nil || b.finished || b.tree == nil {
		return nil, ErrInvalid
	}
	b.finished = true
	if b.entries == 0 {
		return Open(b.tree.writer, 0, b.hierarchy.covered, b.tree.cache.capacity)
	}
	if err := b.flushLeaf(); err != nil {
		return nil, err
	}
	for level := uint8(1); level <= mapstore.MaxLevel; level++ {
		if err := b.hierarchy.flush(level); err != nil {
			return nil, err
		}
	}
	return Open(b.tree.writer, b.hierarchy.root, b.hierarchy.covered, b.tree.cache.capacity)
}

func (b *RebuildBuilder) flushLeaf() error {
	if !b.leaf.active {
		return nil
	}
	prefix, refs := b.leaf.prefix, b.leaf.refs
	b.leaf = rebuildLeaf{}
	addr, err := b.tree.writeChangedLeaf(prefix, b.hierarchy.covered, [mapstore.NodeSlots]recordlog.RecordRef{}, refs, 0)
	if err != nil {
		return err
	}
	return b.hierarchy.push(0, childChange{prefix: prefix, addr: addr})
}
