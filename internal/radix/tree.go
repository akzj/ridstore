package radix

import (
	"context"
	"errors"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

type NodeReader interface {
	Read(model.MapAddr) (mapstore.Node, error)
}

type NodeStore interface {
	NodeReader
	Append(level uint8, prefix uint64, covered model.CommitSeq, slots [mapstore.NodeSlots]uint64) (model.MapAddr, error)
	AppendLeaf(prefix uint64, covered model.CommitSeq, refs [mapstore.NodeSlots]recordlog.RecordRef) (model.MapAddr, error)
}

// Walk visits every leaf in ID order. The tree is immutable, so callers see
// one complete checkpoint root without holding Mapping publication locks.
func (t *Tree) Walk(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	if visit == nil {
		return ErrInvalid
	}
	return t.WalkRefs(ctx, func(id model.ID, ref recordlog.RecordRef) error { return visit(id, ref.Addr) })
}

func (t *Tree) WalkRefs(ctx context.Context, visit func(model.ID, recordlog.RecordRef) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.root == 0 {
		return nil
	}
	return t.walkNode(ctx, t.root, mapstore.MaxLevel, 0, visit)
}

// ReachableBytes returns the exact encoded bytes reachable from this immutable
// root. It counts Mapping nodes, not mapped RecordLog values.
func (t *Tree) ReachableBytes(ctx context.Context) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if t.root == 0 {
		return 0, nil
	}
	return t.reachableNodeBytes(ctx, t.root, mapstore.MaxLevel, 0)
}

func (t *Tree) reachableNodeBytes(ctx context.Context, addr model.MapAddr, level uint8, prefix uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	node, err := t.load(addr, level, prefix, level == mapstore.MaxLevel, false)
	if err != nil {
		return 0, err
	}
	total := uint64(node.PhysicalSize())
	if level == 0 {
		return total, nil
	}
	for slot := uint16(0); slot < mapstore.NodeSlots; slot++ {
		value, exists := node.Lookup(slot)
		if !exists {
			continue
		}
		child, parseErr := model.ParseMapAddr(value)
		if parseErr != nil {
			return 0, errors.Join(ErrCorrupt, parseErr)
		}
		bytes, walkErr := t.reachableNodeBytes(ctx, child, level-1, prefix<<9|uint64(slot))
		if walkErr != nil {
			return 0, walkErr
		}
		if total > ^uint64(0)-bytes {
			return 0, ErrInvalid
		}
		total += bytes
	}
	return total, nil
}

func (t *Tree) walkNode(ctx context.Context, addr model.MapAddr, level uint8, prefix uint64, visit func(model.ID, recordlog.RecordRef) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	node, err := t.load(addr, level, prefix, level == mapstore.MaxLevel, false)
	if err != nil {
		return err
	}
	for slot := uint16(0); slot < mapstore.NodeSlots; slot++ {
		if level == 0 {
			ref, exists := node.LookupRef(slot)
			if !exists {
				continue
			}
			id := model.ID(prefix<<9 | uint64(slot))
			if !ref.Valid() || id == 0 {
				return ErrCorrupt
			}
			if err := visit(id, ref); err != nil {
				return err
			}
			continue
		}
		value, exists := node.Lookup(slot)
		if !exists {
			continue
		}
		child, err := model.ParseMapAddr(value)
		if err != nil {
			return errors.Join(ErrCorrupt, err)
		}
		childPrefix := prefix<<9 | uint64(slot)
		if err := t.walkNode(ctx, child, level-1, childPrefix, visit); err != nil {
			return err
		}
	}
	return nil
}

type Tree struct {
	reader  NodeReader
	writer  NodeStore
	root    model.MapAddr
	covered model.CommitSeq
	cache   *nodeCache
}

func Open(store NodeStore, root model.MapAddr, covered model.CommitSeq, cacheBytes uint64) (*Tree, error) {
	if store == nil {
		return nil, ErrInvalid
	}
	return open(store, store, root, covered, cacheBytes)
}

// OpenReadOnly constructs a tree that can Lookup and Walk but cannot build a
// new root. It keeps verification readers out of the append-capable contract.
func OpenReadOnly(reader NodeReader, root model.MapAddr, covered model.CommitSeq, cacheBytes uint64) (*Tree, error) {
	return open(reader, nil, root, covered, cacheBytes)
}

func open(reader NodeReader, writer NodeStore, root model.MapAddr, covered model.CommitSeq, cacheBytes uint64) (*Tree, error) {
	if reader == nil || cacheBytes == 0 || (root != 0 && !root.Valid()) {
		return nil, ErrInvalid
	}
	tree := &Tree{reader: reader, writer: writer, root: root, covered: covered, cache: newNodeCache(cacheBytes)}
	if root != 0 {
		if _, err := tree.load(root, mapstore.MaxLevel, 0, true, true); err != nil {
			return nil, err
		}
	}
	return tree, nil
}

func (t *Tree) Root() model.MapAddr      { return t.root }
func (t *Tree) Covered() model.CommitSeq { return t.covered }
func (t *Tree) CacheBytes() uint64       { return t.cache.bytes() }

func (t *Tree) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	ref, exists, err := t.LookupRef(id)
	return ref.Addr, exists, err
}

func (t *Tree) LookupRef(id model.ID) (recordlog.RecordRef, bool, error) {
	if id == 0 {
		return recordlog.RecordRef{}, false, ErrInvalid
	}
	addr := t.root
	if addr == 0 {
		return recordlog.RecordRef{}, false, nil
	}
	for level := mapstore.MaxLevel; ; level-- {
		prefix := nodePrefix(id, level)
		node, err := t.load(addr, level, prefix, level == mapstore.MaxLevel, true)
		if err != nil {
			return recordlog.RecordRef{}, false, err
		}
		if level == 0 {
			ref, exists := node.LookupRef(nodeSlot(id, level))
			if !exists {
				return recordlog.RecordRef{}, false, nil
			}
			if !ref.Valid() {
				return recordlog.RecordRef{}, false, ErrCorrupt
			}
			return ref, true, nil
		}
		value, exists := node.Lookup(nodeSlot(id, level))
		if !exists {
			return recordlog.RecordRef{}, false, nil
		}
		childAddr, err := model.ParseMapAddr(value)
		if err != nil {
			return recordlog.RecordRef{}, false, errors.Join(ErrCorrupt, err)
		}
		addr = childAddr
	}
}

func (t *Tree) load(addr model.MapAddr, level uint8, prefix uint64, pin, promote bool) (mapstore.Node, error) {
	node, err := t.cache.get(addr, pin, promote, func() (mapstore.Node, error) { return t.reader.Read(addr) })
	if err != nil {
		return mapstore.Node{}, err
	}
	if node.Level != level || node.Prefix != prefix || node.CoveredCommitSeq > t.covered {
		return mapstore.Node{}, ErrCorrupt
	}
	return node, nil
}

func nodeSlot(id model.ID, level uint8) uint16 {
	if level == mapstore.MaxLevel {
		return uint16(uint64(id) >> 63)
	}
	return uint16(uint64(id) >> (9 * level) & 0x1ff)
}

func nodePrefix(id model.ID, level uint8) uint64 {
	if level == mapstore.MaxLevel {
		return 0
	}
	return uint64(id) >> (9 * (level + 1))
}
