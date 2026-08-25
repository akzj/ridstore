package radix

import (
	"errors"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

type NodeStore interface {
	Read(model.MapAddr) (mapstore.Node, error)
	Append(level uint8, prefix uint64, covered model.CommitSeq, slots [mapstore.NodeSlots]uint64) (model.MapAddr, error)
}

type Tree struct {
	store   NodeStore
	root    model.MapAddr
	covered model.CommitSeq
	cache   *nodeCache
}

func Open(store NodeStore, root model.MapAddr, covered model.CommitSeq, cacheBytes uint64) (*Tree, error) {
	if store == nil || cacheBytes == 0 || (root != 0 && !root.Valid()) {
		return nil, ErrInvalid
	}
	tree := &Tree{store: store, root: root, covered: covered, cache: newNodeCache(cacheBytes)}
	if root != 0 {
		if _, err := tree.load(root, mapstore.MaxLevel, 0, true); err != nil {
			return nil, err
		}
	}
	return tree, nil
}

func (t *Tree) Root() model.MapAddr      { return t.root }
func (t *Tree) Covered() model.CommitSeq { return t.covered }
func (t *Tree) CacheBytes() uint64       { return t.cache.bytes() }

func (t *Tree) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	if id == 0 {
		return 0, false, ErrInvalid
	}
	addr := t.root
	if addr == 0 {
		return 0, false, nil
	}
	for level := mapstore.MaxLevel; ; level-- {
		prefix := nodePrefix(id, level)
		node, err := t.load(addr, level, prefix, level == mapstore.MaxLevel)
		if err != nil {
			return 0, false, err
		}
		value, exists := node.Lookup(nodeSlot(id, level))
		if !exists {
			return 0, false, nil
		}
		if level == 0 {
			result, err := recordlog.ParseVAddr(value)
			if err != nil {
				return 0, false, errors.Join(ErrCorrupt, err)
			}
			return result, true, nil
		}
		childAddr, err := model.ParseMapAddr(value)
		if err != nil {
			return 0, false, errors.Join(ErrCorrupt, err)
		}
		addr = childAddr
	}
}

func (t *Tree) load(addr model.MapAddr, level uint8, prefix uint64, pin bool) (mapstore.Node, error) {
	node, err := t.cache.get(addr, pin, func() (mapstore.Node, error) { return t.store.Read(addr) })
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
