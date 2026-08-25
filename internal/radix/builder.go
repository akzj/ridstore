package radix

import (
	"sort"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

type Mutation struct {
	ID   model.ID
	Addr recordlog.VAddr // zero deletes the ID
}

type childChange struct {
	prefix uint64
	addr   model.MapAddr
}

// Build applies one checkpoint cut. Every affected logical node is rewritten
// at most once; unchanged subtrees remain referenced by their old MapAddr.
func (t *Tree) Build(covered model.CommitSeq, mutations []Mutation) (*Tree, error) {
	if covered < t.covered || (len(mutations) != 0 && covered <= t.covered) {
		return nil, ErrInvalid
	}
	if len(mutations) == 0 {
		return Open(t.store, t.root, covered, t.cache.capacity)
	}
	ordered := append([]Mutation(nil), mutations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	for index, mutation := range ordered {
		if mutation.ID == 0 || (mutation.Addr != 0 && !mutation.Addr.Valid()) || (index != 0 && mutation.ID == ordered[index-1].ID) {
			return nil, ErrInvalid
		}
	}

	changes := make([]childChange, 0)
	for start := 0; start < len(ordered); {
		prefix := nodePrefix(ordered[start].ID, 0)
		end := start + 1
		for end < len(ordered) && nodePrefix(ordered[end].ID, 0) == prefix {
			end++
		}
		oldAddr, err := t.nodeAddress(0, prefix)
		if err != nil {
			return nil, err
		}
		slots, err := t.oldSlots(oldAddr, 0, prefix)
		if err != nil {
			return nil, err
		}
		before := slots
		for _, mutation := range ordered[start:end] {
			slots[nodeSlot(mutation.ID, 0)] = uint64(mutation.Addr)
		}
		newAddr, err := t.writeChangedNode(0, prefix, covered, before, slots, oldAddr)
		if err != nil {
			return nil, err
		}
		if newAddr != oldAddr {
			changes = append(changes, childChange{prefix: prefix, addr: newAddr})
		}
		start = end
	}

	for level := uint8(1); level <= mapstore.MaxLevel && len(changes) != 0; level++ {
		next := make([]childChange, 0)
		for start := 0; start < len(changes); {
			parentPrefix := changes[start].prefix >> 9
			end := start + 1
			for end < len(changes) && changes[end].prefix>>9 == parentPrefix {
				end++
			}
			oldAddr, err := t.nodeAddress(level, parentPrefix)
			if err != nil {
				return nil, err
			}
			slots, err := t.oldSlots(oldAddr, level, parentPrefix)
			if err != nil {
				return nil, err
			}
			before := slots
			for _, change := range changes[start:end] {
				slots[uint16(change.prefix&0x1ff)] = uint64(change.addr)
			}
			newAddr, err := t.writeChangedNode(level, parentPrefix, covered, before, slots, oldAddr)
			if err != nil {
				return nil, err
			}
			if newAddr != oldAddr {
				next = append(next, childChange{prefix: parentPrefix, addr: newAddr})
			}
			start = end
		}
		changes = next
	}
	newRoot := t.root
	if len(changes) != 0 {
		if len(changes) != 1 || changes[0].prefix != 0 {
			return nil, ErrCorrupt
		}
		newRoot = changes[0].addr
	}
	return Open(t.store, newRoot, covered, t.cache.capacity)
}

func (t *Tree) writeChangedNode(level uint8, prefix uint64, covered model.CommitSeq, before, after [mapstore.NodeSlots]uint64, old model.MapAddr) (model.MapAddr, error) {
	if before == after {
		return old, nil
	}
	allZero := true
	for _, value := range after {
		if value != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return 0, nil
	}
	return t.store.Append(level, prefix, covered, after)
}

func (t *Tree) oldSlots(addr model.MapAddr, level uint8, prefix uint64) ([mapstore.NodeSlots]uint64, error) {
	if addr == 0 {
		return [mapstore.NodeSlots]uint64{}, nil
	}
	node, err := t.load(addr, level, prefix, level == mapstore.MaxLevel)
	if err != nil {
		return [mapstore.NodeSlots]uint64{}, err
	}
	return node.Slots(), nil
}

func (t *Tree) nodeAddress(level uint8, prefix uint64) (model.MapAddr, error) {
	if t.root == 0 {
		return 0, nil
	}
	if level == mapstore.MaxLevel {
		return t.root, nil
	}
	id := model.ID(prefix << (9 * (level + 1)))
	addr := t.root
	for current := mapstore.MaxLevel; current > level; current-- {
		node, err := t.load(addr, current, nodePrefix(id, current), current == mapstore.MaxLevel)
		if err != nil {
			return 0, err
		}
		value, exists := node.Lookup(nodeSlot(id, current))
		if !exists {
			return 0, nil
		}
		addr, err = model.ParseMapAddr(value)
		if err != nil {
			return 0, ErrCorrupt
		}
	}
	return addr, nil
}
