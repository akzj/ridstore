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
	ordered := append([]Mutation(nil), mutations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	return t.BuildSorted(covered, ordered)
}

// BuildSorted applies one checkpoint cut from strictly increasing mutations.
// It does not retain or copy the input. Parent changes are folded through a
// fixed-depth pipeline, so auxiliary memory does not grow with mutation count.
func (t *Tree) BuildSorted(covered model.CommitSeq, ordered []Mutation) (*Tree, error) {
	if covered < t.covered || (len(ordered) != 0 && covered <= t.covered) {
		return nil, ErrInvalid
	}
	for index, mutation := range ordered {
		if mutation.ID == 0 || (mutation.Addr != 0 && !mutation.Addr.Valid()) || (index != 0 && mutation.ID <= ordered[index-1].ID) {
			return nil, ErrInvalid
		}
	}
	if len(ordered) == 0 {
		return Open(t.store, t.root, covered, t.cache.capacity)
	}

	builder := streamingBuilder{tree: t, covered: covered, root: t.root}
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
			if err := builder.push(0, childChange{prefix: prefix, addr: newAddr}); err != nil {
				return nil, err
			}
		}
		start = end
	}
	for level := uint8(1); level <= mapstore.MaxLevel; level++ {
		if err := builder.flush(level); err != nil {
			return nil, err
		}
	}
	return Open(t.store, builder.root, covered, t.cache.capacity)
}

type nodeAccumulator struct {
	active  bool
	prefix  uint64
	oldAddr model.MapAddr
	before  [mapstore.NodeSlots]uint64
	slots   [mapstore.NodeSlots]uint64
}

type streamingBuilder struct {
	tree    *Tree
	covered model.CommitSeq
	root    model.MapAddr
	levels  [mapstore.MaxLevel + 1]nodeAccumulator
}

func (b *streamingBuilder) push(childLevel uint8, change childChange) error {
	level := childLevel + 1
	if level > mapstore.MaxLevel {
		return ErrCorrupt
	}
	prefix := change.prefix >> 9
	current := &b.levels[level]
	if current.active && current.prefix != prefix {
		if err := b.flush(level); err != nil {
			return err
		}
	}
	if !current.active {
		oldAddr, err := b.tree.nodeAddress(level, prefix)
		if err != nil {
			return err
		}
		slots, err := b.tree.oldSlots(oldAddr, level, prefix)
		if err != nil {
			return err
		}
		*current = nodeAccumulator{active: true, prefix: prefix, oldAddr: oldAddr, before: slots, slots: slots}
	}
	current.slots[uint16(change.prefix&0x1ff)] = uint64(change.addr)
	return nil
}

func (b *streamingBuilder) flush(level uint8) error {
	current := &b.levels[level]
	if !current.active {
		return nil
	}
	prefix, oldAddr := current.prefix, current.oldAddr
	before, slots := current.before, current.slots
	*current = nodeAccumulator{}
	newAddr, err := b.tree.writeChangedNode(level, prefix, b.covered, before, slots, oldAddr)
	if err != nil {
		return err
	}
	if newAddr == oldAddr {
		return nil
	}
	if level == mapstore.MaxLevel {
		if prefix != 0 {
			return ErrCorrupt
		}
		b.root = newAddr
		return nil
	}
	return b.push(level, childChange{prefix: prefix, addr: newAddr})
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
