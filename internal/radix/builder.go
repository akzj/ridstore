package radix

import (
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

type Mutation struct {
	ID  model.ID
	Ref recordlog.RecordRef // zero deletes the ID
}

// EntryDelta describes the logical cardinality change produced by a build.
// Replacements and relocations do not change either counter.
type EntryDelta struct {
	Added, Removed                             uint64
	ReachableBytesAdded, ReachableBytesRemoved uint64
}

func (d *EntryDelta) addReachable(added, removed uint64) error {
	if d.ReachableBytesAdded > math.MaxUint64-added || d.ReachableBytesRemoved > math.MaxUint64-removed {
		return ErrInvalid
	}
	d.ReachableBytesAdded += added
	d.ReachableBytesRemoved += removed
	return nil
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
	tree, _, err := t.BuildSortedWithEntryDelta(covered, ordered)
	return tree, err
}

// BuildSortedWithEntryDelta applies one checkpoint cut and reports its exact
// logical cardinality change. Counting is folded into the leaf update that
// already reads the old slots, so it performs no additional node reads.
func (t *Tree) BuildSortedWithEntryDelta(covered model.CommitSeq, ordered []Mutation) (*Tree, EntryDelta, error) {
	if t.writer == nil || covered < t.covered || (len(ordered) != 0 && covered <= t.covered) {
		return nil, EntryDelta{}, ErrInvalid
	}
	for index, mutation := range ordered {
		if mutation.ID == 0 || (!mutation.Ref.IsZero() && !mutation.Ref.Valid()) || (index != 0 && mutation.ID <= ordered[index-1].ID) {
			return nil, EntryDelta{}, ErrInvalid
		}
	}
	if len(ordered) == 0 {
		tree, err := Open(t.writer, t.root, covered, t.cache.capacity)
		return tree, EntryDelta{}, err
	}

	var delta EntryDelta
	builder := streamingBuilder{tree: t, covered: covered, root: t.root, delta: &delta}
	for start := 0; start < len(ordered); {
		prefix := nodePrefix(ordered[start].ID, 0)
		end := start + 1
		for end < len(ordered) && nodePrefix(ordered[end].ID, 0) == prefix {
			end++
		}
		oldAddr, err := t.nodeAddress(0, prefix)
		if err != nil {
			return nil, EntryDelta{}, err
		}
		refs, oldBytes, err := t.oldRefs(oldAddr, prefix)
		if err != nil {
			return nil, EntryDelta{}, err
		}
		before := refs
		for _, mutation := range ordered[start:end] {
			slot := nodeSlot(mutation.ID, 0)
			oldExists, newExists := !refs[slot].IsZero(), !mutation.Ref.IsZero()
			if !oldExists && newExists {
				delta.Added++
			} else if oldExists && !newExists {
				delta.Removed++
			}
			refs[slot] = mutation.Ref
		}
		newAddr, err := t.writeChangedLeaf(prefix, covered, before, refs, oldAddr)
		if err != nil {
			return nil, EntryDelta{}, err
		}
		if newAddr != oldAddr {
			added, sizeErr := nodeBuildBytes(0, refs)
			if sizeErr != nil {
				return nil, EntryDelta{}, sizeErr
			}
			removed := oldBytes
			if sizeErr := delta.addReachable(added, removed); sizeErr != nil {
				return nil, EntryDelta{}, sizeErr
			}
			if err := builder.push(0, childChange{prefix: prefix, addr: newAddr}); err != nil {
				return nil, EntryDelta{}, err
			}
		}
		start = end
	}
	for level := uint8(1); level <= mapstore.MaxLevel; level++ {
		if err := builder.flush(level); err != nil {
			return nil, EntryDelta{}, err
		}
	}
	tree, err := Open(t.writer, builder.root, covered, t.cache.capacity)
	return tree, delta, err
}

func (t *Tree) writeChangedLeaf(prefix uint64, covered model.CommitSeq, before, after [mapstore.NodeSlots]recordlog.RecordRef, old model.MapAddr) (model.MapAddr, error) {
	if before == after {
		return old, nil
	}
	allZero := true
	for _, ref := range after {
		if !ref.IsZero() {
			allZero = false
			break
		}
	}
	if allZero {
		return 0, nil
	}
	if t.writer == nil {
		return 0, ErrInvalid
	}
	return t.writer.AppendLeaf(prefix, covered, after)
}

func (t *Tree) oldRefs(addr model.MapAddr, prefix uint64) ([mapstore.NodeSlots]recordlog.RecordRef, uint64, error) {
	if addr == 0 {
		return [mapstore.NodeSlots]recordlog.RecordRef{}, 0, nil
	}
	node, err := t.load(addr, 0, prefix, false, false)
	if err != nil {
		return [mapstore.NodeSlots]recordlog.RecordRef{}, 0, err
	}
	return node.Refs(), uint64(node.PhysicalSize()), nil
}

type nodeAccumulator struct {
	active   bool
	prefix   uint64
	oldAddr  model.MapAddr
	oldBytes uint64
	before   [mapstore.NodeSlots]uint64
	slots    [mapstore.NodeSlots]uint64
}

type streamingBuilder struct {
	tree    *Tree
	covered model.CommitSeq
	root    model.MapAddr
	delta   *EntryDelta
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
		slots, oldBytes, err := b.tree.oldSlots(oldAddr, level, prefix)
		if err != nil {
			return err
		}
		*current = nodeAccumulator{active: true, prefix: prefix, oldAddr: oldAddr, oldBytes: oldBytes, before: slots, slots: slots}
	}
	current.slots[uint16(change.prefix&0x1ff)] = uint64(change.addr)
	return nil
}

func (b *streamingBuilder) flush(level uint8) error {
	current := &b.levels[level]
	if !current.active {
		return nil
	}
	prefix, oldAddr, oldBytes := current.prefix, current.oldAddr, current.oldBytes
	before, slots := current.before, current.slots
	*current = nodeAccumulator{}
	newAddr, err := b.tree.writeChangedNode(level, prefix, b.covered, before, slots, oldAddr)
	if err != nil {
		return err
	}
	if newAddr == oldAddr {
		return nil
	}
	if b.delta != nil {
		added, err := nodeBuildBytes(level, slots)
		if err != nil {
			return err
		}
		if err := b.delta.addReachable(added, oldBytes); err != nil {
			return err
		}
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

func nodeBuildBytes[T comparable](level uint8, values [mapstore.NodeSlots]T) (uint64, error) {
	var zero T
	count := uint16(0)
	for _, value := range values {
		if value != zero {
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	size, err := mapstore.EncodedNodeSize(level, count)
	return uint64(size), err
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
	if t.writer == nil {
		return 0, ErrInvalid
	}
	return t.writer.Append(level, prefix, covered, after)
}

func (t *Tree) oldSlots(addr model.MapAddr, level uint8, prefix uint64) ([mapstore.NodeSlots]uint64, uint64, error) {
	if addr == 0 {
		return [mapstore.NodeSlots]uint64{}, 0, nil
	}
	node, err := t.load(addr, level, prefix, level == mapstore.MaxLevel, false)
	if err != nil {
		return [mapstore.NodeSlots]uint64{}, 0, err
	}
	return node.Slots(), uint64(node.PhysicalSize()), nil
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
		node, err := t.load(addr, current, nodePrefix(id, current), current == mapstore.MaxLevel, false)
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
