package radix

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/catalog"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/api"
)

type deltaEntry struct {
	addr base.VAddr
	seq  base.CommitSeq
}

type Mapping struct {
	mu         sync.RWMutex
	budget     *deltaBudget
	readerMu   sync.Mutex
	readers    map[base.MapAddr]uint64
	readerCond *sync.Cond

	store             *nodeStore
	cache             *nodeCache
	root              base.MapAddr
	rootCovered       base.CommitSeq
	runtimeSeq        base.CommitSeq
	active            map[base.ID]deltaEntry
	frozen            []map[base.ID]deltaEntry // oldest to newest
	checkpoint        bool
	checkpointEntries int
}

type Checkpoint struct {
	root        base.MapAddr
	rootCovered base.CommitSeq
	covered     base.CommitSeq
	layers      []map[base.ID]deltaEntry
}

func (c *Checkpoint) CoveredCommitSeq() base.CommitSeq {
	if c == nil {
		return 0
	}
	return c.covered
}

func (c *Checkpoint) EntryCount() uint64 {
	if c == nil {
		return 0
	}
	var count uint64
	for _, layer := range c.layers {
		count += uint64(len(layer))
	}
	return count
}

var _ api.Mapping = (*Mapping)(nil)

func (m *Mapping) CacheBytes() uint64 {
	if m == nil {
		return 0
	}
	return m.cache.usedBytes()
}

func Open(root string, manifest storeformat.Manifest, cacheBytes int64, catalogs ...*catalog.Manager) (*Mapping, error) {
	return OpenWithHook(root, manifest, cacheBytes, nil, catalogs...)
}

func OpenWithHook(root string, manifest storeformat.Manifest, cacheBytes int64, hook failpoint.Hook, catalogs ...*catalog.Manager) (*Mapping, error) {
	if cacheBytes <= 0 {
		return nil, base.ErrInvalidConfig
	}
	var catalogManager *catalog.Manager
	if len(catalogs) != 0 {
		catalogManager = catalogs[0]
	}
	store, err := openNodeStoreWithHook(root, manifest, catalogManager, hook)
	if err != nil {
		return nil, err
	}
	return newMappingFromStore(store, manifest, cacheBytes)
}

// OpenReadOnly validates Mapping files without truncating an invalid active
// tail or installing recovery state. It is intended for offline verification.
func OpenReadOnly(root string, manifest storeformat.Manifest, cacheBytes int64) (*Mapping, error) {
	if cacheBytes <= 0 {
		return nil, base.ErrInvalidConfig
	}
	store, err := openNodeStoreReadOnly(root, manifest)
	if err != nil {
		return nil, err
	}
	return newMappingFromStore(store, manifest, cacheBytes)
}

func newMappingFromStore(store *nodeStore, manifest storeformat.Manifest, cacheBytes int64) (*Mapping, error) {
	mapping := &Mapping{
		store: store, cache: newNodeCache(cacheBytes), root: manifest.MappingRoot,
		rootCovered: manifest.CoveredCommitSeq, runtimeSeq: manifest.CoveredCommitSeq,
		active: make(map[base.ID]deltaEntry), budget: newDeltaBudget(), checkpointEntries: math.MaxInt,
		readers: make(map[base.MapAddr]uint64),
	}
	mapping.readerCond = sync.NewCond(&mapping.readerMu)
	if mapping.root != 0 {
		if _, err := mapping.loadNode(mapping.root, 7, 0, mapping.rootCovered); err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	return mapping, nil
}

func (m *Mapping) Lookup(id base.ID) (base.VAddr, bool, error) {
	if id == 0 {
		return 0, false, base.ErrInvalidID
	}
	m.mu.RLock()
	if entry, ok := m.active[id]; ok {
		m.mu.RUnlock()
		return entry.addr, entry.addr != 0, nil
	}
	for i := len(m.frozen) - 1; i >= 0; i-- {
		if entry, ok := m.frozen[i][id]; ok {
			m.mu.RUnlock()
			return entry.addr, entry.addr != 0, nil
		}
	}
	root, covered := m.root, m.rootCovered
	if root != 0 {
		m.readerMu.Lock()
		m.readers[root]++
		m.readerMu.Unlock()
	}
	m.mu.RUnlock()
	if root != 0 {
		defer m.releaseRoot(root)
	}
	return m.lookupRoot(root, covered, id)
}

func (m *Mapping) Apply(seq base.CommitSeq, kind api.ApplyKind, changes []api.Change) (api.ApplyResult, error) {
	if seq == 0 {
		return api.ApplyResult{}, base.ErrInvalidConfig
	}
	plan, err := m.Resolve(kind, changes)
	if err != nil {
		return api.ApplyResult{}, err
	}
	return m.applyResolved(nil, seq, plan)
}

func (m *Mapping) ApplyReserved(reservation api.DeltaReservation, seq base.CommitSeq, kind api.ApplyKind, changes []api.Change) (api.ApplyResult, error) {
	reserved, ok := reservation.(*deltaReservation)
	if !ok || reserved.budget != m.budget {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, base.ErrInvalidConfig
	}
	if seq == 0 {
		reservation.Release()
		return api.ApplyResult{}, base.ErrInvalidConfig
	}
	plan, err := m.Resolve(kind, changes)
	if err != nil {
		reservation.Release()
		return api.ApplyResult{}, err
	}
	return m.applyResolved(reserved, seq, plan)
}

func (m *Mapping) Resolve(kind api.ApplyKind, changes []api.Change) (api.ResolvedPlan, error) {
	if err := validateChanges(kind, changes); err != nil {
		return api.ResolvedPlan{}, err
	}
	plan := api.ResolvedPlan{Kind: kind, Changes: make([]api.ResolvedChange, len(changes))}
	m.mu.RLock()
	plan.BaseCommitSeq = m.runtimeSeq
	unresolved := make([]int, 0, len(changes))
	for i, change := range changes {
		if kind == api.ApplyUserCommit {
			plan.Changes[i] = api.ResolvedChange{Change: change, Apply: true}
			continue
		}
		addr, exists, found := m.lookupOverlayLocked(change.RecordID)
		if found {
			plan.Changes[i] = api.ResolvedChange{Change: change, Apply: exists && addr == change.ExpectedOldAddr}
			continue
		}
		plan.Changes[i].Change = change
		unresolved = append(unresolved, i)
	}
	root, covered := m.root, m.rootCovered
	if len(unresolved) != 0 && root != 0 {
		m.readerMu.Lock()
		m.readers[root]++
		m.readerMu.Unlock()
	}
	m.mu.RUnlock()
	if len(unresolved) != 0 && root != 0 {
		defer m.releaseRoot(root)
	}
	for _, index := range unresolved {
		change := changes[index]
		addr, exists, err := m.lookupRoot(root, covered, change.RecordID)
		if err != nil {
			return api.ResolvedPlan{}, err
		}
		plan.Changes[index].Apply = exists && addr == change.ExpectedOldAddr
	}
	return plan, nil
}

func (m *Mapping) ApplyResolved(seq base.CommitSeq, plan api.ResolvedPlan) (api.ApplyResult, error) {
	return m.applyResolved(nil, seq, plan)
}

func (m *Mapping) ApplyResolvedReserved(reservation api.DeltaReservation, seq base.CommitSeq, plan api.ResolvedPlan) (api.ApplyResult, error) {
	reserved, ok := reservation.(*deltaReservation)
	if !ok || reserved.budget != m.budget {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, base.ErrInvalidConfig
	}
	return m.applyResolved(reserved, seq, plan)
}

func (m *Mapping) applyResolved(reservation *deltaReservation, seq base.CommitSeq, plan api.ResolvedPlan) (api.ApplyResult, error) {
	if seq == 0 {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, base.ErrInvalidConfig
	}
	if err := validateResolvedPlan(plan); err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if plan.BaseCommitSeq != m.runtimeSeq {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, fmt.Errorf("stale resolved mapping plan: %w", base.ErrCorrupt)
	}
	if seq <= m.runtimeSeq {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, fmt.Errorf("mapping sequence regression: %w", base.ErrInvalidConfig)
	}
	result := api.ApplyResult{}
	applied := make([]api.Change, 0, len(plan.Changes))
	var newEntries uint64
	for _, resolved := range plan.Changes {
		if !resolved.Apply {
			result.Skipped++
			continue
		}
		change := resolved.Change
		if _, exists := m.active[change.RecordID]; !exists {
			newEntries++
		}
		applied = append(applied, change)
		result.Applied++
	}
	charge, err := base.MulUint64(newEntries, deltaEntryCharge)
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return api.ApplyResult{}, err
	}
	if reservation != nil {
		if err := reservation.consume(charge); err != nil {
			return api.ApplyResult{}, err
		}
	} else if err := m.budget.addReplay(charge); err != nil {
		return api.ApplyResult{}, err
	}
	for _, change := range applied {
		m.active[change.RecordID] = deltaEntry{addr: change.NewAddr, seq: seq}
	}
	m.runtimeSeq = seq
	return result, nil
}

func (m *Mapping) CoveredCommitSeq() base.CommitSeq {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtimeSeq
}

func (m *Mapping) Snapshot() api.Snapshot {
	snapshot, err := m.Materialize()
	if err != nil {
		return api.Snapshot{CoveredCommitSeq: m.CoveredCommitSeq()}
	}
	return snapshot
}

// Materialize returns the current Root plus replay/runtime overlays and reports
// any cold Root read failure instead of collapsing it into an empty Snapshot.
func (m *Mapping) Materialize() (api.Snapshot, error) {
	m.mu.RLock()
	root, covered, runtime := m.root, m.rootCovered, m.runtimeSeq
	if root != 0 {
		m.readerMu.Lock()
		m.readers[root]++
		m.readerMu.Unlock()
	}
	layers := append([]map[base.ID]deltaEntry(nil), m.frozen...)
	active := cloneDelta(m.active)
	m.mu.RUnlock()
	if root != 0 {
		defer m.releaseRoot(root)
	}
	entries, err := m.materializeRoot(root, covered)
	if err != nil {
		return api.Snapshot{CoveredCommitSeq: runtime}, err
	}
	for _, layer := range append(layers, active) {
		applyLayer(entries, layer)
	}
	return api.Snapshot{CoveredCommitSeq: runtime, Entries: entries}, nil
}

func (m *Mapping) releaseRoot(root base.MapAddr) {
	m.readerMu.Lock()
	if count := m.readers[root]; count <= 1 {
		delete(m.readers, root)
		m.readerCond.Broadcast()
	} else {
		m.readers[root] = count - 1
	}
	m.readerMu.Unlock()
}

func (m *Mapping) waitRootReaders(root base.MapAddr) {
	if root == 0 {
		return
	}
	m.readerMu.Lock()
	for m.readers[root] != 0 {
		m.readerCond.Wait()
	}
	m.readerMu.Unlock()
}

func (m *Mapping) SpaceUsage(ctx context.Context) (total, reachable uint64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	m.mu.RLock()
	root, covered := m.root, m.rootCovered
	if root != 0 {
		m.readerMu.Lock()
		m.readers[root]++
		m.readerMu.Unlock()
	}
	m.mu.RUnlock()
	if root != 0 {
		defer m.releaseRoot(root)
	}
	total, err = m.store.totalNodeBytes()
	if err != nil {
		return 0, 0, err
	}
	if root == 0 {
		return total, 0, nil
	}
	reachable, err = m.reachableNodeBytes(ctx, root, 7, 0, covered)
	return total, reachable, err
}

func (m *Mapping) reachableNodeBytes(ctx context.Context, addr base.MapAddr, level uint8, prefix uint64, covered base.CommitSeq) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	node, err := m.loadNode(addr, level, prefix, covered)
	if err != nil {
		return 0, err
	}
	bytes := uint64(storeformat.MappingNodeHeaderSize + storeformat.MappingNodeSlots*8)
	if node.Encoding == storeformat.NodeEncodingSparseBitmap {
		bytes = uint64(storeformat.MappingNodeHeaderSize + 64 + int(node.EntryCount)*8)
	}
	if level == 0 {
		return bytes, nil
	}
	for slot := uint16(0); slot < storeformat.MappingNodeSlots; slot++ {
		value, ok := node.Lookup(slot)
		if !ok {
			continue
		}
		childPrefix := uint64(slot)
		if level != 7 {
			childPrefix = (prefix << 9) | uint64(slot)
		}
		childBytes, err := m.reachableNodeBytes(ctx, base.MapAddr(value), level-1, childPrefix, covered)
		if err != nil {
			return 0, err
		}
		bytes, err = base.AddUint64(bytes, childBytes)
		if err != nil {
			return 0, err
		}
	}
	return bytes, nil
}

func (m *Mapping) installCompactedRoot(oldRoot base.MapAddr, covered base.CommitSeq, newRoot base.MapAddr, manifest storeformat.Manifest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.checkpoint || m.root != oldRoot || m.rootCovered != covered || manifest.MappingRoot != newRoot || manifest.CoveredCommitSeq != covered {
		return base.ErrCorrupt
	}
	if err := m.store.adoptCompacted(manifest); err != nil {
		return err
	}
	m.root = newRoot
	return nil
}

func (m *Mapping) BeginCheckpoint() (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.checkpoint {
		return nil, base.ErrConflict
	}
	m.checkpoint = true
	if len(m.active) != 0 {
		m.frozen = append(m.frozen, m.active)
		m.active = make(map[base.ID]deltaEntry)
	}
	layers := append([]map[base.ID]deltaEntry(nil), m.frozen...)
	return &Checkpoint{root: m.root, rootCovered: m.rootCovered, covered: m.runtimeSeq, layers: layers}, nil
}

func (m *Mapping) SetCheckpointMemory(bytes int64) error {
	const estimatedEntryWorkingBytes = int64(2048)
	if bytes < 64<<10 {
		return base.ErrInvalidConfig
	}
	entries := bytes / estimatedEntryWorkingBytes
	if entries > int64(math.MaxInt) {
		entries = int64(math.MaxInt)
	}
	m.mu.Lock()
	m.checkpointEntries = int(entries)
	m.mu.Unlock()
	return nil
}

func (m *Mapping) BuildCheckpoint(checkpoint *Checkpoint) (base.MapAddr, error) {
	if checkpoint == nil {
		return 0, base.ErrInvalidConfig
	}
	m.mu.RLock()
	chunkEntries := m.checkpointEntries
	m.mu.RUnlock()
	root, rootCovered := checkpoint.root, checkpoint.rootCovered
	for _, layer := range checkpoint.layers {
		dirty := make(map[base.ID]deltaEntry, min(len(layer), chunkEntries))
		for id, entry := range layer {
			dirty[id] = entry
			if len(dirty) == chunkEntries {
				var err error
				root, err = m.buildCOW(root, rootCovered, checkpoint.covered, dirty)
				if err != nil {
					return 0, err
				}
				rootCovered = checkpoint.covered
				dirty = make(map[base.ID]deltaEntry, min(len(layer), chunkEntries))
			}
		}
		if len(dirty) != 0 {
			var err error
			root, err = m.buildCOW(root, rootCovered, checkpoint.covered, dirty)
			if err != nil {
				return 0, err
			}
			rootCovered = checkpoint.covered
		}
	}
	if err := m.store.sync(); err != nil {
		return 0, err
	}
	return root, nil
}

func (m *Mapping) CompleteCheckpoint(checkpoint *Checkpoint, root base.MapAddr) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.checkpoint || checkpoint == nil || len(checkpoint.layers) > len(m.frozen) {
		return base.ErrCorrupt
	}
	for i := range checkpoint.layers {
		if len(checkpoint.layers[i]) != len(m.frozen[i]) {
			return base.ErrCorrupt
		}
	}
	var released uint64
	for _, layer := range checkpoint.layers {
		bytes, err := base.MulUint64(uint64(len(layer)), deltaEntryCharge)
		if err != nil {
			return err
		}
		released, err = base.AddUint64(released, bytes)
		if err != nil {
			return err
		}
	}
	if err := m.budget.releaseCharged(released); err != nil {
		return err
	}
	m.root, m.rootCovered = root, checkpoint.covered
	m.frozen = m.frozen[len(checkpoint.layers):]
	m.checkpoint = false
	return nil
}

func (m *Mapping) AbortCheckpoint() {
	m.mu.Lock()
	m.checkpoint = false
	m.mu.Unlock()
}

func (m *Mapping) DeltaEntries() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := len(m.active)
	for _, layer := range m.frozen {
		count += len(layer)
	}
	return count
}

func (m *Mapping) Close() error { return m.store.Close() }

func (m *Mapping) SetHook(hook failpoint.Hook) { m.store.setHook(hook) }

func (m *Mapping) lookupOverlayLocked(id base.ID) (base.VAddr, bool, bool) {
	if entry, ok := m.active[id]; ok {
		return entry.addr, entry.addr != 0, true
	}
	for i := len(m.frozen) - 1; i >= 0; i-- {
		if entry, ok := m.frozen[i][id]; ok {
			return entry.addr, entry.addr != 0, true
		}
	}
	return 0, false, false
}

func (m *Mapping) lookupRoot(root base.MapAddr, covered base.CommitSeq, id base.ID) (base.VAddr, bool, error) {
	if root == 0 {
		return 0, false, nil
	}
	addr := root
	for level := uint8(7); ; level-- {
		prefix := uint64(0)
		if level != 7 {
			prefix = uint64(id) >> (9 * uint(level+1))
		}
		node, err := m.loadNode(addr, level, prefix, covered)
		if err != nil {
			return 0, false, err
		}
		slot := uint16((uint64(id) >> (9 * uint(level))) & 0x1ff)
		value, ok := node.Lookup(slot)
		if !ok {
			return 0, false, nil
		}
		if level == 0 {
			return base.VAddr(value), true, nil
		}
		addr = base.MapAddr(value)
	}
}

func (m *Mapping) loadNode(addr base.MapAddr, level uint8, prefix uint64, covered base.CommitSeq) (storeformat.MappingNode, error) {
	node, err := m.cache.get(addr, func() (storeformat.MappingNode, int, error) { return m.store.read(addr) })
	if err != nil {
		return storeformat.MappingNode{}, err
	}
	if node.Level != level || node.Prefix != prefix || node.CoveredCommitSeq > covered {
		return storeformat.MappingNode{}, fmt.Errorf("mapping node path identity: %w", base.ErrCorrupt)
	}
	return node, nil
}

func (m *Mapping) buildCOW(root base.MapAddr, rootCovered, covered base.CommitSeq, dirty map[base.ID]deltaEntry) (base.MapAddr, error) {
	if len(dirty) == 0 {
		return root, nil
	}
	changes := make(map[uint64]map[uint16]uint64)
	for id, entry := range dirty {
		prefix := uint64(id) >> 9
		slots := changes[prefix]
		if slots == nil {
			slots = make(map[uint16]uint64)
			changes[prefix] = slots
		}
		slots[uint16(uint64(id)&0x1ff)] = uint64(entry.addr)
	}
	for level := uint8(0); level <= 7; level++ {
		prefixes := make([]uint64, 0, len(changes))
		for prefix := range changes {
			prefixes = append(prefixes, prefix)
		}
		sort.Slice(prefixes, func(i, j int) bool { return prefixes[i] < prefixes[j] })
		parents := make(map[uint64]map[uint16]uint64)
		var result base.MapAddr
		for _, prefix := range prefixes {
			_, oldNode, exists, err := m.existingNode(root, rootCovered, level, prefix)
			if err != nil {
				return 0, err
			}
			var slots [storeformat.MappingNodeSlots]uint64
			if exists {
				for slot := uint16(0); slot < storeformat.MappingNodeSlots; slot++ {
					if value, ok := oldNode.Lookup(slot); ok {
						slots[slot] = value
					}
				}
			}
			for slot, value := range changes[prefix] {
				slots[slot] = value
			}
			var newAddr base.MapAddr
			if slotsOccupied(slots) {
				newAddr, err = m.store.append(storeformat.MappingNodeBuild{
					Level: level, Encoding: storeformat.NodeEncodingAuto, Prefix: prefix,
					CoveredCommitSeq: covered, Slots: slots,
				})
				if err != nil {
					return 0, err
				}
			}
			if level == 7 {
				result = newAddr
				continue
			}
			parentPrefix := prefix >> 9
			if level == 6 {
				parentPrefix = 0
			}
			parentSlots := parents[parentPrefix]
			if parentSlots == nil {
				parentSlots = make(map[uint16]uint64)
				parents[parentPrefix] = parentSlots
			}
			parentSlots[uint16(prefix&0x1ff)] = uint64(newAddr)
		}
		if level == 7 {
			return result, nil
		}
		changes = parents
	}
	return 0, base.ErrCorrupt
}

func (m *Mapping) existingNode(root base.MapAddr, covered base.CommitSeq, targetLevel uint8, targetPrefix uint64) (base.MapAddr, storeformat.MappingNode, bool, error) {
	if root == 0 {
		return 0, storeformat.MappingNode{}, false, nil
	}
	if targetLevel > 7 || targetLevel == 7 && targetPrefix != 0 {
		return 0, storeformat.MappingNode{}, false, base.ErrInvalidConfig
	}
	id := targetPrefix << (9 * uint(targetLevel+1))
	addr := root
	for level := uint8(7); ; level-- {
		prefix := uint64(0)
		if level != 7 {
			prefix = id >> (9 * uint(level+1))
		}
		node, err := m.loadNode(addr, level, prefix, covered)
		if err != nil {
			return 0, storeformat.MappingNode{}, false, err
		}
		if level == targetLevel {
			if prefix != targetPrefix {
				return 0, storeformat.MappingNode{}, false, base.ErrCorrupt
			}
			return addr, node, true, nil
		}
		slot := uint16((id >> (9 * uint(level))) & 0x1ff)
		value, ok := node.Lookup(slot)
		if !ok {
			return 0, storeformat.MappingNode{}, false, nil
		}
		addr = base.MapAddr(value)
	}
}

func slotsOccupied(slots [storeformat.MappingNodeSlots]uint64) bool {
	for _, value := range slots {
		if value != 0 {
			return true
		}
	}
	return false
}

func (m *Mapping) materializeRoot(root base.MapAddr, covered base.CommitSeq) (map[base.ID]base.VAddr, error) {
	entries := make(map[base.ID]base.VAddr)
	err := m.WalkRoot(root, covered, func(id base.ID, addr base.VAddr) error {
		entries[id] = addr
		return nil
	})
	return entries, err
}

func (m *Mapping) WalkRoot(root base.MapAddr, covered base.CommitSeq, visit func(base.ID, base.VAddr) error) error {
	if visit == nil {
		return base.ErrInvalidConfig
	}
	if root == 0 {
		return nil
	}
	var walk func(base.MapAddr, uint8, uint64) error
	walk = func(addr base.MapAddr, level uint8, prefix uint64) error {
		node, err := m.loadNode(addr, level, prefix, covered)
		if err != nil {
			return err
		}
		for slot := uint16(0); slot < storeformat.MappingNodeSlots; slot++ {
			value, ok := node.Lookup(slot)
			if !ok {
				continue
			}
			if level == 0 {
				if err := visit(base.ID((prefix<<9)|uint64(slot)), base.VAddr(value)); err != nil {
					return err
				}
				continue
			}
			childPrefix := uint64(slot)
			if level != 7 {
				childPrefix = (prefix << 9) | uint64(slot)
			}
			if err := walk(base.MapAddr(value), level-1, childPrefix); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root, 7, 0)
}

func (m *Mapping) buildTree(entries map[base.ID]base.VAddr, covered base.CommitSeq) (base.MapAddr, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	ids := make([]base.ID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	levelNodes := make(map[uint64]*[storeformat.MappingNodeSlots]uint64)
	for _, id := range ids {
		prefix := uint64(id) >> 9
		slots := levelNodes[prefix]
		if slots == nil {
			slots = &[storeformat.MappingNodeSlots]uint64{}
			levelNodes[prefix] = slots
		}
		slots[uint64(id)&0x1ff] = uint64(entries[id])
	}
	children, err := m.appendLevel(0, covered, levelNodes)
	if err != nil {
		return 0, err
	}
	for level := uint8(1); level <= 7; level++ {
		parents := make(map[uint64]*[storeformat.MappingNodeSlots]uint64)
		for prefix, addr := range children {
			parentPrefix := prefix >> 9
			if level == 7 {
				parentPrefix = 0
			}
			slots := parents[parentPrefix]
			if slots == nil {
				slots = &[storeformat.MappingNodeSlots]uint64{}
				parents[parentPrefix] = slots
			}
			slots[prefix&0x1ff] = uint64(addr)
		}
		children, err = m.appendLevel(level, covered, parents)
		if err != nil {
			return 0, err
		}
	}
	return children[0], nil
}

func (m *Mapping) appendLevel(level uint8, covered base.CommitSeq, nodes map[uint64]*[storeformat.MappingNodeSlots]uint64) (map[uint64]base.MapAddr, error) {
	prefixes := make([]uint64, 0, len(nodes))
	for prefix := range nodes {
		prefixes = append(prefixes, prefix)
	}
	sort.Slice(prefixes, func(i, j int) bool { return prefixes[i] < prefixes[j] })
	result := make(map[uint64]base.MapAddr, len(nodes))
	for _, prefix := range prefixes {
		build := storeformat.MappingNodeBuild{Level: level, Encoding: storeformat.NodeEncodingAuto, Prefix: prefix, CoveredCommitSeq: covered, Slots: *nodes[prefix]}
		addr, err := m.store.append(build)
		if err != nil {
			return nil, err
		}
		result[prefix] = addr
	}
	return result, nil
}

func cloneDelta(source map[base.ID]deltaEntry) map[base.ID]deltaEntry {
	result := make(map[base.ID]deltaEntry, len(source))
	for id, entry := range source {
		result[id] = entry
	}
	return result
}

func applyLayer(entries map[base.ID]base.VAddr, layer map[base.ID]deltaEntry) {
	for id, entry := range layer {
		if entry.addr == 0 {
			delete(entries, id)
		} else {
			entries[id] = entry.addr
		}
	}
}

func validateChanges(kind api.ApplyKind, changes []api.Change) error {
	if kind != api.ApplyUserCommit && kind != api.ApplyRelocation {
		return base.ErrInvalidConfig
	}
	var previous base.ID
	for _, change := range changes {
		if change.RecordID == 0 || (previous != 0 && change.RecordID <= previous) {
			return base.ErrInvalidConfig
		}
		if kind == api.ApplyUserCommit && change.ExpectedOldAddr != 0 || kind == api.ApplyRelocation && (change.ExpectedOldAddr == 0 || change.NewAddr == 0) {
			return base.ErrInvalidConfig
		}
		previous = change.RecordID
	}
	return nil
}

func validateResolvedPlan(plan api.ResolvedPlan) error {
	changes := make([]api.Change, len(plan.Changes))
	for i, resolved := range plan.Changes {
		if plan.Kind == api.ApplyUserCommit && !resolved.Apply {
			return base.ErrInvalidConfig
		}
		changes[i] = resolved.Change
	}
	return validateChanges(plan.Kind, changes)
}
