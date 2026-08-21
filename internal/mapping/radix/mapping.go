package radix

import (
	"fmt"
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
	mu sync.RWMutex

	store       *nodeStore
	cache       *nodeCache
	root        base.MapAddr
	rootCovered base.CommitSeq
	runtimeSeq  base.CommitSeq
	active      map[base.ID]deltaEntry
	frozen      []map[base.ID]deltaEntry // oldest to newest
	checkpoint  bool
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

var _ api.Mapping = (*Mapping)(nil)

func Open(root string, manifest storeformat.Manifest, cacheBytes int64, catalogs ...*catalog.Manager) (*Mapping, error) {
	if cacheBytes <= 0 {
		return nil, base.ErrInvalidConfig
	}
	var catalogManager *catalog.Manager
	if len(catalogs) != 0 {
		catalogManager = catalogs[0]
	}
	store, err := openNodeStore(root, manifest, catalogManager)
	if err != nil {
		return nil, err
	}
	mapping := &Mapping{
		store: store, cache: newNodeCache(cacheBytes), root: manifest.MappingRoot,
		rootCovered: manifest.CoveredCommitSeq, runtimeSeq: manifest.CoveredCommitSeq,
		active: make(map[base.ID]deltaEntry),
	}
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
	m.mu.RUnlock()
	return m.lookupRoot(root, covered, id)
}

func (m *Mapping) Apply(seq base.CommitSeq, kind api.ApplyKind, changes []api.Change) (api.ApplyResult, error) {
	if seq == 0 || (kind != api.ApplyUserCommit && kind != api.ApplyRelocation) {
		return api.ApplyResult{}, base.ErrInvalidConfig
	}
	if err := validateChanges(kind, changes); err != nil {
		return api.ApplyResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq <= m.runtimeSeq {
		return api.ApplyResult{}, fmt.Errorf("mapping sequence regression: %w", base.ErrInvalidConfig)
	}
	result := api.ApplyResult{}
	for _, change := range changes {
		if kind == api.ApplyRelocation {
			current, exists, err := m.lookupLocked(change.RecordID)
			if err != nil {
				return api.ApplyResult{}, err
			}
			if !exists || current != change.ExpectedOldAddr {
				result.Skipped++
				continue
			}
		}
		m.active[change.RecordID] = deltaEntry{addr: change.NewAddr, seq: seq}
		result.Applied++
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
	m.mu.RLock()
	root, covered, runtime := m.root, m.rootCovered, m.runtimeSeq
	layers := append([]map[base.ID]deltaEntry(nil), m.frozen...)
	active := cloneDelta(m.active)
	m.mu.RUnlock()
	entries, err := m.materializeRoot(root, covered)
	if err != nil {
		return api.Snapshot{CoveredCommitSeq: runtime}
	}
	for _, layer := range append(layers, active) {
		applyLayer(entries, layer)
	}
	return api.Snapshot{CoveredCommitSeq: runtime, Entries: entries}
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

func (m *Mapping) BuildCheckpoint(checkpoint *Checkpoint) (base.MapAddr, map[base.ID]base.VAddr, error) {
	if checkpoint == nil {
		return 0, nil, base.ErrInvalidConfig
	}
	entries, err := m.materializeRoot(checkpoint.root, checkpoint.rootCovered)
	if err != nil {
		return 0, nil, err
	}
	for _, layer := range checkpoint.layers {
		applyLayer(entries, layer)
	}
	root, err := m.buildTree(entries, checkpoint.covered)
	if err != nil {
		return 0, nil, err
	}
	if err := m.store.sync(); err != nil {
		return 0, nil, err
	}
	return root, entries, nil
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

func (m *Mapping) lookupLocked(id base.ID) (base.VAddr, bool, error) {
	if entry, ok := m.active[id]; ok {
		return entry.addr, entry.addr != 0, nil
	}
	for i := len(m.frozen) - 1; i >= 0; i-- {
		if entry, ok := m.frozen[i][id]; ok {
			return entry.addr, entry.addr != 0, nil
		}
	}
	return m.lookupRoot(m.root, m.rootCovered, id)
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
	if node.Level != level || node.Prefix != prefix || node.CoveredCommitSeq != covered {
		return storeformat.MappingNode{}, fmt.Errorf("mapping node path identity: %w", base.ErrCorrupt)
	}
	return node, nil
}

func (m *Mapping) materializeRoot(root base.MapAddr, covered base.CommitSeq) (map[base.ID]base.VAddr, error) {
	entries := make(map[base.ID]base.VAddr)
	if root == 0 {
		return entries, nil
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
				entries[base.ID((prefix<<9)|uint64(slot))] = base.VAddr(value)
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
	return entries, walk(root, 7, 0)
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
