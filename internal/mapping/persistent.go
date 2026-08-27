package mapping

import (
	"context"
	"math"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
)

type NodeSyncer interface {
	Sync() error
}

type persistentDelta struct {
	addr   recordlog.VAddr
	exists bool
}

type deltaLayer struct {
	values map[model.ID]persistentDelta
	charge uint64
}

// Persistent is the v2 production Mapping runtime. The complete Mapping is
// never materialized: only committed deltas and a bounded radix cache live in
// memory.
type Persistent struct {
	mu sync.RWMutex

	root    *radix.Tree
	syncer  NodeSyncer
	covered model.CommitSeq
	epoch   uint64
	active  *deltaLayer
	frozen  []*deltaLayer // oldest to newest

	checkpointID        uint64
	inCheckpoint        bool
	checkpointSortBytes uint64
	budget              *deltaBudget
}

type PersistentConfig struct {
	CheckpointSortBytes uint64
	DeltaSoftLimitBytes uint64
	DeltaHardLimitBytes uint64
}

const checkpointMutationBytes = uint64(16)

func ValidatePersistentConfig(config PersistentConfig) error {
	if config.CheckpointSortBytes < checkpointMutationBytes ||
		config.DeltaHardLimitBytes/deltaEntryCharge > config.CheckpointSortBytes/checkpointMutationBytes {
		return ErrInvalid
	}
	if _, err := newDeltaBudget(config.DeltaSoftLimitBytes, config.DeltaHardLimitBytes); err != nil {
		return ErrInvalid
	}
	return nil
}

func OpenPersistent(root *radix.Tree, syncer NodeSyncer, config PersistentConfig) (*Persistent, error) {
	if root == nil || syncer == nil || ValidatePersistentConfig(config) != nil {
		return nil, ErrInvalid
	}
	budget, _ := newDeltaBudget(config.DeltaSoftLimitBytes, config.DeltaHardLimitBytes)
	return &Persistent{
		root: root, syncer: syncer, covered: root.Covered(),
		active:              &deltaLayer{values: make(map[model.ID]persistentDelta)},
		checkpointSortBytes: config.CheckpointSortBytes,
		budget:              budget,
	}, nil
}

func (m *Persistent) ReserveDelta(ids []model.ID) (DeltaReservation, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var entries uint64
	seen := make(map[model.ID]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			return nil, false, ErrInvalid
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		if _, exists := m.active.values[id]; !exists {
			entries++
		}
	}
	return m.budget.reserve(entries)
}

func (m *Persistent) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	if id == 0 {
		return 0, false, ErrInvalid
	}
	m.mu.RLock()
	if value, found := lookupLayers(m.active, m.frozen, id); found {
		m.mu.RUnlock()
		return value.addr, value.exists, nil
	}
	root := m.root
	m.mu.RUnlock()
	addr, exists, err := root.Lookup(id)
	return addr, exists, err
}

func (m *Persistent) ResolveGroup(proposals []Proposal) (GroupPlan, error) {
	if len(proposals) == 0 || uint64(len(proposals)) > math.MaxUint32 {
		return GroupPlan{}, ErrInvalid
	}
	for _, proposal := range proposals {
		if err := validateProposal(proposal); err != nil {
			return GroupPlan{}, err
		}
	}
	ids := proposalReadSet(proposals)
	for {
		m.mu.RLock()
		epoch, base, root := m.epoch, m.covered, m.root
		active := make(map[model.ID]struct{})
		for _, proposal := range proposals {
			for _, change := range proposal.Changes {
				if _, exists := m.active.values[change.RecordID]; exists {
					active[change.RecordID] = struct{}{}
				}
			}
		}
		values := make(map[model.ID]persistentDelta, len(ids))
		misses := make([]model.ID, 0, len(ids))
		for _, id := range ids {
			if value, found := lookupLayers(m.active, m.frozen, id); found {
				values[id] = value
			} else {
				misses = append(misses, id)
			}
		}
		m.mu.RUnlock()
		for _, id := range misses {
			addr, exists, err := root.Lookup(id)
			if err != nil {
				return GroupPlan{}, err
			}
			value := persistentDelta{exists: exists}
			if exists {
				value.addr = addr
			}
			values[id] = value
		}
		m.mu.RLock()
		stable := epoch == m.epoch && base == m.covered && root == m.root
		m.mu.RUnlock()
		if !stable {
			continue
		}
		plan, err := resolveGroupAt(base, proposals, func(id model.ID) (recordlog.VAddr, bool, error) {
			value := values[id]
			return value.addr, value.exists, nil
		})
		if err == nil {
			assignDeltaEntries(&plan, active)
		}
		return plan, err
	}
}

func (m *Persistent) PublishGroup(first model.CommitSeq, plan GroupPlan, reservations []DeltaReservation) (PublishResult, error) {
	if first == 0 || len(plan.Proposals) == 0 || uint64(len(plan.Proposals)) > math.MaxUint32 || validateResolvedPlan(plan) != nil {
		return PublishResult{}, ErrInvalid
	}
	if err := validateReservations(plan, reservations); err != nil {
		return PublishResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if plan.BaseCommitSeq != m.covered {
		return PublishResult{}, ErrStalePlan
	}
	active := make(map[model.ID]struct{})
	for _, proposal := range plan.Proposals {
		for _, change := range proposal.Changes {
			if _, exists := m.active.values[change.Change.RecordID]; exists {
				active[change.Change.RecordID] = struct{}{}
			}
		}
	}
	if !validDeltaEntries(plan, active) {
		return PublishResult{}, ErrStalePlan
	}
	accepted := uint64(0)
	for _, proposal := range plan.Proposals {
		if proposal.Accepted {
			accepted++
		}
	}
	if accepted == 0 || first != m.covered+1 || accepted-1 > ^uint64(0)-uint64(first) {
		return PublishResult{}, ErrInvalid
	}
	result := PublishResult{Committed: uint32(accepted)}
	var charged uint64
	for index, proposal := range plan.Proposals {
		if !proposal.Accepted {
			reservations[index].Release()
			continue
		}
		consumed, err := consumeReservation(reservations[index], proposal.DeltaEntries)
		if err != nil || charged > math.MaxUint64-consumed {
			return PublishResult{}, ErrCorrupt
		}
		charged += consumed
	}
	if m.active.charge > math.MaxUint64-charged {
		return PublishResult{}, ErrCorrupt
	}
	// Consume every reservation before changing the visible Delta. An invariant
	// failure must not leave a partially published durable commit group.
	for _, proposal := range plan.Proposals {
		if !proposal.Accepted {
			continue
		}
		for _, resolved := range proposal.Changes {
			if !resolved.Apply {
				result.Skipped++
				continue
			}
			if resolved.Change.Operation == OperationDelete {
				m.active.values[resolved.Change.RecordID] = persistentDelta{}
			} else {
				m.active.values[resolved.Change.RecordID] = persistentDelta{addr: resolved.Change.NewAddr, exists: true}
			}
			result.Applied++
		}
	}
	m.active.charge += charged
	m.covered = model.CommitSeq(uint64(first) + accepted - 1)
	m.epoch++
	return result, nil
}

func (m *Persistent) CoveredCommitSeq() model.CommitSeq {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.covered
}

type FrozenCheckpoint struct {
	owner   *Persistent
	id      uint64
	base    *radix.Tree
	covered model.CommitSeq
	layers  []*deltaLayer
}

func (c *FrozenCheckpoint) CoveredCommitSeq() model.CommitSeq {
	if c == nil {
		return 0
	}
	return c.covered
}

type CheckpointCandidate struct {
	checkpoint *FrozenCheckpoint
	tree       *radix.Tree
	root       model.MapAddr
	covered    model.CommitSeq
}

func (c CheckpointCandidate) Root() model.MapAddr               { return c.root }
func (c CheckpointCandidate) CoveredCommitSeq() model.CommitSeq { return c.covered }

func (c CheckpointCandidate) Walk(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	if c.tree == nil {
		return ErrInvalid
	}
	return c.tree.Walk(ctx, visit)
}

func (m *Persistent) Freeze(expected model.CommitSeq) (*FrozenCheckpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inCheckpoint || expected != m.covered {
		return nil, ErrStalePlan
	}
	m.frozen = append(m.frozen, m.active)
	m.active = &deltaLayer{values: make(map[model.ID]persistentDelta)}
	m.checkpointID++
	m.inCheckpoint = true
	m.epoch++
	layers := append([]*deltaLayer(nil), m.frozen...)
	return &FrozenCheckpoint{owner: m, id: m.checkpointID, base: m.root, covered: expected, layers: layers}, nil
}

func (m *Persistent) BuildCheckpoint(checkpoint *FrozenCheckpoint) (CheckpointCandidate, error) {
	if checkpoint == nil || checkpoint.owner != m {
		return CheckpointCandidate{}, ErrInvalid
	}
	m.mu.RLock()
	valid := m.inCheckpoint && checkpoint.id == m.checkpointID && len(m.frozen) >= len(checkpoint.layers)
	if valid {
		for index, layer := range checkpoint.layers {
			if m.frozen[index] != layer {
				valid = false
				break
			}
		}
	}
	m.mu.RUnlock()
	if !valid {
		return CheckpointCandidate{}, ErrStalePlan
	}
	var total uint64
	maxMutations := m.checkpointSortBytes / checkpointMutationBytes
	for _, layer := range checkpoint.layers {
		if uint64(len(layer.values)) > maxMutations-total {
			return CheckpointCandidate{}, ErrBudget
		}
		total += uint64(len(layer.values))
	}
	mutations := make([]radix.Mutation, 0, total)
	for _, layer := range checkpoint.layers {
		for id, value := range layer.values {
			mutations = append(mutations, radix.Mutation{ID: id, Addr: value.addr})
		}
	}
	sort.SliceStable(mutations, func(i, j int) bool { return mutations[i].ID < mutations[j].ID })
	unique := 0
	for start := 0; start < len(mutations); {
		end := start + 1
		for end < len(mutations) && mutations[end].ID == mutations[start].ID {
			end++
		}
		mutations[unique] = mutations[end-1]
		unique++
		start = end
	}
	mutations = mutations[:unique]
	tree, err := checkpoint.base.BuildSorted(checkpoint.covered, mutations)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	if err := m.syncer.Sync(); err != nil {
		return CheckpointCandidate{}, err
	}
	return CheckpointCandidate{checkpoint: checkpoint, tree: tree, root: tree.Root(), covered: tree.Covered()}, nil
}

// InstallCheckpoint is called only after the Catalog generation containing
// candidate.Root and candidate.Covered is durably installed.
func (m *Persistent) InstallCheckpoint(candidate CheckpointCandidate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	checkpoint := candidate.checkpoint
	if checkpoint == nil || checkpoint.owner != m || !m.inCheckpoint || checkpoint.id != m.checkpointID || candidate.tree == nil || candidate.root != candidate.tree.Root() || candidate.covered != checkpoint.covered || candidate.covered != candidate.tree.Covered() || len(m.frozen) < len(checkpoint.layers) {
		return ErrStalePlan
	}
	for index, layer := range checkpoint.layers {
		if m.frozen[index] != layer {
			return ErrStalePlan
		}
	}
	var released uint64
	for _, layer := range checkpoint.layers {
		if released > math.MaxUint64-layer.charge {
			return ErrCorrupt
		}
		released += layer.charge
	}
	m.budget.mu.Lock()
	if released > m.budget.charged {
		m.budget.mu.Unlock()
		return ErrCorrupt
	}
	m.root = candidate.tree
	m.frozen = append([]*deltaLayer(nil), m.frozen[len(checkpoint.layers):]...)
	m.inCheckpoint = false
	m.epoch++
	m.budget.charged -= released
	m.budget.mu.Unlock()
	return nil
}

func (m *Persistent) DeltaUsage() (charged, reserved, soft, hard uint64) {
	return m.budget.usage()
}

func (m *Persistent) CacheBytes() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.root.CacheBytes()
}

func (m *Persistent) AbortCheckpoint(checkpoint *FrozenCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if checkpoint == nil || checkpoint.owner != m || !m.inCheckpoint || checkpoint.id != m.checkpointID {
		return ErrStalePlan
	}
	m.inCheckpoint = false
	return nil
}

// ReplaceCheckpointRoot switches the physical owner of an already installed
// checkpoint without changing its logical contents. Callers must quiesce all
// Mapping users before calling it. A rewrite is only valid when checkpointing
// has drained every delta layer into the current root.
func (m *Persistent) ReplaceCheckpointRoot(root *radix.Tree, syncer NodeSyncer) error {
	if root == nil || syncer == nil {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inCheckpoint || len(m.active.values) != 0 || len(m.frozen) != 0 ||
		m.active.charge != 0 || root.Covered() != m.covered {
		return ErrStalePlan
	}
	charged, reserved, _, _ := m.budget.usage()
	if charged != 0 || reserved != 0 {
		return ErrStalePlan
	}
	m.root = root
	m.syncer = syncer
	m.epoch++
	return nil
}

// WalkCheckpoint visits the fully checkpointed root. It rejects a Mapping
// with any overlay state so callers cannot accidentally omit committed deltas.
func (m *Persistent) WalkCheckpoint(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	if visit == nil {
		return ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.inCheckpoint || len(m.active.values) != 0 || len(m.frozen) != 0 || m.active.charge != 0 {
		return ErrStalePlan
	}
	return m.root.Walk(ctx, visit)
}

func lookupLayers(active *deltaLayer, frozen []*deltaLayer, id model.ID) (persistentDelta, bool) {
	if value, found := active.values[id]; found {
		return value, true
	}
	for index := len(frozen) - 1; index >= 0; index-- {
		if value, found := frozen[index].values[id]; found {
			return value, true
		}
	}
	return persistentDelta{}, false
}

func proposalReadSet(proposals []Proposal) []model.ID {
	set := make(map[model.ID]struct{})
	for _, proposal := range proposals {
		for _, condition := range proposal.Conditions {
			set[condition.RecordID] = struct{}{}
		}
		if proposal.Kind == ProposalRelocation {
			for _, change := range proposal.Changes {
				set[change.RecordID] = struct{}{}
			}
		}
	}
	ids := make([]model.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
