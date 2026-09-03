package mapping

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
)

type NodeSyncer interface {
	Sync() error
}

type persistentDelta struct {
	ref    recordlog.RecordRef
	exists bool
}

type deltaLayer struct {
	values map[model.ID]persistentDelta
	charge uint64
}

type rootOwner struct {
	syncer    NodeSyncer
	readers   atomic.Int64
	retired   atomic.Bool
	closeOnce sync.Once
	drained   chan struct{}
}

// Persistent is the v2 production Mapping runtime. The complete Mapping is
// never materialized: only committed deltas and a bounded radix cache live in
// memory.
type Persistent struct {
	mu sync.RWMutex

	root    *radix.Tree
	owner   *rootOwner
	covered model.CommitSeq
	epoch   uint64
	active  *deltaLayer
	frozen  []*deltaLayer // oldest to newest

	checkpointID        uint64
	inCheckpoint        bool
	checkpointSortBytes uint64
	budget              *deltaBudget
	live                map[recordlog.SegmentID]SegmentLiveStats
}

type PersistentConfig struct {
	CheckpointSortBytes uint64
	DeltaSoftLimitBytes uint64
	DeltaHardLimitBytes uint64
}

// PersistentState is the complete logical state carried by one durable
// checkpoint. Recovery opens the immutable Mapping root and its live Segment
// statistics from the same cut, then advances both through WAL replay.
type PersistentState struct {
	StatsCoveredCommitSeq model.CommitSeq
	LiveStats             map[recordlog.SegmentID]SegmentLiveStats
}

const checkpointMutationBytes = uint64(24)

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

func OpenPersistent(root *radix.Tree, syncer NodeSyncer, config PersistentConfig, state PersistentState) (*Persistent, error) {
	if root == nil || syncer == nil || ValidatePersistentConfig(config) != nil {
		return nil, ErrInvalid
	}
	if state.StatsCoveredCommitSeq != root.Covered() {
		return nil, ErrInvalid
	}
	live := make(map[recordlog.SegmentID]SegmentLiveStats, len(state.LiveStats))
	for segment, stat := range state.LiveStats {
		if segment == 0 || stat.LiveBytes == 0 || stat.LiveRecords == 0 || stat.LastChangedCommitSeq > root.Covered() {
			return nil, ErrInvalid
		}
		live[segment] = stat
	}
	if root.Root() != 0 && len(live) == 0 {
		return nil, ErrInvalid
	}
	budget, _ := newDeltaBudget(config.DeltaSoftLimitBytes, config.DeltaHardLimitBytes)
	return &Persistent{
		root: root, owner: &rootOwner{syncer: syncer, drained: make(chan struct{})}, covered: root.Covered(),
		active:              &deltaLayer{values: make(map[model.ID]persistentDelta)},
		checkpointSortBytes: config.CheckpointSortBytes,
		budget:              budget,
		live:                live,
	}, nil
}

func (m *Persistent) ReserveDelta(ids []model.ID) (DeltaReservation, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var entries uint64
	seen := make(map[model.ID]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			return nil, 0, ErrInvalid
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		if _, exists := m.active.values[id]; !exists {
			entries++
		}
	}
	reservation, pressure, err := m.budget.reserve(entries)
	if err != nil {
		if err != ErrBudget || !pressure {
			return reservation, 0, err
		}
		if m.checkpointID == math.MaxUint64 {
			return nil, 0, base.ErrGenerationExhausted
		}
		return nil, m.checkpointID + 1, err
	}
	if !pressure {
		return reservation, 0, err
	}
	if m.checkpointID == math.MaxUint64 {
		reservation.Release()
		return nil, 0, base.ErrGenerationExhausted
	}
	// The current active Delta will be frozen by the next checkpoint ID.
	// Returning that ID lets the Engine discard a late pressure notification
	// after another checkpoint has already covered this layer.
	return reservation, m.checkpointID + 1, nil
}

func (m *Persistent) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	ref, exists, err := m.LookupRef(id)
	return ref.Addr, exists, err
}

func (m *Persistent) LookupRef(id model.ID) (recordlog.RecordRef, bool, error) {
	if id == 0 {
		return recordlog.RecordRef{}, false, ErrInvalid
	}
	m.mu.RLock()
	if value, found := lookupLayers(m.active, m.frozen, id); found {
		m.mu.RUnlock()
		return value.ref, value.exists, nil
	}
	root, owner := m.root, m.owner
	owner.readers.Add(1)
	m.mu.RUnlock()
	defer m.releaseRoot(owner)
	return root.LookupRef(id)
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
		epoch, base, root, owner := m.epoch, m.covered, m.root, m.owner
		owner.readers.Add(1)
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
			ref, exists, err := root.LookupRef(id)
			if err != nil {
				m.releaseRoot(owner)
				return GroupPlan{}, err
			}
			value := persistentDelta{exists: exists}
			if exists {
				value.ref = ref
			}
			values[id] = value
		}
		m.releaseRoot(owner)
		m.mu.RLock()
		stable := epoch == m.epoch && base == m.covered && root == m.root && owner == m.owner
		m.mu.RUnlock()
		if !stable {
			continue
		}
		plan, err := resolveGroupAt(base, proposals, func(id model.ID) (recordlog.RecordRef, bool, error) {
			value := values[id]
			return value.ref, value.exists, nil
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
	if err := applyPlanLiveStats(m.live, first, plan); err != nil {
		return PublishResult{}, err
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
				m.active.values[resolved.Change.RecordID] = persistentDelta{ref: resolved.Change.NewRef, exists: true}
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

func (m *Persistent) LiveStats() map[recordlog.SegmentID]SegmentLiveStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneLiveStats(m.live)
}

type FrozenCheckpoint struct {
	owner   *Persistent
	id      uint64
	base    *radix.Tree
	covered model.CommitSeq
	layers  []*deltaLayer
	live    map[recordlog.SegmentID]SegmentLiveStats
}

// PressureGeneration identifies the active Delta generation covered by this
// checkpoint. It is runtime scheduling state and is never persisted.
func (c *FrozenCheckpoint) PressureGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.id
}

func (c *FrozenCheckpoint) CoveredCommitSeq() model.CommitSeq {
	if c == nil {
		return 0
	}
	return c.covered
}

// EntryUpperBound returns the number of frozen Delta entries before duplicate
// IDs are folded. It is a conservative input for checkpoint disk admission.
func (c *FrozenCheckpoint) EntryUpperBound() (uint64, error) {
	if c == nil || c.owner == nil {
		return 0, ErrInvalid
	}
	var total uint64
	for _, layer := range c.layers {
		if uint64(len(layer.values)) > math.MaxUint64-total {
			return 0, ErrBudget
		}
		total += uint64(len(layer.values))
	}
	return total, nil
}

type CheckpointCandidate struct {
	checkpoint *FrozenCheckpoint
	tree       *radix.Tree
	root       model.MapAddr
	covered    model.CommitSeq
	changes    []radix.Mutation
	entryDelta radix.EntryDelta
}

func (c CheckpointCandidate) LiveStats() map[recordlog.SegmentID]SegmentLiveStats {
	if c.checkpoint == nil {
		return nil
	}
	return cloneLiveStats(c.checkpoint.live)
}

func (c CheckpointCandidate) Root() model.MapAddr               { return c.root }
func (c CheckpointCandidate) CoveredCommitSeq() model.CommitSeq { return c.covered }

func (c CheckpointCandidate) BaseCoveredCommitSeq() model.CommitSeq {
	if c.checkpoint == nil || c.checkpoint.base == nil {
		return 0
	}
	return c.checkpoint.base.Covered()
}

func (c CheckpointCandidate) BaseRoot() model.MapAddr {
	if c.checkpoint == nil || c.checkpoint.base == nil {
		return 0
	}
	return c.checkpoint.base.Root()
}

// EntryCount applies this checkpoint's exact leaf-level cardinality delta to
// the durable count belonging to its base Root.
func (c CheckpointCandidate) EntryCount(base uint64) (uint64, error) {
	if c.checkpoint == nil || c.tree == nil || c.entryDelta.Removed > base {
		return 0, ErrCorrupt
	}
	remaining := base - c.entryDelta.Removed
	if c.entryDelta.Added > math.MaxUint64-remaining {
		return 0, ErrCorrupt
	}
	return remaining + c.entryDelta.Added, nil
}

// ReachableBytes applies the exact node replacements performed by this COW
// build to a surveyed base Root.
func (c CheckpointCandidate) ReachableBytes(base uint64) (uint64, error) {
	if c.checkpoint == nil || c.tree == nil || c.entryDelta.ReachableBytesRemoved > base {
		return 0, ErrCorrupt
	}
	remaining := base - c.entryDelta.ReachableBytesRemoved
	if c.entryDelta.ReachableBytesAdded > math.MaxUint64-remaining {
		return 0, ErrCorrupt
	}
	return remaining + c.entryDelta.ReachableBytesAdded, nil
}

func (c CheckpointCandidate) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	ref, exists, err := c.LookupRef(id)
	return ref.Addr, exists, err
}

func (c CheckpointCandidate) LookupRef(id model.ID) (recordlog.RecordRef, bool, error) {
	if c.tree == nil {
		return recordlog.RecordRef{}, false, ErrInvalid
	}
	return c.tree.LookupRef(id)
}

// WalkChanges visits the folded base-to-candidate transition once per changed
// ID. It reads old values only from the immutable base root and never observes
// commits published after the checkpoint cut.
func (c CheckpointCandidate) WalkChanges(ctx context.Context, visit func(model.ID, recordlog.RecordRef, bool, recordlog.RecordRef, bool) error) error {
	if c.checkpoint == nil || c.checkpoint.base == nil || c.tree == nil || visit == nil {
		return ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, change := range c.changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		oldRef, oldExists, err := c.checkpoint.base.LookupRef(change.ID)
		if err != nil {
			return err
		}
		newExists := !change.Ref.IsZero()
		if oldExists == newExists && (!oldExists || oldRef == change.Ref) {
			continue
		}
		if err := visit(change.ID, oldRef, oldExists, change.Ref, newExists); err != nil {
			return err
		}
	}
	return nil
}

func (c CheckpointCandidate) Walk(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	if c.tree == nil {
		return ErrInvalid
	}
	return c.tree.Walk(ctx, visit)
}

func (c CheckpointCandidate) WalkRefs(ctx context.Context, visit func(model.ID, recordlog.RecordRef) error) error {
	if c.tree == nil {
		return ErrInvalid
	}
	return c.tree.WalkRefs(ctx, visit)
}

func (m *Persistent) Freeze(expected model.CommitSeq) (*FrozenCheckpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inCheckpoint || expected != m.covered {
		return nil, ErrStalePlan
	}
	if m.checkpointID == math.MaxUint64 {
		return nil, base.ErrGenerationExhausted
	}
	m.frozen = append(m.frozen, m.active)
	m.active = &deltaLayer{values: make(map[model.ID]persistentDelta)}
	m.checkpointID++
	m.inCheckpoint = true
	m.epoch++
	layers := append([]*deltaLayer(nil), m.frozen...)
	return &FrozenCheckpoint{owner: m, id: m.checkpointID, base: m.root, covered: expected, layers: layers, live: cloneLiveStats(m.live)}, nil
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
			mutations = append(mutations, radix.Mutation{ID: id, Ref: value.ref})
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
	tree, entryDelta, err := checkpoint.base.BuildSortedWithEntryDelta(checkpoint.covered, mutations)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	if err := m.owner.syncer.Sync(); err != nil {
		return CheckpointCandidate{}, err
	}
	return CheckpointCandidate{
		checkpoint: checkpoint, tree: tree, root: tree.Root(), covered: tree.Covered(),
		changes: mutations, entryDelta: entryDelta,
	}, nil
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

// CheckpointView pins the immutable checkpoint Root while newer commits may
// continue accumulating in the active Delta layer. The owner must keep the
// underlying NodeSyncer open until the view is no longer used.
type CheckpointView struct {
	owner     *Persistent
	rootOwner *rootOwner
	tree      *radix.Tree
}

func (m *Persistent) CheckpointView() (CheckpointView, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.inCheckpoint || len(m.frozen) != 0 || m.root == nil {
		return CheckpointView{}, ErrStalePlan
	}
	return CheckpointView{owner: m, rootOwner: m.owner, tree: m.root}, nil
}

func (v CheckpointView) Root() model.MapAddr { return v.tree.Root() }

func (v CheckpointView) Covered() model.CommitSeq { return v.tree.Covered() }

func (v CheckpointView) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	if v.owner == nil || v.rootOwner == nil || v.tree == nil {
		return 0, false, ErrInvalid
	}
	if !v.pin() {
		return 0, false, ErrStalePlan
	}
	defer v.owner.releaseRoot(v.rootOwner)
	return v.tree.Lookup(id)
}

func (v CheckpointView) LookupRef(id model.ID) (recordlog.RecordRef, bool, error) {
	if v.owner == nil || v.rootOwner == nil || v.tree == nil {
		return recordlog.RecordRef{}, false, ErrInvalid
	}
	if !v.pin() {
		return recordlog.RecordRef{}, false, ErrStalePlan
	}
	defer v.owner.releaseRoot(v.rootOwner)
	return v.tree.LookupRef(id)
}

func (v CheckpointView) Walk(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	if v.owner == nil || v.rootOwner == nil || v.tree == nil {
		return ErrInvalid
	}
	if !v.pin() {
		return ErrStalePlan
	}
	defer v.owner.releaseRoot(v.rootOwner)
	return v.tree.Walk(ctx, visit)
}

func (v CheckpointView) WalkRefs(ctx context.Context, visit func(model.ID, recordlog.RecordRef) error) error {
	if v.owner == nil || v.rootOwner == nil || v.tree == nil {
		return ErrInvalid
	}
	if !v.pin() {
		return ErrStalePlan
	}
	defer v.owner.releaseRoot(v.rootOwner)
	return v.tree.WalkRefs(ctx, visit)
}

func (v CheckpointView) ReachableBytes(ctx context.Context) (uint64, error) {
	if v.owner == nil || v.rootOwner == nil || v.tree == nil {
		return 0, ErrInvalid
	}
	if !v.pin() {
		return 0, ErrStalePlan
	}
	defer v.owner.releaseRoot(v.rootOwner)
	return v.tree.ReachableBytes(ctx)
}

func (v CheckpointView) pin() bool {
	v.owner.mu.RLock()
	defer v.owner.mu.RUnlock()
	if v.owner.root != v.tree || v.owner.owner != v.rootOwner {
		return false
	}
	v.rootOwner.readers.Add(1)
	return true
}

// ReplaceCheckpointRoot switches the physical owner of an already installed
// checkpoint without changing its logical contents. Newer active Delta entries
// remain layered above the replacement. The returned channel closes after all
// readers of the old root drain; callers keep its NodeSyncer open until then.
func (m *Persistent) ReplaceCheckpointRoot(view CheckpointView, root *radix.Tree, syncer NodeSyncer) (<-chan struct{}, error) {
	if view.owner != m || view.rootOwner == nil || view.tree == nil || root == nil || syncer == nil {
		return nil, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inCheckpoint || len(m.frozen) != 0 || m.root != view.tree || m.owner != view.rootOwner ||
		root.Covered() != view.tree.Covered() {
		return nil, ErrStalePlan
	}
	oldOwner := m.owner
	m.root = root
	m.owner = &rootOwner{syncer: syncer, drained: make(chan struct{})}
	m.epoch++
	oldOwner.retired.Store(true)
	if oldOwner.readers.Load() == 0 {
		oldOwner.closeOnce.Do(func() { close(oldOwner.drained) })
	}
	return oldOwner.drained, nil
}

func (m *Persistent) releaseRoot(owner *rootOwner) {
	if owner == nil {
		return
	}
	remaining := owner.readers.Add(-1)
	if remaining < 0 {
		panic("mapping: root reader reference underflow")
	}
	if remaining == 0 && owner.retired.Load() {
		owner.closeOnce.Do(func() { close(owner.drained) })
	}
}

// WalkCheckpoint visits the fully checkpointed root. It rejects a Mapping
// with any overlay state so callers cannot accidentally omit committed deltas.
func (m *Persistent) WalkCheckpoint(ctx context.Context, visit func(model.ID, recordlog.VAddr) error) error {
	if visit == nil {
		return ErrInvalid
	}
	m.mu.RLock()
	if m.inCheckpoint || len(m.active.values) != 0 || len(m.frozen) != 0 || m.active.charge != 0 {
		m.mu.RUnlock()
		return ErrStalePlan
	}
	root, owner := m.root, m.owner
	owner.readers.Add(1)
	m.mu.RUnlock()
	defer m.releaseRoot(owner)
	return root.Walk(ctx, visit)
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
		for _, change := range proposal.Changes {
			set[change.RecordID] = struct{}{}
		}
	}
	ids := make([]model.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
