package mapping

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
)

type RevisionResolver interface {
	ResolveRevision(recordlog.VAddr, model.ID) (model.Revision, error)
}

type NodeSyncer interface {
	Sync() error
}

type persistentDelta struct {
	entry  Entry
	exists bool
}

type deltaLayer struct {
	values map[model.ID]persistentDelta
}

// Persistent is the v2 production Mapping runtime. The complete Mapping is
// never materialized: only committed deltas and a bounded radix cache live in
// memory.
type Persistent struct {
	mu sync.RWMutex

	root     *radix.Tree
	resolver RevisionResolver
	syncer   NodeSyncer
	covered  model.CommitSeq
	epoch    uint64
	active   *deltaLayer
	frozen   []*deltaLayer // oldest to newest

	checkpointID         uint64
	inCheckpoint         bool
	maxCheckpointEntries uint64
}

func OpenPersistent(root *radix.Tree, resolver RevisionResolver, syncer NodeSyncer, maxCheckpointEntries uint64) (*Persistent, error) {
	if root == nil || resolver == nil || syncer == nil || maxCheckpointEntries == 0 {
		return nil, ErrInvalid
	}
	return &Persistent{
		root: root, resolver: resolver, syncer: syncer, covered: root.Covered(),
		active:               &deltaLayer{values: make(map[model.ID]persistentDelta)},
		maxCheckpointEntries: maxCheckpointEntries,
	}, nil
}

func (m *Persistent) Lookup(id model.ID) (Entry, bool, error) {
	if id == 0 {
		return Entry{}, false, ErrInvalid
	}
	m.mu.RLock()
	if value, found := lookupLayers(m.active, m.frozen, id); found {
		m.mu.RUnlock()
		return value.entry, value.exists, nil
	}
	root := m.root
	m.mu.RUnlock()
	addr, exists, err := root.Lookup(id)
	if err != nil || !exists {
		return Entry{}, exists, err
	}
	revision, err := m.resolver.ResolveRevision(addr, id)
	if err != nil || revision == 0 {
		return Entry{}, false, errors.Join(ErrCorrupt, err)
	}
	return Entry{Addr: addr, Revision: revision}, true, nil
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
				revision, err := m.resolver.ResolveRevision(addr, id)
				if err != nil || revision == 0 {
					return GroupPlan{}, errors.Join(ErrCorrupt, err)
				}
				value.entry = Entry{Addr: addr, Revision: revision}
			}
			values[id] = value
		}
		m.mu.RLock()
		stable := epoch == m.epoch && base == m.covered && root == m.root
		m.mu.RUnlock()
		if !stable {
			continue
		}
		return resolveGroupAt(base, proposals, func(id model.ID) (Entry, bool, error) {
			value := values[id]
			return value.entry, value.exists, nil
		})
	}
}

func (m *Persistent) PublishGroup(first model.CommitSeq, plan GroupPlan) (PublishResult, error) {
	if first == 0 || len(plan.Proposals) == 0 || uint64(len(plan.Proposals)) > math.MaxUint32 || validateResolvedPlan(plan) != nil {
		return PublishResult{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if plan.BaseCommitSeq != m.covered {
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
				m.active.values[resolved.Change.RecordID] = persistentDelta{entry: Entry{Addr: resolved.Change.NewAddr, Revision: resolved.Revision}, exists: true}
			}
			result.Applied++
		}
	}
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
	for _, layer := range checkpoint.layers {
		if uint64(len(layer.values)) > m.maxCheckpointEntries-total {
			return CheckpointCandidate{}, ErrBudget
		}
		total += uint64(len(layer.values))
	}
	latest := make(map[model.ID]persistentDelta, total)
	for _, layer := range checkpoint.layers {
		for id, value := range layer.values {
			latest[id] = value
		}
	}
	mutations := make([]radix.Mutation, 0, len(latest))
	for id, value := range latest {
		mutations = append(mutations, radix.Mutation{ID: id, Addr: value.entry.Addr})
	}
	sort.Slice(mutations, func(i, j int) bool { return mutations[i].ID < mutations[j].ID })
	tree, err := checkpoint.base.Build(checkpoint.covered, mutations)
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
	m.root = candidate.tree
	m.frozen = append([]*deltaLayer(nil), m.frozen[len(checkpoint.layers):]...)
	m.inCheckpoint = false
	m.epoch++
	return nil
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
