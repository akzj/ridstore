package memory

import (
	"fmt"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping/api"
)

type Mapping struct {
	mu      sync.RWMutex
	covered base.CommitSeq
	entries map[base.ID]base.VAddr
}

var _ api.Mapping = (*Mapping)(nil)

func New(snapshot api.Snapshot) (*Mapping, error) {
	entries := make(map[base.ID]base.VAddr, len(snapshot.Entries))
	for id, addr := range snapshot.Entries {
		if id == 0 || !validAddr(addr) {
			return nil, fmt.Errorf("memory mapping snapshot: %w", base.ErrInvalidConfig)
		}
		entries[id] = addr
	}
	return &Mapping{covered: snapshot.CoveredCommitSeq, entries: entries}, nil
}

func NewEmpty() *Mapping {
	m, _ := New(api.Snapshot{})
	return m
}

func (m *Mapping) Lookup(id base.ID) (base.VAddr, bool, error) {
	if id == 0 {
		return 0, false, base.ErrInvalidID
	}
	m.mu.RLock()
	addr, ok := m.entries[id]
	m.mu.RUnlock()
	return addr, ok, nil
}

func (m *Mapping) Apply(seq base.CommitSeq, kind api.ApplyKind, changes []api.Change) (api.ApplyResult, error) {
	if seq == 0 {
		return api.ApplyResult{}, fmt.Errorf("mapping apply identity: %w", base.ErrInvalidConfig)
	}
	plan, err := m.Resolve(kind, changes)
	if err != nil {
		return api.ApplyResult{}, err
	}
	return m.ApplyResolved(seq, plan)
}

func (m *Mapping) Resolve(kind api.ApplyKind, changes []api.Change) (api.ResolvedPlan, error) {
	if err := validateChanges(kind, changes); err != nil {
		return api.ResolvedPlan{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	plan := api.ResolvedPlan{BaseCommitSeq: m.covered, Kind: kind, Changes: make([]api.ResolvedChange, len(changes))}
	for i, change := range changes {
		apply := true
		if kind == api.ApplyRelocation {
			current, ok := m.entries[change.RecordID]
			apply = ok && current == change.ExpectedOldAddr
		}
		plan.Changes[i] = api.ResolvedChange{Change: change, Apply: apply}
	}
	return plan, nil
}

func (m *Mapping) ApplyResolved(seq base.CommitSeq, plan api.ResolvedPlan) (api.ApplyResult, error) {
	if seq == 0 {
		return api.ApplyResult{}, fmt.Errorf("mapping apply identity: %w", base.ErrInvalidConfig)
	}
	if err := validateResolvedPlan(plan); err != nil {
		return api.ApplyResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if plan.BaseCommitSeq != m.covered {
		return api.ApplyResult{}, fmt.Errorf("stale resolved mapping plan: %w", base.ErrCorrupt)
	}
	if seq <= m.covered {
		return api.ApplyResult{}, fmt.Errorf("mapping commit sequence regression: %w", base.ErrInvalidConfig)
	}
	result := api.ApplyResult{}
	for _, resolved := range plan.Changes {
		if !resolved.Apply {
			result.Skipped++
			continue
		}
		change := resolved.Change
		if change.NewAddr == 0 {
			delete(m.entries, change.RecordID)
		} else {
			m.entries[change.RecordID] = change.NewAddr
		}
		result.Applied++
	}
	m.covered = seq
	return result, nil
}

func (m *Mapping) CoveredCommitSeq() base.CommitSeq {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.covered
}

func (m *Mapping) Snapshot() api.Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := make(map[base.ID]base.VAddr, len(m.entries))
	for id, addr := range m.entries {
		entries[id] = addr
	}
	return api.Snapshot{CoveredCommitSeq: m.covered, Entries: entries}
}

func validateChanges(kind api.ApplyKind, changes []api.Change) error {
	if kind != api.ApplyUserCommit && kind != api.ApplyRelocation {
		return fmt.Errorf("mapping apply kind: %w", base.ErrInvalidConfig)
	}
	var previous base.ID
	for _, change := range changes {
		if change.RecordID == 0 || (previous != 0 && change.RecordID <= previous) {
			return fmt.Errorf("mapping change order: %w", base.ErrInvalidConfig)
		}
		switch kind {
		case api.ApplyUserCommit:
			if change.ExpectedOldAddr != 0 || (change.NewAddr != 0 && !validAddr(change.NewAddr)) {
				return fmt.Errorf("user mapping change: %w", base.ErrInvalidConfig)
			}
		case api.ApplyRelocation:
			if !validAddr(change.NewAddr) || !validAddr(change.ExpectedOldAddr) {
				return fmt.Errorf("relocation mapping change: %w", base.ErrInvalidConfig)
			}
		}
		previous = change.RecordID
	}
	return nil
}

func validateResolvedPlan(plan api.ResolvedPlan) error {
	changes := make([]api.Change, len(plan.Changes))
	for i, resolved := range plan.Changes {
		if plan.Kind == api.ApplyUserCommit && !resolved.Apply {
			return fmt.Errorf("skipped user mapping change: %w", base.ErrInvalidConfig)
		}
		changes[i] = resolved.Change
	}
	return validateChanges(plan.Kind, changes)
}

func validAddr(addr base.VAddr) bool {
	_, err := base.ParseVAddr(uint64(addr))
	return err == nil
}
