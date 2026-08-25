package mapping

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

var (
	ErrInvalid   = errors.New("mapping: invalid input")
	ErrCorrupt   = errors.New("mapping: corrupt state")
	ErrBudget    = errors.New("mapping: checkpoint budget exceeded")
	ErrStalePlan = errors.New("mapping: stale resolved plan")
)

type Entry struct {
	Addr     recordlog.VAddr
	Revision model.Revision
}

type ConditionKind uint8

const (
	ConditionRevision ConditionKind = iota + 1
	ConditionAbsent
)

type Condition struct {
	RecordID model.ID
	Kind     ConditionKind
	Revision model.Revision
}

type Operation uint8

const (
	OperationPut Operation = iota + 1
	OperationDelete
	OperationRelocate
)

type Change struct {
	RecordID        model.ID
	NewAddr         recordlog.VAddr
	ExpectedOldAddr recordlog.VAddr
	Operation       Operation
}

type ProposalKind uint8

const (
	ProposalUserCommit ProposalKind = iota + 1
	ProposalRelocation
)

type Proposal struct {
	Kind       ProposalKind
	Revision   model.Revision
	Conditions []Condition
	Changes    []Change
}

type ResolvedChange struct {
	Change   Change
	Revision model.Revision
	Apply    bool
}

type ResolvedProposal struct {
	Kind     ProposalKind
	Revision model.Revision
	Accepted bool
	Changes  []ResolvedChange
}

type GroupPlan struct {
	BaseCommitSeq model.CommitSeq
	Proposals     []ResolvedProposal
}

type PublishResult struct {
	Committed uint32
	Applied   uint32
	Skipped   uint32
}

type Snapshot struct {
	CoveredCommitSeq model.CommitSeq
	Entries          map[model.ID]Entry
}

type Mapping struct {
	mu      sync.RWMutex
	covered model.CommitSeq
	entries map[model.ID]Entry
}

func New(snapshot Snapshot) (*Mapping, error) {
	entries := make(map[model.ID]Entry, len(snapshot.Entries))
	for id, entry := range snapshot.Entries {
		if id == 0 || !validEntry(entry) {
			return nil, ErrInvalid
		}
		entries[id] = entry
	}
	return &Mapping{covered: snapshot.CoveredCommitSeq, entries: entries}, nil
}

func NewEmpty() *Mapping {
	mapping, _ := New(Snapshot{})
	return mapping
}

func (m *Mapping) Lookup(id model.ID) (Entry, bool, error) {
	if id == 0 {
		return Entry{}, false, ErrInvalid
	}
	m.mu.RLock()
	entry, exists := m.entries[id]
	m.mu.RUnlock()
	return entry, exists, nil
}

func (m *Mapping) ResolveGroup(proposals []Proposal) (GroupPlan, error) {
	if len(proposals) == 0 || uint64(len(proposals)) > math.MaxUint32 {
		return GroupPlan{}, ErrInvalid
	}
	for _, proposal := range proposals {
		if err := validateProposal(proposal); err != nil {
			return GroupPlan{}, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return resolveGroupAt(m.covered, proposals, func(id model.ID) (Entry, bool, error) {
		value, ok := m.entries[id]
		return value, ok, nil
	})
}

func resolveGroupAt(base model.CommitSeq, proposals []Proposal, baseLookup func(model.ID) (Entry, bool, error)) (GroupPlan, error) {
	plan := GroupPlan{BaseCommitSeq: base, Proposals: make([]ResolvedProposal, len(proposals))}
	type virtualEntry struct {
		entry  Entry
		exists bool
	}
	virtual := make(map[model.ID]virtualEntry)
	lookup := func(id model.ID) (Entry, bool, error) {
		if value, ok := virtual[id]; ok {
			return value.entry, value.exists, nil
		}
		return baseLookup(id)
	}
	for index, proposal := range proposals {
		resolved := ResolvedProposal{Kind: proposal.Kind, Revision: proposal.Revision, Accepted: true}
		for _, condition := range proposal.Conditions {
			entry, exists, err := lookup(condition.RecordID)
			if err != nil {
				return GroupPlan{}, err
			}
			if (condition.Kind == ConditionAbsent && exists) || (condition.Kind == ConditionRevision && (!exists || entry.Revision != condition.Revision)) {
				resolved.Accepted = false
				break
			}
		}
		if !resolved.Accepted {
			plan.Proposals[index] = resolved
			continue
		}
		resolved.Changes = make([]ResolvedChange, len(proposal.Changes))
		for changeIndex, change := range proposal.Changes {
			result := ResolvedChange{Change: change, Apply: true}
			switch proposal.Kind {
			case ProposalUserCommit:
				result.Revision = proposal.Revision
				if change.Operation == OperationDelete {
					virtual[change.RecordID] = virtualEntry{}
				} else {
					virtual[change.RecordID] = virtualEntry{entry: Entry{Addr: change.NewAddr, Revision: proposal.Revision}, exists: true}
				}
			case ProposalRelocation:
				current, exists, err := lookup(change.RecordID)
				if err != nil {
					return GroupPlan{}, err
				}
				result.Apply = exists && current.Addr == change.ExpectedOldAddr
				if result.Apply {
					result.Revision = current.Revision
					virtual[change.RecordID] = virtualEntry{entry: Entry{Addr: change.NewAddr, Revision: current.Revision}, exists: true}
				}
			}
			resolved.Changes[changeIndex] = result
		}
		plan.Proposals[index] = resolved
	}
	return plan, nil
}

func (m *Mapping) PublishGroup(firstCommitSeq model.CommitSeq, plan GroupPlan) (PublishResult, error) {
	if firstCommitSeq == 0 || len(plan.Proposals) == 0 {
		return PublishResult{}, ErrInvalid
	}
	if err := validateResolvedPlan(plan); err != nil {
		return PublishResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if plan.BaseCommitSeq != m.covered {
		return PublishResult{}, ErrStalePlan
	}
	if m.covered == model.CommitSeq(math.MaxUint64) || firstCommitSeq != m.covered+1 {
		return PublishResult{}, ErrInvalid
	}
	accepted := uint64(0)
	for _, proposal := range plan.Proposals {
		if proposal.Accepted {
			accepted++
		}
	}
	if accepted == 0 || accepted-1 > math.MaxUint64-uint64(firstCommitSeq) {
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
				delete(m.entries, resolved.Change.RecordID)
			} else {
				m.entries[resolved.Change.RecordID] = Entry{Addr: resolved.Change.NewAddr, Revision: resolved.Revision}
			}
			result.Applied++
		}
	}
	m.covered = model.CommitSeq(uint64(firstCommitSeq) + accepted - 1)
	return result, nil
}

func (m *Mapping) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := make(map[model.ID]Entry, len(m.entries))
	for id, entry := range m.entries {
		entries[id] = entry
	}
	return Snapshot{CoveredCommitSeq: m.covered, Entries: entries}
}

func (m *Mapping) CoveredCommitSeq() model.CommitSeq {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.covered
}

func validateProposal(proposal Proposal) error {
	switch proposal.Kind {
	case ProposalUserCommit:
		if proposal.Revision == 0 {
			return ErrInvalid
		}
	case ProposalRelocation:
		if proposal.Revision != 0 || len(proposal.Conditions) != 0 || len(proposal.Changes) == 0 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	var previous model.ID
	for index, condition := range proposal.Conditions {
		if condition.RecordID == 0 || (index != 0 && condition.RecordID <= previous) {
			return ErrInvalid
		}
		if (condition.Kind == ConditionRevision && condition.Revision == 0) || (condition.Kind == ConditionAbsent && condition.Revision != 0) || (condition.Kind != ConditionRevision && condition.Kind != ConditionAbsent) {
			return ErrInvalid
		}
		previous = condition.RecordID
	}
	previous = 0
	for index, change := range proposal.Changes {
		if change.RecordID == 0 || (index != 0 && change.RecordID <= previous) {
			return ErrInvalid
		}
		switch proposal.Kind {
		case ProposalUserCommit:
			if change.ExpectedOldAddr != 0 || (change.Operation == OperationPut && !change.NewAddr.Valid()) || (change.Operation == OperationDelete && change.NewAddr != 0) || (change.Operation != OperationPut && change.Operation != OperationDelete) {
				return ErrInvalid
			}
		case ProposalRelocation:
			if change.Operation != OperationRelocate || !change.NewAddr.Valid() || !change.ExpectedOldAddr.Valid() || change.NewAddr == change.ExpectedOldAddr {
				return ErrInvalid
			}
		}
		previous = change.RecordID
	}
	return nil
}

func validateResolvedPlan(plan GroupPlan) error {
	for _, proposal := range plan.Proposals {
		if !proposal.Accepted {
			if proposal.Kind != ProposalUserCommit || proposal.Revision == 0 || len(proposal.Changes) != 0 {
				return fmt.Errorf("rejected proposal has changes: %w", ErrInvalid)
			}
			continue
		}
		changes := make([]Change, len(proposal.Changes))
		for i, resolved := range proposal.Changes {
			changes[i] = resolved.Change
			if proposal.Kind == ProposalUserCommit && (!resolved.Apply || resolved.Revision != proposal.Revision) {
				return ErrInvalid
			}
			if proposal.Kind == ProposalRelocation && resolved.Apply && resolved.Revision == 0 {
				return ErrInvalid
			}
			if proposal.Kind == ProposalRelocation && !resolved.Apply && resolved.Revision != 0 {
				return ErrInvalid
			}
		}
		if err := validateProposal(Proposal{Kind: proposal.Kind, Revision: proposal.Revision, Changes: changes}); err != nil {
			return err
		}
	}
	return nil
}

func validEntry(entry Entry) bool {
	return entry.Addr.Valid() && entry.Revision != 0
}
