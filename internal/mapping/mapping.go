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

type Condition struct {
	RecordID     model.ID
	ExpectedAddr recordlog.VAddr // zero means the ID must be absent
}

type Operation uint8

const (
	OperationPut Operation = iota + 1
	OperationDelete
	OperationRelocate
)

type Change struct {
	RecordID        model.ID
	NewRef          recordlog.RecordRef
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
	Conditions []Condition
	Changes    []Change
}

type ResolvedChange struct {
	Change    Change
	Apply     bool
	OldRef    recordlog.RecordRef
	OldExists bool
}

type ResolvedProposal struct {
	Kind         ProposalKind
	Accepted     bool
	DeltaEntries uint64
	Changes      []ResolvedChange
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

type SegmentLiveStats struct {
	LiveBytes            uint64
	LiveRecords          uint64
	LastChangedCommitSeq model.CommitSeq
}

type Snapshot struct {
	CoveredCommitSeq model.CommitSeq
	Entries          map[model.ID]recordlog.RecordRef
}

// Index is the single runtime Mapping contract used by commit, replay and
// reads. Persistent is the production implementation; Mapping remains a
// temporary in-memory model for tests until the v2 Open path is complete.
type Index interface {
	Lookup(model.ID) (recordlog.VAddr, bool, error)
	LookupRef(model.ID) (recordlog.RecordRef, bool, error)
	ReserveDelta([]model.ID) (DeltaReservation, uint64, error)
	ResolveGroup([]Proposal) (GroupPlan, error)
	PublishGroup(model.CommitSeq, GroupPlan, []DeltaReservation) (PublishResult, error)
	CoveredCommitSeq() model.CommitSeq
}

var _ Index = (*Mapping)(nil)
var _ Index = (*Persistent)(nil)

type Mapping struct {
	mu      sync.RWMutex
	covered model.CommitSeq
	entries map[model.ID]recordlog.RecordRef
	live    map[recordlog.SegmentID]SegmentLiveStats
}

func New(snapshot Snapshot) (*Mapping, error) {
	entries := make(map[model.ID]recordlog.RecordRef, len(snapshot.Entries))
	live := make(map[recordlog.SegmentID]SegmentLiveStats)
	for id, ref := range snapshot.Entries {
		if id == 0 || !ref.Valid() {
			return nil, ErrInvalid
		}
		entries[id] = ref
		if err := addLiveRef(live, ref, snapshot.CoveredCommitSeq); err != nil {
			return nil, err
		}
	}
	return &Mapping{covered: snapshot.CoveredCommitSeq, entries: entries, live: live}, nil
}

func NewEmpty() *Mapping {
	mapping, _ := New(Snapshot{})
	return mapping
}

func (m *Mapping) Lookup(id model.ID) (recordlog.VAddr, bool, error) {
	ref, exists, err := m.LookupRef(id)
	return ref.Addr, exists, err
}

func (m *Mapping) LookupRef(id model.ID) (recordlog.RecordRef, bool, error) {
	if id == 0 {
		return recordlog.RecordRef{}, false, ErrInvalid
	}
	m.mu.RLock()
	entry, exists := m.entries[id]
	m.mu.RUnlock()
	return entry, exists, nil
}

func (m *Mapping) ReserveDelta([]model.ID) (DeltaReservation, uint64, error) {
	return &unlimitedReservation{}, 0, nil
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
	plan, err := resolveGroupAt(m.covered, proposals, func(id model.ID) (recordlog.RecordRef, bool, error) {
		value, ok := m.entries[id]
		return value, ok, nil
	})
	if err == nil {
		assignDeltaEntries(&plan, nil)
	}
	return plan, err
}

func resolveGroupAt(base model.CommitSeq, proposals []Proposal, baseLookup func(model.ID) (recordlog.RecordRef, bool, error)) (GroupPlan, error) {
	plan := GroupPlan{BaseCommitSeq: base, Proposals: make([]ResolvedProposal, len(proposals))}
	type virtualEntry struct {
		ref    recordlog.RecordRef
		exists bool
	}
	virtual := make(map[model.ID]virtualEntry)
	lookup := func(id model.ID) (recordlog.RecordRef, bool, error) {
		if value, ok := virtual[id]; ok {
			return value.ref, value.exists, nil
		}
		return baseLookup(id)
	}
	for index, proposal := range proposals {
		resolved := ResolvedProposal{Kind: proposal.Kind, Accepted: true}
		for _, condition := range proposal.Conditions {
			ref, exists, err := lookup(condition.RecordID)
			if err != nil {
				return GroupPlan{}, err
			}
			if condition.ExpectedAddr == 0 && exists || condition.ExpectedAddr != 0 && (!exists || ref.Addr != condition.ExpectedAddr) {
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
			current, exists, err := lookup(change.RecordID)
			if err != nil {
				return GroupPlan{}, err
			}
			result := ResolvedChange{Change: change, Apply: true, OldRef: current, OldExists: exists}
			switch proposal.Kind {
			case ProposalUserCommit:
				if change.Operation == OperationDelete {
					virtual[change.RecordID] = virtualEntry{}
				} else {
					virtual[change.RecordID] = virtualEntry{ref: change.NewRef, exists: true}
				}
			case ProposalRelocation:
				result.Apply = exists && current.Addr == change.ExpectedOldAddr
				if result.Apply {
					virtual[change.RecordID] = virtualEntry{ref: change.NewRef, exists: true}
				}
			}
			resolved.Changes[changeIndex] = result
		}
		plan.Proposals[index] = resolved
	}
	return plan, nil
}

func (m *Mapping) PublishGroup(firstCommitSeq model.CommitSeq, plan GroupPlan, reservations []DeltaReservation) (PublishResult, error) {
	if firstCommitSeq == 0 || len(plan.Proposals) == 0 {
		return PublishResult{}, ErrInvalid
	}
	if err := validateReservations(plan, reservations); err != nil {
		return PublishResult{}, err
	}
	if !validDeltaEntries(plan, nil) {
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
	for index, proposal := range plan.Proposals {
		if !proposal.Accepted {
			reservations[index].Release()
			continue
		}
		if _, err := consumeReservation(reservations[index], proposal.DeltaEntries); err != nil {
			return PublishResult{}, err
		}
	}
	if err := applyPlanLiveStats(m.live, firstCommitSeq, plan); err != nil {
		return PublishResult{}, err
	}
	// Validate/consume the complete reservation set before changing visible
	// entries, so an invariant failure cannot expose a partial group.
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
				m.entries[resolved.Change.RecordID] = resolved.Change.NewRef
			}
			result.Applied++
		}
	}
	m.covered = model.CommitSeq(uint64(firstCommitSeq) + accepted - 1)
	return result, nil
}

func assignDeltaEntries(plan *GroupPlan, active map[model.ID]struct{}) {
	seen := make(map[model.ID]struct{}, len(active))
	for id := range active {
		seen[id] = struct{}{}
	}
	for proposalIndex := range plan.Proposals {
		proposal := &plan.Proposals[proposalIndex]
		if !proposal.Accepted {
			continue
		}
		for _, change := range proposal.Changes {
			if !change.Apply {
				continue
			}
			if _, exists := seen[change.Change.RecordID]; !exists {
				proposal.DeltaEntries++
				seen[change.Change.RecordID] = struct{}{}
			}
		}
	}
}

func validDeltaEntries(plan GroupPlan, active map[model.ID]struct{}) bool {
	want := plan
	want.Proposals = append([]ResolvedProposal(nil), plan.Proposals...)
	for index := range want.Proposals {
		want.Proposals[index].DeltaEntries = 0
	}
	assignDeltaEntries(&want, active)
	for index := range plan.Proposals {
		if plan.Proposals[index].DeltaEntries != want.Proposals[index].DeltaEntries {
			return false
		}
	}
	return true
}

func validateReservations(plan GroupPlan, reservations []DeltaReservation) error {
	if len(reservations) != len(plan.Proposals) {
		return ErrInvalid
	}
	for _, reservation := range reservations {
		if reservation == nil {
			return ErrInvalid
		}
	}
	return nil
}

func consumeReservation(reservation DeltaReservation, entries uint64) (uint64, error) {
	return reservation.consume(entries)
}

func (m *Mapping) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := make(map[model.ID]recordlog.RecordRef, len(m.entries))
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

func (m *Mapping) LiveStats() map[recordlog.SegmentID]SegmentLiveStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneLiveStats(m.live)
}

func validateProposal(proposal Proposal) error {
	switch proposal.Kind {
	case ProposalUserCommit:
	case ProposalRelocation:
		if len(proposal.Conditions) != 0 || len(proposal.Changes) == 0 {
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
		if condition.ExpectedAddr != 0 && !condition.ExpectedAddr.Valid() {
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
			if change.ExpectedOldAddr != 0 || (change.Operation == OperationPut && !change.NewRef.Valid()) || (change.Operation == OperationDelete && !change.NewRef.IsZero()) || (change.Operation != OperationPut && change.Operation != OperationDelete) {
				return ErrInvalid
			}
		case ProposalRelocation:
			if change.Operation != OperationRelocate || !change.NewRef.Valid() || !change.ExpectedOldAddr.Valid() || change.NewRef.Addr == change.ExpectedOldAddr {
				return ErrInvalid
			}
		}
		previous = change.RecordID
	}
	return nil
}

// ValidateProposal validates a proposal before it is admitted to the shared
// commit coordinator. ResolveGroup repeats this check at the Mapping boundary.
func ValidateProposal(proposal Proposal) error { return validateProposal(proposal) }

func validateResolvedPlan(plan GroupPlan) error {
	for _, proposal := range plan.Proposals {
		if !proposal.Accepted {
			if proposal.Kind != ProposalUserCommit || len(proposal.Changes) != 0 {
				return fmt.Errorf("rejected proposal has changes: %w", ErrInvalid)
			}
			continue
		}
		changes := make([]Change, len(proposal.Changes))
		for i, resolved := range proposal.Changes {
			changes[i] = resolved.Change
			if resolved.OldExists != !resolved.OldRef.IsZero() || resolved.OldExists && !resolved.OldRef.Valid() {
				return ErrInvalid
			}
			if proposal.Kind == ProposalUserCommit && !resolved.Apply {
				return ErrInvalid
			}
		}
		if err := validateProposal(Proposal{Kind: proposal.Kind, Changes: changes}); err != nil {
			return err
		}
	}
	return nil
}

type segmentLiveDelta struct {
	removeBytes, addBytes     uint64
	removeRecords, addRecords uint64
	lastChanged               model.CommitSeq
}

func applyPlanLiveStats(stats map[recordlog.SegmentID]SegmentLiveStats, first model.CommitSeq, plan GroupPlan) error {
	deltas := make(map[recordlog.SegmentID]segmentLiveDelta)
	seq := first
	for _, proposal := range plan.Proposals {
		if !proposal.Accepted {
			continue
		}
		for _, resolved := range proposal.Changes {
			if !resolved.Apply {
				continue
			}
			if resolved.OldExists {
				if !resolved.OldRef.Valid() {
					return ErrCorrupt
				}
				segment := resolved.OldRef.Addr.SegmentID()
				delta := deltas[segment]
				if delta.removeBytes > math.MaxUint64-uint64(resolved.OldRef.PhysicalSize) || delta.removeRecords == math.MaxUint64 {
					return ErrCorrupt
				}
				delta.removeBytes += uint64(resolved.OldRef.PhysicalSize)
				delta.removeRecords++
				delta.lastChanged = seq
				deltas[segment] = delta
			}
			if resolved.Change.Operation != OperationDelete {
				ref := resolved.Change.NewRef
				if !ref.Valid() {
					return ErrCorrupt
				}
				segment := ref.Addr.SegmentID()
				delta := deltas[segment]
				if delta.addBytes > math.MaxUint64-uint64(ref.PhysicalSize) || delta.addRecords == math.MaxUint64 {
					return ErrCorrupt
				}
				delta.addBytes += uint64(ref.PhysicalSize)
				delta.addRecords++
				delta.lastChanged = seq
				deltas[segment] = delta
			}
		}
		seq++
	}
	for segment, delta := range deltas {
		current := stats[segment]
		removeBytes, addBytes := delta.removeBytes, delta.addBytes
		if removeBytes >= addBytes {
			removeBytes -= addBytes
			addBytes = 0
		} else {
			addBytes -= removeBytes
			removeBytes = 0
		}
		removeRecords, addRecords := delta.removeRecords, delta.addRecords
		if removeRecords >= addRecords {
			removeRecords -= addRecords
			addRecords = 0
		} else {
			addRecords -= removeRecords
			removeRecords = 0
		}
		if current.LiveBytes < removeBytes || current.LiveRecords < removeRecords ||
			current.LiveBytes-removeBytes > math.MaxUint64-addBytes ||
			current.LiveRecords-removeRecords > math.MaxUint64-addRecords {
			return ErrCorrupt
		}
		delta.removeBytes, delta.addBytes = removeBytes, addBytes
		delta.removeRecords, delta.addRecords = removeRecords, addRecords
		deltas[segment] = delta
	}
	for segment, delta := range deltas {
		current := stats[segment]
		current.LiveBytes = current.LiveBytes - delta.removeBytes + delta.addBytes
		current.LiveRecords = current.LiveRecords - delta.removeRecords + delta.addRecords
		if current.LiveRecords == 0 {
			if current.LiveBytes != 0 {
				return ErrCorrupt
			}
			delete(stats, segment)
			continue
		}
		current.LastChangedCommitSeq = delta.lastChanged
		stats[segment] = current
	}
	return nil
}

func addLiveRef(stats map[recordlog.SegmentID]SegmentLiveStats, ref recordlog.RecordRef, seq model.CommitSeq) error {
	if !ref.Valid() {
		return ErrCorrupt
	}
	segment := ref.Addr.SegmentID()
	current := stats[segment]
	if current.LiveBytes > math.MaxUint64-uint64(ref.PhysicalSize) || current.LiveRecords == math.MaxUint64 {
		return ErrCorrupt
	}
	current.LiveBytes += uint64(ref.PhysicalSize)
	current.LiveRecords++
	current.LastChangedCommitSeq = seq
	stats[segment] = current
	return nil
}

func cloneLiveStats(stats map[recordlog.SegmentID]SegmentLiveStats) map[recordlog.SegmentID]SegmentLiveStats {
	result := make(map[recordlog.SegmentID]SegmentLiveStats, len(stats))
	for segment, value := range stats {
		result[segment] = value
	}
	return result
}
