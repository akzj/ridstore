package storecatalog

import (
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

var _ recordlog.CatalogPort = (*Manager)(nil)
var _ mapstore.CatalogPort = (*Manager)(nil)

type Manager struct {
	mu      sync.Mutex
	root    string
	current Manifest
	hook    FaultHook
}

func OpenManager(root string, hook FaultHook) (*Manager, error) {
	manifest, err := LoadRecovering(root, hook)
	if err != nil {
		return nil, err
	}
	return NewManager(root, manifest, hook)
}

func NewManager(root string, current Manifest, hook FaultHook) (*Manager, error) {
	if root == "" {
		return nil, ErrInvalid
	}
	if err := Validate(current); err != nil {
		return nil, err
	}
	return &Manager{root: root, current: current.Clone(), hook: hook}, nil
}

func (m *Manager) Snapshot() Manifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.Clone()
}

func (m *Manager) SnapshotRecordLog() recordlog.CatalogSnapshot {
	current := m.Snapshot()
	return recordLogSnapshot(current)
}

func (m *Manager) SnapshotMapStore() mapstore.CatalogSnapshot {
	return mapStoreSnapshot(m.Snapshot())
}

func (m *Manager) InstallMapStoreRotation(expect uint64, sealed mapstore.SegmentRef, newActive, next model.MapSegmentID) (mapstore.CatalogSnapshot, error) {
	installed, err := m.InstallMapRotation(expect, MapRotation{
		SealedOld: MapSegmentSummary{SegmentID: sealed.SegmentID, ValidEnd: sealed.ValidEnd},
		NewActive: newActive, NextID: next,
	})
	if err != nil {
		return mapstore.CatalogSnapshot{}, err
	}
	return mapStoreSnapshot(installed), nil
}

func mapStoreSnapshot(current Manifest) mapstore.CatalogSnapshot {
	sealed := make([]mapstore.SegmentRef, len(current.SealedMapSegments))
	for index, summary := range current.SealedMapSegments {
		sealed[index] = mapstore.SegmentRef{SegmentID: summary.SegmentID, ValidEnd: summary.ValidEnd}
	}
	return mapstore.CatalogSnapshot{
		Generation: current.Generation, StoreID: mapstore.StoreID(current.StoreUUID), SegmentSize: uint32(current.HardLimits.SegmentSize),
		ActiveSegment: current.ActiveMapSegmentID, NextSegment: current.NextMapSegmentID, SealedSegments: sealed,
		Root: current.MappingRoot, Covered: current.CoveredCommitSeq,
	}
}

func (m Manifest) MapStoreSnapshot() mapstore.CatalogSnapshot { return mapStoreSnapshot(m) }

func recordLogSnapshot(current Manifest) recordlog.CatalogSnapshot {
	return recordlog.CatalogSnapshot{
		Generation: current.Generation, LogID: current.RecordLogID,
		SegmentSize: uint32(current.HardLimits.SegmentSize), MaxPayloadBytes: uint32(current.HardLimits.MaxRecordLogPayload),
		ActiveSegmentID: current.ActiveDataSegmentID, NextSegmentID: current.NextDataSegmentID,
		SealedSegments: append([]recordlog.SegmentSummary(nil), current.SealedDataSegments...),
	}
}

func (m Manifest) RecordLogSnapshot() recordlog.CatalogSnapshot { return recordLogSnapshot(m) }

func (m *Manager) InstallRecordLogRotation(expect uint64, sealed recordlog.SegmentSummary, newActive, next recordlog.SegmentID) (recordlog.CatalogSnapshot, error) {
	installed, err := m.InstallDataRotation(expect, DataRotation{SealedOld: sealed, NewActive: newActive, NextID: next})
	if err != nil {
		return recordlog.CatalogSnapshot{}, err
	}
	return recordLogSnapshot(installed), nil
}

// RemoveRecordLogSegment accepts a proof generation as a lower bound. Later
// generations are safe only after current Manifest state revalidates the same
// source and its checkpoint-derived zero-live condition.
func (m *Manager) RemoveRecordLogSegment(minimumGeneration uint64, sealed recordlog.SegmentSummary) (recordlog.CatalogSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current.Generation < minimumGeneration || m.current.Generation == math.MaxUint64 {
		return recordlog.CatalogSnapshot{}, fmt.Errorf("minimum generation %d, current %d: %w", minimumGeneration, m.current.Generation, ErrConflict)
	}
	next := m.current.Clone()
	next.Generation++
	if err := applyDataRetire(&next, DataRetire{
		Source: sealed, CoveredCommitSeq: m.current.CoveredCommitSeq, ReplayStart: m.current.ReplayStart,
	}); err != nil {
		return recordlog.CatalogSnapshot{}, err
	}
	if err := Validate(next); err != nil {
		return recordlog.CatalogSnapshot{}, err
	}
	if err := Install(m.root, next, m.hook); err != nil {
		return recordlog.CatalogSnapshot{}, err
	}
	m.current = next.Clone()
	return recordLogSnapshot(next), nil
}

type DataRotation struct {
	SealedOld DataSegmentSummary
	NewActive recordlog.SegmentID
	NextID    recordlog.SegmentID
}

func (m *Manager) InstallDataRotation(expect uint64, update DataRotation) (Manifest, error) {
	return m.install(expect, func(next *Manifest) error {
		if update.SealedOld.SegmentID != next.ActiveDataSegmentID || update.NewActive != next.NextDataSegmentID || update.NextID != update.NewActive+1 || update.NewActive == recordlog.SegmentID(math.MaxUint32) {
			return ErrInvalid
		}
		next.SealedDataSegments = append(next.SealedDataSegments, update.SealedOld)
		sortDataSegments(next.SealedDataSegments)
		next.ActiveDataSegmentID = update.NewActive
		next.NextDataSegmentID = update.NextID
		return nil
	})
}

type MapRotation struct {
	SealedOld MapSegmentSummary
	NewActive model.MapSegmentID
	NextID    model.MapSegmentID
}

// MappingRewrite atomically replaces the complete Mapping file set while
// preserving the logical checkpoint cut and every non-Mapping Manifest field.
type MappingRewrite struct {
	SealedSegments []MapSegmentSummary
	ActiveSegment  model.MapSegmentID
	NextSegment    model.MapSegmentID
	Root           model.MapAddr
	Covered        model.CommitSeq
}

func (m *Manager) InstallMappingRewrite(base Manifest, update MappingRewrite) (Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !checkpointBaseCompatible(base, m.current) {
		return Manifest{}, fmt.Errorf("mapping rewrite base generation %d, current %d: %w", base.Generation, m.current.Generation, ErrConflict)
	}
	if m.current.Generation == math.MaxUint64 {
		return Manifest{}, ErrInvalid
	}
	next := m.current.Clone()
	next.Generation++
	if err := func(next *Manifest) error {
		if update.Covered != next.CoveredCommitSeq || update.ActiveSegment == 0 || update.ActiveSegment == model.MapSegmentID(math.MaxUint32) ||
			update.NextSegment != update.ActiveSegment+1 {
			return ErrInvalid
		}
		first := update.ActiveSegment
		if len(update.SealedSegments) != 0 {
			first = update.SealedSegments[0].SegmentID
		}
		if first != next.NextMapSegmentID {
			return ErrInvalid
		}
		want := first
		for _, summary := range update.SealedSegments {
			if summary.SegmentID != want || summary.ValidEnd < mapstore.SegmentHeaderSize || summary.ValidEnd > uint32(next.HardLimits.SegmentSize)-mapstore.SegmentFooterSize || summary.ValidEnd%mapstore.Alignment != 0 {
				return ErrInvalid
			}
			want++
		}
		if want != update.ActiveSegment || update.Root == 0 && len(update.SealedSegments) != 0 {
			return ErrInvalid
		}
		next.SealedMapSegments = append([]MapSegmentSummary(nil), update.SealedSegments...)
		next.ActiveMapSegmentID = update.ActiveSegment
		next.NextMapSegmentID = update.NextSegment
		next.MappingRoot = update.Root
		return nil
	}(&next); err != nil {
		return Manifest{}, err
	}
	if err := Validate(next); err != nil {
		return Manifest{}, err
	}
	if err := Install(m.root, next, m.hook); err != nil {
		return Manifest{}, err
	}
	m.current = next.Clone()
	return next.Clone(), nil
}

func (m *Manager) InstallMapRotation(expect uint64, update MapRotation) (Manifest, error) {
	return m.install(expect, func(next *Manifest) error {
		if update.SealedOld.SegmentID != next.ActiveMapSegmentID || update.NewActive != next.NextMapSegmentID || update.NextID != update.NewActive+1 || update.NewActive == model.MapSegmentID(math.MaxUint32) {
			return ErrInvalid
		}
		next.SealedMapSegments = append(next.SealedMapSegments, update.SealedOld)
		next.ActiveMapSegmentID = update.NewActive
		next.NextMapSegmentID = update.NextID
		return nil
	})
}

type Checkpoint struct {
	MappingRoot            model.MapAddr
	MappingEntryCount      uint64
	CoveredCommitSeq       model.CommitSeq
	ReplayStart            recordlog.LogPos
	ReservedIDHigh         uint64
	ReservedBatchIDHigh    uint64
	IssuedBatchIDHighAtCut uint64
	OpenBatchIDsAtCut      []model.BatchID
	StatsCoveredCommitSeq  model.CommitSeq
	SegmentStats           []SegmentStats
}

func (m *Manager) InstallCheckpoint(base Manifest, update Checkpoint) (Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !checkpointBaseCompatible(base, m.current) {
		return Manifest{}, fmt.Errorf("checkpoint base generation %d, current %d: %w", base.Generation, m.current.Generation, ErrConflict)
	}
	if m.current.Generation == math.MaxUint64 {
		return Manifest{}, ErrInvalid
	}
	next := m.current.Clone()
	next.Generation++
	if err := applyCheckpoint(&next, update); err != nil {
		return Manifest{}, err
	}
	if err := Validate(next); err != nil {
		return Manifest{}, err
	}
	if err := Install(m.root, next, m.hook); err != nil {
		return Manifest{}, err
	}
	m.current = next.Clone()
	return next.Clone(), nil
}

func applyCheckpoint(next *Manifest, update Checkpoint) error {
	if update.CoveredCommitSeq < next.CoveredCommitSeq || update.ReplayStart.Compare(next.ReplayStart) < 0 || update.ReservedIDHigh < next.ReservedIDHigh || update.ReservedBatchIDHigh < next.ReservedBatchIDHigh || update.IssuedBatchIDHighAtCut < next.IssuedBatchIDHighAtCut {
		return ErrInvalid
	}
	next.MappingRoot = update.MappingRoot
	next.MappingEntryCount = update.MappingEntryCount
	next.CoveredCommitSeq = update.CoveredCommitSeq
	next.ReplayStart = update.ReplayStart
	next.ReservedIDHigh = update.ReservedIDHigh
	next.ReservedBatchIDHigh = update.ReservedBatchIDHigh
	next.IssuedBatchIDHighAtCut = update.IssuedBatchIDHighAtCut
	next.OpenBatchIDsAtCut = append([]model.BatchID(nil), update.OpenBatchIDsAtCut...)
	next.StatsCoveredCommitSeq = update.StatsCoveredCommitSeq
	next.SegmentStats = append([]SegmentStats(nil), update.SegmentStats...)
	return nil
}

// checkpointBaseCompatible permits rebasing only over append-only Data
// rotations. Every Mapping and checkpoint field, and the existing Data file
// prefix, must still match the snapshot used to build the candidate.
func checkpointBaseCompatible(base, current Manifest) bool {
	if base.Generation == 0 || current.Generation < base.Generation {
		return false
	}
	left, right := base.Clone(), current.Clone()
	left.Generation, right.Generation = 0, 0
	left.ActiveDataSegmentID, right.ActiveDataSegmentID = 0, 0
	left.NextDataSegmentID, right.NextDataSegmentID = 0, 0
	left.SealedDataSegments, right.SealedDataSegments = nil, nil
	if !reflect.DeepEqual(left, right) || len(current.SealedDataSegments) < len(base.SealedDataSegments) {
		return false
	}
	for index := range base.SealedDataSegments {
		if base.SealedDataSegments[index] != current.SealedDataSegments[index] {
			return false
		}
	}
	active, next := base.ActiveDataSegmentID, base.NextDataSegmentID
	for _, sealed := range current.SealedDataSegments[len(base.SealedDataSegments):] {
		if sealed.SegmentID != active || next != active+1 {
			return false
		}
		active, next = next, next+1
	}
	return current.ActiveDataSegmentID == active && current.NextDataSegmentID == next
}

type DataRetire struct {
	Source           DataSegmentSummary
	CoveredCommitSeq model.CommitSeq
	ReplayStart      recordlog.LogPos
}

func (m *Manager) InstallDataRetire(expect uint64, update DataRetire) (Manifest, error) {
	return m.install(expect, func(next *Manifest) error { return applyDataRetire(next, update) })
}

func applyDataRetire(next *Manifest, update DataRetire) error {
	if update.CoveredCommitSeq != next.CoveredCommitSeq || update.Source.SegmentID == next.ActiveDataSegmentID {
		return ErrInvalid
	}
	index := -1
	for i, summary := range next.SealedDataSegments {
		if summary == update.Source {
			index = i
			break
		}
	}
	if index < 0 {
		return ErrInvalid
	}
	if !StatsKnownForSegment(next.ReplayStart, update.Source) {
		return ErrInvalid
	}
	// For a Segment strictly behind ReplayStart, SegmentStats is a sparse
	// exact table for the same Mapping cut: absence means zero live records.
	// A present non-zero entry forbids retirement; an explicit zero remains
	// valid but is not emitted by the current checkpoint builder.
	zeroStats := true
	for _, stat := range next.SegmentStats {
		if stat.SegmentID == update.Source.SegmentID {
			zeroStats = stat.LiveBytes == 0 && stat.LiveRecords == 0
			break
		}
	}
	if !zeroStats {
		return ErrInvalid
	}
	if update.ReplayStart != next.ReplayStart {
		return ErrInvalid
	}
	next.SealedDataSegments = append(next.SealedDataSegments[:index:index], next.SealedDataSegments[index+1:]...)
	stats := next.SegmentStats[:0:0]
	for _, stat := range next.SegmentStats {
		if stat.SegmentID != update.Source.SegmentID {
			stats = append(stats, stat)
		}
	}
	next.SegmentStats = stats
	next.ReplayStart = update.ReplayStart
	return nil
}

// StatsKnownForSegment reports whether a sealed Segment is strictly behind
// the checkpoint replay boundary. Equality remains unknown because a Segment
// may have rotated immediately after the cut that built the Stats table.
func StatsKnownForSegment(replayStart recordlog.LogPos, segment DataSegmentSummary) bool {
	return (recordlog.LogPos{SegmentID: segment.SegmentID, Offset: segment.ValidEnd}).Compare(replayStart) < 0
}

func (m *Manager) install(expect uint64, mutate func(*Manifest) error) (Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if expect != m.current.Generation {
		return Manifest{}, fmt.Errorf("expected generation %d, current %d: %w", expect, m.current.Generation, ErrConflict)
	}
	if m.current.Generation == math.MaxUint64 {
		return Manifest{}, ErrInvalid
	}
	next := m.current.Clone()
	next.Generation++
	if err := mutate(&next); err != nil {
		return Manifest{}, err
	}
	if err := Validate(next); err != nil {
		return Manifest{}, err
	}
	if err := Install(m.root, next, m.hook); err != nil {
		return Manifest{}, err
	}
	m.current = next.Clone()
	return next.Clone(), nil
}
