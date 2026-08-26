package storecatalog

import (
	"fmt"
	"math"
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
	manifest, err := Load(root)
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

func (m *Manager) RemoveRecordLogSegment(expect uint64, sealed recordlog.SegmentSummary) (recordlog.CatalogSnapshot, error) {
	current := m.Snapshot()
	if current.Generation != expect {
		return recordlog.CatalogSnapshot{}, fmt.Errorf("expected generation %d, current %d: %w", expect, current.Generation, ErrConflict)
	}
	replayStart := current.ReplayStart
	if replayStart.SegmentID == sealed.SegmentID {
		if replayStart.Offset != sealed.ValidEnd {
			return recordlog.CatalogSnapshot{}, ErrInvalid
		}
		successor := current.ActiveDataSegmentID
		for _, summary := range current.SealedDataSegments {
			if summary.SegmentID > sealed.SegmentID && summary.SegmentID < successor {
				successor = summary.SegmentID
			}
		}
		var err error
		replayStart, err = recordlog.NewLogPos(successor, recordlog.SegmentHeaderSize)
		if err != nil {
			return recordlog.CatalogSnapshot{}, err
		}
	}
	installed, err := m.InstallDataRetire(expect, DataRetire{Source: sealed, CoveredCommitSeq: current.CoveredCommitSeq, ReplayStart: replayStart})
	if err != nil {
		return recordlog.CatalogSnapshot{}, err
	}
	return recordLogSnapshot(installed), nil
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

func (m *Manager) InstallMappingRewrite(expect uint64, update MappingRewrite) (Manifest, error) {
	return m.install(expect, func(next *Manifest) error {
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
	})
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
	CoveredCommitSeq       model.CommitSeq
	ReplayStart            recordlog.LogPos
	ReservedIDHigh         uint64
	ReservedBatchIDHigh    uint64
	IssuedBatchIDHighAtCut uint64
	OpenBatchIDsAtCut      []model.BatchID
	StatsCoveredCommitSeq  model.CommitSeq
	SegmentStats           []SegmentStats
}

func (m *Manager) InstallCheckpoint(expect uint64, update Checkpoint) (Manifest, error) {
	return m.install(expect, func(next *Manifest) error {
		if update.CoveredCommitSeq < next.CoveredCommitSeq || update.ReplayStart.Compare(next.ReplayStart) < 0 || update.ReservedIDHigh < next.ReservedIDHigh || update.ReservedBatchIDHigh < next.ReservedBatchIDHigh || update.IssuedBatchIDHighAtCut < next.IssuedBatchIDHighAtCut {
			return ErrInvalid
		}
		next.MappingRoot = update.MappingRoot
		next.CoveredCommitSeq = update.CoveredCommitSeq
		next.ReplayStart = update.ReplayStart
		next.ReservedIDHigh = update.ReservedIDHigh
		next.ReservedBatchIDHigh = update.ReservedBatchIDHigh
		next.IssuedBatchIDHighAtCut = update.IssuedBatchIDHighAtCut
		next.OpenBatchIDsAtCut = append([]model.BatchID(nil), update.OpenBatchIDsAtCut...)
		next.StatsCoveredCommitSeq = update.StatsCoveredCommitSeq
		next.SegmentStats = append([]SegmentStats(nil), update.SegmentStats...)
		return nil
	})
}

type DataRetire struct {
	Source           DataSegmentSummary
	CoveredCommitSeq model.CommitSeq
	ReplayStart      recordlog.LogPos
}

func (m *Manager) InstallDataRetire(expect uint64, update DataRetire) (Manifest, error) {
	return m.install(expect, func(next *Manifest) error {
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
		// SegmentStats is a sparse exact table for the same Mapping cut:
		// absence means zero live records. A present non-zero entry forbids
		// retirement; an explicit zero remains valid but is not emitted by the
		// current checkpoint builder.
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
		if next.ReplayStart.SegmentID != update.Source.SegmentID {
			if update.ReplayStart != next.ReplayStart {
				return ErrInvalid
			}
		} else {
			if next.ReplayStart.Offset != update.Source.ValidEnd {
				return ErrInvalid
			}
			successor := next.ActiveDataSegmentID
			for _, summary := range next.SealedDataSegments {
				if summary.SegmentID > update.Source.SegmentID && summary.SegmentID < successor {
					successor = summary.SegmentID
				}
			}
			want, err := recordlog.NewLogPos(successor, recordlog.SegmentHeaderSize)
			if err != nil || update.ReplayStart != want {
				return ErrInvalid
			}
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
	})
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
