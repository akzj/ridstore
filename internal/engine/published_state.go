package engine

import (
	"sync"
	"sync/atomic"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

// PublishedState is the immutable logical view used as the common input for
// background COW builders. Hot active-segment/delta state remains outside it.
type PublishedState struct {
	Manifest      storecatalog.Manifest
	Generation    uint64
	MappingRoot   model.MapAddr
	CoveredCommit model.CommitSeq
}

// PublishCoordinator is the single runtime path for durable Catalog changes.
// It publishes the corresponding immutable logical view only after the
// Catalog install succeeds.
type PublishCoordinator struct {
	mu        sync.Mutex
	catalog   *storecatalog.Manager
	published atomic.Pointer[PublishedState]
}

var _ recordlog.CatalogPort = (*PublishCoordinator)(nil)
var _ mapstore.CatalogPort = (*PublishCoordinator)(nil)

func newPublishCoordinator(catalog *storecatalog.Manager) *PublishCoordinator {
	return &PublishCoordinator{catalog: catalog}
}

func publishedState(manifest storecatalog.Manifest) *PublishedState {
	return &PublishedState{Manifest: manifest.Clone(), Generation: manifest.Generation, MappingRoot: manifest.MappingRoot, CoveredCommit: manifest.CoveredCommitSeq}
}

func (p *PublishCoordinator) publish(manifest storecatalog.Manifest) {
	p.published.Store(publishedState(manifest))
}

func (p *PublishCoordinator) Snapshot() storecatalog.Manifest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.catalog.Snapshot()
}

func (p *PublishCoordinator) PublishedState() *PublishedState {
	return clonePublishedState(p.published.Load())
}

func (p *PublishCoordinator) SnapshotRecordLog() recordlog.CatalogSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.catalog.SnapshotRecordLog()
}

func (p *PublishCoordinator) InstallRecordLogRotation(expect uint64, sealed recordlog.SegmentSummary, active, next recordlog.SegmentID) (recordlog.CatalogSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, err := p.catalog.InstallRecordLogRotation(expect, sealed, active, next)
	if err == nil {
		p.publish(p.catalog.Snapshot())
	}
	return installed, err
}

func (p *PublishCoordinator) RemoveRecordLogSegment(minimumGeneration uint64, sealed recordlog.SegmentSummary) (recordlog.CatalogSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, err := p.catalog.RemoveRecordLogSegment(minimumGeneration, sealed)
	if err == nil {
		p.publish(p.catalog.Snapshot())
	}
	return installed, err
}

func (p *PublishCoordinator) SnapshotMapStore() mapstore.CatalogSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.catalog.SnapshotMapStore()
}

func (p *PublishCoordinator) InstallMapStoreRotation(expect uint64, sealed mapstore.SegmentRef, active, next model.MapSegmentID) (mapstore.CatalogSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, err := p.catalog.InstallMapStoreRotation(expect, sealed, active, next)
	if err == nil {
		p.publish(p.catalog.Snapshot())
	}
	return installed, err
}

func (p *PublishCoordinator) InstallCheckpoint(base storecatalog.Manifest, update storecatalog.Checkpoint) (storecatalog.Manifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, err := p.catalog.InstallCheckpoint(base, update)
	if err == nil {
		p.publish(installed)
	}
	return installed, err
}

func (p *PublishCoordinator) InstallMappingRewrite(base storecatalog.Manifest, update storecatalog.MappingRewrite) (storecatalog.Manifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, err := p.catalog.InstallMappingRewrite(base, update)
	if err == nil {
		p.publish(installed)
	}
	return installed, err
}

// PrepareAndInstallMappingRewrite serializes the marker/promotion boundary
// with every other Catalog publisher. prepare runs after observing the exact
// base generation that InstallMappingRewrite will consume; it must contain
// only the bounded durable preparation, never the COW rebuild.
func (p *PublishCoordinator) PrepareAndInstallMappingRewrite(
	prepare func(storecatalog.Manifest) error,
	update storecatalog.MappingRewrite,
) (storecatalog.Manifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	base := p.catalog.Snapshot()
	if err := prepare(base); err != nil {
		return storecatalog.Manifest{}, err
	}
	installed, err := p.catalog.InstallMappingRewrite(base, update)
	if err == nil {
		p.publish(installed)
	}
	return installed, err
}

func (p *PublishCoordinator) ReserveCompactionSegments(expect uint64, count uint32) (storecatalog.Manifest, []recordlog.SegmentID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, ids, err := p.catalog.ReserveCompactionSegments(expect, count)
	if err == nil {
		p.publish(installed)
	}
	return installed, ids, err
}

func (p *PublishCoordinator) InstallCompactionOutputs(expect uint64, outputs []recordlog.SegmentSummary) (storecatalog.Manifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	installed, err := p.catalog.InstallCompactionOutputs(expect, outputs)
	if err == nil {
		p.publish(installed)
	}
	return installed, err
}

func (p *PublishCoordinator) InstallDataCompaction(expect uint64, inputs []storecatalog.DataSegmentSummary, covered model.CommitSeq, replayStart recordlog.LogPos) (storecatalog.Manifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state := p.published.Load(); state == nil || state.Generation != expect {
		return storecatalog.Manifest{}, storecatalog.ErrConflict
	}
	installed, err := p.catalog.InstallDataCompaction(expect, inputs, covered, replayStart)
	if err == nil {
		p.publish(installed)
	}
	return installed, err
}

func (p *PublishCoordinator) InstallDataRetire(expect uint64, update storecatalog.DataRetire) (storecatalog.Manifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state := p.published.Load(); state == nil || state.Generation != expect {
		return storecatalog.Manifest{}, storecatalog.ErrConflict
	}
	installed, err := p.catalog.InstallDataRetire(expect, update)
	if err == nil {
		p.publish(installed)
	}
	return installed, err
}

func (s *Store) PublishedState() *PublishedState {
	if s != nil && s.core.publisher != nil {
		return s.core.publisher.PublishedState()
	}
	return nil
}

func clonePublishedState(state *PublishedState) *PublishedState {
	if state == nil {
		return nil
	}
	copy := *state
	copy.Manifest = state.Manifest.Clone()
	return &copy
}

func (s *Store) catalogSnapshot() storecatalog.Manifest {
	if s.core.publisher != nil {
		if state := s.core.publisher.PublishedState(); state != nil {
			return state.Manifest
		}
	}
	return s.core.catalog.Snapshot()
}
