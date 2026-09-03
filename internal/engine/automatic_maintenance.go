package engine

import (
	"context"
	"errors"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapstore"
)

func (s *Store) scheduleAutomaticMaintenance() {
	config := s.maintenance.config
	if !config.Enabled {
		return
	}
	if !config.DisableSegmentGC && s.maintenance.autoSegmentRunning.CompareAndSwap(false, true) {
		go func() {
			defer s.maintenance.autoSegmentRunning.Store(false)
			_, _, err := s.CompactNextSegment(context.Background(), config.SegmentPolicy)
			if err != nil && !errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, base.ErrInsufficientSpace) {
				s.metrics.maintenanceAutomaticFailed.Add(1)
			}
		}()
	}
	if !config.DisableMappingGC && s.maintenance.autoMappingRunning.CompareAndSwap(false, true) {
		go func() {
			defer s.maintenance.autoMappingRunning.Store(false)
			needed, err := s.mappingGCNeeded(context.Background(), config)
			if err != nil {
				if !errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, base.ErrConflict) && !errors.Is(err, mapstore.ErrRecoveryRequired) {
					s.metrics.maintenanceAutomaticFailed.Add(1)
				}
				return
			}
			if !needed {
				return
			}
			if err := s.CompactMapping(context.Background()); err != nil {
				if !errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, base.ErrConflict) {
					s.metrics.maintenanceAutomaticFailed.Add(1)
				}
				return
			}
			s.maintenance.lastMappingGCUnixNano.Store(s.maintenanceNow().UnixNano())
		}()
	}
}

func (s *Store) mappingGCNeeded(ctx context.Context, config MaintenanceConfig) (bool, error) {
	if err := s.beginOperation(); err != nil {
		return false, err
	}
	defer s.endOperation()
	last := s.maintenance.lastMappingGCUnixNano.Load()
	if last != 0 && s.maintenanceNow().Sub(time.Unix(0, last)) < config.MappingMinInterval {
		return false, nil
	}
	usage, err := s.surveyMappingUsage(ctx)
	if err != nil {
		return false, err
	}
	garbage := usage.physicalBytes - usage.reachableBytes
	ratio := uint32(0)
	if usage.physicalBytes != 0 {
		ratio = uint32(garbage * uint64(compactionRatioScale) / usage.physicalBytes)
	}
	return garbage >= config.MappingMinReclaimableBytes && ratio >= config.MappingMinReclaimableRatioBasis, nil
}

func (s *Store) surveyMappingUsage(ctx context.Context) (*mappingUsage, error) {
	err := s.maintenance.scheduler.submit(ctx, maintenanceJobSpec{
		key: "mapping-survey", priority: maintenancePriorityMapping,
		resources: maintenanceHeavyIO | maintenanceRecoveryProtocol, preemptible: true,
		run: func(runCtx context.Context) error {
			published := s.PublishedState()
			if published == nil {
				return base.ErrInvalidConfig
			}
			view, err := s.core.mapping.CheckpointView()
			if err != nil {
				return err
			}
			if view.Root() != published.MappingRoot || view.Covered() != published.CoveredCommit {
				return base.ErrConflict
			}
			reachable, err := view.ReachableBytes(runCtx)
			if err != nil {
				return err
			}
			snapshot := s.core.publisher.SnapshotMapStore()
			if snapshot.Generation != published.Generation || snapshot.Root != published.MappingRoot {
				return base.ErrConflict
			}
			report, err := mapstore.VerifyFiles(runCtx, s.core.root, snapshot)
			if err != nil {
				return err
			}
			if current := s.PublishedState(); current == nil || current.Generation != published.Generation || current.MappingRoot != published.MappingRoot {
				return base.ErrConflict
			}
			if reachable > report.PhysicalBytes {
				return base.ErrCorrupt
			}
			usage := &mappingUsage{generation: published.Generation, root: published.MappingRoot, physicalBytes: report.PhysicalBytes, reachableBytes: reachable}
			s.maintenance.mappingUsage.Store(usage)
			s.metrics.mappingSurveyPhysicalBytes.Store(report.PhysicalBytes)
			s.metrics.mappingSurveyReachableBytes.Store(reachable)
			s.metrics.mappingSurveyGeneration.Store(published.Generation)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	usage := s.maintenance.mappingUsage.Load()
	if usage == nil {
		return nil, base.ErrConflict
	}
	return usage, nil
}

func (s *Store) startMappingUsageSurvey() {
	if !s.maintenance.mappingSurveyRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.maintenance.mappingSurveyRunning.Store(false)
		if err := s.beginOperation(); err != nil {
			return
		}
		defer s.endOperation()
		_, _ = s.surveyMappingUsage(context.Background())
	}()
}

func (s *Store) maintenanceNow() time.Time {
	if s.maintenance.gcNow != nil {
		return s.maintenance.gcNow()
	}
	return time.Now()
}
