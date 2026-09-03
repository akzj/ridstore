package engine

import (
	"context"
	"errors"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

func (s *Store) scheduleAutomaticMaintenance() {
	config := s.maintenance.config
	if !config.Enabled {
		return
	}
	if !config.DisableSegmentGC {
		if err := s.maintenance.scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceSegmentNextRequest, policy: normalizeCompactionPolicy(config.SegmentPolicy), automatic: true}); err != nil && !errors.Is(err, base.ErrClosed) {
			s.metrics.maintenanceAutomaticFailed.Add(1)
		}
	}
	if !config.DisableMappingGC {
		if err := s.maintenance.scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceMappingGCRequest, automatic: true}); err != nil && !errors.Is(err, base.ErrClosed) {
			s.metrics.maintenanceAutomaticFailed.Add(1)
		}
	}
}

func (s *Store) mappingGCNeeded(ctx context.Context, config MaintenanceConfig) (bool, error) {
	ctx, end, err := s.beginOperation(ctx)
	if err != nil {
		return false, err
	}
	defer end()
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
	result, err := s.maintenance.scheduler.Submit(ctx, maintenanceRequest{kind: maintenanceMappingSurveyRequest})
	if err != nil {
		return nil, err
	}
	usage := result.usage
	if usage == nil {
		return nil, base.ErrConflict
	}
	return usage, nil
}

func (s *Store) startMappingUsageSurvey() {
	_ = s.maintenance.scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceMappingSurveyRequest})
}

func (s *Store) maintenanceNow() time.Time {
	if s.maintenance.gcNow != nil {
		return s.maintenance.gcNow()
	}
	return time.Now()
}
