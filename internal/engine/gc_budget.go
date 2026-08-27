package engine

import (
	"context"
	"errors"
	"math"
	"math/bits"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type gcPacer struct {
	started time.Time
	copied  uint64
	rate    uint64
	now     func() time.Time
	wait    func(context.Context, time.Duration) error
}

func (s *Store) newGCPacer() gcPacer {
	now := s.gcNow
	if now == nil {
		now = time.Now
	}
	wait := s.gcWait
	if wait == nil {
		wait = waitContext
	}
	return gcPacer{started: now(), rate: s.gcBytesPerSecond, now: now, wait: wait}
}

func (p *gcPacer) pace(ctx context.Context, copied uint64) (time.Duration, error) {
	if copied == 0 || p.rate == 0 {
		return 0, nil
	}
	if p.copied > math.MaxUint64-copied {
		return 0, base.ErrOverflow
	}
	p.copied += copied
	seconds := p.copied / p.rate
	remainder := p.copied % p.rate
	if seconds > uint64(math.MaxInt64)/uint64(time.Second) {
		return 0, base.ErrOverflow
	}
	target := time.Duration(seconds) * time.Second
	if remainder != 0 {
		high, low := bits.Mul64(remainder, uint64(time.Second))
		nanos, _ := bits.Div64(high, low, p.rate)
		if nanos == 0 {
			nanos = 1
		}
		if nanos > uint64(math.MaxInt64)-uint64(target) {
			return 0, base.ErrOverflow
		}
		target += time.Duration(nanos)
	}
	delay := target - p.now().Sub(p.started)
	if delay <= 0 {
		return 0, nil
	}
	started := p.now()
	err := p.wait(ctx, delay)
	waited := p.now().Sub(started)
	if waited < 0 {
		waited = 0
	}
	return waited, err
}

func (s *Store) reserveGCCopy(ctx context.Context, manifest storecatalog.Manifest, source recordlog.SegmentSummary) (*spaceReservation, error) {
	if s.space == nil || s.gcMinFreeBytes == 0 {
		return nil, nil
	}
	if s.maxRelocationMutations == 0 || s.maxRelocationBytes == 0 {
		return nil, base.ErrInvalidConfig
	}
	// One CommitGroup and one BatchID reserve Record per source Record is the
	// safe upper bound when runtime limits isolate every live copy and the
	// allocator reserve size is one.
	commitPhysical, err := recordlog.PhysicalRecordSize(uint64(recordcodec.CommitGroupHeadSize + recordcodec.DescriptorHeadSize + recordcodec.MutationSize))
	if err != nil {
		return nil, err
	}
	reservePhysical, err := recordlog.PhysicalRecordSize(uint64(recordcodec.FixedRecordSize))
	if err != nil {
		return nil, err
	}
	controlBytes, ok := checkedGCMul(source.RecordCount, uint64(commitPhysical)+uint64(reservePhysical))
	if !ok {
		return nil, base.ErrOverflow
	}
	copyBytes := uint64(source.ValidEnd - recordlog.SegmentHeaderSize)
	rotationBytes, ok := checkedGCMul(manifest.HardLimits.SegmentSize, 2)
	if !ok {
		return nil, base.ErrOverflow
	}
	estimate, ok := checkedGCAdd(copyBytes, controlBytes, rotationBytes)
	if !ok {
		return nil, base.ErrOverflow
	}
	reservation, err := s.space.reserveWithMinimum(ctx, estimate, s.gcMinFreeBytes, false)
	if errors.Is(err, base.ErrInsufficientSpace) {
		s.metrics.gcSpaceRejections.Add(1)
	}
	return reservation, err
}

func (s *Store) reserveGCCheckpoint(ctx context.Context, manifest storecatalog.Manifest, entries uint64) (*spaceReservation, error) {
	if s.space == nil || s.gcMinFreeBytes == 0 {
		return nil, nil
	}
	perEntry, ok := checkedGCMul(uint64(mapstore.MaxLevel)+1, uint64(mapstore.DenseNodeSize))
	if !ok {
		return nil, base.ErrOverflow
	}
	nodeBytes, ok := checkedGCMul(entries, perEntry)
	if !ok {
		return nil, base.ErrOverflow
	}
	estimate, ok := checkedGCAdd(nodeBytes, manifest.HardLimits.SegmentSize)
	if !ok {
		return nil, base.ErrOverflow
	}
	reservation, err := s.space.reserveWithMinimum(ctx, estimate, s.gcMinFreeBytes, false)
	if errors.Is(err, base.ErrInsufficientSpace) {
		s.metrics.gcSpaceRejections.Add(1)
	}
	return reservation, err
}

func checkedGCMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func checkedGCAdd(values ...uint64) (uint64, bool) {
	var result uint64
	for _, value := range values {
		if result > math.MaxUint64-value {
			return 0, false
		}
		result += value
	}
	return result, true
}
