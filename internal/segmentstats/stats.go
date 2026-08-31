package segmentstats

import (
	"context"
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

type Mapping interface {
	WalkRefs(context.Context, func(model.ID, recordlog.RecordRef) error) error
}

type FileSet struct {
	Active recordlog.SegmentID
	Sealed []recordlog.SegmentSummary
}

// Build derives exact live statistics for one immutable Mapping checkpoint.
// Active-segment records are validated but omitted because the Manifest stats
// table describes only sealed segments.
func Build(ctx context.Context, current Mapping, files FileSet, maxEntries uint64) ([]storecatalog.SegmentStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if current == nil || files.Active == 0 || maxEntries == 0 {
		return nil, base.ErrInvalidConfig
	}
	sealed, err := sealedFiles(files)
	if err != nil {
		return nil, err
	}
	stats := make(map[recordlog.SegmentID]storecatalog.SegmentStats)
	err = current.WalkRefs(ctx, func(id model.ID, ref recordlog.RecordRef) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		summary, isSealed, err := locate(ref.Addr, files.Active, sealed)
		if err != nil {
			return err
		}
		if !ref.Valid() || isSealed && (ref.Addr.Offset() > summary.ValidEnd || ref.PhysicalSize > summary.ValidEnd-ref.Addr.Offset()) {
			return base.ErrCorrupt
		}
		if !isSealed {
			return nil
		}
		if err := add(stats, summary.SegmentID, ref.PhysicalSize); err != nil {
			return err
		}
		if uint64(len(stats)) > maxEntries {
			return base.ErrOverflow
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if uint64(len(stats)) > maxEntries {
		return nil, base.ErrOverflow
	}
	return sorted(stats), nil
}

func sealedFiles(files FileSet) (map[recordlog.SegmentID]recordlog.SegmentSummary, error) {
	sealed := make(map[recordlog.SegmentID]recordlog.SegmentSummary, len(files.Sealed))
	var previous recordlog.SegmentID
	for _, summary := range files.Sealed {
		regular := summary.SegmentID < files.Active
		compacted := recordlog.IsCompactionSegment(summary.SegmentID)
		if summary.SegmentID == 0 || (!regular && !compacted) || summary.SegmentID <= previous {
			return nil, base.ErrInvalidConfig
		}
		sealed[summary.SegmentID] = summary
		previous = summary.SegmentID
	}
	return sealed, nil
}

func locate(addr recordlog.VAddr, active recordlog.SegmentID, sealed map[recordlog.SegmentID]recordlog.SegmentSummary) (recordlog.SegmentSummary, bool, error) {
	summary, isSealed := sealed[addr.SegmentID()]
	if !isSealed && addr.SegmentID() != active {
		return recordlog.SegmentSummary{}, false, base.ErrCorrupt
	}
	return summary, isSealed, nil
}

func add(stats map[recordlog.SegmentID]storecatalog.SegmentStats, id recordlog.SegmentID, physicalSize uint32) error {
	stat := stats[id]
	if stat.SegmentID == 0 {
		stat.SegmentID = id
	}
	if stat.LiveBytes > math.MaxUint64-uint64(physicalSize) || stat.LiveRecords == math.MaxUint64 {
		return base.ErrOverflow
	}
	stat.LiveBytes += uint64(physicalSize)
	stat.LiveRecords++
	stats[id] = stat
	return nil
}

func sorted(stats map[recordlog.SegmentID]storecatalog.SegmentStats) []storecatalog.SegmentStats {
	result := make([]storecatalog.SegmentStats, 0, len(stats))
	for _, stat := range stats {
		result = append(result, stat)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SegmentID < result[j].SegmentID })
	return result
}
