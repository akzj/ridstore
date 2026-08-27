package segmentstats

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/recordmeta"
	"github.com/akzj/ridstore/internal/storecatalog"
)

type Mapping interface {
	Walk(context.Context, func(model.ID, recordlog.VAddr) error) error
}

type Inspector interface {
	Inspect(context.Context, recordlog.VAddr, uint32) (recordlog.RecordMetadata, []byte, error)
}

type MetadataLookup interface {
	Lookup(recordlog.VAddr) (recordmeta.Metadata, bool)
}

type FileSet struct {
	Active recordlog.SegmentID
	Sealed []recordlog.SegmentSummary
}

// Build derives exact live statistics for one immutable Mapping checkpoint.
// Active-segment records are validated but omitted because the Manifest stats
// table describes only sealed segments.
func Build(ctx context.Context, current Mapping, records Inspector, cache MetadataLookup, files FileSet, maxValueSize, maxEntries uint64) ([]storecatalog.SegmentStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if current == nil || records == nil || files.Active == 0 || maxValueSize == 0 || maxEntries == 0 {
		return nil, base.ErrInvalidConfig
	}
	sealed := make(map[recordlog.SegmentID]recordlog.SegmentSummary, len(files.Sealed))
	var previous recordlog.SegmentID
	for _, summary := range files.Sealed {
		if summary.SegmentID == 0 || summary.SegmentID >= files.Active || summary.SegmentID <= previous {
			return nil, base.ErrInvalidConfig
		}
		sealed[summary.SegmentID] = summary
		previous = summary.SegmentID
	}
	stats := make(map[recordlog.SegmentID]storecatalog.SegmentStats)
	err := current.Walk(ctx, func(id model.ID, addr recordlog.VAddr) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		summary, isSealed := sealed[addr.SegmentID()]
		if !isSealed && addr.SegmentID() != files.Active {
			return base.ErrCorrupt
		}
		var physicalSize uint32
		if cached, ok := lookupMetadata(cache, addr); ok {
			if cached.RecordID != id || !addr.MatchesPhysicalSize(cached.PhysicalSize) {
				return base.ErrCorrupt
			}
			physicalSize = cached.PhysicalSize
		} else {
			header, prefix, err := records.Inspect(ctx, addr, recordcodec.PutHeaderSize)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			put, err := recordcodec.DecodePutMetadata(prefix, header.PayloadSize, maxValueSize)
			if err != nil || put.RecordID != id || header.Addr != addr || !addr.MatchesPhysicalSize(header.PhysicalSize) {
				return errors.Join(base.ErrCorrupt, err)
			}
			physicalSize = header.PhysicalSize
		}
		if !isSealed {
			return nil
		}
		if addr.Offset() > summary.ValidEnd || physicalSize > summary.ValidEnd-addr.Offset() {
			return base.ErrCorrupt
		}
		stat, exists := stats[summary.SegmentID]
		if !exists {
			if uint64(len(stats)) >= maxEntries {
				return base.ErrOverflow
			}
			stat.SegmentID = summary.SegmentID
		}
		if stat.LiveBytes > math.MaxUint64-uint64(physicalSize) || stat.LiveRecords == math.MaxUint64 {
			return base.ErrOverflow
		}
		stat.LiveBytes += uint64(physicalSize)
		stat.LiveRecords++
		stats[summary.SegmentID] = stat
		return nil
	})
	if err != nil {
		return nil, err
	}
	result := make([]storecatalog.SegmentStats, 0, len(stats))
	for _, stat := range stats {
		result = append(result, stat)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SegmentID < result[j].SegmentID })
	return result, nil
}

func lookupMetadata(cache MetadataLookup, addr recordlog.VAddr) (recordmeta.Metadata, bool) {
	if cache == nil {
		return recordmeta.Metadata{}, false
	}
	return cache.Lookup(addr)
}
