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
	WalkRefs(context.Context, func(model.ID, recordlog.RecordRef) error) error
}

type IncrementalMapping interface {
	LookupRef(model.ID) (recordlog.RecordRef, bool, error)
	WalkChanges(context.Context, func(model.ID, recordlog.RecordRef, bool, recordlog.RecordRef, bool) error) error
}

type Inspector interface {
	Inspect(context.Context, recordlog.VAddr, uint32) (recordlog.RecordMetadata, []byte, error)
}

type SegmentScanner interface {
	ScanSegment(context.Context, recordlog.SegmentID, func(recordlog.AppendResult, []byte) error) error
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

// BuildIncremental derives the next exact table from the previous exact table
// and the folded base-to-candidate Mapping changes. If the base active Segment
// has since sealed, it is rebuilt by one sequential scan joined to current.
func BuildIncremental(ctx context.Context, current IncrementalMapping, records Inspector, scanner SegmentScanner, cache MetadataLookup, baseStats []storecatalog.SegmentStats, baseActive recordlog.SegmentID, files FileSet, maxValueSize, maxEntries uint64) ([]storecatalog.SegmentStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if current == nil || records == nil || scanner == nil || baseActive == 0 || files.Active == 0 || maxValueSize == 0 || maxEntries == 0 {
		return nil, base.ErrInvalidConfig
	}
	sealed, err := sealedFiles(files)
	if err != nil {
		return nil, err
	}
	stats := make(map[recordlog.SegmentID]storecatalog.SegmentStats, len(baseStats))
	var previous recordlog.SegmentID
	for _, stat := range baseStats {
		if stat.SegmentID == 0 || stat.SegmentID <= previous || (stat.LiveBytes == 0) != (stat.LiveRecords == 0) {
			return nil, base.ErrCorrupt
		}
		if _, ok := sealed[stat.SegmentID]; !ok {
			return nil, base.ErrCorrupt
		}
		if stat.LiveRecords != 0 {
			stats[stat.SegmentID] = stat
		}
		previous = stat.SegmentID
	}
	if baseActive != files.Active {
		if _, ok := sealed[baseActive]; !ok {
			return nil, base.ErrInvalidConfig
		}
	}
	err = current.WalkChanges(ctx, func(id model.ID, oldRef recordlog.RecordRef, oldExists bool, newRef recordlog.RecordRef, newExists bool) error {
		if oldExists {
			summary, isSealed, err := locate(oldRef.Addr, files.Active, sealed)
			if err != nil {
				return err
			}
			if !oldRef.Valid() {
				return base.ErrCorrupt
			}
			if isSealed && oldRef.Addr.SegmentID() != baseActive {
				if err := subtract(stats, summary.SegmentID, oldRef.PhysicalSize); err != nil {
					return err
				}
			}
		}
		if newExists {
			summary, isSealed, err := locate(newRef.Addr, files.Active, sealed)
			if err != nil {
				return err
			}
			if !newRef.Valid() {
				return base.ErrCorrupt
			}
			if isSealed && newRef.Addr.SegmentID() != baseActive {
				return add(stats, summary.SegmentID, newRef.PhysicalSize)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if baseActive != files.Active {
		delete(stats, baseActive)
		err = scanner.ScanSegment(ctx, baseActive, func(scanned recordlog.AppendResult, payload []byte) error {
			typ, err := recordcodec.TypeOf(payload)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			if typ != recordcodec.RecordTypePut {
				return nil
			}
			put, err := recordcodec.DecodePut(payload, maxValueSize)
			if err != nil {
				return errors.Join(base.ErrCorrupt, err)
			}
			ref, exists, err := current.LookupRef(put.RecordID)
			if err != nil {
				return err
			}
			if !exists || ref.Addr != scanned.Addr {
				return nil
			}
			physicalSize := scanned.End.Offset - scanned.Addr.Offset()
			if !scanned.Addr.MatchesPhysicalSize(physicalSize) || ref.PhysicalSize != physicalSize {
				return base.ErrCorrupt
			}
			return add(stats, baseActive, physicalSize)
		})
		if err != nil {
			return nil, err
		}
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

func inspectMetadata(ctx context.Context, id model.ID, addr recordlog.VAddr, records Inspector, cache MetadataLookup, active recordlog.SegmentID, sealed map[recordlog.SegmentID]recordlog.SegmentSummary, maxValueSize uint64) (uint32, recordlog.SegmentSummary, bool, error) {
	summary, isSealed, err := locate(addr, active, sealed)
	if err != nil {
		return 0, recordlog.SegmentSummary{}, false, err
	}
	var physicalSize uint32
	if cached, ok := lookupMetadata(cache, addr); ok {
		if cached.RecordID != id || !addr.MatchesPhysicalSize(cached.PhysicalSize) {
			return 0, recordlog.SegmentSummary{}, false, base.ErrCorrupt
		}
		physicalSize = cached.PhysicalSize
	} else {
		header, prefix, err := records.Inspect(ctx, addr, recordcodec.PutHeaderSize)
		if err != nil {
			return 0, recordlog.SegmentSummary{}, false, errors.Join(base.ErrCorrupt, err)
		}
		put, err := recordcodec.DecodePutMetadata(prefix, header.PayloadSize, maxValueSize)
		if err != nil || put.RecordID != id || header.Addr != addr || !addr.MatchesPhysicalSize(header.PhysicalSize) {
			return 0, recordlog.SegmentSummary{}, false, errors.Join(base.ErrCorrupt, err)
		}
		physicalSize = header.PhysicalSize
	}
	if isSealed && (addr.Offset() > summary.ValidEnd || physicalSize > summary.ValidEnd-addr.Offset()) {
		return 0, recordlog.SegmentSummary{}, false, base.ErrCorrupt
	}
	return physicalSize, summary, isSealed, nil
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

func subtract(stats map[recordlog.SegmentID]storecatalog.SegmentStats, id recordlog.SegmentID, physicalSize uint32) error {
	stat, ok := stats[id]
	if !ok || stat.LiveBytes < uint64(physicalSize) || stat.LiveRecords == 0 {
		return base.ErrCorrupt
	}
	stat.LiveBytes -= uint64(physicalSize)
	stat.LiveRecords--
	if stat.LiveBytes == 0 && stat.LiveRecords == 0 {
		delete(stats, id)
		return nil
	}
	if stat.LiveBytes == 0 || stat.LiveRecords == 0 {
		return base.ErrCorrupt
	}
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

func lookupMetadata(cache MetadataLookup, addr recordlog.VAddr) (recordmeta.Metadata, bool) {
	if cache == nil {
		return recordmeta.Metadata{}, false
	}
	return cache.Lookup(addr)
}
