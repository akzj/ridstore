package format

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

const MaxManifestPayloadSize = 64 << 20

type HardLimits struct {
	SegmentSize, MaxValueSize, MaxBatchBytes              uint64
	MaxBatchMutations, MaxBatchConditions, MaxOpenBatches uint64
	IDReserveSize, BatchIDReserveSize                     uint64
}

type FileSummary struct {
	FileID                      uint32
	ValidEnd, FirstSeq, LastSeq uint64
}

type SegmentStatsEntry struct {
	SegmentID                        base.DataSegmentID
	ExactLiveBytes, ExactLiveRecords uint64
}

type Manifest struct {
	Generation, MaintenanceGeneration uint64
	StoreUUID                         base.StoreUUID
	HardLimits                        HardLimits
	NextDataSegmentID                 base.DataSegmentID
	NextMapSegmentID                  base.MapSegmentID
	ActiveDataSegmentID               base.DataSegmentID
	SealedDataSegments                []FileSummary
	ActiveMapSegmentID                base.MapSegmentID
	SealedMappingSegments             []FileSummary
	MappingRoot                       base.MapAddr
	CoveredCommitSeq                  base.CommitSeq
	CutFrameSeq                       base.FrameSeq
	ReplayStart                       base.LogPos
	ReservedIDHighExclusive           uint64
	ReservedBatchIDHighExclusive      uint64
	IssuedBatchIDHighExclusiveAtCut   uint64
	OpenBatchIDsAtCut                 []base.BatchID
	NextFrameSeq                      base.FrameSeq
	NextCommitSeq                     base.CommitSeq
	StatsCoveredCommitSeq             base.CommitSeq
	SegmentStats                      []SegmentStatsEntry
}

func EncodeManifest(m Manifest) ([]byte, error) {
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	tlvs := []TLV{
		requiredTLV(1, scalar16x2(FormatMajorVersion, FormatMinorVersion)),
		requiredTLV(2, encodeHardLimits(m.HardLimits)),
		requiredTLV(3, scalar32(uint32(m.NextDataSegmentID))),
		requiredTLV(4, scalar32(uint32(m.NextMapSegmentID))),
		requiredTLV(5, scalar32(uint32(m.ActiveDataSegmentID))),
		requiredTLV(6, encodeFileSummaries(m.SealedDataSegments)),
		requiredTLV(7, scalar32(uint32(m.ActiveMapSegmentID))),
		requiredTLV(8, encodeFileSummaries(m.SealedMappingSegments)),
		requiredTLV(9, scalar64(uint64(m.MappingRoot))),
		requiredTLV(10, scalar64(uint64(m.CoveredCommitSeq))),
		requiredTLV(11, scalar64(uint64(m.CutFrameSeq))),
		requiredTLV(12, scalar64(uint64(m.ReplayStart))),
		requiredTLV(13, scalar64(m.ReservedIDHighExclusive)),
		requiredTLV(14, scalar64(m.ReservedBatchIDHighExclusive)),
		requiredTLV(15, scalar64(m.IssuedBatchIDHighExclusiveAtCut)),
		requiredTLV(16, encodeBatchIDs(m.OpenBatchIDsAtCut)),
		requiredTLV(17, scalar64(uint64(m.NextFrameSeq))),
		requiredTLV(18, scalar64(uint64(m.NextCommitSeq))),
		requiredTLV(19, scalar64(m.MaintenanceGeneration)),
		requiredTLV(20, scalar64(uint64(m.StatsCoveredCommitSeq))),
		requiredTLV(21, encodeSegmentStats(m.SegmentStats)),
	}
	encoded, err := EncodeContainer(Container{Magic: ManifestMagic, Generation: m.Generation, StoreUUID: m.StoreUUID, TLVs: tlvs})
	if err != nil {
		return nil, err
	}
	if len(encoded)-ContainerHeaderSize > MaxManifestPayloadSize {
		return nil, fmt.Errorf("manifest payload exceeds format limit: %w", base.ErrInvalidConfig)
	}
	return encoded, nil
}

func DecodeManifest(src []byte) (Manifest, error) {
	container, err := DecodeContainer(src, ManifestMagic, MaxManifestPayloadSize)
	if err != nil {
		return Manifest{}, err
	}
	items := make(map[uint16][]byte, 21)
	for _, item := range container.TLVs {
		if item.Type > 21 {
			if item.Required {
				return Manifest{}, fmt.Errorf("required manifest TLV %d: %w", item.Type, base.ErrUnsupported)
			}
			continue
		}
		if !item.Required {
			return Manifest{}, corruptf("known manifest TLV %d is optional", item.Type)
		}
		items[item.Type] = item.Value
	}
	for typ := uint16(1); typ <= 21; typ++ {
		if _, ok := items[typ]; !ok {
			return Manifest{}, corruptf("missing manifest TLV %d", typ)
		}
	}
	major, minor, err := decode16x2(items[1])
	if err != nil {
		return Manifest{}, err
	}
	if major != FormatMajorVersion || minor > FormatMinorVersion {
		return Manifest{}, fmt.Errorf("manifest format version %d.%d: %w", major, minor, base.ErrUnsupported)
	}
	m := Manifest{Generation: container.Generation, StoreUUID: container.StoreUUID}
	if m.HardLimits, err = decodeHardLimits(items[2]); err != nil {
		return Manifest{}, err
	}
	var v32 uint32
	if v32, err = decode32(items[3]); err != nil {
		return Manifest{}, err
	}
	m.NextDataSegmentID = base.DataSegmentID(v32)
	if v32, err = decode32(items[4]); err != nil {
		return Manifest{}, err
	}
	m.NextMapSegmentID = base.MapSegmentID(v32)
	if v32, err = decode32(items[5]); err != nil {
		return Manifest{}, err
	}
	m.ActiveDataSegmentID = base.DataSegmentID(v32)
	if m.SealedDataSegments, err = decodeFileSummaries(items[6]); err != nil {
		return Manifest{}, err
	}
	if v32, err = decode32(items[7]); err != nil {
		return Manifest{}, err
	}
	m.ActiveMapSegmentID = base.MapSegmentID(v32)
	if m.SealedMappingSegments, err = decodeFileSummaries(items[8]); err != nil {
		return Manifest{}, err
	}
	var v64 uint64
	if v64, err = decode64(items[9]); err != nil {
		return Manifest{}, err
	}
	m.MappingRoot = base.MapAddr(v64)
	if v64, err = decode64(items[10]); err != nil {
		return Manifest{}, err
	}
	m.CoveredCommitSeq = base.CommitSeq(v64)
	if v64, err = decode64(items[11]); err != nil {
		return Manifest{}, err
	}
	m.CutFrameSeq = base.FrameSeq(v64)
	if v64, err = decode64(items[12]); err != nil {
		return Manifest{}, err
	}
	m.ReplayStart = base.LogPos(v64)
	if m.ReservedIDHighExclusive, err = decode64(items[13]); err != nil {
		return Manifest{}, err
	}
	if m.ReservedBatchIDHighExclusive, err = decode64(items[14]); err != nil {
		return Manifest{}, err
	}
	if m.IssuedBatchIDHighExclusiveAtCut, err = decode64(items[15]); err != nil {
		return Manifest{}, err
	}
	if m.OpenBatchIDsAtCut, err = decodeBatchIDs(items[16]); err != nil {
		return Manifest{}, err
	}
	if v64, err = decode64(items[17]); err != nil {
		return Manifest{}, err
	}
	m.NextFrameSeq = base.FrameSeq(v64)
	if v64, err = decode64(items[18]); err != nil {
		return Manifest{}, err
	}
	m.NextCommitSeq = base.CommitSeq(v64)
	if m.MaintenanceGeneration, err = decode64(items[19]); err != nil {
		return Manifest{}, err
	}
	if v64, err = decode64(items[20]); err != nil {
		return Manifest{}, err
	}
	m.StatsCoveredCommitSeq = base.CommitSeq(v64)
	if m.SegmentStats, err = decodeSegmentStats(items[21]); err != nil {
		return Manifest{}, err
	}
	if err := validateManifest(m); err != nil {
		return Manifest{}, corruptf("manifest fields: %v", err)
	}
	return m, nil
}

func validateManifest(m Manifest) error {
	if m.Generation == 0 || m.StoreUUID == (base.StoreUUID{}) || m.NextDataSegmentID == 0 || m.NextMapSegmentID == 0 ||
		m.ActiveDataSegmentID == 0 || m.ActiveMapSegmentID == 0 || m.NextFrameSeq == 0 || m.NextCommitSeq == 0 {
		return fmt.Errorf("manifest identity: %w", base.ErrInvalidConfig)
	}
	if err := ValidateHardLimits(m.HardLimits); err != nil {
		return err
	}
	if uint64(len(m.SealedDataSegments)) > math.MaxUint32 || uint64(len(m.SealedMappingSegments)) > math.MaxUint32 ||
		uint64(len(m.OpenBatchIDsAtCut)) > math.MaxUint32 || uint64(len(m.SegmentStats)) > math.MaxUint32 {
		return fmt.Errorf("manifest array count: %w", base.ErrInvalidConfig)
	}
	if err := validateFileSet(m.SealedDataSegments, uint32(m.ActiveDataSegmentID), uint32(m.NextDataSegmentID)); err != nil {
		return err
	}
	if err := validateFileSet(m.SealedMappingSegments, uint32(m.ActiveMapSegmentID), uint32(m.NextMapSegmentID)); err != nil {
		return err
	}
	if uint32(m.ActiveDataSegmentID) >= uint32(m.NextDataSegmentID) || uint32(m.ActiveMapSegmentID) >= uint32(m.NextMapSegmentID) {
		return fmt.Errorf("manifest next file ID: %w", base.ErrInvalidConfig)
	}
	if _, err := base.ParseLogPos(uint64(m.ReplayStart)); err != nil {
		return fmt.Errorf("replay start: %w", base.ErrInvalidConfig)
	}
	if err := validateManifestAddresses(m); err != nil {
		return err
	}
	if m.CoveredCommitSeq >= m.NextCommitSeq || m.CutFrameSeq >= m.NextFrameSeq || m.StatsCoveredCommitSeq != m.CoveredCommitSeq {
		return fmt.Errorf("manifest checkpoint sequence: %w", base.ErrInvalidConfig)
	}
	if m.ReservedIDHighExclusive == 0 || m.ReservedBatchIDHighExclusive == 0 || m.IssuedBatchIDHighExclusiveAtCut == 0 ||
		m.IssuedBatchIDHighExclusiveAtCut > m.ReservedBatchIDHighExclusive {
		return fmt.Errorf("manifest reserve high: %w", base.ErrInvalidConfig)
	}
	if uint64(len(m.OpenBatchIDsAtCut)) > m.HardLimits.MaxOpenBatches || !strictBatchIDs(m.OpenBatchIDsAtCut, m.IssuedBatchIDHighExclusiveAtCut) {
		return fmt.Errorf("manifest open batches: %w", base.ErrInvalidConfig)
	}
	dataIDs := map[uint32]struct{}{uint32(m.ActiveDataSegmentID): {}}
	for _, file := range m.SealedDataSegments {
		dataIDs[file.FileID] = struct{}{}
	}
	var previous uint32
	for _, stat := range m.SegmentStats {
		id := uint32(stat.SegmentID)
		if id == 0 || id <= previous || stat.ExactLiveBytes == 0 || stat.ExactLiveRecords == 0 {
			return fmt.Errorf("segment stats order/value: %w", base.ErrInvalidConfig)
		}
		if _, ok := dataIDs[id]; !ok {
			return fmt.Errorf("segment stats file: %w", base.ErrInvalidConfig)
		}
		previous = id
	}
	return nil
}

// ValidateHardLimits validates persisted limits, including the physical
// single-Segment capacity required by the append protocol.
func ValidateHardLimits(h HardLimits) error {
	values := [...]uint64{h.SegmentSize, h.MaxValueSize, h.MaxBatchBytes, h.MaxBatchMutations, h.MaxBatchConditions, h.MaxOpenBatches, h.IDReserveSize, h.BatchIDReserveSize}
	for _, value := range values {
		if value == 0 {
			return fmt.Errorf("zero hard limit: %w", base.ErrInvalidConfig)
		}
	}
	if h.SegmentSize > uint64(math.MaxUint32)+1 || h.MaxValueSize > h.MaxBatchBytes ||
		h.MaxBatchMutations > math.MaxUint32 || h.MaxBatchConditions > math.MaxUint32 || h.MaxOpenBatches > math.MaxUint32 {
		return fmt.Errorf("inconsistent hard limits: %w", base.ErrInvalidConfig)
	}
	if _, _, err := FramePayloadLimits(h); err != nil {
		return fmt.Errorf("inconsistent hard limits: %w", err)
	}
	return nil
}

// DataAppendCapacity is the maximum number of bytes one normal append may
// occupy in an otherwise empty Data Segment while retaining space for the
// terminal SegmentSeal and fixed footer.
func DataAppendCapacity(segmentSize uint64) (uint64, error) {
	const fixed = uint64(SegmentHeaderSize + SegmentFooterSize + FrameHeaderSize + 64)
	if segmentSize <= fixed {
		return 0, fmt.Errorf("data segment capacity: %w", base.ErrInvalidConfig)
	}
	return segmentSize - fixed, nil
}

// FramePayloadLimits derives the persisted Frame decoder limit and descriptor
// part size from HardLimits. It also proves that the largest legal Put and
// Commit/Relocation descriptor each fit wholly within one Data Segment.
func FramePayloadLimits(h HardLimits) (uint64, uint64, error) {
	appendCapacity, err := DataAppendCapacity(h.SegmentSize)
	if err != nil {
		return 0, 0, err
	}
	descriptorBytes, err := base.MulUint64(h.MaxBatchMutations, MutationEntrySize)
	if err != nil || descriptorBytes == 0 {
		return 0, 0, fmt.Errorf("descriptor payload capacity: %w", base.ErrInvalidConfig)
	}
	if appendCapacity <= FrameHeaderSize {
		return 0, 0, fmt.Errorf("frame payload capacity: %w", base.ErrInvalidConfig)
	}
	framePayloadCapacity := appendCapacity - FrameHeaderSize
	maxPart := descriptorBytes
	if maxPart > framePayloadCapacity {
		maxPart = framePayloadCapacity - framePayloadCapacity%MutationEntrySize
	}
	if maxPart < MutationEntrySize {
		return 0, 0, fmt.Errorf("descriptor part capacity: %w", base.ErrInvalidConfig)
	}
	largestDescriptor, err := DescriptorPhysicalSize(h.MaxBatchMutations, maxPart)
	if err != nil || largestDescriptor > appendCapacity {
		return 0, 0, fmt.Errorf("descriptor exceeds data segment: %w", base.ErrInvalidConfig)
	}
	largestPut, err := base.AddUint64(FrameHeaderSize, h.MaxValueSize)
	if err == nil {
		largestPut, err = base.Align8(largestPut)
	}
	if err != nil || largestPut > appendCapacity {
		return 0, 0, fmt.Errorf("put exceeds data segment: %w", base.ErrInvalidConfig)
	}
	maxFrame := h.MaxValueSize
	if maxPart > maxFrame {
		maxFrame = maxPart
	}
	if maxFrame < DescriptorSealSize {
		maxFrame = DescriptorSealSize
	}
	return maxFrame, maxPart, nil
}

func validateManifestAddresses(m Manifest) error {
	contentLimit := m.HardLimits.SegmentSize - SegmentFooterSize
	for _, file := range m.SealedDataSegments {
		if file.ValidEnd > contentLimit {
			return fmt.Errorf("sealed data extent exceeds segment: %w", base.ErrInvalidConfig)
		}
	}
	for _, file := range m.SealedMappingSegments {
		if file.ValidEnd > contentLimit {
			return fmt.Errorf("sealed mapping extent exceeds segment: %w", base.ErrInvalidConfig)
		}
	}

	replaySegment, replayOffset := m.ReplayStart.SegmentID(), uint64(m.ReplayStart.Offset())
	replayBound, replayFound := uint64(0), false
	if replaySegment == m.ActiveDataSegmentID {
		replayBound, replayFound = contentLimit, true
	} else {
		for _, file := range m.SealedDataSegments {
			if file.FileID == uint32(replaySegment) {
				replayBound, replayFound = file.ValidEnd, true
				break
			}
		}
	}
	if !replayFound || replayOffset > replayBound {
		return fmt.Errorf("replay start is outside manifest data files: %w", base.ErrInvalidConfig)
	}

	if m.MappingRoot == 0 {
		return nil
	}
	if !validMapAddr(m.MappingRoot) {
		return fmt.Errorf("mapping root: %w", base.ErrInvalidConfig)
	}
	rootSegment, rootOffset := m.MappingRoot.SegmentID(), uint64(m.MappingRoot.Offset())
	rootBound, rootFound := uint64(0), false
	if rootSegment == m.ActiveMapSegmentID {
		rootBound, rootFound = contentLimit, true
	} else {
		for _, file := range m.SealedMappingSegments {
			if file.FileID == uint32(rootSegment) {
				rootBound, rootFound = file.ValidEnd, true
				break
			}
		}
	}
	if !rootFound || rootOffset >= rootBound {
		return fmt.Errorf("mapping root is outside manifest mapping files: %w", base.ErrInvalidConfig)
	}
	return nil
}

func validateFileSet(files []FileSummary, activeID, nextID uint32) error {
	var previous uint32
	for _, file := range files {
		if file.FileID == 0 || file.FileID == activeID || file.FileID >= nextID || file.FileID <= previous || file.ValidEnd <= SegmentHeaderSize || file.ValidEnd > math.MaxUint32 || file.ValidEnd%8 != 0 || file.FirstSeq == 0 || file.LastSeq < file.FirstSeq {
			return fmt.Errorf("file summary: %w", base.ErrInvalidConfig)
		}
		previous = file.FileID
	}
	return nil
}

func requiredTLV(typ uint16, value []byte) TLV { return TLV{Type: typ, Required: true, Value: value} }
func scalar32(v uint32) []byte                 { b := make([]byte, 4); binary.LittleEndian.PutUint32(b, v); return b }
func scalar64(v uint64) []byte                 { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, v); return b }
func scalar16x2(a, b uint16) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint16(out, a)
	binary.LittleEndian.PutUint16(out[2:], b)
	return out
}
func decode32(b []byte) (uint32, error) {
	if len(b) != 4 {
		return 0, corruptf("uint32 TLV length")
	}
	return binary.LittleEndian.Uint32(b), nil
}
func decode64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, corruptf("uint64 TLV length")
	}
	return binary.LittleEndian.Uint64(b), nil
}
func decode16x2(b []byte) (uint16, uint16, error) {
	if len(b) != 4 {
		return 0, 0, corruptf("version TLV length")
	}
	return binary.LittleEndian.Uint16(b), binary.LittleEndian.Uint16(b[2:]), nil
}

func encodeHardLimits(h HardLimits) []byte {
	out := make([]byte, 64)
	values := [...]uint64{h.SegmentSize, h.MaxValueSize, h.MaxBatchBytes, h.MaxBatchMutations, h.MaxBatchConditions, h.MaxOpenBatches, h.IDReserveSize, h.BatchIDReserveSize}
	for i, v := range values {
		binary.LittleEndian.PutUint64(out[i*8:], v)
	}
	return out
}
func decodeHardLimits(b []byte) (HardLimits, error) {
	if len(b) != 64 {
		return HardLimits{}, corruptf("hard limits length")
	}
	v := [8]uint64{}
	for i := range v {
		v[i] = binary.LittleEndian.Uint64(b[i*8:])
	}
	return HardLimits{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]}, nil
}

func encodeFileSummaries(files []FileSummary) []byte {
	out := make([]byte, 8+len(files)*32)
	binary.LittleEndian.PutUint32(out, uint32(len(files)))
	for i, f := range files {
		o := 8 + i*32
		binary.LittleEndian.PutUint32(out[o:], f.FileID)
		binary.LittleEndian.PutUint64(out[o+8:], f.ValidEnd)
		binary.LittleEndian.PutUint64(out[o+16:], f.FirstSeq)
		binary.LittleEndian.PutUint64(out[o+24:], f.LastSeq)
	}
	return out
}
func decodeFileSummaries(b []byte) ([]FileSummary, error) {
	count, err := arrayCount(b, 32)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]FileSummary, count)
	for i := range out {
		o := 8 + i*32
		if binary.LittleEndian.Uint32(b[o+4:o+8]) != 0 {
			return nil, corruptf("file summary flags")
		}
		out[i] = FileSummary{binary.LittleEndian.Uint32(b[o:]), binary.LittleEndian.Uint64(b[o+8:]), binary.LittleEndian.Uint64(b[o+16:]), binary.LittleEndian.Uint64(b[o+24:])}
	}
	return out, nil
}
func encodeBatchIDs(ids []base.BatchID) []byte {
	out := make([]byte, 8+len(ids)*8)
	binary.LittleEndian.PutUint32(out, uint32(len(ids)))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(out[8+i*8:], uint64(id))
	}
	return out
}
func decodeBatchIDs(b []byte) ([]base.BatchID, error) {
	count, err := arrayCount(b, 8)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]base.BatchID, count)
	for i := range out {
		out[i] = base.BatchID(binary.LittleEndian.Uint64(b[8+i*8:]))
	}
	return out, nil
}
func encodeSegmentStats(stats []SegmentStatsEntry) []byte {
	out := make([]byte, 8+len(stats)*24)
	binary.LittleEndian.PutUint32(out, uint32(len(stats)))
	for i, s := range stats {
		o := 8 + i*24
		binary.LittleEndian.PutUint32(out[o:], uint32(s.SegmentID))
		binary.LittleEndian.PutUint64(out[o+8:], s.ExactLiveBytes)
		binary.LittleEndian.PutUint64(out[o+16:], s.ExactLiveRecords)
	}
	return out
}
func decodeSegmentStats(b []byte) ([]SegmentStatsEntry, error) {
	count, err := arrayCount(b, 24)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]SegmentStatsEntry, count)
	for i := range out {
		o := 8 + i*24
		if binary.LittleEndian.Uint32(b[o+4:o+8]) != 0 {
			return nil, corruptf("segment stats flags")
		}
		out[i] = SegmentStatsEntry{base.DataSegmentID(binary.LittleEndian.Uint32(b[o:])), binary.LittleEndian.Uint64(b[o+8:]), binary.LittleEndian.Uint64(b[o+16:])}
	}
	return out, nil
}
func arrayCount(b []byte, elem int) (int, error) {
	if len(b) < 8 || binary.LittleEndian.Uint32(b[4:8]) != 0 {
		return 0, corruptf("array header")
	}
	count := uint64(binary.LittleEndian.Uint32(b))
	size, err := base.MulUint64(count, uint64(elem))
	if err != nil || size+8 != uint64(len(b)) {
		return 0, corruptf("array length")
	}
	n, err := base.Uint64ToInt(count)
	if err != nil {
		return 0, corruptf("array count")
	}
	return n, nil
}
func strictBatchIDs(ids []base.BatchID, upper uint64) bool {
	for i, id := range ids {
		if id == 0 || uint64(id) >= upper || (i > 0 && id <= ids[i-1]) {
			return false
		}
	}
	return true
}
func validMapAddr(a base.MapAddr) bool { _, err := base.ParseMapAddr(uint64(a)); return err == nil }
