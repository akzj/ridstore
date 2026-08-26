package storecatalog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

const (
	containerHeaderSize = 64
	tlvHeaderSize       = 8
	maxManifestPayload  = 64 << 20
	tlvRequired         = uint16(1)
)

const (
	tlvHardLimits uint16 = iota + 1
	tlvRecordLogID
	tlvDataState
	tlvDataSegments
	tlvMapState
	tlvMapSegments
	tlvMappingRoot
	tlvCheckpoint
	tlvOpenBatches
	tlvSegmentStats
	tlvCount
)

var (
	manifestMagic = [8]byte{'R', 'I', 'D', 'M', 'V', '2', 0, 0}
	manifestCRC   = crc32.MakeTable(crc32.Castagnoli)
)

type tlv struct {
	typ   uint16
	value []byte
}

func Encode(m Manifest) ([]byte, error) {
	if err := Validate(m); err != nil {
		return nil, err
	}
	items := []tlv{
		{tlvHardLimits, encodeHardLimits(m.HardLimits)},
		{tlvRecordLogID, append([]byte(nil), m.RecordLogID[:]...)},
		{tlvDataState, encodePair32(uint32(m.ActiveDataSegmentID), uint32(m.NextDataSegmentID))},
		{tlvDataSegments, encodeDataSegments(m.SealedDataSegments)},
		{tlvMapState, encodePair32(uint32(m.ActiveMapSegmentID), uint32(m.NextMapSegmentID))},
		{tlvMapSegments, encodeMapSegments(m.SealedMapSegments)},
		{tlvMappingRoot, encodeUint64(uint64(m.MappingRoot))},
		{tlvCheckpoint, encodeCheckpoint(m)},
		{tlvOpenBatches, encodeBatchIDs(m.OpenBatchIDsAtCut)},
		{tlvSegmentStats, encodeStats(m.StatsCoveredCommitSeq, m.SegmentStats)},
	}
	payloadSize := 0
	for _, item := range items {
		padded := align8(len(item.value))
		if padded < 0 || payloadSize > maxManifestPayload-tlvHeaderSize-padded {
			return nil, ErrInvalid
		}
		payloadSize += tlvHeaderSize + padded
	}
	dst := make([]byte, containerHeaderSize+payloadSize)
	copy(dst[0:8], manifestMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatMajor)
	binary.LittleEndian.PutUint16(dst[10:12], FormatMinor)
	binary.LittleEndian.PutUint16(dst[12:14], containerHeaderSize)
	binary.LittleEndian.PutUint64(dst[16:24], m.Generation)
	copy(dst[24:40], m.StoreUUID[:])
	binary.LittleEndian.PutUint64(dst[40:48], uint64(payloadSize))
	offset := containerHeaderSize
	for _, item := range items {
		binary.LittleEndian.PutUint16(dst[offset:offset+2], item.typ)
		binary.LittleEndian.PutUint16(dst[offset+2:offset+4], tlvRequired)
		binary.LittleEndian.PutUint32(dst[offset+4:offset+8], uint32(len(item.value)))
		copy(dst[offset+tlvHeaderSize:], item.value)
		offset += tlvHeaderSize + align8(len(item.value))
	}
	binary.LittleEndian.PutUint32(dst[48:52], crc32.Checksum(dst[containerHeaderSize:], manifestCRC))
	binary.LittleEndian.PutUint32(dst[52:56], crc32.Checksum(dst[:52], manifestCRC))
	return dst, nil
}

func Decode(src []byte) (Manifest, error) {
	if len(src) < containerHeaderSize || string(src[:8]) != string(manifestMagic[:]) {
		return Manifest{}, fmt.Errorf("manifest magic or size: %w", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatMajor || binary.LittleEndian.Uint16(src[10:12]) > FormatMinor {
		return Manifest{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[12:14]) != containerHeaderSize || binary.LittleEndian.Uint16(src[14:16]) != 0 || !zero(src[56:64]) ||
		binary.LittleEndian.Uint32(src[52:56]) != crc32.Checksum(src[:52], manifestCRC) {
		return Manifest{}, fmt.Errorf("manifest header: %w", ErrCorrupt)
	}
	payloadSize := binary.LittleEndian.Uint64(src[40:48])
	if payloadSize > maxManifestPayload || payloadSize != uint64(len(src)-containerHeaderSize) ||
		binary.LittleEndian.Uint32(src[48:52]) != crc32.Checksum(src[containerHeaderSize:], manifestCRC) {
		return Manifest{}, fmt.Errorf("manifest payload: %w", ErrCorrupt)
	}
	items, err := decodeTLVs(src[containerHeaderSize:])
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	m.Generation = binary.LittleEndian.Uint64(src[16:24])
	copy(m.StoreUUID[:], src[24:40])
	if m.HardLimits, err = decodeHardLimits(items[tlvHardLimits]); err != nil {
		return Manifest{}, err
	}
	if len(items[tlvRecordLogID]) != len(m.RecordLogID) {
		return Manifest{}, fmt.Errorf("record log id: %w", ErrCorrupt)
	}
	copy(m.RecordLogID[:], items[tlvRecordLogID])
	dataActive, dataNext, err := decodePair32(items[tlvDataState])
	if err != nil {
		return Manifest{}, err
	}
	m.ActiveDataSegmentID, m.NextDataSegmentID = recordlog.SegmentID(dataActive), recordlog.SegmentID(dataNext)
	if m.SealedDataSegments, err = decodeDataSegments(items[tlvDataSegments]); err != nil {
		return Manifest{}, err
	}
	mapActive, mapNext, err := decodePair32(items[tlvMapState])
	if err != nil {
		return Manifest{}, err
	}
	m.ActiveMapSegmentID, m.NextMapSegmentID = model.MapSegmentID(mapActive), model.MapSegmentID(mapNext)
	if m.SealedMapSegments, err = decodeMapSegments(items[tlvMapSegments]); err != nil {
		return Manifest{}, err
	}
	if len(items[tlvMappingRoot]) != 8 {
		return Manifest{}, fmt.Errorf("mapping root: %w", ErrCorrupt)
	}
	m.MappingRoot = model.MapAddr(binary.LittleEndian.Uint64(items[tlvMappingRoot]))
	if err := decodeCheckpoint(items[tlvCheckpoint], &m); err != nil {
		return Manifest{}, err
	}
	if m.OpenBatchIDsAtCut, err = decodeBatchIDs(items[tlvOpenBatches]); err != nil {
		return Manifest{}, err
	}
	if m.StatsCoveredCommitSeq, m.SegmentStats, err = decodeStats(items[tlvSegmentStats]); err != nil {
		return Manifest{}, err
	}
	if err := Validate(m); err != nil {
		return Manifest{}, fmt.Errorf("manifest values: %w", ErrCorrupt)
	}
	return m, nil
}

func Validate(m Manifest) error {
	if m.Generation == 0 || m.StoreUUID == (StoreUUID{}) || m.RecordLogID == (recordlog.LogID{}) || m.ActiveDataSegmentID == 0 || m.NextDataSegmentID <= m.ActiveDataSegmentID ||
		m.ActiveMapSegmentID == 0 || m.NextMapSegmentID <= m.ActiveMapSegmentID || !m.ReplayStart.Valid() {
		return ErrInvalid
	}
	if err := validateHardLimits(m.HardLimits); err != nil {
		return err
	}
	if len(m.SealedDataSegments) > maxManifestPayload/32 || len(m.SealedMapSegments) > maxManifestPayload/8 || len(m.OpenBatchIDsAtCut) > maxManifestPayload/8 || len(m.SegmentStats) > maxManifestPayload/24 {
		return ErrInvalid
	}
	if err := validateDataSet(m); err != nil {
		return err
	}
	if err := validateMapSet(m); err != nil {
		return err
	}
	if m.ReservedIDHigh == 0 || m.ReservedBatchIDHigh == 0 || m.IssuedBatchIDHighAtCut == 0 || m.IssuedBatchIDHighAtCut > m.ReservedBatchIDHigh ||
		uint64(len(m.OpenBatchIDsAtCut)) > m.HardLimits.MaxOpenBatches || !strictBatchIDs(m.OpenBatchIDsAtCut, m.IssuedBatchIDHighAtCut) ||
		m.StatsCoveredCommitSeq != m.CoveredCommitSeq {
		return ErrInvalid
	}
	sealed := make(map[recordlog.SegmentID]DataSegmentSummary, len(m.SealedDataSegments))
	for _, summary := range m.SealedDataSegments {
		sealed[summary.SegmentID] = summary
	}
	var previous recordlog.SegmentID
	for _, stat := range m.SegmentStats {
		if stat.SegmentID == 0 || stat.SegmentID <= previous {
			return ErrInvalid
		}
		summary, ok := sealed[stat.SegmentID]
		if !ok || stat.LiveRecords > summary.RecordCount || stat.LiveBytes > uint64(summary.ValidEnd-recordlog.SegmentHeaderSize) {
			return ErrInvalid
		}
		previous = stat.SegmentID
	}
	return nil
}

func validateHardLimits(h HardLimits) error {
	values := [...]uint64{h.SegmentSize, h.MaxValueSize, h.MaxBatchBytes, h.MaxBatchMutations, h.MaxBatchConditions, h.MaxOpenBatches, h.MaxRecordLogPayload, h.IDReserveSize, h.BatchIDReserveSize}
	for _, value := range values {
		if value == 0 {
			return ErrInvalid
		}
	}
	minimumSegment := uint64(recordlog.SegmentHeaderSize + recordlog.RecordHeaderSize + recordlog.SegmentFooterSize)
	minimumMapSegment := uint64(mapstore.SegmentHeaderSize + mapstore.DenseNodeSize + mapstore.SegmentFooterSize)
	if h.SegmentSize <= minimumSegment || h.SegmentSize < minimumMapSegment || h.SegmentSize > math.MaxUint32 || h.SegmentSize&uint64(recordlog.RecordAlignment-1) != 0 ||
		h.MaxBatchMutations > math.MaxUint32 || h.MaxBatchConditions > math.MaxUint32 || h.MaxOpenBatches > math.MaxUint32 || h.MaxRecordLogPayload > math.MaxUint32 {
		return ErrInvalid
	}
	if h.MaxBatchBytes < h.MaxValueSize {
		return ErrInvalid
	}
	putMax, err := recordcodec.PutPayloadSize(h.MaxValueSize)
	if err != nil {
		return ErrInvalid
	}
	descriptorMax, err := recordcodec.DescriptorSize(h.MaxBatchMutations)
	if err != nil || uint64(recordcodec.CommitGroupHeadSize)+uint64(descriptorMax) > math.MaxUint32 {
		return ErrInvalid
	}
	commitMax := uint64(recordcodec.CommitGroupHeadSize) + uint64(descriptorMax)
	if uint64(putMax) > h.MaxRecordLogPayload || commitMax > h.MaxRecordLogPayload {
		return ErrInvalid
	}
	physical, err := recordlog.PhysicalRecordSize(h.MaxRecordLogPayload)
	if err != nil || uint64(physical) > h.SegmentSize-uint64(recordlog.SegmentHeaderSize)-uint64(recordlog.SegmentFooterSize) {
		return ErrInvalid
	}
	return nil
}

func validateDataSet(m Manifest) error {
	var previous recordlog.SegmentID
	for _, summary := range m.SealedDataSegments {
		if summary.SegmentID == 0 || summary.SegmentID >= m.ActiveDataSegmentID || summary.SegmentID >= m.NextDataSegmentID || summary.SegmentID <= previous ||
			summary.ValidEnd < recordlog.SegmentHeaderSize || uint64(summary.ValidEnd) > m.HardLimits.SegmentSize-uint64(recordlog.SegmentFooterSize) || summary.ValidEnd&uint32(recordlog.RecordAlignment-1) != 0 {
			return ErrInvalid
		}
		if summary.RecordCount == 0 {
			if summary.ValidEnd != recordlog.SegmentHeaderSize || summary.FirstAddr != 0 || summary.LastAddr != 0 {
				return ErrInvalid
			}
		} else if !summary.FirstAddr.Valid() || !summary.LastAddr.Valid() || summary.FirstAddr.SegmentID() != summary.SegmentID || summary.LastAddr.SegmentID() != summary.SegmentID || summary.FirstAddr > summary.LastAddr || summary.LastAddr.Offset() >= summary.ValidEnd {
			return ErrInvalid
		}
		previous = summary.SegmentID
	}
	replayFound := false
	if m.ReplayStart.SegmentID == m.ActiveDataSegmentID {
		replayFound = uint64(m.ReplayStart.Offset) <= m.HardLimits.SegmentSize-uint64(recordlog.SegmentFooterSize)
	} else {
		for _, summary := range m.SealedDataSegments {
			if summary.SegmentID == m.ReplayStart.SegmentID {
				replayFound = m.ReplayStart.Offset <= summary.ValidEnd
				break
			}
		}
	}
	if !replayFound {
		return ErrInvalid
	}
	return nil
}

func validateMapSet(m Manifest) error {
	var previous model.MapSegmentID
	for _, summary := range m.SealedMapSegments {
		if summary.SegmentID == 0 || summary.SegmentID >= m.ActiveMapSegmentID || summary.SegmentID >= m.NextMapSegmentID || summary.SegmentID <= previous || summary.ValidEnd < mapstore.SegmentHeaderSize || uint64(summary.ValidEnd) > m.HardLimits.SegmentSize-uint64(mapstore.SegmentFooterSize) || summary.ValidEnd&uint32(mapstore.Alignment-1) != 0 {
			return ErrInvalid
		}
		previous = summary.SegmentID
	}
	// Zero is the canonical root of an empty Mapping. Empty nodes are never
	// encoded merely to give the Manifest a non-zero address.
	if m.MappingRoot == 0 {
		return nil
	}
	if !m.MappingRoot.Valid() {
		return ErrInvalid
	}
	rootFound := false
	if m.MappingRoot.SegmentID() == m.ActiveMapSegmentID {
		rootFound = uint64(m.MappingRoot.Offset()) < m.HardLimits.SegmentSize-uint64(mapstore.SegmentFooterSize)
	} else {
		for _, summary := range m.SealedMapSegments {
			if summary.SegmentID == m.MappingRoot.SegmentID() {
				rootFound = m.MappingRoot.Offset() < summary.ValidEnd
				break
			}
		}
	}
	if !rootFound {
		return ErrInvalid
	}
	return nil
}

func decodeTLVs(src []byte) (map[uint16][]byte, error) {
	items := make(map[uint16][]byte, tlvCount-1)
	for offset := 0; offset < len(src); {
		if len(src)-offset < tlvHeaderSize {
			return nil, fmt.Errorf("TLV header: %w", ErrCorrupt)
		}
		typ := binary.LittleEndian.Uint16(src[offset : offset+2])
		flags := binary.LittleEndian.Uint16(src[offset+2 : offset+4])
		length := uint64(binary.LittleEndian.Uint32(src[offset+4 : offset+8]))
		padded := uint64(align8(int(length)))
		if flags & ^tlvRequired != 0 || padded < length || padded > uint64(len(src)-offset-tlvHeaderSize) {
			return nil, fmt.Errorf("TLV bounds: %w", ErrCorrupt)
		}
		start := offset + tlvHeaderSize
		end := start + int(length)
		if !zero(src[end : start+int(padded)]) {
			return nil, fmt.Errorf("TLV padding: %w", ErrCorrupt)
		}
		if typ == 0 || typ >= tlvCount {
			if flags&tlvRequired != 0 {
				return nil, ErrUnsupported
			}
		} else {
			if flags&tlvRequired == 0 {
				return nil, fmt.Errorf("known optional TLV: %w", ErrCorrupt)
			}
			if _, duplicate := items[typ]; duplicate {
				return nil, fmt.Errorf("duplicate TLV: %w", ErrCorrupt)
			}
			items[typ] = src[start:end]
		}
		offset = start + int(padded)
	}
	for typ := uint16(1); typ < tlvCount; typ++ {
		if _, ok := items[typ]; !ok {
			return nil, fmt.Errorf("missing TLV %d: %w", typ, ErrCorrupt)
		}
	}
	return items, nil
}

func encodeHardLimits(h HardLimits) []byte {
	dst := make([]byte, 9*8)
	values := [...]uint64{h.SegmentSize, h.MaxValueSize, h.MaxBatchBytes, h.MaxBatchMutations, h.MaxBatchConditions, h.MaxOpenBatches, h.MaxRecordLogPayload, h.IDReserveSize, h.BatchIDReserveSize}
	for i, value := range values {
		binary.LittleEndian.PutUint64(dst[i*8:], value)
	}
	return dst
}

func decodeHardLimits(src []byte) (HardLimits, error) {
	if len(src) != 9*8 {
		return HardLimits{}, fmt.Errorf("hard limits size: %w", ErrCorrupt)
	}
	values := [9]uint64{}
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(src[i*8:])
	}
	return HardLimits{SegmentSize: values[0], MaxValueSize: values[1], MaxBatchBytes: values[2], MaxBatchMutations: values[3], MaxBatchConditions: values[4], MaxOpenBatches: values[5], MaxRecordLogPayload: values[6], IDReserveSize: values[7], BatchIDReserveSize: values[8]}, nil
}

func encodeDataSegments(values []DataSegmentSummary) []byte {
	dst := make([]byte, 8+len(values)*32)
	binary.LittleEndian.PutUint32(dst[0:4], uint32(len(values)))
	for i, value := range values {
		offset := 8 + i*32
		binary.LittleEndian.PutUint32(dst[offset:offset+4], uint32(value.SegmentID))
		binary.LittleEndian.PutUint32(dst[offset+4:offset+8], value.ValidEnd)
		binary.LittleEndian.PutUint64(dst[offset+8:offset+16], value.RecordCount)
		binary.LittleEndian.PutUint64(dst[offset+16:offset+24], uint64(value.FirstAddr))
		binary.LittleEndian.PutUint64(dst[offset+24:offset+32], uint64(value.LastAddr))
	}
	return dst
}

func decodeDataSegments(src []byte) ([]DataSegmentSummary, error) {
	if len(src) < 8 || binary.LittleEndian.Uint32(src[4:8]) != 0 {
		return nil, fmt.Errorf("data segments header: %w", ErrCorrupt)
	}
	count := uint64(binary.LittleEndian.Uint32(src[0:4]))
	if count > uint64((len(src)-8)/32) || 8+count*32 != uint64(len(src)) {
		return nil, fmt.Errorf("data segments length: %w", ErrCorrupt)
	}
	values := make([]DataSegmentSummary, count)
	for i := range values {
		offset := 8 + i*32
		values[i] = DataSegmentSummary{SegmentID: recordlog.SegmentID(binary.LittleEndian.Uint32(src[offset : offset+4])), ValidEnd: binary.LittleEndian.Uint32(src[offset+4 : offset+8]), RecordCount: binary.LittleEndian.Uint64(src[offset+8 : offset+16]), FirstAddr: recordlog.VAddr(binary.LittleEndian.Uint64(src[offset+16 : offset+24])), LastAddr: recordlog.VAddr(binary.LittleEndian.Uint64(src[offset+24 : offset+32]))}
	}
	return values, nil
}

func encodeMapSegments(values []MapSegmentSummary) []byte {
	dst := make([]byte, 8+len(values)*8)
	binary.LittleEndian.PutUint32(dst[0:4], uint32(len(values)))
	for i, value := range values {
		offset := 8 + i*8
		binary.LittleEndian.PutUint32(dst[offset:offset+4], uint32(value.SegmentID))
		binary.LittleEndian.PutUint32(dst[offset+4:offset+8], value.ValidEnd)
	}
	return dst
}

func decodeMapSegments(src []byte) ([]MapSegmentSummary, error) {
	if len(src) < 8 || binary.LittleEndian.Uint32(src[4:8]) != 0 {
		return nil, fmt.Errorf("map segments header: %w", ErrCorrupt)
	}
	count := uint64(binary.LittleEndian.Uint32(src[0:4]))
	if count > uint64((len(src)-8)/8) || 8+count*8 != uint64(len(src)) {
		return nil, fmt.Errorf("map segments length: %w", ErrCorrupt)
	}
	values := make([]MapSegmentSummary, count)
	for i := range values {
		offset := 8 + i*8
		values[i] = MapSegmentSummary{SegmentID: model.MapSegmentID(binary.LittleEndian.Uint32(src[offset : offset+4])), ValidEnd: binary.LittleEndian.Uint32(src[offset+4 : offset+8])}
	}
	return values, nil
}

func encodeCheckpoint(m Manifest) []byte {
	dst := make([]byte, 56)
	binary.LittleEndian.PutUint64(dst[0:8], uint64(m.CoveredCommitSeq))
	binary.LittleEndian.PutUint64(dst[8:16], m.ReplayStart.Uint64())
	binary.LittleEndian.PutUint64(dst[16:24], m.ReservedIDHigh)
	binary.LittleEndian.PutUint64(dst[24:32], m.ReservedBatchIDHigh)
	binary.LittleEndian.PutUint64(dst[32:40], m.IssuedBatchIDHighAtCut)
	return dst
}

func decodeCheckpoint(src []byte, m *Manifest) error {
	if len(src) != 56 || !zero(src[40:56]) {
		return fmt.Errorf("checkpoint size: %w", ErrCorrupt)
	}
	m.CoveredCommitSeq = model.CommitSeq(binary.LittleEndian.Uint64(src[0:8]))
	pos, err := recordlog.ParseLogPos(binary.LittleEndian.Uint64(src[8:16]))
	if err != nil {
		return fmt.Errorf("replay start: %w", ErrCorrupt)
	}
	m.ReplayStart = pos
	m.ReservedIDHigh = binary.LittleEndian.Uint64(src[16:24])
	m.ReservedBatchIDHigh = binary.LittleEndian.Uint64(src[24:32])
	m.IssuedBatchIDHighAtCut = binary.LittleEndian.Uint64(src[32:40])
	return nil
}

func encodeBatchIDs(values []model.BatchID) []byte {
	dst := make([]byte, 8+len(values)*8)
	binary.LittleEndian.PutUint32(dst[0:4], uint32(len(values)))
	for i, value := range values {
		binary.LittleEndian.PutUint64(dst[8+i*8:], uint64(value))
	}
	return dst
}

func decodeBatchIDs(src []byte) ([]model.BatchID, error) {
	if len(src) < 8 || binary.LittleEndian.Uint32(src[4:8]) != 0 {
		return nil, fmt.Errorf("batch ids header: %w", ErrCorrupt)
	}
	count := uint64(binary.LittleEndian.Uint32(src[0:4]))
	if count > uint64((len(src)-8)/8) || 8+count*8 != uint64(len(src)) {
		return nil, fmt.Errorf("batch ids length: %w", ErrCorrupt)
	}
	values := make([]model.BatchID, count)
	for i := range values {
		values[i] = model.BatchID(binary.LittleEndian.Uint64(src[8+i*8:]))
	}
	return values, nil
}

func encodeStats(covered model.CommitSeq, values []SegmentStats) []byte {
	dst := make([]byte, 16+len(values)*24)
	binary.LittleEndian.PutUint64(dst[0:8], uint64(covered))
	binary.LittleEndian.PutUint32(dst[8:12], uint32(len(values)))
	for i, value := range values {
		offset := 16 + i*24
		binary.LittleEndian.PutUint32(dst[offset:offset+4], uint32(value.SegmentID))
		binary.LittleEndian.PutUint64(dst[offset+8:offset+16], value.LiveBytes)
		binary.LittleEndian.PutUint64(dst[offset+16:offset+24], value.LiveRecords)
	}
	return dst
}

func decodeStats(src []byte) (model.CommitSeq, []SegmentStats, error) {
	if len(src) < 16 || binary.LittleEndian.Uint32(src[12:16]) != 0 {
		return 0, nil, fmt.Errorf("stats header: %w", ErrCorrupt)
	}
	covered := model.CommitSeq(binary.LittleEndian.Uint64(src[0:8]))
	count := uint64(binary.LittleEndian.Uint32(src[8:12]))
	if count > uint64((len(src)-16)/24) || 16+count*24 != uint64(len(src)) {
		return 0, nil, fmt.Errorf("stats length: %w", ErrCorrupt)
	}
	values := make([]SegmentStats, count)
	for i := range values {
		offset := 16 + i*24
		if binary.LittleEndian.Uint32(src[offset+4:offset+8]) != 0 {
			return 0, nil, fmt.Errorf("stats reserved: %w", ErrCorrupt)
		}
		values[i] = SegmentStats{SegmentID: recordlog.SegmentID(binary.LittleEndian.Uint32(src[offset : offset+4])), LiveBytes: binary.LittleEndian.Uint64(src[offset+8 : offset+16]), LiveRecords: binary.LittleEndian.Uint64(src[offset+16 : offset+24])}
	}
	return covered, values, nil
}

func encodePair32(a, b uint32) []byte {
	dst := make([]byte, 8)
	binary.LittleEndian.PutUint32(dst[0:4], a)
	binary.LittleEndian.PutUint32(dst[4:8], b)
	return dst
}

func decodePair32(src []byte) (uint32, uint32, error) {
	if len(src) != 8 {
		return 0, 0, fmt.Errorf("uint32 pair: %w", ErrCorrupt)
	}
	return binary.LittleEndian.Uint32(src[0:4]), binary.LittleEndian.Uint32(src[4:8]), nil
}

func encodeUint64(value uint64) []byte {
	dst := make([]byte, 8)
	binary.LittleEndian.PutUint64(dst, value)
	return dst
}

func strictBatchIDs(values []model.BatchID, high uint64) bool {
	var previous model.BatchID
	for _, value := range values {
		if value == 0 || uint64(value) >= high || value <= previous {
			return false
		}
		previous = value
	}
	return true
}

func align8(value int) int {
	if value < 0 || value > math.MaxInt-7 {
		return -1
	}
	return (value + 7) &^ 7
}

func zero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}

func sortDataSegments(values []DataSegmentSummary) {
	sort.Slice(values, func(i, j int) bool { return values[i].SegmentID < values[j].SegmentID })
}
