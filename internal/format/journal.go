package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

const (
	MaxJournalPayloadSize       = 16 << 20
	maintenanceFixedPayloadSize = 120
)

type InitializingPhase uint16

const (
	InitializingPrepared InitializingPhase = iota + 1
	InitializingDirectoriesDurable
	InitializingDataHeaderDurable
	InitializingMapHeaderDurable
	InitializingManifestInstalled
)

type InitializingMarker struct {
	StoreUUID  base.StoreUUID
	HardLimits HardLimits
	Phase      InitializingPhase
}

type MaintenanceType uint16

const (
	MaintenanceDataGC MaintenanceType = iota + 1
	MaintenanceMappingCheckpoint
	MaintenanceMappingGC
)

type FileKind uint16
type FileState uint16

const (
	FileKindData FileKind = iota + 1
	FileKindMapping
)

const (
	FileStateActive FileState = iota + 1
	FileStateSealed
	FileStateTemporary
	FileStateTrash
)

type JournalFileRef struct {
	Kind     FileKind
	State    FileState
	FileID   uint32
	ValidEnd uint64
	FirstSeq uint64
	LastSeq  uint64
}

type MaintenanceJournal struct {
	Generation            uint64
	StoreUUID             base.StoreUUID
	OperationID           [16]byte
	OperationType         MaintenanceType
	Phase                 uint16
	SourceFiles           []JournalFileRef
	DestinationFiles      []JournalFileRef
	OldManifestGeneration uint64
	NewManifestGeneration uint64
}

type RotationJournal struct {
	StoreUUID                   base.StoreUUID
	OldSegmentID                base.DataSegmentID
	NewSegmentID                base.DataSegmentID
	BaseManifestGeneration      uint64
	InstalledManifestGeneration uint64
	Phase                       uint32
}

func EncodeInitializingMarker(marker InitializingMarker) ([]byte, error) {
	if marker.Phase < InitializingPrepared || marker.Phase > InitializingManifestInstalled {
		return nil, fmt.Errorf("initializing phase: %w", base.ErrInvalidConfig)
	}
	if err := ValidateHardLimits(marker.HardLimits); err != nil {
		return nil, err
	}
	return EncodeContainer(Container{Magic: InitializingMagic, Generation: 1, StoreUUID: marker.StoreUUID, TLVs: []TLV{
		requiredTLV(1, encodeHardLimits(marker.HardLimits)), requiredTLV(2, scalar16(uint16(marker.Phase))),
	}})
}

func DecodeInitializingMarker(src []byte) (InitializingMarker, error) {
	c, err := DecodeContainer(src, InitializingMagic, MaxJournalPayloadSize)
	if err != nil {
		return InitializingMarker{}, err
	}
	if c.Generation != 1 || len(c.TLVs) != 2 || c.TLVs[0].Type != 1 || !c.TLVs[0].Required || c.TLVs[1].Type != 2 || !c.TLVs[1].Required {
		return InitializingMarker{}, corruptf("initializing TLV set")
	}
	h, err := decodeHardLimits(c.TLVs[0].Value)
	if err != nil {
		return InitializingMarker{}, err
	}
	p, err := decode16(c.TLVs[1].Value)
	if err != nil {
		return InitializingMarker{}, err
	}
	marker := InitializingMarker{StoreUUID: c.StoreUUID, HardLimits: h, Phase: InitializingPhase(p)}
	if marker.Phase < InitializingPrepared || marker.Phase > InitializingManifestInstalled || ValidateHardLimits(h) != nil {
		return InitializingMarker{}, corruptf("initializing fields")
	}
	return marker, nil
}

func EncodeMaintenanceJournal(j MaintenanceJournal) ([]byte, error) {
	if err := validateMaintenance(j); err != nil {
		return nil, err
	}
	encoded, err := EncodeContainer(Container{Magic: MaintenanceMagic, Generation: j.Generation, StoreUUID: j.StoreUUID, TLVs: []TLV{
		requiredTLV(1, append([]byte(nil), j.OperationID[:]...)),
		requiredTLV(2, scalar16(uint16(j.OperationType))), requiredTLV(3, scalar16(j.Phase)),
		requiredTLV(4, encodeJournalFileRefs(j.SourceFiles)), requiredTLV(5, encodeJournalFileRefs(j.DestinationFiles)),
		requiredTLV(6, scalar64(j.OldManifestGeneration)), requiredTLV(7, scalar64(j.NewManifestGeneration)),
	}})
	if err != nil {
		return nil, err
	}
	if len(encoded)-ContainerHeaderSize > MaxJournalPayloadSize {
		return nil, fmt.Errorf("maintenance journal payload exceeds format limit: %w", base.ErrInvalidConfig)
	}
	return encoded, nil
}

func DecodeMaintenanceJournal(src []byte) (MaintenanceJournal, error) {
	c, err := DecodeContainer(src, MaintenanceMagic, MaxJournalPayloadSize)
	if err != nil {
		return MaintenanceJournal{}, err
	}
	if len(c.TLVs) != 7 {
		return MaintenanceJournal{}, corruptf("maintenance TLV count")
	}
	for i, item := range c.TLVs {
		if item.Type != uint16(i+1) || !item.Required {
			return MaintenanceJournal{}, corruptf("maintenance TLV set")
		}
	}
	if len(c.TLVs[0].Value) != 16 {
		return MaintenanceJournal{}, corruptf("operation ID length")
	}
	j := MaintenanceJournal{Generation: c.Generation, StoreUUID: c.StoreUUID}
	copy(j.OperationID[:], c.TLVs[0].Value)
	v, err := decode16(c.TLVs[1].Value)
	if err != nil {
		return MaintenanceJournal{}, err
	}
	j.OperationType = MaintenanceType(v)
	if j.Phase, err = decode16(c.TLVs[2].Value); err != nil {
		return MaintenanceJournal{}, err
	}
	if j.SourceFiles, err = decodeJournalFileRefs(c.TLVs[3].Value); err != nil {
		return MaintenanceJournal{}, err
	}
	if j.DestinationFiles, err = decodeJournalFileRefs(c.TLVs[4].Value); err != nil {
		return MaintenanceJournal{}, err
	}
	if j.OldManifestGeneration, err = decode64(c.TLVs[5].Value); err != nil {
		return MaintenanceJournal{}, err
	}
	if j.NewManifestGeneration, err = decode64(c.TLVs[6].Value); err != nil {
		return MaintenanceJournal{}, err
	}
	if err := validateMaintenance(j); err != nil {
		return MaintenanceJournal{}, corruptf("maintenance fields: %v", err)
	}
	return j, nil
}

func EncodeRotationJournal(j RotationJournal) ([]byte, error) {
	if err := validateRotation(j); err != nil {
		return nil, err
	}
	payload := make([]byte, 32)
	binary.LittleEndian.PutUint32(payload[0:4], uint32(j.OldSegmentID))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(j.NewSegmentID))
	binary.LittleEndian.PutUint64(payload[8:16], j.BaseManifestGeneration)
	binary.LittleEndian.PutUint64(payload[16:24], j.InstalledManifestGeneration)
	binary.LittleEndian.PutUint32(payload[24:28], j.Phase)
	return encodeFixedContainer(RotationMagic, uint64(j.NewSegmentID), j.StoreUUID, payload)
}

func DecodeRotationJournal(src []byte) (RotationJournal, error) {
	gen, uuid, payload, err := decodeFixedContainer(src, RotationMagic, 32)
	if err != nil {
		return RotationJournal{}, err
	}
	j := RotationJournal{StoreUUID: uuid, OldSegmentID: base.DataSegmentID(binary.LittleEndian.Uint32(payload[0:4])), NewSegmentID: base.DataSegmentID(binary.LittleEndian.Uint32(payload[4:8])), BaseManifestGeneration: binary.LittleEndian.Uint64(payload[8:16]), InstalledManifestGeneration: binary.LittleEndian.Uint64(payload[16:24]), Phase: binary.LittleEndian.Uint32(payload[24:28])}
	if gen != uint64(j.NewSegmentID) || binary.LittleEndian.Uint32(payload[28:32]) != 0 {
		return RotationJournal{}, corruptf("rotation generation or reserved")
	}
	if err := validateRotation(j); err != nil {
		return RotationJournal{}, corruptf("rotation fields: %v", err)
	}
	return j, nil
}

func ValidateMaintenanceTransition(old, next MaintenanceJournal) error {
	if err := validateMaintenance(old); err != nil {
		return fmt.Errorf("old maintenance journal: %w", err)
	}
	if old.Generation != next.Generation || old.StoreUUID != next.StoreUUID || old.OperationID != next.OperationID || old.OperationType != next.OperationType || old.OldManifestGeneration != next.OldManifestGeneration {
		return fmt.Errorf("maintenance identity changed: %w", base.ErrInvalidConfig)
	}
	if next.Phase < old.Phase || next.Phase > old.Phase+1 {
		return fmt.Errorf("maintenance phase transition: %w", base.ErrInvalidConfig)
	}
	if old.NewManifestGeneration != 0 && old.NewManifestGeneration != next.NewManifestGeneration {
		return fmt.Errorf("maintenance manifest generation changed: %w", base.ErrInvalidConfig)
	}
	if !refsExtend(old.SourceFiles, next.SourceFiles) || !refsExtend(old.DestinationFiles, next.DestinationFiles) {
		return fmt.Errorf("maintenance file refs regressed: %w", base.ErrInvalidConfig)
	}
	return validateMaintenance(next)
}

func ValidateInitializingTransition(old, next InitializingMarker) error {
	if _, err := EncodeInitializingMarker(old); err != nil {
		return fmt.Errorf("old initializing marker: %w", err)
	}
	if old.StoreUUID != next.StoreUUID || old.HardLimits != next.HardLimits || next.Phase < old.Phase || next.Phase > old.Phase+1 {
		return fmt.Errorf("initializing transition: %w", base.ErrInvalidConfig)
	}
	_, err := EncodeInitializingMarker(next)
	return err
}

func ValidateRotationTransition(old, next RotationJournal) error {
	if err := validateRotation(old); err != nil {
		return fmt.Errorf("old rotation journal: %w", err)
	}
	if old.StoreUUID != next.StoreUUID || old.OldSegmentID != next.OldSegmentID || old.NewSegmentID != next.NewSegmentID ||
		old.BaseManifestGeneration != next.BaseManifestGeneration || next.Phase < old.Phase || next.Phase > old.Phase+1 {
		return fmt.Errorf("rotation transition: %w", base.ErrInvalidConfig)
	}
	if old.InstalledManifestGeneration != 0 && old.InstalledManifestGeneration != next.InstalledManifestGeneration {
		return fmt.Errorf("rotation manifest generation changed: %w", base.ErrInvalidConfig)
	}
	return validateRotation(next)
}

func validateMaintenance(j MaintenanceJournal) error {
	if j.Generation == 0 || j.StoreUUID == (base.StoreUUID{}) || j.OperationID == ([16]byte{}) || j.OldManifestGeneration == 0 {
		return fmt.Errorf("maintenance identity: %w", base.ErrInvalidConfig)
	}
	maxPhase := map[MaintenanceType]uint16{MaintenanceDataGC: 7, MaintenanceMappingCheckpoint: 5, MaintenanceMappingGC: 6}[j.OperationType]
	if maxPhase == 0 || j.Phase == 0 || j.Phase > maxPhase {
		return fmt.Errorf("maintenance type/phase: %w", base.ErrInvalidConfig)
	}
	if j.Phase < 4 && j.NewManifestGeneration != 0 {
		return fmt.Errorf("premature new manifest generation: %w", base.ErrInvalidConfig)
	}
	if j.Phase >= 4 && (j.NewManifestGeneration <= j.OldManifestGeneration) {
		return fmt.Errorf("missing installed manifest generation: %w", base.ErrInvalidConfig)
	}
	refCount := uint64(len(j.SourceFiles)) + uint64(len(j.DestinationFiles))
	if refCount > (MaxJournalPayloadSize-maintenanceFixedPayloadSize)/40 {
		return fmt.Errorf("maintenance journal payload exceeds format limit: %w", base.ErrInvalidConfig)
	}
	if err := validateJournalRefs(j.SourceFiles); err != nil {
		return err
	}
	return validateJournalRefs(j.DestinationFiles)
}

func validateRotation(j RotationJournal) error {
	if j.StoreUUID == (base.StoreUUID{}) || j.OldSegmentID == 0 || j.NewSegmentID <= j.OldSegmentID || j.BaseManifestGeneration == 0 || j.Phase < 1 || j.Phase > 5 {
		return fmt.Errorf("rotation identity or phase: %w", base.ErrInvalidConfig)
	}
	if j.Phase < 5 && j.InstalledManifestGeneration != 0 {
		return fmt.Errorf("premature rotation manifest: %w", base.ErrInvalidConfig)
	}
	if j.Phase == 5 && j.InstalledManifestGeneration <= j.BaseManifestGeneration {
		return fmt.Errorf("missing rotation manifest: %w", base.ErrInvalidConfig)
	}
	return nil
}

func validateJournalRefs(refs []JournalFileRef) error {
	// Two arrays and the remaining TLVs must fit the journal container limit.
	// This per-array bound also keeps encodeJournalFileRefs integer arithmetic safe.
	if uint64(len(refs)) > MaxJournalPayloadSize/40 {
		return fmt.Errorf("journal ref count: %w", base.ErrInvalidConfig)
	}
	var prev JournalFileRef
	for i, r := range refs {
		if (r.Kind != FileKindData && r.Kind != FileKindMapping) || (r.State < 1 || r.State > 4) || r.FileID == 0 || r.ValidEnd < SegmentHeaderSize || r.ValidEnd > math.MaxUint32 || r.ValidEnd%8 != 0 || r.FirstSeq == 0 || r.LastSeq < r.FirstSeq || (i > 0 && !refLess(prev, r)) {
			return fmt.Errorf("journal file ref: %w", base.ErrInvalidConfig)
		}
		prev = r
	}
	return nil
}
func refLess(a, b JournalFileRef) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.FileID < b.FileID
}
func refsExtend(old, next []JournalFileRef) bool {
	j := 0
	for _, o := range old {
		for j < len(next) && refIdentityLess(next[j], o) {
			j++
		}
		if j == len(next) || !sameRefIdentity(next[j], o) || !validRefStateTransition(o.State, next[j].State) || next[j].ValidEnd < o.ValidEnd ||
			next[j].FirstSeq != o.FirstSeq || next[j].LastSeq < o.LastSeq {
			return false
		}
		j++
	}
	return true
}

func refIdentityLess(a, b JournalFileRef) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.FileID < b.FileID
}

func sameRefIdentity(a, b JournalFileRef) bool {
	return a.Kind == b.Kind && a.FileID == b.FileID
}

func validRefStateTransition(old, next FileState) bool {
	if old == next {
		return true
	}
	switch old {
	case FileStateTemporary:
		return next == FileStateActive || next == FileStateSealed || next == FileStateTrash
	case FileStateActive:
		return next == FileStateSealed || next == FileStateTrash
	case FileStateSealed:
		return next == FileStateTrash
	default:
		return false
	}
}
func encodeJournalFileRefs(refs []JournalFileRef) []byte {
	out := make([]byte, 8+len(refs)*40)
	binary.LittleEndian.PutUint32(out, uint32(len(refs)))
	for i, r := range refs {
		o := 8 + i*40
		binary.LittleEndian.PutUint16(out[o:], uint16(r.Kind))
		binary.LittleEndian.PutUint16(out[o+2:], uint16(r.State))
		binary.LittleEndian.PutUint32(out[o+4:], r.FileID)
		binary.LittleEndian.PutUint64(out[o+8:], r.ValidEnd)
		binary.LittleEndian.PutUint64(out[o+16:], r.FirstSeq)
		binary.LittleEndian.PutUint64(out[o+24:], r.LastSeq)
	}
	return out
}
func decodeJournalFileRefs(b []byte) ([]JournalFileRef, error) {
	count, err := arrayCount(b, 40)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	out := make([]JournalFileRef, count)
	for i := range out {
		o := 8 + i*40
		if binary.LittleEndian.Uint64(b[o+32:o+40]) != 0 {
			return nil, corruptf("journal ref reserved")
		}
		out[i] = JournalFileRef{FileKind(binary.LittleEndian.Uint16(b[o:])), FileState(binary.LittleEndian.Uint16(b[o+2:])), binary.LittleEndian.Uint32(b[o+4:]), binary.LittleEndian.Uint64(b[o+8:]), binary.LittleEndian.Uint64(b[o+16:]), binary.LittleEndian.Uint64(b[o+24:])}
	}
	return out, nil
}
func scalar16(v uint16) []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, v); return b }
func decode16(b []byte) (uint16, error) {
	if len(b) != 2 {
		return 0, corruptf("uint16 TLV length")
	}
	return binary.LittleEndian.Uint16(b), nil
}

func encodeFixedContainer(magic ContainerMagic, generation uint64, uuid base.StoreUUID, payload []byte) ([]byte, error) {
	if generation == 0 || uuid == (base.StoreUUID{}) {
		return nil, fmt.Errorf("fixed container identity: %w", base.ErrInvalidConfig)
	}
	dst := make([]byte, ContainerHeaderSize+len(payload))
	copy(dst[:8], magic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatMajorVersion)
	binary.LittleEndian.PutUint16(dst[10:12], FormatMinorVersion)
	binary.LittleEndian.PutUint32(dst[12:16], ContainerHeaderSize)
	binary.LittleEndian.PutUint64(dst[16:24], generation)
	copy(dst[24:40], uuid[:])
	binary.LittleEndian.PutUint64(dst[40:48], uint64(len(payload)))
	copy(dst[64:], payload)
	binary.LittleEndian.PutUint32(dst[48:52], crc32.Checksum(payload, castagnoliTable))
	binary.LittleEndian.PutUint32(dst[52:56], crc32.Checksum(dst[:64], castagnoliTable))
	return dst, nil
}
func decodeFixedContainer(src []byte, magic ContainerMagic, wantPayload int) (uint64, base.StoreUUID, []byte, error) {
	var uuid base.StoreUUID
	if len(src) != ContainerHeaderSize+wantPayload || !equalMagic(src[:8], [8]byte(magic)) || binary.LittleEndian.Uint32(src[12:16]) != ContainerHeaderSize || !validChecksum(src[:64], 52) {
		return 0, uuid, nil, corruptf("fixed container header")
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatMajorVersion || binary.LittleEndian.Uint16(src[10:12]) > FormatMinorVersion {
		return 0, uuid, nil, fmt.Errorf("fixed container version: %w", base.ErrUnsupported)
	}
	if binary.LittleEndian.Uint64(src[40:48]) != uint64(wantPayload) || binary.LittleEndian.Uint32(src[56:60]) != 0 || binary.LittleEndian.Uint32(src[60:64]) != 0 || crc32.Checksum(src[64:], castagnoliTable) != binary.LittleEndian.Uint32(src[48:52]) {
		return 0, uuid, nil, corruptf("fixed container payload")
	}
	copy(uuid[:], src[24:40])
	gen := binary.LittleEndian.Uint64(src[16:24])
	if gen == 0 || uuid == (base.StoreUUID{}) {
		return 0, uuid, nil, corruptf("fixed container identity")
	}
	return gen, uuid, src[64:], nil
}
