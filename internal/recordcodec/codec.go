package recordcodec

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

var protocolMagic = [4]byte{'R', 'S', 'P', '2'}

// TypeOf validates the common protocol header and returns the concrete record
// type. Callers must still run the matching Decode function before using fields.
func TypeOf(src []byte) (RecordType, error) {
	if len(src) < int(CommonHeaderSize) {
		return 0, fmt.Errorf("protocol header truncated: %w", ErrCorrupt)
	}
	typ := RecordType(src[6])
	var headerSize uint32
	switch typ {
	case RecordTypePut:
		headerSize = PutHeaderSize
	case RecordTypeCommitGroup:
		headerSize = CommitGroupHeadSize
	case RecordTypeAbort, RecordTypeIDReserve, RecordTypeBatchIDReserve, RecordTypeCheckpoint:
		headerSize = FixedRecordSize
	default:
		return 0, fmt.Errorf("protocol record type: %w", ErrCorrupt)
	}
	if _, err := decodeHeader(src, typ, headerSize); err != nil {
		return 0, err
	}
	return typ, nil
}

type header struct {
	Type       RecordType
	HeaderSize uint16
	TotalSize  uint32
}

func PutPayloadSize(valueBytes uint64) (uint32, error) {
	return checkedSize(uint64(PutHeaderSize), valueBytes)
}

func DescriptorSize(mutationCount uint64) (uint32, error) {
	mutations, ok := checkedMul(mutationCount, uint64(MutationSize))
	if !ok {
		return 0, ErrTooLarge
	}
	return checkedSize(uint64(DescriptorHeadSize), mutations)
}

func CommitGroupPayloadSize(descriptors []Descriptor) (uint32, error) {
	if len(descriptors) == 0 || uint64(len(descriptors)) > math.MaxUint32 {
		return 0, ErrInvalid
	}
	total := uint64(CommitGroupHeadSize)
	for i := range descriptors {
		size, err := DescriptorSize(uint64(len(descriptors[i].Mutations)))
		if err != nil || total > math.MaxUint32-uint64(size) {
			return 0, ErrTooLarge
		}
		total += uint64(size)
	}
	return uint32(total), nil
}

func EncodePut(record PutRecord, maxValueSize uint64) ([]byte, error) {
	if record.OriginBatchID == 0 || record.RecordID == 0 || uint64(len(record.Value)) > maxValueSize {
		return nil, ErrInvalid
	}
	total, err := PutPayloadSize(uint64(len(record.Value)))
	if err != nil {
		return nil, err
	}
	dst := make([]byte, total)
	encodeHeader(dst, RecordTypePut, PutHeaderSize)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(record.OriginBatchID))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(record.RecordID))
	copy(dst[PutHeaderSize:], record.Value)
	return dst, nil
}

// DecodePut returns a Value slice that aliases src.
func DecodePut(src []byte, maxValueSize uint64) (PutRecord, error) {
	if _, err := decodeHeader(src, RecordTypePut, PutHeaderSize); err != nil {
		return PutRecord{}, err
	}
	record := PutRecord{
		OriginBatchID: model.BatchID(binary.LittleEndian.Uint64(src[16:24])),
		RecordID:      model.ID(binary.LittleEndian.Uint64(src[24:32])),
		Value:         src[PutHeaderSize:],
	}
	if record.OriginBatchID == 0 || record.RecordID == 0 || uint64(len(record.Value)) > maxValueSize {
		return PutRecord{}, fmt.Errorf("put fields: %w", ErrCorrupt)
	}
	return record, nil
}

func EncodeCommitGroup(group CommitGroup, maxPayloadSize uint64) ([]byte, error) {
	total, err := CommitGroupPayloadSize(group.Descriptors)
	if err != nil || uint64(total) > maxPayloadSize {
		if err == nil {
			err = ErrTooLarge
		}
		return nil, err
	}
	if err := validateDescriptors(group.Descriptors); err != nil {
		return nil, err
	}
	dst := make([]byte, total)
	encodeHeader(dst, RecordTypeCommitGroup, CommitGroupHeadSize)
	binary.LittleEndian.PutUint32(dst[16:20], uint32(len(group.Descriptors)))
	var totalMutations uint64
	for i := range group.Descriptors {
		totalMutations += uint64(len(group.Descriptors[i].Mutations))
	}
	if totalMutations > math.MaxUint32 {
		return nil, ErrTooLarge
	}
	binary.LittleEndian.PutUint32(dst[20:24], uint32(totalMutations))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(group.Descriptors[0].CommitSeq))

	offset := uint32(CommitGroupHeadSize)
	for i := range group.Descriptors {
		descriptor := &group.Descriptors[i]
		size, _ := DescriptorSize(uint64(len(descriptor.Mutations)))
		dst[offset] = byte(descriptor.Kind)
		binary.LittleEndian.PutUint16(dst[offset+2:offset+4], uint16(DescriptorHeadSize))
		binary.LittleEndian.PutUint32(dst[offset+4:offset+8], uint32(len(descriptor.Mutations)))
		binary.LittleEndian.PutUint64(dst[offset+8:offset+16], uint64(descriptor.BatchID))
		binary.LittleEndian.PutUint64(dst[offset+16:offset+24], uint64(descriptor.CommitSeq))
		binary.LittleEndian.PutUint64(dst[offset+24:offset+32], descriptor.LogicalPayloadBytes)
		binary.LittleEndian.PutUint32(dst[offset+32:offset+36], size)
		offset += DescriptorHeadSize
		for _, mutation := range descriptor.Mutations {
			binary.LittleEndian.PutUint64(dst[offset:offset+8], uint64(mutation.RecordID))
			binary.LittleEndian.PutUint64(dst[offset+8:offset+16], uint64(mutation.NewAddr))
			binary.LittleEndian.PutUint64(dst[offset+16:offset+24], uint64(mutation.ExpectedOldAddr))
			dst[offset+24] = byte(mutation.Operation)
			offset += MutationSize
		}
	}
	return dst, nil
}

func DecodeCommitGroup(src []byte, maxPayloadSize, maxDescriptors, maxMutations uint64) (CommitGroup, error) {
	if uint64(len(src)) > maxPayloadSize {
		return CommitGroup{}, ErrTooLarge
	}
	if _, err := decodeHeader(src, RecordTypeCommitGroup, CommitGroupHeadSize); err != nil {
		return CommitGroup{}, err
	}
	count := uint64(binary.LittleEndian.Uint32(src[16:20]))
	wantMutations := uint64(binary.LittleEndian.Uint32(src[20:24]))
	firstSeq := model.CommitSeq(binary.LittleEndian.Uint64(src[24:32]))
	maxByInput := (uint64(len(src)) - uint64(CommitGroupHeadSize)) / uint64(DescriptorHeadSize)
	if count == 0 || count > maxDescriptors || count > maxByInput || wantMutations > maxMutations || firstSeq == 0 {
		return CommitGroup{}, fmt.Errorf("commit group counts: %w", ErrCorrupt)
	}
	descriptors := make([]Descriptor, 0, count)
	offset := uint64(CommitGroupHeadSize)
	var totalMutations uint64
	for i := uint64(0); i < count; i++ {
		if offset > uint64(len(src)) || uint64(len(src))-offset < uint64(DescriptorHeadSize) {
			return CommitGroup{}, fmt.Errorf("descriptor header truncated: %w", ErrCorrupt)
		}
		headerBytes := src[offset : offset+uint64(DescriptorHeadSize)]
		if headerBytes[1] != 0 || binary.LittleEndian.Uint16(headerBytes[2:4]) != uint16(DescriptorHeadSize) || binary.LittleEndian.Uint32(headerBytes[36:40]) != 0 {
			return CommitGroup{}, fmt.Errorf("descriptor header fields: %w", ErrCorrupt)
		}
		mutationCount := uint64(binary.LittleEndian.Uint32(headerBytes[4:8]))
		descriptorSize := uint64(binary.LittleEndian.Uint32(headerBytes[32:36]))
		wantSize, err := DescriptorSize(mutationCount)
		if err != nil || descriptorSize != uint64(wantSize) || descriptorSize > uint64(len(src))-offset || mutationCount > maxMutations-totalMutations {
			return CommitGroup{}, fmt.Errorf("descriptor size: %w", ErrCorrupt)
		}
		descriptor := Descriptor{
			Kind:                DescriptorKind(headerBytes[0]),
			BatchID:             model.BatchID(binary.LittleEndian.Uint64(headerBytes[8:16])),
			CommitSeq:           model.CommitSeq(binary.LittleEndian.Uint64(headerBytes[16:24])),
			LogicalPayloadBytes: binary.LittleEndian.Uint64(headerBytes[24:32]),
			Mutations:           make([]Mutation, mutationCount),
		}
		entryOffset := offset + uint64(DescriptorHeadSize)
		for index := range descriptor.Mutations {
			entry := src[entryOffset : entryOffset+uint64(MutationSize)]
			if !zero(entry[25:32]) {
				return CommitGroup{}, fmt.Errorf("mutation reserved bytes: %w", ErrCorrupt)
			}
			descriptor.Mutations[index] = Mutation{
				RecordID:        model.ID(binary.LittleEndian.Uint64(entry[0:8])),
				NewAddr:         recordlog.VAddr(binary.LittleEndian.Uint64(entry[8:16])),
				ExpectedOldAddr: recordlog.VAddr(binary.LittleEndian.Uint64(entry[16:24])),
				Operation:       Operation(entry[24]),
			}
			entryOffset += uint64(MutationSize)
		}
		descriptors = append(descriptors, descriptor)
		totalMutations += mutationCount
		offset += descriptorSize
	}
	if offset != uint64(len(src)) || totalMutations != wantMutations {
		return CommitGroup{}, fmt.Errorf("commit group trailing bytes or mutation count: %w", ErrCorrupt)
	}
	if err := validateDescriptors(descriptors); err != nil {
		return CommitGroup{}, fmt.Errorf("commit group semantics: %w", ErrCorrupt)
	}
	if descriptors[0].CommitSeq != firstSeq {
		return CommitGroup{}, fmt.Errorf("commit group first sequence: %w", ErrCorrupt)
	}
	return CommitGroup{Descriptors: descriptors}, nil
}

func EncodeAbort(record AbortRecord) ([]byte, error) {
	if record.BatchID == 0 {
		return nil, ErrInvalid
	}
	dst := make([]byte, FixedRecordSize)
	encodeHeader(dst, RecordTypeAbort, FixedRecordSize)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(record.BatchID))
	binary.LittleEndian.PutUint32(dst[24:28], record.Reason)
	return dst, nil
}

func DecodeAbort(src []byte) (AbortRecord, error) {
	if _, err := decodeHeader(src, RecordTypeAbort, FixedRecordSize); err != nil {
		return AbortRecord{}, err
	}
	record := AbortRecord{BatchID: model.BatchID(binary.LittleEndian.Uint64(src[16:24])), Reason: binary.LittleEndian.Uint32(src[24:28])}
	if record.BatchID == 0 || !zero(src[28:32]) {
		return AbortRecord{}, fmt.Errorf("abort fields: %w", ErrCorrupt)
	}
	return record, nil
}

func EncodeReserve(typ RecordType, record ReserveRecord) ([]byte, error) {
	if (typ != RecordTypeIDReserve && typ != RecordTypeBatchIDReserve) || record.HighExclusive == 0 {
		return nil, ErrInvalid
	}
	dst := make([]byte, FixedRecordSize)
	encodeHeader(dst, typ, FixedRecordSize)
	binary.LittleEndian.PutUint64(dst[16:24], record.HighExclusive)
	return dst, nil
}

func DecodeReserve(src []byte, typ RecordType) (ReserveRecord, error) {
	if typ != RecordTypeIDReserve && typ != RecordTypeBatchIDReserve {
		return ReserveRecord{}, ErrInvalid
	}
	if _, err := decodeHeader(src, typ, FixedRecordSize); err != nil {
		return ReserveRecord{}, err
	}
	record := ReserveRecord{HighExclusive: binary.LittleEndian.Uint64(src[16:24])}
	if record.HighExclusive == 0 || !zero(src[24:32]) {
		return ReserveRecord{}, fmt.Errorf("reserve fields: %w", ErrCorrupt)
	}
	return record, nil
}

func EncodeCheckpoint(record CheckpointMarker) []byte {
	dst := make([]byte, FixedRecordSize)
	encodeHeader(dst, RecordTypeCheckpoint, FixedRecordSize)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(record.CoveredCommitSeq))
	return dst
}

func DecodeCheckpoint(src []byte) (CheckpointMarker, error) {
	if _, err := decodeHeader(src, RecordTypeCheckpoint, FixedRecordSize); err != nil {
		return CheckpointMarker{}, err
	}
	if !zero(src[24:32]) {
		return CheckpointMarker{}, fmt.Errorf("checkpoint reserved bytes: %w", ErrCorrupt)
	}
	return CheckpointMarker{CoveredCommitSeq: model.CommitSeq(binary.LittleEndian.Uint64(src[16:24]))}, nil
}

func encodeHeader(dst []byte, typ RecordType, headerSize uint32) {
	copy(dst[0:4], protocolMagic[:])
	binary.LittleEndian.PutUint16(dst[4:6], FormatVersion)
	dst[6] = byte(typ)
	binary.LittleEndian.PutUint16(dst[8:10], uint16(headerSize))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(len(dst)))
}

func decodeHeader(src []byte, typ RecordType, headerSize uint32) (header, error) {
	if len(src) < int(CommonHeaderSize) || uint64(len(src)) > math.MaxUint32 || string(src[:4]) != string(protocolMagic[:]) {
		return header{}, fmt.Errorf("protocol magic or size: %w", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(src[4:6]) != FormatVersion {
		return header{}, ErrUnsupported
	}
	h := header{Type: RecordType(src[6]), HeaderSize: binary.LittleEndian.Uint16(src[8:10]), TotalSize: binary.LittleEndian.Uint32(src[12:16])}
	if h.Type != typ || src[7] != 0 || h.HeaderSize != uint16(headerSize) || binary.LittleEndian.Uint16(src[10:12]) != 0 || h.TotalSize != uint32(len(src)) || len(src) < int(headerSize) {
		return header{}, fmt.Errorf("protocol header fields: %w", ErrCorrupt)
	}
	return h, nil
}

func validateDescriptors(descriptors []Descriptor) error {
	if len(descriptors) == 0 {
		return ErrInvalid
	}
	var previousSeq model.CommitSeq
	batchIDs := make(map[model.BatchID]struct{}, len(descriptors))
	for i := range descriptors {
		descriptor := &descriptors[i]
		if descriptor.BatchID == 0 || descriptor.CommitSeq == 0 || (i != 0 && (previousSeq == model.CommitSeq(math.MaxUint64) || descriptor.CommitSeq != previousSeq+1)) {
			return ErrInvalid
		}
		if _, exists := batchIDs[descriptor.BatchID]; exists {
			return ErrInvalid
		}
		batchIDs[descriptor.BatchID] = struct{}{}
		if descriptor.Kind == DescriptorRelocation && len(descriptor.Mutations) == 0 {
			return ErrInvalid
		}
		previousSeq = descriptor.CommitSeq
		var previousID model.ID
		for index, mutation := range descriptor.Mutations {
			if mutation.RecordID == 0 || (index != 0 && mutation.RecordID <= previousID) {
				return ErrInvalid
			}
			previousID = mutation.RecordID
			switch descriptor.Kind {
			case DescriptorUserCommit:
				if mutation.Operation == OperationPut {
					if !mutation.NewAddr.Valid() || mutation.ExpectedOldAddr != 0 {
						return ErrInvalid
					}
				} else if mutation.Operation == OperationDelete {
					if mutation.NewAddr != 0 || mutation.ExpectedOldAddr != 0 {
						return ErrInvalid
					}
				} else {
					return ErrInvalid
				}
			case DescriptorRelocation:
				if mutation.Operation != OperationRelocate || !mutation.NewAddr.Valid() || !mutation.ExpectedOldAddr.Valid() || mutation.NewAddr == mutation.ExpectedOldAddr {
					return ErrInvalid
				}
			default:
				return ErrInvalid
			}
		}
	}
	return nil
}

func checkedSize(a, b uint64) (uint32, error) {
	if a > math.MaxUint32 || b > math.MaxUint32-a {
		return 0, ErrTooLarge
	}
	return uint32(a + b), nil
}

func checkedMul(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func zero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}
