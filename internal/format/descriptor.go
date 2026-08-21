package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

const DescriptorSealSize = 64

type MutationOperation uint8

const (
	MutationPut MutationOperation = iota + 1
	MutationDelete
	MutationRelocate
)

type DescriptorKind uint8

const (
	DescriptorCommit DescriptorKind = iota + 1
	DescriptorRelocation
)

type MutationEntry struct {
	RecordID        base.ID
	Operation       MutationOperation
	NewVAddr        base.VAddr
	ExpectedOldAddr base.VAddr
}

type DescriptorSeal struct {
	CommitSeq           base.CommitSeq
	PartCount           uint32
	MutationCount       uint32
	LogicalPayloadBytes uint64
	FirstPartFrameSeq   base.FrameSeq
	LastPartFrameSeq    base.FrameSeq
	DescriptorCRC       uint32
}

type DecodedDescriptor struct {
	Kind    DescriptorKind
	BatchID base.BatchID
	Seal    DescriptorSeal
	Entries []MutationEntry
}

func EncodeMutationEntries(kind DescriptorKind, entries []MutationEntry) ([]byte, error) {
	if len(entries) == 0 || uint64(len(entries)) > math.MaxUint32 {
		return nil, fmt.Errorf("mutation count: %w", base.ErrInvalidConfig)
	}
	encodedSize, err := base.MulUint64(uint64(len(entries)), MutationEntrySize)
	if err != nil {
		return nil, fmt.Errorf("mutation bytes: %w", err)
	}
	encodedSizeInt, err := base.Uint64ToInt(encodedSize)
	if err != nil {
		return nil, fmt.Errorf("mutation bytes: %w", err)
	}
	dst := make([]byte, encodedSizeInt)
	var previous base.ID
	for i, entry := range entries {
		if err := validateMutation(kind, entry, previous); err != nil {
			return nil, fmt.Errorf("mutation %d: %w", i, err)
		}
		off := i * MutationEntrySize
		binary.LittleEndian.PutUint64(dst[off:off+8], uint64(entry.RecordID))
		dst[off+8] = byte(entry.Operation)
		binary.LittleEndian.PutUint64(dst[off+16:off+24], uint64(entry.NewVAddr))
		binary.LittleEndian.PutUint64(dst[off+24:off+32], uint64(entry.ExpectedOldAddr))
		previous = entry.RecordID
	}
	return dst, nil
}

func DecodeMutationEntries(kind DescriptorKind, src []byte, maxMutations uint32) ([]MutationEntry, error) {
	if len(src) == 0 || len(src)%MutationEntrySize != 0 {
		return nil, corruptf("mutation payload size")
	}
	count := len(src) / MutationEntrySize
	if maxMutations == 0 || uint64(count) > uint64(maxMutations) {
		return nil, corruptf("mutation count limit")
	}
	entries := make([]MutationEntry, count)
	var previous base.ID
	for i := range entries {
		off := i * MutationEntrySize
		if !allZero(src[off+9 : off+16]) {
			return nil, corruptf("mutation reserved bytes")
		}
		entry := MutationEntry{
			RecordID:        base.ID(binary.LittleEndian.Uint64(src[off : off+8])),
			Operation:       MutationOperation(src[off+8]),
			NewVAddr:        base.VAddr(binary.LittleEndian.Uint64(src[off+16 : off+24])),
			ExpectedOldAddr: base.VAddr(binary.LittleEndian.Uint64(src[off+24 : off+32])),
		}
		if err := validateMutation(kind, entry, previous); err != nil {
			return nil, corruptf("mutation %d: %v", i, err)
		}
		entries[i] = entry
		previous = entry.RecordID
	}
	return entries, nil
}

func EncodeDescriptorSealPayload(seal DescriptorSeal, partPayloads [][]byte) ([DescriptorSealSize]byte, error) {
	var dst [DescriptorSealSize]byte
	if seal.DescriptorCRC != 0 {
		return dst, fmt.Errorf("descriptor CRC is output-only: %w", base.ErrInvalidConfig)
	}
	if err := validateSealFields(seal, partPayloads); err != nil {
		return dst, err
	}
	encodeSealFields(dst[:], seal, 0)
	crc := descriptorChecksum(partPayloads, dst[:])
	binary.LittleEndian.PutUint32(dst[40:44], crc)
	return dst, nil
}

func DecodeDescriptorSealPayload(src []byte, partPayloads [][]byte) (DescriptorSeal, error) {
	var seal DescriptorSeal
	if len(src) != DescriptorSealSize || binary.LittleEndian.Uint32(src[44:48]) != 0 || !allZero(src[48:64]) {
		return seal, corruptf("descriptor seal size, flags, or reserved bytes")
	}
	seal = DescriptorSeal{
		CommitSeq:           base.CommitSeq(binary.LittleEndian.Uint64(src[0:8])),
		PartCount:           binary.LittleEndian.Uint32(src[8:12]),
		MutationCount:       binary.LittleEndian.Uint32(src[12:16]),
		LogicalPayloadBytes: binary.LittleEndian.Uint64(src[16:24]),
		FirstPartFrameSeq:   base.FrameSeq(binary.LittleEndian.Uint64(src[24:32])),
		LastPartFrameSeq:    base.FrameSeq(binary.LittleEndian.Uint64(src[32:40])),
		DescriptorCRC:       binary.LittleEndian.Uint32(src[40:44]),
	}
	if err := validateSealFields(seal, partPayloads); err != nil {
		return DescriptorSeal{}, corruptf("descriptor seal fields: %v", err)
	}
	if descriptorChecksum(partPayloads, src) != seal.DescriptorCRC {
		return DescriptorSeal{}, corruptf("descriptor checksum")
	}
	return seal, nil
}

func ValidateDescriptorFrames(kind DescriptorKind, parts []Frame, sealFrame Frame, maxMutations uint32) (DecodedDescriptor, error) {
	wantPartType, wantSealType, err := descriptorFrameTypes(kind)
	if err != nil {
		return DecodedDescriptor{}, err
	}
	if sealFrame.Type != wantSealType || sealFrame.FrameSeq == 0 || sealFrame.BatchID == 0 || sealFrame.RecordID != 0 {
		return DecodedDescriptor{}, corruptf("descriptor seal frame identity")
	}
	payloads := make([][]byte, len(parts))
	entries := make([]MutationEntry, 0)
	for i, part := range parts {
		if part.Type != wantPartType || part.FrameSeq == 0 || part.BatchID != sealFrame.BatchID || part.RecordID != 0 {
			return DecodedDescriptor{}, corruptf("descriptor part frame identity")
		}
		if i != 0 && (parts[i-1].FrameSeq == base.FrameSeq(math.MaxUint64) || part.FrameSeq != parts[i-1].FrameSeq+1) {
			return DecodedDescriptor{}, corruptf("descriptor part sequence")
		}
		decoded, err := DecodeMutationEntries(kind, part.Payload, maxMutations)
		if err != nil {
			return DecodedDescriptor{}, err
		}
		if uint64(len(entries))+uint64(len(decoded)) > uint64(maxMutations) {
			return DecodedDescriptor{}, corruptf("descriptor mutation count limit")
		}
		entries = append(entries, decoded...)
		payloads[i] = part.Payload
	}
	seal, err := DecodeDescriptorSealPayload(sealFrame.Payload, payloads)
	if err != nil {
		return DecodedDescriptor{}, err
	}
	if len(parts) != 0 {
		if seal.FirstPartFrameSeq != parts[0].FrameSeq || seal.LastPartFrameSeq != parts[len(parts)-1].FrameSeq {
			return DecodedDescriptor{}, corruptf("descriptor seal part range")
		}
		if parts[len(parts)-1].FrameSeq == base.FrameSeq(math.MaxUint64) || parts[len(parts)-1].FrameSeq+1 != sealFrame.FrameSeq {
			return DecodedDescriptor{}, corruptf("descriptor seal adjacency")
		}
	}
	if uint64(len(entries)) != uint64(seal.MutationCount) {
		return DecodedDescriptor{}, corruptf("descriptor mutation count")
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].RecordID <= entries[i-1].RecordID {
			return DecodedDescriptor{}, corruptf("descriptor global mutation order")
		}
	}
	return DecodedDescriptor{Kind: kind, BatchID: sealFrame.BatchID, Seal: seal, Entries: entries}, nil
}

func validateMutation(kind DescriptorKind, entry MutationEntry, previous base.ID) error {
	if entry.RecordID == 0 || (previous != 0 && entry.RecordID <= previous) {
		return fmt.Errorf("record order: %w", base.ErrInvalidConfig)
	}
	switch kind {
	case DescriptorCommit:
		if entry.Operation == MutationPut {
			if !validVAddr(entry.NewVAddr) || entry.ExpectedOldAddr != 0 {
				return fmt.Errorf("put addresses: %w", base.ErrInvalidConfig)
			}
		} else if entry.Operation == MutationDelete {
			if entry.NewVAddr != 0 || entry.ExpectedOldAddr != 0 {
				return fmt.Errorf("delete addresses: %w", base.ErrInvalidConfig)
			}
		} else {
			return fmt.Errorf("commit operation: %w", base.ErrInvalidConfig)
		}
	case DescriptorRelocation:
		if entry.Operation != MutationRelocate || !validVAddr(entry.NewVAddr) || !validVAddr(entry.ExpectedOldAddr) {
			return fmt.Errorf("relocation operation or addresses: %w", base.ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("descriptor kind: %w", base.ErrInvalidConfig)
	}
	return nil
}

func validateSealFields(seal DescriptorSeal, partPayloads [][]byte) error {
	if seal.CommitSeq == 0 || uint64(len(partPayloads)) != uint64(seal.PartCount) {
		return fmt.Errorf("seal sequence or part count: %w", base.ErrInvalidConfig)
	}
	if seal.PartCount == 0 {
		if seal.MutationCount != 0 || seal.FirstPartFrameSeq != 0 || seal.LastPartFrameSeq != 0 {
			return fmt.Errorf("empty descriptor fields: %w", base.ErrInvalidConfig)
		}
		return nil
	}
	var mutationCount uint64
	for _, payload := range partPayloads {
		if len(payload) == 0 || len(payload)%MutationEntrySize != 0 {
			return fmt.Errorf("descriptor part payload size: %w", base.ErrInvalidConfig)
		}
		mutationCount += uint64(len(payload) / MutationEntrySize)
	}
	if seal.MutationCount == 0 || seal.FirstPartFrameSeq == 0 || seal.LastPartFrameSeq < seal.FirstPartFrameSeq ||
		uint64(seal.LastPartFrameSeq-seal.FirstPartFrameSeq)+1 != uint64(seal.PartCount) ||
		mutationCount != uint64(seal.MutationCount) {
		return fmt.Errorf("descriptor range: %w", base.ErrInvalidConfig)
	}
	return nil
}

func encodeSealFields(dst []byte, seal DescriptorSeal, crc uint32) {
	binary.LittleEndian.PutUint64(dst[0:8], uint64(seal.CommitSeq))
	binary.LittleEndian.PutUint32(dst[8:12], seal.PartCount)
	binary.LittleEndian.PutUint32(dst[12:16], seal.MutationCount)
	binary.LittleEndian.PutUint64(dst[16:24], seal.LogicalPayloadBytes)
	binary.LittleEndian.PutUint64(dst[24:32], uint64(seal.FirstPartFrameSeq))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(seal.LastPartFrameSeq))
	binary.LittleEndian.PutUint32(dst[40:44], crc)
}

func descriptorChecksum(parts [][]byte, sealPayload []byte) uint32 {
	h := crc32.New(castagnoliTable)
	for _, part := range parts {
		_, _ = h.Write(part)
	}
	_, _ = h.Write(sealPayload[:40])
	var zero [4]byte
	_, _ = h.Write(zero[:])
	_, _ = h.Write(sealPayload[44:])
	return h.Sum32()
}

func descriptorFrameTypes(kind DescriptorKind) (FrameType, FrameType, error) {
	switch kind {
	case DescriptorCommit:
		return FrameTypeCommitPart, FrameTypeCommitSeal, nil
	case DescriptorRelocation:
		return FrameTypeRelocationPart, FrameTypeRelocationSeal, nil
	default:
		return 0, 0, fmt.Errorf("descriptor kind: %w", base.ErrInvalidConfig)
	}
}

func validVAddr(addr base.VAddr) bool {
	_, err := base.ParseVAddr(uint64(addr))
	return err == nil
}
