package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

const (
	FormatMajorVersion = uint16(1)
	FormatMinorVersion = uint16(0)
	SegmentHeaderSize  = 4096
	SegmentFooterSize  = 4096
)

var (
	dataSegmentMagic    = [8]byte{'R', 'I', 'D', 'S', 'E', 'G', '0', '1'}
	mappingSegmentMagic = [8]byte{'R', 'I', 'D', 'M', 'A', 'P', '0', '1'}
	dataFooterMagic     = [8]byte{'R', 'I', 'D', 'E', 'N', 'D', '0', '1'}
	mappingFooterMagic  = [8]byte{'R', 'I', 'D', 'M', 'E', 'N', 'D', '1'}
	castagnoliTable     = crc32.MakeTable(crc32.Castagnoli)
)

type SegmentKind uint8

const (
	SegmentKindData SegmentKind = iota + 1
	SegmentKindMapping
)

type StoreUUID = base.StoreUUID

type SegmentHeader struct {
	Kind            SegmentKind
	StoreUUID       StoreUUID
	FileID          uint32
	CreatedUnixNano uint64
	FirstSeq        uint64
}

type DataSegmentFooter struct {
	SegmentID     base.DataSegmentID
	ValidDataEnd  uint64
	FirstFrameSeq base.FrameSeq
	LastFrameSeq  base.FrameSeq
	FrameCount    uint64
	MinCommitSeq  base.CommitSeq
	MaxCommitSeq  base.CommitSeq
}

type MappingSegmentFooter struct {
	SegmentID    base.MapSegmentID
	ValidNodeEnd uint64
	FirstNodeSeq base.NodeSeq
	LastNodeSeq  base.NodeSeq
	NodeCount    uint64
}

func EncodeSegmentHeader(h SegmentHeader) ([SegmentHeaderSize]byte, error) {
	var dst [SegmentHeaderSize]byte
	magic, err := segmentMagic(h.Kind)
	if err != nil {
		return dst, err
	}
	if h.StoreUUID == (StoreUUID{}) {
		return dst, fmt.Errorf("segment header store UUID: %w", base.ErrInvalidConfig)
	}
	if h.FileID == 0 || h.FirstSeq == 0 {
		return dst, fmt.Errorf("segment header identity: %w", base.ErrInvalidConfig)
	}

	copy(dst[0:8], magic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatMajorVersion)
	binary.LittleEndian.PutUint16(dst[10:12], FormatMinorVersion)
	binary.LittleEndian.PutUint32(dst[12:16], SegmentHeaderSize)
	copy(dst[16:32], h.StoreUUID[:])
	binary.LittleEndian.PutUint32(dst[32:36], h.FileID)
	binary.LittleEndian.PutUint64(dst[40:48], h.CreatedUnixNano)
	binary.LittleEndian.PutUint64(dst[48:56], h.FirstSeq)
	binary.LittleEndian.PutUint32(dst[64:68], crc32.Checksum(dst[:], castagnoliTable))
	return dst, nil
}

func DecodeSegmentHeader(src []byte) (SegmentHeader, error) {
	var h SegmentHeader
	if len(src) != SegmentHeaderSize {
		return h, corruptf("segment header size %d", len(src))
	}

	kind, err := kindFromSegmentMagic(src[0:8])
	if err != nil {
		return h, err
	}
	if binary.LittleEndian.Uint32(src[12:16]) != SegmentHeaderSize {
		return h, corruptf("segment header declared size")
	}
	if !validChecksum(src, 64) {
		return h, corruptf("segment header checksum")
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatMajorVersion ||
		binary.LittleEndian.Uint16(src[10:12]) > FormatMinorVersion {
		return h, fmt.Errorf("segment header version %d.%d: %w",
			binary.LittleEndian.Uint16(src[8:10]),
			binary.LittleEndian.Uint16(src[10:12]),
			base.ErrUnsupported)
	}
	if binary.LittleEndian.Uint32(src[36:40]) != 0 ||
		binary.LittleEndian.Uint64(src[56:64]) != 0 ||
		!allZero(src[68:]) {
		return h, corruptf("segment header flags or reserved bytes")
	}

	h.Kind = kind
	copy(h.StoreUUID[:], src[16:32])
	h.FileID = binary.LittleEndian.Uint32(src[32:36])
	h.CreatedUnixNano = binary.LittleEndian.Uint64(src[40:48])
	h.FirstSeq = binary.LittleEndian.Uint64(src[48:56])
	if h.StoreUUID == (StoreUUID{}) || h.FileID == 0 || h.FirstSeq == 0 {
		return SegmentHeader{}, corruptf("segment header identity")
	}
	return h, nil
}

func EncodeDataSegmentFooter(f DataSegmentFooter) ([SegmentFooterSize]byte, error) {
	var dst [SegmentFooterSize]byte
	if err := validateDataFooter(f); err != nil {
		return dst, err
	}
	copy(dst[0:8], dataFooterMagic[:])
	binary.LittleEndian.PutUint32(dst[8:12], uint32(f.SegmentID))
	binary.LittleEndian.PutUint32(dst[12:16], SegmentFooterSize)
	binary.LittleEndian.PutUint64(dst[16:24], f.ValidDataEnd)
	binary.LittleEndian.PutUint64(dst[24:32], uint64(f.FirstFrameSeq))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(f.LastFrameSeq))
	binary.LittleEndian.PutUint64(dst[40:48], f.FrameCount)
	binary.LittleEndian.PutUint64(dst[48:56], uint64(f.MinCommitSeq))
	binary.LittleEndian.PutUint64(dst[56:64], uint64(f.MaxCommitSeq))
	binary.LittleEndian.PutUint32(dst[64:68], crc32.Checksum(dst[:], castagnoliTable))
	return dst, nil
}

func DecodeDataSegmentFooter(src []byte) (DataSegmentFooter, error) {
	var f DataSegmentFooter
	if len(src) != SegmentFooterSize {
		return f, corruptf("data footer size %d", len(src))
	}
	if !equalMagic(src[0:8], dataFooterMagic) {
		return f, corruptf("data footer magic")
	}
	if binary.LittleEndian.Uint32(src[12:16]) != SegmentFooterSize {
		return f, corruptf("data footer declared size")
	}
	if !validChecksum(src, 64) {
		return f, corruptf("data footer checksum")
	}
	if !allZero(src[68:]) {
		return f, corruptf("data footer reserved bytes")
	}
	f = DataSegmentFooter{
		SegmentID:     base.DataSegmentID(binary.LittleEndian.Uint32(src[8:12])),
		ValidDataEnd:  binary.LittleEndian.Uint64(src[16:24]),
		FirstFrameSeq: base.FrameSeq(binary.LittleEndian.Uint64(src[24:32])),
		LastFrameSeq:  base.FrameSeq(binary.LittleEndian.Uint64(src[32:40])),
		FrameCount:    binary.LittleEndian.Uint64(src[40:48]),
		MinCommitSeq:  base.CommitSeq(binary.LittleEndian.Uint64(src[48:56])),
		MaxCommitSeq:  base.CommitSeq(binary.LittleEndian.Uint64(src[56:64])),
	}
	if err := validateDataFooter(f); err != nil {
		return DataSegmentFooter{}, corruptf("data footer fields: %v", err)
	}
	return f, nil
}

func EncodeMappingSegmentFooter(f MappingSegmentFooter) ([SegmentFooterSize]byte, error) {
	var dst [SegmentFooterSize]byte
	if err := validateMappingFooter(f); err != nil {
		return dst, err
	}
	copy(dst[0:8], mappingFooterMagic[:])
	binary.LittleEndian.PutUint32(dst[8:12], uint32(f.SegmentID))
	binary.LittleEndian.PutUint32(dst[12:16], SegmentFooterSize)
	binary.LittleEndian.PutUint64(dst[16:24], f.ValidNodeEnd)
	binary.LittleEndian.PutUint64(dst[24:32], uint64(f.FirstNodeSeq))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(f.LastNodeSeq))
	binary.LittleEndian.PutUint64(dst[40:48], f.NodeCount)
	binary.LittleEndian.PutUint32(dst[48:52], crc32.Checksum(dst[:], castagnoliTable))
	return dst, nil
}

func DecodeMappingSegmentFooter(src []byte) (MappingSegmentFooter, error) {
	var f MappingSegmentFooter
	if len(src) != SegmentFooterSize {
		return f, corruptf("mapping footer size %d", len(src))
	}
	if !equalMagic(src[0:8], mappingFooterMagic) {
		return f, corruptf("mapping footer magic")
	}
	if binary.LittleEndian.Uint32(src[12:16]) != SegmentFooterSize {
		return f, corruptf("mapping footer declared size")
	}
	if !validChecksum(src, 48) {
		return f, corruptf("mapping footer checksum")
	}
	if !allZero(src[52:]) {
		return f, corruptf("mapping footer reserved bytes")
	}
	f = MappingSegmentFooter{
		SegmentID:    base.MapSegmentID(binary.LittleEndian.Uint32(src[8:12])),
		ValidNodeEnd: binary.LittleEndian.Uint64(src[16:24]),
		FirstNodeSeq: base.NodeSeq(binary.LittleEndian.Uint64(src[24:32])),
		LastNodeSeq:  base.NodeSeq(binary.LittleEndian.Uint64(src[32:40])),
		NodeCount:    binary.LittleEndian.Uint64(src[40:48]),
	}
	if err := validateMappingFooter(f); err != nil {
		return MappingSegmentFooter{}, corruptf("mapping footer fields: %v", err)
	}
	return f, nil
}

func validateDataFooter(f DataSegmentFooter) error {
	if f.SegmentID == 0 || f.ValidDataEnd <= SegmentHeaderSize || f.ValidDataEnd > math.MaxUint32 || f.ValidDataEnd%8 != 0 ||
		f.FirstFrameSeq == 0 || f.LastFrameSeq < f.FirstFrameSeq || f.FrameCount == 0 ||
		f.FrameCount > uint64(f.LastFrameSeq-f.FirstFrameSeq)+1 {
		return fmt.Errorf("invalid data footer: %w", base.ErrInvalidConfig)
	}
	if (f.MinCommitSeq == 0) != (f.MaxCommitSeq == 0) || f.MaxCommitSeq < f.MinCommitSeq {
		return fmt.Errorf("invalid data footer commit range: %w", base.ErrInvalidConfig)
	}
	return nil
}

func validateMappingFooter(f MappingSegmentFooter) error {
	if f.SegmentID == 0 || f.ValidNodeEnd <= SegmentHeaderSize || f.ValidNodeEnd > math.MaxUint32 || f.ValidNodeEnd%8 != 0 ||
		f.FirstNodeSeq == 0 || f.LastNodeSeq < f.FirstNodeSeq || f.NodeCount == 0 ||
		f.NodeCount > uint64(f.LastNodeSeq-f.FirstNodeSeq)+1 {
		return fmt.Errorf("invalid mapping footer: %w", base.ErrInvalidConfig)
	}
	return nil
}

func segmentMagic(kind SegmentKind) ([8]byte, error) {
	switch kind {
	case SegmentKindData:
		return dataSegmentMagic, nil
	case SegmentKindMapping:
		return mappingSegmentMagic, nil
	default:
		return [8]byte{}, fmt.Errorf("segment kind %d: %w", kind, base.ErrInvalidConfig)
	}
}

func kindFromSegmentMagic(src []byte) (SegmentKind, error) {
	switch {
	case equalMagic(src, dataSegmentMagic):
		return SegmentKindData, nil
	case equalMagic(src, mappingSegmentMagic):
		return SegmentKindMapping, nil
	default:
		return 0, corruptf("segment header magic")
	}
}

func equalMagic(src []byte, magic [8]byte) bool {
	if len(src) != len(magic) {
		return false
	}
	for i := range magic {
		if src[i] != magic[i] {
			return false
		}
	}
	return true
}

func validChecksum(src []byte, offset int) bool {
	const checksumSize = 4
	if offset < 0 || offset+checksumSize > len(src) {
		return false
	}
	want := binary.LittleEndian.Uint32(src[offset : offset+checksumSize])
	h := crc32.New(castagnoliTable)
	_, _ = h.Write(src[:offset])
	var zero [checksumSize]byte
	_, _ = h.Write(zero[:])
	_, _ = h.Write(src[offset+checksumSize:])
	return h.Sum32() == want
}

func allZero(src []byte) bool {
	for _, b := range src {
		if b != 0 {
			return false
		}
	}
	return true
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), base.ErrCorrupt)
}
