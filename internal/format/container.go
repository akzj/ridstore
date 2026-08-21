package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

const (
	ContainerHeaderSize = 64
	tlvHeaderSize       = 8
	tlvRequiredFlag     = uint16(1)
)

type ContainerMagic [8]byte

var (
	ManifestMagic     = ContainerMagic{'R', 'I', 'D', 'M', 'A', 'N', '0', '1'}
	InitializingMagic = ContainerMagic{'R', 'I', 'D', 'I', 'N', 'I', 'T', '1'}
	MaintenanceMagic  = ContainerMagic{'R', 'I', 'D', 'J', 'N', 'L', '0', '1'}
	RotationMagic     = ContainerMagic{'R', 'I', 'D', 'R', 'O', 'T', '0', '1'}
)

type TLV struct {
	Type     uint16
	Required bool
	Value    []byte
}

type Container struct {
	Magic      ContainerMagic
	Generation uint64
	StoreUUID  base.StoreUUID
	TLVs       []TLV
}

type ContainerHeader struct {
	MajorVersion uint16
	MinorVersion uint16
	Generation   uint64
	StoreUUID    base.StoreUUID
	PayloadSize  uint64
}

// InspectContainerHeader validates the fixed header and declared file size but
// deliberately does not reject an unknown format version. Offline migration
// planning uses it to identify formats that the current decoder cannot open.
func InspectContainerHeader(src []byte, expectedMagic ContainerMagic, fileSize, maxPayloadSize uint64) (ContainerHeader, error) {
	var header ContainerHeader
	if len(src) < ContainerHeaderSize || fileSize < ContainerHeaderSize {
		return header, corruptf("container header truncated")
	}
	if !equalMagic(src[0:8], [8]byte(expectedMagic)) {
		return header, corruptf("container magic")
	}
	if binary.LittleEndian.Uint32(src[12:16]) != ContainerHeaderSize || !validChecksum(src[:ContainerHeaderSize], 52) {
		return header, corruptf("container header size or checksum")
	}
	if binary.LittleEndian.Uint32(src[56:60]) != 0 || binary.LittleEndian.Uint32(src[60:64]) != 0 {
		return header, corruptf("container flags or reserved bytes")
	}
	header = ContainerHeader{
		MajorVersion: binary.LittleEndian.Uint16(src[8:10]), MinorVersion: binary.LittleEndian.Uint16(src[10:12]),
		Generation: binary.LittleEndian.Uint64(src[16:24]), PayloadSize: binary.LittleEndian.Uint64(src[40:48]),
	}
	copy(header.StoreUUID[:], src[24:40])
	if header.Generation == 0 || header.StoreUUID == (base.StoreUUID{}) {
		return ContainerHeader{}, corruptf("container identity")
	}
	if maxPayloadSize == 0 || header.PayloadSize > maxPayloadSize || header.PayloadSize != fileSize-ContainerHeaderSize {
		return ContainerHeader{}, corruptf("container payload length")
	}
	return header, nil
}

func EncodeContainer(container Container) ([]byte, error) {
	if container.Generation == 0 || container.StoreUUID == (base.StoreUUID{}) {
		return nil, fmt.Errorf("container identity: %w", base.ErrInvalidConfig)
	}
	payloadSize := uint64(0)
	previousType := uint16(0)
	for i, item := range container.TLVs {
		if item.Type == 0 || (i != 0 && item.Type <= previousType) || uint64(len(item.Value)) > math.MaxUint32 {
			return nil, fmt.Errorf("TLV order, type, or length: %w", base.ErrInvalidConfig)
		}
		itemSize, err := base.Align8(tlvHeaderSize + uint64(len(item.Value)))
		if err != nil {
			return nil, err
		}
		payloadSize, err = base.AddUint64(payloadSize, itemSize)
		if err != nil {
			return nil, err
		}
		previousType = item.Type
	}
	totalSize, err := base.AddUint64(ContainerHeaderSize, payloadSize)
	if err != nil {
		return nil, err
	}
	totalSizeInt, err := base.Uint64ToInt(totalSize)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, totalSizeInt)
	copy(dst[0:8], container.Magic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatMajorVersion)
	binary.LittleEndian.PutUint16(dst[10:12], FormatMinorVersion)
	binary.LittleEndian.PutUint32(dst[12:16], ContainerHeaderSize)
	binary.LittleEndian.PutUint64(dst[16:24], container.Generation)
	copy(dst[24:40], container.StoreUUID[:])
	binary.LittleEndian.PutUint64(dst[40:48], payloadSize)

	offset := ContainerHeaderSize
	for _, item := range container.TLVs {
		binary.LittleEndian.PutUint16(dst[offset:offset+2], item.Type)
		if item.Required {
			binary.LittleEndian.PutUint16(dst[offset+2:offset+4], tlvRequiredFlag)
		}
		binary.LittleEndian.PutUint32(dst[offset+4:offset+8], uint32(len(item.Value)))
		copy(dst[offset+8:], item.Value)
		itemSize, _ := base.Align8(tlvHeaderSize + uint64(len(item.Value)))
		offset += int(itemSize)
	}
	binary.LittleEndian.PutUint32(dst[48:52], crc32.Checksum(dst[ContainerHeaderSize:], castagnoliTable))
	binary.LittleEndian.PutUint32(dst[52:56], crc32.Checksum(dst[:ContainerHeaderSize], castagnoliTable))
	return dst, nil
}

func DecodeContainer(src []byte, expectedMagic ContainerMagic, maxPayloadSize uint64) (Container, error) {
	var container Container
	header, err := InspectContainerHeader(src, expectedMagic, uint64(len(src)), maxPayloadSize)
	if err != nil {
		return container, err
	}
	major, minor := header.MajorVersion, header.MinorVersion
	if major != FormatMajorVersion || minor > FormatMinorVersion {
		return container, fmt.Errorf("container version %d.%d: %w", major, minor, base.ErrUnsupported)
	}
	if crc32.Checksum(src[ContainerHeaderSize:], castagnoliTable) != binary.LittleEndian.Uint32(src[48:52]) {
		return container, corruptf("container payload checksum")
	}
	container = Container{
		Magic: expectedMagic, Generation: header.Generation, StoreUUID: header.StoreUUID,
	}

	seen := make(map[uint16]struct{})
	for offset := ContainerHeaderSize; offset < len(src); {
		if len(src)-offset < tlvHeaderSize {
			return Container{}, corruptf("TLV header truncated")
		}
		typ := binary.LittleEndian.Uint16(src[offset : offset+2])
		flags := binary.LittleEndian.Uint16(src[offset+2 : offset+4])
		length := binary.LittleEndian.Uint32(src[offset+4 : offset+8])
		if typ == 0 || flags&^tlvRequiredFlag != 0 {
			return Container{}, corruptf("TLV type or flags")
		}
		if _, exists := seen[typ]; exists {
			return Container{}, corruptf("duplicate TLV type %d", typ)
		}
		itemSize, err := base.Align8(tlvHeaderSize + uint64(length))
		if err != nil || itemSize > uint64(len(src)-offset) {
			return Container{}, corruptf("TLV length")
		}
		valueEnd := offset + tlvHeaderSize + int(length)
		itemEnd := offset + int(itemSize)
		if !allZero(src[valueEnd:itemEnd]) {
			return Container{}, corruptf("TLV padding")
		}
		container.TLVs = append(container.TLVs, TLV{
			Type: typ, Required: flags&tlvRequiredFlag != 0, Value: src[offset+tlvHeaderSize : valueEnd],
		})
		seen[typ] = struct{}{}
		offset = itemEnd
	}
	return container, nil
}
