package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"

	"github.com/akzj/ridstore/internal/base"
)

const (
	MappingNodeHeaderSize = 64
	MappingNodeSlots      = 512
	SparseDenseThreshold  = 504
)

var mappingNodeMagic = [8]byte{'R', 'I', 'D', 'N', 'O', 'D', 'E', '1'}

type NodeEncoding uint8

const (
	NodeEncodingAuto NodeEncoding = iota
	NodeEncodingSparseBitmap
	NodeEncodingDense512
)

type MappingNode struct {
	Level            uint8
	Encoding         NodeEncoding
	NodeSeq          base.NodeSeq
	Prefix           uint64
	CoveredCommitSeq base.CommitSeq
	EntryCount       uint16
	Bitmap           [8]uint64
	Values           []uint64
}

type MappingNodeBuild struct {
	Level            uint8
	Encoding         NodeEncoding
	NodeSeq          base.NodeSeq
	Prefix           uint64
	CoveredCommitSeq base.CommitSeq
	Slots            [MappingNodeSlots]uint64
}

func EncodeMappingNode(node MappingNodeBuild) ([]byte, error) {
	count := countNonZero(node.Slots[:])
	if count == 0 || node.NodeSeq == 0 || !validNodePrefix(node.Level, node.Prefix) || !validTopSlots(node.Level, node.Slots[:]) {
		return nil, fmt.Errorf("mapping node identity or slots: %w", base.ErrInvalidConfig)
	}
	encoding := node.Encoding
	if encoding == NodeEncodingAuto {
		if count < SparseDenseThreshold {
			encoding = NodeEncodingSparseBitmap
		} else {
			encoding = NodeEncodingDense512
		}
	}
	if encoding != NodeEncodingSparseBitmap && encoding != NodeEncodingDense512 {
		return nil, fmt.Errorf("mapping node encoding: %w", base.ErrInvalidConfig)
	}
	if err := validateNodeValues(node.Level, node.Slots[:]); err != nil {
		return nil, err
	}

	nodeSize := MappingNodeHeaderSize + MappingNodeSlots*8
	if encoding == NodeEncodingSparseBitmap {
		nodeSize = MappingNodeHeaderSize + 64 + count*8
	}
	dst := make([]byte, nodeSize)
	copy(dst[0:8], mappingNodeMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatMajorVersion)
	dst[10] = node.Level
	dst[11] = byte(encoding)
	binary.LittleEndian.PutUint32(dst[12:16], uint32(nodeSize))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(node.NodeSeq))
	binary.LittleEndian.PutUint64(dst[24:32], node.Prefix)
	binary.LittleEndian.PutUint64(dst[32:40], uint64(node.CoveredCommitSeq))
	binary.LittleEndian.PutUint16(dst[40:42], uint16(count))

	payload := dst[MappingNodeHeaderSize:]
	if encoding == NodeEncodingSparseBitmap {
		valueOffset := 64
		for slot, value := range node.Slots {
			if value == 0 {
				continue
			}
			word := slot / 64
			binary.LittleEndian.PutUint64(payload[word*8:word*8+8], binary.LittleEndian.Uint64(payload[word*8:word*8+8])|uint64(1)<<uint(slot%64))
			binary.LittleEndian.PutUint64(payload[valueOffset:valueOffset+8], value)
			valueOffset += 8
		}
	} else {
		for slot, value := range node.Slots {
			binary.LittleEndian.PutUint64(payload[slot*8:slot*8+8], value)
		}
	}
	binary.LittleEndian.PutUint32(dst[44:48], crc32.Checksum(payload, castagnoliTable))
	binary.LittleEndian.PutUint32(dst[48:52], crc32.Checksum(dst[:MappingNodeHeaderSize], castagnoliTable))
	return dst, nil
}

func DecodeMappingNode(src []byte, remainingSegmentSize uint64) (MappingNode, int, error) {
	var node MappingNode
	if len(src) < MappingNodeHeaderSize {
		return node, 0, corruptf("mapping node header truncated")
	}
	if !equalMagic(src[0:8], mappingNodeMagic) {
		return node, 0, corruptf("mapping node magic")
	}
	if !validChecksum(src[:MappingNodeHeaderSize], 48) {
		return node, 0, corruptf("mapping node header checksum")
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatMajorVersion {
		return node, 0, fmt.Errorf("mapping node version %d: %w", binary.LittleEndian.Uint16(src[8:10]), base.ErrUnsupported)
	}
	if binary.LittleEndian.Uint16(src[42:44]) != 0 || binary.LittleEndian.Uint32(src[52:56]) != 0 || binary.LittleEndian.Uint64(src[56:64]) != 0 {
		return node, 0, corruptf("mapping node flags or reserved bytes")
	}
	nodeSize := binary.LittleEndian.Uint32(src[12:16])
	if nodeSize < MappingNodeHeaderSize || nodeSize%8 != 0 || uint64(nodeSize) > remainingSegmentSize || uint64(nodeSize) > uint64(len(src)) {
		return node, 0, corruptf("mapping node size or segment boundary")
	}
	node.Level = src[10]
	node.Encoding = NodeEncoding(src[11])
	node.NodeSeq = base.NodeSeq(binary.LittleEndian.Uint64(src[16:24]))
	node.Prefix = binary.LittleEndian.Uint64(src[24:32])
	node.CoveredCommitSeq = base.CommitSeq(binary.LittleEndian.Uint64(src[32:40]))
	node.EntryCount = binary.LittleEndian.Uint16(src[40:42])
	if node.NodeSeq == 0 || node.EntryCount == 0 || node.EntryCount > MappingNodeSlots || !validNodePrefix(node.Level, node.Prefix) {
		return MappingNode{}, 0, corruptf("mapping node identity")
	}
	payload := src[MappingNodeHeaderSize:nodeSize]
	if crc32.Checksum(payload, castagnoliTable) != binary.LittleEndian.Uint32(src[44:48]) {
		return MappingNode{}, 0, corruptf("mapping node payload checksum")
	}

	switch node.Encoding {
	case NodeEncodingSparseBitmap:
		wantSize := MappingNodeHeaderSize + 64 + int(node.EntryCount)*8
		if int(nodeSize) != wantSize {
			return MappingNode{}, 0, corruptf("sparse mapping node size")
		}
		for i := range node.Bitmap {
			node.Bitmap[i] = binary.LittleEndian.Uint64(payload[i*8 : i*8+8])
		}
		if bitmapCount(node.Bitmap) != int(node.EntryCount) || (node.Level == 7 && (node.Bitmap[0]&^3 != 0 || !allZeroUint64(node.Bitmap[1:]))) {
			return MappingNode{}, 0, corruptf("sparse mapping node bitmap")
		}
		node.Values = make([]uint64, node.EntryCount)
		for i := range node.Values {
			node.Values[i] = binary.LittleEndian.Uint64(payload[64+i*8 : 72+i*8])
		}
	case NodeEncodingDense512:
		if nodeSize != MappingNodeHeaderSize+MappingNodeSlots*8 {
			return MappingNode{}, 0, corruptf("dense mapping node size")
		}
		node.Values = make([]uint64, MappingNodeSlots)
		for i := range node.Values {
			node.Values[i] = binary.LittleEndian.Uint64(payload[i*8 : i*8+8])
		}
		if countNonZero(node.Values) != int(node.EntryCount) || !validTopSlots(node.Level, node.Values) {
			return MappingNode{}, 0, corruptf("dense mapping node entries")
		}
	default:
		return MappingNode{}, 0, fmt.Errorf("mapping node encoding %d: %w", node.Encoding, base.ErrUnsupported)
	}
	if err := validateNodeValues(node.Level, node.Values); err != nil {
		return MappingNode{}, 0, corruptf("mapping node values: %v", err)
	}
	return node, int(nodeSize), nil
}

func (node MappingNode) Lookup(slot uint16) (uint64, bool) {
	if slot >= MappingNodeSlots {
		return 0, false
	}
	if node.Encoding == NodeEncodingDense512 {
		value := node.Values[slot]
		return value, value != 0
	}
	word, bit := slot/64, slot%64
	if node.Bitmap[word]&(uint64(1)<<bit) == 0 {
		return 0, false
	}
	index := 0
	for i := uint16(0); i < word; i++ {
		index += bits.OnesCount64(node.Bitmap[i])
	}
	index += bits.OnesCount64(node.Bitmap[word] & ((uint64(1) << bit) - 1))
	return node.Values[index], true
}

func validNodePrefix(level uint8, prefix uint64) bool {
	if level > 7 {
		return false
	}
	if level == 7 {
		return prefix == 0
	}
	bitsUsed := uint(64 - 9*uint(level+1))
	return prefix < uint64(1)<<bitsUsed
}

func validateNodeValues(level uint8, values []uint64) error {
	for _, value := range values {
		if value == 0 {
			continue
		}
		var err error
		if level == 0 {
			_, err = base.ParseVAddr(value)
		} else {
			_, err = base.ParseMapAddr(value)
		}
		if err != nil {
			return fmt.Errorf("mapping node address: %w", base.ErrInvalidConfig)
		}
	}
	return nil
}

func countNonZero(values []uint64) int {
	count := 0
	for _, value := range values {
		if value != 0 {
			count++
		}
	}
	return count
}

func bitmapCount(bitmap [8]uint64) int {
	count := 0
	for _, word := range bitmap {
		count += bits.OnesCount64(word)
	}
	return count
}

func validTopSlots(level uint8, values []uint64) bool {
	if level != 7 {
		return true
	}
	for i := 2; i < len(values); i++ {
		if values[i] != 0 {
			return false
		}
	}
	return true
}

func allZeroUint64(values []uint64) bool {
	for _, value := range values {
		if value != 0 {
			return false
		}
	}
	return true
}
