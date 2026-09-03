package mapstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/bits"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

const (
	NodeHeaderSize        = uint32(64)
	NodeSlots             = uint16(512)
	SparseBitmapBytes     = uint32(64)
	DenseInternalNodeSize = NodeHeaderSize + uint32(NodeSlots)*8
	DenseLeafNodeSize     = NodeHeaderSize + uint32(NodeSlots)*12
	// DenseNodeSize is the maximum encoded node size and remains the capacity
	// planning bound used by MapStore and GC admission.
	DenseNodeSize       = DenseLeafNodeSize
	SparseDenseBoundary = uint16(504)
	MaxLevel            = uint8(7)
	FormatVersion       = uint16(3)
)

var (
	nodeMagic = [8]byte{'R', 'I', 'D', 'M', 'A', 'P', 'N', '2'}
	crcTable  = crc32.MakeTable(crc32.Castagnoli)
)

type Encoding uint8

const (
	EncodingSparse Encoding = 1
	EncodingDense  Encoding = 2
)

// Node is an immutable radix node. Sparse Values are packed in ascending slot
// order; Dense Values have exactly NodeSlots elements.
type Node struct {
	Level            uint8
	Encoding         Encoding
	NodeSeq          uint64
	Prefix           uint64
	CoveredCommitSeq model.CommitSeq
	EntryCount       uint16
	Bitmap           [8]uint64
	Values           []uint64
	Sizes            []uint32
}

type NodeBuild struct {
	Level            uint8
	NodeSeq          uint64
	Prefix           uint64
	CoveredCommitSeq model.CommitSeq
	Slots            [NodeSlots]uint64
	Sizes            [NodeSlots]uint32
}

// PhysicalSize returns the exact encoded bytes occupied by a verified Node.
func (n Node) PhysicalSize() uint32 {
	size, _ := EncodedNodeSize(n.Level, n.EntryCount)
	return size
}

// EncodedNodeSize returns the exact bytes for a valid node shape.
func EncodedNodeSize(level uint8, entries uint16) (uint32, error) {
	if level > MaxLevel || entries == 0 || entries > NodeSlots {
		return 0, ErrInvalid
	}
	if entries >= SparseDenseBoundary {
		if level == 0 {
			return DenseLeafNodeSize, nil
		}
		return DenseInternalNodeSize, nil
	}
	width := uint32(8)
	if level == 0 {
		width = 12
	}
	return alignNodeSize(NodeHeaderSize + SparseBitmapBytes + uint32(entries)*width), nil
}

func EncodeNode(build NodeBuild) ([]byte, error) {
	if build.NodeSeq == 0 || build.CoveredCommitSeq == 0 || !validPrefix(build.Level, build.Prefix) {
		return nil, ErrInvalid
	}
	count := countValues(build.Slots[:])
	if count == 0 || (build.Level == MaxLevel && !topSlotsEmpty(build.Slots[:])) {
		return nil, ErrInvalid
	}
	if err := validateValues(build.Level, build.Slots[:], build.Sizes[:]); err != nil {
		return nil, err
	}

	encoding := EncodingSparse
	width := uint32(8)
	if build.Level == 0 {
		width = 12
	}
	size := alignNodeSize(NodeHeaderSize + SparseBitmapBytes + uint32(count)*width)
	if count >= int(SparseDenseBoundary) {
		encoding = EncodingDense
		size = DenseInternalNodeSize
		if build.Level == 0 {
			size = DenseLeafNodeSize
		}
	}
	dst := make([]byte, size)
	copy(dst[0:8], nodeMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatVersion)
	dst[10] = build.Level
	dst[11] = byte(encoding)
	binary.LittleEndian.PutUint32(dst[12:16], size)
	binary.LittleEndian.PutUint64(dst[16:24], build.NodeSeq)
	binary.LittleEndian.PutUint64(dst[24:32], build.Prefix)
	binary.LittleEndian.PutUint64(dst[32:40], uint64(build.CoveredCommitSeq))
	binary.LittleEndian.PutUint16(dst[40:42], uint16(count))

	payload := dst[NodeHeaderSize:]
	if encoding == EncodingSparse {
		valueOffset := SparseBitmapBytes
		for slot, value := range build.Slots {
			if value == 0 {
				continue
			}
			word := slot / 64
			current := binary.LittleEndian.Uint64(payload[word*8 : word*8+8])
			binary.LittleEndian.PutUint64(payload[word*8:word*8+8], current|uint64(1)<<uint(slot%64))
			binary.LittleEndian.PutUint64(payload[valueOffset:valueOffset+8], value)
			valueOffset += 8
			if build.Level == 0 {
				binary.LittleEndian.PutUint32(payload[valueOffset:valueOffset+4], build.Sizes[slot])
				valueOffset += 4
			}
		}
	} else {
		for slot, value := range build.Slots {
			if build.Level == 0 {
				offset := uint32(slot) * 12
				binary.LittleEndian.PutUint64(payload[offset:offset+8], value)
				binary.LittleEndian.PutUint32(payload[offset+8:offset+12], build.Sizes[slot])
			} else {
				binary.LittleEndian.PutUint64(payload[slot*8:slot*8+8], value)
			}
		}
	}
	binary.LittleEndian.PutUint32(dst[44:48], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(dst[48:52], headerChecksum(dst[:NodeHeaderSize]))
	return dst, nil
}

// DecodeNode validates one complete encoded node. segmentRemaining is the
// number of bytes from this node's MapAddr to the containing segment's valid
// end, preventing a valid-looking size from crossing a file boundary.
func DecodeNode(src []byte, segmentRemaining uint32) (Node, uint32, error) {
	node, size, err := decodeNodeHeader(src, segmentRemaining)
	if err != nil {
		return Node{}, 0, err
	}
	header := src[:NodeHeaderSize]
	if uint64(size) > uint64(len(src)) {
		return Node{}, 0, ErrCorrupt
	}
	payload := src[NodeHeaderSize:size]
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(header[44:48]) {
		return Node{}, 0, ErrCorrupt
	}
	switch node.Encoding {
	case EncodingSparse:
		for index := range node.Bitmap {
			node.Bitmap[index] = binary.LittleEndian.Uint64(payload[index*8 : index*8+8])
		}
		if bitmapCount(node.Bitmap) != int(node.EntryCount) || (node.Level == MaxLevel && !topBitmapEmpty(node.Bitmap)) {
			return Node{}, 0, ErrCorrupt
		}
		node.Values = make([]uint64, node.EntryCount)
		if node.Level == 0 {
			node.Sizes = make([]uint32, node.EntryCount)
		}
		valueOffset := SparseBitmapBytes
		for index := range node.Values {
			node.Values[index] = binary.LittleEndian.Uint64(payload[valueOffset : valueOffset+8])
			valueOffset += 8
			if node.Level == 0 {
				node.Sizes[index] = binary.LittleEndian.Uint32(payload[valueOffset : valueOffset+4])
				valueOffset += 4
			}
		}
	case EncodingDense:
		node.Values = make([]uint64, NodeSlots)
		if node.Level == 0 {
			node.Sizes = make([]uint32, NodeSlots)
		}
		for index := range node.Values {
			if node.Level == 0 {
				offset := uint32(index) * 12
				node.Values[index] = binary.LittleEndian.Uint64(payload[offset : offset+8])
				node.Sizes[index] = binary.LittleEndian.Uint32(payload[offset+8 : offset+12])
			} else {
				node.Values[index] = binary.LittleEndian.Uint64(payload[index*8 : index*8+8])
			}
		}
		if countValues(node.Values) != int(node.EntryCount) || (node.Level == MaxLevel && !topSlotsEmpty(node.Values)) {
			return Node{}, 0, ErrCorrupt
		}
	}
	if err := validateValues(node.Level, node.Values, node.Sizes); err != nil {
		return Node{}, 0, errors.Join(ErrCorrupt, err)
	}
	return node, size, nil
}

func decodeNodeHeader(src []byte, segmentRemaining uint32) (Node, uint32, error) {
	if len(src) < int(NodeHeaderSize) {
		return Node{}, 0, ErrCorrupt
	}
	header := src[:NodeHeaderSize]
	if string(header[:8]) != string(nodeMagic[:]) || binary.LittleEndian.Uint32(header[48:52]) != headerChecksum(header) {
		return Node{}, 0, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(header[8:10]) != FormatVersion {
		return Node{}, 0, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(header[42:44]) != 0 || binary.LittleEndian.Uint32(header[52:56]) != 0 || binary.LittleEndian.Uint64(header[56:64]) != 0 {
		return Node{}, 0, ErrCorrupt
	}
	size := binary.LittleEndian.Uint32(header[12:16])
	if size < NodeHeaderSize || size&7 != 0 || size > segmentRemaining {
		return Node{}, 0, ErrCorrupt
	}
	node := Node{
		Level:            header[10],
		Encoding:         Encoding(header[11]),
		NodeSeq:          binary.LittleEndian.Uint64(header[16:24]),
		Prefix:           binary.LittleEndian.Uint64(header[24:32]),
		CoveredCommitSeq: model.CommitSeq(binary.LittleEndian.Uint64(header[32:40])),
		EntryCount:       binary.LittleEndian.Uint16(header[40:42]),
	}
	if node.NodeSeq == 0 || node.CoveredCommitSeq == 0 || node.EntryCount == 0 || node.EntryCount > NodeSlots || !validPrefix(node.Level, node.Prefix) {
		return Node{}, 0, ErrCorrupt
	}
	switch node.Encoding {
	case EncodingSparse:
		width := uint32(8)
		if node.Level == 0 {
			width = 12
		}
		want := alignNodeSize(NodeHeaderSize + SparseBitmapBytes + uint32(node.EntryCount)*width)
		if size != want {
			return Node{}, 0, ErrCorrupt
		}
	case EncodingDense:
		want := DenseInternalNodeSize
		if node.Level == 0 {
			want = DenseLeafNodeSize
		}
		if size != want {
			return Node{}, 0, ErrCorrupt
		}
	default:
		return Node{}, 0, ErrUnsupported
	}
	return node, size, nil
}

func (n Node) Lookup(slot uint16) (uint64, bool) {
	if slot >= NodeSlots {
		return 0, false
	}
	if n.Encoding == EncodingDense {
		value := n.Values[slot]
		return value, value != 0
	}
	word, bit := slot/64, slot%64
	if n.Bitmap[word]&(uint64(1)<<bit) == 0 {
		return 0, false
	}
	index := 0
	for i := uint16(0); i < word; i++ {
		index += bits.OnesCount64(n.Bitmap[i])
	}
	index += bits.OnesCount64(n.Bitmap[word] & ((uint64(1) << bit) - 1))
	return n.Values[index], true
}

func (n Node) LookupRef(slot uint16) (recordlog.RecordRef, bool) {
	if n.Level != 0 || slot >= NodeSlots {
		return recordlog.RecordRef{}, false
	}
	if n.Encoding == EncodingDense {
		if n.Values[slot] == 0 {
			return recordlog.RecordRef{}, false
		}
		return recordlog.RecordRef{Addr: recordlog.VAddr(n.Values[slot]), PhysicalSize: n.Sizes[slot]}, true
	}
	word, bit := slot/64, slot%64
	if n.Bitmap[word]&(uint64(1)<<bit) == 0 {
		return recordlog.RecordRef{}, false
	}
	index := 0
	for i := uint16(0); i < word; i++ {
		index += bits.OnesCount64(n.Bitmap[i])
	}
	index += bits.OnesCount64(n.Bitmap[word] & ((uint64(1) << bit) - 1))
	return recordlog.RecordRef{Addr: recordlog.VAddr(n.Values[index]), PhysicalSize: n.Sizes[index]}, true
}

// Slots expands a decoded node for copy-on-write construction. Point lookups
// should use Lookup so sparse nodes stay compact in cache.
func (n Node) Slots() [NodeSlots]uint64 {
	var slots [NodeSlots]uint64
	if n.Encoding == EncodingDense {
		copy(slots[:], n.Values)
		return slots
	}
	valueIndex := 0
	for wordIndex, word := range n.Bitmap {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			slots[wordIndex*64+bit] = n.Values[valueIndex]
			valueIndex++
			word &^= uint64(1) << bit
		}
	}
	return slots
}

func (n Node) Refs() [NodeSlots]recordlog.RecordRef {
	var refs [NodeSlots]recordlog.RecordRef
	if n.Level != 0 {
		return refs
	}
	if n.Encoding == EncodingDense {
		for index, value := range n.Values {
			if value != 0 {
				refs[index] = recordlog.RecordRef{Addr: recordlog.VAddr(value), PhysicalSize: n.Sizes[index]}
			}
		}
		return refs
	}
	valueIndex := 0
	for wordIndex, word := range n.Bitmap {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			refs[wordIndex*64+bit] = recordlog.RecordRef{Addr: recordlog.VAddr(n.Values[valueIndex]), PhysicalSize: n.Sizes[valueIndex]}
			valueIndex++
			word &^= uint64(1) << bit
		}
	}
	return refs
}

func validPrefix(level uint8, prefix uint64) bool {
	if level > MaxLevel {
		return false
	}
	if level == MaxLevel {
		return prefix == 0
	}
	used := uint(64 - 9*uint(level+1))
	return prefix < uint64(1)<<used
}

func validateValues(level uint8, values []uint64, sizes []uint32) error {
	if level == 0 && len(sizes) != len(values) || level != 0 && len(sizes) != 0 && len(sizes) != len(values) {
		return ErrInvalid
	}
	for index, value := range values {
		if value == 0 {
			if level == 0 && sizes[index] != 0 {
				return ErrInvalid
			}
			continue
		}
		if level == 0 {
			if !(recordlog.RecordRef{Addr: recordlog.VAddr(value), PhysicalSize: sizes[index]}).Valid() {
				return ErrInvalid
			}
		} else if len(sizes) != 0 && sizes[index] != 0 {
			return ErrInvalid
		} else if _, err := model.ParseMapAddr(value); err != nil {
			return ErrInvalid
		}
	}
	return nil
}

func alignNodeSize(size uint32) uint32 {
	return (size + 7) &^ 7
}

func headerChecksum(header []byte) uint32 {
	var copyHeader [NodeHeaderSize]byte
	copy(copyHeader[:], header)
	clear(copyHeader[48:52])
	return crc32.Checksum(copyHeader[:], crcTable)
}

func countValues(values []uint64) int {
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

func topSlotsEmpty(values []uint64) bool {
	for _, value := range values[2:] {
		if value != 0 {
			return false
		}
	}
	return true
}

func topBitmapEmpty(bitmap [8]uint64) bool {
	return bitmap[0]&^uint64(3) == 0 && bitmap[1] == 0 && bitmap[2] == 0 && bitmap[3] == 0 && bitmap[4] == 0 && bitmap[5] == 0 && bitmap[6] == 0 && bitmap[7] == 0
}
