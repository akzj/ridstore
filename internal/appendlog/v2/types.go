package v2

import (
	"errors"
	"fmt"
	"math"
)

var (
	ErrClosed        = errors.New("appendlog/v2: closed")
	ErrPoisoned      = errors.New("appendlog/v2: poisoned")
	ErrInvalidConfig = errors.New("appendlog/v2: invalid configuration")
	ErrPayloadTooBig = errors.New("appendlog/v2: payload too large")
	ErrInvalidVAddr  = errors.New("appendlog/v2: invalid virtual address")
	ErrCorrupt       = errors.New("appendlog/v2: corrupt log")
)

type VAddr uint64

const (
	vaddrOffsetBits   = 32
	vaddrSizeBits     = 3
	vaddrSizeMask     = uint32(1<<vaddrSizeBits) - 1
	vaddrReservedSize = vaddrSizeMask
	maxSegmentID      = uint64(math.MaxUint32)
	maxSegmentSize    = uint64(math.MaxUint32)
)

func makeVAddr(segmentID uint32, offset, physicalSize uint64) (VAddr, error) {
	sizeClass, err := vaddrSizeClass(physicalSize)
	if err != nil || segmentID == 0 || offset > math.MaxUint32 || offset&uint64(vaddrSizeMask) != 0 {
		return 0, ErrInvalidVAddr
	}
	return VAddr(uint64(segmentID)<<vaddrOffsetBits | offset | uint64(sizeClass)), nil
}

func (v VAddr) SegmentID() uint32 { return uint32(uint64(v) >> vaddrOffsetBits) }
func (v VAddr) Offset() uint32    { return uint32(v) &^ vaddrSizeMask }

func (v VAddr) Valid() bool {
	return v.SegmentID() != 0 && uint64(v.Offset()) >= segmentHeaderSize && v.sizeClass() != vaddrReservedSize
}

func (v VAddr) sizeClass() uint32 { return uint32(v) & vaddrSizeMask }

func (v VAddr) readHint() (uint64, error) {
	if !v.Valid() {
		return 0, ErrInvalidVAddr
	}
	return uint64(64) << v.sizeClass(), nil
}

func (v VAddr) matchesPhysicalSize(physicalSize uint64) bool {
	sizeClass, err := vaddrSizeClass(physicalSize)
	return err == nil && v.sizeClass() == sizeClass
}

func vaddrSizeClass(physicalSize uint64) (uint32, error) {
	if physicalSize < recordHeaderSize || physicalSize > math.MaxUint32 || physicalSize&(recordAlignment-1) != 0 {
		return 0, ErrInvalidVAddr
	}
	for sizeClass, upperBound := uint32(0), uint64(64); sizeClass < vaddrReservedSize; sizeClass, upperBound = sizeClass+1, upperBound<<1 {
		if physicalSize <= upperBound || sizeClass == vaddrReservedSize-1 {
			return sizeClass, nil
		}
	}
	return 0, ErrInvalidVAddr
}

func (v VAddr) String() string {
	return fmt.Sprintf("%d:%d", v.SegmentID(), v.Offset())
}

type Position struct {
	SegmentID uint32
	Offset    uint64
}

type Watermarks struct {
	Reserved Position
	Written  Position
	Durable  Position
}

type Status struct {
	Watermarks       Watermarks
	QueueRequests    int
	OutstandingBytes uint64
	PendingRecords   int
	PendingBytes     uint64
	WriteCalls       uint64
	SyncCalls        uint64
	Poisoned         bool
	LastError        string
}

type Config struct {
	Dir              string
	SegmentSize      uint64
	MaxPayloadSize   uint64
	MaxBufferBytes   uint64
	MaxBufferRecords int
	ChannelCapacity  int
	MaxQueuedBytes   uint64
	FaultHook        func(FaultPoint) error
	files            fileBackend
}

type FaultPoint string

const (
	FaultBeforeAppendWrite   FaultPoint = "before-append-write"
	FaultBeforeSync          FaultPoint = "before-sync"
	FaultBeforeFooterWrite   FaultPoint = "before-footer-write"
	FaultBeforeFooterSync    FaultPoint = "before-footer-sync"
	FaultBeforeRename        FaultPoint = "before-rename"
	FaultBeforeSealDirSync   FaultPoint = "before-seal-dir-sync"
	FaultBeforeHeaderWrite   FaultPoint = "before-header-write"
	FaultBeforeHeaderSync    FaultPoint = "before-header-sync"
	FaultBeforeActiveRename  FaultPoint = "before-active-rename"
	FaultBeforeCreateDirSync FaultPoint = "before-create-dir-sync"
	FaultBeforeTailTruncate  FaultPoint = "before-tail-truncate"
	FaultBeforeTailSync      FaultPoint = "before-tail-sync"
)

func hitFault(hook func(FaultPoint) error, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

func DefaultConfig(dir string) Config {
	return Config{
		Dir:              dir,
		SegmentSize:      64 << 20,
		MaxPayloadSize:   16 << 20,
		MaxBufferBytes:   1 << 20,
		MaxBufferRecords: 1024,
		ChannelCapacity:  1024,
		MaxQueuedBytes:   64 << 20,
	}
}

func (c Config) validate() error {
	if c.Dir == "" || c.SegmentSize > maxSegmentSize || c.SegmentSize <= segmentHeaderSize+recordHeaderSize+segmentFooterSize ||
		c.MaxPayloadSize == 0 || c.MaxBufferBytes == 0 || c.MaxBufferRecords <= 0 || c.ChannelCapacity <= 0 || c.MaxQueuedBytes == 0 {
		return ErrInvalidConfig
	}
	maxRecord, err := encodedRecordSize(c.MaxPayloadSize)
	if err != nil || maxRecord > c.SegmentSize-segmentHeaderSize-segmentFooterSize {
		return ErrInvalidConfig
	}
	if c.MaxBufferBytes > math.MaxUint64-maxRecord || c.MaxQueuedBytes < c.MaxBufferBytes+maxRecord {
		return ErrInvalidConfig
	}
	return nil
}
