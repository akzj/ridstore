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
	vaddrOffsetBits = 32
	maxSegmentID    = uint64(math.MaxUint32)
	maxSegmentSize  = uint64(math.MaxUint32)
)

func makeVAddr(segmentID uint32, offset uint64) (VAddr, error) {
	if segmentID == 0 || offset > math.MaxUint32 {
		return 0, ErrInvalidVAddr
	}
	return VAddr(uint64(segmentID)<<vaddrOffsetBits | offset), nil
}

func (v VAddr) SegmentID() uint32 { return uint32(uint64(v) >> vaddrOffsetBits) }
func (v VAddr) Offset() uint32    { return uint32(v) }

func (v VAddr) Valid() bool {
	return v.SegmentID() != 0 && uint64(v.Offset()) >= segmentHeaderSize
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
}

type FaultPoint string

const (
	FaultBeforeAppendWrite FaultPoint = "before-append-write"
	FaultBeforeSync        FaultPoint = "before-sync"
	FaultBeforeFooterWrite FaultPoint = "before-footer-write"
	FaultBeforeFooterSync  FaultPoint = "before-footer-sync"
	FaultBeforeRename      FaultPoint = "before-rename"
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
