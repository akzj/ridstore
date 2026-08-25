package idalloc

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type Kind uint8

const (
	RecordID Kind = iota + 1
	BatchID
)

type Appender interface {
	Append(context.Context, []byte, bool) (recordlog.AppendResult, error)
}

type Allocator struct {
	mu      sync.Mutex
	kind    Kind
	reserve uint64
	next    uint64
	high    uint64
	log     Appender
}

// New starts with an empty in-memory range at durableHigh. Recovery therefore
// never reissues an ID from a range that an earlier process might have used.
func New(kind Kind, reserveSize, durableHigh uint64, log Appender) (*Allocator, error) {
	if !validKind(kind) || reserveSize == 0 || durableHigh == 0 || log == nil || (durableHigh-1)%reserveSize != 0 {
		return nil, fmt.Errorf("id allocator configuration: %w", base.ErrInvalidConfig)
	}
	return &Allocator{kind: kind, reserve: reserveSize, next: durableHigh, high: durableHigh, log: log}, nil
}

func (a *Allocator) Allocate(ctx context.Context) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if a.next == a.high {
		if err := a.reserveRange(ctx); err != nil {
			return 0, err
		}
	}
	id := a.next
	a.next++
	return id, nil
}

func (a *Allocator) DurableHigh() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.high
}

// IssuedHigh is the exclusive upper bound of IDs handed to callers in this
// process. It is captured for BatchID checkpoint recovery while allocation is
// quiesced by the Engine barrier.
func (a *Allocator) IssuedHigh() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next
}

func (a *Allocator) reserveRange(ctx context.Context) error {
	if a.high > math.MaxUint64-a.reserve {
		return base.ErrIDExhausted
	}
	newHigh := a.high + a.reserve
	payload, err := recordcodec.EncodeReserve(a.recordType(), recordcodec.ReserveRecord{HighExclusive: newHigh})
	if err != nil {
		return err
	}
	if _, err := a.log.Append(ctx, payload, true); err != nil {
		return err
	}
	a.next = a.high
	a.high = newHigh
	return nil
}

func (a *Allocator) recordType() recordcodec.RecordType {
	if a.kind == BatchID {
		return recordcodec.RecordTypeBatchIDReserve
	}
	return recordcodec.RecordTypeIDReserve
}

// AdvanceRecovered validates one reserve record in replay order. The immutable
// reserve size makes a skipped, duplicated, or reordered record corruption.
func AdvanceRecovered(kind Kind, reserveSize, currentHigh uint64, record recordcodec.ReserveRecord) (uint64, error) {
	if !validKind(kind) || reserveSize == 0 || currentHigh == 0 || record.HighExclusive <= currentHigh ||
		currentHigh > math.MaxUint64-reserveSize || record.HighExclusive != currentHigh+reserveSize ||
		(record.HighExclusive-1)%reserveSize != 0 {
		return 0, fmt.Errorf("reserve recovery chain: %w", base.ErrCorrupt)
	}
	return record.HighExclusive, nil
}

func validKind(kind Kind) bool { return kind == RecordID || kind == BatchID }
