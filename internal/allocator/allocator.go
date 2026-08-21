package allocator

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type Kind uint8

const (
	RecordID Kind = iota + 1
	BatchID
)

type ReserveWriter interface {
	AppendReserve(context.Context, storeformat.FrameType, storeformat.ReservePayload) error
}

type Allocator struct {
	mu         sync.Mutex
	kind       Kind
	reserve    uint64
	next       uint64
	high       uint64
	generation uint64
	writer     ReserveWriter
}

// New opens an allocator at a durable high watermark. It deliberately starts
// with an empty in-memory range, so every ID below durableHigh is skipped after
// recovery even if the previous process may not have returned all of them.
func New(kind Kind, reserveSize, durableHigh uint64, writer ReserveWriter) (*Allocator, error) {
	if (kind != RecordID && kind != BatchID) || reserveSize == 0 || durableHigh == 0 || writer == nil || (durableHigh-1)%reserveSize != 0 {
		return nil, fmt.Errorf("allocator configuration: %w", base.ErrInvalidConfig)
	}
	return &Allocator{
		kind: kind, reserve: reserveSize, next: durableHigh, high: durableHigh,
		generation: (durableHigh - 1) / reserveSize, writer: writer,
	}, nil
}

func (a *Allocator) Allocate(ctx context.Context) (uint64, error) {
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

func (a *Allocator) reserveRange(ctx context.Context) error {
	if a.high > math.MaxUint64-a.reserve || a.generation == math.MaxUint64 {
		return base.ErrIDExhausted
	}
	newHigh := a.high + a.reserve
	payload := storeformat.ReservePayload{
		PreviousHighExclusive: a.high,
		NewHighExclusive:      newHigh,
		Generation:            a.generation + 1,
	}
	frameType := storeformat.FrameTypeIDReserve
	if a.kind == BatchID {
		frameType = storeformat.FrameTypeBatchIDReserve
	}
	if err := a.writer.AppendReserve(ctx, frameType, payload); err != nil {
		return err
	}
	a.next = a.high
	a.high = newHigh
	a.generation++
	return nil
}

// AdvanceRecovered validates and applies one durable Reserve payload scanned
// after a manifest checkpoint.
func AdvanceRecovered(kind Kind, reserveSize, currentHigh uint64, payload storeformat.ReservePayload) (uint64, error) {
	if (kind != RecordID && kind != BatchID) || reserveSize == 0 || currentHigh == 0 ||
		payload.PreviousHighExclusive != currentHigh || payload.NewHighExclusive <= currentHigh || payload.NewHighExclusive-currentHigh != reserveSize ||
		payload.Generation != (payload.NewHighExclusive-1)/reserveSize || (payload.NewHighExclusive-1)%reserveSize != 0 {
		return 0, fmt.Errorf("reserve recovery chain: %w", base.ErrCorrupt)
	}
	return payload.NewHighExclusive, nil
}
