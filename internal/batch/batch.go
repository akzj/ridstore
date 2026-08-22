package batch

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type State uint8

const (
	StateOpen State = iota + 1
	StateCommitting
	StateCommitted
	StateAborted
	StateCommitUnknown
	StateFailed
)

type Operation uint8

const (
	Put Operation = iota + 1
	Delete
)

type ConditionKind uint8

const (
	ConditionRevision ConditionKind = iota + 1
	ConditionAbsent
)

type Mutation struct {
	RecordID     base.ID
	Operation    Operation
	Addr         base.VAddr
	ValueBytes   uint64
	PhysicalSize uint64
}

type Condition struct {
	RecordID base.ID
	Kind     ConditionKind
	Revision base.Revision
}

type Prepared struct {
	BatchID              base.BatchID
	Mutations            []Mutation
	Conditions           []Condition
	LogicalPayloadBytes  uint64
	AppendedPayloadBytes uint64
	LastBatchFrameSeq    base.FrameSeq
}

type Limits struct {
	MaxValueSize       uint64
	MaxBatchBytes      uint64
	MaxBatchMutations  uint64
	MaxBatchConditions uint64
}

type Appender interface {
	AppendPut(context.Context, base.BatchID, base.ID, []byte) (base.VAddr, base.FrameSeq, uint64, error)
	AppendAbort(context.Context, base.BatchID, storeformat.BatchAbortPayload) error
}

type PutAdmitter interface {
	AdmitPut(context.Context, uint64) error
	ReleasePut()
}

type IDAllocator interface {
	Allocate(context.Context) (uint64, error)
}

type Batch struct {
	mu sync.Mutex

	id        base.BatchID
	state     State
	limits    Limits
	appender  Appender
	allocator IDAllocator

	mutations            map[base.ID]Mutation
	conditions           map[base.ID]Condition
	appendedPayloadBytes uint64
	lastBatchFrameSeq    base.FrameSeq
	commitSeq            base.CommitSeq
}

func New(id base.BatchID, limits Limits, appender Appender, allocator IDAllocator) (*Batch, error) {
	if id == 0 || limits.MaxValueSize == 0 || limits.MaxBatchBytes == 0 || limits.MaxBatchMutations == 0 || limits.MaxBatchConditions == 0 ||
		limits.MaxValueSize > limits.MaxBatchBytes || limits.MaxBatchMutations > math.MaxUint32 || limits.MaxBatchConditions > math.MaxUint32 || appender == nil || allocator == nil {
		return nil, fmt.Errorf("batch configuration: %w", base.ErrInvalidConfig)
	}
	return &Batch{
		id: id, state: StateOpen, limits: limits, appender: appender, allocator: allocator,
		mutations: make(map[base.ID]Mutation), conditions: make(map[base.ID]Condition),
	}, nil
}

func (b *Batch) ID() base.BatchID { return b.id }

func (b *Batch) State() (State, base.CommitSeq) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, b.commitSeq
}

func (b *Batch) CheckOpen() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requireOpen()
}

func (b *Batch) Allocate(ctx context.Context) (base.ID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return 0, err
	}
	return b.allocateLocked(ctx)
}

func (b *Batch) allocateLocked(ctx context.Context) (base.ID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	id, err := b.allocator.Allocate(ctx)
	if err != nil {
		return 0, err
	}
	if id == 0 {
		b.state = StateFailed
		return 0, fmt.Errorf("allocator returned ID zero: %w", base.ErrInvalidID)
	}
	return base.ID(id), nil
}

// Create allocates a never-reused ID and appends its first value. The ID is
// consumed even when the later append fails; allocator gaps are intentional.
func (b *Batch) Create(ctx context.Context, value []byte) (base.ID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return 0, err
	}
	if err := b.validatePutLocked(0, value, true); err != nil {
		return 0, err
	}
	id, err := b.allocateLocked(ctx)
	if err != nil {
		return 0, err
	}
	if err := b.putLocked(ctx, id, value); err != nil {
		return id, err
	}
	return id, nil
}

func (b *Batch) Put(ctx context.Context, id base.ID, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	return b.putLocked(ctx, id, value)
}

// Update appends a new value and atomically declares the logical revision that
// must still be current when the batch reaches the commit serialization point.
func (b *Batch) Update(ctx context.Context, id base.ID, expected base.Revision, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	condition := Condition{RecordID: id, Kind: ConditionRevision, Revision: expected}
	if err := b.validateConditionLocked(condition); err != nil {
		return err
	}
	if err := b.putLocked(ctx, id, value); err != nil {
		return err
	}
	b.conditions[id] = condition
	return nil
}

func (b *Batch) putLocked(ctx context.Context, id base.ID, value []byte) error {
	if err := b.validatePutLocked(id, value, false); err != nil {
		return err
	}
	newPayloadBytes, err := base.AddUint64(b.appendedPayloadBytes, uint64(len(value)))
	// validatePutLocked checked this addition; retain the checked result used to
	// update the batch after the append succeeds.
	if err != nil {
		return base.ErrBatchTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	physicalSize, err := base.Align8(storeformat.FrameHeaderSize + uint64(len(value)))
	if err != nil {
		return err
	}
	if admitter, ok := b.appender.(PutAdmitter); ok {
		if err := admitter.AdmitPut(ctx, physicalSize); err != nil {
			return err
		}
		defer admitter.ReleasePut()
	}
	addr, frameSeq, physicalSize, err := b.appender.AppendPut(ctx, b.id, id, value)
	if err != nil {
		b.state = StateFailed
		return err
	}
	if addr == 0 || frameSeq == 0 || physicalSize < storeformat.FrameHeaderSize {
		b.state = StateFailed
		return fmt.Errorf("append result: %w", base.ErrCorrupt)
	}
	b.appendedPayloadBytes = newPayloadBytes
	b.lastBatchFrameSeq = frameSeq
	b.mutations[id] = Mutation{RecordID: id, Operation: Put, Addr: addr, ValueBytes: uint64(len(value)), PhysicalSize: physicalSize}
	return nil
}

func (b *Batch) validatePutLocked(id base.ID, value []byte, idPendingAllocation bool) error {
	if !idPendingAllocation && id == 0 {
		return base.ErrInvalidID
	}
	if uint64(len(value)) > b.limits.MaxValueSize {
		return base.ErrValueTooLarge
	}
	if !idPendingAllocation && uint64(len(b.mutations)) == b.limits.MaxBatchMutations {
		if _, exists := b.mutations[id]; !exists {
			return base.ErrBatchTooLarge
		}
	}
	if idPendingAllocation && uint64(len(b.mutations)) == b.limits.MaxBatchMutations {
		return base.ErrBatchTooLarge
	}
	newPayloadBytes, err := base.AddUint64(b.appendedPayloadBytes, uint64(len(value)))
	if err != nil || newPayloadBytes > b.limits.MaxBatchBytes {
		return base.ErrBatchTooLarge
	}
	return nil
}

func (b *Batch) Delete(id base.ID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	if id == 0 {
		return base.ErrInvalidID
	}
	if uint64(len(b.mutations)) == b.limits.MaxBatchMutations {
		if _, exists := b.mutations[id]; !exists {
			return base.ErrBatchTooLarge
		}
	}
	b.mutations[id] = Mutation{RecordID: id, Operation: Delete}
	return nil
}

// DeleteIfRevision atomically records a delete and the logical revision that
// must still be current when the batch reaches commit serialization.
func (b *Batch) DeleteIfRevision(id base.ID, expected base.Revision) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	condition := Condition{RecordID: id, Kind: ConditionRevision, Revision: expected}
	if err := b.validateConditionLocked(condition); err != nil {
		return err
	}
	if uint64(len(b.mutations)) == b.limits.MaxBatchMutations {
		if _, exists := b.mutations[id]; !exists {
			return base.ErrBatchTooLarge
		}
	}
	b.mutations[id] = Mutation{RecordID: id, Operation: Delete}
	b.conditions[id] = condition
	return nil
}

func (b *Batch) ExpectRevision(id base.ID, revision base.Revision) error {
	if revision == 0 {
		return base.ErrInvalidRevision
	}
	return b.addCondition(Condition{RecordID: id, Kind: ConditionRevision, Revision: revision})
}

func (b *Batch) ExpectAbsent(id base.ID) error {
	return b.addCondition(Condition{RecordID: id, Kind: ConditionAbsent})
}

func (b *Batch) addCondition(condition Condition) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	if err := b.validateConditionLocked(condition); err != nil {
		return err
	}
	b.conditions[condition.RecordID] = condition
	return nil
}

func (b *Batch) validateConditionLocked(condition Condition) error {
	if condition.RecordID == 0 {
		return base.ErrInvalidID
	}
	if condition.Kind == ConditionRevision && condition.Revision == 0 {
		return base.ErrInvalidRevision
	}
	if old, exists := b.conditions[condition.RecordID]; exists {
		if old == condition {
			return nil
		}
		b.state = StateFailed
		return base.ErrBatchFailed
	}
	if uint64(len(b.conditions)) == b.limits.MaxBatchConditions {
		return base.ErrBatchTooLarge
	}
	return nil
}

func (b *Batch) Prepare() (Prepared, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return Prepared{}, err
	}
	b.state = StateCommitting
	prepared := Prepared{
		BatchID: b.id, Mutations: make([]Mutation, 0, len(b.mutations)), Conditions: make([]Condition, 0, len(b.conditions)),
		AppendedPayloadBytes: b.appendedPayloadBytes, LastBatchFrameSeq: b.lastBatchFrameSeq,
	}
	for _, mutation := range b.mutations {
		prepared.Mutations = append(prepared.Mutations, mutation)
		if mutation.Operation == Put {
			prepared.LogicalPayloadBytes += mutation.ValueBytes
		}
	}
	for _, condition := range b.conditions {
		prepared.Conditions = append(prepared.Conditions, condition)
	}
	sort.Slice(prepared.Mutations, func(i, j int) bool { return prepared.Mutations[i].RecordID < prepared.Mutations[j].RecordID })
	sort.Slice(prepared.Conditions, func(i, j int) bool { return prepared.Conditions[i].RecordID < prepared.Conditions[j].RecordID })
	return prepared, nil
}

func (b *Batch) Abort(ctx context.Context, reason storeformat.AbortReason) error {
	b.mu.Lock()
	if b.state != StateOpen && b.state != StateFailed {
		err := b.stateError()
		b.mu.Unlock()
		return err
	}
	payload := storeformat.BatchAbortPayload{
		Reason: reason, FinalMutationCount: uint32(len(b.mutations)),
		AppendedPayloadBytes: b.appendedPayloadBytes, LastBatchFrameSeq: b.lastBatchFrameSeq,
	}
	if _, err := storeformat.EncodeBatchAbortPayload(payload); err != nil {
		b.mu.Unlock()
		return err
	}
	b.state = StateAborted
	b.mutations = nil
	b.conditions = nil
	b.mu.Unlock()
	return b.appender.AppendAbort(ctx, b.id, payload)
}

func (b *Batch) MarkCommitted(seq base.CommitSeq) error {
	if seq == 0 {
		return base.ErrInvalidConfig
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateCommitting {
		return b.stateError()
	}
	b.state, b.commitSeq = StateCommitted, seq
	b.mutations, b.conditions = nil, nil
	return nil
}

func (b *Batch) MarkAborted() error       { return b.markTerminal(StateAborted) }
func (b *Batch) MarkCommitUnknown() error { return b.markTerminal(StateCommitUnknown) }

func (b *Batch) markTerminal(state State) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateCommitting {
		return b.stateError()
	}
	b.state = state
	b.mutations, b.conditions = nil, nil
	return nil
}

func (b *Batch) requireOpen() error {
	if b.state == StateOpen {
		return nil
	}
	return b.stateError()
}

func (b *Batch) stateError() error {
	switch b.state {
	case StateFailed:
		return base.ErrBatchFailed
	case StateOpen:
		return nil
	default:
		return base.ErrBatchClosed
	}
}
