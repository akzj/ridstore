package transaction

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
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

type Limits struct {
	MaxValueSize       uint64
	MaxBatchBytes      uint64
	MaxBatchMutations  uint64
	MaxBatchConditions uint64
}

type Appender interface {
	Append(context.Context, []byte, bool) (recordlog.AppendResult, error)
}

type IDAllocator interface {
	Allocate(context.Context) (uint64, error)
}

type Mutation struct {
	RecordID   model.ID
	Operation  mapping.Operation
	Addr       recordlog.VAddr
	ValueBytes uint64
}

type Prepared struct {
	BatchID             model.BatchID
	Mutations           []Mutation
	Conditions          []mapping.Condition
	LogicalPayloadBytes uint64
}

func (p Prepared) Proposal() mapping.Proposal {
	changes := make([]mapping.Change, len(p.Mutations))
	for i, mutation := range p.Mutations {
		changes[i] = mapping.Change{RecordID: mutation.RecordID, NewAddr: mutation.Addr, Operation: mutation.Operation}
	}
	return mapping.Proposal{
		Kind:       mapping.ProposalUserCommit,
		Conditions: append([]mapping.Condition(nil), p.Conditions...),
		Changes:    changes,
	}
}

type Batch struct {
	mu sync.Mutex

	id        model.BatchID
	state     State
	limits    Limits
	log       Appender
	allocator IDAllocator

	mutations     map[model.ID]Mutation
	conditions    map[model.ID]mapping.Condition
	appendedBytes uint64
	commitSeq     model.CommitSeq
}

func New(id model.BatchID, limits Limits, log Appender, allocator IDAllocator) (*Batch, error) {
	if id == 0 || ValidateLimits(limits) != nil || log == nil || allocator == nil {
		return nil, fmt.Errorf("transaction configuration: %w", base.ErrInvalidConfig)
	}
	return &Batch{
		id: id, state: StateOpen, limits: limits, log: log, allocator: allocator,
		mutations: make(map[model.ID]Mutation), conditions: make(map[model.ID]mapping.Condition),
	}, nil
}

func ValidateLimits(limits Limits) error {
	if limits.MaxValueSize == 0 || limits.MaxBatchBytes == 0 || limits.MaxBatchMutations == 0 || limits.MaxBatchConditions == 0 ||
		limits.MaxValueSize > limits.MaxBatchBytes || limits.MaxBatchMutations > math.MaxUint32 || limits.MaxBatchConditions > math.MaxUint32 {
		return base.ErrInvalidConfig
	}
	return nil
}

func (b *Batch) ID() model.BatchID { return b.id }

func (b *Batch) State() (State, model.CommitSeq) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, b.commitSeq
}

func (b *Batch) CheckOpen() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requireOpen()
}

func (b *Batch) Allocate(ctx context.Context) (model.ID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return 0, err
	}
	return b.allocateLocked(ctx)
}

// Create allocates a never-reused ID and appends its first value without a
// Mapping read. An allocated ID remains consumed when a later append or commit
// fails.
func (b *Batch) Create(ctx context.Context, value []byte) (model.ID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return 0, err
	}
	if err := b.validatePut(0, value, true); err != nil {
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

func (b *Batch) Put(ctx context.Context, id model.ID, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	return b.putLocked(ctx, id, value)
}

func (b *Batch) CompareAndPut(ctx context.Context, id model.ID, expected recordlog.VAddr, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	condition := mapping.Condition{RecordID: id, ExpectedAddr: expected}
	if err := b.validateCondition(condition); err != nil {
		return err
	}
	if err := b.putLocked(ctx, id, value); err != nil {
		return err
	}
	b.conditions[id] = condition
	return nil
}

func (b *Batch) Delete(id model.ID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	return b.deleteLocked(id)
}

func (b *Batch) CompareAndDelete(id model.ID, expected recordlog.VAddr) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	condition := mapping.Condition{RecordID: id, ExpectedAddr: expected}
	if err := b.validateCondition(condition); err != nil {
		return err
	}
	if err := b.deleteLocked(id); err != nil {
		return err
	}
	b.conditions[id] = condition
	return nil
}

func (b *Batch) ExpectAddress(id model.ID, addr recordlog.VAddr) error {
	return b.addCondition(mapping.Condition{RecordID: id, ExpectedAddr: addr})
}

func (b *Batch) ExpectAbsent(id model.ID) error {
	return b.addCondition(mapping.Condition{RecordID: id})
}

func (b *Batch) MutationIDs() ([]model.ID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return nil, err
	}
	ids := make([]model.ID, 0, len(b.mutations))
	for id := range b.mutations {
		ids = append(ids, id)
	}
	return ids, nil
}

func (b *Batch) Prepare() (Prepared, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return Prepared{}, err
	}
	b.state = StateCommitting
	prepared := Prepared{
		BatchID: b.id, Mutations: make([]Mutation, 0, len(b.mutations)), Conditions: make([]mapping.Condition, 0, len(b.conditions)),
	}
	for _, mutation := range b.mutations {
		prepared.Mutations = append(prepared.Mutations, mutation)
		if mutation.Operation == mapping.OperationPut {
			if prepared.LogicalPayloadBytes > math.MaxUint64-mutation.ValueBytes {
				b.state = StateFailed
				return Prepared{}, base.ErrBatchTooLarge
			}
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

// Abort closes the Batch immediately. The diagnostic record is best-effort
// with respect to durability and never changes Mapping visibility.
func (b *Batch) Abort(ctx context.Context, reason uint32) error {
	b.mu.Lock()
	if b.state != StateOpen && b.state != StateFailed {
		err := b.stateError()
		b.mu.Unlock()
		return err
	}
	payload, err := recordcodec.EncodeAbort(recordcodec.AbortRecord{BatchID: b.id, Reason: reason})
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.state = StateAborted
	b.clearLocked()
	b.mu.Unlock()
	_, err = b.log.Append(ctx, payload, false)
	return err
}

func (b *Batch) MarkCommitted(seq model.CommitSeq) error {
	if seq == 0 {
		return base.ErrInvalidConfig
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateCommitting {
		return b.stateError()
	}
	b.state, b.commitSeq = StateCommitted, seq
	b.clearLocked()
	return nil
}

func (b *Batch) MarkAborted() error       { return b.markTerminal(StateAborted) }
func (b *Batch) MarkCommitUnknown() error { return b.markTerminal(StateCommitUnknown) }

func (b *Batch) allocateLocked(ctx context.Context) (model.ID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	raw, err := b.allocator.Allocate(ctx)
	if err != nil {
		return 0, err
	}
	if raw == 0 {
		b.state = StateFailed
		return 0, base.ErrInvalidID
	}
	return model.ID(raw), nil
}

func (b *Batch) putLocked(ctx context.Context, id model.ID, value []byte) error {
	if err := b.validatePut(id, value, false); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: b.id, RecordID: id, Value: value}, b.limits.MaxValueSize)
	if err != nil {
		return err
	}
	result, err := b.log.Append(ctx, payload, false)
	if err != nil {
		b.state = StateFailed
		return err
	}
	if !result.Addr.Valid() {
		b.state = StateFailed
		return fmt.Errorf("put append address: %w", base.ErrCorrupt)
	}
	b.appendedBytes += uint64(len(value))
	b.mutations[id] = Mutation{RecordID: id, Operation: mapping.OperationPut, Addr: result.Addr, ValueBytes: uint64(len(value))}
	return nil
}

func (b *Batch) validatePut(id model.ID, value []byte, pendingID bool) error {
	if !pendingID && id == 0 {
		return base.ErrInvalidID
	}
	if uint64(len(value)) > b.limits.MaxValueSize {
		return base.ErrValueTooLarge
	}
	if _, exists := b.mutations[id]; !exists && uint64(len(b.mutations)) == b.limits.MaxBatchMutations {
		return base.ErrBatchTooLarge
	}
	if uint64(len(value)) > math.MaxUint64-b.appendedBytes || b.appendedBytes+uint64(len(value)) > b.limits.MaxBatchBytes {
		return base.ErrBatchTooLarge
	}
	return nil
}

func (b *Batch) deleteLocked(id model.ID) error {
	if id == 0 {
		return base.ErrInvalidID
	}
	if _, exists := b.mutations[id]; !exists && uint64(len(b.mutations)) == b.limits.MaxBatchMutations {
		return base.ErrBatchTooLarge
	}
	b.mutations[id] = Mutation{RecordID: id, Operation: mapping.OperationDelete}
	return nil
}

func (b *Batch) addCondition(condition mapping.Condition) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireOpen(); err != nil {
		return err
	}
	if err := b.validateCondition(condition); err != nil {
		return err
	}
	b.conditions[condition.RecordID] = condition
	return nil
}

func (b *Batch) validateCondition(condition mapping.Condition) error {
	if condition.RecordID == 0 {
		return base.ErrInvalidID
	}
	if condition.ExpectedAddr != 0 && !condition.ExpectedAddr.Valid() {
		return base.ErrInvalidAddress
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

func (b *Batch) markTerminal(state State) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateCommitting {
		return b.stateError()
	}
	b.state = state
	b.clearLocked()
	return nil
}

func (b *Batch) clearLocked() {
	b.mutations = nil
	b.conditions = nil
}

func (b *Batch) requireOpen() error {
	if b.state == StateOpen {
		return nil
	}
	return b.stateError()
}

func (b *Batch) stateError() error {
	if b.state == StateFailed {
		return base.ErrBatchFailed
	}
	if b.state == StateOpen {
		return nil
	}
	return base.ErrBatchClosed
}
