// Package ridstore provides an embedded stable-ID log-structured record store.
package ridstore

import (
	"context"
	"errors"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/engine"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

// ID is a stable logical record identifier. Zero is invalid and IDs are never reused.
type ID = model.ID

// BatchID identifies one atomic batch. Zero is invalid.
type BatchID = model.BatchID

// CommitSeq orders durable publications. It is not a record version.
type CommitSeq = model.CommitSeq

// VersionToken is an opaque observation of one record version. It may only be
// returned unchanged to conditional methods on the same persistent Store.
type VersionToken struct {
	store [16]byte
	addr  uint64
}

// Record is a copied value and the token observing its current Mapping entry.
type Record struct {
	Value []byte
	Token VersionToken
}

type BatchState uint8

const (
	BatchStateOpen BatchState = iota + 1
	BatchStateCommitting
	BatchStateCommitted
	BatchStateAborted
	BatchStateCommitUnknown
)

type BatchStatus struct {
	BatchID   BatchID
	State     BatchState
	CommitSeq CommitSeq
}

type CommitResult struct {
	BatchID   BatchID
	CommitSeq CommitSeq
}

// CompactionPolicy sets lower bounds for selecting one sealed segment. Ratio
// uses basis points (10_000 == 100%). Zero disables a bound.
type CompactionPolicy struct {
	MinReclaimableBytes      uint64
	MinReclaimableRatioBasis uint32
}

type CompactionResult struct {
	SourceSegmentID       uint32
	ReclaimableBytes      uint64
	ReclaimableRatioBasis uint32
	ScannedRecords        uint64
	CopiedRecords         uint64
	Applied               uint64
	Skipped               uint64
	FirstCommitSeq        CommitSeq
	LastCommitSeq         CommitSeq
}

// Store owns one exclusively opened data directory.
type Store struct {
	inner    *engine.Store
	identity [16]byte
}

// Create initializes and opens a new v2 Store. Interrupted initialization can
// be resumed by calling Create again with the same hard limits.
func Create(ctx context.Context, config CreateConfig) (*Store, error) {
	inner, err := engine.Create(ctx, config.Dir, config.engineConfig())
	if err != nil {
		return nil, err
	}
	return wrapStore(inner), nil
}

// Open recovers and exclusively opens an existing v2 Store.
func Open(ctx context.Context, config OpenConfig) (*Store, error) {
	inner, err := engine.Open(ctx, config.Dir, config.engineConfig())
	if err != nil {
		return nil, err
	}
	return wrapStore(inner), nil
}

func wrapStore(inner *engine.Store) *Store {
	return &Store{inner: inner, identity: inner.Identity()}
}

func (s *Store) Begin(ctx context.Context) (*Batch, error) {
	if s == nil || s.inner == nil {
		return nil, base.ErrClosed
	}
	inner, err := s.inner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &Batch{store: s, inner: inner}, nil
}

func (s *Store) Get(ctx context.Context, id ID) (Record, error) {
	if s == nil || s.inner == nil {
		return Record{}, base.ErrClosed
	}
	record, err := s.inner.Get(ctx, id)
	if err != nil {
		return Record{}, err
	}
	return Record{Value: record.Value, Token: s.token(record.Addr)}, nil
}

func (s *Store) Status(ctx context.Context, id BatchID) (BatchStatus, error) {
	if s == nil || s.inner == nil {
		return BatchStatus{}, base.ErrClosed
	}
	status, err := s.inner.Status(ctx, id)
	if err != nil {
		return BatchStatus{}, err
	}
	return BatchStatus{BatchID: status.BatchID, State: BatchState(status.State), CommitSeq: status.CommitSeq}, nil
}

func (s *Store) Checkpoint(ctx context.Context) error {
	if s == nil || s.inner == nil {
		return base.ErrClosed
	}
	return s.inner.Checkpoint(ctx)
}

// CompactNextSegment selects and fully retires at most one eligible segment.
func (s *Store) CompactNextSegment(ctx context.Context, policy CompactionPolicy) (CompactionResult, bool, error) {
	if s == nil || s.inner == nil {
		return CompactionResult{}, false, base.ErrClosed
	}
	result, found, err := s.inner.CompactNextSegment(ctx, engine.CompactionPolicy{
		MinReclaimableBytes:      policy.MinReclaimableBytes,
		MinReclaimableRatioBasis: policy.MinReclaimableRatioBasis,
	})
	if err != nil || !found {
		return CompactionResult{}, found, err
	}
	return CompactionResult{
		SourceSegmentID:       uint32(result.Candidate.Source.SegmentID),
		ReclaimableBytes:      result.Candidate.ReclaimableBytesLower,
		ReclaimableRatioBasis: result.Candidate.ReclaimableRatioBasis,
		ScannedRecords:        result.Compaction.Relocation.ScannedRecords,
		CopiedRecords:         result.Compaction.Relocation.CopiedRecords,
		Applied:               result.Compaction.Relocation.Applied,
		Skipped:               result.Compaction.Relocation.Skipped,
		FirstCommitSeq:        result.Compaction.Relocation.FirstCommitSeq,
		LastCommitSeq:         result.Compaction.Relocation.LastCommitSeq,
	}, true, nil
}

func (s *Store) Close() error {
	if s == nil || s.inner == nil {
		return base.ErrClosed
	}
	return s.inner.Close()
}

func (s *Store) token(addr recordlog.VAddr) VersionToken {
	return VersionToken{store: s.identity, addr: uint64(addr)}
}

func (s *Store) address(token VersionToken) (recordlog.VAddr, error) {
	if token.store == ([16]byte{}) || token.store != s.identity {
		return 0, base.ErrInvalidToken
	}
	addr, err := recordlog.ParseVAddr(token.addr)
	if err != nil {
		return 0, errors.Join(base.ErrInvalidToken, err)
	}
	return addr, nil
}

type Batch struct {
	store *Store
	inner *engine.Batch
}

func (b *Batch) ID() BatchID {
	if b == nil || b.inner == nil {
		return 0
	}
	return b.inner.ID()
}

func (b *Batch) Allocate(ctx context.Context) (ID, error) {
	if err := b.valid(); err != nil {
		return 0, err
	}
	return b.inner.Allocate(ctx)
}

func (b *Batch) Create(ctx context.Context, value []byte) (ID, error) {
	if err := b.valid(); err != nil {
		return 0, err
	}
	return b.inner.Create(ctx, value)
}

func (b *Batch) Put(ctx context.Context, id ID, value []byte) error {
	if err := b.valid(); err != nil {
		return err
	}
	return b.inner.Put(ctx, id, value)
}

func (b *Batch) CompareAndPut(ctx context.Context, id ID, token VersionToken, value []byte) error {
	if err := b.valid(); err != nil {
		return err
	}
	addr, err := b.store.address(token)
	if err != nil {
		return err
	}
	return b.inner.CompareAndPut(ctx, id, addr, value)
}

func (b *Batch) Delete(id ID) error {
	if err := b.valid(); err != nil {
		return err
	}
	return b.inner.Delete(id)
}

func (b *Batch) CompareAndDelete(id ID, token VersionToken) error {
	if err := b.valid(); err != nil {
		return err
	}
	addr, err := b.store.address(token)
	if err != nil {
		return err
	}
	return b.inner.CompareAndDelete(id, addr)
}

func (b *Batch) ExpectToken(id ID, token VersionToken) error {
	if err := b.valid(); err != nil {
		return err
	}
	addr, err := b.store.address(token)
	if err != nil {
		return err
	}
	return b.inner.ExpectAddress(id, addr)
}

func (b *Batch) ExpectAbsent(id ID) error {
	if err := b.valid(); err != nil {
		return err
	}
	return b.inner.ExpectAbsent(id)
}

func (b *Batch) Commit(ctx context.Context) (CommitResult, error) {
	if err := b.valid(); err != nil {
		return CommitResult{}, err
	}
	result, err := b.inner.Commit(ctx)
	return CommitResult{BatchID: result.BatchID, CommitSeq: result.CommitSeq}, err
}

func (b *Batch) Abort(ctx context.Context) error {
	if err := b.valid(); err != nil {
		return err
	}
	return b.inner.Abort(ctx)
}

func (b *Batch) valid() error {
	if b == nil || b.store == nil || b.store.inner == nil || b.inner == nil {
		return base.ErrBatchClosed
	}
	return nil
}
