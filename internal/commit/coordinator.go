package commit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/batch"
	"github.com/akzj/ridstore/internal/failpoint"
	"github.com/akzj/ridstore/internal/mapping/api"
	"github.com/akzj/ridstore/internal/segment"
)

type RecordHeader struct {
	RecordID     base.ID
	OriginBatch  base.BatchID
	ValueBytes   uint64
	PhysicalSize uint64
}

type RecordReader interface {
	ReadPutHeader(base.VAddr) (RecordHeader, error)
}

type CommitLog interface {
	AppendCommit(batch.Prepared, base.CommitSeq) (appendlog.CommitAppendResult, error)
}

type Result struct {
	BatchID   base.BatchID
	CommitSeq base.CommitSeq
}

type Coordinator struct {
	mu       sync.Mutex
	next     base.CommitSeq
	log      CommitLog
	mapping  api.Mapping
	reader   RecordReader
	faulted  bool
	faultErr error
	hook     failpoint.Hook
}

const (
	PointMappingPublished failpoint.Point = "commit.mapping-published"
	PointResultReady      failpoint.Point = "commit.result-ready"
)

func New(next base.CommitSeq, log CommitLog, mapping api.Mapping, reader RecordReader) (*Coordinator, error) {
	return NewWithHook(next, log, mapping, reader, nil)
}

func NewWithHook(next base.CommitSeq, log CommitLog, mapping api.Mapping, reader RecordReader, hook failpoint.Hook) (*Coordinator, error) {
	if next == 0 || log == nil || mapping == nil || reader == nil || next <= mapping.CoveredCommitSeq() {
		return nil, fmt.Errorf("commit coordinator configuration: %w", base.ErrInvalidConfig)
	}
	return &Coordinator{next: next, log: log, mapping: mapping, reader: reader, hook: hook}, nil
}

func (c *Coordinator) Commit(ctx context.Context, b *batch.Batch) (Result, error) {
	if b == nil {
		return Result{}, fmt.Errorf("nil batch: %w", base.ErrInvalidConfig)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.faulted {
		return Result{}, errors.Join(base.ErrReadOnly, c.faultErr)
	}
	prepared, err := b.Prepare()
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		_ = b.MarkAborted()
		return Result{}, err
	}
	if c.next == base.CommitSeq(math.MaxUint64) {
		_ = b.MarkAborted()
		return Result{}, base.ErrGenerationExhausted
	}
	if err := c.validatePutRecords(prepared); err != nil {
		_ = b.MarkAborted()
		c.fail(err)
		return Result{}, err
	}
	conflict, err := c.hasConflict(prepared.Conditions)
	if err != nil {
		_ = b.MarkAborted()
		c.fail(err)
		return Result{}, err
	}
	if conflict {
		_ = b.MarkAborted()
		return Result{}, base.ErrConflict
	}
	if err := ctx.Err(); err != nil {
		_ = b.MarkAborted()
		return Result{}, err
	}
	commitSeq := c.next
	appendResult, err := c.log.AppendCommit(prepared, commitSeq)
	if err != nil {
		if appendResult.SealStarted {
			_ = b.MarkCommitUnknown()
			c.fail(err)
			return Result{}, errors.Join(base.ErrCommitUnknown, err)
		}
		_ = b.MarkAborted()
		if !errors.Is(err, segment.ErrFull) {
			c.fail(err)
		}
		return Result{}, err
	}
	changes := make([]api.Change, len(prepared.Mutations))
	for i, mutation := range prepared.Mutations {
		changes[i] = api.Change{RecordID: mutation.RecordID}
		if mutation.Operation == batch.Put {
			changes[i].NewAddr = mutation.Addr
		}
	}
	applyResult, err := c.mapping.Apply(commitSeq, api.ApplyUserCommit, changes)
	if err != nil || applyResult.Applied != uint32(len(changes)) || applyResult.Skipped != 0 {
		if err == nil {
			err = fmt.Errorf("mapping applied %d/%d changes: %w", applyResult.Applied, len(changes), base.ErrCorrupt)
		}
		_ = b.MarkCommitUnknown()
		c.fail(err)
		return Result{}, errors.Join(base.ErrCommitUnknown, err)
	}
	if err := failpoint.Hit(c.hook, PointMappingPublished); err != nil {
		_ = b.MarkCommitUnknown()
		c.fail(err)
		return Result{}, errors.Join(base.ErrCommitUnknown, err)
	}
	if err := b.MarkCommitted(commitSeq); err != nil {
		c.fail(err)
		return Result{}, errors.Join(base.ErrCommitUnknown, err)
	}
	c.next++
	if err := failpoint.Hit(c.hook, PointResultReady); err != nil {
		c.fail(err)
		return Result{}, errors.Join(base.ErrCommitUnknown, err)
	}
	return Result{BatchID: prepared.BatchID, CommitSeq: commitSeq}, nil
}

func (c *Coordinator) validatePutRecords(prepared batch.Prepared) error {
	for _, mutation := range prepared.Mutations {
		if mutation.Operation != batch.Put {
			continue
		}
		header, err := c.reader.ReadPutHeader(mutation.Addr)
		if err != nil {
			return err
		}
		if header.RecordID != mutation.RecordID || header.OriginBatch != prepared.BatchID ||
			header.ValueBytes != mutation.ValueBytes || header.PhysicalSize != mutation.PhysicalSize {
			return fmt.Errorf("put record identity mismatch: %w", base.ErrCorrupt)
		}
	}
	return nil
}

func (c *Coordinator) hasConflict(conditions []batch.Condition) (bool, error) {
	for _, condition := range conditions {
		addr, exists, err := c.mapping.Lookup(condition.RecordID)
		if err != nil {
			return false, err
		}
		switch condition.Kind {
		case batch.ConditionAbsent:
			if exists {
				return true, nil
			}
		case batch.ConditionRevision:
			if !exists {
				return true, nil
			}
			header, err := c.reader.ReadPutHeader(addr)
			if err != nil {
				return false, err
			}
			if header.RecordID != condition.RecordID {
				return false, fmt.Errorf("mapping record identity mismatch: %w", base.ErrCorrupt)
			}
			if base.Revision(header.OriginBatch) != condition.Revision {
				return true, nil
			}
		default:
			return false, fmt.Errorf("condition kind: %w", base.ErrCorrupt)
		}
	}
	return false, nil
}

func (c *Coordinator) NextCommitSeq() base.CommitSeq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.next
}

func (c *Coordinator) Fault() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.faulted {
		return nil
	}
	return c.faultErr
}

func (c *Coordinator) fail(err error) {
	c.faulted = true
	c.faultErr = err
}
