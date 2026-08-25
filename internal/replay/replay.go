package replay

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type Log interface {
	Scan(context.Context, recordlog.LogPos, func(recordlog.AppendResult, []byte) error) error
	Read(context.Context, recordlog.VAddr) ([]byte, error)
}

type Config struct {
	MaxValueSize        uint64
	MaxRecordPayload    uint64
	MaxGroupDescriptors uint64
	MaxGroupMutations   uint64
	IDReserveSize       uint64
	BatchIDReserveSize  uint64
}

type Checkpoint struct {
	Mapping             mapping.Index
	ReplayStart         recordlog.LogPos
	ReservedIDHigh      uint64
	ReservedBatchIDHigh uint64
	OpenBatchIDs        []model.BatchID
}

type BatchState uint8

const (
	BatchAborted BatchState = iota + 1
	BatchCommitted
)

type BatchStatus struct {
	State     BatchState
	CommitSeq model.CommitSeq
}

type Result struct {
	Mapping             mapping.Index
	NextCommitSeq       model.CommitSeq
	ReservedIDHigh      uint64
	ReservedBatchIDHigh uint64
	Statuses            map[model.BatchID]BatchStatus
}

func Recover(ctx context.Context, log Log, checkpoint Checkpoint, config Config) (Result, error) {
	if log == nil || !checkpoint.ReplayStart.Valid() || checkpoint.ReservedIDHigh == 0 || checkpoint.ReservedBatchIDHigh == 0 ||
		config.MaxValueSize == 0 || config.MaxRecordPayload == 0 || config.MaxGroupDescriptors == 0 || config.MaxGroupMutations == 0 ||
		config.IDReserveSize == 0 || config.BatchIDReserveSize == 0 || checkpoint.Mapping == nil || checkpoint.Mapping.CoveredCommitSeq() == model.CommitSeq(math.MaxUint64) {
		return Result{}, base.ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	current := checkpoint.Mapping
	result := Result{
		Mapping: current, NextCommitSeq: checkpoint.Mapping.CoveredCommitSeq() + 1,
		ReservedIDHigh: checkpoint.ReservedIDHigh, ReservedBatchIDHigh: checkpoint.ReservedBatchIDHigh,
		Statuses: make(map[model.BatchID]BatchStatus, len(checkpoint.OpenBatchIDs)),
	}
	seenTerminal := make(map[model.BatchID]struct{})
	for _, id := range checkpoint.OpenBatchIDs {
		if id == 0 || uint64(id) >= result.ReservedBatchIDHigh {
			return Result{}, corrupt(nil)
		}
		if _, exists := result.Statuses[id]; exists {
			return Result{}, corrupt(nil)
		}
		result.Statuses[id] = BatchStatus{State: BatchAborted}
	}
	err := log.Scan(ctx, checkpoint.ReplayStart, func(physical recordlog.AppendResult, payload []byte) error {
		typ, err := recordcodec.TypeOf(payload)
		if err != nil {
			return corrupt(err)
		}
		switch typ {
		case recordcodec.RecordTypePut:
			put, err := recordcodec.DecodePut(payload, config.MaxValueSize)
			if err != nil || uint64(put.RecordID) >= result.ReservedIDHigh || uint64(put.OriginBatchID) >= result.ReservedBatchIDHigh {
				return corruptAt("put allocation bounds", err)
			}
		case recordcodec.RecordTypeCommitGroup:
			if err := replayGroup(ctx, log, current, &result, seenTerminal, physical, payload, config); err != nil {
				return err
			}
		case recordcodec.RecordTypeAbort:
			abort, err := recordcodec.DecodeAbort(payload)
			if err != nil || uint64(abort.BatchID) >= result.ReservedBatchIDHigh {
				return corrupt(err)
			}
			if _, exists := seenTerminal[abort.BatchID]; exists {
				return corrupt(errors.New("duplicate terminal batch"))
			}
			seenTerminal[abort.BatchID] = struct{}{}
			result.Statuses[abort.BatchID] = BatchStatus{State: BatchAborted}
		case recordcodec.RecordTypeIDReserve, recordcodec.RecordTypeBatchIDReserve:
			record, err := recordcodec.DecodeReserve(payload, typ)
			if err != nil {
				return corruptAt("reserve record", err)
			}
			if typ == recordcodec.RecordTypeIDReserve {
				result.ReservedIDHigh, err = idalloc.AdvanceRecovered(idalloc.RecordID, config.IDReserveSize, result.ReservedIDHigh, record)
			} else {
				result.ReservedBatchIDHigh, err = idalloc.AdvanceRecovered(idalloc.BatchID, config.BatchIDReserveSize, result.ReservedBatchIDHigh, record)
			}
			if err != nil {
				return corrupt(err)
			}
		case recordcodec.RecordTypeCheckpoint:
			marker, err := recordcodec.DecodeCheckpoint(payload)
			if err != nil || marker.CoveredCommitSeq > current.CoveredCommitSeq() {
				return corrupt(err)
			}
		default:
			return corrupt(nil)
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func replayGroup(ctx context.Context, log Log, current mapping.Index, result *Result, seenTerminal map[model.BatchID]struct{}, physical recordlog.AppendResult, payload []byte, config Config) error {
	group, err := recordcodec.DecodeCommitGroup(payload, config.MaxRecordPayload, config.MaxGroupDescriptors, config.MaxGroupMutations)
	if err != nil || group.Descriptors[0].CommitSeq != result.NextCommitSeq {
		return corruptAt("commit sequence", err)
	}
	proposals := make([]mapping.Proposal, len(group.Descriptors))
	reservations := make([]mapping.DeltaReservation, len(group.Descriptors))
	releaseReservations := func() {
		for _, reservation := range reservations {
			if reservation != nil {
				reservation.Release()
			}
		}
	}
	for i, descriptor := range group.Descriptors {
		if uint64(descriptor.BatchID) >= result.ReservedBatchIDHigh {
			return corrupt(errors.New("descriptor batch outside reserved range"))
		}
		if _, exists := seenTerminal[descriptor.BatchID]; exists {
			return corrupt(errors.New("duplicate terminal batch"))
		}
		proposal, err := descriptorProposal(ctx, log, descriptor, physical.Addr, result.ReservedIDHigh, config.MaxValueSize)
		if err != nil {
			return err
		}
		proposals[i] = proposal
		ids := make([]model.ID, len(descriptor.Mutations))
		for mutationIndex, mutation := range descriptor.Mutations {
			ids[mutationIndex] = mutation.RecordID
		}
		reservation, _, err := current.ReserveDelta(ids)
		if err != nil {
			releaseReservations()
			return errors.Join(base.ErrInvalidConfig, err)
		}
		reservations[i] = reservation
		seenTerminal[descriptor.BatchID] = struct{}{}
	}
	plan, err := current.ResolveGroup(proposals)
	if err != nil {
		releaseReservations()
		return corrupt(err)
	}
	for i, proposal := range plan.Proposals {
		if !proposal.Accepted {
			return corrupt(errors.New("replayed proposal was rejected"))
		}
		result.Statuses[group.Descriptors[i].BatchID] = BatchStatus{State: BatchCommitted, CommitSeq: group.Descriptors[i].CommitSeq}
	}
	if _, err := current.PublishGroup(result.NextCommitSeq, plan, reservations); err != nil {
		releaseReservations()
		return corrupt(err)
	}
	last := group.Descriptors[len(group.Descriptors)-1].CommitSeq
	if last == model.CommitSeq(math.MaxUint64) {
		return corrupt(errors.New("commit sequence exhausted"))
	}
	result.NextCommitSeq = last + 1
	return nil
}

func descriptorProposal(ctx context.Context, log Log, descriptor recordcodec.Descriptor, commitAddr recordlog.VAddr, reservedIDHigh, maxValue uint64) (mapping.Proposal, error) {
	proposal := mapping.Proposal{Changes: make([]mapping.Change, len(descriptor.Mutations))}
	if descriptor.Kind == recordcodec.DescriptorRelocation {
		proposal.Kind = mapping.ProposalRelocation
	} else {
		proposal.Kind = mapping.ProposalUserCommit
	}
	var logical uint64
	for i, mutation := range descriptor.Mutations {
		if uint64(mutation.RecordID) >= reservedIDHigh {
			return mapping.Proposal{}, corrupt(errors.New("mutation outside reserved ID range"))
		}
		change := mapping.Change{RecordID: mutation.RecordID, NewAddr: mutation.NewAddr, ExpectedOldAddr: mutation.ExpectedOldAddr}
		switch mutation.Operation {
		case recordcodec.OperationDelete:
			change.Operation = mapping.OperationDelete
		case recordcodec.OperationPut, recordcodec.OperationRelocate:
			if !before(mutation.NewAddr, commitAddr) || mutation.Operation == recordcodec.OperationRelocate && !before(mutation.ExpectedOldAddr, commitAddr) {
				return mapping.Proposal{}, corrupt(errors.New("mutation address does not precede commit"))
			}
			put, err := readPut(ctx, log, mutation.NewAddr, maxValue)
			if err != nil || put.RecordID != mutation.RecordID {
				return mapping.Proposal{}, corruptAt("new put identity", err)
			}
			if logical > math.MaxUint64-uint64(len(put.Value)) {
				return mapping.Proposal{}, corrupt(errors.New("logical payload overflow"))
			}
			logical += uint64(len(put.Value))
			if mutation.Operation == recordcodec.OperationPut {
				if put.OriginBatchID != descriptor.BatchID {
					return mapping.Proposal{}, corrupt(errors.New("user put origin batch mismatch"))
				}
				change.Operation = mapping.OperationPut
			} else {
				old, err := readPut(ctx, log, mutation.ExpectedOldAddr, maxValue)
				if err != nil || old.RecordID != put.RecordID || old.OriginBatchID != put.OriginBatchID || !equalBytes(old.Value, put.Value) {
					return mapping.Proposal{}, corruptAt("relocation record mismatch", err)
				}
				change.Operation = mapping.OperationRelocate
			}
		default:
			return mapping.Proposal{}, corrupt(errors.New("unknown mutation operation"))
		}
		proposal.Changes[i] = change
	}
	if logical != descriptor.LogicalPayloadBytes {
		return mapping.Proposal{}, corrupt(errors.New("logical payload size mismatch"))
	}
	return proposal, nil
}

func before(addr, limit recordlog.VAddr) bool {
	if !addr.Valid() || !limit.Valid() {
		return false
	}
	return addr.SegmentID() < limit.SegmentID() || addr.SegmentID() == limit.SegmentID() && addr.Offset() < limit.Offset()
}

func readPut(ctx context.Context, log Log, addr recordlog.VAddr, maxValue uint64) (recordcodec.PutRecord, error) {
	payload, err := log.Read(ctx, addr)
	if err != nil {
		return recordcodec.PutRecord{}, err
	}
	return recordcodec.DecodePut(payload, maxValue)
}

func corrupt(cause error) error {
	if cause == nil {
		return base.ErrCorrupt
	}
	return errors.Join(base.ErrCorrupt, cause)
}

func corruptAt(message string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%s: %w", message, base.ErrCorrupt)
	}
	return fmt.Errorf("%s: %w", message, errors.Join(base.ErrCorrupt, cause))
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
