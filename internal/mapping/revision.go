package mapping

import (
	"context"
	"errors"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type RecordReader interface {
	Read(context.Context, recordlog.VAddr) ([]byte, error)
}

// PutRevisionResolver derives the logical revision from the authoritative
// PutRecord. Relocation copies preserve OriginBatchID, so no revision field is
// needed in a radix leaf.
type PutRevisionResolver struct {
	reader       RecordReader
	maxValueSize uint64
}

func NewPutRevisionResolver(reader RecordReader, maxValueSize uint64) (*PutRevisionResolver, error) {
	if reader == nil || maxValueSize == 0 {
		return nil, ErrInvalid
	}
	return &PutRevisionResolver{reader: reader, maxValueSize: maxValueSize}, nil
}

func (r *PutRevisionResolver) ResolveRevision(addr recordlog.VAddr, id model.ID) (model.Revision, error) {
	if !addr.Valid() || id == 0 {
		return 0, ErrInvalid
	}
	payload, err := r.reader.Read(context.Background(), addr)
	if err != nil {
		return 0, err
	}
	put, err := recordcodec.DecodePut(payload, r.maxValueSize)
	if err != nil || put.RecordID != id || put.OriginBatchID == 0 {
		return 0, errors.Join(ErrCorrupt, err)
	}
	return model.Revision(put.OriginBatchID), nil
}
