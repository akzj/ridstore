package engine

import (
	"context"

	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/recordmeta"
)

type metadataAppender struct {
	next         transactionAppender
	cache        *recordmeta.Cache
	maxValueSize uint64
}

func (a *metadataAppender) Append(ctx context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	result, err := a.next.Append(ctx, payload, syncWrite)
	if err != nil || a.cache == nil || len(payload) < int(recordcodec.PutHeaderSize) {
		return result, err
	}
	put, decodeErr := recordcodec.DecodePutMetadata(payload[:recordcodec.PutHeaderSize], uint32(len(payload)), a.maxValueSize)
	physical, sizeErr := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if decodeErr == nil && sizeErr == nil {
		a.cache.Remember(result.Addr, put.RecordID, physical)
	}
	return result, nil
}
