package mapping

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type revisionReader struct {
	payload []byte
	err     error
}

func (r revisionReader) Read(context.Context, recordlog.VAddr) ([]byte, error) {
	return r.payload, r.err
}

func TestPutRevisionResolverValidatesRecordIdentity(t *testing.T) {
	payload, err := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 19, RecordID: 7, Value: []byte("value")}, 64)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPutRevisionResolver(revisionReader{payload: payload}, 64)
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(t, 1, 64)
	if got, err := resolver.ResolveRevision(addr, 7); err != nil || got != model.Revision(19) {
		t.Fatalf("revision=%d err=%v", got, err)
	}
	if _, err := resolver.ResolveRevision(addr, 8); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}
