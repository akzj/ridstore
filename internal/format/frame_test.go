package format

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

const testMaxPayload = uint64(1 << 20)

func TestFrameGoldenAndRoundTrip(t *testing.T) {
	t.Parallel()

	want := Frame{
		Type: FrameTypePutRecord, FrameSeq: 0x0807060504030201,
		BatchID: 0x1817161514131211, RecordID: 0x2827262524232221,
		Payload: []byte{0xde, 0xad, 0xbe, 0xef, 0x01},
	}
	encoded, err := EncodeFrame(want, testMaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, encoded, "9f59f26da78435e600a686a369ea4d6e9f97040937226c249213ccad5c13c4de")
	if got, wantSize := len(encoded), 72; got != wantSize {
		t.Fatalf("encoded size = %d, want %d", got, wantSize)
	}
	if got := encoded[64:69]; !equalBytes(got, want.Payload) {
		t.Fatalf("payload = %x, want %x", got, want.Payload)
	}
	if !allZero(encoded[69:72]) {
		t.Fatalf("padding is non-zero: %x", encoded[69:72])
	}

	limits := FrameLimits{MaxPayloadSize: testMaxPayload, RemainingSegmentSize: uint64(len(encoded))}
	header, err := DecodeFrameHeader(encoded, limits)
	if err != nil {
		t.Fatal(err)
	}
	if header.TotalSize != 72 || header.PayloadSize != 5 || header.Type != want.Type {
		t.Fatalf("unexpected header: %+v", header)
	}
	got, consumed, err := DecodeFrame(encoded, limits)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(encoded) || got.Type != want.Type || got.FrameSeq != want.FrameSeq ||
		got.BatchID != want.BatchID || got.RecordID != want.RecordID || !equalBytes(got.Payload, want.Payload) {
		t.Fatalf("decoded frame = %+v, consumed=%d", got, consumed)
	}
	value, revision, err := PutRecordValue(got)
	if err != nil || !equalBytes(value, want.Payload) || revision != base.Revision(want.BatchID) {
		t.Fatalf("put value=%x revision=%d error=%v", value, revision, err)
	}
}

func TestEmptyPutRecord(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeFrame(Frame{
		Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 2, RecordID: 3,
	}, testMaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != FrameHeaderSize || binary.LittleEndian.Uint32(encoded[56:60]) != 0 {
		t.Fatalf("empty frame size=%d payloadCRC=%d", len(encoded), binary.LittleEndian.Uint32(encoded[56:60]))
	}
	if _, _, err := DecodeFrame(encoded, FrameLimits{testMaxPayload, uint64(len(encoded))}); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeFrameToOverwritesReusedBuffer(t *testing.T) {
	t.Parallel()
	frame := Frame{Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 2, RecordID: 3, Payload: []byte{1}}
	size, err := EncodedFrameSize(frame, testMaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, size)
	for i := range dst {
		dst[i] = 0xff
	}
	written, err := EncodeFrameTo(dst, frame, testMaxPayload)
	if err != nil || written != len(dst) {
		t.Fatalf("written=%d error=%v", written, err)
	}
	if !allZero(dst[FrameHeaderSize+len(frame.Payload):]) {
		t.Fatalf("padding was not cleared: %x", dst[FrameHeaderSize+len(frame.Payload):])
	}
	if _, err := EncodeFrameTo(dst[:len(dst)-1], frame, testMaxPayload); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("short destination error=%v", err)
	}
}

func TestFrameRejectsInvalidIdentityAndShape(t *testing.T) {
	t.Parallel()

	tests := []Frame{
		{Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 0, RecordID: 1},
		{Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 0},
		{Type: FrameTypeCommitSeal, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: make([]byte, 64)},
		{Type: FrameTypeIDReserve, FrameSeq: 1, BatchID: 1, Payload: make([]byte, 24)},
		{Type: FrameTypeCommitPart, FrameSeq: 1, BatchID: 1, Payload: make([]byte, 31)},
		{Type: FrameTypeBatchAbort, FrameSeq: 1, BatchID: 1, Payload: make([]byte, 31)},
	}
	for i, frame := range tests {
		if _, err := EncodeFrame(frame, testMaxPayload); !errors.Is(err, base.ErrInvalidConfig) {
			t.Fatalf("case %d error = %v, want ErrInvalidConfig", i, err)
		}
	}
	if _, err := EncodeFrame(Frame{
		Type: FrameTypeBatchAbort, FrameSeq: 1, BatchID: 1, Payload: make([]byte, 32),
	}, testMaxPayload); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("semantic payload error = %v, want ErrInvalidConfig", err)
	}
}

func TestFrameDecoderRejectsCorruptionAndLimits(t *testing.T) {
	t.Parallel()

	encoded, err := EncodeFrame(Frame{
		Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 2, RecordID: 3,
		Payload: []byte{1, 2, 3},
	}, testMaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	goodLimits := FrameLimits{testMaxPayload, uint64(len(encoded))}

	tests := []struct {
		name string
		edit func([]byte)
		want error
	}{
		{name: "header checksum", edit: func(b []byte) { b[20] ^= 1 }, want: base.ErrCorrupt},
		{name: "payload checksum", edit: func(b []byte) { b[64] ^= 1 }, want: base.ErrCorrupt},
		{name: "padding", edit: func(b []byte) { b[len(b)-1] = 1 }, want: base.ErrCorrupt},
		{name: "reserved", edit: func(b []byte) { b[60] = 1; rewriteChecksum(b[:64], 52) }, want: base.ErrCorrupt},
		{name: "unknown type", edit: func(b []byte) { b[6] = 99; rewriteChecksum(b[:64], 52) }, want: base.ErrUnsupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bad := append([]byte(nil), encoded...)
			tt.edit(bad)
			if _, _, err := DecodeFrame(bad, goodLimits); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
	if _, _, err := DecodeFrame(encoded[:63], goodLimits); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("truncated header error = %v", err)
	}
	if _, _, err := DecodeFrame(encoded, FrameLimits{MaxPayloadSize: 2, RemainingSegmentSize: uint64(len(encoded))}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("payload limit error = %v", err)
	}
	if _, _, err := DecodeFrame(encoded, FrameLimits{MaxPayloadSize: testMaxPayload, RemainingSegmentSize: uint64(len(encoded) - 1)}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("segment boundary error = %v", err)
	}

	semantic, err := EncodeFrame(Frame{
		Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 2, RecordID: 3,
		Payload: make([]byte, 32),
	}, testMaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	semantic[6] = byte(FrameTypeBatchAbort)
	binary.LittleEndian.PutUint64(semantic[36:44], 0)
	rewriteChecksum(semantic[:64], 52)
	if _, _, err := DecodeFrame(semantic, FrameLimits{testMaxPayload, uint64(len(semantic))}); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("semantic payload decode error = %v, want ErrCorrupt", err)
	}
}

func TestSystemPayloadCodecs(t *testing.T) {
	t.Parallel()

	abort := BatchAbortPayload{
		Reason: AbortReasonConflict, FinalMutationCount: 7,
		AppendedPayloadBytes: 4096, LastBatchFrameSeq: 11,
	}
	abortBytes, err := EncodeBatchAbortPayload(abort)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeBatchAbortPayload(abortBytes[:]); err != nil || got != abort {
		t.Fatalf("abort decoded=%+v error=%v", got, err)
	}

	reserve := ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 1025, Generation: 1}
	reserveBytes, err := EncodeReservePayload(reserve)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeReservePayload(reserveBytes[:]); err != nil || got != reserve {
		t.Fatalf("reserve decoded=%+v error=%v", got, err)
	}

	seal := SegmentSealPayload{
		SegmentID: 3, ValidDataEnd: 8192,
		FirstFrameSeq: 5, LastFrameSeq: 13, FrameCount: 8,
		MinCommitSeq: 2, MaxCommitSeq: 4,
	}
	sealBytes, err := EncodeSegmentSealPayload(seal)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSegmentSealPayload(sealBytes[:]); err != nil || got != seal {
		t.Fatalf("seal decoded=%+v error=%v", got, err)
	}
	if got, err := DecodeSegmentSealFrame(Frame{Type: FrameTypeSegmentSeal, FrameSeq: 13, Payload: sealBytes[:]}); err != nil || got != seal {
		t.Fatalf("seal frame decoded=%+v error=%v", got, err)
	}
}

func TestSystemPayloadRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	if _, err := EncodeBatchAbortPayload(BatchAbortPayload{}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("abort error = %v", err)
	}
	if _, err := EncodeReservePayload(ReservePayload{PreviousHighExclusive: 2, NewHighExclusive: 2, Generation: 1}); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("reserve error = %v", err)
	}
	badAbort := make([]byte, 32)
	binary.LittleEndian.PutUint32(badAbort[0:4], 99)
	if _, err := DecodeBatchAbortPayload(badAbort); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("abort decode error = %v", err)
	}
}

func FuzzDecodeFrame(f *testing.F) {
	seed, _ := EncodeFrame(Frame{
		Type: FrameTypePutRecord, FrameSeq: 1, BatchID: 2, RecordID: 3,
		Payload: []byte("value"),
	}, testMaxPayload)
	f.Add(seed)
	f.Add([]byte("short"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeFrame(data, FrameLimits{
			MaxPayloadSize: testMaxPayload, RemainingSegmentSize: uint64(len(data)),
		})
	})
}

func FuzzDecodeSystemPayloads(f *testing.F) {
	abort, _ := EncodeBatchAbortPayload(BatchAbortPayload{Reason: AbortReasonCaller})
	reserve, _ := EncodeReservePayload(ReservePayload{PreviousHighExclusive: 1, NewHighExclusive: 2, Generation: 1})
	seal, _ := EncodeSegmentSealPayload(SegmentSealPayload{SegmentID: 1, ValidDataEnd: 8192, FirstFrameSeq: 1, LastFrameSeq: 1, FrameCount: 1})
	f.Add(byte(0), abort[:])
	f.Add(byte(1), reserve[:])
	f.Add(byte(2), seal[:])
	f.Fuzz(func(t *testing.T, kind byte, data []byte) {
		switch kind % 3 {
		case 0:
			_, _ = DecodeBatchAbortPayload(data)
		case 1:
			_, _ = DecodeReservePayload(data)
		case 2:
			_, _ = DecodeSegmentSealPayload(data)
		}
	})
}
