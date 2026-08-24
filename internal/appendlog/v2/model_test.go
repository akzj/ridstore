package v2

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"testing"
)

type modelRecord struct {
	addr    VAddr
	payload []byte
}

func TestRandomizedAppendReadScanAndReopen(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentSize = 1024
	cfg.MaxPayloadSize = 192
	cfg.MaxBufferBytes = 384
	cfg.MaxBufferRecords = 7

	rng := rand.New(rand.NewSource(0x5eed))
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var model []modelRecord
	for operation := 0; operation < 750; operation++ {
		size := rng.Intn(int(cfg.MaxPayloadSize) + 1)
		payload := make([]byte, size)
		if len(payload) >= 8 {
			binary.LittleEndian.PutUint64(payload, uint64(operation))
			_, _ = rng.Read(payload[8:])
		} else {
			_, _ = rng.Read(payload)
		}
		want := append([]byte(nil), payload...)
		addr, err := log.Append(context.Background(), payload, rng.Intn(7) == 0)
		if err != nil {
			t.Fatalf("operation %d append: %v", operation, err)
		}
		clear(payload)
		if len(model) != 0 && addr <= model[len(model)-1].addr {
			t.Fatalf("operation %d address %v after %v", operation, addr, model[len(model)-1].addr)
		}
		model = append(model, modelRecord{addr: addr, payload: want})
		assertWatermarksOrdered(t, log.Status().Watermarks)

		if operation%11 == 0 {
			index := rng.Intn(len(model))
			got, err := log.Read(context.Background(), model[index].addr)
			if err != nil || !bytes.Equal(got, model[index].payload) {
				t.Fatalf("operation %d read %d = %x, %v; want %x", operation, index, got, err, model[index].payload)
			}
		}
		if operation%37 == 0 {
			assertScanMatchesModel(t, log, model, rng.Intn(len(model)))
		}
		if operation != 0 && operation%113 == 0 {
			if err := log.Close(); err != nil {
				t.Fatalf("operation %d close: %v", operation, err)
			}
			log, err = Open(cfg)
			if err != nil {
				t.Fatalf("operation %d reopen: %v", operation, err)
			}
			assertScanMatchesModel(t, log, model, 0)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	assertScanMatchesModel(t, log, model, 0)
}

func assertScanMatchesModel(t *testing.T, log *Log, model []modelRecord, start int) {
	t.Helper()
	var got []modelRecord
	if err := log.Scan(context.Background(), model[start].addr, func(addr VAddr, payload []byte) error {
		got = append(got, modelRecord{addr: addr, payload: append([]byte(nil), payload...)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := model[start:]
	if len(got) != len(want) {
		t.Fatalf("scan from %d returned %d records, want %d", start, len(got), len(want))
	}
	for i := range want {
		if got[i].addr != want[i].addr || !bytes.Equal(got[i].payload, want[i].payload) {
			t.Fatalf("scan record %d = (%v, %x), want (%v, %x)", start+i, got[i].addr, got[i].payload, want[i].addr, want[i].payload)
		}
	}
}

func assertWatermarksOrdered(t *testing.T, watermarks Watermarks) {
	t.Helper()
	if !positionLessEqual(watermarks.Durable, watermarks.Written) || !positionLessEqual(watermarks.Written, watermarks.Reserved) {
		t.Fatalf("unordered watermarks: %+v", watermarks)
	}
}

func positionLessEqual(left, right Position) bool {
	return left.SegmentID < right.SegmentID || left.SegmentID == right.SegmentID && left.Offset <= right.Offset
}
