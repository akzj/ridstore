package recordlog

import (
	"encoding/hex"
	"testing"
)

func TestRecordGolden(t *testing.T) {
	t.Parallel()
	payload := []byte("abc")
	physical, _ := PhysicalRecordSize(uint64(len(payload)))
	addr, _ := NewVAddr(7, SegmentHeaderSize, physical)
	encoded, err := EncodeRecord(addr, payload)
	if err != nil {
		t.Fatal(err)
	}
	const want = "523252430200200028000000030000004000000007000000b73f4b366820f6ed6162630000000000"
	if got := hex.EncodeToString(encoded); got != want {
		t.Fatalf("golden mismatch\ngot:  %s\nwant: %s", got, want)
	}
}
