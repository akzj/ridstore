package recordcodec

import (
	"encoding/hex"
	"testing"
)

func TestPutGolden(t *testing.T) {
	t.Parallel()
	encoded, err := EncodePut(PutRecord{OriginBatchID: 7, RecordID: 9, Value: []byte("abc")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	const want = "5253503203000100200000002300000007000000000000000900000000000000616263"
	if got := hex.EncodeToString(encoded); got != want {
		t.Fatalf("golden mismatch\ngot:  %s\nwant: %s", got, want)
	}
}
