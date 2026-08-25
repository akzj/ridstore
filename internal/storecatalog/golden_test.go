package storecatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestManifestGoldenDigest(t *testing.T) {
	t.Parallel()
	encoded, err := Encode(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	const want = "ffc8ca910bf0b8ea20243c73fc1490416e14722c2b74da59e2ec259f70a3cfbb"
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("golden digest mismatch\ngot:  %s\nwant: %s", got, want)
	}
}
