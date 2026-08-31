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
	const want = "a92c2ab3476822830f5c91f16e34a9d17ba5ae120529ba550884f2b8ac0f6479"
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("golden digest mismatch\ngot:  %s\nwant: %s", got, want)
	}
}
