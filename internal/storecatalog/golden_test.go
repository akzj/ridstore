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
	const want = "7a3cba71f2d625fb68fa4759a413f6b6a658a7f5361f3fc567e13bc01fcb4888"
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("golden digest mismatch\ngot:  %s\nwant: %s", got, want)
	}
}
