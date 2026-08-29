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
	const want = "eeb1616eddf25b08bbd7686d6d252956aad1e971a20c5b9b823fd102405c4e77"
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("golden digest mismatch\ngot:  %s\nwant: %s", got, want)
	}
}
