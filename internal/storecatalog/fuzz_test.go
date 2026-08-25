package storecatalog

import "testing"

func FuzzDecodeManifest(f *testing.F) {
	manifest := testManifest()
	seed, err := Encode(manifest)
	if err == nil {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = Decode(value)
	})
}
