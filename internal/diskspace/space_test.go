package diskspace

import "testing"

func TestAvailable(t *testing.T) {
	available, err := Available(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if available == 0 {
		t.Fatal("filesystem reported no available bytes")
	}
}
