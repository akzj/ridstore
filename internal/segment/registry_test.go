package segment

import (
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func TestRegistryReadsActiveSegment(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistry(active, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	addr, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := registry.ReadFrame(addr)
	if err != nil || frame.RecordID != 1 {
		t.Fatalf("frame=%+v error=%v", frame, err)
	}
}
