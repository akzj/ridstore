package backuprestore

import (
	"errors"
	"testing"
)

func TestMetadataRoundTripAndCanonicalOrder(t *testing.T) {
	want := Metadata{
		StoreID: [16]byte{1}, RecordLogID: [16]byte{2}, ManifestGeneration: 9, CreatedUnixNano: 42,
		Entries: []Entry{
			{Path: "records/record-0000000001.active", Size: 10, SHA256: [32]byte{3}},
			{Path: "MANIFEST-v2-1", Size: 20, SHA256: [32]byte{4}},
		},
	}
	encoded, err := EncodeMetadata(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMetadata(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.StoreID != want.StoreID || got.RecordLogID != want.RecordLogID || got.ManifestGeneration != want.ManifestGeneration || got.CreatedUnixNano != want.CreatedUnixNano ||
		len(got.Entries) != 2 || got.Entries[0].Path != "MANIFEST-v2-1" || got.Entries[1].Path != "records/record-0000000001.active" {
		t.Fatalf("metadata=%+v", got)
	}
}

func TestMetadataRejectsCorruptionUnsupportedAndUnsafePaths(t *testing.T) {
	base := Metadata{StoreID: [16]byte{1}, RecordLogID: [16]byte{2}, ManifestGeneration: 1, Entries: []Entry{{Path: "MANIFEST-v2-1"}}}
	encoded, err := EncodeMetadata(base)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := DecodeMetadata(corrupt); !errors.Is(err, errInvalid) {
		t.Fatalf("corrupt err=%v", err)
	}
	unsupported := append([]byte(nil), encoded...)
	unsupported[10] = 1
	if _, err := DecodeMetadata(unsupported); !errors.Is(err, errUnsupported) {
		t.Fatalf("unsupported err=%v", err)
	}
	for _, path := range []string{"", ".", "../escape", "/absolute", "records/../escape", `records\escape`} {
		candidate := base
		candidate.Entries = []Entry{{Path: path}}
		if _, err := EncodeMetadata(candidate); !errors.Is(err, errInvalid) {
			t.Fatalf("path=%q err=%v", path, err)
		}
	}
}

func FuzzDecodeMetadata(f *testing.F) {
	encoded, err := EncodeMetadata(Metadata{
		StoreID: [16]byte{1}, RecordLogID: [16]byte{2}, ManifestGeneration: 1,
		Entries: []Entry{{Path: "MANIFEST-v2-1", Size: 10}},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodeMetadata(value)
	})
}
