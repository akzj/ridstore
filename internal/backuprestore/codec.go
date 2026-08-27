package backuprestore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"sort"
	"strings"
)

const (
	MetadataName          = "BACKUP-v2"
	IncompleteName        = "INCOMPLETE-v2"
	RestoreIncompleteName = "RESTORE-INCOMPLETE-v2"
	PayloadDirectory      = "payload"
	metadataHeaderSize    = 80
	entryHeaderSize       = 44
	maxMetadataSize       = 256 << 20
	maxEntries            = 1 << 20
	maxPathBytes          = 4096
	formatMajor           = 2
	formatMinor           = 0
)

var (
	metadataMagic  = [8]byte{'R', 'I', 'D', 'B', 'A', 'K', '2', 0}
	crcTable       = crc32.MakeTable(crc32.Castagnoli)
	errInvalid     = errors.New("backuprestore: invalid artifact")
	errUnsupported = errors.New("backuprestore: unsupported artifact")
)

type Entry struct {
	Path   string
	Size   uint64
	SHA256 [32]byte
}

type Metadata struct {
	StoreID            [16]byte
	RecordLogID        [16]byte
	ManifestGeneration uint64
	CreatedUnixNano    int64
	Entries            []Entry
}

func EncodeMetadata(metadata Metadata) ([]byte, error) {
	entries := append([]Entry(nil), metadata.Entries...)
	if metadata.StoreID == ([16]byte{}) || metadata.RecordLogID == ([16]byte{}) || metadata.ManifestGeneration == 0 || len(entries) == 0 || len(entries) > maxEntries {
		return nil, errInvalid
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	payloadSize := uint64(0)
	for index, entry := range entries {
		if !validRelativePath(entry.Path) || len(entry.Path) > maxPathBytes || index != 0 && entries[index-1].Path == entry.Path {
			return nil, errInvalid
		}
		payloadSize += entryHeaderSize + uint64(len(entry.Path))
		if payloadSize > maxMetadataSize-metadataHeaderSize {
			return nil, errInvalid
		}
	}
	encoded := make([]byte, metadataHeaderSize+int(payloadSize))
	copy(encoded[:8], metadataMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], formatMajor)
	binary.LittleEndian.PutUint16(encoded[10:12], formatMinor)
	binary.LittleEndian.PutUint32(encoded[12:16], metadataHeaderSize)
	copy(encoded[16:32], metadata.StoreID[:])
	copy(encoded[32:48], metadata.RecordLogID[:])
	binary.LittleEndian.PutUint64(encoded[48:56], metadata.ManifestGeneration)
	binary.LittleEndian.PutUint64(encoded[56:64], uint64(metadata.CreatedUnixNano))
	binary.LittleEndian.PutUint32(encoded[64:68], uint32(len(entries)))
	binary.LittleEndian.PutUint64(encoded[68:76], payloadSize)
	offset := metadataHeaderSize
	for _, entry := range entries {
		binary.LittleEndian.PutUint16(encoded[offset:offset+2], uint16(len(entry.Path)))
		binary.LittleEndian.PutUint64(encoded[offset+4:offset+12], entry.Size)
		copy(encoded[offset+12:offset+44], entry.SHA256[:])
		copy(encoded[offset+44:], entry.Path)
		offset += entryHeaderSize + len(entry.Path)
	}
	checksum := crc32.Update(0, crcTable, encoded[:76])
	checksum = crc32.Update(checksum, crcTable, encoded[80:])
	binary.LittleEndian.PutUint32(encoded[76:80], checksum)
	return encoded, nil
}

func DecodeMetadata(encoded []byte) (Metadata, error) {
	if len(encoded) < metadataHeaderSize || len(encoded) > maxMetadataSize || string(encoded[:8]) != string(metadataMagic[:]) {
		return Metadata{}, errInvalid
	}
	if binary.LittleEndian.Uint16(encoded[8:10]) != formatMajor || binary.LittleEndian.Uint16(encoded[10:12]) != formatMinor {
		return Metadata{}, errUnsupported
	}
	if binary.LittleEndian.Uint32(encoded[12:16]) != metadataHeaderSize {
		return Metadata{}, errUnsupported
	}
	checksum := crc32.Update(0, crcTable, encoded[:76])
	checksum = crc32.Update(checksum, crcTable, encoded[80:])
	if binary.LittleEndian.Uint32(encoded[76:80]) != checksum {
		return Metadata{}, errInvalid
	}
	count := binary.LittleEndian.Uint32(encoded[64:68])
	payloadSize := binary.LittleEndian.Uint64(encoded[68:76])
	if count == 0 || count > maxEntries || uint64(count)*entryHeaderSize > payloadSize ||
		payloadSize > maxMetadataSize-metadataHeaderSize || payloadSize != uint64(len(encoded)-metadataHeaderSize) {
		return Metadata{}, errInvalid
	}
	metadata := Metadata{
		ManifestGeneration: binary.LittleEndian.Uint64(encoded[48:56]),
		CreatedUnixNano:    int64(binary.LittleEndian.Uint64(encoded[56:64])),
		Entries:            make([]Entry, 0, count),
	}
	copy(metadata.StoreID[:], encoded[16:32])
	copy(metadata.RecordLogID[:], encoded[32:48])
	if metadata.StoreID == ([16]byte{}) || metadata.RecordLogID == ([16]byte{}) || metadata.ManifestGeneration == 0 {
		return Metadata{}, errInvalid
	}
	offset := metadataHeaderSize
	for index := uint32(0); index < count; index++ {
		if len(encoded)-offset < entryHeaderSize {
			return Metadata{}, errInvalid
		}
		pathSize := int(binary.LittleEndian.Uint16(encoded[offset : offset+2]))
		if encoded[offset+2] != 0 || encoded[offset+3] != 0 || pathSize == 0 || pathSize > maxPathBytes || pathSize > len(encoded)-offset-entryHeaderSize {
			return Metadata{}, errInvalid
		}
		entry := Entry{Path: string(encoded[offset+44 : offset+44+pathSize]), Size: binary.LittleEndian.Uint64(encoded[offset+4 : offset+12])}
		copy(entry.SHA256[:], encoded[offset+12:offset+44])
		if !validRelativePath(entry.Path) || index != 0 && metadata.Entries[index-1].Path >= entry.Path {
			return Metadata{}, errInvalid
		}
		metadata.Entries = append(metadata.Entries, entry)
		offset += entryHeaderSize + pathSize
	}
	if offset != len(encoded) {
		return Metadata{}, errInvalid
	}
	return metadata, nil
}

func validRelativePath(path string) bool {
	return path != "" && path != "." && !filepath.IsAbs(path) && filepath.Clean(path) == path &&
		!strings.HasPrefix(path, ".."+string(filepath.Separator)) && !strings.Contains(path, "\\") && !strings.ContainsRune(path, 0)
}
