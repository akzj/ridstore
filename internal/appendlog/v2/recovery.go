package v2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type recoveredRecord struct {
	addr    VAddr
	payload []byte
	size    uint64
}

type recoveredSegment struct {
	header  segmentHeader
	end     uint64
	first   VAddr
	last    VAddr
	records uint64
	sealed  bool
}

func scanSegment(path string, expectSealed bool, repairTail bool) (recoveredSegment, []recoveredRecord, error) {
	var records []recoveredRecord
	recovered, err := visitSegment(path, expectSealed, repairTail, nil, osFileBackend{}, func(record recoveredRecord) error {
		records = append(records, record)
		return nil
	})
	return recovered, records, err
}

func scanSegmentMetadata(path string, expectSealed bool, repairTail bool) (recoveredSegment, error) {
	return visitSegment(path, expectSealed, repairTail, nil, osFileBackend{}, nil)
}

func scanSegmentMetadataWithHook(path string, expectSealed bool, repairTail bool, hook func(FaultPoint) error, files fileBackend) (recoveredSegment, error) {
	return visitSegment(path, expectSealed, repairTail, hook, files, nil)
}

func visitSegment(path string, expectSealed bool, repairTail bool, hook func(FaultPoint) error, files fileBackend, visit func(recoveredRecord) error) (recoveredSegment, error) {
	flag := os.O_RDONLY
	if repairTail {
		flag = os.O_RDWR
	}
	f, err := files.openFile(path, flag, 0)
	if err != nil {
		return recoveredSegment{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return recoveredSegment{}, err
	}
	if !info.Mode().IsRegular() || info.Size() < int64(segmentHeaderSize) {
		return recoveredSegment{}, fmt.Errorf("segment size: %w", ErrCorrupt)
	}
	headerBytes := make([]byte, segmentHeaderSize)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		return recoveredSegment{}, err
	}
	header, err := decodeSegmentHeader(headerBytes)
	if err != nil {
		return recoveredSegment{}, err
	}
	if uint64(info.Size()) > header.SegmentSize {
		return recoveredSegment{}, fmt.Errorf("segment exceeds capacity: %w", ErrCorrupt)
	}

	result := recoveredSegment{header: header, end: segmentHeaderSize}
	physicalEnd := uint64(info.Size())
	for result.end < physicalEnd {
		remaining := physicalEnd - result.end
		if repairTail && !expectSealed && remaining < segmentFooterSize {
			prefixSize := remaining
			if prefixSize > uint64(len(segmentFooterMagic)) {
				prefixSize = uint64(len(segmentFooterMagic))
			}
			prefix := make([]byte, prefixSize)
			if _, err := f.ReadAt(prefix, int64(result.end)); err == nil && string(prefix) == string(segmentFooterMagic[:prefixSize]) {
				if err := truncateActiveTail(f, result.end, hook); err != nil {
					return recoveredSegment{}, err
				}
				physicalEnd = result.end
				break
			}
		}
		if remaining >= segmentFooterSize {
			magic := make([]byte, len(segmentFooterMagic))
			if _, err := f.ReadAt(magic, int64(result.end)); err == nil && string(magic) == string(segmentFooterMagic[:]) {
				footerBytes := make([]byte, segmentFooterSize)
				if _, err := f.ReadAt(footerBytes, int64(result.end)); err != nil {
					return recoveredSegment{}, err
				}
				footer, err := decodeSegmentFooter(footerBytes)
				if err != nil || footer.SegmentID != header.SegmentID || footer.DataEnd != result.end || footer.FirstAddr != result.first || footer.LastAddr != result.last || footer.RecordCount != result.records || remaining != segmentFooterSize {
					return recoveredSegment{}, errors.Join(err, ErrCorrupt)
				}
				result.sealed = true
				break
			}
		}
		if remaining < recordHeaderSize {
			if repairTail && !expectSealed {
				if err := truncateActiveTail(f, result.end, hook); err != nil {
					return recoveredSegment{}, err
				}
				physicalEnd = result.end
				break
			}
			return recoveredSegment{}, fmt.Errorf("short record header: %w", ErrCorrupt)
		}
		headerBytes := make([]byte, recordHeaderSize)
		if _, err := f.ReadAt(headerBytes, int64(result.end)); err != nil {
			return recoveredSegment{}, err
		}
		recordHead, err := decodeRecordHeader(headerBytes)
		if err != nil {
			return recoveredSegment{}, err
		}
		if uint64(recordHead.PhysicalSize) > remaining {
			if repairTail && !expectSealed {
				if err := truncateActiveTail(f, result.end, hook); err != nil {
					return recoveredSegment{}, err
				}
				physicalEnd = result.end
				break
			}
			return recoveredSegment{}, fmt.Errorf("short record: %w", ErrCorrupt)
		}
		encoded := make([]byte, recordHead.PhysicalSize)
		if _, err := f.ReadAt(encoded, int64(result.end)); err != nil {
			return recoveredSegment{}, err
		}
		decoded, payload, err := decodeRecord(encoded)
		if err != nil {
			return recoveredSegment{}, err
		}
		wantAddr, err := makeVAddr(header.SegmentID, result.end)
		if err != nil || decoded.Addr != wantAddr || (result.last != 0 && decoded.Addr <= result.last) {
			return recoveredSegment{}, errors.Join(err, ErrCorrupt)
		}
		if result.records == 0 {
			result.first = decoded.Addr
		}
		result.last = decoded.Addr
		result.records++
		result.end += uint64(decoded.PhysicalSize)
		if visit != nil {
			if err := visit(recoveredRecord{addr: decoded.Addr, payload: payload, size: uint64(decoded.PhysicalSize)}); err != nil {
				return recoveredSegment{}, err
			}
		}
	}
	if expectSealed && !result.sealed {
		return recoveredSegment{}, fmt.Errorf("segment sealed state: %w", ErrCorrupt)
	}
	return result, nil
}

func truncateActiveTail(file fileHandle, end uint64, hook func(FaultPoint) error) error {
	if err := hitFault(hook, FaultBeforeTailTruncate); err != nil {
		return err
	}
	if err := file.Truncate(int64(end)); err != nil {
		return err
	}
	if err := hitFault(hook, FaultBeforeTailSync); err != nil {
		return err
	}
	return file.Sync()
}

func readRecordFile(path string, addr VAddr, maxPayload uint64, files fileBackend) ([]byte, error) {
	f, err := files.open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	headerBytes := make([]byte, recordHeaderSize)
	if _, err := f.ReadAt(headerBytes, int64(addr.Offset())); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrInvalidVAddr
		}
		return nil, err
	}
	h, err := decodeRecordHeader(headerBytes)
	if err != nil || h.Addr != addr || uint64(h.PayloadSize) > maxPayload {
		return nil, errors.Join(err, ErrInvalidVAddr)
	}
	encoded := make([]byte, h.PhysicalSize)
	if _, err := f.ReadAt(encoded, int64(addr.Offset())); err != nil {
		return nil, err
	}
	_, payload, err := decodeRecord(encoded)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), payload...), nil
}

func (l *Log) scanSnapshot(ctx context.Context, from VAddr, snapshot scanSnapshot, fn func(VAddr, []byte) error) error {
	if snapshot.last == 0 {
		return nil
	}
	for id := uint32(1); id <= snapshot.written.SegmentID; id++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		path, sealed, err := l.resolveSegmentPath(id)
		if err != nil {
			return err
		}
		visit := func(record recoveredRecord) error {
			if record.addr > snapshot.last || (from != 0 && record.addr < from) {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return fn(record.addr, append([]byte(nil), record.payload...))
		}
		if id < snapshot.written.SegmentID {
			if !sealed {
				return fmt.Errorf("non-terminal active segment: %w", ErrCorrupt)
			}
			_, err = visitSegment(path, true, false, nil, l.cfg.files, visit)
		} else {
			err = visitRecordPrefix(path, id, snapshot.written.Offset, l.cfg.MaxPayloadSize, l.cfg.files, visit)
		}
		if err != nil {
			return err
		}
	}
	addresses := make([]VAddr, 0, len(snapshot.pending))
	for addr := range snapshot.pending {
		if from == 0 || addr >= from {
			addresses = append(addresses, addr)
		}
	}
	sort.Slice(addresses, func(i, j int) bool { return addresses[i] < addresses[j] })
	for _, addr := range addresses {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(addr, append([]byte(nil), snapshot.pending[addr]...)); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) resolveSegmentPath(id uint32) (string, bool, error) {
	sealed := filepath.Join(l.cfg.Dir, sealedSegmentName(id))
	if _, err := l.cfg.files.stat(sealed); err == nil {
		return sealed, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	active := filepath.Join(l.cfg.Dir, activeSegmentName(id))
	if _, err := l.cfg.files.stat(active); err == nil {
		return active, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	return "", false, fmt.Errorf("segment %d missing: %w", id, ErrCorrupt)
}

func visitRecordPrefix(path string, segmentID uint32, limit, maxPayload uint64, files fileBackend, visit func(recoveredRecord) error) error {
	if limit < segmentHeaderSize {
		return fmt.Errorf("scan limit: %w", ErrCorrupt)
	}
	f, err := files.open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	headerBytes := make([]byte, segmentHeaderSize)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := decodeSegmentHeader(headerBytes)
	if err != nil || header.SegmentID != segmentID || limit > header.SegmentSize-segmentFooterSize {
		return errors.Join(err, ErrCorrupt)
	}
	for offset := segmentHeaderSize; offset < limit; {
		if limit-offset < recordHeaderSize {
			return fmt.Errorf("scan prefix header: %w", ErrCorrupt)
		}
		recordHeaderBytes := make([]byte, recordHeaderSize)
		if _, err := f.ReadAt(recordHeaderBytes, int64(offset)); err != nil {
			return err
		}
		h, err := decodeRecordHeader(recordHeaderBytes)
		if err != nil || uint64(h.PayloadSize) > maxPayload || uint64(h.PhysicalSize) > limit-offset {
			return errors.Join(err, ErrCorrupt)
		}
		encoded := make([]byte, h.PhysicalSize)
		if _, err := f.ReadAt(encoded, int64(offset)); err != nil {
			return err
		}
		decoded, payload, err := decodeRecord(encoded)
		want, addrErr := makeVAddr(segmentID, offset)
		if err != nil || addrErr != nil || decoded.Addr != want {
			return errors.Join(err, addrErr, ErrCorrupt)
		}
		if err := visit(recoveredRecord{addr: decoded.Addr, payload: payload, size: uint64(decoded.PhysicalSize)}); err != nil {
			return err
		}
		offset += uint64(decoded.PhysicalSize)
	}
	return nil
}
