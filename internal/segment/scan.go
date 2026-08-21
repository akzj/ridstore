package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

type ScanSummary struct {
	FirstFrameSeq base.FrameSeq
	LastFrameSeq  base.FrameSeq
	FrameCount    uint64
	LastFrame     storeformat.Frame
}

func SealedDataFileName(id base.DataSegmentID) string {
	return fmt.Sprintf("DATA-%08d.seg", id)
}

// ValidateSealedData performs a strict full-file scan. It never truncates or
// repairs a sealed file.
func ValidateSealedData(root string, uuid base.StoreUUID, summary storeformat.FileSummary, segmentSize, maxPayloadSize uint64) (retErr error) {
	if uuid == (base.StoreUUID{}) || summary.FileID == 0 || summary.ValidEnd <= storeformat.SegmentHeaderSize ||
		summary.ValidEnd+storeformat.SegmentFooterSize > segmentSize || maxPayloadSize == 0 {
		return fmt.Errorf("sealed data configuration: %w", base.ErrInvalidConfig)
	}
	path := filepath.Join(root, "data", SealedDataFileName(base.DataSegmentID(summary.FileID)))
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("open sealed data segment")
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) != summary.ValidEnd+storeformat.SegmentFooterSize {
		return fmt.Errorf("sealed data file size: %w", base.ErrCorrupt)
	}
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil {
		return err
	}
	if header.Kind != storeformat.SegmentKindData || header.StoreUUID != uuid || header.FileID != summary.FileID || header.FirstSeq != summary.FirstSeq {
		return fmt.Errorf("sealed data header identity: %w", base.ErrCorrupt)
	}
	validEnd, scanned, err := scanDataFrames(file, summary.ValidEnd, summary.ValidEnd, maxPayloadSize, base.FrameSeq(summary.FirstSeq), true, nil)
	if err != nil {
		return err
	}
	if validEnd != summary.ValidEnd || scanned.FirstFrameSeq != base.FrameSeq(summary.FirstSeq) || scanned.LastFrameSeq != base.FrameSeq(summary.LastSeq) {
		return fmt.Errorf("sealed data scan summary: %w", base.ErrCorrupt)
	}
	footerBytes := make([]byte, storeformat.SegmentFooterSize)
	if _, err := file.ReadAt(footerBytes, int64(summary.ValidEnd)); err != nil {
		return err
	}
	footer, err := storeformat.DecodeDataSegmentFooter(footerBytes)
	if err != nil {
		return err
	}
	if footer.SegmentID != base.DataSegmentID(summary.FileID) || footer.ValidDataEnd != summary.ValidEnd ||
		footer.FirstFrameSeq != scanned.FirstFrameSeq || footer.LastFrameSeq != scanned.LastFrameSeq || footer.FrameCount != scanned.FrameCount {
		return fmt.Errorf("sealed data footer mismatch: %w", base.ErrCorrupt)
	}
	seal, err := storeformat.DecodeSegmentSealFrame(scanned.LastFrame)
	if err != nil {
		return err
	}
	if seal.SegmentID != footer.SegmentID || seal.ValidDataEnd != footer.ValidDataEnd || seal.FirstFrameSeq != footer.FirstFrameSeq ||
		seal.LastFrameSeq != footer.LastFrameSeq || seal.FrameCount != footer.FrameCount || seal.MinCommitSeq != footer.MinCommitSeq || seal.MaxCommitSeq != footer.MaxCommitSeq {
		return fmt.Errorf("segment seal/footer mismatch: %w", base.ErrCorrupt)
	}
	return nil
}

func scanDataFrames(file *os.File, physicalEnd, contentLimit, maxPayloadSize uint64, firstSeq base.FrameSeq, strict bool, visit func(uint64, storeformat.Frame) error) (uint64, ScanSummary, error) {
	offset := uint64(storeformat.SegmentHeaderSize)
	var summary ScanSummary
	var previous base.FrameSeq
	for offset < physicalEnd {
		if summary.FrameCount != 0 && summary.LastFrame.Type == storeformat.FrameTypeSegmentSeal {
			return offset, summary, fmt.Errorf("frame follows segment seal: %w", base.ErrCorrupt)
		}
		remaining := physicalEnd - offset
		if remaining < storeformat.FrameHeaderSize {
			if strict {
				return offset, summary, fmt.Errorf("truncated frame header before strict end: %w", base.ErrCorrupt)
			}
			return offset, summary, nil
		}
		headerBytes := make([]byte, storeformat.FrameHeaderSize)
		if _, err := file.ReadAt(headerBytes, int64(offset)); err != nil {
			return offset, summary, err
		}
		limits := storeformat.FrameLimits{MaxPayloadSize: maxPayloadSize, RemainingSegmentSize: contentLimit - offset}
		header, err := storeformat.DecodeFrameHeader(headerBytes, limits)
		if err != nil {
			return offset, summary, err
		}
		if header.TotalSize > remaining {
			if strict {
				return offset, summary, fmt.Errorf("truncated frame payload before strict end: %w", base.ErrCorrupt)
			}
			return offset, summary, nil
		}
		total, err := base.Uint64ToInt(header.TotalSize)
		if err != nil {
			return offset, summary, fmt.Errorf("frame size: %w", base.ErrCorrupt)
		}
		encoded := make([]byte, total)
		if _, err := file.ReadAt(encoded, int64(offset)); err != nil {
			if !strict && errors.Is(err, io.EOF) {
				return offset, summary, nil
			}
			return offset, summary, err
		}
		frame, consumed, err := storeformat.DecodeFrame(encoded, limits)
		if err != nil {
			return offset, summary, err
		}
		if consumed != total || (summary.FrameCount == 0 && frame.FrameSeq != firstSeq) || (previous != 0 && frame.FrameSeq <= previous) {
			return offset, summary, fmt.Errorf("frame sequence or size: %w", base.ErrCorrupt)
		}
		if summary.FrameCount == 0 {
			summary.FirstFrameSeq = frame.FrameSeq
		}
		summary.LastFrameSeq = frame.FrameSeq
		summary.FrameCount++
		summary.LastFrame = frame
		if visit != nil {
			if err := visit(offset, frame); err != nil {
				return offset, summary, err
			}
		}
		previous = frame.FrameSeq
		offset += header.TotalSize
	}
	if strict && offset != physicalEnd {
		return offset, summary, fmt.Errorf("strict scan end mismatch: %w", base.ErrCorrupt)
	}
	return offset, summary, nil
}
