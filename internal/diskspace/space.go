package diskspace

import (
	"fmt"
	"math"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
)

// Available returns bytes currently available to the process on the
// filesystem containing path. It is an admission signal, not a reservation;
// callers must still handle ENOSPC from every subsequent write and fsync.
func Available(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	blocks, blockSize := uint64(stat.Bavail), uint64(stat.Bsize)
	if blockSize != 0 && blocks > math.MaxUint64/blockSize {
		return 0, fmt.Errorf("filesystem available bytes: %w", base.ErrOverflow)
	}
	return blocks * blockSize, nil
}
