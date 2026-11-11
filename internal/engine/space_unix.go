//go:build unix

package engine

import (
	"errors"
	"math"
	"syscall"
)

// filesystemAvailableBytes reports the free space the space gate may count on.
// Bavail — not Bfree — excludes the blocks reserved for privileged users,
// which matches how the gate reasons about headroom: ridstore must not consume
// the margin the filesystem keeps for everything else.
func filesystemAvailable(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, errors.New("invalid filesystem block size")
	}
	if stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
		return math.MaxUint64, nil
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
