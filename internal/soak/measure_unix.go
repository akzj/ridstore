//go:build linux || darwin

package soak

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// filesystemStat reports the identity of the filesystem holding path.
func filesystemStat(path string) (fsIdentity, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return fsIdentity{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fsIdentity{}, err
	}
	var device int64
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		device = int64(st.Dev)
	}
	return fsIdentity{Type: int64(stat.Type), BlockSize: int64(stat.Bsize), Device: device}, nil
}

// allocatedFor reports on-disk bytes held by a file when the platform exposes
// the block count. Allocated and logical size differ on sparse files, and the
// soak report must not conflate them.
func allocatedFor(info os.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(stat.Blocks) * 512, true
}

// countFDs reports the open descriptor count, or -1 when the platform does not
// expose it (macOS has no /proc).
func countFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

// rssBytes reports resident set size, or 0 when the platform does not expose
// it (macOS has no /proc).
func rssBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
