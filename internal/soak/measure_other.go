//go:build !linux && !darwin

package soak

import (
	"fmt"
	"os"

	"github.com/akzj/ridstore/internal/base"
)

// The soak harness is a Linux evidence tool: its report carries filesystem
// block size, filesystem type, device id, descriptor counts and RSS, all read
// from POSIX interfaces. On other platforms the package still builds and the
// harness refuses to start rather than emitting a report with invented values.
func filesystemStat(string) (fsIdentity, error) {
	return fsIdentity{}, fmt.Errorf("soak harness requires a POSIX filesystem (linux or darwin): %w", base.ErrUnsupported)
}

func allocatedFor(os.FileInfo) (uint64, bool) { return 0, false }

func countFDs() int { return -1 }

func rssBytes() uint64 { return 0 }
