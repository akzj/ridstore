//go:build windows

package engine

import (
	"golang.org/x/sys/windows"
)

// filesystemAvailableBytes reports the free space the space gate may count on.
//
// Windows has no statfs. GetDiskFreeSpaceEx returns "available to the calling
// user", which is the same quantity Bavail*Bsize approximates on POSIX: it
// already excludes quotas and the volume's reserved space, so the gate keeps
// the same conservative meaning. The value is reported for the directory
// holding the store, which is what the caller passes in.
func filesystemAvailable(path string) (uint64, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var availableToCaller, totalBytes, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(name, &availableToCaller, &totalBytes, &totalFree); err != nil {
		return 0, err
	}
	return availableToCaller, nil
}
