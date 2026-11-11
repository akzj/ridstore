// Package dirsync makes directory-entry changes durable.
//
// POSIX requires an explicit flush of the directory itself for a create,
// rename or unlink to survive a crash; ridstore calls it before reporting any
// publication as durable. Windows has no equivalent call: FlushFileBuffers
// returns ERROR_ACCESS_DENIED for directory handles, so the operation is a
// no-op and durability of directory entries relies on the filesystem's own
// metadata journaling.
//
// The difference is deliberate, not an oversight: it is listed in README's
// platform matrix. File data is still synced on every platform through
// *os.File.Sync, so a Windows store keeps the record-level durable-before-
// publish guarantee; only the ordering of newly created directory entries is
// left to NTFS.
package dirsync

// Flusher is the subset of a directory handle this package needs. Every
// caller in ridstore wraps the handle in its own file abstraction so fault
// injection stays possible, hence the interface rather than *os.File.
type Flusher interface {
	Sync() error
}
