//go:build windows

package dirsync

// flush is a no-op on Windows: FlushFileBuffers rejects directory handles with
// ERROR_ACCESS_DENIED, and there is no supported API that forces a directory
// entry to disk. See the package comment for what this costs and why it is
// acceptable here.
// Flush makes the entries of an already-open directory durable.
func Flush(Flusher) error {
	return nil
}
