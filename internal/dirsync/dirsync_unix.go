//go:build unix

package dirsync

// Flush makes the entries of an already-open directory durable.
func Flush(flusher Flusher) error {
	if flusher == nil {
		return nil
	}
	return flusher.Sync()
}
