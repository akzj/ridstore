//go:build !linux && !darwin

package backuprestore

import "github.com/akzj/ridstore/internal/base"

func publicationSupported() bool { return false }

// Backup/Restore publication requires an atomic no-replace directory rename.
// Platforms without an implementation fail safely instead of falling back to
// a check-then-rename sequence that could overwrite a racing empty directory.
func renameNoReplace(_, _ string) error { return base.ErrUnsupported }
