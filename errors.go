package ridstore

import "github.com/akzj/ridstore/internal/base"

var (
	ErrInvalidID           = base.ErrInvalidID
	ErrNotFound            = base.ErrNotFound
	ErrLocked              = base.ErrLocked
	ErrAlreadyExists       = base.ErrAlreadyExists
	ErrNotInitialized      = base.ErrNotInitialized
	ErrInvalidConfig       = base.ErrInvalidConfig
	ErrConfigMismatch      = base.ErrConfigMismatch
	ErrClosed              = base.ErrClosed
	ErrReadOnly            = base.ErrReadOnly
	ErrBatchClosed         = base.ErrBatchClosed
	ErrBatchFailed         = base.ErrBatchFailed
	ErrBatchTooLarge       = base.ErrBatchTooLarge
	ErrValueTooLarge       = base.ErrValueTooLarge
	ErrInvalidRevision     = base.ErrInvalidRevision
	ErrConflict            = base.ErrConflict
	ErrIDExhausted         = base.ErrIDExhausted
	ErrAddressExhausted    = base.ErrAddressExhausted
	ErrGenerationExhausted = base.ErrGenerationExhausted
	ErrCommitUnknown       = base.ErrCommitUnknown
	ErrStatusExpired       = base.ErrStatusExpired
	ErrCorrupt             = base.ErrCorrupt
	ErrUnsupported         = base.ErrUnsupported
)
