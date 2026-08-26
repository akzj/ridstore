package ridstore

import (
	"errors"

	"github.com/akzj/ridstore/internal/base"
)

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
	ErrInvalidToken        = base.ErrInvalidToken
	ErrConflict            = base.ErrConflict
	ErrIDExhausted         = base.ErrIDExhausted
	ErrAddressExhausted    = base.ErrAddressExhausted
	ErrGenerationExhausted = base.ErrGenerationExhausted
	ErrCommitUnknown       = base.ErrCommitUnknown
	ErrStatusExpired       = base.ErrStatusExpired
	ErrStatusCapacity      = base.ErrStatusCapacity
	ErrCorrupt             = base.ErrCorrupt
	ErrUnsupported         = base.ErrUnsupported
	ErrInsufficientSpace   = base.ErrInsufficientSpace
	ErrRecoveryRequired    = base.ErrRecoveryRequired
	ErrVerifyLimit         = errors.New("ridstore: verification resource limit reached")
)
