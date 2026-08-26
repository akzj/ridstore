package mapstore

import "errors"

var (
	ErrInvalid          = errors.New("mapstore: invalid input")
	ErrCorrupt          = errors.New("mapstore: corrupt data")
	ErrUnsupported      = errors.New("mapstore: unsupported format")
	ErrFull             = errors.New("mapstore: segment full")
	ErrClosed           = errors.New("mapstore: closed")
	ErrPoisoned         = errors.New("mapstore: write state uncertain")
	ErrRecoveryRequired = errors.New("mapstore: recovery required")
)
