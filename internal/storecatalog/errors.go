package storecatalog

import "errors"

var (
	ErrInvalid          = errors.New("storecatalog: invalid manifest")
	ErrCorrupt          = errors.New("storecatalog: corrupt manifest")
	ErrUnsupported      = errors.New("storecatalog: unsupported manifest")
	ErrConflict         = errors.New("storecatalog: generation conflict")
	ErrRecoveryRequired = errors.New("storecatalog: recovery required")
)
