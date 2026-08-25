package mapstore

import "errors"

var (
	ErrInvalid     = errors.New("mapstore: invalid input")
	ErrCorrupt     = errors.New("mapstore: corrupt data")
	ErrUnsupported = errors.New("mapstore: unsupported format")
)
