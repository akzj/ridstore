package recordcodec

import "errors"

var (
	ErrInvalid     = errors.New("recordcodec: invalid value")
	ErrTooLarge    = errors.New("recordcodec: payload too large")
	ErrCorrupt     = errors.New("recordcodec: corrupt payload")
	ErrUnsupported = errors.New("recordcodec: unsupported format")
)
