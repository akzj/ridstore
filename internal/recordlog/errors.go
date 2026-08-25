package recordlog

import "errors"

var (
	ErrClosed        = errors.New("recordlog: closed")
	ErrPoisoned      = errors.New("recordlog: poisoned")
	ErrInvalidConfig = errors.New("recordlog: invalid configuration")
	ErrPayloadTooBig = errors.New("recordlog: payload too large")
	ErrInvalidVAddr  = errors.New("recordlog: invalid virtual address")
	ErrInvalidLogPos = errors.New("recordlog: invalid log position")
	ErrCorrupt       = errors.New("recordlog: corrupt data")
	ErrUnsupported   = errors.New("recordlog: unsupported format")
)
