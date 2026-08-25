package radix

import "errors"

var (
	ErrInvalid = errors.New("radix: invalid input")
	ErrCorrupt = errors.New("radix: corrupt tree")
)
