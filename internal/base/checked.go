package base

import "math"

func AddUint32(a, b uint32) (uint32, error) {
	if b > math.MaxUint32-a {
		return 0, ErrOverflow
	}
	return a + b, nil
}

func AddUint64(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, ErrOverflow
	}
	return a + b, nil
}

func MulUint64(a, b uint64) (uint64, error) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, ErrOverflow
	}
	return a * b, nil
}

func Align8(v uint64) (uint64, error) {
	withPadding, err := AddUint64(v, 7)
	if err != nil {
		return 0, err
	}
	return withPadding &^ 7, nil
}

func Uint64ToInt(v uint64) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if v > maxInt {
		return 0, ErrOverflow
	}
	return int(v), nil
}
