package failpoint

import (
	"errors"
	"testing"
)

func TestNilAndFuncHook(t *testing.T) {
	if err := Hit(nil, "ignored"); err != nil {
		t.Fatal(err)
	}
	want := errors.New("stop")
	var got Point
	err := Hit(Func(func(point Point) error { got = point; return want }), "boundary")
	if !errors.Is(err, want) || got != "boundary" {
		t.Fatalf("point=%q error=%v", got, err)
	}
}
