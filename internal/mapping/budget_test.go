package mapping

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

func TestDeltaBudgetBoundsAndRelease(t *testing.T) {
	budget, err := newDeltaBudget(64, 128)
	if err != nil {
		t.Fatal(err)
	}
	first, pressure, err := budget.reserve(1)
	if err != nil || !pressure {
		t.Fatalf("first reservation pressure=%v err=%v", pressure, err)
	}
	second, _, err := budget.reserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := budget.reserve(1); !errors.Is(err, ErrBudget) {
		t.Fatalf("third reservation err=%v", err)
	}
	if charged, reserved, _, hard := budget.usage(); charged != 0 || reserved != hard {
		t.Fatalf("charged=%d reserved=%d hard=%d", charged, reserved, hard)
	}
	second.Release()
	if charged, reserved, _, _ := budget.usage(); charged != 0 || reserved != 64 {
		t.Fatalf("after release charged=%d reserved=%d", charged, reserved)
	}
	if consumed, err := first.(*deltaReservation).consume(1); err != nil || consumed != 64 {
		t.Fatalf("consumed=%d err=%v", consumed, err)
	}
	first.Release()
	if charged, reserved, _, _ := budget.usage(); charged != 64 || reserved != 0 {
		t.Fatalf("after consume charged=%d reserved=%d", charged, reserved)
	}
}

func TestDeltaBudgetRejectsRequestLargerThanHardLimit(t *testing.T) {
	budget, err := newDeltaBudget(64, 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := budget.reserve(3); !errors.Is(err, base.ErrBatchTooLarge) {
		t.Fatalf("reservation err=%v", err)
	}
	if charged, reserved, _, _ := budget.usage(); charged != 0 || reserved != 0 {
		t.Fatalf("charged=%d reserved=%d", charged, reserved)
	}
}
