package handler

import "testing"

func TestToMinorUnits_RoundsToNearestCent(t *testing.T) {
	cases := map[float64]int64{
		100.50: 10050,
		0.005:  1, // rounds up
		0.004:  0, // rounds down
		250.00: 25000,
	}
	for amount, want := range cases {
		if got := toMinorUnits(amount); got != want {
			t.Errorf("toMinorUnits(%v) = %d, want %d", amount, got, want)
		}
	}
}
