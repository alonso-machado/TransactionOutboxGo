package handler

import "testing"

func TestParseOptionalUUID_NilOrEmpty_ReturnsNil(t *testing.T) {
	if id, err := parseOptionalUUID(nil); err != nil || id != nil {
		t.Fatalf("expected nil, nil for nil input, got %v, %v", id, err)
	}
	empty := ""
	if id, err := parseOptionalUUID(&empty); err != nil || id != nil {
		t.Fatalf("expected nil, nil for empty string, got %v, %v", id, err)
	}
}

func TestParseOptionalUUID_Valid_ReturnsParsedUUID(t *testing.T) {
	valid := "018f7f9e-6e8b-7c3a-8f2a-000000000001"
	id, err := parseOptionalUUID(&valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == nil || id.String() != valid {
		t.Fatalf("expected parsed uuid %s, got %v", valid, id)
	}
}

func TestParseOptionalUUID_Invalid_ReturnsError(t *testing.T) {
	invalid := "not-a-uuid"
	if _, err := parseOptionalUUID(&invalid); err == nil {
		t.Fatal("expected error for invalid uuid")
	}
}

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
