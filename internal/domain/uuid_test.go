package domain

import "testing"

func TestParseOptionalUUID_NilOrEmpty_ReturnsNil(t *testing.T) {
	if id, err := ParseOptionalUUID(nil); err != nil || id != nil {
		t.Fatalf("expected nil, nil for nil input, got %v, %v", id, err)
	}
	empty := ""
	if id, err := ParseOptionalUUID(&empty); err != nil || id != nil {
		t.Fatalf("expected nil, nil for empty string, got %v, %v", id, err)
	}
}

func TestParseOptionalUUID_Valid_ReturnsParsedUUID(t *testing.T) {
	valid := "018f7f9e-6e8b-7c3a-8f2a-000000000001"
	id, err := ParseOptionalUUID(&valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == nil || id.String() != valid {
		t.Fatalf("expected parsed uuid %s, got %v", valid, id)
	}
}

func TestParseOptionalUUID_Invalid_ReturnsError(t *testing.T) {
	invalid := "not-a-uuid"
	if _, err := ParseOptionalUUID(&invalid); err == nil {
		t.Fatal("expected error for invalid uuid")
	}
}
