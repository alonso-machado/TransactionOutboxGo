package pii

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRedact_PlainTextKeyValue_Masked(t *testing.T) {
	got := Redact(`document: 12345678900`)
	if strings.Contains(got, "12345678900") {
		t.Fatalf("expected document value to be masked, got %q", got)
	}
	if !strings.Contains(got, mask) {
		t.Fatalf("expected mask in output, got %q", got)
	}
}

func TestRedact_NonJSONNonMatchingText_Unchanged(t *testing.T) {
	in := "outbox fetch error: context canceled"
	if got := Redact(in); got != in {
		t.Fatalf("expected unrelated text unchanged, got %q", got)
	}
}

func TestRedact_JSONObject_MasksSensitiveKeysOnly(t *testing.T) {
	in := `{"email":"buyer@example.com","document":"12345678900","amount":100,"validationCode":"vc1","signature":"sig1","currency":"BRL"}`
	got := Redact(in)

	var out map[string]interface{}
	if err := json.Unmarshal([]byte(got), &out); err != nil {
		t.Fatalf("expected valid JSON output, got error %v (output: %s)", err, got)
	}
	for _, k := range []string{"email", "document", "validationCode", "signature"} {
		if out[k] != mask {
			t.Errorf("expected %s to be masked, got %v", k, out[k])
		}
	}
	if out["amount"] != float64(100) {
		t.Errorf("expected non-sensitive field amount to be untouched, got %v", out["amount"])
	}
	if out["currency"] != "BRL" {
		t.Errorf("expected non-sensitive field currency to be untouched, got %v", out["currency"])
	}
}

func TestRedactJSON_NestedObjectsAndArrays_Masked(t *testing.T) {
	in := []byte(`{"customer":{"email":"buyer@example.com","name":"Jane"},"items":[{"document":"999"},{"other":"x"}]}`)
	out := RedactJSON(in)

	var v map[string]interface{}
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("expected valid JSON, got error %v", err)
	}
	customer := v["customer"].(map[string]interface{})
	if customer["email"] != mask {
		t.Errorf("expected nested email masked, got %v", customer["email"])
	}
	if customer["name"] != "Jane" {
		t.Errorf("expected name untouched, got %v", customer["name"])
	}
	items := v["items"].([]interface{})
	first := items[0].(map[string]interface{})
	if first["document"] != mask {
		t.Errorf("expected document inside array element masked, got %v", first["document"])
	}
	second := items[1].(map[string]interface{})
	if second["other"] != "x" {
		t.Errorf("expected unrelated array element field untouched, got %v", second["other"])
	}
}

func TestRedactJSON_InvalidJSON_ReturnedUnchanged(t *testing.T) {
	in := []byte("not json at all")
	out := RedactJSON(in)
	if string(out) != string(in) {
		t.Fatalf("expected invalid JSON to be returned unchanged, got %q", out)
	}
}

func TestRedact_Signature_MaskedInJSONAndText(t *testing.T) {
	inJSON := `{"signature":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","status":"VALID"}`
	gotJSON := Redact(inJSON)
	if strings.Contains(gotJSON, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") {
		t.Fatalf("expected signature value to be masked in JSON, got %q", gotJSON)
	}

	inText := "signature: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	gotText := Redact(inText)
	if strings.Contains(gotText, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") {
		t.Fatalf("expected signature value to be masked in text, got %q", gotText)
	}
}

func TestRedactJSON_KeyMatchIsCaseInsensitive(t *testing.T) {
	in := []byte(`{"DOCUMENT":"12345678900"}`)
	out := RedactJSON(in)
	var v map[string]interface{}
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v["DOCUMENT"] != mask {
		t.Errorf("expected case-insensitive key match to mask value, got %v", v["DOCUMENT"])
	}
}
