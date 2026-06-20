package pii

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRedact_PlainTextKeyValue_Masked(t *testing.T) {
	got := Redact(`payerDocument: 12345678900`)
	if strings.Contains(got, "12345678900") {
		t.Fatalf("expected payerDocument value to be masked, got %q", got)
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
	in := `{"payerDocument":"12345678900","barcode":"000111","amount":100,"endToEndId":"E1","txid":"T1","currency":"BRL"}`
	got := Redact(in)

	var out map[string]interface{}
	if err := json.Unmarshal([]byte(got), &out); err != nil {
		t.Fatalf("expected valid JSON output, got error %v (output: %s)", err, got)
	}
	for _, k := range []string{"payerDocument", "barcode", "endToEndId", "txid"} {
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
	in := []byte(`{"boleto":{"barcode":"123","dueDate":"2026-01-01"},"items":[{"payerDocument":"999"},{"other":"x"}]}`)
	out := RedactJSON(in)

	var v map[string]interface{}
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("expected valid JSON, got error %v", err)
	}
	boleto := v["boleto"].(map[string]interface{})
	if boleto["barcode"] != mask {
		t.Errorf("expected nested barcode masked, got %v", boleto["barcode"])
	}
	if boleto["dueDate"] != "2026-01-01" {
		t.Errorf("expected dueDate untouched, got %v", boleto["dueDate"])
	}
	items := v["items"].([]interface{})
	first := items[0].(map[string]interface{})
	if first["payerDocument"] != mask {
		t.Errorf("expected payerDocument inside array element masked, got %v", first["payerDocument"])
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

func TestRedact_CardNumber_MaskedInJSONAndText(t *testing.T) {
	inJSON := `{"cardNumber":"4111111111111111","cardType":"CREDIT"}`
	gotJSON := Redact(inJSON)
	if strings.Contains(gotJSON, "4111111111111111") {
		t.Fatalf("expected cardNumber value to be masked in JSON, got %q", gotJSON)
	}

	inText := "cardNumber: 4111111111111111"
	gotText := Redact(inText)
	if strings.Contains(gotText, "4111111111111111") {
		t.Fatalf("expected cardNumber value to be masked in text, got %q", gotText)
	}
}

func TestRedactJSON_KeyMatchIsCaseInsensitive(t *testing.T) {
	in := []byte(`{"PAYERDOCUMENT":"12345678900"}`)
	out := RedactJSON(in)
	var v map[string]interface{}
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v["PAYERDOCUMENT"] != mask {
		t.Errorf("expected case-insensitive key match to mask value, got %v", v["PAYERDOCUMENT"])
	}
}
