// Package pii masks the personally-identifiable fields that can appear in
// a payment payload — boleto.payerDocument (CPF/CNPJ), boleto.barcode,
// pix.endToEndId/txid, and cardNumber (PAN) — before they reach a log line,
// error response, or trace span. It has zero framework dependencies so
// usecase and adapter code can both call it without breaking the Clean
// Architecture dependency rule.
package pii

import (
	"encoding/json"
	"regexp"
	"strings"
)

const mask = "***"

// sensitiveKeys are matched case-insensitively against JSON object keys.
var sensitiveKeys = map[string]struct{}{
	"payerdocument": {},
	"barcode":       {},
	"endtoendid":    {},
	"txid":          {},
	"cardnumber":    {},
}

// keyValuePattern catches the same field names when they show up in plain
// text (e.g. inside a Go error string) rather than structured JSON, in the
// form `key: value`, `key=value`, or `"key":"value"`.
var keyValuePattern = regexp.MustCompile(`(?i)\b(payerDocument|barcode|endToEndId|txid|cardNumber)("?\s*[:=]\s*"?)([^",}\s]+)`)

// Redact masks known PII fields wherever they appear in s. If s is a JSON
// object/array, matching keys are masked structurally; otherwise (or for
// any text outside the JSON structure) a key=value text pattern is masked
// as a fallback. Safe to call on arbitrary strings, including error
// messages and free-form log text.
func Redact(s string) string {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		redactValue(v)
		if out, err := json.Marshal(v); err == nil {
			return string(out)
		}
	}
	return keyValuePattern.ReplaceAllString(s, "$1$2"+mask)
}

// RedactJSON masks known PII fields in a raw JSON document, returning the
// input unchanged if it cannot be parsed as JSON.
func RedactJSON(raw []byte) []byte {
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	redactValue(v)
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

func redactValue(v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			if _, sensitive := sensitiveKeys[strings.ToLower(k)]; sensitive {
				t[k] = mask
				continue
			}
			redactValue(val)
		}
	case []interface{}:
		for _, item := range t {
			redactValue(item)
		}
	}
}
