package database

import "testing"

func TestWithSSLMode(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		sslMode string
		want    string
	}{
		{"empty mode leaves dsn untouched", "postgres://h/db", "", "postgres://h/db"},
		{"appends with ? when no query", "postgres://h/db", "require", "postgres://h/db?sslmode=require"},
		{"appends with & when query exists", "postgres://h/db?x=1", "require", "postgres://h/db?x=1&sslmode=require"},
		{"respects caller-set sslmode", "postgres://h/db?sslmode=disable", "require", "postgres://h/db?sslmode=disable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withSSLMode(tc.dsn, tc.sslMode); got != tc.want {
				t.Fatalf("withSSLMode(%q, %q) = %q, want %q", tc.dsn, tc.sslMode, got, tc.want)
			}
		})
	}
}
