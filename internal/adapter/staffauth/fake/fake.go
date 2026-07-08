// Package fake implements domain.StaffAuthenticator with no network calls
// — the default staff-auth provider for local dev (make up) and the
// integration test suite, neither of which have a real Clerk account to
// verify a session token against. Accepts exactly one fixed,
// config-driven test token (config.StaffAuthFakeToken) and maps it to one
// fixed Clerk user ID.
package fake

import "errors"

var ErrInvalidToken = errors.New("fake staffauth: invalid token")

type Verifier struct {
	fakeToken       string
	fakeClerkUserID string
}

// New builds a Verifier that only accepts fakeToken, mapping it to
// fakeClerkUserID — pair this with a staff_users row seeded with the same
// Clerk user ID (see docker-compose.yml/tests/integration's seed data).
func New(fakeToken, fakeClerkUserID string) *Verifier {
	return &Verifier{fakeToken: fakeToken, fakeClerkUserID: fakeClerkUserID}
}

func (v *Verifier) VerifyToken(token string) (string, error) {
	if token == "" || token != v.fakeToken {
		return "", ErrInvalidToken
	}
	return v.fakeClerkUserID, nil
}
