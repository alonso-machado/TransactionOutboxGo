package domain

// StaffAuthenticator is the outbound port for verifying a staff member's
// Bearer token (Clerk in production, a fixed test token in the fake
// adapter) — the only place the domain touches an auth provider. It
// answers "is this token valid" only; StaffUserRepository answers "is this
// authenticated identity a registered staff member, and for which venue".
type StaffAuthenticator interface {
	// VerifyToken authenticates token and returns the provider's own user
	// ID on success (e.g. a Clerk user ID).
	VerifyToken(token string) (clerkUserID string, err error)
}
