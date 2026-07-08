// Package clerk implements domain.StaffAuthenticator against the real
// Clerk API: VerifyToken decodes and verifies a Clerk session JWT
// (fetching the signing key from Clerk's JWKS endpoint) and returns the
// token's Clerk user ID.
package clerk

import (
	"context"
	"fmt"

	clerksdk "github.com/clerk/clerk-sdk-go/v2"
	"github.com/clerk/clerk-sdk-go/v2/jwks"
	"github.com/clerk/clerk-sdk-go/v2/jwt"
)

type Verifier struct {
	jwksClient *jwks.Client
}

// New configures a JWKS client bound to secretKey (config.ClerkSecretKey).
func New(secretKey string) *Verifier {
	cfg := &clerksdk.ClientConfig{}
	cfg.Key = clerksdk.String(secretKey)
	return &Verifier{jwksClient: jwks.NewClient(cfg)}
}

// VerifyToken decodes token (to learn which signing key was used), fetches
// that JSON Web Key from Clerk, verifies the token against it, and returns
// the verified claims' Subject (the Clerk user ID) on success.
func (v *Verifier) VerifyToken(token string) (string, error) {
	ctx := context.Background()

	unsafeClaims, err := jwt.Decode(ctx, &jwt.DecodeParams{Token: token})
	if err != nil {
		return "", fmt.Errorf("decode clerk token: %w", err)
	}

	jwk, err := jwt.GetJSONWebKey(ctx, &jwt.GetJSONWebKeyParams{
		KeyID:      unsafeClaims.KeyID,
		JWKSClient: v.jwksClient,
	})
	if err != nil {
		return "", fmt.Errorf("fetch clerk jwk: %w", err)
	}

	claims, err := jwt.Verify(ctx, &jwt.VerifyParams{Token: token, JWK: jwk})
	if err != nil {
		return "", fmt.Errorf("verify clerk token: %w", err)
	}
	return claims.Subject, nil
}
