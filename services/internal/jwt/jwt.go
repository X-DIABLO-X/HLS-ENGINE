package jwt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

// Claims represents standard JWT claims extended with token ID.
type Claims struct {
	jwt.RegisteredClaims
	UserID   string `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	Role     string `json:"role,omitempty"`
	Type     string `json:"type,omitempty"`
}

// Manager creates and validates JWTs.
type Manager struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
}

func NewManager(secret string, accessTTL, refreshTTL time.Duration) *Manager {
	return &Manager{
		secret:     []byte(secret),
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
	}
}

func (m *Manager) GenerateAccess(userID, username, role string) (string, string, error) {
	jti := uuid.NewString()
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.accessTTL)),
		},
		UserID:   userID,
		Username: username,
		Role:     role,
		Type:     "access",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := token.SignedString(m.secret)
	return s, jti, err
}

func (m *Manager) GenerateRefresh(userID string) (string, string, error) {
	jti := uuid.NewString()
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.refreshTTL)),
		},
		UserID: userID,
		Type:   "refresh",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := token.SignedString(m.secret)
	return s, jti, err
}

func (m *Manager) Validate(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.secret, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, ErrInvalidToken
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// SignURL signs a URL path with an expiry timestamp using HMAC-SHA256.
func SignURL(secret []byte, path string, expiry time.Time) string {
	payload := fmt.Sprintf("%s:%d", path, expiry.Unix())
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return sig
}

// VerifyURL verifies a signed URL token.
func VerifyURL(secret []byte, path, token string, now time.Time) (time.Time, bool) {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return time.Time{}, false
	}
	expiryUnix, err := parseInt64(parts[0])
	if err != nil {
		return time.Time{}, false
	}
	expiry := time.Unix(expiryUnix, 0)
	if now.After(expiry) {
		return expiry, false
	}
	expected := SignURL(secret, path, expiry)
	return expiry, hmac.Equal([]byte(token), []byte(fmt.Sprintf("%d:%s", expiryUnix, expected)))
}

// BuildSignedToken creates a token string containing expiry and signature.
func BuildSignedToken(secret []byte, path string, expiry time.Time) string {
	sig := SignURL(secret, path, expiry)
	return fmt.Sprintf("%d:%s", expiry.Unix(), sig)
}

// SignURLPrefix signs a URL path prefix with an expiry timestamp.
func SignURLPrefix(secret []byte, prefix string, expiry time.Time) string {
	payload := fmt.Sprintf("prefix:%s:%d", prefix, expiry.Unix())
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return sig
}

// VerifyURLPrefix verifies a signed URL prefix token.
// The token may have been issued for an exact path or for a parent prefix.
func VerifyURLPrefix(secret []byte, path, token string, now time.Time) (time.Time, bool) {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) != 2 {
		return time.Time{}, false
	}
	expiryUnix, err := parseInt64(parts[0])
	if err != nil {
		return time.Time{}, false
	}
	expiry := time.Unix(expiryUnix, 0)
	if now.After(expiry) {
		return expiry, false
	}
	for {
		candidates := []string{path}
		if !strings.HasSuffix(path, "/") {
			candidates = append(candidates, path+"/")
		}
		for _, candidate := range candidates {
			expected := SignURLPrefix(secret, candidate, expiry)
			if hmac.Equal([]byte(token), []byte(fmt.Sprintf("%d:%s", expiryUnix, expected))) {
				return expiry, true
			}
		}
		// Strip the last path segment and retry; break when no further stripping is possible.
		path = strings.TrimSuffix(path, "/")
		idx := strings.LastIndex(path, "/")
		if idx <= 0 {
			break
		}
		path = path[:idx+1]
	}
	return expiry, false
}

// BuildSignedTokenPrefix creates a prefix-scoped token.
func BuildSignedTokenPrefix(secret []byte, prefix string, expiry time.Time) string {
	sig := SignURLPrefix(secret, prefix, expiry)
	return fmt.Sprintf("%d:%s", expiry.Unix(), sig)
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
