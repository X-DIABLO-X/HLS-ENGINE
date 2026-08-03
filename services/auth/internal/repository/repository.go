package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrUserExists    = errors.New("user already exists")
	ErrUserNotFound  = errors.New("user not found")
	ErrInvalidCreds  = errors.New("invalid credentials")
	ErrTokenNotFound = errors.New("token not found")
)

// User represents an account.
type User struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Username  string    `json:"username"`
	Password  string    `json:"-"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RefreshToken stores refresh token metadata.
type RefreshToken struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	TokenHash string    `json:"-"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// APIKey stores API keys.
type APIKey struct {
	ID        string     `json:"id"`
	UserID    string     `json:"user_id"`
	KeyHash   string     `json:"-"`
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

// Repository abstracts auth persistence.
type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateUser(ctx context.Context, email, username, password string) (*User, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	user := &User{
		ID:       uuid.NewString(),
		Email:    email,
		Username: username,
		Password: string(hash),
		Role:     "user",
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO users (id, email, username, password_hash, role, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
	`, user.ID, user.Email, user.Username, user.Password, user.Role)
	if err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}
	return user, nil
}

func (r *Repository) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, email, username, password_hash, role, created_at, updated_at
		FROM users WHERE email = $1
	`, email)
	u := &User{}
	err := row.Scan(&u.ID, &u.Email, &u.Username, &u.Password, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return u, nil
}

func (r *Repository) GetUserByID(ctx context.Context, id string) (*User, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, email, username, password_hash, role, created_at, updated_at
		FROM users WHERE id = $1
	`, id)
	u := &User{}
	err := row.Scan(&u.ID, &u.Email, &u.Username, &u.Password, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return u, nil
}

func (r *Repository) VerifyPassword(ctx context.Context, email, password string) (*User, error) {
	u, err := r.GetUserByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(password)); err != nil {
		return nil, ErrInvalidCreds
	}
	return u, nil
}

func (r *Repository) StoreRefreshToken(ctx context.Context, userID, tokenID string, expiresAt time.Time) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO refresh_tokens (id, user_id, expires_at, created_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (id) DO UPDATE SET expires_at = EXCLUDED.expires_at
	`, tokenID, userID, expiresAt)
	return err
}

func (r *Repository) GetRefreshToken(ctx context.Context, tokenID string) (*RefreshToken, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, user_id, expires_at, created_at FROM refresh_tokens WHERE id = $1
	`, tokenID)
	t := &RefreshToken{}
	err := row.Scan(&t.ID, &t.UserID, &t.ExpiresAt, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTokenNotFound
		}
		return nil, err
	}
	return t, nil
}

func (r *Repository) DeleteRefreshToken(ctx context.Context, tokenID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM refresh_tokens WHERE id = $1`, tokenID)
	return err
}

func (r *Repository) CreateAPIKey(ctx context.Context, userID, name string, scopes []string, expiresAt *time.Time) (*APIKey, string, error) {
	raw := "hls_" + uuid.NewString() + uuid.NewString()
	hash, err := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
	if err != nil {
		return nil, "", err
	}
	key := &APIKey{
		ID:        uuid.NewString(),
		UserID:    userID,
		KeyHash:   string(hash),
		Name:      name,
		Scopes:    scopes,
		ExpiresAt: expiresAt,
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO api_keys (id, user_id, key_hash, name, scopes, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`, key.ID, key.UserID, key.KeyHash, key.Name, key.Scopes, key.ExpiresAt)
	if err != nil {
		return nil, "", err
	}
	return key, raw, nil
}

func (r *Repository) GetAPIKeyByHash(ctx context.Context, raw string) (*APIKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, key_hash, name, scopes, expires_at, created_at FROM api_keys
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		k := &APIKey{}
		if err := rows.Scan(&k.ID, &k.UserID, &k.KeyHash, &k.Name, &k.Scopes, &k.ExpiresAt, &k.CreatedAt); err != nil {
			continue
		}
		if bcrypt.CompareHashAndPassword([]byte(k.KeyHash), []byte(raw)) == nil {
			return k, nil
		}
	}
	return nil, ErrTokenNotFound
}
