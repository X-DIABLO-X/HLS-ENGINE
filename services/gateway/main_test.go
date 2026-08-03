package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"hls-engine/internal/jwt"
)

func TestAuthMiddlewareAcceptsAccessToken(t *testing.T) {
	t.Parallel()

	jm := jwt.NewManager("test-secret", time.Minute, time.Hour)
	token, _, err := jm.GenerateAccess("user-id", "user", "user")
	if err != nil {
		t.Fatalf("GenerateAccess() error = %v", err)
	}

	called := false
	log := zerolog.Nop()
	handler := authMiddleware(jm, "X-API-Key", &log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/videos", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if !called {
		t.Fatal("next handler was not called")
	}
}

func TestAuthMiddlewareRejectsRefreshToken(t *testing.T) {
	t.Parallel()

	jm := jwt.NewManager("test-secret", time.Minute, time.Hour)
	token, _, err := jm.GenerateRefresh("user-id")
	if err != nil {
		t.Fatalf("GenerateRefresh() error = %v", err)
	}

	called := false
	log := zerolog.Nop()
	handler := authMiddleware(jm, "X-API-Key", &log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/videos", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	if called {
		t.Fatal("next handler was called for a refresh token")
	}
}
