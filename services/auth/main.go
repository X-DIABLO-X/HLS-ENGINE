package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"hls-engine/auth/internal/repository"
	"hls-engine/internal/config"
	"hls-engine/internal/db"
	"hls-engine/internal/jwt"
	"hls-engine/internal/logger"
	"hls-engine/internal/middleware"
)

type server struct {
	cfg  config.Config
	log  zerolog.Logger
	pool *pgxpool.Pool
	repo *repository.Repository
	jm   *jwt.Manager
}

func main() {
	cfg := config.Load()
	cfg.ServiceName = "auth"
	log := logger.WithService("auth")

	pool, err := db.NewPool(cfg.PostgresDSN, cfg.PostgresMaxOpen, cfg.PostgresMaxIdle)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connection failed")
	}
	defer pool.Close()

	srv := &server{
		cfg:  cfg,
		log:  log,
		pool: pool,
		repo: repository.New(pool),
		jm:   jwt.NewManager(cfg.JWTSecret, cfg.JWTAccessTTL, cfg.JWTRefreshTTL),
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger(log))
	r.Use(middleware.Recoverer)
	r.Use(middleware.CORS)

	r.Get("/health", srv.health)
	r.Get("/ready", srv.ready)

	r.Post("/api/v1/auth/register", srv.register)
	r.Post("/api/v1/auth/login", srv.login)
	r.Post("/api/v1/auth/refresh", srv.refresh)
	r.Post("/api/v1/auth/logout", srv.logout)
	r.Post("/api/v1/auth/apikeys", srv.createAPIKey)
	r.Get("/api/v1/auth/apikeys/validate", srv.validateAPIKey)

	port := cfg.HTTPPort
	if port == "" {
		port = "8080"
	}
	httpSrv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info().Str("addr", httpSrv.Addr).Msg("auth service starting")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("auth listen failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	log.Info().Msg("auth service stopped")
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "reason": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type registerReq struct {
	Email    string `json:"email"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *server) register(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Email == "" || req.Password == "" {
		respondError(w, http.StatusBadRequest, "email and password required")
		return
	}
	user, err := s.repo.CreateUser(r.Context(), req.Email, req.Username, req.Password)
	if err != nil {
		s.log.Error().Err(err).Msg("register failed")
		respondError(w, http.StatusConflict, "user already exists")
		return
	}
	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"id":       user.ID,
		"email":    user.Email,
		"username": user.Username,
		"role":     user.Role,
	})
}

type loginReq struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *server) login(w http.ResponseWriter, r *http.Request) {
	var req loginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	user, err := s.repo.VerifyPassword(r.Context(), req.Email, req.Password)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) || errors.Is(err, repository.ErrInvalidCreds) {
			respondError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		s.log.Error().Err(err).Msg("login failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	access, _, err := s.jm.GenerateAccess(user.ID, user.Username, user.Role)
	if err != nil {
		s.log.Error().Err(err).Msg("generate access token failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	refresh, refreshJTI, err := s.jm.GenerateRefresh(user.ID)
	if err != nil {
		s.log.Error().Err(err).Msg("generate refresh token failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := s.repo.StoreRefreshToken(r.Context(), user.ID, refreshJTI, time.Now().Add(s.cfg.JWTRefreshTTL)); err != nil {
		s.log.Error().Err(err).Msg("store refresh token failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"access_token":  access,
		"refresh_token": refresh,
		"token_type":    "Bearer",
		"expires_in":    int(s.cfg.JWTAccessTTL.Seconds()),
		"user": map[string]interface{}{
			"id":       user.ID,
			"email":    user.Email,
			"username": user.Username,
			"role":     user.Role,
		},
	})
}

type refreshReq struct {
	RefreshToken string `json:"refresh_token"`
}

func (s *server) refresh(w http.ResponseWriter, r *http.Request) {
	var req refreshReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	claims, err := s.jm.Validate(req.RefreshToken)
	if err != nil {
		respondError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if claims.Type != "refresh" {
		respondError(w, http.StatusUnauthorized, "invalid token type")
		return
	}
	stored, err := s.repo.GetRefreshToken(r.Context(), claims.ID)
	if err != nil {
		respondError(w, http.StatusUnauthorized, "token revoked")
		return
	}
	if time.Now().After(stored.ExpiresAt) {
		respondError(w, http.StatusUnauthorized, "token expired")
		return
	}
	user, err := s.repo.GetUserByID(r.Context(), claims.Subject)
	if err != nil {
		respondError(w, http.StatusUnauthorized, "user not found")
		return
	}
	access, _, err := s.jm.GenerateAccess(user.ID, user.Username, user.Role)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"access_token": access,
		"token_type":   "Bearer",
		"expires_in":   int(s.cfg.JWTAccessTTL.Seconds()),
	})
}

type logoutReq struct {
	RefreshToken string `json:"refresh_token"`
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	var req logoutReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	claims, err := s.jm.Validate(req.RefreshToken)
	if err != nil {
		respondJSON(w, http.StatusOK, map[string]string{"status": "logged out"})
		return
	}
	_ = s.repo.DeleteRefreshToken(r.Context(), claims.ID)
	respondJSON(w, http.StatusOK, map[string]string{"status": "logged out"})
}

type createAPIKeyReq struct {
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (s *server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	// In a real service this endpoint would be authenticated.
	// We accept X-User-ID header for demonstration.
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		userID = "demo-user"
	}
	var req createAPIKeyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	key, raw, err := s.repo.CreateAPIKey(r.Context(), userID, req.Name, req.Scopes, req.ExpiresAt)
	if err != nil {
		s.log.Error().Err(err).Msg("create api key failed")
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"id":         key.ID,
		"name":       key.Name,
		"key":        raw,
		"scopes":     key.Scopes,
		"expires_at": key.ExpiresAt,
	})
}

func (s *server) validateAPIKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		respondError(w, http.StatusBadRequest, "key required")
		return
	}
	k, err := s.repo.GetAPIKeyByHash(r.Context(), key)
	if err != nil {
		respondError(w, http.StatusUnauthorized, "invalid key")
		return
	}
	if k.ExpiresAt != nil && time.Now().After(*k.ExpiresAt) {
		respondError(w, http.StatusUnauthorized, "key expired")
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"valid":   true,
		"user_id": k.UserID,
		"scopes":  k.Scopes,
	})
}

func respondJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, map[string]string{"error": msg})
}
