package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"hls-engine/metadata/internal/repository"
)

type stubVideoDeleter struct {
	err error
}

func (d stubVideoDeleter) Delete(context.Context, string) error {
	return d.err
}

func TestDeleteVideoResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		status     int
		retryAfter string
	}{
		{name: "deleted", status: http.StatusNoContent},
		{name: "missing", err: repository.ErrNotFound, status: http.StatusNotFound},
		{
			name:       "active",
			err:        repository.ErrVideoActive,
			status:     http.StatusConflict,
			retryAfter: "1",
		},
		{
			name:       "deletion already running",
			err:        repository.ErrVideoDeletionInProgress,
			status:     http.StatusConflict,
			retryAfter: "1",
		},
		{
			name:       "cleanup failure",
			err:        errVideoDeletionRetryable,
			status:     http.StatusServiceUnavailable,
			retryAfter: "1",
		},
		{name: "database failure", err: errors.New("database unavailable"), status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := &server{
				log:          zerolog.Nop(),
				videoDeleter: stubVideoDeleter{err: tt.err},
			}
			router := chi.NewRouter()
			router.Delete("/api/v1/videos/{id}", srv.deleteVideo)

			request := httptest.NewRequest(
				http.MethodDelete,
				"/api/v1/videos/"+testVideoID,
				nil,
			)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tt.status, response.Body.String())
			}
			if got := response.Header().Get("Retry-After"); got != tt.retryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tt.retryAfter)
			}
			if tt.status == http.StatusNoContent && response.Body.Len() != 0 {
				t.Fatalf("204 response body = %q, want empty", response.Body.String())
			}
		})
	}
}
