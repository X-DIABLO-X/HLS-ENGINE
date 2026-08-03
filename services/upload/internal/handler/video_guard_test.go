package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"hls-engine/internal/videolease"
)

const guardTestVideoID = "4c106c1b-0859-4b16-a053-210f52264ead"

type fakeVideoLeaseManager struct {
	mu     sync.Mutex
	locked bool
	status string
}

func (m *fakeVideoLeaseManager) TryAcquire(
	_ context.Context,
	_ string,
) (VideoLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locked {
		return nil, videolease.ErrBusy
	}
	m.locked = true
	return &fakeVideoLease{manager: m}, nil
}

type fakeVideoLease struct {
	manager *fakeVideoLeaseManager
}

func (l *fakeVideoLease) VideoStatus(context.Context) (string, error) {
	l.manager.mu.Lock()
	defer l.manager.mu.Unlock()
	if l.manager.status == "missing" {
		return "", videolease.ErrVideoNotFound
	}
	return l.manager.status, nil
}

func (l *fakeVideoLease) Release(context.Context) error {
	l.manager.mu.Lock()
	defer l.manager.mu.Unlock()
	l.manager.locked = false
	return nil
}

func TestUploadAndDeleteShareExclusiveVideoFence(t *testing.T) {
	t.Parallel()

	manager := &fakeVideoLeaseManager{status: "uploading"}
	h := &Handler{
		log:         zerolog.Nop(),
		videoLeases: manager,
	}

	// The upload completion owns the fence through its storage commit and event.
	uploadLease, canonicalID, err := h.acquireVideoWrite(
		context.Background(),
		guardTestVideoID,
	)
	if err != nil {
		t.Fatalf("upload acquire returned %v", err)
	}
	if canonicalID != guardTestVideoID {
		t.Fatalf("canonical ID = %q", canonicalID)
	}

	// A concurrent DELETE uses the same manager in production and must receive
	// a retryable conflict instead of cleaning underneath the upload.
	if _, err := manager.TryAcquire(
		context.Background(),
		guardTestVideoID,
	); !errors.Is(err, videolease.ErrBusy) {
		t.Fatalf("concurrent deletion acquire = %v, want busy", err)
	}
	releaseVideoLease(uploadLease)

	// Once DELETE has committed its tombstone and released after cleanup, a
	// stale multipart completion is fenced before it can recreate raw media.
	manager.mu.Lock()
	manager.status = "deleting"
	manager.mu.Unlock()
	if _, _, err := h.acquireVideoWrite(
		context.Background(),
		guardTestVideoID,
	); !errors.Is(err, errVideoDeleting) {
		t.Fatalf("post-tombstone upload acquire = %v, want deleting", err)
	}
}

func TestUploadFenceRejectsMissingAndInvalidVideo(t *testing.T) {
	t.Parallel()

	manager := &fakeVideoLeaseManager{status: "missing"}
	h := &Handler{videoLeases: manager}
	if _, _, err := h.acquireVideoWrite(
		context.Background(),
		guardTestVideoID,
	); !errors.Is(err, videolease.ErrVideoNotFound) {
		t.Fatalf("missing video error = %v", err)
	}
	if _, _, err := h.acquireVideoWrite(
		context.Background(),
		"../unsafe",
	); !errors.Is(err, videolease.ErrVideoNotFound) {
		t.Fatalf("invalid video error = %v", err)
	}
}

func TestPresignedUploadFailsClosed(t *testing.T) {
	t.Parallel()

	h := &Handler{log: zerolog.Nop()}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/videos/"+guardTestVideoID+"/upload/presign",
		nil,
	)
	response := httptest.NewRecorder()
	h.PresignUpload(response, request)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", response.Code)
	}
}
