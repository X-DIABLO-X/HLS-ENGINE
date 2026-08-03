package hls

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProtectedResourcesRejectMissingToken(t *testing.T) {
	svc := New(nil, nil, []byte("test-secret"), "http://localhost")
	ctx := context.Background()
	expiry := time.Now().Add(time.Hour)

	if _, _, err := svc.RewriteMasterManifest(ctx, "video-id", "", expiry); err == nil {
		t.Fatal("master manifest accepted a missing token")
	}
	if _, _, err := svc.RewriteVariantManifest(ctx, "video-id", "video_720p/video.m3u8", "", expiry); err == nil {
		t.Fatal("variant manifest accepted a missing token")
	}

	req := httptest.NewRequest("GET", "/hls/video-id/video_720p/00001.m4s", nil)
	rec := httptest.NewRecorder()
	if err := svc.ProxySegment(ctx, rec, req, "video-id", "video_720p/00001.m4s", ""); err == nil {
		t.Fatal("segment accepted a missing token")
	}
}

func TestValidateTokenRejectsInvalidToken(t *testing.T) {
	svc := New(nil, nil, []byte("test-secret"), "http://localhost")
	if svc.ValidateToken("/hls/video-id/", "not-a-token") {
		t.Fatal("invalid token was accepted")
	}
}

func TestValidateTokenAcceptsExactToken(t *testing.T) {
	svc := New(nil, nil, []byte("test-secret"), "http://localhost")
	token := svc.GenerateToken("/hls/video-id/", time.Now().Add(time.Hour))

	if !svc.ValidateToken("/hls/video-id/", token) {
		t.Fatal("exact-path token was rejected")
	}

	prefixToken := strings.TrimSpace(token)
	if prefixToken == "" {
		t.Fatal("expected non-empty token")
	}
}

func TestValidateTokenExpiryPreservesInboundLimit(t *testing.T) {
	svc := New(nil, nil, []byte("test-secret"), "http://localhost")
	path := "/hls/video-id/"
	wantExpiry := time.Now().Add(10 * time.Minute).Truncate(time.Second)
	token := svc.GenerateToken(path, wantExpiry)

	gotExpiry, ok := svc.validateTokenExpiry(path, token)
	if !ok {
		t.Fatal("valid token was rejected")
	}
	if !gotExpiry.Equal(wantExpiry) {
		t.Fatalf("validated expiry = %s, want %s", gotExpiry, wantExpiry)
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		size      int64
		wantStart int64
		wantEnd   int64
		wantOK    bool
	}{
		{name: "bounded", value: "bytes=0-1023", size: 4096, wantStart: 0, wantEnd: 1023, wantOK: true},
		{name: "open ended", value: "bytes=1024-", size: 4096, wantStart: 1024, wantEnd: 4095, wantOK: true},
		{name: "suffix", value: "bytes=-500", size: 4096, wantStart: 3596, wantEnd: 4095, wantOK: true},
		{name: "clamped", value: "bytes=4000-9999", size: 4096, wantStart: 4000, wantEnd: 4095, wantOK: true},
		{name: "start past end", value: "bytes=4096-", size: 4096, wantOK: false},
		{name: "reversed", value: "bytes=10-9", size: 4096, wantOK: false},
		{name: "multiple unsupported", value: "bytes=0-1,4-5", size: 4096, wantOK: false},
		{name: "wrong unit", value: "items=0-1", size: 4096, wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start, end, ok := parseRange(test.value, test.size)
			if ok != test.wantOK {
				t.Fatalf("parseRange(%q, %d) ok=%v, want %v", test.value, test.size, ok, test.wantOK)
			}
			if ok && (start != test.wantStart || end != test.wantEnd) {
				t.Fatalf(
					"parseRange(%q, %d)=(%d,%d), want (%d,%d)",
					test.value,
					test.size,
					start,
					end,
					test.wantStart,
					test.wantEnd,
				)
			}
		})
	}
}
