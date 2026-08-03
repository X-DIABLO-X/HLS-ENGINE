package redis

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func TestIsMultipartSessionRecordKey(t *testing.T) {
	t.Parallel()

	const sessionID = "89e3242a-ba04-49d8-86f5-794a6ee0fcb4"
	tests := []struct {
		key  string
		want bool
	}{
		{key: "upload:multipart:" + sessionID, want: true},
		{key: "upload:multipart:" + sessionID + ":completion-lock"},
		{key: "upload:multipart:video:bcf9bbb0-02e6-440f-a665-dcdfa5a74c51"},
		{key: "upload:multipart:not-a-uuid"},
		{key: "other:" + sessionID},
	}
	for _, test := range tests {
		test := test
		t.Run(test.key, func(t *testing.T) {
			t.Parallel()
			if got := isMultipartSessionRecordKey(test.key); got != test.want {
				t.Fatalf("isMultipartSessionRecordKey(%q) = %t, want %t", test.key, got, test.want)
			}
		})
	}
}

func TestMultipartSessionVideoID(t *testing.T) {
	t.Parallel()

	const videoID = "bcf9bbb0-02e6-440f-a665-dcdfa5a74c51"
	got, err := multipartSessionVideoID(`{"video_id":"` + videoID + `","status":"event_published"}`)
	if err != nil {
		t.Fatalf("multipartSessionVideoID() error = %v", err)
	}
	if got != videoID {
		t.Fatalf("multipartSessionVideoID() = %q, want %q", got, videoID)
	}

	for _, value := range []string{
		`{`,
		`{"status":"event_published"}`,
		`{"video_id":"not-a-uuid"}`,
		`{"video_id":"BCF9BBB0-02E6-440F-A665-DCDF5A74C51"}`,
	} {
		if _, err := multipartSessionVideoID(value); err == nil {
			t.Fatalf("multipartSessionVideoID(%q) unexpectedly succeeded", value)
		}
	}
}

func TestDeleteMultipartSessionsForVideoIntegration(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := New(addr, os.Getenv("REDIS_TEST_PASSWORD"), 0)
	t.Cleanup(func() {
		_ = client.Close()
	})
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	targetVideoID := uuid.NewString()
	otherVideoID := uuid.NewString()
	targetSessionID := uuid.NewString()
	otherSessionID := uuid.NewString()
	targetKey := multipartSessionPrefix + targetSessionID
	targetLockKey := targetKey + ":completion-lock"
	otherKey := multipartSessionPrefix + otherSessionID
	otherLockKey := otherKey + ":completion-lock"
	allKeys := []string{targetKey, targetLockKey, otherKey, otherLockKey}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()
		_ = client.Delete(cleanupCtx, allKeys...)
	})

	saveRecord := func(key, sessionID, videoID string) {
		t.Helper()
		value, err := json.Marshal(map[string]string{
			"session_id": sessionID,
			"video_id":   videoID,
			"status":     "event_published",
		})
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		if err := client.Set(ctx, key, string(value), time.Minute); err != nil {
			t.Fatalf("Set(%q) error = %v", key, err)
		}
		if err := client.Set(ctx, key+":completion-lock", "lock", time.Minute); err != nil {
			t.Fatalf("Set(%q) error = %v", key+":completion-lock", err)
		}
	}
	saveRecord(targetKey, targetSessionID, targetVideoID)
	saveRecord(otherKey, otherSessionID, otherVideoID)

	if err := client.DeleteMultipartSessionsForVideo(ctx, targetVideoID); err != nil {
		t.Fatalf("DeleteMultipartSessionsForVideo() error = %v", err)
	}
	for _, key := range []string{targetKey, targetLockKey} {
		if _, err := client.Get(ctx, key); !errors.Is(err, goredis.Nil) {
			t.Fatalf("Get(%q) error = %v, want redis.Nil", key, err)
		}
	}
	for _, key := range []string{otherKey, otherLockKey} {
		if _, err := client.Get(ctx, key); err != nil {
			t.Fatalf("Get(%q) error = %v, want preserved key", key, err)
		}
	}
}
