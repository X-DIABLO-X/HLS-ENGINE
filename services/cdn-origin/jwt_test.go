package main

import (
	"testing"
	"time"

	"hls-engine/internal/jwt"
)

func TestPrefixTokenCoversVideoResources(t *testing.T) {
	secret := []byte("test-secret")
	prefix := "/hls/4c106c1b-0859-4b16-a053-210f52264ead/"
	expiry := time.Now().Add(time.Hour)
	token := jwt.BuildSignedTokenPrefix(secret, prefix, expiry)

	paths := []string{
		prefix,
		"/hls/4c106c1b-0859-4b16-a053-210f52264ead/video_720p/video.m3u8",
		"/hls/4c106c1b-0859-4b16-a053-210f52264ead/video_720p/00000.m4s",
		"/hls/4c106c1b-0859-4b16-a053-210f52264ead/audio_hin/audio.m3u8",
	}

	for _, p := range paths {
		_, ok := jwt.VerifyURLPrefix(secret, p, token, time.Now())
		if !ok {
			t.Fatalf("expected prefix token to authorize %q", p)
		}
	}
}
