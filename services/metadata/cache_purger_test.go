package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testCachePurgeSecret = "0123456789abcdef0123456789abcdef"
const testSiblingVideoID = "9b79a3d3-e701-4bbc-89f1-859ab13e4806"

func TestHTTPVideoCachePurgerUsesAuthenticatedExactCanonicalPath(t *testing.T) {
	var method, path, authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		path = r.URL.Path
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(
			`{"video_id":"` + testVideoID +
				`","revoked":true,"state":"pending","required_generation":7}`,
		))
	}))
	defer server.Close()

	purger, err := newHTTPVideoCachePurger(
		server.URL,
		testCachePurgeSecret,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := purger.Purge(context.Background(), strings.ToUpper(testVideoID)); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost {
		t.Fatalf("method = %s, want POST", method)
	}
	if path != "/v1/purge/"+testVideoID {
		t.Fatalf("path = %q, want exact canonical video path", path)
	}
	if authorization != "Bearer "+testCachePurgeSecret {
		t.Fatal("cache purger request is not authenticated")
	}
}

func TestHTTPVideoCachePurgerFailsClosedOnErrorOrRedirect(t *testing.T) {
	for _, status := range []int{
		http.StatusOK,
		http.StatusTemporaryRedirect,
		http.StatusUnauthorized,
		http.StatusServiceUnavailable,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("purge incomplete"))
		}))
		purger, err := newHTTPVideoCachePurger(
			server.URL,
			testCachePurgeSecret,
			time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}
		err = purger.Purge(context.Background(), testVideoID)
		server.Close()
		if err == nil {
			t.Fatalf("status %d unexpectedly succeeded", status)
		}
	}
}

func TestHTTPVideoCachePurgerValidatesAcknowledgementIdentityAndDurability(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"video_id":"` + testSiblingVideoID + `","revoked":true,"state":"pending","required_generation":1}`,
		`{"video_id":"` + testVideoID + `","revoked":false,"state":"pending","required_generation":1}`,
		`{"video_id":"` + testVideoID + `","revoked":true,"state":"pending","required_generation":0}`,
		`{"video_id":"` + testVideoID + `","revoked":true,"state":"unknown","required_generation":1}`,
		`not-json`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(body))
		}))
		purger, err := newHTTPVideoCachePurger(
			server.URL,
			testCachePurgeSecret,
			time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}
		err = purger.Purge(context.Background(), testVideoID)
		server.Close()
		if err == nil {
			t.Fatalf("invalid acknowledgement %q unexpectedly succeeded", body)
		}
	}
}

func TestHTTPVideoCachePurgerRejectsUnsafeConfigurationAndID(t *testing.T) {
	for _, test := range []struct {
		url    string
		secret string
	}{
		{url: "file:///cache", secret: testCachePurgeSecret},
		{url: "http://user:pass@purger", secret: testCachePurgeSecret},
		{url: "http://purger?unsafe=1", secret: testCachePurgeSecret},
		{url: "http://purger", secret: "short"},
	} {
		if _, err := newHTTPVideoCachePurger(test.url, test.secret, time.Second); err == nil {
			t.Fatalf("configuration URL=%q unexpectedly succeeded", test.url)
		}
	}
	if _, err := newHTTPVideoCachePurger(
		"http://purger",
		testCachePurgeSecret,
		31*time.Second,
	); err == nil {
		t.Fatal("timeout beyond metadata's write-timeout budget unexpectedly succeeded")
	}

	purger, err := newHTTPVideoCachePurger(
		"http://purger",
		testCachePurgeSecret,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := purger.Purge(context.Background(), "../sibling"); err == nil {
		t.Fatal("unsafe video ID unexpectedly reached cache purger")
	}
}
