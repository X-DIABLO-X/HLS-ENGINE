package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testSecret  = "0123456789abcdef0123456789abcdef"
	testVideoID = "4c106c1b-0859-4b16-a053-210f52264ead"
	siblingID   = "9b79a3d3-e701-4bbc-89f1-859ab13e4806"
)

func newTestPurger(t *testing.T) *cachePurger {
	t.Helper()
	root := t.TempDir()
	return newTestPurgerAt(
		t,
		filepath.Join(root, "hls"),
		filepath.Join(root, "tombstones"),
	)
}

func newTestPurgerAt(t *testing.T, cacheRoot, tombstoneRoot string) *cachePurger {
	t.Helper()
	purger, err := newCachePurger(
		cacheRoot,
		tombstoneRoot,
		testSecret,
		64<<10,
		2,
		10*time.Millisecond,
		0,
		100*time.Millisecond,
		100*time.Millisecond,
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	return purger
}

func writeCacheFile(t *testing.T, root, name, requestURI string) string {
	t.Helper()
	cachePath := filepath.Join(root, name[:1], name[1:3], name)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
		t.Fatal(err)
	}
	content := []byte("\x00nginx-cache-header\nKEY: httpGETedge.example" + requestURI + "\nHTTP/1.1 200 OK\r\n\r\nmedia")
	if err := os.WriteFile(cachePath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return cachePath
}

func revokeAndSweep(t *testing.T, purger *cachePurger, videoID string) sweepResult {
	t.Helper()
	status, err := purger.revoke(context.Background(), videoID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Revoked || status.State != statePending {
		t.Fatalf("initial status = %#v", status)
	}
	result, err := purger.reconcileOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not converge before timeout")
}

func TestReconcileIsExactAndPreservesAdversarialSiblingKeys(t *testing.T) {
	purger := newTestPurger(t)
	targetA := writeCacheFile(t, purger.cacheRoot, "aaa", "/hls/"+testVideoID+"/master.m3u8?token=one")
	targetB := writeCacheFile(t, purger.cacheRoot, "bbb", "/hls/"+testVideoID+"/video/00001.m4s?token=two")
	sibling := writeCacheFile(t, purger.cacheRoot, "ccc", "/hls/"+siblingID+"/master.m3u8?token=one")
	nestedTarget := writeCacheFile(
		t,
		purger.cacheRoot,
		"ddd",
		"/hls/"+siblingID+"/redirect/hls/"+testVideoID+"/asset.m4s",
	)
	queryTarget := writeCacheFile(
		t,
		purger.cacheRoot,
		"eee",
		"/hls/"+siblingID+"/segment.m4s?next=/hls/"+testVideoID+"/asset.m4s",
	)

	result := revokeAndSweep(t, purger, testVideoID)
	if result.FilesPurged != 2 {
		t.Fatalf("files purged = %d, want 2", result.FilesPurged)
	}
	for _, cachePath := range []string{targetA, targetB} {
		if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
			t.Fatalf("target cache %s still exists: %v", cachePath, err)
		}
	}
	for _, cachePath := range []string{sibling, nestedTarget, queryTarget} {
		if _, err := os.Stat(cachePath); err != nil {
			t.Fatalf("sibling cache %s was touched: %v", cachePath, err)
		}
	}
	status, ok := purger.statusFor(testVideoID)
	if !ok || status.State != stateConverged ||
		status.CompletedGeneration < status.RequiredGeneration ||
		status.FilesPurged != 2 {
		t.Fatalf("convergence status = %#v", status)
	}
}

func TestPurgeEndpointDurablyRevokesAndReturnsPendingStatus(t *testing.T) {
	purger := newTestPurger(t)
	server := httptest.NewServer(purger.routes())
	defer server.Close()

	request, _ := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/purge/"+strings.ToUpper(testVideoID),
		nil,
	)
	request.Header.Set("Authorization", "Bearer "+testSecret)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
	if strings.Contains(string(body), testSecret) {
		t.Fatal("purge response leaked the shared secret")
	}
	revoked, lookupErr := purger.lookupRevoked(testVideoID)
	if lookupErr != nil || !revoked {
		t.Fatal("202 response was sent before durable revocation")
	}
	status, ok := purger.statusFor(testVideoID)
	if !ok || !status.Revoked || status.State != statePending ||
		status.RequiredGeneration == 0 {
		t.Fatalf("pending status = %#v", status)
	}
}

func TestPurgeEndpointRejectsMalformedIDsAndBadAuth(t *testing.T) {
	purger := newTestPurger(t)
	server := httptest.NewServer(purger.routes())
	defer server.Close()

	for _, test := range []struct {
		id     string
		secret string
		status int
	}{
		{id: "../escape", secret: testSecret, status: http.StatusNotFound},
		{id: "not-a-uuid", secret: testSecret, status: http.StatusBadRequest},
		{id: testVideoID, secret: "wrong", status: http.StatusUnauthorized},
	} {
		request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/purge/"+test.id, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+test.secret)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != test.status {
			t.Fatalf("purge %q status = %d, want %d", test.id, response.StatusCode, test.status)
		}
	}
	if entries, _ := os.ReadDir(purger.tombstoneRoot); len(entries) != 0 {
		t.Fatalf("malformed request wrote tombstones: %v", entries)
	}
}

func TestRetryAndRestartAreIdempotentAndCrossReplicaGuardIsCoherent(t *testing.T) {
	purger := newTestPurger(t)
	otherReplica := newTestPurgerAt(t, purger.cacheRoot, purger.tombstoneRoot)
	writeCacheFile(t, purger.cacheRoot, "aaa", "/hls/"+testVideoID+"/master.m3u8?token=old")
	revokeAndSweep(t, purger, testVideoID)

	status, err := purger.revoke(context.Background(), testVideoID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != stateConverged {
		t.Fatalf("idempotent retry status = %#v", status)
	}
	revoked, err := otherReplica.lookupRevoked(testVideoID)
	if err != nil || !revoked {
		t.Fatal("replica did not observe the shared durable tombstone")
	}

	restarted := newTestPurgerAt(t, purger.cacheRoot, purger.tombstoneRoot)
	request := httptest.NewRequest(http.MethodGet, "/v1/guard", nil)
	request.Header.Set("X-HLS-URI", "/hls/"+testVideoID+"/master.m3u8?token=old")
	response := httptest.NewRecorder()
	restarted.guard(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("guard after restart status = %d, want 403", response.Code)
	}
	status, ok := restarted.statusFor(testVideoID)
	if !ok || status.State != statePending {
		t.Fatalf("restart must enqueue a complete inventory pass: %#v", status)
	}
}

func TestPeriodicReconcilerReclaimsLateFillAfterInitialConvergence(t *testing.T) {
	purger := newTestPurger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go purger.runReconciler(ctx)

	if _, err := purger.revoke(context.Background(), testVideoID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		status, ok := purger.statusFor(testVideoID)
		return ok && status.State == stateConverged
	})

	late := writeCacheFile(
		t,
		purger.cacheRoot,
		"late",
		"/hls/"+testVideoID+"/video/late.m4s?token=old",
	)
	waitFor(t, 2*time.Second, func() bool {
		_, err := os.Stat(late)
		return os.IsNotExist(err)
	})
	revoked, err := purger.lookupRevoked(testVideoID)
	if err != nil || !revoked {
		t.Fatal("late-fill convergence reopened playback")
	}
	waitFor(t, time.Second, func() bool {
		status, ok := purger.statusFor(testVideoID)
		return ok && status.FilesPurged >= 1
	})
}

func TestConcurrentOpenReadCanFinishButEveryNewReadIsRevoked(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows cannot unlink an open file; the Linux container test covers nginx's POSIX read semantics")
	}
	purger := newTestPurger(t)
	cachePath := writeCacheFile(t, purger.cacheRoot, "aaa", "/hls/"+testVideoID+"/video/00001.m4s?token=old")
	openReader, err := os.Open(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	defer openReader.Close()

	var wait sync.WaitGroup
	wait.Add(1)
	var readErr error
	go func() {
		defer wait.Done()
		_, readErr = io.ReadAll(openReader)
	}()

	revokeAndSweep(t, purger, testVideoID)
	wait.Wait()
	if readErr != nil {
		t.Fatalf("already-open response could not finish: %v", readErr)
	}
	if _, err := os.Open(cachePath); !os.IsNotExist(err) {
		t.Fatalf("new cache read was possible after purge: %v", err)
	}
}

func TestBoundedBatchesCompleteInventoryBeyondOldHardLimit(t *testing.T) {
	purger := newTestPurger(t)
	for index := 0; index < 105; index++ {
		name := fmt.Sprintf("%03x", index)
		writeCacheFile(
			t,
			purger.cacheRoot,
			name,
			"/hls/"+siblingID+"/video/"+name+".m4s",
		)
	}
	target := writeCacheFile(
		t,
		purger.cacheRoot,
		"zzz",
		"/hls/"+testVideoID+"/video/last.m4s",
	)
	result := revokeAndSweep(t, purger, testVideoID)
	if result.FilesScanned != 106 || result.FilesPurged != 1 {
		t.Fatalf("bounded sweep result = %#v", result)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target beyond prior limit remains: %v", err)
	}
}

func TestSweepGateAcquisitionHonorsContextCancellation(t *testing.T) {
	purger := newTestPurger(t)
	<-purger.sweepGate
	defer func() { purger.sweepGate <- struct{}{} }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := purger.reconcileOnce(ctx); !errorsIsContext(err) {
		t.Fatalf("reconcile error = %v, want context deadline", err)
	}
	if time.Since(started) > 200*time.Millisecond {
		t.Fatal("cancelled sweep waited on a global uninterruptible lock")
	}
}

func errorsIsContext(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func TestGuardAllowsSiblingAndRejectsMalformedHLSPaths(t *testing.T) {
	purger := newTestPurger(t)
	if _, err := purger.revoke(context.Background(), testVideoID); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		uri    string
		status int
	}{
		{uri: "/hls/" + siblingID + "/master.m3u8?token=ok", status: http.StatusNoContent},
		{uri: "/hls/" + testVideoID + "/master.m3u8?token=old", status: http.StatusForbidden},
		{uri: "/hls/../" + testVideoID + "/master.m3u8", status: http.StatusForbidden},
		{uri: "/hls/" + siblingID + "/../" + testVideoID + "/master.m3u8", status: http.StatusForbidden},
		{uri: "/hls//" + siblingID + "/master.m3u8", status: http.StatusForbidden},
		{uri: "/hls/%39b79a3d3-e701-4bbc-89f1-859ab13e4806/master.m3u8", status: http.StatusForbidden},
		{uri: "/hls/not-a-uuid/master.m3u8", status: http.StatusForbidden},
		{uri: "/api/" + testVideoID, status: http.StatusForbidden},
	} {
		request := httptest.NewRequest(http.MethodGet, "/v1/guard", nil)
		request.Header.Set("X-HLS-URI", test.uri)
		response := httptest.NewRecorder()
		purger.guard(response, request)
		if response.Code != test.status {
			t.Fatalf("guard %q status = %d, want %d", test.uri, response.Code, test.status)
		}
	}
}

func TestGuardFailsClosedWhenTombstoneStorageIsMissingOrCorrupt(t *testing.T) {
	purger := newTestPurger(t)
	request := httptest.NewRequest(http.MethodGet, "/v1/guard", nil)
	request.Header.Set("X-HLS-URI", "/hls/"+siblingID+"/master.m3u8?token=old")

	corrupt := filepath.Join(purger.tombstoneRoot, siblingID)
	if err := os.Mkdir(corrupt, 0o700); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	purger.guard(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt tombstone guard status = %d, want 503", response.Code)
	}
	if err := os.Remove(corrupt); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(purger.tombstoneRoot); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	purger.guard(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing tombstone root guard status = %d, want 503", response.Code)
	}
	if err := os.Mkdir(purger.tombstoneRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		if err := os.Chmod(purger.tombstoneRoot, 0o000); err != nil {
			t.Fatal(err)
		}
		response = httptest.NewRecorder()
		purger.guard(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("inaccessible tombstone root guard status = %d, want 503", response.Code)
		}
		if err := os.Chmod(purger.tombstoneRoot, 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHealthWritesSyncsAndReportsReconcilerErrors(t *testing.T) {
	purger := newTestPurger(t)
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()
	purger.health(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("healthy status = %d: %s", response.Code, response.Body.String())
	}

	purger.mu.Lock()
	purger.lastError = "disk scan failed"
	purger.mu.Unlock()
	response = httptest.NewRecorder()
	purger.health(response, request)
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), "disk scan failed") {
		t.Fatalf("degraded status = %d: %s", response.Code, response.Body.String())
	}

	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		purger.mu.Lock()
		purger.lastError = ""
		purger.mu.Unlock()
		if err := os.Chmod(purger.tombstoneRoot, 0o500); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(purger.tombstoneRoot, 0o700)
		response = httptest.NewRecorder()
		purger.health(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("read-only tombstone health status = %d, want 503", response.Code)
		}
	}
}

func TestCacheKeyParserUsesOnlyFirstRequestPathVideoID(t *testing.T) {
	for _, test := range []struct {
		key  string
		want string
	}{
		{key: "httpGETedge/hls/" + testVideoID + "/a?token=x", want: testVideoID},
		{
			key:  "httpGETedge/hls/" + siblingID + "/redirect/hls/" + testVideoID + "/a",
			want: siblingID,
		},
		{
			key:  "httpGETedge/hls/" + siblingID + "/a?next=/hls/" + testVideoID + "/a",
			want: siblingID,
		},
		{key: "httpGETedge/no-hls/" + testVideoID, want: ""},
	} {
		got, err := videoIDFromNginxCacheKey(test.key)
		if test.want == "" {
			if err == nil {
				t.Fatalf("videoIDFromNginxCacheKey(%q) = %q, want error", test.key, got)
			}
			continue
		}
		if err != nil || got != test.want {
			t.Fatalf("videoIDFromNginxCacheKey(%q) = %q, %v; want %q", test.key, got, err, test.want)
		}
	}
}

func TestCacheKeyHeaderParserDoesNotMatchBodyOrPartialKey(t *testing.T) {
	for _, test := range []struct {
		content string
		want    string
	}{
		{content: "\x00\nKEY: httpGET/hls/" + testVideoID + "/a\nHTTP/1.1 200\n", want: "httpGET/hls/" + testVideoID + "/a"},
		{content: "KEY: httpGET/hls/" + testVideoID + "/a\r\n", want: "httpGET/hls/" + testVideoID + "/a"},
		{content: "body mentions /hls/" + testVideoID + "/ only", want: ""},
		{content: "\nKEY: no-newline", want: ""},
	} {
		if got := string(nginxCacheKey([]byte(test.content))); got != test.want {
			t.Fatalf("nginxCacheKey(%q) = %q, want %q", test.content, got, test.want)
		}
	}
}

func TestConfigurationRejectsUnsafeOrUnboundedValues(t *testing.T) {
	root := t.TempDir()
	valid := func(cacheRoot, tombstones, secret string, batchFiles int) (*cachePurger, error) {
		return newCachePurger(
			cacheRoot,
			tombstones,
			secret,
			64<<10,
			batchFiles,
			time.Millisecond,
			0,
			time.Second,
			time.Second,
			time.Second,
		)
	}
	for _, test := range []struct {
		name       string
		cacheRoot  string
		tombstones string
		secret     string
		batchFiles int
	}{
		{name: "weak secret", cacheRoot: filepath.Join(root, "a"), tombstones: filepath.Join(root, "b"), secret: "short", batchFiles: 1},
		{name: "same root", cacheRoot: filepath.Join(root, "a"), tombstones: filepath.Join(root, "a"), secret: testSecret, batchFiles: 1},
		{name: "nested roots", cacheRoot: filepath.Join(root, "a"), tombstones: filepath.Join(root, "a", "t"), secret: testSecret, batchFiles: 1},
		{name: "zero batch", cacheRoot: filepath.Join(root, "a"), tombstones: filepath.Join(root, "b"), secret: testSecret, batchFiles: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := valid(test.cacheRoot, test.tombstones, test.secret, test.batchFiles); err == nil {
				t.Fatal("invalid configuration unexpectedly succeeded")
			}
		})
	}
}

func Example_cacheKey() {
	fmt.Println(string(nginxCacheKey([]byte("\x00\nKEY: httpGETedge/hls/id/file?token=x\nHTTP/1.1 200\n"))))
	// Output: httpGETedge/hls/id/file?token=x
}
