package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultMaxHeaderBytes = 64 << 10
	statePending          = "pending"
	stateConverged        = "converged"
	stateRetrying         = "retrying"
)

type cachePurger struct {
	cacheRoot     string
	tombstoneRoot string
	secret        []byte
	maxHeaderSize int64
	batchFiles    int
	batchDuration time.Duration
	batchPause    time.Duration
	sweepInterval time.Duration
	retryDelay    time.Duration
	staleAfter    time.Duration

	mu                  sync.RWMutex
	deleted             map[string]struct{}
	records             map[string]*purgeStatus
	completedGeneration uint64
	activeGeneration    uint64
	sweepActive         bool
	filesScanned        uint64
	lastProgressAt      time.Time
	lastSuccessAt       time.Time
	lastError           string

	wake      chan struct{}
	sweepGate chan struct{}
}

type purgeStatus struct {
	VideoID             string    `json:"video_id"`
	Revoked             bool      `json:"revoked"`
	State               string    `json:"state"`
	RequiredGeneration  uint64    `json:"required_generation"`
	CompletedGeneration uint64    `json:"completed_generation"`
	FilesPurged         uint64    `json:"files_purged"`
	RequestedAt         time.Time `json:"requested_at"`
	LastSweepAt         time.Time `json:"last_sweep_at,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
}

type healthStatus struct {
	State               string    `json:"state"`
	RevokedVideos       int       `json:"revoked_videos"`
	PendingVideos       int       `json:"pending_videos"`
	SweepActive         bool      `json:"sweep_active"`
	ActiveGeneration    uint64    `json:"active_generation"`
	CompletedGeneration uint64    `json:"completed_generation"`
	FilesScanned        uint64    `json:"files_scanned"`
	LastProgressAt      time.Time `json:"last_progress_at,omitempty"`
	LastSuccessAt       time.Time `json:"last_success_at,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
}

type sweepResult struct {
	Generation   uint64
	FilesScanned uint64
	FilesPurged  uint64
}

func main() {
	purger, err := newCachePurger(
		getEnv("NGINX_CACHE_ROOT", "/var/cache/nginx/hls"),
		getEnv("NGINX_CACHE_TOMBSTONE_ROOT", "/var/cache/nginx/tombstones"),
		os.Getenv("NGINX_CACHE_PURGE_SECRET"),
		int64(getEnvInt("NGINX_CACHE_PURGE_MAX_HEADER_BYTES", defaultMaxHeaderBytes)),
		getEnvInt("NGINX_CACHE_PURGE_BATCH_FILES", 1000),
		getEnvDuration("NGINX_CACHE_PURGE_BATCH_DURATION", 100*time.Millisecond),
		getEnvDuration("NGINX_CACHE_PURGE_BATCH_PAUSE", 10*time.Millisecond),
		getEnvDuration("NGINX_CACHE_PURGE_SWEEP_INTERVAL", 30*time.Second),
		getEnvDuration("NGINX_CACHE_PURGE_RETRY_DELAY", time.Second),
		getEnvDuration("NGINX_CACHE_PURGE_STALE_AFTER", 30*time.Second),
	)
	if err != nil {
		log.Fatalf("cache purger configuration: %v", err)
	}

	processCtx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()
	go purger.runReconciler(processCtx)

	server := &http.Server{
		Addr:              getEnv("HTTP_ADDR", ":8080"),
		Handler:           purger.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("nginx cache purger listening on %s", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-processCtx.Done():
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("cache purger server failed: %v", err)
		}
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func newCachePurger(
	cacheRoot string,
	tombstoneRoot string,
	secret string,
	maxHeaderSize int64,
	batchFiles int,
	batchDuration time.Duration,
	batchPause time.Duration,
	sweepInterval time.Duration,
	retryDelay time.Duration,
	staleAfter time.Duration,
) (*cachePurger, error) {
	if len(secret) < 32 {
		return nil, errors.New("NGINX_CACHE_PURGE_SECRET must contain at least 32 characters")
	}
	if maxHeaderSize < 1024 || maxHeaderSize > 1<<20 {
		return nil, errors.New("NGINX_CACHE_PURGE_MAX_HEADER_BYTES must be between 1024 and 1048576")
	}
	if batchFiles < 1 || batchFiles > 100_000 {
		return nil, errors.New("NGINX_CACHE_PURGE_BATCH_FILES must be between 1 and 100000")
	}
	if batchDuration <= 0 || batchDuration > 5*time.Second {
		return nil, errors.New("NGINX_CACHE_PURGE_BATCH_DURATION must be between 0 and 5s")
	}
	if batchPause < 0 || batchPause > time.Second {
		return nil, errors.New("NGINX_CACHE_PURGE_BATCH_PAUSE must be between 0 and 1s")
	}
	if sweepInterval < 100*time.Millisecond || sweepInterval > 24*time.Hour {
		return nil, errors.New("NGINX_CACHE_PURGE_SWEEP_INTERVAL must be between 100ms and 24h")
	}
	if retryDelay < 100*time.Millisecond || retryDelay > time.Minute {
		return nil, errors.New("NGINX_CACHE_PURGE_RETRY_DELAY must be between 100ms and 1m")
	}
	if staleAfter < time.Second || staleAfter > time.Hour {
		return nil, errors.New("NGINX_CACHE_PURGE_STALE_AFTER must be between 1s and 1h")
	}

	cacheRoot, err := filepath.Abs(filepath.Clean(cacheRoot))
	if err != nil {
		return nil, fmt.Errorf("resolve cache root: %w", err)
	}
	tombstoneRoot, err = filepath.Abs(filepath.Clean(tombstoneRoot))
	if err != nil {
		return nil, fmt.Errorf("resolve tombstone root: %w", err)
	}
	if cacheRoot == tombstoneRoot || pathContains(cacheRoot, tombstoneRoot) ||
		pathContains(tombstoneRoot, cacheRoot) {
		return nil, errors.New("cache and tombstone roots must be separate directories")
	}
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create cache root: %w", err)
	}
	if err := os.MkdirAll(tombstoneRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create tombstone root: %w", err)
	}

	purger := &cachePurger{
		cacheRoot:     cacheRoot,
		tombstoneRoot: tombstoneRoot,
		secret:        []byte(secret),
		maxHeaderSize: maxHeaderSize,
		batchFiles:    batchFiles,
		batchDuration: batchDuration,
		batchPause:    batchPause,
		sweepInterval: sweepInterval,
		retryDelay:    retryDelay,
		staleAfter:    staleAfter,
		deleted:       make(map[string]struct{}),
		records:       make(map[string]*purgeStatus),
		wake:          make(chan struct{}, 1),
		sweepGate:     make(chan struct{}, 1),
	}
	purger.sweepGate <- struct{}{}
	if err := purger.loadTombstones(); err != nil {
		return nil, err
	}
	return purger, nil
}

func (p *cachePurger) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", p.health)
	mux.HandleFunc("GET /v1/guard", p.guard)
	mux.HandleFunc("POST /v1/purge/{videoID}", p.purge)
	mux.HandleFunc("GET /v1/purge/{videoID}", p.purgeStatus)
	return mux
}

func (p *cachePurger) health(w http.ResponseWriter, _ *http.Request) {
	if err := probeWritableDurable(p.cacheRoot); err != nil {
		http.Error(w, "cache storage is not durably writable", http.StatusServiceUnavailable)
		return
	}
	if err := probeWritableDurable(p.tombstoneRoot); err != nil {
		http.Error(w, "tombstone storage is not durably writable", http.StatusServiceUnavailable)
		return
	}

	status, degraded := p.healthSnapshot(time.Now())
	w.Header().Set("Content-Type", "application/json")
	if degraded {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(status)
}

func probeWritableDurable(root string) error {
	probe, err := os.CreateTemp(root, ".health-*")
	if err != nil {
		return err
	}
	name := probe.Name()
	defer os.Remove(name)
	if _, err := probe.Write([]byte{0}); err != nil {
		_ = probe.Close()
		return err
	}
	if err := probe.Sync(); err != nil {
		_ = probe.Close()
		return err
	}
	if err := probe.Close(); err != nil {
		return err
	}
	if err := os.Remove(name); err != nil {
		return err
	}
	return syncDirectory(root)
}

func (p *cachePurger) healthSnapshot(now time.Time) (healthStatus, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pending := 0
	for _, record := range p.records {
		if record.State != stateConverged {
			pending++
		}
	}
	status := healthStatus{
		State:               "healthy",
		RevokedVideos:       len(p.deleted),
		PendingVideos:       pending,
		SweepActive:         p.sweepActive,
		ActiveGeneration:    p.activeGeneration,
		CompletedGeneration: p.completedGeneration,
		FilesScanned:        p.filesScanned,
		LastProgressAt:      p.lastProgressAt,
		LastSuccessAt:       p.lastSuccessAt,
		LastError:           p.lastError,
	}
	degraded := p.lastError != ""
	if p.sweepActive && !p.lastProgressAt.IsZero() &&
		now.Sub(p.lastProgressAt) > p.staleAfter {
		degraded = true
		status.LastError = "cache reconciliation has stopped making progress"
	}
	if degraded {
		status.State = "degraded"
	}
	return status, degraded
}

func (p *cachePurger) guard(w http.ResponseWriter, r *http.Request) {
	videoID, err := videoIDFromHLSURI(r.Header.Get("X-HLS-URI"))
	if err != nil {
		http.Error(w, "invalid HLS path", http.StatusForbidden)
		return
	}
	revoked, err := p.lookupRevoked(videoID)
	if err != nil {
		log.Printf("revocation lookup for %s failed: %v", videoID, err)
		http.Error(w, "revocation state unavailable", http.StatusServiceUnavailable)
		return
	}
	if revoked {
		http.Error(w, "video deleted", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (p *cachePurger) purge(w http.ResponseWriter, r *http.Request) {
	if !p.authorized(r.Header.Get("Authorization")) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	videoID, err := canonicalVideoID(r.PathValue("videoID"))
	if err != nil {
		http.Error(w, "invalid video ID", http.StatusBadRequest)
		return
	}

	status, err := p.revoke(r.Context(), videoID)
	if err != nil {
		log.Printf("revoke video %s failed: %v", videoID, err)
		http.Error(w, "cache revocation incomplete; retry", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(status)
}

func (p *cachePurger) purgeStatus(w http.ResponseWriter, r *http.Request) {
	if !p.authorized(r.Header.Get("Authorization")) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	videoID, err := canonicalVideoID(r.PathValue("videoID"))
	if err != nil {
		http.Error(w, "invalid video ID", http.StatusBadRequest)
		return
	}
	revoked, err := p.lookupRevoked(videoID)
	if err != nil {
		http.Error(w, "revocation state unavailable", http.StatusServiceUnavailable)
		return
	}
	if !revoked {
		http.Error(w, "video is not revoked", http.StatusNotFound)
		return
	}
	status, ok := p.statusFor(videoID)
	if !ok {
		http.Error(w, "revocation status unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func (p *cachePurger) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	candidate := []byte(strings.TrimSpace(strings.TrimPrefix(header, prefix)))
	return len(candidate) == len(p.secret) &&
		subtle.ConstantTimeCompare(candidate, p.secret) == 1
}

func (p *cachePurger) revoke(ctx context.Context, videoID string) (purgeStatus, error) {
	if err := ctx.Err(); err != nil {
		return purgeStatus{}, err
	}
	if err := p.writeTombstone(videoID); err != nil {
		return purgeStatus{}, fmt.Errorf("write revocation tombstone: %w", err)
	}
	status := p.registerRevoked(videoID)
	p.signalReconcile()
	return status, nil
}

func (p *cachePurger) registerRevoked(videoID string) purgeStatus {
	now := time.Now().UTC()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted[videoID] = struct{}{}
	record, exists := p.records[videoID]
	if !exists {
		required := p.completedGeneration + 1
		if p.sweepActive {
			required = p.activeGeneration + 1
		}
		record = &purgeStatus{
			VideoID:            videoID,
			Revoked:            true,
			State:              statePending,
			RequiredGeneration: required,
			RequestedAt:        now,
		}
		p.records[videoID] = record
	}
	return *record
}

func (p *cachePurger) statusFor(videoID string) (purgeStatus, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	record, ok := p.records[videoID]
	if !ok {
		return purgeStatus{}, false
	}
	return *record, true
}

func (p *cachePurger) lookupRevoked(videoID string) (bool, error) {
	p.mu.RLock()
	_, revoked := p.deleted[videoID]
	p.mu.RUnlock()
	if revoked {
		return true, nil
	}

	// A shared tombstone volume keeps rolling or multi-replica guards coherent.
	// A local negative map entry is never authoritative.
	info, err := os.Lstat(filepath.Join(p.tombstoneRoot, videoID))
	if err == nil {
		if !info.Mode().IsRegular() {
			return false, errors.New("revocation tombstone is not a regular file")
		}
		p.registerRevoked(videoID)
		p.signalReconcile()
		return true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("stat revocation tombstone: %w", err)
	}

	// ENOENT for the exact file is authoritative only if the fixed parent
	// storage is itself present, accessible, and a real directory. This keeps a
	// missing/unmounted/corrupt tombstone volume fail closed.
	rootInfo, rootErr := os.Stat(p.tombstoneRoot)
	if rootErr != nil {
		return false, fmt.Errorf("verify tombstone root: %w", rootErr)
	}
	if !rootInfo.IsDir() {
		return false, errors.New("tombstone root is not a directory")
	}
	return false, nil
}

func (p *cachePurger) runReconciler(ctx context.Context) {
	p.signalReconcile()
	timer := time.NewTimer(p.sweepInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-timer.C:
		}

		_, err := p.reconcileOnce(ctx)
		delay := p.sweepInterval
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("cache reconciliation failed: %v", err)
			delay = p.retryDelay
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
	}
}

func (p *cachePurger) signalReconcile() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *cachePurger) reconcileOnce(ctx context.Context) (sweepResult, error) {
	select {
	case <-ctx.Done():
		return sweepResult{}, ctx.Err()
	case <-p.sweepGate:
	}
	defer func() { p.sweepGate <- struct{}{} }()

	if err := p.loadTombstones(); err != nil {
		p.mu.RLock()
		generation := p.completedGeneration + 1
		snapshot := make(map[string]struct{}, len(p.deleted))
		for videoID := range p.deleted {
			snapshot[videoID] = struct{}{}
		}
		p.mu.RUnlock()
		return sweepResult{}, p.finishSweepFailure(generation, snapshot, err)
	}

	p.mu.Lock()
	if len(p.deleted) == 0 {
		now := time.Now().UTC()
		p.lastError = ""
		p.lastProgressAt = now
		p.lastSuccessAt = now
		p.mu.Unlock()
		return sweepResult{}, nil
	}
	generation := p.completedGeneration + 1
	p.activeGeneration = generation
	p.sweepActive = true
	p.filesScanned = 0
	p.lastProgressAt = time.Now().UTC()
	snapshot := make(map[string]struct{}, len(p.deleted))
	for videoID := range p.deleted {
		snapshot[videoID] = struct{}{}
	}
	p.mu.Unlock()

	walker, err := newCacheWalker(p.cacheRoot)
	if err != nil {
		return sweepResult{}, p.finishSweepFailure(generation, snapshot, err)
	}
	defer walker.Close()

	result := sweepResult{Generation: generation}
	purgedByVideo := make(map[string]uint64)
	var scanErrors []error
	errorCount := 0
	done := false
	for !done {
		batchStarted := time.Now()
		batchFiles := 0
		for batchFiles < p.batchFiles && time.Since(batchStarted) < p.batchDuration {
			if err := ctx.Err(); err != nil {
				return result, p.finishSweepFailure(generation, snapshot, err)
			}
			cachePath, complete, walkErr := walker.Next(ctx)
			if walkErr != nil {
				errorCount++
				if len(scanErrors) < 20 {
					scanErrors = append(scanErrors, walkErr)
				}
				continue
			}
			if complete {
				done = true
				break
			}
			batchFiles++
			result.FilesScanned++
			videoID, match, parseErr := cacheFileVideoID(cachePath, p.maxHeaderSize)
			if parseErr != nil {
				errorCount++
				if len(scanErrors) < 20 {
					scanErrors = append(scanErrors, parseErr)
				}
				continue
			}
			if !match {
				continue
			}
			if _, revoked := snapshot[videoID]; !revoked {
				continue
			}
			if err := os.Remove(cachePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				errorCount++
				if len(scanErrors) < 20 {
					scanErrors = append(scanErrors, fmt.Errorf("remove %s: %w", cachePath, err))
				}
				continue
			}
			result.FilesPurged++
			purgedByVideo[videoID]++
		}
		p.mu.Lock()
		p.filesScanned = result.FilesScanned
		p.lastProgressAt = time.Now().UTC()
		p.mu.Unlock()
		if !done && p.batchPause > 0 {
			timer := time.NewTimer(p.batchPause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, p.finishSweepFailure(generation, snapshot, ctx.Err())
			case <-timer.C:
			}
		}
	}

	if errorCount > 0 {
		scanErrors = append(
			scanErrors,
			fmt.Errorf("%d cache entries or directories could not be reconciled", errorCount),
		)
		return result, p.finishSweepFailure(
			generation,
			snapshot,
			errors.Join(scanErrors...),
		)
	}
	p.finishSweepSuccess(generation, snapshot, purgedByVideo)
	return result, nil
}

func (p *cachePurger) finishSweepSuccess(
	generation uint64,
	snapshot map[string]struct{},
	purgedByVideo map[string]uint64,
) {
	now := time.Now().UTC()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completedGeneration = generation
	p.activeGeneration = 0
	p.sweepActive = false
	p.lastProgressAt = now
	p.lastSuccessAt = now
	p.lastError = ""
	for videoID := range snapshot {
		record := p.records[videoID]
		if record == nil {
			continue
		}
		record.CompletedGeneration = generation
		record.FilesPurged += purgedByVideo[videoID]
		record.LastSweepAt = now
		record.LastError = ""
		if record.RequiredGeneration <= generation {
			record.State = stateConverged
		}
	}
}

func (p *cachePurger) finishSweepFailure(
	generation uint64,
	snapshot map[string]struct{},
	err error,
) error {
	if err == nil {
		return nil
	}
	now := time.Now().UTC()
	p.mu.Lock()
	p.activeGeneration = 0
	p.sweepActive = false
	p.lastProgressAt = now
	p.lastError = err.Error()
	for videoID := range snapshot {
		record := p.records[videoID]
		if record == nil || record.RequiredGeneration > generation {
			continue
		}
		record.State = stateRetrying
		record.LastError = err.Error()
	}
	p.mu.Unlock()
	return err
}

func cacheFileVideoID(cachePath string, maxHeaderSize int64) (string, bool, error) {
	file, err := os.Open(cachePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("open %s: %w", cachePath, err)
	}
	defer file.Close()
	header, err := io.ReadAll(io.LimitReader(file, maxHeaderSize))
	if err != nil {
		return "", false, fmt.Errorf("read cache header %s: %w", cachePath, err)
	}
	key := nginxCacheKey(header)
	if len(key) == 0 {
		return "", false, nil
	}
	videoID, err := videoIDFromNginxCacheKey(string(key))
	if err != nil {
		return "", false, nil
	}
	return videoID, true, nil
}

func videoIDFromNginxCacheKey(key string) (string, error) {
	pathStart := strings.IndexByte(key, '/')
	if pathStart < 0 {
		return "", errors.New("cache key has no request path")
	}
	requestPath := key[pathStart:]
	if queryStart := strings.IndexByte(requestPath, '?'); queryStart >= 0 {
		requestPath = requestPath[:queryStart]
	}
	return videoIDFromHLSPath(requestPath)
}

func videoIDFromHLSPath(requestPath string) (string, error) {
	if !strings.HasPrefix(requestPath, "/hls/") {
		return "", errors.New("not an HLS cache path")
	}
	remainder := strings.TrimPrefix(requestPath, "/hls/")
	slash := strings.IndexByte(remainder, '/')
	if slash <= 0 {
		return "", errors.New("HLS cache path has no asset")
	}
	return canonicalVideoID(remainder[:slash])
}

func nginxCacheKey(header []byte) []byte {
	const keyPrefix = "KEY: "
	start := bytes.Index(header, []byte("\n"+keyPrefix))
	if start >= 0 {
		start++
	} else if bytes.HasPrefix(header, []byte(keyPrefix)) {
		start = 0
	} else {
		return nil
	}
	start += len(keyPrefix)
	end := bytes.IndexByte(header[start:], '\n')
	if end < 0 {
		return nil
	}
	key := header[start : start+end]
	return bytes.TrimSuffix(key, []byte{'\r'})
}

func (p *cachePurger) writeTombstone(videoID string) error {
	target := filepath.Join(p.tombstoneRoot, videoID)
	if info, err := os.Lstat(target); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("existing tombstone is not a regular file")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	temp, err := os.CreateTemp(p.tombstoneRoot, ".pending-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := io.WriteString(temp, videoID+"\n"); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, target); err != nil {
		if info, statErr := os.Lstat(target); statErr == nil && info.Mode().IsRegular() {
			return nil
		}
		return err
	}
	return syncDirectory(p.tombstoneRoot)
}

func (p *cachePurger) loadTombstones() error {
	entries, err := os.ReadDir(p.tombstoneRoot)
	if err != nil {
		return fmt.Errorf("read tombstones: %w", err)
	}
	for _, entry := range entries {
		videoID, err := canonicalVideoID(entry.Name())
		if err != nil || videoID != entry.Name() {
			continue
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("tombstone %s is not a regular file", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat tombstone %s: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("tombstone %s is not a regular file", entry.Name())
		}
		p.registerRevoked(videoID)
	}
	return nil
}

type cacheWalker struct {
	stack []*directoryFrame
}

type directoryFrame struct {
	path    string
	dir     *os.File
	entries []os.DirEntry
	index   int
	eof     bool
}

func newCacheWalker(root string) (*cacheWalker, error) {
	dir, err := os.Open(root)
	if err != nil {
		return nil, fmt.Errorf("open cache root: %w", err)
	}
	return &cacheWalker{
		stack: []*directoryFrame{{path: root, dir: dir}},
	}, nil
}

func (w *cacheWalker) Next(ctx context.Context) (string, bool, error) {
	for len(w.stack) > 0 {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		frame := w.stack[len(w.stack)-1]
		if frame.index >= len(frame.entries) {
			if frame.eof {
				_ = frame.dir.Close()
				w.stack = w.stack[:len(w.stack)-1]
				continue
			}
			entries, err := frame.dir.ReadDir(128)
			frame.entries = entries
			frame.index = 0
			if errors.Is(err, io.EOF) {
				frame.eof = true
			} else if err != nil {
				_ = frame.dir.Close()
				w.stack = w.stack[:len(w.stack)-1]
				return "", false, fmt.Errorf("read cache directory %s: %w", frame.path, err)
			}
			if len(entries) == 0 {
				continue
			}
		}

		entry := frame.entries[frame.index]
		frame.index++
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		fullPath := filepath.Join(frame.path, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return "", false, fmt.Errorf("stat cache entry %s: %w", fullPath, err)
		}
		if info.IsDir() {
			child, err := os.Open(fullPath)
			if err != nil {
				return "", false, fmt.Errorf("open cache directory %s: %w", fullPath, err)
			}
			w.stack = append(w.stack, &directoryFrame{path: fullPath, dir: child})
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		return fullPath, false, nil
	}
	return "", true, nil
}

func (w *cacheWalker) Close() {
	for _, frame := range w.stack {
		_ = frame.dir.Close()
	}
	w.stack = nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func videoIDFromHLSURI(rawURI string) (string, error) {
	parsed, err := url.ParseRequestURI(rawURI)
	if err != nil {
		return "", err
	}
	if parsed.IsAbs() || parsed.Host != "" || parsed.RawPath != "" ||
		path.Clean(parsed.Path) != parsed.Path ||
		strings.Contains(parsed.Path, `\`) {
		return "", errors.New("HLS path is not canonical")
	}
	parts := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "hls" || parts[2] == "" {
		return "", errors.New("path must be /hls/{videoID}/{asset}")
	}
	return canonicalVideoID(parts[1])
}

func canonicalVideoID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) != 36 || raw[8] != '-' || raw[13] != '-' ||
		raw[18] != '-' || raw[23] != '-' {
		return "", errors.New("not a canonical UUID")
	}
	for index, char := range raw {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !((char >= '0' && char <= '9') ||
			(char >= 'a' && char <= 'f') ||
			(char >= 'A' && char <= 'F')) {
			return "", errors.New("not a UUID")
		}
	}
	return strings.ToLower(raw), nil
}

func pathContains(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("%s must be an integer: %v", key, err)
	}
	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("%s must be a duration: %v", key, err)
	}
	return parsed
}
