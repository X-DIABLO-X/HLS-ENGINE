package main

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"hls-engine/metadata/internal/repository"
)

const testVideoID = "4c106c1b-0859-4b16-a053-210f52264ead"

type recordingPrefixDeleter struct {
	mu       sync.Mutex
	prefixes []string
	err      error
	started  chan struct{}
	unblock  chan struct{}
}

func (d *recordingPrefixDeleter) DeletePrefix(
	ctx context.Context,
	prefix string,
) error {
	d.mu.Lock()
	d.prefixes = append(d.prefixes, prefix)
	started := d.started
	unblock := d.unblock
	err := d.err
	d.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if unblock != nil {
		select {
		case <-unblock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (d *recordingPrefixDeleter) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.prefixes...)
}

type recordingRedisDeleter struct {
	mu                  sync.Mutex
	keys                []string
	multipartVideoIDs   []string
	err                 error
	multipartSessionErr error
}

type recordingCachePurger struct {
	mu      sync.Mutex
	videoID []string
	err     error
}

func (p *recordingCachePurger) Purge(
	_ context.Context,
	videoID string,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.videoID = append(p.videoID, videoID)
	return p.err
}

func (p *recordingCachePurger) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.videoID...)
}

func (d *recordingRedisDeleter) Delete(
	_ context.Context,
	keys ...string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.keys = append(d.keys, keys...)
	return d.err
}

func (d *recordingRedisDeleter) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.keys...)
}

func (d *recordingRedisDeleter) DeleteMultipartSessionsForVideo(
	_ context.Context,
	videoID string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.multipartVideoIDs = append(d.multipartVideoIDs, videoID)
	return d.multipartSessionErr
}

func (d *recordingRedisDeleter) multipartCalls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.multipartVideoIDs...)
}

type fakeDeletionRepository struct {
	mu            sync.Mutex
	state         string
	active        bool
	inFlight      bool
	beginErr      error
	finalizeErr   error
	releaseErr    error
	beginCalls    int
	finalizeCalls int
}

func (r *fakeDeletionRepository) BeginVideoDeletion(
	_ context.Context,
	_ string,
) (repository.VideoDeletion, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.beginCalls++
	if r.beginErr != nil {
		return nil, r.beginErr
	}
	if r.inFlight {
		return nil, repository.ErrVideoDeletionInProgress
	}
	if r.state == "missing" {
		return nil, repository.ErrNotFound
	}
	if r.active {
		return nil, repository.ErrVideoActive
	}
	r.inFlight = true
	r.state = "deleting"
	return &fakeRepositoryDeletion{repo: r}, nil
}

func (r *fakeDeletionRepository) snapshot() (string, int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.beginCalls, r.finalizeCalls, r.inFlight
}

type fakeRepositoryDeletion struct {
	repo *fakeDeletionRepository
}

func (d *fakeRepositoryDeletion) Finalize(context.Context) error {
	d.repo.mu.Lock()
	defer d.repo.mu.Unlock()
	d.repo.finalizeCalls++
	if d.repo.finalizeErr != nil {
		return d.repo.finalizeErr
	}
	if d.repo.active {
		return repository.ErrVideoActive
	}
	if d.repo.state != "deleting" {
		return repository.ErrVideoNotDeleting
	}
	d.repo.state = "missing"
	return nil
}

func (d *fakeRepositoryDeletion) Release(context.Context) error {
	d.repo.mu.Lock()
	defer d.repo.mu.Unlock()
	d.repo.inFlight = false
	return d.repo.releaseErr
}

func newDeletionService(
	repo *fakeDeletionRepository,
) (*videoDeletionService, *recordingPrefixDeleter, *recordingPrefixDeleter, *recordingPrefixDeleter, *recordingRedisDeleter) {
	raw := &recordingPrefixDeleter{}
	hls := &recordingPrefixDeleter{}
	thumbnails := &recordingPrefixDeleter{}
	redis := &recordingRedisDeleter{}
	return &videoDeletionService{
		repo:       repo,
		cache:      &recordingCachePurger{},
		raw:        raw,
		hls:        hls,
		thumbnails: thumbnails,
		redis:      redis,
	}, raw, hls, thumbnails, redis
}

func TestStoragePrefixesForVideoAreExactAndSharded(t *testing.T) {
	t.Parallel()

	prefixes, err := storagePrefixesForVideo(testVideoID)
	if err != nil {
		t.Fatalf("storagePrefixesForVideo returned %v", err)
	}
	want := videoStoragePrefixes{
		Raw:        "raw/" + testVideoID + "/",
		HLS:        "4c/" + testVideoID + "/",
		Thumbnails: "4c/" + testVideoID + "/",
	}
	if prefixes != want {
		t.Fatalf("prefixes = %#v, want %#v", prefixes, want)
	}
}

func TestStoragePrefixesForVideoRejectsUnsafeID(t *testing.T) {
	t.Parallel()

	for _, id := range []string{"", "../other-video", "not-a-uuid"} {
		if _, err := storagePrefixesForVideo(id); err == nil {
			t.Fatalf("storagePrefixesForVideo(%q) unexpectedly succeeded", id)
		}
	}
}

func TestDeleteCommitsTombstoneBeforeConcurrentResourceCleanup(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "ready"}
	service, raw, hls, thumbnails, redis := newDeletionService(repo)
	if err := service.Delete(context.Background(), testVideoID); err != nil {
		t.Fatalf("Delete returned %v", err)
	}

	state, _, finalizes, inFlight := repo.snapshot()
	if state != "missing" || finalizes != 1 || inFlight {
		t.Fatalf(
			"repository state=%q finalizes=%d inFlight=%t",
			state,
			finalizes,
			inFlight,
		)
	}
	if !reflect.DeepEqual(raw.calls(), []string{"raw/" + testVideoID + "/"}) {
		t.Fatalf("raw prefixes = %#v", raw.calls())
	}
	if got := service.cache.(*recordingCachePurger).calls(); !reflect.DeepEqual(got, []string{testVideoID}) {
		t.Fatalf("cache purges = %#v, want exact video ID", got)
	}
	if !reflect.DeepEqual(hls.calls(), []string{"4c/" + testVideoID + "/"}) {
		t.Fatalf("HLS prefixes = %#v", hls.calls())
	}
	if !reflect.DeepEqual(thumbnails.calls(), []string{"4c/" + testVideoID + "/"}) {
		t.Fatalf("thumbnail prefixes = %#v", thumbnails.calls())
	}
	wantKeys := []string{
		"video:" + testVideoID + ":progress",
		"video:" + testVideoID + ":version",
		"video:" + testVideoID + ":prefix",
	}
	if !reflect.DeepEqual(redis.calls(), wantKeys) {
		t.Fatalf("Redis keys = %#v, want %#v", redis.calls(), wantKeys)
	}
	if got := redis.multipartCalls(); !reflect.DeepEqual(got, []string{testVideoID}) {
		t.Fatalf("multipart cleanup video IDs = %#v, want exact video ID", got)
	}
}

func TestTombstoneCommitFailureRunsNoCleanupAndIsRetryable(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{
		state:    "ready",
		beginErr: errors.New("commit failed"),
	}
	service, raw, hls, thumbnails, redis := newDeletionService(repo)

	err := service.Delete(context.Background(), testVideoID)
	if !errors.Is(err, errVideoDeletionRetryable) {
		t.Fatalf("Delete error = %v, want retryable", err)
	}
	state, _, finalizes, _ := repo.snapshot()
	if state != "ready" || finalizes != 0 {
		t.Fatalf("state=%q finalizes=%d, want ready/0", state, finalizes)
	}
	if len(raw.calls())+len(hls.calls())+len(thumbnails.calls())+
		len(redis.calls())+len(redis.multipartCalls()) != 0 {
		t.Fatal("external cleanup ran before the deletion tombstone committed")
	}
}

func TestPartialBucketFailureLeavesDeletingAndRetryResumes(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "ready"}
	service, raw, hls, thumbnails, redis := newDeletionService(repo)
	hls.err = errors.New("HLS MinIO unavailable")

	err := service.Delete(context.Background(), testVideoID)
	if !errors.Is(err, errVideoResourceCleanup) ||
		!errors.Is(err, errVideoDeletionRetryable) {
		t.Fatalf("Delete error = %v, want retryable resource cleanup", err)
	}
	state, begins, finalizes, inFlight := repo.snapshot()
	if state != "deleting" || begins != 1 || finalizes != 0 || inFlight {
		t.Fatalf(
			"after failure state=%q begins=%d finalizes=%d inFlight=%t",
			state,
			begins,
			finalizes,
			inFlight,
		)
	}
	// Independent cleanup targets still run, minimizing retry work.
	if len(raw.calls()) != 1 || len(hls.calls()) != 1 ||
		len(thumbnails.calls()) != 1 || len(redis.calls()) == 0 ||
		len(redis.multipartCalls()) != 1 {
		t.Fatal("all independent cleanup targets must be attempted")
	}

	hls.err = nil
	if err := service.Delete(context.Background(), testVideoID); err != nil {
		t.Fatalf("retry Delete returned %v", err)
	}
	state, begins, finalizes, _ = repo.snapshot()
	if state != "missing" || begins != 2 || finalizes != 1 {
		t.Fatalf(
			"after retry state=%q begins=%d finalizes=%d",
			state,
			begins,
			finalizes,
		)
	}
}

func TestCachePurgeFailureLeavesDeletingAndOriginIntactUntilRetry(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "ready"}
	service, raw, hls, thumbnails, redis := newDeletionService(repo)
	cache := service.cache.(*recordingCachePurger)
	cache.err = errors.New("cache purger unavailable")

	err := service.Delete(context.Background(), testVideoID)
	if !errors.Is(err, errVideoResourceCleanup) ||
		!errors.Is(err, errVideoDeletionRetryable) {
		t.Fatalf("Delete error = %v, want retryable resource cleanup", err)
	}
	state, begins, finalizes, inFlight := repo.snapshot()
	if state != "deleting" || begins != 1 || finalizes != 0 || inFlight {
		t.Fatalf(
			"after purge failure state=%q begins=%d finalizes=%d inFlight=%t",
			state,
			begins,
			finalizes,
			inFlight,
		)
	}
	if len(raw.calls())+len(hls.calls())+len(thumbnails.calls())+
		len(redis.calls())+len(redis.multipartCalls()) != 0 {
		t.Fatal("origin resources were deleted before cache revocation succeeded")
	}

	cache.err = nil
	if err := service.Delete(context.Background(), testVideoID); err != nil {
		t.Fatalf("retry Delete returned %v", err)
	}
	state, begins, finalizes, _ = repo.snapshot()
	if state != "missing" || begins != 2 || finalizes != 1 {
		t.Fatalf(
			"after retry state=%q begins=%d finalizes=%d",
			state,
			begins,
			finalizes,
		)
	}
	if len(cache.calls()) != 2 {
		t.Fatalf("idempotent purge calls = %d, want 2", len(cache.calls()))
	}
	if len(raw.calls()) != 1 || len(hls.calls()) != 1 ||
		len(thumbnails.calls()) != 1 || len(redis.calls()) == 0 ||
		len(redis.multipartCalls()) != 1 {
		t.Fatal("origin cleanup did not run exactly after revocation recovered")
	}
}

func TestMultipartSessionCleanupFailureLeavesDeletingAndRetryResumes(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "ready"}
	service, _, _, _, redis := newDeletionService(repo)
	redis.multipartSessionErr = errors.New("Redis scan unavailable")

	err := service.Delete(context.Background(), testVideoID)
	if !errors.Is(err, errVideoResourceCleanup) ||
		!errors.Is(err, errVideoDeletionRetryable) {
		t.Fatalf("Delete error = %v, want retryable resource cleanup", err)
	}
	state, begins, finalizes, inFlight := repo.snapshot()
	if state != "deleting" || begins != 1 || finalizes != 0 || inFlight {
		t.Fatalf(
			"after failure state=%q begins=%d finalizes=%d inFlight=%t",
			state,
			begins,
			finalizes,
			inFlight,
		)
	}
	if got := redis.multipartCalls(); !reflect.DeepEqual(got, []string{testVideoID}) {
		t.Fatalf("multipart cleanup calls = %#v, want one exact video ID", got)
	}

	redis.mu.Lock()
	redis.multipartSessionErr = nil
	redis.mu.Unlock()
	if err := service.Delete(context.Background(), testVideoID); err != nil {
		t.Fatalf("retry Delete returned %v", err)
	}
	state, begins, finalizes, _ = repo.snapshot()
	if state != "missing" || begins != 2 || finalizes != 1 {
		t.Fatalf(
			"after retry state=%q begins=%d finalizes=%d",
			state,
			begins,
			finalizes,
		)
	}
	if got := redis.multipartCalls(); !reflect.DeepEqual(
		got,
		[]string{testVideoID, testVideoID},
	) {
		t.Fatalf("multipart retry calls = %#v, want two exact video IDs", got)
	}
}

func TestFinalCommitFailureLeavesDeletingAndRetryResumes(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{
		state:       "ready",
		finalizeErr: errors.New("final commit failed"),
	}
	service, raw, _, _, _ := newDeletionService(repo)

	err := service.Delete(context.Background(), testVideoID)
	if !errors.Is(err, errVideoDeletionRetryable) {
		t.Fatalf("Delete error = %v, want retryable", err)
	}
	state, begins, finalizes, inFlight := repo.snapshot()
	if state != "deleting" || begins != 1 || finalizes != 1 || inFlight {
		t.Fatalf(
			"after failure state=%q begins=%d finalizes=%d inFlight=%t",
			state,
			begins,
			finalizes,
			inFlight,
		)
	}

	repo.mu.Lock()
	repo.finalizeErr = nil
	repo.mu.Unlock()
	if err := service.Delete(context.Background(), testVideoID); err != nil {
		t.Fatalf("retry Delete returned %v", err)
	}
	state, begins, finalizes, _ = repo.snapshot()
	if state != "missing" || begins != 2 || finalizes != 2 {
		t.Fatalf(
			"after retry state=%q begins=%d finalizes=%d",
			state,
			begins,
			finalizes,
		)
	}
	if len(raw.calls()) != 2 {
		t.Fatalf("idempotent cleanup calls = %d, want 2", len(raw.calls()))
	}
}

func TestConcurrentDeleteReturnsConflictWithoutDuplicateCleanup(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "ready"}
	service, raw, _, _, _ := newDeletionService(repo)
	raw.started = make(chan struct{}, 1)
	raw.unblock = make(chan struct{})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.Delete(context.Background(), testVideoID)
	}()
	<-raw.started

	secondErr := service.Delete(context.Background(), testVideoID)
	if !errors.Is(secondErr, repository.ErrVideoDeletionInProgress) {
		t.Fatalf("concurrent Delete error = %v, want in progress", secondErr)
	}
	close(raw.unblock)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Delete returned %v", err)
	}

	state, begins, finalizes, _ := repo.snapshot()
	if state != "missing" || begins != 2 || finalizes != 1 {
		t.Fatalf(
			"state=%q begins=%d finalizes=%d",
			state,
			begins,
			finalizes,
		)
	}
	if len(raw.calls()) != 1 {
		t.Fatalf("raw cleanup calls = %d, want 1", len(raw.calls()))
	}
}

func TestActiveVideoIsExplicitConflictAndRunsNoCleanup(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "processing", active: true}
	service, raw, hls, thumbnails, redis := newDeletionService(repo)
	err := service.Delete(context.Background(), testVideoID)
	if !errors.Is(err, repository.ErrVideoActive) {
		t.Fatalf("Delete error = %v, want active-video conflict", err)
	}
	state, _, finalizes, _ := repo.snapshot()
	if state != "processing" || finalizes != 0 {
		t.Fatalf("state=%q finalizes=%d", state, finalizes)
	}
	if len(raw.calls())+len(hls.calls())+len(thumbnails.calls())+
		len(redis.calls())+len(redis.multipartCalls()) != 0 {
		t.Fatal("active video cleanup unexpectedly ran")
	}
}

func TestDeleteRejectsInvalidVideoIDBeforeRepository(t *testing.T) {
	t.Parallel()

	repo := &fakeDeletionRepository{state: "ready"}
	service := &videoDeletionService{repo: repo}

	err := service.Delete(context.Background(), "../other-video")
	if !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("Delete error = %v, want ErrNotFound", err)
	}
	_, begins, _, _ := repo.snapshot()
	if begins != 0 {
		t.Fatal("repository was called for an unsafe video ID")
	}
}
