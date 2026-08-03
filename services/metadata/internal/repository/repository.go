package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"hls-engine/internal/videolease"
)

var (
	ErrNotFound                = errors.New("not found")
	ErrVideoActive             = errors.New("video has active processing jobs")
	ErrVideoDeletionInProgress = errors.New("video deletion is already in progress")
	ErrVideoNotDeleting        = errors.New("video is not marked for deletion")
)

// VideoDeletion owns the cross-service mutation fence acquired while the
// durable deleting tombstone was committed.
type VideoDeletion interface {
	Finalize(ctx context.Context) error
	Release(ctx context.Context) error
}

// Video represents a video asset.
type Video struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	Status      string     `json:"status"`
	Duration    *float64   `json:"duration,omitempty"`
	Tags        []string   `json:"tags,omitempty"`
	Metadata    []byte     `json:"metadata,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	PublishedAt *time.Time `json:"publishedAt,omitempty"`
}

// Rendition represents an HLS rendition.
type Rendition struct {
	ID        string    `json:"id"`
	VideoID   string    `json:"video_id"`
	Name      string    `json:"name"`
	Bandwidth int       `json:"bandwidth"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	Codec     string    `json:"codec"`
	MasterURL string    `json:"master_url,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// AudioTrack represents an audio track.
type AudioTrack struct {
	ID        string    `json:"id"`
	VideoID   string    `json:"video_id"`
	Language  string    `json:"language"`
	Name      string    `json:"name"`
	Codec     string    `json:"codec"`
	Default   bool      `json:"default"`
	DelayMs   float64   `json:"delay_ms"`
	CreatedAt time.Time `json:"created_at"`
}

// Subtitle represents a subtitle track.
type Subtitle struct {
	ID        string    `json:"id"`
	VideoID   string    `json:"video_id"`
	Language  string    `json:"language"`
	Name      string    `json:"name"`
	Format    string    `json:"format"`
	CreatedAt time.Time `json:"created_at"`
}

// Repository abstracts metadata persistence.
type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateVideo(ctx context.Context, title, description string, tags []string) (*Video, error) {
	v := &Video{
		ID:          uuid.NewString(),
		Title:       title,
		Description: description,
		Status:      "uploading",
		Tags:        tags,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO videos (id, title, description, status, tags, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
	`, v.ID, v.Title, v.Description, v.Status, v.Tags, v.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert video: %w", err)
	}
	return v, nil
}

func (r *Repository) GetVideo(ctx context.Context, id string) (*Video, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, title, description, status, duration, tags, metadata, created_at, updated_at, published_at
		FROM videos WHERE id = $1
	`, id)
	v := &Video{}
	var description *string
	var tags []string
	var publishedAt *time.Time
	err := row.Scan(&v.ID, &v.Title, &description, &v.Status, &v.Duration, &tags, &v.Metadata, &v.CreatedAt, &v.UpdatedAt, &publishedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if description != nil {
		v.Description = *description
	}
	v.Tags = tags
	v.PublishedAt = publishedAt
	return v, nil
}

func (r *Repository) ListVideos(ctx context.Context, search string, status string, page, pageSize int) ([]*Video, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	where := "WHERE 1=1"
	args := []interface{}{}
	argIdx := 1
	if search != "" {
		where += fmt.Sprintf(" AND (title ILIKE $%d OR description ILIKE $%d)", argIdx, argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}
	if status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	countArgs := append([]interface{}{}, args...)
	var total int
	countQuery := "SELECT COUNT(*) FROM videos " + where
	if err := r.pool.QueryRow(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, pageSize, offset)
	query := fmt.Sprintf(`
		SELECT id, title, description, status, duration, tags, metadata, created_at, updated_at, published_at
		FROM videos %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var videos = []*Video{}
	for rows.Next() {
		v := &Video{}
		var description *string
		var tags []string
		var publishedAt *time.Time
		if err := rows.Scan(&v.ID, &v.Title, &description, &v.Status, &v.Duration, &tags, &v.Metadata, &v.CreatedAt, &v.UpdatedAt, &publishedAt); err != nil {
			return nil, 0, err
		}
		if description != nil {
			v.Description = *description
		}
		v.Tags = tags
		v.PublishedAt = publishedAt
		videos = append(videos, v)
	}
	return videos, total, nil
}

func (r *Repository) UpdateVideo(ctx context.Context, id string, updates map[string]interface{}) (*Video, error) {
	set := []string{}
	args := []interface{}{}
	idx := 1
	for k, v := range updates {
		set = append(set, fmt.Sprintf("%s = $%d", k, idx))
		args = append(args, v)
		idx++
	}
	if len(set) == 0 {
		return r.GetVideo(ctx, id)
	}
	set = append(set, fmt.Sprintf("updated_at = $%d", idx))
	args = append(args, time.Now().UTC())
	idx++
	args = append(args, id)

	query := fmt.Sprintf("UPDATE videos SET %s WHERE id = $%d", join(set, ", "), idx)
	_, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return r.GetVideo(ctx, id)
}

// BeginVideoDeletion acquires the same cross-service fence used by upload
// commits, verifies the explicit active-work policy, and commits a durable
// deleting tombstone before any external resource can be removed.
func (r *Repository) BeginVideoDeletion(ctx context.Context, id string) (VideoDeletion, error) {
	lease, err := videolease.TryAcquire(ctx, r.pool, id)
	if err != nil {
		if errors.Is(err, videolease.ErrBusy) {
			return nil, ErrVideoDeletionInProgress
		}
		return nil, fmt.Errorf("acquire video deletion lease: %w", err)
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = lease.Release(releaseCtx)
		}
	}()

	tx, err := lease.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin video tombstone: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	var videoID, status string
	if err := tx.QueryRow(ctx, `
		SELECT id, status
		FROM videos
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&videoID, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("lock video for tombstone: %w", err)
	}

	var activeJobs bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM jobs
			WHERE video_id = $1
			  AND status NOT IN ('completed', 'failed', 'cancelled')
		)
	`, videoID).Scan(&activeJobs); err != nil {
		return nil, fmt.Errorf("check active video jobs: %w", err)
	}
	if activeJobs {
		return nil, ErrVideoActive
	}

	if status != "deleting" {
		tag, err := tx.Exec(ctx, `
			UPDATE videos
			SET status = 'deleting', updated_at = NOW()
			WHERE id = $1
		`, videoID)
		if err != nil {
			return nil, fmt.Errorf("write video deletion tombstone: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, ErrNotFound
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit video deletion tombstone: %w", err)
	}

	releaseOnError = false
	return &repositoryVideoDeletion{lease: lease, videoID: videoID}, nil
}

type repositoryVideoDeletion struct {
	lease   *videolease.Lease
	videoID string
}

// Finalize deletes the tombstoned row in a fresh, short transaction after all
// external cleanup has been verified. Any failure rolls back to deleting.
func (d *repositoryVideoDeletion) Finalize(ctx context.Context) error {
	tx, err := d.lease.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin final video deletion: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	var status string
	if err := tx.QueryRow(ctx, `
		SELECT status
		FROM videos
		WHERE id = $1
		FOR UPDATE
	`, d.videoID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("lock tombstoned video: %w", err)
	}
	if status != "deleting" {
		return fmt.Errorf("%w: status is %q", ErrVideoNotDeleting, status)
	}

	// Recheck at the destructive commit boundary. A job that appeared after
	// the initial policy check must leave the durable tombstone in place.
	var activeJobs bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM jobs
			WHERE video_id = $1
			  AND status NOT IN ('completed', 'failed', 'cancelled')
		)
	`, d.videoID).Scan(&activeJobs); err != nil {
		return fmt.Errorf("recheck active video jobs: %w", err)
	}
	if activeJobs {
		return ErrVideoActive
	}

	tag, err := tx.Exec(ctx, `
		DELETE FROM videos
		WHERE id = $1 AND status = 'deleting'
	`, d.videoID)
	if err != nil {
		return fmt.Errorf("delete tombstoned video: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrVideoNotDeleting
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit final video deletion: %w", err)
	}
	return nil
}

func (d *repositoryVideoDeletion) Release(ctx context.Context) error {
	return d.lease.Release(ctx)
}

func (r *Repository) UpdateStatus(ctx context.Context, id, status string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE videos
		SET status = $1, updated_at = NOW()
		WHERE id = $2 AND status <> 'deleting'
	`, status, id)
	if err == nil && tag.RowsAffected() == 0 {
		var current string
		switch scanErr := r.pool.QueryRow(
			ctx,
			`SELECT status FROM videos WHERE id = $1`,
			id,
		).Scan(&current); {
		case errors.Is(scanErr, pgx.ErrNoRows):
			return ErrNotFound
		case scanErr != nil:
			return scanErr
		case current == "deleting":
			return ErrVideoDeletionInProgress
		default:
			return ErrNotFound
		}
	}
	return err
}

func (r *Repository) CreateRendition(ctx context.Context, videoID, name, codec string, bandwidth, width, height int) (*Rendition, error) {
	rend := &Rendition{
		ID:        uuid.NewString(),
		VideoID:   videoID,
		Name:      name,
		Codec:     codec,
		Bandwidth: bandwidth,
		Width:     width,
		Height:    height,
		CreatedAt: time.Now().UTC(),
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO renditions (id, video_id, name, codec, bandwidth, width, height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, rend.ID, rend.VideoID, rend.Name, rend.Codec, rend.Bandwidth, rend.Width, rend.Height, rend.CreatedAt)
	if err != nil {
		return nil, err
	}
	return rend, nil
}

func (r *Repository) ListRenditions(ctx context.Context, videoID string) ([]*Rendition, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, video_id, name, codec, bandwidth, width, height, master_url, created_at
		FROM renditions WHERE video_id = $1 ORDER BY bandwidth DESC
	`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list = []*Rendition{}
	for rows.Next() {
		rend := &Rendition{}
		if err := rows.Scan(&rend.ID, &rend.VideoID, &rend.Name, &rend.Codec, &rend.Bandwidth, &rend.Width, &rend.Height, &rend.MasterURL, &rend.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, rend)
	}
	return list, nil
}

func (r *Repository) CreateAudioTrack(ctx context.Context, videoID, language, name, codec string, def bool, delayMs float64) (*AudioTrack, error) {
	t := &AudioTrack{
		ID:        uuid.NewString(),
		VideoID:   videoID,
		Language:  language,
		Name:      name,
		Codec:     codec,
		Default:   def,
		DelayMs:   delayMs,
		CreatedAt: time.Now().UTC(),
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO audio_tracks (id, video_id, language, name, codec, "default", delay_ms, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, t.ID, t.VideoID, t.Language, t.Name, t.Codec, t.Default, t.DelayMs, t.CreatedAt)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (r *Repository) ListAudioTracks(ctx context.Context, videoID string) ([]*AudioTrack, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, video_id, language, name, codec, "default", delay_ms, created_at
		FROM audio_tracks WHERE video_id = $1
	`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list = []*AudioTrack{}
	for rows.Next() {
		t := &AudioTrack{}
		if err := rows.Scan(&t.ID, &t.VideoID, &t.Language, &t.Name, &t.Codec, &t.Default, &t.DelayMs, &t.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, t)
	}
	return list, nil
}

func (r *Repository) CreateSubtitle(ctx context.Context, videoID, language, name, format string) (*Subtitle, error) {
	s := &Subtitle{
		ID:        uuid.NewString(),
		VideoID:   videoID,
		Language:  language,
		Name:      name,
		Format:    format,
		CreatedAt: time.Now().UTC(),
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO subtitles (id, video_id, language, name, format, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, s.ID, s.VideoID, s.Language, s.Name, s.Format, s.CreatedAt)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (r *Repository) ListSubtitles(ctx context.Context, videoID string) ([]*Subtitle, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, video_id, language, name, format, created_at
		FROM subtitles WHERE video_id = $1
	`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list = []*Subtitle{}
	for rows.Next() {
		s := &Subtitle{}
		if err := rows.Scan(&s.ID, &s.VideoID, &s.Language, &s.Name, &s.Format, &s.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, s)
	}
	return list, nil
}

func join(elems []string, sep string) string {
	if len(elems) == 0 {
		return ""
	}
	s := elems[0]
	for i := 1; i < len(elems); i++ {
		s += sep + elems[i]
	}
	return s
}
