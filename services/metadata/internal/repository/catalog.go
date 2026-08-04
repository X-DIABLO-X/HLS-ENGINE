package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrForbidden      = errors.New("forbidden")
	ErrLinkedVideo    = errors.New("video is linked to catalog content")
	ErrVideoNotReady  = errors.New("video is not ready")
	ErrInvalidCatalog = errors.New("invalid catalog relationship")
	ErrShareNotFound  = errors.New("share link not found")
)

// Title is the root catalog record. Movies have one playable; series have
// seasons and episode playables.
type Title struct {
	ID             string    `json:"id"`
	OwnerUserID    string    `json:"ownerUserId"`
	Type           string    `json:"type"`
	Title          string    `json:"title"`
	Synopsis       string    `json:"synopsis,omitempty"`
	Genres         []string  `json:"genres,omitempty"`
	ReleaseDate    *string   `json:"releaseDate,omitempty"`
	MaturityRating string    `json:"maturityRating,omitempty"`
	PosterURL      string    `json:"posterUrl,omitempty"`
	BackdropURL    string    `json:"backdropUrl,omitempty"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type Season struct {
	ID           string    `json:"id"`
	SeriesID     string    `json:"seriesId"`
	SeasonNumber int       `json:"seasonNumber"`
	Title        string    `json:"title,omitempty"`
	Synopsis     string    `json:"synopsis,omitempty"`
	PosterURL    string    `json:"posterUrl,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type Playable struct {
	ID            string    `json:"id"`
	TitleID       string    `json:"titleId"`
	SeasonID      *string   `json:"seasonId,omitempty"`
	VideoID       string    `json:"videoId"`
	Type          string    `json:"type"`
	EpisodeNumber *int      `json:"episodeNumber,omitempty"`
	Title         string    `json:"title"`
	Synopsis      string    `json:"synopsis,omitempty"`
	ArtworkURL    string    `json:"artworkUrl,omitempty"`
	Status        string    `json:"status"`
	ShareID       *string   `json:"shareId,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type NavigationItem struct {
	Title   string `json:"title"`
	ShareID string `json:"shareId"`
}

type SharedPlayable struct {
	Playable
	SeriesTitle string          `json:"seriesTitle,omitempty"`
	Previous    *NavigationItem `json:"previous,omitempty"`
	Next        *NavigationItem `json:"next,omitempty"`
}

const creatorSchema = `
ALTER TABLE videos ADD COLUMN IF NOT EXISTS owner_user_id UUID;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS share_id UUID;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS share_enabled BOOLEAN NOT NULL DEFAULT FALSE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_videos_share_id_unique ON videos(share_id) WHERE share_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_videos_owner_created_at ON videos(owner_user_id, created_at DESC);
CREATE TABLE IF NOT EXISTS catalog_titles (
 id UUID PRIMARY KEY, owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
 type VARCHAR(20) NOT NULL CHECK (type IN ('movie','series')), title VARCHAR(500) NOT NULL,
 synopsis TEXT, genres TEXT[] NOT NULL DEFAULT '{}', release_date DATE, maturity_rating VARCHAR(50),
 poster_url TEXT, backdrop_url TEXT, status VARCHAR(20) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_catalog_titles_owner_created ON catalog_titles(owner_user_id, created_at DESC);
CREATE TABLE IF NOT EXISTS catalog_seasons (
 id UUID PRIMARY KEY, series_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE,
 season_number INTEGER NOT NULL CHECK (season_number >= 1), title VARCHAR(500), synopsis TEXT, poster_url TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(series_id, season_number));
CREATE TABLE IF NOT EXISTS catalog_playables (
 id UUID PRIMARY KEY, title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE,
 season_id UUID REFERENCES catalog_seasons(id) ON DELETE CASCADE,
 video_id UUID NOT NULL UNIQUE REFERENCES videos(id) ON DELETE RESTRICT,
 type VARCHAR(20) NOT NULL CHECK (type IN ('movie','episode')), episode_number INTEGER CHECK (episode_number >= 1),
 title VARCHAR(500) NOT NULL, synopsis TEXT, artwork_url TEXT,
 status VARCHAR(20) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','published')),
 share_id UUID UNIQUE, share_enabled BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CHECK ((type = 'movie' AND season_id IS NULL AND episode_number IS NULL) OR (type = 'episode' AND season_id IS NOT NULL AND episode_number IS NOT NULL)));
CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_movie_per_title ON catalog_playables(title_id) WHERE type = 'movie';
CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_episode_order ON catalog_playables(season_id, episode_number) WHERE type = 'episode';
CREATE INDEX IF NOT EXISTS idx_catalog_playables_share ON catalog_playables(share_id) WHERE share_enabled;
`

// MigrateCreatorCatalog makes ownership mandatory while preserving every media
// object. The legacy owner is used only for pre-existing unowned records.
func (r *Repository) MigrateCreatorCatalog(ctx context.Context, legacyOwnerID string) error {
	if _, err := r.pool.Exec(ctx, creatorSchema); err != nil {
		return fmt.Errorf("apply creator catalog schema: %w", err)
	}
	var unowned int
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM videos WHERE owner_user_id IS NULL`).Scan(&unowned); err != nil {
		return fmt.Errorf("count unowned videos: %w", err)
	}
	if unowned > 0 {
		if _, err := uuid.Parse(legacyOwnerID); err != nil {
			return fmt.Errorf("invalid legacy owner id: %w", err)
		}
		var exists bool
		if err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, legacyOwnerID).Scan(&exists); err != nil || !exists {
			if err != nil {
				return fmt.Errorf("verify legacy owner: %w", err)
			}
			return fmt.Errorf("legacy owner does not exist")
		}
		if _, err := r.pool.Exec(ctx, `UPDATE videos SET owner_user_id = $1 WHERE owner_user_id IS NULL`, legacyOwnerID); err != nil {
			return fmt.Errorf("backfill video owner: %w", err)
		}
	}
	if _, err := r.pool.Exec(ctx, `ALTER TABLE videos ALTER COLUMN owner_user_id SET NOT NULL`); err != nil {
		return fmt.Errorf("require video owner: %w", err)
	}
	return nil
}

func (r *Repository) GetOwnedVideo(ctx context.Context, id, ownerID string) (*Video, error) {
	v := &Video{}
	var description *string
	err := r.pool.QueryRow(ctx, `SELECT id, owner_user_id, share_id, share_enabled, title, description, status, duration, tags, metadata, created_at, updated_at, published_at FROM videos WHERE id=$1 AND owner_user_id=$2`, id, ownerID).
		Scan(&v.ID, &v.OwnerUserID, &v.ShareID, &v.ShareEnabled, &v.Title, &description, &v.Status, &v.Duration, &v.Tags, &v.Metadata, &v.CreatedAt, &v.UpdatedAt, &v.PublishedAt)
	if err != nil {
		return nil, classifyOwnedError(err)
	}
	if description != nil {
		v.Description = *description
	}
	return v, nil
}

func (r *Repository) ListOwnedVideos(ctx context.Context, ownerID, search, status string, page, pageSize int) ([]*Video, int, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	where := `WHERE owner_user_id=$1`
	args := []interface{}{ownerID}
	next := 2
	if search != "" {
		where += fmt.Sprintf(` AND (title ILIKE $%d OR description ILIKE $%d)`, next, next)
		args = append(args, "%"+search+"%")
		next++
	}
	if status != "" {
		where += fmt.Sprintf(` AND status=$%d`, next)
		args = append(args, status)
		next++
	}
	var total int
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM videos `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.pool.Query(ctx, fmt.Sprintf(`SELECT id, owner_user_id, share_id, share_enabled, title, description, status, duration, tags, metadata, created_at, updated_at, published_at FROM videos %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, where, next, next+1), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	list := []*Video{}
	for rows.Next() {
		v := &Video{}
		var description *string
		if err := rows.Scan(&v.ID, &v.OwnerUserID, &v.ShareID, &v.ShareEnabled, &v.Title, &description, &v.Status, &v.Duration, &v.Tags, &v.Metadata, &v.CreatedAt, &v.UpdatedAt, &v.PublishedAt); err != nil {
			return nil, 0, err
		}
		if description != nil {
			v.Description = *description
		}
		list = append(list, v)
	}
	return list, total, rows.Err()
}

func (r *Repository) IsOwnedReadyVideo(ctx context.Context, ownerID, videoID string) error {
	var status string
	err := r.pool.QueryRow(ctx, `SELECT status FROM videos WHERE id=$1 AND owner_user_id=$2`, videoID, ownerID).Scan(&status)
	if err != nil {
		return classifyOwnedError(err)
	}
	if status != "ready" {
		return ErrVideoNotReady
	}
	return nil
}

func (r *Repository) SetVideoShare(ctx context.Context, ownerID, videoID string, enabled, rotate bool) (*Video, error) {
	if err := r.IsOwnedReadyVideo(ctx, ownerID, videoID); err != nil {
		return nil, err
	}
	var shareID *string
	if enabled {
		value := uuid.NewString()
		shareID = &value
		if !rotate {
			_ = r.pool.QueryRow(ctx, `SELECT share_id FROM videos WHERE id=$1 AND owner_user_id=$2 AND share_enabled`, videoID, ownerID).Scan(&shareID)
			if shareID == nil {
				value = uuid.NewString()
				shareID = &value
			}
		}
	}
	_, err := r.pool.Exec(ctx, `UPDATE videos SET share_id=$1, share_enabled=$2, updated_at=NOW() WHERE id=$3 AND owner_user_id=$4`, shareID, enabled, videoID, ownerID)
	if err != nil {
		return nil, err
	}
	return r.GetOwnedVideo(ctx, videoID, ownerID)
}

func (r *Repository) CreateTitle(ctx context.Context, ownerID string, title *Title) (*Title, error) {
	if title.Type != "movie" && title.Type != "series" {
		return nil, ErrInvalidCatalog
	}
	if title.Title == "" {
		return nil, ErrInvalidCatalog
	}
	if title.Genres == nil {
		title.Genres = []string{}
	}
	title.ID = uuid.NewString()
	title.OwnerUserID = ownerID
	title.Status = "draft"
	title.CreatedAt = time.Now().UTC()
	title.UpdatedAt = title.CreatedAt
	_, err := r.pool.Exec(ctx, `INSERT INTO catalog_titles (id,owner_user_id,type,title,synopsis,genres,release_date,maturity_rating,poster_url,backdrop_url,status,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,'')::date,$8,$9,$10,$11,$12,$13)`, title.ID, ownerID, title.Type, title.Title, title.Synopsis, title.Genres, nullableString(title.ReleaseDate), title.MaturityRating, title.PosterURL, title.BackdropURL, title.Status, title.CreatedAt, title.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return title, nil
}

func (r *Repository) ListTitles(ctx context.Context, ownerID, kind string) ([]*Title, error) {
	rows, err := r.pool.Query(ctx, `SELECT id,owner_user_id,type,title,synopsis,genres,COALESCE(release_date::text,''),maturity_rating,poster_url,backdrop_url,status,created_at,updated_at FROM catalog_titles WHERE owner_user_id=$1 AND ($2='' OR type=$2) ORDER BY created_at DESC`, ownerID, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []*Title{}
	for rows.Next() {
		v := &Title{}
		var rd string
		if err := rows.Scan(&v.ID, &v.OwnerUserID, &v.Type, &v.Title, &v.Synopsis, &v.Genres, &rd, &v.MaturityRating, &v.PosterURL, &v.BackdropURL, &v.Status, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		if rd != "" {
			v.ReleaseDate = &rd
		}
		list = append(list, v)
	}
	return list, rows.Err()
}

func (r *Repository) GetTitle(ctx context.Context, ownerID, id string) (*Title, error) {
	v := &Title{}
	var rd string
	err := r.pool.QueryRow(ctx, `SELECT id,owner_user_id,type,title,synopsis,genres,COALESCE(release_date::text,''),maturity_rating,poster_url,backdrop_url,status,created_at,updated_at FROM catalog_titles WHERE id=$1 AND owner_user_id=$2`, id, ownerID).Scan(&v.ID, &v.OwnerUserID, &v.Type, &v.Title, &v.Synopsis, &v.Genres, &rd, &v.MaturityRating, &v.PosterURL, &v.BackdropURL, &v.Status, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		return nil, classifyOwnedError(err)
	}
	if rd != "" {
		v.ReleaseDate = &rd
	}
	return v, nil
}

func (r *Repository) UpdateTitle(ctx context.Context, ownerID, id string, next *Title) (*Title, error) {
	current, err := r.GetTitle(ctx, ownerID, id)
	if err != nil {
		return nil, err
	}
	if next.Title != "" {
		current.Title = next.Title
	}
	if next.Synopsis != "" {
		current.Synopsis = next.Synopsis
	}
	if next.Genres != nil {
		current.Genres = next.Genres
	}
	if next.ReleaseDate != nil {
		current.ReleaseDate = next.ReleaseDate
	}
	if next.MaturityRating != "" {
		current.MaturityRating = next.MaturityRating
	}
	if next.PosterURL != "" {
		current.PosterURL = next.PosterURL
	}
	if next.BackdropURL != "" {
		current.BackdropURL = next.BackdropURL
	}
	_, err = r.pool.Exec(ctx, `UPDATE catalog_titles SET title=$1,synopsis=$2,genres=$3,release_date=NULLIF($4,'')::date,maturity_rating=$5,poster_url=$6,backdrop_url=$7,updated_at=NOW() WHERE id=$8 AND owner_user_id=$9`, current.Title, current.Synopsis, current.Genres, nullableString(current.ReleaseDate), current.MaturityRating, current.PosterURL, current.BackdropURL, id, ownerID)
	if err != nil {
		return nil, err
	}
	return r.GetTitle(ctx, ownerID, id)
}

// DeleteTitle removes the creator-owned catalog record and its children. The
// linked upload assets stay in the upload library.
func (r *Repository) DeleteTitle(ctx context.Context, ownerID, id string) error {
	cmd, err := r.pool.Exec(ctx, `DELETE FROM catalog_titles WHERE id=$1 AND owner_user_id=$2`, id, ownerID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) CreateSeason(ctx context.Context, ownerID, seriesID string, season *Season) (*Season, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_titles WHERE id=$1 AND owner_user_id=$2 AND type='series')`, seriesID, ownerID).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrForbidden
	}
	if season.SeasonNumber < 1 {
		return nil, ErrInvalidCatalog
	}
	season.ID = uuid.NewString()
	season.SeriesID = seriesID
	season.CreatedAt = time.Now().UTC()
	season.UpdatedAt = season.CreatedAt
	_, err = r.pool.Exec(ctx, `INSERT INTO catalog_seasons (id,series_id,season_number,title,synopsis,poster_url,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, season.ID, seriesID, season.SeasonNumber, season.Title, season.Synopsis, season.PosterURL, season.CreatedAt, season.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return season, nil
}

func (r *Repository) ListSeasons(ctx context.Context, ownerID, seriesID string) ([]*Season, error) {
	rows, err := r.pool.Query(ctx, `SELECT s.id,s.series_id,s.season_number,COALESCE(s.title,''),COALESCE(s.synopsis,''),COALESCE(s.poster_url,''),s.created_at,s.updated_at FROM catalog_seasons s JOIN catalog_titles t ON t.id=s.series_id WHERE s.series_id=$1 AND t.owner_user_id=$2 ORDER BY s.season_number`, seriesID, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []*Season{}
	for rows.Next() {
		v := &Season{}
		if err := rows.Scan(&v.ID, &v.SeriesID, &v.SeasonNumber, &v.Title, &v.Synopsis, &v.PosterURL, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		list = append(list, v)
	}
	return list, rows.Err()
}

func (r *Repository) CreatePlayable(ctx context.Context, ownerID string, p *Playable) (*Playable, error) {
	if p.Type != "movie" && p.Type != "episode" {
		return nil, ErrInvalidCatalog
	}
	if p.Title == "" {
		return nil, ErrInvalidCatalog
	}
	if err := r.IsOwnedReadyVideo(ctx, ownerID, p.VideoID); err != nil {
		return nil, err
	}
	var titleType string
	err := r.pool.QueryRow(ctx, `SELECT type FROM catalog_titles WHERE id=$1 AND owner_user_id=$2`, p.TitleID, ownerID).Scan(&titleType)
	if err != nil {
		return nil, classifyOwnedError(err)
	}
	if (p.Type == "movie" && titleType != "movie") || (p.Type == "episode" && titleType != "series") {
		return nil, ErrInvalidCatalog
	}
	if p.Type == "episode" {
		if p.SeasonID == nil || p.EpisodeNumber == nil || *p.EpisodeNumber < 1 {
			return nil, ErrInvalidCatalog
		}
		var ok bool
		err = r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_seasons WHERE id=$1 AND series_id=$2)`, *p.SeasonID, p.TitleID).Scan(&ok)
		if err != nil || !ok {
			return nil, ErrInvalidCatalog
		}
	} else {
		p.SeasonID = nil
		p.EpisodeNumber = nil
	}
	p.ID = uuid.NewString()
	p.Status = "draft"
	p.CreatedAt = time.Now().UTC()
	p.UpdatedAt = p.CreatedAt
	_, err = r.pool.Exec(ctx, `INSERT INTO catalog_playables (id,title_id,season_id,video_id,type,episode_number,title,synopsis,artwork_url,status,share_enabled,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,FALSE,$11,$12)`, p.ID, p.TitleID, p.SeasonID, p.VideoID, p.Type, p.EpisodeNumber, p.Title, p.Synopsis, p.ArtworkURL, p.Status, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (r *Repository) ListPlayables(ctx context.Context, ownerID, titleID string) ([]*Playable, error) {
	rows, err := r.pool.Query(ctx, `SELECT p.id,p.title_id,p.season_id,p.video_id,p.type,p.episode_number,p.title,COALESCE(p.synopsis,''),COALESCE(p.artwork_url,''),p.status,p.share_id,p.created_at,p.updated_at FROM catalog_playables p JOIN catalog_titles t ON t.id=p.title_id LEFT JOIN catalog_seasons s ON s.id=p.season_id WHERE p.title_id=$1 AND t.owner_user_id=$2 ORDER BY COALESCE(s.season_number,0),COALESCE(p.episode_number,0)`, titleID, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPlayables(rows)
}

func (r *Repository) SetPlayablePublication(ctx context.Context, ownerID, id string, publish bool, rotate bool) (*Playable, error) {
	var p Playable
	var seasonID *string
	err := r.pool.QueryRow(ctx, `SELECT p.id,p.title_id,p.season_id,p.video_id,p.type,p.episode_number,p.title,COALESCE(p.synopsis,''),COALESCE(p.artwork_url,''),p.status,p.share_id,p.created_at,p.updated_at FROM catalog_playables p JOIN catalog_titles t ON t.id=p.title_id WHERE p.id=$1 AND t.owner_user_id=$2`, id, ownerID).Scan(&p.ID, &p.TitleID, &seasonID, &p.VideoID, &p.Type, &p.EpisodeNumber, &p.Title, &p.Synopsis, &p.ArtworkURL, &p.Status, &p.ShareID, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, classifyOwnedError(err)
	}
	p.SeasonID = seasonID
	if publish {
		if err := r.IsOwnedReadyVideo(ctx, ownerID, p.VideoID); err != nil {
			return nil, err
		}
		if p.ShareID == nil || rotate {
			share := uuid.NewString()
			p.ShareID = &share
		}
		p.Status = "published"
		_, err = r.pool.Exec(ctx, `UPDATE catalog_playables SET status='published',share_id=$1,share_enabled=TRUE,updated_at=NOW() WHERE id=$2`, p.ShareID, id)
		if err == nil {
			_, err = r.pool.Exec(ctx, `UPDATE catalog_titles SET status='published',updated_at=NOW() WHERE id=$1`, p.TitleID)
		}
	} else {
		p.Status = "draft"
		p.ShareID = nil
		_, err = r.pool.Exec(ctx, `UPDATE catalog_playables SET status='draft',share_id=NULL,share_enabled=FALSE,updated_at=NOW() WHERE id=$1`, id)
	}
	if err != nil {
		return nil, err
	}
	return r.GetOwnedPlayable(ctx, ownerID, id)
}

func (r *Repository) GetOwnedPlayable(ctx context.Context, ownerID, id string) (*Playable, error) {
	rows, err := r.pool.Query(ctx, `SELECT p.id,p.title_id,p.season_id,p.video_id,p.type,p.episode_number,p.title,COALESCE(p.synopsis,''),COALESCE(p.artwork_url,''),p.status,p.share_id,p.created_at,p.updated_at FROM catalog_playables p JOIN catalog_titles t ON t.id=p.title_id WHERE p.id=$1 AND t.owner_user_id=$2`, id, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanPlayables(rows)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, ErrNotFound
	}
	return items[0], nil
}

// DeletePlayable unlinks an upload from catalog management while retaining the
// underlying video, so it can safely be re-used or deleted afterwards.
func (r *Repository) DeletePlayable(ctx context.Context, ownerID, id string) error {
	cmd, err := r.pool.Exec(ctx, `DELETE FROM catalog_playables p USING catalog_titles t WHERE p.id=$1 AND p.title_id=t.id AND t.owner_user_id=$2`, id, ownerID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) GetSharedPlayable(ctx context.Context, shareID string) (*SharedPlayable, error) {
	result := &SharedPlayable{}
	var seasonID *string
	err := r.pool.QueryRow(ctx, `SELECT p.id,p.title_id,p.season_id,p.video_id,p.type,p.episode_number,p.title,COALESCE(p.synopsis,''),COALESCE(p.artwork_url,''),p.status,p.share_id,p.created_at,p.updated_at,COALESCE(t.title,'') FROM catalog_playables p JOIN catalog_titles t ON t.id=p.title_id JOIN videos v ON v.id=p.video_id WHERE p.share_id=$1 AND p.share_enabled AND p.status='published' AND v.status='ready'`, shareID).Scan(&result.ID, &result.TitleID, &seasonID, &result.VideoID, &result.Type, &result.EpisodeNumber, &result.Title, &result.Synopsis, &result.ArtworkURL, &result.Status, &result.ShareID, &result.CreatedAt, &result.UpdatedAt, &result.SeriesTitle)
	if err != nil {
		return nil, ErrShareNotFound
	}
	result.SeasonID = seasonID
	if result.Type == "episode" {
		rows, err := r.pool.Query(ctx, `SELECT p.title,p.share_id FROM catalog_playables p JOIN catalog_seasons s ON s.id=p.season_id WHERE p.title_id=$1 AND p.type='episode' AND p.status='published' AND p.share_enabled ORDER BY s.season_number,p.episode_number`, result.TitleID)
		if err == nil {
			defer rows.Close()
			items := []NavigationItem{}
			for rows.Next() {
				var x NavigationItem
				if rows.Scan(&x.Title, &x.ShareID) == nil {
					items = append(items, x)
				}
			}
			for i, x := range items {
				if result.ShareID != nil && x.ShareID == *result.ShareID {
					if i > 0 {
						v := items[i-1]
						result.Previous = &v
					}
					if i+1 < len(items) {
						v := items[i+1]
						result.Next = &v
					}
					break
				}
			}
		}
	}
	return result, nil
}

func (r *Repository) GetSharedVideo(ctx context.Context, shareID string) (*Video, error) {
	v := &Video{}
	var d *string
	err := r.pool.QueryRow(ctx, `SELECT id,owner_user_id,share_id,share_enabled,title,description,status,duration,tags,metadata,created_at,updated_at,published_at FROM videos WHERE share_id=$1 AND share_enabled AND status='ready'`, shareID).Scan(&v.ID, &v.OwnerUserID, &v.ShareID, &v.ShareEnabled, &v.Title, &d, &v.Status, &v.Duration, &v.Tags, &v.Metadata, &v.CreatedAt, &v.UpdatedAt, &v.PublishedAt)
	if err != nil {
		return nil, ErrShareNotFound
	}
	if d != nil {
		v.Description = *d
	}
	return v, nil
}

func (r *Repository) VideoIsLinked(ctx context.Context, videoID string) (bool, error) {
	var linked bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_playables WHERE video_id=$1)`, videoID).Scan(&linked)
	return linked, err
}

func classifyOwnedError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
func nullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

type rowScanner interface {
	Next() bool
	Scan(...interface{}) error
	Err() error
}

func scanPlayables(rows rowScanner) ([]*Playable, error) {
	list := []*Playable{}
	for rows.Next() {
		v := &Playable{}
		var sid *string
		if err := rows.Scan(&v.ID, &v.TitleID, &sid, &v.VideoID, &v.Type, &v.EpisodeNumber, &v.Title, &v.Synopsis, &v.ArtworkURL, &v.Status, &v.ShareID, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		v.SeasonID = sid
		list = append(list, v)
	}
	return list, rows.Err()
}
