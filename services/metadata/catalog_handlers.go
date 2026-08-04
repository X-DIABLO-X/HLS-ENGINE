package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"hls-engine/internal/jwt"
	"hls-engine/metadata/internal/repository"
)

func creatorID(w http.ResponseWriter, r *http.Request) (string, bool) {
	if r.Header.Get("X-Authenticated-API-Key") == "true" {
		respondError(w, http.StatusForbidden, "API keys cannot manage creator-owned content")
		return "", false
	}
	ownerID := r.Header.Get("X-Authenticated-User-ID")
	if _, err := uuid.Parse(ownerID); err != nil {
		respondError(w, http.StatusUnauthorized, "creator identity is required")
		return "", false
	}
	return ownerID, true
}

func respondOwnedError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrNotFound), errors.Is(err, repository.ErrForbidden):
		respondError(w, http.StatusNotFound, "resource not found")
	case errors.Is(err, repository.ErrVideoNotReady):
		respondError(w, http.StatusConflict, "video is not ready")
	case errors.Is(err, repository.ErrInvalidCatalog):
		respondError(w, http.StatusBadRequest, "invalid catalog data")
	default:
		respondError(w, http.StatusInternalServerError, "internal error")
	}
}

func (s *server) ownedVideoOrReject(w http.ResponseWriter, r *http.Request) (*repository.Video, bool) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return nil, false
	}
	video, err := s.repo.GetOwnedVideo(r.Context(), chi.URLParam(r, "id"), ownerID)
	if err != nil {
		respondOwnedError(w, err)
		return nil, false
	}
	return video, true
}

type shareReq struct {
	Enabled bool `json:"enabled"`
	Rotate  bool `json:"rotate"`
}

func (s *server) updateVideoShare(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	var req shareReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	video, err := s.repo.SetVideoShare(r.Context(), ownerID, chi.URLParam(r, "id"), req.Enabled, req.Rotate)
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"shareId": video.ShareID, "enabled": req.Enabled})
}

func (s *server) createTitle(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	var req repository.Title
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	title, err := s.repo.CreateTitle(r.Context(), ownerID, &req)
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusCreated, title)
}
func (s *server) listTitles(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	list, err := s.repo.ListTitles(r.Context(), ownerID, r.URL.Query().Get("type"))
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"titles": list})
}
func (s *server) getTitle(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	title, err := s.repo.GetTitle(r.Context(), ownerID, chi.URLParam(r, "id"))
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, title)
}
func (s *server) updateTitle(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	var req repository.Title
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	title, err := s.repo.UpdateTitle(r.Context(), ownerID, chi.URLParam(r, "id"), &req)
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, title)
}
func (s *server) deleteTitle(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	if err := s.repo.DeleteTitle(r.Context(), ownerID, chi.URLParam(r, "id")); err != nil {
		respondOwnedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) createSeason(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	var req repository.Season
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	season, err := s.repo.CreateSeason(r.Context(), ownerID, chi.URLParam(r, "id"), &req)
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusCreated, season)
}
func (s *server) listSeasons(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	items, err := s.repo.ListSeasons(r.Context(), ownerID, chi.URLParam(r, "id"))
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"seasons": items})
}
func (s *server) createPlayable(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	var req repository.Playable
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json")
		return
	}
	req.TitleID = chi.URLParam(r, "id")
	item, err := s.repo.CreatePlayable(r.Context(), ownerID, &req)
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusCreated, item)
}
func (s *server) listPlayables(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	items, err := s.repo.ListPlayables(r.Context(), ownerID, chi.URLParam(r, "id"))
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"playables": items})
}
func (s *server) deletePlayable(w http.ResponseWriter, r *http.Request) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	if err := s.repo.DeletePlayable(r.Context(), ownerID, chi.URLParam(r, "id")); err != nil {
		respondOwnedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) publishPlayable(w http.ResponseWriter, r *http.Request) {
	s.setPlayablePublication(w, r, true, false)
}
func (s *server) unpublishPlayable(w http.ResponseWriter, r *http.Request) {
	s.setPlayablePublication(w, r, false, false)
}
func (s *server) rotatePlayableShare(w http.ResponseWriter, r *http.Request) {
	s.setPlayablePublication(w, r, true, true)
}
func (s *server) setPlayablePublication(w http.ResponseWriter, r *http.Request, publish, rotate bool) {
	ownerID, ok := creatorID(w, r)
	if !ok {
		return
	}
	item, err := s.repo.SetPlayablePublication(r.Context(), ownerID, chi.URLParam(r, "id"), publish, rotate)
	if err != nil {
		respondOwnedError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, item)
}

func (s *server) getSharedEmbed(w http.ResponseWriter, r *http.Request) {
	shareID := chi.URLParam(r, "shareID")
	if _, err := uuid.Parse(shareID); err != nil {
		respondError(w, http.StatusNotFound, "share link not found")
		return
	}
	if item, err := s.repo.GetSharedPlayable(r.Context(), shareID); err == nil {
		respondJSON(w, http.StatusOK, s.embedPayload(item.VideoID, item.Title, item.Type, item.Previous, item.Next))
		return
	}
	video, err := s.repo.GetSharedVideo(r.Context(), shareID)
	if err != nil {
		respondError(w, http.StatusNotFound, "share link not found")
		return
	}
	respondJSON(w, http.StatusOK, s.embedPayload(video.ID, video.Title, "standalone", nil, nil))
}

func (s *server) embedPayload(videoID, title, kind string, previous, next *repository.NavigationItem) map[string]interface{} {
	manifestPath := "/hls/" + videoID + "/"
	secret := []byte(getEnv("JWT_HMAC_SECRET", "change-me-signed-url-hmac-secret"))
	expires := time.Now().Add(24 * time.Hour)
	token := jwt.BuildSignedTokenPrefix(secret, manifestPath, expires)
	return map[string]interface{}{"title": title, "kind": kind, "url": "/hls/" + videoID + "/master.m3u8?token=" + url.QueryEscape(token), "token": token, "expiresAt": expires.UTC().Format(time.RFC3339), "previous": previous, "next": next}
}
