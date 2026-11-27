package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/spotify"
	"github.com/google/uuid"
)

type webPlaylistItemDTO struct {
	Index   int              `json:"index"`
	Kind    string           `json:"kind"` // "track" or "missing"
	Track   *webTrackDTO     `json:"track,omitempty"`
	Missing *db.MissingTrack `json:"missing,omitempty"`
}

func (s *Server) buildWebPlaylistDTO(p db.SharedPlaylist, byID map[string]webTrackDTO) webPlaylistDTO {
	missingByID := make(map[string]db.MissingTrack, len(p.MissingTracks))
	for _, m := range p.MissingTracks {
		missingByID[m.ID] = m
	}
	resolved := make([]webTrackDTO, 0, len(p.TrackIDs))
	items := make([]webPlaylistItemDTO, 0, len(p.TrackIDs))
	missingCount := 0

	for idx, id := range p.TrackIDs {
		if db.IsMissingTrackID(id) {
			if m, ok := missingByID[id]; ok {
				mCopy := m
				missingCount++
				items = append(items, webPlaylistItemDTO{
					Index:   idx,
					Kind:    "missing",
					Missing: &mCopy,
				})
			}
			continue
		}
		if t, ok := byID[id]; ok {
			tCopy := t
			resolved = append(resolved, tCopy)
			items = append(items, webPlaylistItemDTO{
				Index: idx,
				Kind:  "track",
				Track: &tCopy,
			})
		}
	}

	return webPlaylistDTO{
		ID:                p.ID,
		Name:              p.Name,
		Description:       p.Description,
		TrackCount:        len(resolved),
		MissingCount:      missingCount,
		SpotifyPlaylistID: p.SpotifyPlaylistID,
		SpotifySourceURL:  p.SpotifySourceURL,
		Tracks:            resolved,
		Items:             items,
	}
}

func (s *Server) loadWebTrackLookup() map[string]webTrackDTO {
	tracks, _ := s.store.LoadAllTracks()
	statsMap := s.loadTrackStatsMap()
	analysisMap, _ := s.store.ListTrackAnalysisMap()
	edGenresByAlb := s.loadAlbumEditorialGenresMap()

	byID := make(map[string]webTrackDTO, len(tracks)*2)
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		enrichTrackDTO(&dto, tr, statsMap, analysisMap, edGenresByAlb)
		byID[dto.ID] = dto
		byID[tr.TrackKey] = dto
		if tr.LegacyID != "" {
			byID[tr.LegacyID] = dto
		}
	}
	return byID
}

// POST /api/v2/web/playlists/create
func (s *Server) handleWebPlaylistCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		TrackIDs    []string `json:"trackIDs"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 131_072)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Playlist name is required"})
		return
	}
	now := time.Now().UTC()
	pl := db.SharedPlaylist{
		ID:          uuid.NewString(),
		Name:        name,
		Description: strings.TrimSpace(req.Description),
		TrackIDs:    req.TrackIDs,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if pl.TrackIDs == nil {
		pl.TrackIDs = []string{}
	}
	if err := s.store.SavePlaylist(pl); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	byID := s.loadWebTrackLookup()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"playlist": s.buildWebPlaylistDTO(pl, byID),
	})
}

// POST /api/v2/web/playlists/update
func (s *Server) handleWebPlaylistUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		ID          string   `json:"id"`
		Action      string   `json:"action"` // "rename" | "addTrack" | "removeTrack" | "toggleTrack" | "removeMissing" | "clearMissing" | "addTracks" | "reorder"
		Name        string   `json:"name"`
		Description *string  `json:"description"`
		TrackID     string   `json:"trackId"`
		TrackIDs    []string `json:"trackIds"`
		MissingID   string   `json:"missingId"`
		Index       *int     `json:"index"`
		FromIndex   *int     `json:"fromIndex"`
		ToIndex     *int     `json:"toIndex"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 262_144)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	pl, err := s.store.GetPlaylist(strings.TrimSpace(req.ID))
	if err != nil || pl == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Playlist not found"})
		return
	}

	added := false
	removed := false

	switch strings.TrimSpace(req.Action) {
	case "rename", "":
		if n := strings.TrimSpace(req.Name); n != "" {
			pl.Name = n
		}
		if req.Description != nil {
			pl.Description = strings.TrimSpace(*req.Description)
		}
	case "addTrack":
		tid := strings.TrimSpace(req.TrackID)
		if tid == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing trackId"})
			return
		}
		exists := false
		for _, id := range pl.TrackIDs {
			if id == tid {
				exists = true
				break
			}
		}
		if !exists {
			pl.TrackIDs = append(pl.TrackIDs, tid)
			added = true
		}
	case "addTracks":
		existing := make(map[string]bool, len(pl.TrackIDs))
		for _, id := range pl.TrackIDs {
			existing[id] = true
		}
		for _, tid := range req.TrackIDs {
			tid = strings.TrimSpace(tid)
			if tid != "" && !existing[tid] {
				pl.TrackIDs = append(pl.TrackIDs, tid)
				existing[tid] = true
				added = true
			}
		}
	case "toggleTrack":
		tid := strings.TrimSpace(req.TrackID)
		if tid == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing trackId"})
			return
		}
		foundIdx := -1
		for i, id := range pl.TrackIDs {
			if id == tid {
				foundIdx = i
				break
			}
		}
		if foundIdx >= 0 {
			pl.TrackIDs = append(pl.TrackIDs[:foundIdx], pl.TrackIDs[foundIdx+1:]...)
			removed = true
		} else {
			pl.TrackIDs = append(pl.TrackIDs, tid)
			added = true
		}
	case "removeTrack":
		if req.Index != nil && *req.Index >= 0 && *req.Index < len(pl.TrackIDs) {
			remID := pl.TrackIDs[*req.Index]
			pl.TrackIDs = append(pl.TrackIDs[:*req.Index], pl.TrackIDs[*req.Index+1:]...)
			if db.IsMissingTrackID(remID) {
				pl.MissingTracks = filterOutMissing(pl.MissingTracks, remID)
			}
			removed = true
		} else if tid := strings.TrimSpace(req.TrackID); tid != "" {
			next := make([]string, 0, len(pl.TrackIDs))
			for _, id := range pl.TrackIDs {
				if id == tid {
					removed = true
					continue
				}
				next = append(next, id)
			}
			pl.TrackIDs = next
		}
	case "removeMissing":
		mid := strings.TrimSpace(req.MissingID)
		if mid != "" {
			next := make([]string, 0, len(pl.TrackIDs))
			for _, id := range pl.TrackIDs {
				if id == mid {
					removed = true
					continue
				}
				next = append(next, id)
			}
			pl.TrackIDs = next
			pl.MissingTracks = filterOutMissing(pl.MissingTracks, mid)
		}
	case "clearMissing":
		next := make([]string, 0, len(pl.TrackIDs))
		for _, id := range pl.TrackIDs {
			if db.IsMissingTrackID(id) {
				removed = true
				continue
			}
			next = append(next, id)
		}
		pl.TrackIDs = next
		pl.MissingTracks = nil
	case "reorder":
		if req.FromIndex == nil || req.ToIndex == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing fromIndex or toIndex"})
			return
		}
		from, to := *req.FromIndex, *req.ToIndex
		n := len(pl.TrackIDs)
		if from < 0 || from >= n || to < 0 || to >= n {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Reorder index out of range"})
			return
		}
		if from != to {
			moved := pl.TrackIDs[from]
			without := make([]string, 0, n-1)
			without = append(without, pl.TrackIDs[:from]...)
			without = append(without, pl.TrackIDs[from+1:]...)
			reordered := make([]string, 0, n)
			reordered = append(reordered, without[:to]...)
			reordered = append(reordered, moved)
			reordered = append(reordered, without[to:]...)
			pl.TrackIDs = reordered
		}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Unknown playlist action: " + req.Action})
		return
	}

	pl.UpdatedAt = time.Now().UTC()
	if err := s.store.SavePlaylist(*pl); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	byID := s.loadWebTrackLookup()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"added":    added,
		"removed":  removed,
		"playlist": s.buildWebPlaylistDTO(*pl, byID),
	})
}

func filterOutMissing(list []db.MissingTrack, targetID string) []db.MissingTrack {
	out := make([]db.MissingTrack, 0, len(list))
	for _, m := range list {
		if m.ID != targetID {
			out = append(out, m)
		}
	}
	return out
}

// POST /api/v2/web/playlists/delete
func (s *Server) handleWebPlaylistDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST or DELETE required"})
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		var req struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 16_384)).Decode(&req)
		id = strings.TrimSpace(req.ID)
	}
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing playlist id"})
		return
	}
	if err := s.store.DeletePlaylist(id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Playlist not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": id})
}

// POST /api/v2/web/playlists/spotify/preview
func (s *Server) handleSpotifyPlaylistPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.spotify == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Spotify importer unavailable"})
		return
	}
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 32_768)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	payload, err := s.spotify.FetchPlaylist(r.Context(), req.URL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tracks, _ := s.store.LoadAllTracks()
	report := spotify.MatchPlaylist(*payload, tracks)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":               true,
		"report":           report,
		"lidarrConfigured": s.lidarr != nil && s.lidarr.IsConfigured(),
	})
}

// POST /api/v2/web/playlists/spotify/import
func (s *Server) handleSpotifyPlaylistImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.spotify == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Spotify importer unavailable"})
		return
	}
	var req struct {
		URL              string `json:"url"`
		Name             string `json:"name"`
		Description      string `json:"description"`
		AutoSearchLidarr bool   `json:"autoSearchLidarr"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 65_536)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	payload, err := s.spotify.FetchPlaylist(r.Context(), req.URL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tracks, _ := s.store.LoadAllTracks()
	report := spotify.MatchPlaylist(*payload, tracks)
	pl := spotify.BuildSharedPlaylist(report, req.Name, req.Description)
	if err := s.store.SavePlaylist(pl); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if report.MissingCount > 0 {
		s.spotify.EnrichPlaylistArtworkAsync(pl.ID)
	}

	lidarrStarted := false
	var lidarrStatus *spotify.LidarrSearchStatus
	if req.AutoSearchLidarr && report.MissingCount > 0 && s.lidarr != nil && s.lidarr.IsConfigured() {
		if st, err := s.spotify.StartAutoLidarrSearch(pl.ID, "", true); err == nil {
			lidarrStarted = true
			lidarrStatus = &st
		}
	}

	byID := s.loadWebTrackLookup()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"playlist":      s.buildWebPlaylistDTO(pl, byID),
		"matchedCount":  report.MatchedCount,
		"missingCount":  report.MissingCount,
		"lidarrStarted": lidarrStarted,
		"lidarrStatus":  lidarrStatus,
	})
}

// POST /api/v2/web/playlists/spotify/resync
func (s *Server) handleSpotifyPlaylistResync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.spotify == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Spotify importer unavailable"})
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16_384)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	pl, err := s.store.GetPlaylist(strings.TrimSpace(req.ID))
	if err != nil || pl == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Playlist not found"})
		return
	}
	src := strings.TrimSpace(pl.SpotifySourceURL)
	if src == "" {
		src = strings.TrimSpace(pl.SpotifyPlaylistID)
	}
	if src == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Playlist was not imported from Spotify"})
		return
	}

	payload, err := s.spotify.FetchPlaylist(r.Context(), src)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	prevBySpotifyID := make(map[string]db.MissingTrack, len(pl.MissingTracks))
	for _, m := range pl.MissingTracks {
		if m.SpotifyTrackID != "" {
			prevBySpotifyID[m.SpotifyTrackID] = m
		}
	}

	tracks, _ := s.store.LoadAllTracks()
	report := spotify.MatchPlaylist(*payload, tracks)
	rebuilt := spotify.BuildSharedPlaylist(report, pl.Name, pl.Description)
	for i := range rebuilt.MissingTracks {
		sid := rebuilt.MissingTracks[i].SpotifyTrackID
		if prev, ok := prevBySpotifyID[sid]; ok {
			if rebuilt.MissingTracks[i].Album == "" {
				rebuilt.MissingTracks[i].Album = prev.Album
			}
			if rebuilt.MissingTracks[i].ArtworkURL == "" {
				rebuilt.MissingTracks[i].ArtworkURL = prev.ArtworkURL
			}
			rebuilt.MissingTracks[i].LidarrStatus = prev.LidarrStatus
			rebuilt.MissingTracks[i].LidarrAlbumTitle = prev.LidarrAlbumTitle
			rebuilt.MissingTracks[i].LidarrAlbumID = prev.LidarrAlbumID
			rebuilt.MissingTracks[i].LidarrStatusDetail = prev.LidarrStatusDetail
			rebuilt.MissingTracks[i].LastSearchedAt = prev.LastSearchedAt
		}
	}

	pl.TrackIDs = rebuilt.TrackIDs
	pl.MissingTracks = rebuilt.MissingTracks
	pl.SpotifyPlaylistID = payload.PlaylistID
	pl.SpotifySourceURL = payload.SourceURL
	pl.UpdatedAt = time.Now().UTC()
	if err := s.store.SavePlaylist(*pl); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if report.MissingCount > 0 {
		s.spotify.EnrichPlaylistArtworkAsync(pl.ID)
	}

	byID := s.loadWebTrackLookup()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"playlist":     s.buildWebPlaylistDTO(*pl, byID),
		"matchedCount": report.MatchedCount,
		"missingCount": report.MissingCount,
	})
}

// POST /api/v2/web/playlists/rematch
func (s *Server) handleWebPlaylistRematch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16_384)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	pl, err := s.store.GetPlaylist(strings.TrimSpace(req.ID))
	if err != nil || pl == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Playlist not found"})
		return
	}
	tracks, _ := s.store.LoadAllTracks()
	updated, resolvedCount := spotify.ResolveMissingInPlaylist(*pl, tracks)
	if resolvedCount > 0 {
		if err := s.store.SavePlaylist(updated); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	byID := s.loadWebTrackLookup()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"resolvedCount": resolvedCount,
		"playlist":      s.buildWebPlaylistDTO(updated, byID),
	})
}

// POST /api/v2/web/playlists/lidarr/search
func (s *Server) handleWebPlaylistLidarrSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.spotify == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Spotify importer unavailable"})
		return
	}
	var req struct {
		PlaylistID     string `json:"playlistId"`
		MissingTrackID string `json:"missingTrackId"`
		OnlyUnsearched *bool  `json:"onlyUnsearched"`
		Stop           bool   `json:"stop"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 32_768)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	if req.Stop {
		s.spotify.StopLidarrSearch()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"status": s.spotify.LidarrStatus(),
		})
		return
	}
	onlyUnsearched := true
	if req.OnlyUnsearched != nil {
		onlyUnsearched = *req.OnlyUnsearched
	}
	st, err := s.spotify.StartAutoLidarrSearch(strings.TrimSpace(req.PlaylistID), strings.TrimSpace(req.MissingTrackID), onlyUnsearched)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok":      true,
		"message": fmt.Sprintf("Started Lidarr auto-search for %d missing track(s)", st.Total),
		"status":  st,
	})
}

// GET /api/v2/web/playlists/lidarr/status
func (s *Server) handleWebPlaylistLidarrStatus(w http.ResponseWriter, r *http.Request) {
	if s.spotify == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": spotify.LidarrSearchStatus{}})
		return
	}
	st := s.spotify.LidarrStatus()
	resp := map[string]any{
		"configured": s.lidarr != nil && s.lidarr.IsConfigured(),
		"status":     st,
	}
	if pid := strings.TrimSpace(r.URL.Query().Get("playlistId")); pid != "" {
		if pl, err := s.store.GetPlaylist(pid); err == nil && pl != nil {
			byID := s.loadWebTrackLookup()
			resp["playlist"] = s.buildWebPlaylistDTO(*pl, byID)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
