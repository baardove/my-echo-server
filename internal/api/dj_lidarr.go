package api

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/dj"
	"github.com/baardove/my-echo/server/internal/lidarr"
)

// --- AI Radio DJ Endpoints ---

func (s *Server) handleDJState(w http.ResponseWriter, r *http.Request) {
	if s.dj == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DJ engine not initialized"})
		return
	}
	writeJSON(w, http.StatusOK, s.dj.State())
}

func (s *Server) handleDJOptions(w http.ResponseWriter, r *http.Request) {
	// Return top genres, smart analysis tags, eras, and DJ Radio profiles
	genres := s.topLibraryGenres(24)
	tags := s.topLibraryTags(24)
	djCfg := s.cfg.DJ
	config.NormalizeDJConfig(&djCfg)
	writeJSON(w, http.StatusOK, map[string]any{
		"ttsModels": dj.AvailableTTSModels(),
		"llmModels": dj.AvailableLLMModels(),
		"voices":    dj.AvailableVoices(),
		"personas":  dj.AvailablePersonas(),
		"languages": []string{"English", "Norwegian", "Auto"},
		"eras":      []string{"1950s", "1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "2020s"},
		"genres":    genres,
		"tags":      tags,
		"profiles":  djCfg.Profiles,
	})
}

func (s *Server) handleDJProfiles(w http.ResponseWriter, r *http.Request) {
	config.NormalizeDJConfig(&s.cfg.DJ)
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"profiles":        s.cfg.DJ.Profiles,
			"activeProfileId": s.cfg.DJ.ActiveProfileID,
		})
	case http.MethodPost:
		var prof config.DJProfile
		if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&prof); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid profile JSON"})
			return
		}
		prof.Name = strings.TrimSpace(prof.Name)
		if prof.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Profile name is required"})
			return
		}
		if strings.TrimSpace(prof.ID) == "" {
			prof.ID = slugifyProfileID(prof.Name)
		}
		updated := false
		for i := range s.cfg.DJ.Profiles {
			if strings.EqualFold(s.cfg.DJ.Profiles[i].ID, prof.ID) {
				s.cfg.DJ.Profiles[i] = prof
				updated = true
				break
			}
		}
		if !updated {
			s.cfg.DJ.Profiles = append(s.cfg.DJ.Profiles, prof)
		}
		_ = s.cfg.Save()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"profile":  prof,
			"profiles": s.cfg.DJ.Profiles,
		})
	case http.MethodDelete:
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing profile id"})
			return
		}
		var next []config.DJProfile
		for _, p := range s.cfg.DJ.Profiles {
			if !strings.EqualFold(p.ID, id) {
				next = append(next, p)
			}
		}
		if len(next) == 0 {
			next = config.DefaultDJProfiles()
		}
		s.cfg.DJ.Profiles = next
		_ = s.cfg.Save()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"profiles": s.cfg.DJ.Profiles,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET, POST, or DELETE required"})
	}
}

func slugifyProfileID(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else if r == ' ' || r == '-' || r == '_' {
			if b.Len() > 0 && b.String()[b.Len()-1] != '-' {
				b.WriteByte('-')
			}
		}
	}
	res := strings.Trim(b.String(), "-")
	if res == "" {
		return "dj-profile"
	}
	return res
}

func (s *Server) handleAdminAnalysisRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.analyzer == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Analyzer unavailable"})
		return
	}
	s.cfg.AdvancedAnalysisEnabled = true
	_ = s.cfg.Save()
	s.analyzer.TriggerRun()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok":      true,
		"message": "Advanced audio analysis started (BPM, vocal cue points & smart tags)",
		"status":  s.analyzer.Status(),
	})
}

func (s *Server) handleAdminAnalysisStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.analyzer != nil {
		s.analyzer.StopCurrentRun()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "Analysis paused"})
}

func (s *Server) topLibraryTags(limit int) []string {
	analysisMap, err := s.store.ListTrackAnalysisMap()
	if err != nil {
		return []string{}
	}
	counts := make(map[string]int)
	display := make(map[string]string)
	for _, an := range analysisMap {
		for _, t := range an.Tags {
			k := strings.ToLower(strings.TrimSpace(t))
			if k == "" {
				continue
			}
			counts[k]++
			if display[k] == "" {
				display[k] = t
			}
		}
	}
	type kv struct {
		name string
		n    int
	}
	var list []kv
	for k, n := range counts {
		list = append(list, kv{name: display[k], n: n})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].n != list[j].n {
			return list[i].n > list[j].n
		}
		return list[i].name < list[j].name
	})
	if len(list) > limit {
		list = list[:limit]
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, item.name)
	}
	return out
}

func (s *Server) handleDJSessionCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dj == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DJ engine not initialized"})
		return
	}
	var req dj.CreateSessionRequest
	_ = json.NewDecoder(io.LimitReader(r.Body, 262_144)).Decode(&req)
	st, err := s.dj.CreateSession(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleDJQueueAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dj == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DJ engine not initialized"})
		return
	}
	var req struct {
		TrackID  string   `json:"trackId"`
		TrackIDs []string `json:"trackIds"`
		AlbumKey string   `json:"albumKey"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 262_144)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	ids := req.TrackIDs
	if req.TrackID != "" {
		ids = append(ids, req.TrackID)
	}
	if req.AlbumKey != "" {
		target := strings.ToLower(strings.TrimSpace(req.AlbumKey))
		if tracks, err := s.store.LoadAllTracks(); err == nil {
			for _, tr := range tracks {
				if strings.ToLower(strings.TrimSpace(tr.Album)) == target {
					ids = append(ids, tr.EffectiveID())
				}
			}
		}
	}
	st, err := s.dj.AppendTracks(ids)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleDJQueueAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dj == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DJ engine not initialized"})
		return
	}
	var req struct {
		Action    string `json:"action"` // "advance", "remove", "move", "clear", "updateBreak"
		Index     int    `json:"index"`
		FromIndex int    `json:"fromIndex"`
		ToIndex   int    `json:"toIndex"`
		Mode      string `json:"mode"` // "intro", "outro", "bridge", "none", "keep"
		Regen     bool   `json:"regenerate"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 32_768)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	switch req.Action {
	case "advance":
		writeJSON(w, http.StatusOK, s.dj.SetCurrentIndex(req.Index))
	case "remove":
		writeJSON(w, http.StatusOK, s.dj.RemoveSlot(req.Index))
	case "move":
		st, err := s.dj.MoveSlot(req.FromIndex, req.ToIndex)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, st)
	case "clear":
		writeJSON(w, http.StatusOK, s.dj.ClearSession())
	case "updateBreak":
		st, err := s.dj.UpdateSlotBreak(req.Index, req.Mode, req.Regen)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, st)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Unknown DJ action: " + req.Action})
	}
}

func (s *Server) handleDJAudio(w http.ResponseWriter, r *http.Request) {
	if s.dj == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "DJ engine unavailable"})
		return
	}
	breakID := strings.TrimPrefix(r.URL.Path, "/api/v2/dj/audio/")
	breakID = strings.TrimSuffix(breakID, ".m4a")
	path, ok := s.dj.AudioPathForBreak(breakID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "DJ audio break not found"})
		return
	}
	if _, err := os.Stat(path); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "DJ audio file missing"})
		return
	}
	w.Header().Set("Content-Type", "audio/mp4")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	http.ServeFile(w, r, path)
}

func (s *Server) handleDJTestVoice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dj == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DJ engine unavailable"})
		return
	}
	var req config.DJConfig
	_ = json.NewDecoder(io.LimitReader(r.Body, 32_768)).Decode(&req)
	brk, err := s.dj.TestVoice(r.Context(), &req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"break": brk,
	})
}

func (s *Server) handleDJLiveModels(w http.ResponseWriter, r *http.Request) {
	if s.dj == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DJ engine unavailable"})
		return
	}
	var req struct {
		GeminiAPIKey string `json:"geminiAPIKey"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req)
	}
	tts, llm, err := s.dj.ListLiveGeminiModels(r.Context(), req.GeminiAPIKey)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"ttsModels": tts,
		"llmModels": llm,
	})
}

func (s *Server) topLibraryGenres(limit int) []string {
	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		return []string{}
	}
	counts := make(map[string]int)
	display := make(map[string]string)
	for _, tr := range tracks {
		if tr.Genre == nil {
			continue
		}
		g := strings.TrimSpace(*tr.Genre)
		if g == "" || strings.EqualFold(g, "unknown") {
			continue
		}
		k := strings.ToLower(g)
		counts[k]++
		if display[k] == "" {
			display[k] = g
		}
	}
	type kv struct {
		name string
		n    int
	}
	var list []kv
	for k, n := range counts {
		if n >= 3 {
			list = append(list, kv{name: display[k], n: n})
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].n != list[j].n {
			return list[i].n > list[j].n
		}
		return list[i].name < list[j].name
	})
	if len(list) > limit {
		list = list[:limit]
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, item.name)
	}
	return out
}

// --- Interactive Lidarr Endpoints ---

func (s *Server) handleLidarrStatus(w http.ResponseWriter, r *http.Request) {
	if s.lidarr == nil {
		writeJSON(w, http.StatusOK, lidarr.ConnectionStatus{})
		return
	}
	if r.URL.Query().Get("test") == "1" {
		writeJSON(w, http.StatusOK, s.lidarr.TestConnection(r.Context()))
		return
	}
	writeJSON(w, http.StatusOK, s.lidarr.Status())
}

func (s *Server) handleLidarrRegisterWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.lidarr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Lidarr manager unavailable"})
		return
	}
	msg, err := s.lidarr.RegisterWebhook(r.Context())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": msg,
		"status":  s.lidarr.Status(),
	})
}

func maskSecret(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "••••••••"
	}
	return s[:4] + "••••••" + s[len(s)-4:]
}
