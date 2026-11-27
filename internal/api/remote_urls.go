package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/baardove/my-echo-server/internal/admin"
	"github.com/baardove/my-echo-server/internal/artwork"
	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
)

func (s *Server) adminHandler() *admin.Handler {
	if h, ok := s.adminUI.(*admin.Handler); ok {
		return h
	}
	return nil
}

func constantTimeTokenMatch(got, want string) bool {
	got = strings.TrimSpace(got)
	want = strings.TrimSpace(want)
	if got == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func isHTTPSRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https") {
		return true
	}
	if cfv := strings.ToLower(strings.TrimSpace(r.Header.Get("CF-Visitor"))); strings.Contains(cfv, `"https"`) {
		return true
	}
	return false
}

func (s *Server) activePublicBaseURL(r *http.Request) string {
	if s.tunnel != nil {
		if u := strings.TrimRight(strings.TrimSpace(s.tunnel.ActiveBaseURL()), "/"); u != "" {
			return u
		}
	}
	if u := s.cfg.Remote.NormalizedPublicBaseURL(); u != "" {
		return u
	}
	if r != nil && !admin.IsLANRequest(r) && strings.TrimSpace(r.Host) != "" {
		scheme := "https"
		if !isHTTPSRequest(r) {
			scheme = "http"
		}
		return scheme + "://" + strings.TrimSpace(r.Host)
	}
	return ""
}

func (s *Server) requestOriginBaseURL(r *http.Request) string {
	if base := s.activePublicBaseURL(r); base != "" && !admin.IsLANRequest(r) {
		return base
	}
	if r != nil && strings.TrimSpace(r.Host) != "" {
		scheme := "http"
		if isHTTPSRequest(r) {
			scheme = "https"
		}
		return scheme + "://" + strings.TrimSpace(r.Host)
	}
	return fmt.Sprintf("http://%s:%d", s.cfg.EffectiveAdvertiseHost(), s.cfg.APIPort)
}

func (s *Server) buildRemoteURLs(r *http.Request) map[string]any {
	lanBase := fmt.Sprintf("http://%s:%d", s.cfg.EffectiveAdvertiseHost(), s.cfg.APIPort)
	pubBase := s.activePublicBaseURL(r)

	buildSet := func(base string) map[string]string {
		if base == "" {
			return map[string]string{}
		}
		out := map[string]string{
			"base":  base,
			"login": base + "/login",
			"admin": base + "/admin",
		}
		switch s.cfg.Remote.PlayerURLAccess {
		case config.RemoteAccessPublic:
			out["player"] = base + "/player"
		case config.RemoteAccessToken:
			out["player"] = base + "/player/" + url.PathEscape(s.cfg.Remote.PlayerShareToken)
		case config.RemoteAccessLogin:
			out["player"] = base + "/player"
		}
		switch s.cfg.Remote.RadioDJURLAccess {
		case config.RemoteAccessPublic:
			out["radioPlayer"] = base + "/radio"
			out["radioStream"] = base + "/stream/dj.mp3"
			out["radioM3U"] = base + "/stream/dj.m3u"
		case config.RemoteAccessToken:
			tok := url.PathEscape(s.cfg.Remote.RadioDJShareToken)
			out["radioPlayer"] = base + "/radio/" + tok
			out["radioStream"] = base + "/stream/dj/" + tok + ".mp3"
			out["radioM3U"] = base + "/stream/dj/" + tok + ".m3u"
		case config.RemoteAccessLogin:
			out["radioPlayer"] = base + "/radio"
			out["radioStream"] = base + "/stream/dj.mp3"
			out["radioM3U"] = base + "/stream/dj.m3u"
		}
		switch s.cfg.Remote.PublicPartyAccess {
		case config.RemoteAccessPublic:
			out["party"] = base + "/party"
		case config.RemoteAccessToken:
			out["party"] = base + "/party/" + url.PathEscape(s.cfg.Remote.PartyShareToken)
		case config.RemoteAccessLogin:
			out["party"] = base + "/party"
		}
		return out
	}

	return map[string]any{
		"publicBaseURL": pubBase,
		"lanBaseURL":    lanBase,
		"public":        buildSet(pubBase),
		"lan":           buildSet(lanBase),
	}
}

func (s *Server) handleRemoteRotateToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Target string `json:"target"` // "player" | "radio" | "party" | "all"
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	switch strings.ToLower(strings.TrimSpace(req.Target)) {
	case "player":
		s.cfg.Remote.PlayerShareToken = config.GenerateURLToken(18)
	case "radio":
		s.cfg.Remote.RadioDJShareToken = config.GenerateURLToken(18)
	case "party":
		s.cfg.Remote.PartyShareToken = config.GenerateURLToken(18)
	case "all":
		s.cfg.Remote.PlayerShareToken = config.GenerateURLToken(18)
		s.cfg.Remote.RadioDJShareToken = config.GenerateURLToken(18)
		s.cfg.Remote.PartyShareToken = config.GenerateURLToken(18)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Unknown target (expected player, radio, party, or all)"})
		return
	}
	_ = s.cfg.Save()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"urls": s.buildRemoteURLs(r),
	})
}

// --- Single-Domain Player via URL (/player and /player/<token>) ---

func (s *Server) handlePlayerPage(w http.ResponseWriter, r *http.Request) {
	ah := s.adminHandler()
	if ah == nil {
		http.NotFound(w, r)
		return
	}
	tok := strings.Trim(strings.TrimPrefix(r.URL.Path, "/player"), "/")
	if strings.Contains(tok, "/") {
		http.NotFound(w, r)
		return
	}
	isLAN := admin.IsLANRequest(r)
	claims, hasCookie := admin.ParseSessionCookie(r, s.cfg)

	if isLAN {
		if !hasCookie {
			claims = admin.PlayerClaims(true)
		}
		ah.RenderWebAppWithClaims(w, r, claims)
		return
	}

	setPlayerCookie := func() admin.SessionClaims {
		pc := admin.PlayerClaims(false)
		http.SetCookie(w, &http.Cookie{
			Name:     admin.SessionCookieName,
			Value:    admin.SignPlayerCookie(s.cfg),
			Path:     "/",
			HttpOnly: true,
			Secure:   isHTTPSRequest(r),
			SameSite: http.SameSiteLaxMode,
			MaxAge:   7 * 86400,
		})
		return pc
	}

	switch s.cfg.Remote.PlayerURLAccess {
	case config.RemoteAccessOff:
		if hasCookie && (claims.Role == "admin" || claims.Role == "user") {
			ah.RenderWebAppWithClaims(w, r, claims)
			return
		}
		http.Error(w, "Remote player URL access is disabled", http.StatusForbidden)
	case config.RemoteAccessLogin:
		if !hasCookie || (claims.Role != "admin" && claims.Role != "user") {
			http.Redirect(w, r, "/login?next=/player", http.StatusFound)
			return
		}
		ah.RenderWebAppWithClaims(w, r, claims)
	case config.RemoteAccessToken:
		if tok != "" && constantTimeTokenMatch(tok, s.cfg.Remote.PlayerShareToken) {
			if !hasCookie || (claims.Role != "admin" && claims.Role != "user") {
				claims = setPlayerCookie()
			}
			ah.RenderWebAppWithClaims(w, r, claims)
			return
		}
		if hasCookie {
			ah.RenderWebAppWithClaims(w, r, claims)
			return
		}
		if tok != "" {
			http.Error(w, "Invalid or expired player URL", http.StatusForbidden)
			return
		}
		http.Redirect(w, r, "/login?next=/player", http.StatusFound)
	case config.RemoteAccessPublic:
		if !hasCookie {
			claims = setPlayerCookie()
		}
		ah.RenderWebAppWithClaims(w, r, claims)
	default:
		http.Error(w, "Forbidden", http.StatusForbidden)
	}
}

// --- Single-Domain Party Mode (/party and /party/<token>) ---

func (s *Server) handlePartyPage(w http.ResponseWriter, r *http.Request) {
	tok := strings.Trim(strings.TrimPrefix(r.URL.Path, "/party"), "/")
	if strings.Contains(tok, "/") {
		http.NotFound(w, r)
		return
	}
	if admin.IsLANRequest(r) {
		s.party.ServeGuestUI(w, r)
		return
	}
	claims, hasCookie := admin.ParseSessionCookie(r, s.cfg)
	switch s.cfg.Remote.PublicPartyAccess {
	case config.RemoteAccessPublic:
		s.party.ServeGuestUI(w, r)
	case config.RemoteAccessToken:
		if tok != "" && constantTimeTokenMatch(tok, s.cfg.Remote.PartyShareToken) {
			http.SetCookie(w, &http.Cookie{
				Name:     admin.PartyAccessCookieName,
				Value:    admin.SignPartyAccessCookie(s.cfg),
				Path:     "/",
				HttpOnly: true,
				Secure:   isHTTPSRequest(r),
				SameSite: http.SameSiteLaxMode,
				MaxAge:   7 * 86400,
			})
			s.party.ServeGuestUI(w, r)
			return
		}
		if admin.HasValidPartyAccessCookie(r, s.cfg) || (hasCookie && (claims.Role == "admin" || claims.Role == "user")) {
			s.party.ServeGuestUI(w, r)
			return
		}
		http.Error(w, "Valid Party Mode share link required", http.StatusForbidden)
	case config.RemoteAccessLogin:
		if hasCookie && (claims.Role == "admin" || claims.Role == "user") {
			s.party.ServeGuestUI(w, r)
			return
		}
		http.Redirect(w, r, "/login?next=/party", http.StatusFound)
	default:
		if hasCookie && claims.Role == "admin" {
			s.party.ServeGuestUI(w, r)
			return
		}
		http.Error(w, "Remote Party Mode access is disabled", http.StatusForbidden)
	}
}

func (s *Server) requirePartyAccess(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if admin.IsLANRequest(r) {
			next.ServeHTTP(w, r)
			return
		}
		if claims, ok := s.authenticateRequest(r); ok && (claims.Role == "admin" || claims.Role == "user") {
			next.ServeHTTP(w, r)
			return
		}
		switch s.cfg.Remote.PublicPartyAccess {
		case config.RemoteAccessPublic:
			next.ServeHTTP(w, r)
			return
		case config.RemoteAccessToken:
			if admin.HasValidPartyAccessCookie(r, s.cfg) || constantTimeTokenMatch(r.URL.Query().Get("k"), s.cfg.Remote.PartyShareToken) {
				next.ServeHTTP(w, r)
				return
			}
		}
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Party Mode remote access requires a valid link or sign-in"})
	})
}

// --- URL-Only Radio DJ Streaming Player (/radio, /radio/<token>, /stream/dj..., /api/public/radio/...) ---

func (s *Server) isRadioAccessAuthorized(r *http.Request, urlToken string) (bool, string) {
	if admin.IsLANRequest(r) {
		return true, urlToken
	}
	if claims, ok := s.authenticateRequest(r); ok && (claims.Role == "admin" || claims.AllowDJ) {
		return true, urlToken
	}
	qTok := strings.TrimSpace(r.URL.Query().Get("token"))
	if qTok == "" {
		qTok = strings.TrimSpace(r.URL.Query().Get("k"))
	}
	cand := strings.TrimSpace(urlToken)
	if cand == "" {
		cand = qTok
	}
	switch s.cfg.Remote.RadioDJURLAccess {
	case config.RemoteAccessPublic:
		return true, cand
	case config.RemoteAccessToken:
		if constantTimeTokenMatch(cand, s.cfg.Remote.RadioDJShareToken) {
			return true, s.cfg.Remote.RadioDJShareToken
		}
		return false, ""
	case config.RemoteAccessLogin:
		return false, ""
	default:
		return false, ""
	}
}

func (s *Server) handleRadioPlayerPage(w http.ResponseWriter, r *http.Request) {
	ah := s.adminHandler()
	if ah == nil {
		http.NotFound(w, r)
		return
	}
	tok := strings.Trim(strings.TrimPrefix(r.URL.Path, "/radio"), "/")
	if strings.Contains(tok, "/") {
		http.NotFound(w, r)
		return
	}
	ok, effTok := s.isRadioAccessAuthorized(r, tok)
	if !ok {
		if s.cfg.Remote.RadioDJURLAccess == config.RemoteAccessLogin {
			http.Redirect(w, r, "/login?next=/radio", http.StatusFound)
			return
		}
		http.Error(w, "Valid Radio DJ streaming URL required", http.StatusForbidden)
		return
	}
	renderTok := ""
	if tok != "" {
		renderTok = effTok
	}
	ah.RenderRadioPlayer(w, r, renderTok)
}

func (s *Server) handleDJStreamRoute(w http.ResponseWriter, r *http.Request) {
	if s.broadcast == nil {
		http.Error(w, "Broadcast hub unavailable", http.StatusServiceUnavailable)
		return
	}
	path := r.URL.Path
	switch {
	case path == "/stream/dj.mp3":
		if ok, _ := s.isRadioAccessAuthorized(r, ""); !ok {
			http.Error(w, "Radio DJ stream requires a valid streaming URL", http.StatusForbidden)
			return
		}
		s.broadcast.ServeDJStream(w, r)
	case path == "/stream/dj.m3u":
		if ok, _ := s.isRadioAccessAuthorized(r, ""); !ok {
			http.Error(w, "Radio DJ playlist requires a valid streaming URL", http.StatusForbidden)
			return
		}
		s.broadcast.ServeDJM3U(w, r, s.requestOriginBaseURL(r)+"/stream/dj.mp3")
	case strings.HasPrefix(path, "/stream/dj/"):
		tail := strings.TrimPrefix(path, "/stream/dj/")
		if strings.HasSuffix(tail, ".mp3") {
			tok := strings.TrimSuffix(tail, ".mp3")
			if ok, _ := s.isRadioAccessAuthorized(r, tok); !ok {
				http.Error(w, "Invalid Radio DJ stream URL", http.StatusForbidden)
				return
			}
			s.broadcast.ServeDJStream(w, r)
			return
		}
		if strings.HasSuffix(tail, ".m3u") {
			tok := strings.TrimSuffix(tail, ".m3u")
			if ok, _ := s.isRadioAccessAuthorized(r, tok); !ok {
				http.Error(w, "Invalid Radio DJ playlist URL", http.StatusForbidden)
				return
			}
			s.broadcast.ServeDJM3U(w, r, s.requestOriginBaseURL(r)+"/stream/dj/"+url.PathEscape(tok)+".mp3")
			return
		}
		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handlePublicRadioAPI(w http.ResponseWriter, r *http.Request) {
	tail := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/public/radio/"), "/")
	parts := strings.Split(tail, "/")

	var tok, action, artHash string
	switch {
	case len(parts) == 1 && parts[0] == "now":
		action = "now"
	case len(parts) == 2 && parts[0] == "artwork":
		action = "artwork"
		artHash = parts[1]
	case len(parts) == 2 && parts[1] == "now":
		tok = parts[0]
		action = "now"
	case len(parts) == 3 && parts[1] == "artwork":
		tok = parts[0]
		action = "artwork"
		artHash = parts[2]
	default:
		http.NotFound(w, r)
		return
	}

	ok, _ := s.isRadioAccessAuthorized(r, tok)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Unauthorized radio access"})
		return
	}

	if action == "now" {
		if s.broadcast == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Broadcast hub unavailable"})
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, s.broadcast.RadioStatus())
		return
	}

	if !artwork.IsSafeHash(artHash) {
		http.NotFound(w, r)
		return
	}
	data, err := s.artCache.LoadThumb(artHash)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(data)
}

func (s *Server) handleWebPlayed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		TrackID string `json:"trackId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil || strings.TrimSpace(req.TrackID) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing trackId"})
		return
	}
	tid := strings.TrimSpace(req.TrackID)
	now := time.Now().UTC()
	curCount := 0
	if list, err := s.store.ListTrackStats(); err == nil {
		for _, st := range list {
			if st.TrackID == tid {
				curCount = st.UserPlayCount
				break
			}
		}
	}
	nextCount := curCount + 1
	_ = s.store.MergeTrackStats([]db.SharedTrackStats{{
		TrackID:       tid,
		UserPlayCount: nextCount,
		LastPlayedAt:  &now,
		UpdatedAt:     now,
	}})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "userPlayCount": nextCount})
}
