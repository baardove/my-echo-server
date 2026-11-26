package api

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo/server/internal/admin"
)

type loginAttemptBucket struct {
	count   int
	resetAt time.Time
}

var (
	loginMu      sync.Mutex
	loginBuckets = make(map[string]*loginAttemptBucket)
)

func clientIPKey(r *http.Request) string {
	if cf := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); cf != "" {
		return cf
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return h
	}
	return r.RemoteAddr
}

// handleAuthLogin verifies the Pairing PIN for remote visitors (Cloudflare Tunnel / UPnP)
// with per-IP brute-force rate limiting (max 10 failures per 5 minutes) and sets an HttpOnly session cookie.
func (s *Server) handleAuthLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	ip := clientIPKey(r)
	now := time.Now()
	loginMu.Lock()
	b := loginBuckets[ip]
	if b != nil && now.Before(b.resetAt) && b.count >= 10 {
		loginMu.Unlock()
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "Too many failed attempts — try again in a few minutes"})
		return
	}
	loginMu.Unlock()

	var req struct {
		PIN string `json:"pin"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	pin := strings.TrimSpace(req.PIN)
	if subtle.ConstantTimeCompare([]byte(pin), []byte(s.cfg.PairingPIN)) != 1 {
		loginMu.Lock()
		b = loginBuckets[ip]
		if b == nil || now.After(b.resetAt) {
			b = &loginAttemptBucket{resetAt: now.Add(5 * time.Minute)}
			loginBuckets[ip] = b
		}
		b.count++
		loginMu.Unlock()
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Incorrect Pairing PIN"})
		return
	}

	loginMu.Lock()
	delete(loginBuckets, ip)
	loginMu.Unlock()

	isHTTPS := r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
	http.SetCookie(w, &http.Cookie{
		Name:     admin.SessionCookieName,
		Value:    admin.SessionCookieValue(s.cfg.PairingPIN),
		Path:     "/",
		HttpOnly: true,
		Secure:   isHTTPS,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   30 * 86400,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleRemoteStatus(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{
		"ok": true,
		"remoteConfig": map[string]any{
			"cloudflareEnabled":     s.cfg.Remote.CloudflareEnabled,
			"cloudflareMode":        s.cfg.Remote.CloudflareMode,
			"hasCloudflareToken":    strings.TrimSpace(s.cfg.Remote.CloudflareToken) != "",
			"cloudflareTokenMask":   maskSecret(s.cfg.Remote.CloudflareToken),
			"cloudflareExposeDJ":    s.cfg.Remote.CloudflareExposeDJ,
			"cloudflareExposeParty": s.cfg.Remote.CloudflareExposeParty,
			"upnpEnabled":           s.cfg.Remote.UPnPEnabled,
			"upnpExposeParty":       s.cfg.Remote.UPnPExposeParty,
			"upnpExposeDJ":          s.cfg.Remote.UPnPExposeDJ,
			"upnpExternalParty":     s.cfg.Remote.UPnPExternalParty,
			"upnpExternalDJ":        s.cfg.Remote.UPnPExternalDJ,
			"upnpGatewayOverride":   s.cfg.Remote.UPnPGatewayOverride,
			"requirePINOnRemote":    s.cfg.Remote.RequirePINOnRemote,
		},
		"dlnaConfig": s.cfg.DLNA,
	}
	if s.broadcast != nil {
		out["broadcast"] = s.broadcast.Stats()
	}
	if s.dlna != nil {
		out["dlna"] = s.dlna.Status()
	}
	if s.netmap != nil {
		out["upnp"] = s.netmap.Status()
	}
	if s.tunnel != nil {
		out["tunnel"] = s.tunnel.Status()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRemoteUPnPSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.netmap == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "UPnP mapper unavailable"})
		return
	}
	st := s.netmap.Sync(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "upnp": st})
}

func (s *Server) handleRemoteTunnelRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.tunnel == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Cloudflare tunnel unavailable"})
		return
	}
	s.tunnel.Reconcile()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "tunnel": s.tunnel.Status()})
}

func (s *Server) handleDLNADiscover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dlna == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DLNA manager unavailable"})
		return
	}
	list, err := s.dlna.DiscoverRenderers(r.Context(), 2200*time.Millisecond)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "renderers": list, "dlna": s.dlna.Status()})
}

func (s *Server) handleDLNACast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dlna == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DLNA manager unavailable"})
		return
	}
	var req struct {
		RendererID string `json:"rendererId"`
		Channel    string `json:"channel"` // "dj" | "party"
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16_384)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	rObj, err := s.dlna.CastStream(r.Context(), req.RendererID, req.Channel)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "renderer": rObj, "dlna": s.dlna.Status()})
}

func (s *Server) handleDLNAStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.dlna == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "DLNA manager unavailable"})
		return
	}
	var req struct {
		RendererID string `json:"rendererId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16_384)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	if err := s.dlna.StopRenderer(r.Context(), req.RendererID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "dlna": s.dlna.Status()})
}
