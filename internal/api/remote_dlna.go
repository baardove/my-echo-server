package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo-server/internal/admin"
	"github.com/baardove/my-echo-server/internal/config"
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

// handleAuthLogin verifies either a managed user (username + password) OR the Admin Pairing PIN
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
		Username string `json:"username"`
		Password string `json:"password"`
		PIN      string `json:"pin"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}

	isLAN := admin.IsLANRequest(r)
	hasUsers := len(s.cfg.Users) > 0
	var claims admin.SessionClaims
	authed := false

	username := strings.TrimSpace(req.Username)
	pin := strings.TrimSpace(req.PIN)

	if username != "" {
		if u, ok := s.cfg.AuthenticateUser(username, req.Password); ok {
			claims = admin.ClaimsFromUser(u, isLAN, hasUsers)
			authed = true
		}
	} else if pin != "" {
		if subtle.ConstantTimeCompare([]byte(pin), []byte(s.cfg.PairingPIN)) == 1 {
			claims = admin.AdminClaims(isLAN, hasUsers)
			authed = true
		}
	}

	if !authed {
		loginMu.Lock()
		b = loginBuckets[ip]
		if b == nil || now.After(b.resetAt) {
			b = &loginAttemptBucket{resetAt: now.Add(5 * time.Minute)}
			loginBuckets[ip] = b
		}
		b.count++
		loginMu.Unlock()
		msg := "Incorrect Pairing PIN"
		if username != "" {
			msg = "Invalid username or password"
		}
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": msg})
		return
	}

	loginMu.Lock()
	delete(loginBuckets, ip)
	loginMu.Unlock()

	isHTTPS := r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
	http.SetCookie(w, &http.Cookie{
		Name:     admin.SessionCookieName,
		Value:    admin.SignSessionCookie(claims, s.cfg.PairingPIN),
		Path:     "/",
		HttpOnly: true,
		Secure:   isHTTPS,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   30 * 86400,
	})
	// Clear any explicit sign-out marker so the freshly authenticated session takes effect.
	http.SetCookie(w, &http.Cookie{
		Name:     admin.LoggedOutCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   isHTTPS,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user": claims})
}

func (s *Server) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	isHTTPS := r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
	http.SetCookie(w, &http.Cookie{
		Name:     admin.SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   isHTTPS,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	// Keep the browser signed out on LAN (prevent auto-Admin re-login on Back / visiting "/")
	// until the user explicitly signs in again.
	http.SetCookie(w, &http.Cookie{
		Name:     admin.LoggedOutCookieName,
		Value:    "1",
		Path:     "/",
		HttpOnly: true,
		Secure:   isHTTPS,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   7 * 86400,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleAuthMe(w http.ResponseWriter, r *http.Request) {
	claims := s.sessionFromRequest(r)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user": claims})
}

// handleAdminUsers manages user accounts (Admin only). All users share the global Gemini API key.
func (s *Server) handleAdminUsers(w http.ResponseWriter, r *http.Request) {
	type publicUser struct {
		ID              string `json:"id"`
		Username        string `json:"username"`
		DisplayName     string `json:"displayName"`
		Role            string `json:"role"`
		AllowLibrary    bool   `json:"allowLibrary"`
		AllowDJ         bool   `json:"allowDJ"`
		AllowParty      bool   `json:"allowParty"`
		CanEditProfiles bool   `json:"canEditProfiles"`
		CreatedAt       string `json:"createdAt"`
		LoginURL        string `json:"loginURL,omitempty"`
	}
	pubBase := s.activePublicBaseURL(r)
	serialize := func() []publicUser {
		out := make([]publicUser, 0, len(s.cfg.Users))
		for _, u := range s.cfg.Users {
			loginURL := ""
			if pubBase != "" {
				loginURL = pubBase + "/u/" + url.PathEscape(u.Username)
			}
			out = append(out, publicUser{
				ID:              u.ID,
				Username:        u.Username,
				DisplayName:     u.DisplayName,
				Role:            u.Role,
				AllowLibrary:    u.AllowLibrary,
				AllowDJ:         u.AllowDJ,
				AllowParty:      u.AllowParty,
				CanEditProfiles: u.CanEditProfiles,
				CreatedAt:       u.CreatedAt,
				LoginURL:        loginURL,
			})
		}
		return out
	}

	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":                    true,
			"users":                 serialize(),
			"authRequireLoginOnLAN": s.cfg.AuthRequireLoginOnLAN,
		})
	case http.MethodPost:
		var req struct {
			ID                    string `json:"id"`
			Username              string `json:"username"`
			DisplayName           string `json:"displayName"`
			Password              string `json:"password"`
			Role                  string `json:"role"`
			AllowLibrary          *bool  `json:"allowLibrary"`
			AllowDJ               *bool  `json:"allowDJ"`
			AllowParty            *bool  `json:"allowParty"`
			CanEditProfiles       *bool  `json:"canEditProfiles"`
			AuthRequireLoginOnLAN *bool  `json:"authRequireLoginOnLAN"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 32_768)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
			return
		}
		// Allow toggling AuthRequireLoginOnLAN alone
		if req.AuthRequireLoginOnLAN != nil && strings.TrimSpace(req.Username) == "" && strings.TrimSpace(req.ID) == "" {
			s.cfg.AuthRequireLoginOnLAN = *req.AuthRequireLoginOnLAN
			_ = s.cfg.Save()
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":                    true,
				"users":                 serialize(),
				"authRequireLoginOnLAN": s.cfg.AuthRequireLoginOnLAN,
			})
			return
		}

		uname := strings.TrimSpace(req.Username)
		if uname == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Username is required"})
			return
		}
		role := strings.ToLower(strings.TrimSpace(req.Role))
		if role != "admin" {
			role = "user"
		}
		disp := strings.TrimSpace(req.DisplayName)
		if disp == "" {
			disp = uname
		}
		allowLib := true
		if req.AllowLibrary != nil {
			allowLib = *req.AllowLibrary
		}
		allowDJ := true
		if req.AllowDJ != nil {
			allowDJ = *req.AllowDJ
		}
		allowParty := true
		if req.AllowParty != nil {
			allowParty = *req.AllowParty
		}
		canEditProf := false
		if req.CanEditProfiles != nil {
			canEditProf = *req.CanEditProfiles
		}
		if role == "admin" {
			allowLib, allowDJ, allowParty, canEditProf = true, true, true, true
		}

		if strings.TrimSpace(req.ID) != "" {
			u := s.cfg.FindUserByID(strings.TrimSpace(req.ID))
			if u == nil {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "User not found"})
				return
			}
			// Check duplicate username against other users
			if other := s.cfg.FindUserByUsername(uname); other != nil && other.ID != u.ID {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Username already exists"})
				return
			}
			u.Username = uname
			u.DisplayName = disp
			u.Role = role
			u.AllowLibrary = allowLib
			u.AllowDJ = allowDJ
			u.AllowParty = allowParty
			u.CanEditProfiles = canEditProf
			if req.Password != "" {
				u.PassSalt = config.NewSalt()
				u.PassHash = config.HashPassword(u.PassSalt, req.Password)
			}
		} else {
			if s.cfg.FindUserByUsername(uname) != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Username already exists"})
				return
			}
			if len(req.Password) < 3 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Password must be at least 3 characters"})
				return
			}
			var idBytes [8]byte
			_, _ = rand.Read(idBytes[:])
			salt := config.NewSalt()
			s.cfg.Users = append(s.cfg.Users, config.UserAccount{
				ID:              "usr_" + hex.EncodeToString(idBytes[:]),
				Username:        uname,
				DisplayName:     disp,
				Role:            role,
				PassSalt:        salt,
				PassHash:        config.HashPassword(salt, req.Password),
				AllowLibrary:    allowLib,
				AllowDJ:         allowDJ,
				AllowParty:      allowParty,
				CanEditProfiles: canEditProf,
				CreatedAt:       time.Now().UTC().Format(time.RFC3339),
			})
		}
		if req.AuthRequireLoginOnLAN != nil {
			s.cfg.AuthRequireLoginOnLAN = *req.AuthRequireLoginOnLAN
		}
		if err := s.cfg.Save(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":                    true,
			"users":                 serialize(),
			"authRequireLoginOnLAN": s.cfg.AuthRequireLoginOnLAN,
		})
	case http.MethodDelete:
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing user id"})
			return
		}
		next := make([]config.UserAccount, 0, len(s.cfg.Users))
		for _, u := range s.cfg.Users {
			if u.ID != id {
				next = append(next, u)
			}
		}
		s.cfg.Users = next
		_ = s.cfg.Save()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":                    true,
			"users":                 serialize(),
			"authRequireLoginOnLAN": s.cfg.AuthRequireLoginOnLAN,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET, POST, or DELETE required"})
	}
}

func (s *Server) handleRemoteStatus(w http.ResponseWriter, r *http.Request) {
	claims := s.sessionFromRequest(r)
	out := map[string]any{
		"ok": true,
		"remoteConfig": map[string]any{
			"cloudflareEnabled":      s.cfg.Remote.CloudflareEnabled,
			"cloudflareMode":         s.cfg.Remote.CloudflareMode,
			"cloudflarePublicDomain": s.cfg.Remote.CloudflarePublicDomain,
			"hasCloudflareToken":     strings.TrimSpace(s.cfg.Remote.CloudflareToken) != "",
			"cloudflareTokenMask":    maskSecret(s.cfg.Remote.CloudflareToken),
			"publicPartyAccess":      s.cfg.Remote.PublicPartyAccess,
			"playerURLAccess":        s.cfg.Remote.PlayerURLAccess,
			"radioDJURLAccess":       s.cfg.Remote.RadioDJURLAccess,
			"allowUserLogin":         s.cfg.Remote.AllowUserLogin,
			"allowPINLogin":          s.cfg.Remote.AllowPINLogin,
			"cloudflareExposeDJ":     s.cfg.Remote.CloudflareExposeDJ,
			"cloudflareExposeParty":  s.cfg.Remote.CloudflareExposeParty,
			"upnpEnabled":            s.cfg.Remote.UPnPEnabled,
			"upnpExposeParty":        s.cfg.Remote.UPnPExposeParty,
			"upnpExposeDJ":           s.cfg.Remote.UPnPExposeDJ,
			"upnpExternalParty":      s.cfg.Remote.UPnPExternalParty,
			"upnpExternalDJ":         s.cfg.Remote.UPnPExternalDJ,
			"upnpGatewayOverride":    s.cfg.Remote.UPnPGatewayOverride,
			"requirePINOnRemote":     s.cfg.Remote.RequirePINOnRemote,
		},
		"dlnaConfig": s.cfg.DLNA,
	}
	if claims.Role == "admin" {
		out["urls"] = s.buildRemoteURLs(r)
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
