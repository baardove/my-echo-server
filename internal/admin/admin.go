package admin

import (
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/indexer"
	"github.com/baardove/my-echo-server/internal/party"
)

//go:embed icon.png
var appIconPNG []byte

//go:embed webui.html
var webUIHTML []byte

//go:embed radio_player.html
var radioPlayerHTML []byte

const (
	SessionCookieName     = "echo_session"
	LoggedOutCookieName   = "echo_logged_out"
	PartyAccessCookieName = "echo_party_access"
)

// IsExplicitlyLoggedOut reports whether the browser explicitly clicked Sign Out.
func IsExplicitlyLoggedOut(r *http.Request) bool {
	c, err := r.Cookie(LoggedOutCookieName)
	return err == nil && c != nil && c.Value == "1"
}

// SessionClaims identifies the signed-in browser session and its effective permissions.
type SessionClaims struct {
	UserID          string `json:"userId"`
	Username        string `json:"username"`
	DisplayName     string `json:"displayName"`
	Role            string `json:"role"` // "admin" | "user" | "guest"
	AllowLibrary    bool   `json:"allowLibrary"`
	AllowDJ         bool   `json:"allowDJ"`
	AllowParty      bool   `json:"allowParty"`
	CanEditProfiles bool   `json:"canEditProfiles"`
	IsLAN           bool   `json:"isLAN"`
	HasUsers        bool   `json:"hasUsers"`
}

func AdminClaims(isLAN, hasUsers bool) SessionClaims {
	return SessionClaims{
		UserID:          "admin",
		Username:        "admin",
		DisplayName:     "Admin",
		Role:            "admin",
		AllowLibrary:    true,
		AllowDJ:         true,
		AllowParty:      true,
		CanEditProfiles: true,
		IsLAN:           isLAN,
		HasUsers:        hasUsers,
	}
}

func GuestClaims(isLAN bool) SessionClaims {
	return SessionClaims{
		UserID:          "guest",
		Username:        "guest",
		DisplayName:     "Remote Listener",
		Role:            "guest",
		AllowLibrary:    true,
		AllowDJ:         true,
		AllowParty:      true,
		CanEditProfiles: false,
		IsLAN:           isLAN,
	}
}

func PlayerClaims(isLAN bool) SessionClaims {
	return SessionClaims{
		UserID:          "player",
		Username:        "player",
		DisplayName:     "Player Link",
		Role:            "player",
		AllowLibrary:    true,
		AllowDJ:         true,
		AllowParty:      false,
		CanEditProfiles: false,
		IsLAN:           isLAN,
	}
}

func SignPartyAccessCookie(cfg *config.Config) string {
	mac := hmac.New(sha256.New, []byte(cfg.Remote.SessionSecret+":party:"+cfg.Remote.PartyShareToken))
	mac.Write([]byte("party-access-v1"))
	return hex.EncodeToString(mac.Sum(nil))
}

func HasValidPartyAccessCookie(r *http.Request, cfg *config.Config) bool {
	c, err := r.Cookie(PartyAccessCookieName)
	if err != nil || c == nil || c.Value == "" {
		return false
	}
	want := SignPartyAccessCookie(cfg)
	return hmac.Equal([]byte(c.Value), []byte(want))
}

func SignPlayerCookie(cfg *config.Config) string {
	claims := PlayerClaims(false)
	return SignSessionCookie(claims, cfg.PairingPIN+":"+cfg.Remote.PlayerShareToken)
}

func ClaimsFromUser(u *config.UserAccount, isLAN, hasUsers bool) SessionClaims {
	if u.Role == "admin" {
		return SessionClaims{
			UserID:          u.ID,
			Username:        u.Username,
			DisplayName:     firstNonEmpty(u.DisplayName, u.Username),
			Role:            "admin",
			AllowLibrary:    true,
			AllowDJ:         true,
			AllowParty:      true,
			CanEditProfiles: true,
			IsLAN:           isLAN,
			HasUsers:        hasUsers,
		}
	}
	return SessionClaims{
		UserID:          u.ID,
		Username:        u.Username,
		DisplayName:     firstNonEmpty(u.DisplayName, u.Username),
		Role:            "user",
		AllowLibrary:    u.AllowLibrary,
		AllowDJ:         u.AllowDJ,
		AllowParty:      u.AllowParty,
		CanEditProfiles: u.CanEditProfiles,
		IsLAN:           isLAN,
		HasUsers:        hasUsers,
	}
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return strings.TrimSpace(a)
	}
	return strings.TrimSpace(b)
}

type Handler struct {
	cfg   *config.Config
	store *db.Store
	idx   *indexer.Indexer
	party *party.Engine
}

func NewHandler(cfg *config.Config, store *db.Store, idx *indexer.Indexer, partyEngine *party.Engine) http.Handler {
	return &Handler{cfg: cfg, store: store, idx: idx, party: partyEngine}
}

// SessionCookieValue returns the legacy deterministic HMAC token for the current PairingPIN.
func SessionCookieValue(pin string) string {
	mac := hmac.New(sha256.New, []byte("my-echo-remote-session-v1:"+pin))
	mac.Write([]byte(pin))
	return hex.EncodeToString(mac.Sum(nil))
}

// SignSessionCookie signs a SessionClaims payload with HMAC-SHA256 keyed by the server PIN.
func SignSessionCookie(claims SessionClaims, pin string) string {
	raw, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, []byte("my-echo-session-v2:"+pin))
	mac.Write([]byte(payload))
	return payload + "." + hex.EncodeToString(mac.Sum(nil))
}

// ParseSessionCookie validates the signed session cookie and refreshes live user permissions from cfg.
func ParseSessionCookie(r *http.Request, cfg *config.Config) (SessionClaims, bool) {
	c, err := r.Cookie(SessionCookieName)
	if err != nil || c == nil || c.Value == "" {
		return SessionClaims{}, false
	}
	isLAN := IsLANRequest(r)
	hasUsers := len(cfg.Users) > 0

	// Backward compatibility with v1 admin PIN cookie
	if !strings.Contains(c.Value, ".") {
		want := SessionCookieValue(cfg.PairingPIN)
		if hmac.Equal([]byte(c.Value), []byte(want)) {
			return AdminClaims(isLAN, hasUsers), true
		}
		return SessionClaims{}, false
	}

	parts := strings.SplitN(c.Value, ".", 2)
	if len(parts) != 2 {
		return SessionClaims{}, false
	}
	// Check scoped Player URL session first (bound to current PlayerShareToken so rotation revokes immediately)
	if (cfg.Remote.PlayerURLAccess == config.RemoteAccessToken || cfg.Remote.PlayerURLAccess == config.RemoteAccessPublic) &&
		strings.TrimSpace(cfg.Remote.PlayerShareToken) != "" {
		pmac := hmac.New(sha256.New, []byte("my-echo-session-v2:"+cfg.PairingPIN+":"+cfg.Remote.PlayerShareToken))
		pmac.Write([]byte(parts[0]))
		if hmac.Equal([]byte(parts[1]), []byte(hex.EncodeToString(pmac.Sum(nil)))) {
			return PlayerClaims(isLAN), true
		}
	}
	mac := hmac.New(sha256.New, []byte("my-echo-session-v2:"+cfg.PairingPIN))
	mac.Write([]byte(parts[0]))
	wantSig := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(parts[1]), []byte(wantSig)) {
		return SessionClaims{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return SessionClaims{}, false
	}
	var claims SessionClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return SessionClaims{}, false
	}
	claims.IsLAN = isLAN
	claims.HasUsers = hasUsers

	switch claims.UserID {
	case "admin":
		return AdminClaims(isLAN, hasUsers), true
	case "guest":
		// If remote PIN or user accounts were turned on after guest cookie was issued, revoke guest access.
		if !isLAN && (cfg.Remote.RequirePINOnRemote || len(cfg.Users) > 0) {
			return SessionClaims{}, false
		}
		return GuestClaims(isLAN), true
	default:
		u := cfg.FindUserByID(claims.UserID)
		if u == nil {
			// User was deleted -> revoke immediately!
			return SessionClaims{}, false
		}
		return ClaimsFromUser(u, isLAN, hasUsers), true
	}
}

// HasValidSessionCookie checks whether the request carries a valid signed session cookie.
func HasValidSessionCookie(r *http.Request, cfg *config.Config) bool {
	_, ok := ParseSessionCookie(r, cfg)
	return ok
}

// IsLANRequest returns true when the request originated directly from a local/private network
// (RFC1918, loopback, link-local, or Tailscale CGNAT) and was NOT proxied from the public internet
// (e.g. Cloudflare Tunnel CF-Connecting-IP or public X-Forwarded-For).
func IsLANRequest(r *http.Request) bool {
	if strings.TrimSpace(r.Header.Get("CF-Connecting-IP")) != "" {
		return false
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		first := strings.TrimSpace(strings.Split(xff, ",")[0])
		if ip := net.ParseIP(first); ip != nil && !isPrivateIP(ip) {
			return false
		}
	}
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		host = h
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	return isPrivateIP(ip)
}

func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
		return true
	}
	// Tailscale CGNAT 100.64.0.0/10
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return true
	}
	return false
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/icon.png", "/favicon.ico", "/apple-touch-icon.png":
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(appIconPNG)
		return
	case "/login", "/login/":
		h.RenderLoginPage(w, r, strings.TrimSpace(r.URL.Query().Get("u")), sanitizeNextPath(r.URL.Query().Get("next")))
		return
	}
	if strings.HasPrefix(r.URL.Path, "/u/") {
		uname := strings.Trim(strings.TrimPrefix(r.URL.Path, "/u/"), "/")
		if uname != "" && !strings.Contains(uname, "/") {
			h.RenderLoginPage(w, r, uname, "/")
			return
		}
	}

	isLAN := IsLANRequest(r)
	hasUsers := len(h.cfg.Users) > 0
	claims, hasCookie := ParseSessionCookie(r, h.cfg)

	// Explicit ?login=1 lets a LAN user switch accounts cleanly
	if r.URL.Query().Get("login") == "1" {
		h.RenderLoginPage(w, r, strings.TrimSpace(r.URL.Query().Get("u")), sanitizeNextPath(r.URL.Query().Get("next")))
		return
	}

	if !hasCookie {
		explicitlyLoggedOut := IsExplicitlyLoggedOut(r)
		if !isLAN {
			if h.cfg.Remote.RequirePINOnRemote || hasUsers || explicitlyLoggedOut {
				h.RenderLoginPage(w, r, "", sanitizeNextPath(r.URL.RequestURI()))
				return
			}
			// Open remote mode (no users configured & RequirePINOnRemote unchecked)
			claims = GuestClaims(false)
			isHTTPS := r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
			http.SetCookie(w, &http.Cookie{
				Name:     SessionCookieName,
				Value:    SignSessionCookie(claims, h.cfg.PairingPIN),
				Path:     "/",
				HttpOnly: true,
				Secure:   isHTTPS,
				SameSite: http.SameSiteLaxMode,
				MaxAge:   7 * 86400,
			})
		} else {
			if (h.cfg.AuthRequireLoginOnLAN && hasUsers) || explicitlyLoggedOut {
				h.RenderLoginPage(w, r, "", "/")
				return
			}
			claims = AdminClaims(true, hasUsers)
		}
	}

	h.RenderWebAppWithClaims(w, r, claims)
}

// RenderWebAppWithClaims serves the embedded Web UI HTML bound to the given SessionClaims.
func (h *Handler) RenderWebAppWithClaims(w http.ResponseWriter, r *http.Request, claims SessionClaims) {
	isLAN := IsLANRequest(r)
	// Only embed raw Admin Pairing PIN into HTML for LAN admins; remote & non-admin users authenticate via HttpOnly cookie.
	pinForHTML := ""
	if isLAN && claims.Role == "admin" {
		pinForHTML = h.cfg.PairingPIN
	}
	sessBytes, _ := json.Marshal(claims)

	html := string(webUIHTML)
	replacer := strings.NewReplacer(
		"__ECHO_SERVER_NAME__", htmlEscape(h.cfg.ServerName),
		"__ECHO_PAIRING_PIN__", htmlEscape(pinForHTML),
		"__ECHO_HOST__", htmlEscape(h.cfg.EffectiveAdvertiseHost()),
		"__ECHO_API_PORT__", intToStr(h.cfg.APIPort),
		"__ECHO_PARTY_PORT__", intToStr(h.cfg.PartyHTTPPort),
		"__ECHO_PARTY_HTTPS_PORT__", intToStr(h.cfg.PartyHTTPSPort),
		"__ECHO_SESSION_JSON__", string(sessBytes),
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(replacer.Replace(html)))
}

// RenderRadioPlayer serves the dedicated zero-clutter URL-only Radio DJ Streaming Player.
func (h *Handler) RenderRadioPlayer(w http.ResponseWriter, r *http.Request, shareToken string) {
	station := strings.TrimSpace(h.cfg.DJ.RadioStationName)
	if station == "" {
		station = "Echo FM"
	}
	streamURL := "/stream/dj.mp3"
	m3uURL := "/stream/dj.m3u"
	metaURL := "/api/public/radio/now"
	artBase := "/api/public/radio/artwork/"
	if tok := strings.TrimSpace(shareToken); tok != "" {
		escTok := url.PathEscape(tok)
		streamURL = "/stream/dj/" + escTok + ".mp3"
		m3uURL = "/stream/dj/" + escTok + ".m3u"
		metaURL = "/api/public/radio/" + escTok + "/now"
		artBase = "/api/public/radio/" + escTok + "/artwork/"
	}

	replacer := strings.NewReplacer(
		"__RADIO_SERVER_NAME__", htmlEscape(h.cfg.ServerName),
		"__RADIO_STATION_NAME__", htmlEscape(station),
		"__RADIO_STREAM_URL__", htmlEscape(streamURL),
		"__RADIO_M3U_URL__", htmlEscape(m3uURL),
		"__RADIO_META_URL__", htmlEscape(metaURL),
		"__RADIO_ART_BASE__", htmlEscape(artBase),
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(replacer.Replace(string(radioPlayerHTML))))
}

// RenderLoginPage renders the sign-in screen supporting optional prefilled username and safe post-login redirect.
func (h *Handler) RenderLoginPage(w http.ResponseWriter, r *http.Request, prefillUser, nextURL string) {
	hasUsers := len(h.cfg.Users) > 0 || prefillUser != ""
	if nextURL == "" {
		nextURL = "/"
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(renderRemoteLoginHTMLWithOpts(h.cfg.ServerName, hasUsers, prefillUser, nextURL, h.cfg.Remote.AllowUserLogin, h.cfg.Remote.AllowPINLogin)))
}

func sanitizeNextPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "//") || strings.HasPrefix(raw, "/login") {
		return "/"
	}
	return raw
}

func renderRemoteLoginHTML(serverName string, hasUsers bool) string {
	return renderRemoteLoginHTMLWithOpts(serverName, hasUsers, "", "/", true, true)
}

func renderRemoteLoginHTMLWithOpts(serverName string, hasUsers bool, prefillUser, nextURL string, allowUser, allowPIN bool) string {
	if !allowUser && !allowPIN {
		allowUser, allowPIN = true, true
	}
	userBlock := ""
	subText := "Enter your 6-digit My Echo Pairing PIN to sign in."
	if hasUsers && allowUser {
		subText = "Sign in with your user account" + func() string {
			if allowPIN {
				return " (or Admin Pairing PIN)."
			}
			return "."
		}()
		pinPart := ""
		if allowPIN {
			pinPart = `
    <div class="divider"><span>OR ADMIN PIN</span></div>
    <input id="pin" type="password" inputmode="numeric" autocomplete="one-time-code" placeholder="6-digit Admin PIN" />`
		}
		userFocus := "autofocus"
		passFocus := ""
		if prefillUser != "" {
			userFocus = ""
			passFocus = "autofocus"
		}
		userBlock = fmt.Sprintf(`
    <div id="userFields">
      <input id="username" type="text" autocomplete="username" value="%s" placeholder="Username" %s style="margin-bottom:8px;letter-spacing:0;text-align:left;font-size:15px;font-family:inherit;" />
      <input id="password" type="password" autocomplete="current-password" placeholder="Password" %s style="letter-spacing:1px;text-align:left;font-size:15px;font-family:inherit;" />
    </div>%s`, htmlEscape(prefillUser), userFocus, passFocus, pinPart)
	} else {
		userBlock = `
    <input id="pin" type="password" inputmode="numeric" autocomplete="one-time-code" placeholder="••••••" autofocus required />`
	}

	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Sign In · %s</title>
<link rel="icon" type="image/png" href="/icon.png" />
<style>
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center; background:#050608; color:#fff; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; padding:20px; }
  .box { width:100%%; max-width:370px; background:#0d1018; border:1px solid rgba(20,241,149,0.35); border-radius:8px; padding:24px; text-align:center; }
  img { width:54px; height:54px; border-radius:8px; margin-bottom:10px; }
  h1 { margin:0 0 4px; font-size:18px; }
  p { margin:0 0 16px; font-size:12.5px; color:rgba(255,255,255,0.65); }
  input { width:100%%; box-sizing:border-box; background:#050608; color:#fff; border:1px solid rgba(76,217,255,0.35); border-radius:6px; padding:11px 12px; font-size:18px; text-align:center; letter-spacing:4px; font-family:ui-monospace,monospace; outline:none; }
  input:focus { border-color:#14F195; }
  .divider { display:flex; align-items:center; gap:10px; margin:14px 0 10px; color:rgba(255,255,255,0.42); font-size:10.5px; font-weight:700; letter-spacing:0.8px; }
  .divider::before, .divider::after { content:""; flex:1; height:1px; background:rgba(255,255,255,0.12); }
  button { width:100%%; margin-top:14px; border:none; border-radius:6px; padding:10px; background:#14F195; color:#04150d; font-weight:800; font-size:14px; cursor:pointer; }
  .err { color:#FF5560; font-size:12px; margin-top:10px; min-height:16px; }
</style>
</head>
<body>
  <form class="box" onsubmit="return doLogin(event)">
    <img src="/icon.png" alt="My Echo" />
    <h1>%s</h1>
    <p>%s</p>
    %s
    <button type="submit">Sign In</button>
    <div class="err" id="err"></div>
  </form>
  <script>
    const NEXT_URL = %q;
    async function doLogin(e) {
      e.preventDefault();
      const err = document.getElementById("err");
      err.textContent = "";
      const uEl = document.getElementById("username");
      const pEl = document.getElementById("password");
      const pinEl = document.getElementById("pin");
      const body = {
        username: uEl ? uEl.value.trim() : "",
        password: pEl ? pEl.value : "",
        pin: pinEl ? pinEl.value.trim() : ""
      };
      const res = await fetch("/api/v2/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      const d = await res.json().catch(() => ({}));
      if (res.ok && d.ok) { location.href = NEXT_URL || "/"; return false; }
      err.textContent = d.error || "Sign in failed";
      return false;
    }
  </script>
</body>
</html>`, htmlEscape(serverName), htmlEscape(serverName), htmlEscape(subText), userBlock, nextURL)
}

func htmlEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;").Replace(s)
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
