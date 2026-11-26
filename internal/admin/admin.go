package admin

import (
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
	"github.com/baardove/my-echo/server/internal/indexer"
	"github.com/baardove/my-echo/server/internal/party"
)

//go:embed icon.png
var appIconPNG []byte

//go:embed webui.html
var webUIHTML []byte

const SessionCookieName = "echo_session"

type Handler struct {
	cfg   *config.Config
	store *db.Store
	idx   *indexer.Indexer
	party *party.Engine
}

func NewHandler(cfg *config.Config, store *db.Store, idx *indexer.Indexer, partyEngine *party.Engine) http.Handler {
	return &Handler{cfg: cfg, store: store, idx: idx, party: partyEngine}
}

// SessionCookieValue returns a deterministic HMAC token for the current PairingPIN.
func SessionCookieValue(pin string) string {
	mac := hmac.New(sha256.New, []byte("my-echo-remote-session-v1:"+pin))
	mac.Write([]byte(pin))
	return hex.EncodeToString(mac.Sum(nil))
}

// HasValidSessionCookie checks whether the request carries a valid signed session cookie for the current PIN.
func HasValidSessionCookie(r *http.Request, pin string) bool {
	c, err := r.Cookie(SessionCookieName)
	if err != nil || c == nil || c.Value == "" {
		return false
	}
	want := SessionCookieValue(pin)
	return hmac.Equal([]byte(c.Value), []byte(want))
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
	}

	// Remote security guardrail: when accessed from WAN (Cloudflare Tunnel or UPnP port-forward),
	// require PIN login before serving the Web App (never leak the Pairing PIN in unauthenticated HTML).
	isLAN := IsLANRequest(r)
	if !isLAN && h.cfg.Remote.RequirePINOnRemote && !HasValidSessionCookie(r, h.cfg.PairingPIN) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(renderRemoteLoginHTML(h.cfg.ServerName)))
		return
	}

	// On remote sessions, avoid embedding raw PIN in HTML if cookie auth is active (frontend falls back to cookie automatically).
	pinForHTML := h.cfg.PairingPIN
	if !isLAN {
		pinForHTML = ""
	}

	html := string(webUIHTML)
	replacer := strings.NewReplacer(
		"__ECHO_SERVER_NAME__", htmlEscape(h.cfg.ServerName),
		"__ECHO_PAIRING_PIN__", htmlEscape(pinForHTML),
		"__ECHO_HOST__", htmlEscape(h.cfg.EffectiveAdvertiseHost()),
		"__ECHO_API_PORT__", intToStr(h.cfg.APIPort),
		"__ECHO_PARTY_PORT__", intToStr(h.cfg.PartyHTTPPort),
		"__ECHO_PARTY_HTTPS_PORT__", intToStr(h.cfg.PartyHTTPSPort),
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(replacer.Replace(html)))
}

func renderRemoteLoginHTML(serverName string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Sign In · %s</title>
<link rel="icon" type="image/png" href="/icon.png" />
<style>
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center; background:#050608; color:#fff; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; padding:20px; }
  .box { width:100%%; max-width:360px; background:#0d1018; border:1px solid rgba(20,241,149,0.35); border-radius:8px; padding:24px; text-align:center; }
  img { width:54px; height:54px; border-radius:8px; margin-bottom:10px; }
  h1 { margin:0 0 4px; font-size:18px; }
  p { margin:0 0 16px; font-size:12.5px; color:rgba(255,255,255,0.65); }
  input { width:100%%; box-sizing:border-box; background:#050608; color:#fff; border:1px solid rgba(76,217,255,0.35); border-radius:6px; padding:11px; font-size:20px; text-align:center; letter-spacing:5px; font-family:ui-monospace,monospace; outline:none; }
  input:focus { border-color:#14F195; }
  button { width:100%%; margin-top:12px; border:none; border-radius:6px; padding:10px; background:#14F195; color:#04150d; font-weight:800; font-size:14px; cursor:pointer; }
  .err { color:#FF5560; font-size:12px; margin-top:10px; min-height:16px; }
</style>
</head>
<body>
  <form class="box" onsubmit="return doLogin(event)">
    <img src="/icon.png" alt="My Echo" />
    <h1>%s</h1>
    <p>Enter your 6-digit My Echo Pairing PIN to unlock remote access.</p>
    <input id="pin" type="password" inputmode="numeric" autocomplete="one-time-code" placeholder="••••••" autofocus required />
    <button type="submit">Unlock My Echo</button>
    <div class="err" id="err"></div>
  </form>
  <script>
    async function doLogin(e) {
      e.preventDefault();
      const err = document.getElementById("err");
      err.textContent = "";
      const res = await fetch("/api/v2/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pin: document.getElementById("pin").value.trim() })
      });
      const d = await res.json().catch(() => ({}));
      if (res.ok && d.ok) { location.reload(); return false; }
      err.textContent = d.error || "Invalid PIN";
      return false;
    }
  </script>
</body>
</html>`, htmlEscape(serverName), htmlEscape(serverName))
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
