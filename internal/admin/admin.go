package admin

import (
	_ "embed"
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

type Handler struct {
	cfg   *config.Config
	store *db.Store
	idx   *indexer.Indexer
	party *party.Engine
}

func NewHandler(cfg *config.Config, store *db.Store, idx *indexer.Indexer, partyEngine *party.Engine) http.Handler {
	return &Handler{cfg: cfg, store: store, idx: idx, party: partyEngine}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/icon.png", "/favicon.ico", "/apple-touch-icon.png":
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(appIconPNG)
		return
	}

	html := string(webUIHTML)
	replacer := strings.NewReplacer(
		"__ECHO_SERVER_NAME__", htmlEscape(h.cfg.ServerName),
		"__ECHO_PAIRING_PIN__", htmlEscape(h.cfg.PairingPIN),
		"__ECHO_HOST__", htmlEscape(h.cfg.EffectiveAdvertiseHost()),
		"__ECHO_API_PORT__", intToStr(h.cfg.APIPort),
		"__ECHO_PARTY_PORT__", intToStr(h.cfg.PartyHTTPPort),
		"__ECHO_PARTY_HTTPS_PORT__", intToStr(h.cfg.PartyHTTPSPort),
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(replacer.Replace(html)))
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
