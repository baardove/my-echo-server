package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baardove/my-echo-server/internal/admin"
	"github.com/baardove/my-echo-server/internal/artwork"
	"github.com/baardove/my-echo-server/internal/broadcast"
	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/dj"
	"github.com/baardove/my-echo-server/internal/indexer"
	"github.com/baardove/my-echo-server/internal/party"
	"github.com/baardove/my-echo-server/internal/snapshot"
	"github.com/baardove/my-echo-server/internal/tunnel"
)

func newRemoteTestServer(t *testing.T) (http.Handler, *config.Config) {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		ConfigDir:      dir,
		ServerName:     "Test Echo",
		PairingPIN:     "654321",
		APIPort:        48721,
		PartyHTTPPort:  53333,
		PartyHTTPSPort: 53334,
		DJ:             config.DefaultDJConfig(),
		Remote:         config.DefaultRemoteConfig(),
		DLNA:           config.DefaultDLNAConfig(),
	}
	cfg.Remote.CloudflarePublicDomain = "echo.example.com"
	config.NormalizeDJConfig(&cfg.DJ)
	config.NormalizeRemoteConfig(&cfg.Remote)

	store, err := db.Open(filepath.Join(dir, "echo.sqlite"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	artCache, err := artwork.NewCache(filepath.Join(dir, "art"))
	if err != nil {
		t.Fatalf("artwork.NewCache: %v", err)
	}
	snap, err := snapshot.NewBuilder(filepath.Join(dir, "shards"), store)
	if err != nil {
		t.Fatalf("snapshot.NewBuilder: %v", err)
	}
	partyEng := party.NewEngine(cfg, store, artCache)
	idx := indexer.New(cfg, store, artCache, snap)
	djEng := dj.NewEngine(cfg, store, nil)
	bcHub := broadcast.NewHub(cfg, store, djEng, partyEng)
	tunMgr := tunnel.NewManager(cfg)
	adminUI := admin.NewHandler(cfg, store, idx, partyEng)

	srv := New(cfg, store, artCache, snap, idx, partyEng, adminUI)
	srv.SetDJEngine(djEng)
	srv.SetBroadcastHub(bcHub)
	srv.SetTunnelManager(tunMgr)
	return srv.Handler(), cfg
}

func cfWANReq(method, target string, body []byte) *http.Request {
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, target, bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	r.Host = "echo.example.com"
	r.RemoteAddr = "127.0.0.1:44444"
	r.Header.Set("CF-Connecting-IP", "198.51.100.42")
	r.Header.Set("X-Forwarded-Proto", "https")
	return r
}

func TestSingleDomainCloudflareRemoteURLsAndRadioPlayer(t *testing.T) {
	handler, cfg := newRemoteTestServer(t)

	// 1. User login page at /login and /u/alice
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, cfWANReq("GET", "/u/alice", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `value="alice"`) {
		t.Fatalf("expected prefilled login page on /u/alice, got %d", rec.Code)
	}

	// 2. URL-only Radio DJ Streaming Player gated by token on WAN
	noTok := httptest.NewRecorder()
	handler.ServeHTTP(noTok, cfWANReq("GET", "/radio", nil))
	if noTok.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for /radio without token, got %d", noTok.Code)
	}

	radioTok := cfg.Remote.RadioDJShareToken
	okRadio := httptest.NewRecorder()
	handler.ServeHTTP(okRadio, cfWANReq("GET", "/radio/"+radioTok, nil))
	if okRadio.Code != http.StatusOK || !strings.Contains(okRadio.Body.String(), "/stream/dj/"+radioTok+".mp3") {
		t.Fatalf("expected tokenized radio player HTML, got %d", okRadio.Code)
	}

	okMeta := httptest.NewRecorder()
	handler.ServeHTTP(okMeta, cfWANReq("GET", "/api/public/radio/"+radioTok+"/now", nil))
	if okMeta.Code != http.StatusOK || !strings.Contains(okMeta.Body.String(), `"stationName"`) {
		t.Fatalf("expected public radio metadata, got %d", okMeta.Code)
	}

	okM3U := httptest.NewRecorder()
	handler.ServeHTTP(okM3U, cfWANReq("GET", "/stream/dj/"+radioTok+".m3u", nil))
	if okM3U.Code != http.StatusOK || !strings.Contains(okM3U.Body.String(), "https://echo.example.com/stream/dj/"+radioTok+".mp3") {
		t.Fatalf("expected single-domain HTTPS M3U, got %d %s", okM3U.Code, okM3U.Body.String())
	}

	// 3. Player via URL (/player/<token>) issues scoped player cookie that can browse tracks but cannot hit admin settings
	plTok := cfg.Remote.PlayerShareToken
	plRec := httptest.NewRecorder()
	handler.ServeHTTP(plRec, cfWANReq("GET", "/player/"+plTok, nil))
	if plRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /player/<token>, got %d", plRec.Code)
	}
	plCookies := plRec.Result().Cookies()
	if len(plCookies) == 0 {
		t.Fatal("expected player session cookie")
	}
	tracksReq := cfWANReq("GET", "/api/v2/web/tracks", nil)
	for _, c := range plCookies {
		tracksReq.AddCookie(c)
	}
	tracksRec := httptest.NewRecorder()
	handler.ServeHTTP(tracksRec, tracksReq)
	if tracksRec.Code != http.StatusOK {
		t.Fatalf("expected player URL session to browse tracks, got %d", tracksRec.Code)
	}
	adminReq := cfWANReq("GET", "/api/v2/admin/settings", nil)
	for _, c := range plCookies {
		adminReq.AddCookie(c)
	}
	adminRec := httptest.NewRecorder()
	handler.ServeHTTP(adminRec, adminReq)
	if adminRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for player URL on admin settings, got %d", adminRec.Code)
	}

	// 4. Public Party Mode on the SAME single domain (/party)
	partyRec := httptest.NewRecorder()
	handler.ServeHTTP(partyRec, cfWANReq("GET", "/party", nil))
	if partyRec.Code != http.StatusOK || !strings.Contains(partyRec.Body.String(), "/api/party/") {
		t.Fatalf("expected public Party Mode Guest UI on /party, got %d", partyRec.Code)
	}

	// 5. Rotating Radio token immediately invalidates old link
	rotBody, _ := json.Marshal(map[string]string{"target": "radio"})
	rotReq := cfWANReq("POST", "/api/v2/remote/tokens/rotate", rotBody)
	rotReq.Header.Set("X-Echo-Token", cfg.PairingPIN)
	rotRec := httptest.NewRecorder()
	handler.ServeHTTP(rotRec, rotReq)
	if rotRec.Code != http.StatusOK {
		t.Fatalf("rotate token failed: %d", rotRec.Code)
	}
	oldRec := httptest.NewRecorder()
	handler.ServeHTTP(oldRec, cfWANReq("GET", "/radio/"+radioTok, nil))
	if oldRec.Code != http.StatusForbidden {
		t.Fatalf("expected old radio token to be rejected after rotation, got %d", oldRec.Code)
	}
}
