package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/baardove/my-echo-server/internal/config"
)

func TestExplicitLogoutStaysLoggedOutOnLAN(t *testing.T) {
	cfg := &config.Config{
		ServerName:            "Test Echo",
		PairingPIN:            "123456",
		APIPort:               48721,
		PartyHTTPPort:         53333,
		PartyHTTPSPort:        53334,
		AuthRequireLoginOnLAN: false,
	}
	h := NewHandler(cfg, nil, nil, nil)

	// 1. Fresh LAN request without cookies grants auto-Admin WebUI
	reqFresh := httptest.NewRequest(http.MethodGet, "/", nil)
	reqFresh.RemoteAddr = "192.168.0.50:54321"
	recFresh := httptest.NewRecorder()
	h.ServeHTTP(recFresh, reqFresh)
	if recFresh.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recFresh.Code)
	}
	if strings.Contains(recFresh.Body.String(), "<title>Sign In") {
		t.Fatalf("expected fresh LAN request to enter WebUI directly without login screen")
	}

	// 2. After clicking Sign Out (echo_logged_out=1 cookie, no echo_session cookie),
	// navigating to "/" on LAN must stay on the Sign In screen and NEVER leak PairingPIN!
	reqOut := httptest.NewRequest(http.MethodGet, "/", nil)
	reqOut.RemoteAddr = "192.168.0.50:54321"
	reqOut.AddCookie(&http.Cookie{Name: LoggedOutCookieName, Value: "1"})
	recOut := httptest.NewRecorder()
	h.ServeHTTP(recOut, reqOut)
	bodyOut := recOut.Body.String()
	if !strings.Contains(bodyOut, "<title>Sign In") {
		t.Fatalf("expected explicitly logged-out LAN browser visiting / to stay on Sign In screen")
	}
	if strings.Contains(bodyOut, cfg.PairingPIN) {
		t.Fatalf("logged-out login page must NEVER contain PairingPIN")
	}

	// 3. After signing back in (valid session cookie present), "/" serves the WebUI again
	reqIn := httptest.NewRequest(http.MethodGet, "/", nil)
	reqIn.RemoteAddr = "192.168.0.50:54321"
	reqIn.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: SignSessionCookie(AdminClaims(true, false), cfg.PairingPIN),
	})
	recIn := httptest.NewRecorder()
	h.ServeHTTP(recIn, reqIn)
	if strings.Contains(recIn.Body.String(), "<title>Sign In") {
		t.Fatalf("expected authenticated session cookie to enter WebUI")
	}
}
