package tunnel

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo-server/internal/config"
)

var tryCFURLRe = regexp.MustCompile(`https://[a-z0-9-]+\.trycloudflare\.com`)

type Status struct {
	Available      bool   `json:"available"`
	Enabled        bool   `json:"enabled"`
	Running        bool   `json:"running"`
	Mode           string `json:"mode"` // "token" (custom domain) | "quick" (single trycloudflare.com)
	PublicBaseURL  string `json:"publicBaseURL,omitempty"`
	PublicDJURL    string `json:"publicDJURL,omitempty"`
	PublicPartyURL string `json:"publicPartyURL,omitempty"`
	LastError      string `json:"lastError,omitempty"`
	StartedAt      string `json:"startedAt,omitempty"`
}

// Manager supervises a SINGLE cloudflared tunnel targeting http://127.0.0.1:<APIPort>
// so one single HTTPS domain serves all remote access surfaces:
//   - /login          (User-based login + Admin)
//   - /player[/token] (Web Player via URL)
//   - /radio[/token]  (URL-only Radio DJ Streaming Player + /stream/dj[/token].mp3)
//   - /party[/token]  (Public Party Mode Guest UI over HTTPS)
type Manager struct {
	cfg *config.Config

	mu        sync.RWMutex
	status    Status
	cancelRun context.CancelFunc
	parentCtx context.Context
}

func NewManager(cfg *config.Config) *Manager {
	_, err := exec.LookPath("cloudflared")
	return &Manager{
		cfg: cfg,
		status: Status{
			Available: err == nil,
			Mode:      cfg.Remote.CloudflareMode,
		},
	}
}

// ActiveBaseURL returns the active single-domain public HTTPS base URL (no trailing slash).
func (m *Manager) ActiveBaseURL() string {
	m.mu.RLock()
	live := strings.TrimRight(strings.TrimSpace(m.status.PublicBaseURL), "/")
	m.mu.RUnlock()
	if live != "" {
		return live
	}
	return m.cfg.Remote.NormalizedPublicBaseURL()
}

func (m *Manager) Status() Status {
	m.mu.RLock()
	cp := m.status
	m.mu.RUnlock()

	cp.Enabled = m.cfg.Remote.CloudflareEnabled
	if cp.Mode == "" {
		cp.Mode = m.cfg.Remote.CloudflareMode
	}
	if cp.Mode == "" {
		cp.Mode = "token"
	}
	if cp.Mode == "token" {
		if dom := m.cfg.Remote.NormalizedPublicBaseURL(); dom != "" {
			cp.PublicBaseURL = dom
		}
	}
	if cp.PublicBaseURL != "" {
		cp.PublicDJURL = cp.PublicBaseURL + "/radio"
		cp.PublicPartyURL = cp.PublicBaseURL + "/party"
	}
	return cp
}

func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	m.parentCtx = ctx
	m.mu.Unlock()
	m.Reconcile()
}

// Reconcile starts, restarts, or stops cloudflared to match current cfg.Remote settings.
func (m *Manager) Reconcile() {
	m.mu.Lock()
	if m.cancelRun != nil {
		m.cancelRun()
		m.cancelRun = nil
	}
	parent := m.parentCtx
	if parent == nil {
		parent = context.Background()
	}

	_, err := exec.LookPath("cloudflared")
	m.status.Available = err == nil
	m.status.Enabled = m.cfg.Remote.CloudflareEnabled
	m.status.Mode = strings.ToLower(strings.TrimSpace(m.cfg.Remote.CloudflareMode))
	if m.status.Mode != "quick" && m.status.Mode != "token" {
		m.status.Mode = "token"
	}
	m.status.PublicBaseURL = ""
	m.status.PublicDJURL = ""
	m.status.PublicPartyURL = ""
	m.status.LastError = ""
	m.status.Running = false

	if m.status.Mode == "token" {
		m.status.PublicBaseURL = m.cfg.Remote.NormalizedPublicBaseURL()
	}

	if !m.cfg.Remote.CloudflareEnabled {
		m.mu.Unlock()
		return
	}
	if err != nil {
		m.status.LastError = "cloudflared binary not found in container"
		m.mu.Unlock()
		return
	}

	runCtx, cancel := context.WithCancel(parent)
	m.cancelRun = cancel
	m.status.Running = true
	m.status.StartedAt = time.Now().UTC().Format(time.RFC3339)
	mode := m.status.Mode
	token := strings.TrimSpace(m.cfg.Remote.CloudflareToken)
	originURL := fmt.Sprintf("http://127.0.0.1:%d", m.cfg.APIPort)
	m.mu.Unlock()

	if mode == "token" {
		if token == "" {
			m.mu.Lock()
			m.status.Running = false
			m.status.LastError = "Cloudflare Tunnel Token is required for Named Tunnel mode (or switch to Quick Tunnel)"
			m.mu.Unlock()
			cancel()
			return
		}
		go m.runNamedTunnel(runCtx, token)
		return
	}

	// Quick Tunnel mode: launch ONE single HTTPS tunnel serving all routes on APIPort!
	go m.runQuickTunnel(runCtx, originURL)
}

func (m *Manager) runNamedTunnel(ctx context.Context, token string) {
	for ctx.Err() == nil {
		log.Printf("[tunnel] starting single-domain Cloudflare Named Tunnel...")
		cmd := exec.CommandContext(ctx, "cloudflared", "tunnel", "--no-autoupdate", "run", "--token", token)
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			m.setErr(fmt.Sprintf("cloudflared start failed: %v", err))
			return
		}
		if stderr != nil {
			sc := bufio.NewScanner(stderr)
			for sc.Scan() {
				line := sc.Text()
				if strings.Contains(line, "ERR ") || strings.Contains(line, "failed") {
					m.setErr(strings.TrimSpace(line))
				} else if strings.Contains(line, "Registered tunnel connection") {
					m.clearErr()
				}
			}
		}
		_ = cmd.Wait()
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (m *Manager) runQuickTunnel(ctx context.Context, localURL string) {
	for ctx.Err() == nil {
		log.Printf("[tunnel] starting single-domain Cloudflare Quick Tunnel (-> %s)...", localURL)
		cmd := exec.CommandContext(ctx, "cloudflared", "tunnel", "--no-autoupdate", "--url", localURL)
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			m.setErr(fmt.Sprintf("cloudflared quick tunnel failed: %v", err))
			return
		}
		if stderr != nil {
			sc := bufio.NewScanner(stderr)
			for sc.Scan() {
				line := sc.Text()
				if u := tryCFURLRe.FindString(line); u != "" {
					u = strings.TrimRight(u, "/")
					m.mu.Lock()
					m.status.PublicBaseURL = u
					m.status.PublicDJURL = u + "/radio"
					m.status.PublicPartyURL = u + "/party"
					m.status.LastError = ""
					m.mu.Unlock()
					log.Printf("[tunnel] Cloudflare Quick Tunnel LIVE (single domain): %s", u)
				}
			}
		}
		_ = cmd.Wait()
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (m *Manager) setErr(msg string) {
	if len(msg) > 220 {
		msg = msg[:220] + "…"
	}
	m.mu.Lock()
	m.status.LastError = msg
	m.mu.Unlock()
}

func (m *Manager) clearErr() {
	m.mu.Lock()
	m.status.LastError = ""
	m.mu.Unlock()
}
