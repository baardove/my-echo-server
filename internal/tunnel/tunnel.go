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

	"github.com/baardove/my-echo/server/internal/config"
)

var tryCFURLRe = regexp.MustCompile(`https://[a-z0-9-]+\.trycloudflare\.com`)

type Status struct {
	Available      bool   `json:"available"`
	Enabled        bool   `json:"enabled"`
	Running        bool   `json:"running"`
	Mode           string `json:"mode"` // "quick" | "token"
	PublicDJURL    string `json:"publicDJURL,omitempty"`
	PublicPartyURL string `json:"publicPartyURL,omitempty"`
	LastError      string `json:"lastError,omitempty"`
	StartedAt      string `json:"startedAt,omitempty"`
}

// Manager supervises cloudflared for zero-open-port remote HTTPS access to DJ Radio and Party Mode.
// Supports both zero-config Quick Tunnels (https://*.trycloudflare.com) and persistent Named Tunnel Tokens.
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

func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := m.status
	cp.Enabled = m.cfg.Remote.CloudflareEnabled
	if cp.Mode == "" {
		cp.Mode = "quick"
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
	if m.status.Mode == "" {
		m.status.Mode = "quick"
	}
	m.status.PublicDJURL = ""
	m.status.PublicPartyURL = ""
	m.status.LastError = ""
	m.status.Running = false

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
	exposeDJ := m.cfg.Remote.CloudflareExposeDJ
	exposeParty := m.cfg.Remote.CloudflareExposeParty
	m.mu.Unlock()

	if mode == "token" {
		if token == "" {
			m.mu.Lock()
			m.status.Running = false
			m.status.LastError = "Cloudflare Tunnel Token is required for Named Tunnel mode"
			m.mu.Unlock()
			cancel()
			return
		}
		go m.runNamedTunnel(runCtx, token)
		return
	}

	// Quick Tunnel mode: launch instant trusted HTTPS tunnels for enabled services!
	if exposeDJ {
		go m.runQuickTunnel(runCtx, fmt.Sprintf("http://127.0.0.1:%d", m.cfg.APIPort), "dj")
	}
	if exposeParty {
		go m.runQuickTunnel(runCtx, fmt.Sprintf("http://127.0.0.1:%d", m.cfg.PartyHTTPPort), "party")
	}
	if !exposeDJ && !exposeParty {
		m.mu.Lock()
		m.status.Running = false
		m.status.LastError = "Enable DJ Radio and/or Party Mode under Cloudflare Tunnel"
		m.mu.Unlock()
		cancel()
	}
}

func (m *Manager) runNamedTunnel(ctx context.Context, token string) {
	for ctx.Err() == nil {
		log.Printf("[tunnel] starting Cloudflare Named Tunnel...")
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

func (m *Manager) runQuickTunnel(ctx context.Context, localURL, kind string) {
	for ctx.Err() == nil {
		log.Printf("[tunnel] starting Cloudflare Quick Tunnel (%s -> %s)...", kind, localURL)
		cmd := exec.CommandContext(ctx, "cloudflared", "tunnel", "--no-autoupdate", "--url", localURL)
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			m.setErr(fmt.Sprintf("cloudflared %s failed: %v", kind, err))
			return
		}
		if stderr != nil {
			sc := bufio.NewScanner(stderr)
			for sc.Scan() {
				line := sc.Text()
				if u := tryCFURLRe.FindString(line); u != "" {
					m.mu.Lock()
					if kind == "party" {
						m.status.PublicPartyURL = u
					} else {
						m.status.PublicDJURL = u
					}
					m.status.LastError = ""
					m.mu.Unlock()
					log.Printf("[tunnel] Cloudflare Quick Tunnel (%s) LIVE: %s", kind, u)
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
