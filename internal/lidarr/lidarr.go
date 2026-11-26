package lidarr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/editorial"
	"github.com/baardove/my-echo/server/internal/indexer"
)

const webhookDisplayName = "My Echo Server"

type NudgeEvent struct {
	ID         string    `json:"id"`
	Source     string    `json:"source"` // "webhook" or "poll"
	EventType  string    `json:"eventType"`
	ArtistName string    `json:"artistName"`
	AlbumTitle string    `json:"albumTitle,omitempty"`
	AlbumKey   string    `json:"albumKey,omitempty"`
	SubPath    string    `json:"subPath,omitempty"`
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
}

type ConnectionStatus struct {
	Configured         bool         `json:"configured"`
	Connected          bool         `json:"connected"`
	Version            string       `json:"version,omitempty"`
	InstanceName       string       `json:"instanceName,omitempty"`
	WebhookInstalled   bool         `json:"webhookInstalled"`
	WebhookURL         string       `json:"webhookURL"`
	PollEnabled        bool         `json:"pollEnabled"`
	LastPollAt         *time.Time   `json:"lastPollAt,omitempty"`
	Error              string       `json:"error,omitempty"`
	RecentNudges       []NudgeEvent `json:"recentNudges"`
	RecentAlbumKeys    []string     `json:"recentAlbumKeys"`
}

type WebhookPayload struct {
	EventType string `json:"eventType"`
	Artist    struct {
		Name string `json:"name"`
		Path string `json:"path"`
	} `json:"artist"`
	Album struct {
		Title string `json:"title"`
	} `json:"album"`
	Albums []struct {
		Title string `json:"title"`
	} `json:"albums"`
}

type Manager struct {
	cfg        *config.Config
	idx        *indexer.Indexer
	editorial  *editorial.Fetcher
	httpClient *http.Client

	mu              sync.RWMutex
	nudges          []NudgeEvent
	recentAlbumKeys map[string]time.Time
	lastHistoryID   int
	lastPollAt      *time.Time
	lastError       string
	lastVersion     string
	lastConnected   bool
	webhookPresent  bool
}

func NewManager(cfg *config.Config, idx *indexer.Indexer, ed *editorial.Fetcher) *Manager {
	return &Manager{
		cfg:             cfg,
		idx:             idx,
		editorial:       ed,
		httpClient:      &http.Client{Timeout: 8 * time.Second},
		recentAlbumKeys: make(map[string]time.Time),
	}
}

func (m *Manager) WebhookURL() string {
	return fmt.Sprintf("http://%s:%d/api/v2/webhooks/lidarr?token=%s",
		m.cfg.EffectiveAdvertiseHost(), m.cfg.APIPort, m.cfg.PairingPIN)
}

func (m *Manager) IsRecentLidarrAlbum(albumKey string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, ok := m.recentAlbumKeys[strings.ToLower(strings.TrimSpace(albumKey))]
	return ok && time.Since(ts) < 7*24*time.Hour
}

func (m *Manager) RecentAlbumKeys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.recentAlbumKeys))
	for k := range m.recentAlbumKeys {
		out = append(out, k)
	}
	return out
}

func (m *Manager) Status() ConnectionStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	configured := strings.TrimSpace(m.cfg.LidarrURL) != "" && strings.TrimSpace(m.cfg.LidarrAPIKey) != ""
	nudges := make([]NudgeEvent, len(m.nudges))
	copy(nudges, m.nudges)
	keys := make([]string, 0, len(m.recentAlbumKeys))
	for k := range m.recentAlbumKeys {
		keys = append(keys, k)
	}
	return ConnectionStatus{
		Configured:       configured,
		Connected:        configured && m.lastConnected,
		Version:          m.lastVersion,
		WebhookInstalled: m.webhookPresent,
		WebhookURL:       m.WebhookURL(),
		PollEnabled:      m.cfg.LidarrPollEnabled,
		LastPollAt:       m.lastPollAt,
		Error:            m.lastError,
		RecentNudges:     nudges,
		RecentAlbumKeys:  keys,
	}
}

// StartPoller gently polls Lidarr's import history every 60s when configured,
// giving automatic "new music added" nudges even without configuring webhooks.
func (m *Manager) StartPoller(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(8 * time.Second):
		}
		m.pollOnce(ctx, true)

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.pollOnce(ctx, false)
			}
		}
	}()
}

// TestConnection verifies Lidarr connectivity, version, and whether our webhook is installed.
func (m *Manager) TestConnection(ctx context.Context) ConnectionStatus {
	baseURL := strings.TrimRight(strings.TrimSpace(m.cfg.LidarrURL), "/")
	apiKey := strings.TrimSpace(m.cfg.LidarrAPIKey)
	if baseURL == "" || apiKey == "" {
		m.mu.Lock()
		m.lastConnected = false
		m.lastError = "Set Lidarr URL and API Key first"
		m.mu.Unlock()
		return m.Status()
	}

	var sysStatus struct {
		Version      string `json:"version"`
		InstanceName string `json:"instanceName"`
	}
	if err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/system/status", nil, &sysStatus); err != nil {
		m.mu.Lock()
		m.lastConnected = false
		m.lastError = err.Error()
		m.mu.Unlock()
		return m.Status()
	}

	hookInstalled := false
	var notifs []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/notification", nil, &notifs); err == nil {
		for _, n := range notifs {
			if strings.EqualFold(strings.TrimSpace(n.Name), webhookDisplayName) {
				hookInstalled = true
				break
			}
		}
	}

	m.mu.Lock()
	m.lastConnected = true
	m.lastVersion = sysStatus.Version
	m.webhookPresent = hookInstalled
	m.lastError = ""
	m.mu.Unlock()

	st := m.Status()
	st.InstanceName = sysStatus.InstanceName
	return st
}

// RegisterWebhook automatically creates or updates a "My Echo Server" webhook in Lidarr
// so Lidarr pushes instant real-time nudges whenever albums are imported, upgraded, renamed, or deleted.
func (m *Manager) RegisterWebhook(ctx context.Context) (string, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(m.cfg.LidarrURL), "/")
	apiKey := strings.TrimSpace(m.cfg.LidarrAPIKey)
	if baseURL == "" || apiKey == "" {
		return "", fmt.Errorf("Lidarr URL and API Key must be saved before registering the webhook")
	}

	hookURL := m.WebhookURL()
	payload := map[string]any{
		"name":                 webhookDisplayName,
		"implementation":       "Webhook",
		"implementationName":   "Webhook",
		"configContract":       "WebhookSettings",
		"onGrab":               false,
		"onReleaseImport":      true,
		"onUpgrade":            true,
		"onRename":             true,
		"onArtistAdd":          false,
		"onArtistDelete":       true,
		"onAlbumDelete":        true,
		"onHealthIssue":        false,
		"onDownloadFailure":    false,
		"onImportFailure":      false,
		"onTrackRetag":         true,
		"onApplicationUpdate":  false,
		"includeHealthWarnings": false,
		"tags":                 []int{},
		"fields": []map[string]any{
			{"name": "url", "value": hookURL},
			{"name": "method", "value": 1},
			{"name": "username", "value": ""},
			{"name": "password", "value": ""},
		},
	}

	var existing []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	existingID := 0
	if err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/notification", nil, &existing); err == nil {
		for _, n := range existing {
			if strings.EqualFold(strings.TrimSpace(n.Name), webhookDisplayName) {
				existingID = n.ID
				break
			}
		}
	}

	if existingID > 0 {
		payload["id"] = existingID
		if err := m.lidarrReq(ctx, http.MethodPut, fmt.Sprintf("/api/v1/notification/%d", existingID), payload, nil); err != nil {
			return "", fmt.Errorf("update Lidarr webhook: %w", err)
		}
		m.mu.Lock()
		m.webhookPresent = true
		m.lastConnected = true
		m.lastError = ""
		m.mu.Unlock()
		return "Updated existing 'My Echo Server' webhook in Lidarr!", nil
	}

	if err := m.lidarrReq(ctx, http.MethodPost, "/api/v1/notification", payload, nil); err != nil {
		return "", fmt.Errorf("create Lidarr webhook: %w", err)
	}
	m.mu.Lock()
	m.webhookPresent = true
	m.lastConnected = true
	m.lastError = ""
	m.mu.Unlock()
	return "Registered 'My Echo Server' webhook in Lidarr! New imports will now nudge My Echo instantly.", nil
}

// HandleWebhook processes an incoming push nudge from Lidarr.
func (m *Manager) HandleWebhook(payload WebhookPayload) NudgeEvent {
	artistName := strings.TrimSpace(payload.Artist.Name)
	albumTitle := strings.TrimSpace(payload.Album.Title)
	if albumTitle == "" && len(payload.Albums) > 0 {
		albumTitle = strings.TrimSpace(payload.Albums[0].Title)
	}
	eventType := strings.TrimSpace(payload.EventType)
	if eventType == "" {
		eventType = "ReleaseImport"
	}

	if strings.EqualFold(eventType, "Test") {
		ev := m.recordNudge("webhook", "Test", artistName, albumTitle, "", "Lidarr webhook test nudge received!")
		return ev
	}

	targetDir := filepath.Base(strings.TrimSpace(payload.Artist.Path))
	if targetDir == "" || targetDir == "." || targetDir == "/" {
		targetDir = artistName
	}

	msg := fmt.Sprintf("Lidarr %s: %s", eventType, artistName)
	if albumTitle != "" {
		msg = fmt.Sprintf("Lidarr %s: %s — %s", eventType, artistName, albumTitle)
	}
	ev := m.recordNudge("webhook", eventType, artistName, albumTitle, targetDir, msg)

	go m.triggerTargetedScan(targetDir)
	return ev
}

func (m *Manager) pollOnce(ctx context.Context, seedOnly bool) {
	if !m.cfg.LidarrPollEnabled {
		return
	}
	if strings.TrimSpace(m.cfg.LidarrURL) == "" || strings.TrimSpace(m.cfg.LidarrAPIKey) == "" {
		return
	}

	var resp struct {
		Records []struct {
			ID        int    `json:"id"`
			EventType string `json:"eventType"`
			Date      string `json:"date"`
			Artist    struct {
				ArtistName string `json:"artistName"`
				Path       string `json:"path"`
			} `json:"artist"`
			Album struct {
				Title string `json:"title"`
			} `json:"album"`
		} `json:"records"`
	}
	err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/history?page=1&pageSize=12&sortKey=date&sortDirection=descending&includeArtist=true&includeAlbum=true", nil, &resp)
	now := time.Now().UTC()
	m.mu.Lock()
	m.lastPollAt = &now
	if err != nil {
		m.lastError = err.Error()
		m.mu.Unlock()
		return
	}
	m.lastConnected = true
	m.lastError = ""
	prevID := m.lastHistoryID
	maxID := prevID
	for _, rec := range resp.Records {
		if rec.ID > maxID {
			maxID = rec.ID
		}
	}
	if seedOnly || prevID == 0 {
		m.lastHistoryID = maxID
		// Also seed recent imported albums from history so DJ "Fresh from Lidarr" works immediately
		for _, rec := range resp.Records {
			if isImportEvent(rec.EventType) && strings.TrimSpace(rec.Album.Title) != "" {
				m.recentAlbumKeys[strings.ToLower(strings.TrimSpace(rec.Album.Title))] = now
			}
		}
		m.mu.Unlock()
		return
	}
	m.lastHistoryID = maxID
	m.mu.Unlock()

	seenTargets := make(map[string]bool)
	for _, rec := range resp.Records {
		if rec.ID <= prevID || !isImportEvent(rec.EventType) {
			continue
		}
		artistName := strings.TrimSpace(rec.Artist.ArtistName)
		albumTitle := strings.TrimSpace(rec.Album.Title)
		dedupe := strings.ToLower(artistName + "|" + albumTitle)
		if seenTargets[dedupe] {
			continue
		}
		seenTargets[dedupe] = true

		targetDir := filepath.Base(strings.TrimSpace(rec.Artist.Path))
		if targetDir == "" || targetDir == "." || targetDir == "/" {
			targetDir = artistName
		}
		msg := fmt.Sprintf("New Lidarr import: %s — %s", artistName, albumTitle)
		if albumTitle == "" {
			msg = fmt.Sprintf("New Lidarr import: %s", artistName)
		}
		m.recordNudge("poll", rec.EventType, artistName, albumTitle, targetDir, msg)
		log.Printf("[lidarr] %s -> triggering targeted scan (%s)", msg, targetDir)
		go m.triggerTargetedScan(targetDir)
	}
}

func (m *Manager) triggerTargetedScan(targetDir string) {
	for _, lib := range m.cfg.Libraries {
		sub := ""
		if targetDir != "" {
			if fi, err := os.Stat(filepath.Join(lib.Path, targetDir)); err == nil && fi.IsDir() {
				sub = targetDir
			}
		}
		_, _ = m.idx.ScanLibrary(context.Background(), lib, sub)
	}
	if m.editorial != nil && m.cfg.AutoFetchEditorial {
		m.editorial.TriggerEnrichment()
	}
}

func (m *Manager) recordNudge(source, eventType, artist, album, subPath, message string) NudgeEvent {
	now := time.Now().UTC()
	albKey := strings.ToLower(strings.TrimSpace(album))
	ev := NudgeEvent{
		ID:         fmt.Sprintf("lidarr-%d", now.UnixNano()),
		Source:     source,
		EventType:  eventType,
		ArtistName: artist,
		AlbumTitle: album,
		AlbumKey:   albKey,
		SubPath:    subPath,
		Message:    message,
		Timestamp:  now,
	}
	m.mu.Lock()
	m.nudges = append([]NudgeEvent{ev}, m.nudges...)
	if len(m.nudges) > 40 {
		m.nudges = m.nudges[:40]
	}
	if albKey != "" {
		m.recentAlbumKeys[albKey] = now
	}
	m.mu.Unlock()
	return ev
}

func (m *Manager) lidarrReq(ctx context.Context, method, path string, body any, dst any) error {
	baseURL := strings.TrimRight(strings.TrimSpace(m.cfg.LidarrURL), "/")
	apiKey := strings.TrimSpace(m.cfg.LidarrAPIKey)
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("X-Api-Key", apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2_097_152))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(raw))
		if len(msg) > 180 {
			msg = msg[:180] + "..."
		}
		return fmt.Errorf("Lidarr HTTP %d: %s", resp.StatusCode, msg)
	}
	if dst != nil && len(raw) > 0 {
		return json.Unmarshal(raw, dst)
	}
	return nil
}

func isImportEvent(ev string) bool {
	l := strings.ToLower(strings.TrimSpace(ev))
	return l == "trackfileimported" || l == "downloadimported" || l == "albumimportincomplete" || l == "releaseimport"
}
