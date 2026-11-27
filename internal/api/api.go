package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/baardove/my-echo-server/internal/admin"
	"github.com/baardove/my-echo-server/internal/analysis"
	"github.com/baardove/my-echo-server/internal/artwork"
	"github.com/baardove/my-echo-server/internal/backfill"
	"github.com/baardove/my-echo-server/internal/broadcast"
	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/dj"
	"github.com/baardove/my-echo-server/internal/dlna"
	"github.com/baardove/my-echo-server/internal/editorial"
	"github.com/baardove/my-echo-server/internal/indexer"
	"github.com/baardove/my-echo-server/internal/lidarr"
	"github.com/baardove/my-echo-server/internal/netmap"
	"github.com/baardove/my-echo-server/internal/party"
	"github.com/baardove/my-echo-server/internal/snapshot"
	"github.com/baardove/my-echo-server/internal/spotify"
	"github.com/baardove/my-echo-server/internal/tunnel"
)

type Server struct {
	cfg       *config.Config
	store     *db.Store
	artCache  *artwork.Cache
	snap      *snapshot.Builder
	idx       *indexer.Indexer
	party     *party.Engine
	editorial *editorial.Fetcher
	backfill  *backfill.Worker
	dj        *dj.Engine
	lidarr    *lidarr.Manager
	spotify   *spotify.Importer
	analyzer  *analysis.Analyzer
	broadcast *broadcast.Hub
	dlna      *dlna.Manager
	netmap    *netmap.Manager
	tunnel    *tunnel.Manager
	adminUI   http.Handler
}

func New(
	cfg *config.Config,
	store *db.Store,
	artCache *artwork.Cache,
	snap *snapshot.Builder,
	idx *indexer.Indexer,
	partyEngine *party.Engine,
	adminUI http.Handler,
) *Server {
	return &Server{
		cfg:      cfg,
		store:    store,
		artCache: artCache,
		snap:     snap,
		idx:      idx,
		party:    partyEngine,
		adminUI:  adminUI,
	}
}

func (s *Server) SetEditorialFetcher(f *editorial.Fetcher) {
	s.editorial = f
}

func (s *Server) SetBackfillWorker(w *backfill.Worker) {
	s.backfill = w
}

func (s *Server) SetDJEngine(d *dj.Engine) {
	s.dj = d
}

func (s *Server) SetLidarrManager(l *lidarr.Manager) {
	s.lidarr = l
}

func (s *Server) SetSpotifyImporter(sp *spotify.Importer) {
	s.spotify = sp
}

func (s *Server) SetAnalyzer(a *analysis.Analyzer) {
	s.analyzer = a
}

func (s *Server) SetBroadcastHub(b *broadcast.Hub) {
	s.broadcast = b
}

func (s *Server) SetDLNAManager(d *dlna.Manager) {
	s.dlna = d
}

func (s *Server) SetNetmapManager(n *netmap.Manager) {
	s.netmap = n
}

func (s *Server) SetTunnelManager(t *tunnel.Manager) {
	s.tunnel = t
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	partyHandler := s.party.Handler()

	// Public health, Docker/Web app icon, remote PIN/user login & DLNA/live stream endpoints
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/info", s.handleInfo)
	mux.HandleFunc("/api/party/qr.png", s.handlePartyQR)
	mux.HandleFunc("/api/v2/auth/login", s.handleAuthLogin)
	mux.HandleFunc("/api/v2/auth/logout", s.handleAuthLogout)
	mux.HandleFunc("/api/v2/auth/me", s.requireAuth(s.handleAuthMe))
	// Single-domain URL-only Radio DJ streaming player & live streams
	mux.HandleFunc("/radio", s.handleRadioPlayerPage)
	mux.HandleFunc("/radio/", s.handleRadioPlayerPage)
	mux.HandleFunc("/api/public/radio/", s.handlePublicRadioAPI)
	if s.broadcast != nil {
		mux.HandleFunc("/stream/dj.mp3", s.handleDJStreamRoute)
		mux.HandleFunc("/stream/dj.m3u", s.handleDJStreamRoute)
		mux.HandleFunc("/stream/dj/", s.handleDJStreamRoute)
		mux.HandleFunc("/stream/party.mp3", s.broadcast.ServePartyStream)
	}
	// Single-domain Player via URL (/player and /player/<token>) & Party Mode (/party and /party/<token>)
	mux.HandleFunc("/player", s.handlePlayerPage)
	mux.HandleFunc("/player/", s.handlePlayerPage)
	mux.HandleFunc("/party", s.handlePartyPage)
	mux.HandleFunc("/party/", s.handlePartyPage)
	if s.dlna != nil {
		mux.HandleFunc("/dlna/device.xml", s.dlna.ServeDeviceXML)
		mux.HandleFunc("/dlna/cds.xml", s.dlna.ServeCDSXML)
		mux.HandleFunc("/dlna/control", s.dlna.ServeControl)
	}
	if s.adminUI != nil {
		mux.Handle("/icon.png", s.adminUI)
		mux.Handle("/favicon.ico", s.adminUI)
		mux.Handle("/apple-touch-icon.png", s.adminUI)
		mux.Handle("/login", s.adminUI)
		mux.Handle("/login/", s.adminUI)
		mux.Handle("/u/", s.adminUI)
	}

	// Full Dark-Neon Web App (Browse, Play, Party Mode & Server Control)
	if s.adminUI != nil {
		mux.Handle("/admin", s.adminUI)
		mux.Handle("/admin/", s.adminUI)
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" || r.URL.Path == "/index.html" {
				s.adminUI.ServeHTTP(w, r)
				return
			}
			http.NotFound(w, r)
		})
	}

	// Authenticated v1 Apple TV / Companion endpoints
	mux.HandleFunc("/api/v1/ping", s.requireAuth(s.handlePing))
	mux.HandleFunc("/api/v1/catalog/full", s.requireAuth(s.handleCatalogFull))
	mux.HandleFunc("/api/v1/catalog/track/", s.requireAuth(s.handleCatalogTrackStream))
	mux.HandleFunc("/api/v1/artwork/", s.requireAuth(s.handleArtwork))
	mux.HandleFunc("/api/v1/party/state", s.requireAuth(s.handlePartyState))
	mux.HandleFunc("/api/v1/remote/command", s.requireAuth(s.handleRemoteCommand))

	// Authenticated v2 Partitioned Compressed Delta Index, Web Browse, Settings & Shared State endpoints
	mux.HandleFunc("/api/v2/libraries", s.requireAuth(s.handleLibraries))
	mux.HandleFunc("/api/v2/index/manifest", s.requireAuth(s.handleIndexManifest))
	mux.HandleFunc("/api/v2/index/delta", s.requireAuth(s.handleIndexDelta))
	mux.HandleFunc("/api/v2/index/shards/", s.requireAuth(s.handleIndexShard))
	mux.HandleFunc("/api/v2/index/editorial", s.requireAuth(s.handleIndexEditorial))
	mux.HandleFunc("/api/v2/artwork/batch", s.requireAuth(s.handleArtworkBatch))
	mux.HandleFunc("/api/v2/sync/state", s.requireAuth(s.handleSyncState))
	mux.HandleFunc("/api/v2/admin/status", s.requireAdmin(s.handleAdminStatus))
	mux.HandleFunc("/api/v2/admin/scan", s.requireAdmin(s.handleAdminScan))
	mux.HandleFunc("/api/v2/admin/settings", s.requireAdmin(s.handleAdminSettings))
	mux.HandleFunc("/api/v2/admin/users", s.requireAdmin(s.handleAdminUsers))
	mux.HandleFunc("/api/v2/admin/editorial/enrich", s.requireAdmin(s.handleAdminEditorialEnrich))
	mux.HandleFunc("/api/v2/admin/backfill/run", s.requireAdmin(s.handleAdminBackfillRun))
	mux.HandleFunc("/api/v2/admin/backfill/stop", s.requireAdmin(s.handleAdminBackfillStop))
	mux.HandleFunc("/api/v2/admin/analysis/run", s.requireAdmin(s.handleAdminAnalysisRun))
	mux.HandleFunc("/api/v2/admin/analysis/stop", s.requireAdmin(s.handleAdminAnalysisStop))
	mux.HandleFunc("/api/v2/admin/lidarr/status", s.requireAdmin(s.handleLidarrStatus))
	mux.HandleFunc("/api/v2/admin/lidarr/register-webhook", s.requireAdmin(s.handleLidarrRegisterWebhook))
	mux.HandleFunc("/api/v2/lidarr/search", s.requireAdmin(s.handleLidarrSearch))
	mux.HandleFunc("/api/v2/lidarr/order-album", s.requireAdmin(s.handleLidarrOrderAlbum))
	mux.HandleFunc("/api/v2/lidarr/queue", s.requireAdmin(s.handleLidarrQueue))
	mux.HandleFunc("/api/v2/webhooks/lidarr", s.requireAuth(s.handleLidarrWebhook))

	// AI Radio DJ Mode endpoints (uses server's global Gemini API key for all authorized users;
	// non-admin listeners are stream-only: GET state + /stream/dj.mp3, no skipping/mutating)
	mux.HandleFunc("/api/v2/dj/state", s.requirePerm("dj", s.handleDJState))
	mux.HandleFunc("/api/v2/dj/options", s.requirePerm("dj", s.handleDJOptions))
	mux.HandleFunc("/api/v2/dj/session/create", s.requireAdmin(s.handleDJSessionCreate))
	mux.HandleFunc("/api/v2/dj/queue/add", s.requireAdmin(s.handleDJQueueAdd))
	mux.HandleFunc("/api/v2/dj/queue/action", s.requireAdmin(s.handleDJQueueAction))
	mux.HandleFunc("/api/v2/dj/audio/", s.requirePerm("dj", s.handleDJAudio))
	mux.HandleFunc("/api/v2/dj/test-voice", s.requireAdmin(s.handleDJTestVoice))
	mux.HandleFunc("/api/v2/dj/models/live", s.requireAdmin(s.handleDJLiveModels))
	mux.HandleFunc("/api/v2/dj/profiles", s.requirePerm("dj", s.handleDJProfiles))
	mux.HandleFunc("/api/v2/web/tracks", s.requirePerm("library", s.handleWebTracks))
	mux.HandleFunc("/api/v2/web/track", s.requirePerm("library", s.handleWebTrackDetail))
	mux.HandleFunc("/api/v2/web/albums", s.requirePerm("library", s.handleWebAlbums))
	mux.HandleFunc("/api/v2/web/album", s.requirePerm("library", s.handleWebAlbumDetail))
	mux.HandleFunc("/api/v2/web/artists", s.requirePerm("library", s.handleWebArtists))
	mux.HandleFunc("/api/v2/web/artist", s.requirePerm("library", s.handleWebArtistDetail))
	mux.HandleFunc("/api/v2/web/playlists", s.requirePerm("library", s.handleWebPlaylists))
	mux.HandleFunc("/api/v2/web/playlists/create", s.requirePerm("library", s.handleWebPlaylistCreate))
	mux.HandleFunc("/api/v2/web/playlists/update", s.requirePerm("library", s.handleWebPlaylistUpdate))
	mux.HandleFunc("/api/v2/web/playlists/delete", s.requirePerm("library", s.handleWebPlaylistDelete))
	mux.HandleFunc("/api/v2/web/playlists/rematch", s.requirePerm("library", s.handleWebPlaylistRematch))
	mux.HandleFunc("/api/v2/web/playlists/spotify/preview", s.requirePerm("library", s.handleSpotifyPlaylistPreview))
	mux.HandleFunc("/api/v2/web/playlists/spotify/import", s.requirePerm("library", s.handleSpotifyPlaylistImport))
	mux.HandleFunc("/api/v2/web/playlists/spotify/resync", s.requirePerm("library", s.handleSpotifyPlaylistResync))
	mux.HandleFunc("/api/v2/web/playlists/lidarr/search", s.requireAdmin(s.handleWebPlaylistLidarrSearch))
	mux.HandleFunc("/api/v2/web/playlists/lidarr/status", s.requirePerm("library", s.handleWebPlaylistLidarrStatus))
	mux.HandleFunc("/api/v2/web/facets", s.requirePerm("library", s.handleWebFacets))
	mux.HandleFunc("/api/v2/web/rate", s.requirePerm("library", s.handleWebRate))
	mux.HandleFunc("/api/v2/web/played", s.requirePerm("library", s.handleWebPlayed))
	mux.HandleFunc("/api/v2/web/random-queue", s.requirePerm("library", s.handleWebRandomQueue))

	// Remote Access (Cloudflare Tunnel + Share Tokens + Legacy UPnP) & DLNA Casting endpoints
	mux.HandleFunc("/api/v2/remote/status", s.requireAuth(s.handleRemoteStatus))
	mux.HandleFunc("/api/v2/remote/tokens/rotate", s.requireAdmin(s.handleRemoteRotateToken))
	mux.HandleFunc("/api/v2/remote/upnp/sync", s.requireAdmin(s.handleRemoteUPnPSync))
	mux.HandleFunc("/api/v2/remote/tunnel/restart", s.requireAdmin(s.handleRemoteTunnelRestart))
	mux.HandleFunc("/api/v2/dlna/discover", s.requireAuth(s.handleDLNADiscover))
	mux.HandleFunc("/api/v2/dlna/cast", s.requireAuth(s.handleDLNACast))
	mux.HandleFunc("/api/v2/dlna/stop", s.requireAuth(s.handleDLNAStop))

	// Expose Party host & guest routes on main API port under the single Cloudflare domain
	mux.Handle("/api/party/", s.requirePartyAccess(partyHandler))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Echo-Token, Accept-Encoding")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type sessionCtxKey struct{}

func (s *Server) sessionFromRequest(r *http.Request) admin.SessionClaims {
	if v, ok := r.Context().Value(sessionCtxKey{}).(admin.SessionClaims); ok {
		return v
	}
	return admin.SessionClaims{}
}

func (s *Server) authenticateRequest(r *http.Request) (admin.SessionClaims, bool) {
	if claims, ok := admin.ParseSessionCookie(r, s.cfg); ok {
		return claims, true
	}
	tok := strings.TrimSpace(r.Header.Get("X-Echo-Token"))
	if tok == "" {
		tok = strings.TrimSpace(r.URL.Query().Get("token"))
	}
	if tok != "" && tok == s.cfg.PairingPIN {
		return admin.AdminClaims(admin.IsLANRequest(r), len(s.cfg.Users) > 0), true
	}
	return admin.SessionClaims{}, false
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := s.authenticateRequest(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"error": "Sign in required (invalid or missing session/PIN)",
			})
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), sessionCtxKey{}, claims)))
	}
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return s.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		claims := s.sessionFromRequest(r)
		if claims.Role != "admin" {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "Admin access required",
			})
			return
		}
		next(w, r)
	})
}

func (s *Server) requirePerm(perm string, next http.HandlerFunc) http.HandlerFunc {
	return s.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		claims := s.sessionFromRequest(r)
		if claims.Role != "admin" {
			allowed := false
			switch perm {
			case "library":
				allowed = claims.AllowLibrary
			case "dj":
				allowed = claims.AllowDJ
			case "party":
				allowed = claims.AllowParty
			}
			if !allowed {
				writeJSON(w, http.StatusForbidden, map[string]string{
					"error": "Your account does not have permission for " + perm,
				})
				return
			}
		}
		next(w, r)
	})
}

func (s *Server) handleDJStreamM3U(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = fmt.Sprintf("%s:%d", s.cfg.EffectiveAdvertiseHost(), s.cfg.APIPort)
	}
	station := s.cfg.DJ.RadioStationName
	if station == "" {
		station = "My Echo DJ Radio"
	}
	w.Header().Set("Content-Type", "audio/x-mpegurl; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="my-echo-dj-radio.m3u"`)
	_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXTINF:-1,%s (Live AI Radio DJ)\n%s://%s/stream/dj.mp3\n", station, scheme, host)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"serverName": s.cfg.ServerName,
		"serverKind": "echo-server",
	})
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	libs, _ := s.store.ListLibraries()
	var totalTracks int
	var totalBytes int64
	for _, l := range libs {
		totalTracks += l.TrackCount
		totalBytes += l.TotalBytes
	}
	pls, _ := s.store.ListPlaylists(false)
	writeJSON(w, http.StatusOK, map[string]any{
		"app":             "My Echo Server",
		"serverKind":      "echo-server",
		"serverName":      s.cfg.ServerName,
		"deviceID":        "echo-server",
		"deviceName":      s.cfg.ServerName,
		"manifestVersion": fmt.Sprintf("server-%d", totalTracks),
		"syncMode":        "catalog",
		"trackCount":      totalTracks,
		"playlistCount":   len(pls),
		"episodeCount":    0,
		"totalBytes":      totalBytes,
		"capabilities":    []string{"index-v2", "delta-shards", "party", "shared-state", "artwork-batch"},
		"partyPort":       s.cfg.PartyHTTPPort,
		"partyHTTPSPort":  s.cfg.PartyHTTPSPort,
		"advertiseHost":   s.cfg.EffectiveAdvertiseHost(),
	})
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	s.handleInfo(w, r)
}

// Wire structs matching EchoCore WirelessFullCatalogPayload / TVFullCatalogPayload
type wireTrack struct {
	ID            string    `json:"id"`
	Title         string    `json:"title"`
	Artist        string    `json:"artist"`
	AlbumArtist   string    `json:"albumArtist"`
	Album         string    `json:"album"`
	TrackNumber   *int      `json:"trackNumber,omitempty"`
	DiscNumber    *int      `json:"discNumber,omitempty"`
	Year          *int      `json:"year,omitempty"`
	Genre         *string   `json:"genre,omitempty"`
	Duration      float64   `json:"duration"`
	Format        string    `json:"format"`
	FileExtension string    `json:"fileExtension"`
	SampleRate    *int      `json:"sampleRate,omitempty"`
	BitDepth      *int      `json:"bitDepth,omitempty"`
	Bitrate       *int      `json:"bitrate,omitempty"`
	FileSize      int64     `json:"fileSize"`
	ModifiedAt    time.Time `json:"modifiedAt"`
	ArtworkHash   *string   `json:"artworkHash,omitempty"`
}

type wirePlaylist struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	TrackIDs    []string `json:"trackIDs"`
}

type wireFullCatalog struct {
	ServerName      string                 `json:"serverName"`
	ServerKind      string                 `json:"serverKind"`
	CatalogVersion  string                 `json:"catalogVersion"`
	GeneratedAt     time.Time              `json:"generatedAt"`
	Tracks          []wireTrack            `json:"tracks"`
	Playlists       []wirePlaylist         `json:"playlists"`
	PodcastFeeds    []db.SharedPodcastFeed `json:"podcastFeeds"`
	Episodes        []any                  `json:"episodes"`
	AlbumEditorial  []db.AlbumEditorial    `json:"albumEditorial"`
	ArtistEditorial []db.ArtistEditorial   `json:"artistEditorial"`
	PartyStatus     party.Status           `json:"partyStatus"`
}

func (s *Server) handleCatalogFull(w http.ResponseWriter, r *http.Request) {
	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	wTracks := make([]wireTrack, 0, len(tracks))
	h := sha256.New()
	for _, tr := range tracks {
		effID := tr.EffectiveID()
		wTracks = append(wTracks, wireTrack{
			ID:            effID,
			Title:         tr.Title,
			Artist:        tr.Artist,
			AlbumArtist:   tr.AlbumArtist,
			Album:         tr.Album,
			TrackNumber:   tr.TrackNumber,
			DiscNumber:    tr.DiscNumber,
			Year:          tr.Year,
			Genre:         tr.Genre,
			Duration:      tr.Duration,
			Format:        tr.Format,
			FileExtension: tr.FileExtension,
			SampleRate:    tr.SampleRate,
			BitDepth:      tr.BitDepth,
			Bitrate:       tr.Bitrate,
			FileSize:      tr.FileSize,
			ModifiedAt:    tr.ModifiedAt.UTC(),
			ArtworkHash:   tr.ArtworkHash,
		})
		fmt.Fprintf(h, "%s:%d:%d|", effID, tr.FileSize, tr.ModifiedAt.Unix())
	}

	pls, _ := s.store.ListPlaylists(false)
	wPls := make([]wirePlaylist, 0, len(pls))
	for _, p := range pls {
		tids := p.PlayableTrackIDs()
		if tids == nil {
			tids = []string{}
		}
		wPls = append(wPls, wirePlaylist{
			ID:          p.ID,
			Name:        p.Name,
			Description: p.Description,
			TrackIDs:    tids,
		})
		fmt.Fprintf(h, "p:%s:%d|", p.ID, p.UpdatedAt.Unix())
	}

	pods, _ := s.store.ListPodcasts(false)
	if pods == nil {
		pods = []db.SharedPodcastFeed{}
	}
	albEd, _ := s.store.ListAlbumEditorial()
	if albEd == nil {
		albEd = []db.AlbumEditorial{}
	}
	artEd, _ := s.store.ListArtistEditorial()
	if artEd == nil {
		artEd = []db.ArtistEditorial{}
	}

	ver := hex.EncodeToString(h.Sum(nil)[:8])
	payload := wireFullCatalog{
		ServerName:      s.cfg.ServerName,
		ServerKind:      "echo-server",
		CatalogVersion:  ver,
		GeneratedAt:     time.Now().UTC(),
		Tracks:          wTracks,
		Playlists:       wPls,
		PodcastFeeds:    pods,
		Episodes:        []any{},
		AlbumEditorial:  albEd,
		ArtistEditorial: artEd,
		PartyStatus:     s.party.Status(),
	}

	writeMaybeGzipJSON(w, r, http.StatusOK, payload)
}

func (s *Server) handleCatalogTrackStream(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimPrefix(r.URL.Path, "/api/v1/catalog/track/")
	tr, err := s.store.FindTrackByID(trackID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Track not found"})
		return
	}
	lib, ok := s.cfg.LibraryByID(tr.LibraryID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Library not mounted"})
		return
	}
	fullPath, err := safeJoin(lib.Path, tr.RelativePath)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Invalid path"})
		return
	}
	if _, err := os.Stat(fullPath); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Audio file missing on disk"})
		return
	}
	if r.URL.Query().Get("transcode") == "1" {
		if ffBin, err := exec.LookPath("ffmpeg"); err == nil {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("Cache-Control", "no-store")
			cmd := exec.CommandContext(r.Context(), ffBin,
				"-v", "error",
				"-i", fullPath,
				"-vn",
				"-c:a", "libmp3lame",
				"-b:a", "320k",
				"-f", "mp3",
				"pipe:1",
			)
			cmd.Stdout = w
			_ = cmd.Run()
			return
		}
	}
	w.Header().Set("Content-Type", mimeForPath(fullPath))
	http.ServeFile(w, r, fullPath)
}

func (s *Server) handleArtwork(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimPrefix(r.URL.Path, "/api/v1/artwork/")
	size := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("size")))
	var data []byte
	var err error
	if size == "orig" || size == "full" {
		data, err = s.artCache.LoadOrig(hash)
	} else {
		data, err = s.artCache.LoadThumb(hash)
	}
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Artwork not found"})
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	_, _ = w.Write(data)
}

func (s *Server) handlePartyState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.party.Envelope())
}

func (s *Server) handleRemoteCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid payload"})
		return
	}
	switch req.Action {
	case "startPartyMode":
		s.party.StartSession("", "")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "Party Mode started on Echo Server"})
	case "stopPartyMode":
		s.party.StopSession()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "Party Mode stopped on Echo Server"})
	case "togglePlayPause", "play", "pause", "next", "previous":
		ok, msg, _ := s.party.HandleHostCommand(map[string]any{"action": req.Action})
		writeJSON(w, http.StatusOK, map[string]any{"ok": ok, "message": msg})
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "Command requires Mac Remote: " + req.Action})
	}
}

// --- V2 Partitioned Compressed Delta Index Endpoints ---

func (s *Server) handleLibraries(w http.ResponseWriter, r *http.Request) {
	libs, err := s.store.ListLibraries()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if libs == nil {
		libs = []db.LibraryMeta{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"serverName": s.cfg.ServerName,
		"libraries":  libs,
		"scanStatus": s.idx.Status(),
	})
}

func (s *Server) handleIndexManifest(w http.ResponseWriter, r *http.Request) {
	libID := strings.TrimSpace(r.URL.Query().Get("libraryId"))
	if libID == "" && len(s.cfg.Libraries) > 0 {
		libID = s.cfg.Libraries[0].ID
	}
	libMeta, err := s.store.GetLibrary(libID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Library not found: " + libID})
		return
	}
	shards, err := s.store.ListShards(libID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if shards == nil {
		shards = []db.ShardMeta{}
	}
	var totalCompressed, totalUncompressed int64
	for _, sh := range shards {
		totalCompressed += sh.CompressedBytes
		totalUncompressed += sh.UncompressedBytes
	}
	edMeta := s.snap.GetEditorialMeta()
	writeJSON(w, http.StatusOK, map[string]any{
		"serverName":               s.cfg.ServerName,
		"libraryId":                libMeta.ID,
		"libraryName":              libMeta.Name,
		"revision":                 libMeta.CurrentRevision,
		"minDeltaRevision":         libMeta.MinDeltaRev,
		"shardCount":               libMeta.ShardCount,
		"trackCount":               libMeta.TrackCount,
		"totalAudioBytes":          libMeta.TotalBytes,
		"compressedBytes":          totalCompressed,
		"uncompressedBytes":        totalUncompressed,
		"lastScannedAt":            libMeta.LastScannedAt,
		"compression":              "gzip",
		"shards":                   shards,
		"editorialSha256":          edMeta.SHA256,
		"editorialCompressedBytes": edMeta.CompressedBytes,
		"editorialAlbumCount":      edMeta.AlbumCount,
		"editorialArtistCount":     edMeta.ArtistCount,
	})
}

func (s *Server) handleIndexEditorial(w http.ResponseWriter, r *http.Request) {
	gzBytes, err := s.snap.ReadEditorialBundleBytes()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	sum := sha256.Sum256(gzBytes)
	hashHex := hex.EncodeToString(sum[:])
	w.Header().Set("X-Echo-SHA256", hashHex)
	w.Header().Set("ETag", `"`+hashHex+`"`)
	w.Header().Set("Cache-Control", "public, max-age=300")
	if r.URL.Query().Get("raw") == "1" {
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", strconv.Itoa(len(gzBytes)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(gzBytes)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Content-Length", strconv.Itoa(len(gzBytes)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(gzBytes)
}

func (s *Server) handleIndexDelta(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	libID := strings.TrimSpace(q.Get("libraryId"))
	if libID == "" && len(s.cfg.Libraries) > 0 {
		libID = s.cfg.Libraries[0].ID
	}
	sinceRev, err := strconv.Atoi(q.Get("since"))
	if err != nil || sinceRev < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid since revision"})
		return
	}

	libMeta, err := s.store.GetLibrary(libID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Library not found"})
		return
	}

	if sinceRev >= libMeta.CurrentRevision {
		writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
			"libraryId":            libID,
			"fromRevision":         sinceRev,
			"toRevision":           libMeta.CurrentRevision,
			"requiresShardSync":    false,
			"upserted":             []db.TrackRecord{},
			"removedTrackKeys":     []string{},
			"removedRelativePaths": []string{},
		})
		return
	}

	if sinceRev == 0 || (libMeta.MinDeltaRev > 1 && sinceRev < libMeta.MinDeltaRev-1) {
		writeJSON(w, http.StatusOK, map[string]any{
			"libraryId":         libID,
			"fromRevision":      sinceRev,
			"toRevision":        libMeta.CurrentRevision,
			"requiresShardSync": true,
			"reason":            "Client revision is outside delta retention window; fetch changed shards instead.",
		})
		return
	}

	upserted, removedKeys, removedRelPaths, err := s.store.LoadDeltaSince(libID, sinceRev)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if upserted == nil {
		upserted = []db.TrackRecord{}
	}
	if removedKeys == nil {
		removedKeys = []string{}
	}
	if removedRelPaths == nil {
		removedRelPaths = []string{}
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"libraryId":            libID,
		"fromRevision":         sinceRev,
		"toRevision":           libMeta.CurrentRevision,
		"requiresShardSync":    false,
		"upserted":             upserted,
		"removedTrackKeys":     removedKeys,
		"removedRelativePaths": removedRelPaths,
	})
}

func (s *Server) handleIndexShard(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v2/index/shards/{libraryId}/{shardId}
	tail := strings.TrimPrefix(r.URL.Path, "/api/v2/index/shards/")
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) != 2 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Expected /api/v2/index/shards/{libraryId}/{shardId}"})
		return
	}
	libID := parts[0]
	shardStr := strings.TrimSuffix(strings.TrimSuffix(parts[1], ".gz"), ".ndjson")
	shardID, err := strconv.Atoi(shardStr)
	if err != nil || shardID < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid shardId"})
		return
	}

	gzBytes, err := s.snap.ReadShardBytes(libID, shardID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Shard not found"})
		return
	}
	sum := sha256.Sum256(gzBytes)
	hashHex := hex.EncodeToString(sum[:])

	w.Header().Set("X-Echo-SHA256", hashHex)
	w.Header().Set("ETag", `"`+hashHex+`"`)
	w.Header().Set("Cache-Control", "public, max-age=3600")

	if r.URL.Query().Get("raw") == "1" {
		// Return raw .ndjson.gz bytes so client can verify SHA-256 over the compressed payload and gunzip itself
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", strconv.Itoa(len(gzBytes)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(gzBytes)
		return
	}

	// Standard HTTP Content-Encoding: gzip (URLSession transparently decompresses)
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Content-Length", strconv.Itoa(len(gzBytes)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(gzBytes)
}

func (s *Server) handleArtworkBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Hashes []string `json:"hashes"`
		Size   string   `json:"size"` // "thumb" (default) or "orig"
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 262_144)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	if len(req.Hashes) > 200 {
		req.Hashes = req.Hashes[:200]
	}
	preferOrig := strings.EqualFold(req.Size, "orig") || strings.EqualFold(req.Size, "full")

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range req.Hashes {
		if !artwork.IsSafeHash(h) {
			continue
		}
		var data []byte
		var err error
		name := h + "_thumb.jpg"
		if preferOrig {
			data, err = s.artCache.LoadOrig(h)
			name = h + "_orig.dat"
		} else {
			data, err = s.artCache.LoadThumb(h)
		}
		if err != nil || len(data) == 0 {
			continue
		}
		_ = tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(data)),
		})
		_, _ = tw.Write(data)
	}
	_ = tw.Close()

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// --- Shared Library State Bidirectional Sync ---

type sharedStatePayload struct {
	Playlists       []db.SharedPlaylist    `json:"playlists,omitempty"`
	TrackStats      []db.SharedTrackStats  `json:"trackStats,omitempty"`
	AlbumRatings    []db.SharedAlbumRating `json:"albumRatings,omitempty"`
	Podcasts        []db.SharedPodcastFeed `json:"podcasts,omitempty"`
	AlbumEditorial  []db.AlbumEditorial    `json:"albumEditorial,omitempty"`
	ArtistEditorial []db.ArtistEditorial   `json:"artistEditorial,omitempty"`
}

func (s *Server) handleSyncState(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var incoming sharedStatePayload
		if err := json.NewDecoder(io.LimitReader(r.Body, 16_777_216)).Decode(&incoming); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid state JSON"})
			return
		}
		if len(incoming.Playlists) > 0 {
			_ = s.store.MergePlaylists(incoming.Playlists)
		}
		if len(incoming.TrackStats) > 0 {
			_ = s.store.MergeTrackStats(incoming.TrackStats)
		}
		if len(incoming.AlbumRatings) > 0 {
			_ = s.store.MergeAlbumRatings(incoming.AlbumRatings)
		}
		if len(incoming.Podcasts) > 0 {
			_ = s.store.MergePodcasts(incoming.Podcasts)
		}
		if len(incoming.AlbumEditorial) > 0 {
			_ = s.store.MergeAlbumEditorial(incoming.AlbumEditorial)
		}
		if len(incoming.ArtistEditorial) > 0 {
			_ = s.store.MergeArtistEditorial(incoming.ArtistEditorial)
		}
	}

	pls, _ := s.store.ListPlaylists(true)
	stats, _ := s.store.ListTrackStats()
	albs, _ := s.store.ListAlbumRatings(true)
	pods, _ := s.store.ListPodcasts(true)
	albEd, _ := s.store.ListAlbumEditorial()
	artEd, _ := s.store.ListArtistEditorial()

	writeMaybeGzipJSON(w, r, http.StatusOK, sharedStatePayload{
		Playlists:       pls,
		TrackStats:      stats,
		AlbumRatings:    albs,
		Podcasts:        pods,
		AlbumEditorial:  albEd,
		ArtistEditorial: artEd,
	})
}

func (s *Server) handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	libs, _ := s.store.ListLibraries()
	type libInfo struct {
		db.LibraryMeta
		Shards []db.ShardMeta `json:"shards"`
	}
	var out []libInfo
	for _, l := range libs {
		sh, _ := s.store.ListShards(l.ID)
		out = append(out, libInfo{LibraryMeta: l, Shards: sh})
	}
	var edStatus any
	if s.editorial != nil {
		edStatus = s.editorial.Status()
	}
	edMeta := s.snap.GetEditorialMeta()
	var lidarrStatus any
	if s.lidarr != nil {
		lidarrStatus = s.lidarr.Status()
	}
	var analysisStatus any
	if s.analyzer != nil {
		analysisStatus = s.analyzer.Status()
	}
	var backfillStatus any
	if s.backfill != nil {
		backfillStatus = s.backfill.Status()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"serverName":      s.cfg.ServerName,
		"pairingPIN":      s.cfg.PairingPIN,
		"advertiseHost":   s.cfg.EffectiveAdvertiseHost(),
		"apiPort":         s.cfg.APIPort,
		"partyPort":       s.cfg.PartyHTTPPort,
		"partyHTTPSPort":  s.cfg.PartyHTTPSPort,
		"scanStatus":      s.idx.Status(),
		"editorialStatus": edStatus,
		"editorialMeta":   edMeta,
		"analysisStatus":  analysisStatus,
		"backfillStatus":  backfillStatus,
		"lidarrStatus":    lidarrStatus,
		"partyStatus":     s.party.Status(),
		"libraries":       out,
	})
}

func (s *Server) handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req struct {
			ServerName         *string  `json:"serverName"`
			PairingPIN         *string  `json:"pairingPIN"`
			AdvertiseHost      *string  `json:"advertiseHost"`
			ScanInterval       *string  `json:"scanInterval"`
			ShardCount         *int     `json:"shardCount"`
			AutoFetchEditorial *bool    `json:"autoFetchEditorial"`
			MirrorToShare      *bool    `json:"mirrorToShare"`
			LidarrURL               *string  `json:"lidarrURL"`
			LidarrAPIKey            *string  `json:"lidarrAPIKey"`
			LidarrPollEnabled       *bool    `json:"lidarrPollEnabled"`
			AdvancedAnalysisEnabled *bool    `json:"advancedAnalysisEnabled"`
			MetadataBackfillModel   *string  `json:"metadataBackfillModel"`
			AutoMetadataBackfill    *bool    `json:"autoMetadataBackfill"`
			NormEnabled             *bool    `json:"normEnabled"`
			NormTargetDB            *float64 `json:"normTargetDB"`
			LegacyMacPrefix         *string  `json:"legacyMacPrefix"`
			DJ                 *struct {
				Provider         *string  `json:"provider"`
				GeminiAPIKey     *string  `json:"geminiAPIKey"`
				ClearGeminiKey   *bool    `json:"clearGeminiKey"`
				LLMModel         *string  `json:"llmModel"`
				TTSModel         *string  `json:"ttsModel"`
				VoiceName        *string  `json:"voiceName"`
				Persona          *string  `json:"persona"`
				Language         *string  `json:"language"`
				RadioStationName *string  `json:"radioStationName"`
				TalkFrequency    *float64 `json:"talkFrequency"`
				CrossfadeSeconds *float64 `json:"crossfadeSeconds"`
				DuckVolume       *float64 `json:"duckVolume"`
				AutoRefill       *bool    `json:"autoRefill"`
				MinQueueAhead    *int     `json:"minQueueAhead"`
				MaxScriptWords   *int     `json:"maxScriptWords"`
				ActiveProfileID  *string  `json:"activeProfileId"`
				CustomPrompt     *string  `json:"customPrompt"`
			} `json:"dj"`
			Remote *struct {
				CloudflareEnabled      *bool   `json:"cloudflareEnabled"`
				CloudflareMode         *string `json:"cloudflareMode"`
				CloudflarePublicDomain *string `json:"cloudflarePublicDomain"`
				CloudflareToken        *string `json:"cloudflareToken"`
				ClearCloudflareToken   *bool   `json:"clearCloudflareToken"`
				PublicPartyAccess      *string `json:"publicPartyAccess"`
				PlayerURLAccess        *string `json:"playerURLAccess"`
				RadioDJURLAccess       *string `json:"radioDJURLAccess"`
				AllowUserLogin         *bool   `json:"allowUserLogin"`
				AllowPINLogin          *bool   `json:"allowPINLogin"`
				CloudflareExposeDJ     *bool   `json:"cloudflareExposeDJ"`
				CloudflareExposeParty  *bool   `json:"cloudflareExposeParty"`
				UPnPEnabled            *bool   `json:"upnpEnabled"`
				UPnPExposeParty        *bool   `json:"upnpExposeParty"`
				UPnPExposeDJ           *bool   `json:"upnpExposeDJ"`
				UPnPExternalParty      *int    `json:"upnpExternalParty"`
				UPnPExternalDJ         *int    `json:"upnpExternalDJ"`
				UPnPGatewayOverride    *string `json:"upnpGatewayOverride"`
				RequirePINOnRemote     *bool   `json:"requirePINOnRemote"`
			} `json:"remote"`
			DLNA *struct {
				Enabled      *bool   `json:"enabled"`
				FriendlyName *string `json:"friendlyName"`
			} `json:"dlna"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid settings JSON"})
			return
		}
		if req.ServerName != nil && strings.TrimSpace(*req.ServerName) != "" {
			s.cfg.ServerName = strings.TrimSpace(*req.ServerName)
		}
		if req.PairingPIN != nil && len(strings.TrimSpace(*req.PairingPIN)) >= 4 {
			s.cfg.PairingPIN = strings.TrimSpace(*req.PairingPIN)
		}
		if req.AdvertiseHost != nil {
			s.cfg.AdvertiseHost = strings.TrimSpace(*req.AdvertiseHost)
		}
		if req.ScanInterval != nil && strings.TrimSpace(*req.ScanInterval) != "" {
			raw := strings.TrimSpace(*req.ScanInterval)
			s.cfg.ScanIntervalS = raw
			if strings.EqualFold(raw, "manual") || raw == "0" {
				s.cfg.ScanInterval = 0
			} else if d, err := time.ParseDuration(raw); err == nil && d >= time.Minute {
				s.cfg.ScanInterval = d
			}
		}
		if req.ShardCount != nil && *req.ShardCount >= 1 && *req.ShardCount <= 128 {
			s.cfg.ShardCount = *req.ShardCount
			for i := range s.cfg.Libraries {
				s.cfg.Libraries[i].ShardCount = *req.ShardCount
			}
		}
		if req.AutoFetchEditorial != nil {
			s.cfg.AutoFetchEditorial = *req.AutoFetchEditorial
		}
		if req.MirrorToShare != nil {
			s.cfg.MirrorToShare = *req.MirrorToShare
		}
		if req.LidarrURL != nil {
			s.cfg.LidarrURL = strings.TrimRight(strings.TrimSpace(*req.LidarrURL), "/")
		}
		if req.LidarrAPIKey != nil && strings.TrimSpace(*req.LidarrAPIKey) != "" {
			s.cfg.LidarrAPIKey = strings.TrimSpace(*req.LidarrAPIKey)
		}
		if req.LidarrPollEnabled != nil {
			s.cfg.LidarrPollEnabled = *req.LidarrPollEnabled
		}
		if req.AdvancedAnalysisEnabled != nil {
			s.cfg.AdvancedAnalysisEnabled = *req.AdvancedAnalysisEnabled
			if s.analyzer != nil {
				if *req.AdvancedAnalysisEnabled {
					s.analyzer.TriggerRun()
				} else {
					s.analyzer.StopCurrentRun()
				}
			}
		}
		if req.MetadataBackfillModel != nil {
			s.cfg.MetadataBackfillModel = strings.TrimSpace(*req.MetadataBackfillModel)
		}
		if req.AutoMetadataBackfill != nil {
			s.cfg.AutoMetadataBackfill = *req.AutoMetadataBackfill
		}
		if req.NormEnabled != nil {
			v := *req.NormEnabled
			s.cfg.NormEnabled = &v
		}
		if req.NormTargetDB != nil && *req.NormTargetDB >= -24 && *req.NormTargetDB <= -6 {
			s.cfg.NormTargetDB = *req.NormTargetDB
		}
		if req.LegacyMacPrefix != nil && len(s.cfg.Libraries) > 0 {
			s.cfg.Libraries[0].LegacyMacPrefix = strings.TrimSpace(*req.LegacyMacPrefix)
		}
		if req.DJ != nil {
			if req.DJ.ClearGeminiKey != nil && *req.DJ.ClearGeminiKey {
				s.cfg.DJ.GeminiAPIKey = ""
			} else if req.DJ.GeminiAPIKey != nil && strings.TrimSpace(*req.DJ.GeminiAPIKey) != "" {
				s.cfg.DJ.GeminiAPIKey = strings.TrimSpace(*req.DJ.GeminiAPIKey)
			}
			if req.DJ.Provider != nil && strings.TrimSpace(*req.DJ.Provider) != "" {
				s.cfg.DJ.Provider = strings.TrimSpace(*req.DJ.Provider)
			}
			if req.DJ.LLMModel != nil && strings.TrimSpace(*req.DJ.LLMModel) != "" {
				s.cfg.DJ.LLMModel = strings.TrimSpace(*req.DJ.LLMModel)
			}
			if req.DJ.TTSModel != nil && strings.TrimSpace(*req.DJ.TTSModel) != "" {
				s.cfg.DJ.TTSModel = strings.TrimSpace(*req.DJ.TTSModel)
			}
			if req.DJ.VoiceName != nil && strings.TrimSpace(*req.DJ.VoiceName) != "" {
				s.cfg.DJ.VoiceName = strings.TrimSpace(*req.DJ.VoiceName)
			}
			if req.DJ.Persona != nil && strings.TrimSpace(*req.DJ.Persona) != "" {
				s.cfg.DJ.Persona = strings.TrimSpace(*req.DJ.Persona)
			}
			if req.DJ.Language != nil && strings.TrimSpace(*req.DJ.Language) != "" {
				s.cfg.DJ.Language = strings.TrimSpace(*req.DJ.Language)
			}
			if req.DJ.RadioStationName != nil && strings.TrimSpace(*req.DJ.RadioStationName) != "" {
				s.cfg.DJ.RadioStationName = strings.TrimSpace(*req.DJ.RadioStationName)
			}
			if req.DJ.TalkFrequency != nil {
				s.cfg.DJ.TalkFrequency = *req.DJ.TalkFrequency
			}
			if req.DJ.CrossfadeSeconds != nil {
				s.cfg.DJ.CrossfadeSeconds = *req.DJ.CrossfadeSeconds
			}
			if req.DJ.DuckVolume != nil {
				s.cfg.DJ.DuckVolume = *req.DJ.DuckVolume
			}
			if req.DJ.AutoRefill != nil {
				s.cfg.DJ.AutoRefill = *req.DJ.AutoRefill
			}
			if req.DJ.MinQueueAhead != nil {
				s.cfg.DJ.MinQueueAhead = *req.DJ.MinQueueAhead
			}
			if req.DJ.MaxScriptWords != nil {
				s.cfg.DJ.MaxScriptWords = *req.DJ.MaxScriptWords
			}
			if req.DJ.ActiveProfileID != nil {
				s.cfg.DJ.ActiveProfileID = strings.TrimSpace(*req.DJ.ActiveProfileID)
			}
			if req.DJ.CustomPrompt != nil {
				s.cfg.DJ.CustomPrompt = strings.TrimSpace(*req.DJ.CustomPrompt)
			}
			config.NormalizeDJConfig(&s.cfg.DJ)
		}
		remoteChanged := false
		if req.Remote != nil {
			remoteChanged = true
			if req.Remote.CloudflareEnabled != nil {
				s.cfg.Remote.CloudflareEnabled = *req.Remote.CloudflareEnabled
			}
			if req.Remote.CloudflareMode != nil && strings.TrimSpace(*req.Remote.CloudflareMode) != "" {
				s.cfg.Remote.CloudflareMode = strings.ToLower(strings.TrimSpace(*req.Remote.CloudflareMode))
			}
			if req.Remote.CloudflarePublicDomain != nil {
				s.cfg.Remote.CloudflarePublicDomain = strings.TrimRight(strings.TrimSpace(*req.Remote.CloudflarePublicDomain), "/")
			}
			if req.Remote.ClearCloudflareToken != nil && *req.Remote.ClearCloudflareToken {
				s.cfg.Remote.CloudflareToken = ""
			} else if req.Remote.CloudflareToken != nil && strings.TrimSpace(*req.Remote.CloudflareToken) != "" {
				s.cfg.Remote.CloudflareToken = strings.TrimSpace(*req.Remote.CloudflareToken)
			}
			if req.Remote.PublicPartyAccess != nil {
				s.cfg.Remote.PublicPartyAccess = *req.Remote.PublicPartyAccess
			}
			if req.Remote.PlayerURLAccess != nil {
				s.cfg.Remote.PlayerURLAccess = *req.Remote.PlayerURLAccess
			}
			if req.Remote.RadioDJURLAccess != nil {
				s.cfg.Remote.RadioDJURLAccess = *req.Remote.RadioDJURLAccess
			}
			if req.Remote.AllowUserLogin != nil {
				s.cfg.Remote.AllowUserLogin = *req.Remote.AllowUserLogin
			}
			if req.Remote.AllowPINLogin != nil {
				s.cfg.Remote.AllowPINLogin = *req.Remote.AllowPINLogin
			}
			if req.Remote.CloudflareExposeDJ != nil {
				s.cfg.Remote.CloudflareExposeDJ = *req.Remote.CloudflareExposeDJ
			}
			if req.Remote.CloudflareExposeParty != nil {
				s.cfg.Remote.CloudflareExposeParty = *req.Remote.CloudflareExposeParty
			}
			if req.Remote.UPnPEnabled != nil {
				s.cfg.Remote.UPnPEnabled = *req.Remote.UPnPEnabled
			}
			if req.Remote.UPnPExposeParty != nil {
				s.cfg.Remote.UPnPExposeParty = *req.Remote.UPnPExposeParty
			}
			if req.Remote.UPnPExposeDJ != nil {
				s.cfg.Remote.UPnPExposeDJ = *req.Remote.UPnPExposeDJ
			}
			if req.Remote.UPnPExternalParty != nil {
				s.cfg.Remote.UPnPExternalParty = *req.Remote.UPnPExternalParty
			}
			if req.Remote.UPnPExternalDJ != nil {
				s.cfg.Remote.UPnPExternalDJ = *req.Remote.UPnPExternalDJ
			}
			if req.Remote.UPnPGatewayOverride != nil {
				s.cfg.Remote.UPnPGatewayOverride = strings.TrimSpace(*req.Remote.UPnPGatewayOverride)
			}
			if req.Remote.RequirePINOnRemote != nil {
				s.cfg.Remote.RequirePINOnRemote = *req.Remote.RequirePINOnRemote
			}
			config.NormalizeRemoteConfig(&s.cfg.Remote)
		}
		if req.DLNA != nil {
			if req.DLNA.Enabled != nil {
				s.cfg.DLNA.Enabled = *req.DLNA.Enabled
			}
			if req.DLNA.FriendlyName != nil {
				s.cfg.DLNA.FriendlyName = strings.TrimSpace(*req.DLNA.FriendlyName)
			}
		}
		_ = s.cfg.Save()
		if remoteChanged {
			if s.tunnel != nil {
				s.tunnel.Reconcile()
			}
			if s.netmap != nil {
				s.netmap.TriggerSync()
			}
		}
	}

	legacyPrefix := ""
	if len(s.cfg.Libraries) > 0 {
		legacyPrefix = s.cfg.Libraries[0].LegacyMacPrefix
	}
	djCfg := s.cfg.DJ
	config.NormalizeDJConfig(&djCfg)
	normTargetDB, normEnabled := s.cfg.EffectiveNormTargetDB()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                      true,
		"serverName":              s.cfg.ServerName,
		"pairingPIN":              s.cfg.PairingPIN,
		"advertiseHost":           s.cfg.EffectiveAdvertiseHost(),
		"scanInterval":            s.cfg.ScanIntervalS,
		"shardCount":              s.cfg.ShardCount,
		"autoFetchEditorial":      s.cfg.AutoFetchEditorial,
		"mirrorToShare":           s.cfg.MirrorToShare,
		"lidarrURL":               s.cfg.LidarrURL,
		"hasLidarrAPIKey":         strings.TrimSpace(s.cfg.LidarrAPIKey) != "",
		"lidarrAPIKeyMask":        maskSecret(s.cfg.LidarrAPIKey),
		"lidarrPollEnabled":       s.cfg.LidarrPollEnabled,
		"advancedAnalysisEnabled": s.cfg.AdvancedAnalysisEnabled,
		"metadataBackfillModel":   s.cfg.MetadataBackfillModel,
		"autoMetadataBackfill":    s.cfg.AutoMetadataBackfill,
		"normEnabled":             normEnabled,
		"normTargetDB":            normTargetDB,
		"legacyMacPrefix":         legacyPrefix,
		"dj": map[string]any{
			"provider":          djCfg.Provider,
			"hasGeminiAPIKey":   strings.TrimSpace(djCfg.GeminiAPIKey) != "",
			"geminiAPIKeyMask":  maskSecret(djCfg.GeminiAPIKey),
			"llmModel":          djCfg.LLMModel,
			"ttsModel":          djCfg.TTSModel,
			"voiceName":         djCfg.VoiceName,
			"persona":           djCfg.Persona,
			"language":          djCfg.Language,
			"radioStationName":  djCfg.RadioStationName,
			"talkFrequency":     djCfg.TalkFrequency,
			"crossfadeSeconds":  djCfg.CrossfadeSeconds,
			"duckVolume":        djCfg.DuckVolume,
			"autoRefill":        djCfg.AutoRefill,
			"minQueueAhead":     djCfg.MinQueueAhead,
			"maxScriptWords":    djCfg.MaxScriptWords,
			"activeProfileId":   djCfg.ActiveProfileID,
			"profiles":          djCfg.Profiles,
			"customPrompt":      djCfg.CustomPrompt,
		},
		"remote": map[string]any{
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
			"upnpExposeDJ":          s.cfg.Remote.UPnPExposeDJ,
			"upnpExternalParty":     s.cfg.Remote.UPnPExternalParty,
			"upnpExternalDJ":        s.cfg.Remote.UPnPExternalDJ,
			"upnpGatewayOverride":   s.cfg.Remote.UPnPGatewayOverride,
			"requirePINOnRemote":    s.cfg.Remote.RequirePINOnRemote,
		},
		"dlna":      s.cfg.DLNA,
		"libraries": s.cfg.Libraries,
	})
}

func (s *Server) handleAdminEditorialEnrich(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.editorial != nil {
		s.editorial.TriggerEnrichment()
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "message": "Editorial enrichment triggered"})
}

func (s *Server) handleAdminScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		LibraryID string `json:"libraryId"`
		SubPath   string `json:"subPath"`
		Sync      bool   `json:"sync"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req)

	run := func() ([]indexer.ScanSummary, error) {
		if req.LibraryID != "" {
			if lib, ok := s.cfg.LibraryByID(req.LibraryID); ok {
				sum, err := s.idx.ScanLibrary(context.Background(), lib, req.SubPath)
				if err != nil {
					return nil, err
				}
				return []indexer.ScanSummary{sum}, nil
			}
			return nil, fmt.Errorf("unknown libraryId: %s", req.LibraryID)
		}
		return s.idx.ScanAll(context.Background())
	}

	if req.Sync {
		sums, err := run()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "summaries": sums})
		return
	}

	go func() {
		_, _ = run()
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "message": "Scan started in background"})
}

func (s *Server) handleLidarrWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var payload lidarr.WebhookPayload
	_ = json.NewDecoder(io.LimitReader(r.Body, 262_144)).Decode(&payload)

	if s.lidarr != nil {
		ev := s.lidarr.HandleWebhook(payload)
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "eventType": ev.EventType, "nudge": ev})
		return
	}

	// Fallback if lidarr manager is not attached
	go func() {
		targetDir := filepath.Base(strings.TrimSpace(payload.Artist.Path))
		if targetDir == "" || targetDir == "." || targetDir == "/" {
			targetDir = strings.TrimSpace(payload.Artist.Name)
		}
		for _, lib := range s.cfg.Libraries {
			sub := ""
			if targetDir != "" {
				if fi, err := os.Stat(filepath.Join(lib.Path, targetDir)); err == nil && fi.IsDir() {
					sub = targetDir
				}
			}
			_, _ = s.idx.ScanLibrary(context.Background(), lib, sub)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "eventType": payload.EventType})
}

// --- Helpers ---

func safeJoin(root, rel string) (string, error) {
	cleanRoot := filepath.Clean(root)
	joined := filepath.Clean(filepath.Join(cleanRoot, filepath.FromSlash(rel)))
	relCheck, err := filepath.Rel(cleanRoot, joined)
	if err != nil || strings.HasPrefix(relCheck, "..") || filepath.IsAbs(relCheck) {
		return "", fmt.Errorf("path escape detected")
	}
	return joined, nil
}

func mimeForPath(p string) string {
	switch strings.ToLower(filepath.Ext(p)) {
	case ".flac":
		return "audio/flac"
	case ".mp3":
		return "audio/mpeg"
	case ".m4a", ".mp4", ".aac":
		return "audio/mp4"
	case ".wav", ".wave":
		return "audio/wav"
	case ".aiff", ".aif":
		return "audio/aiff"
	case ".ogg", ".oga":
		return "audio/ogg"
	case ".opus":
		return "audio/opus"
	default:
		return "application/octet-stream"
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeMaybeGzipJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && len(raw) > 1024 {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write(raw)
		_ = gz.Close()
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
		w.WriteHeader(status)
		_, _ = w.Write(buf.Bytes())
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}
