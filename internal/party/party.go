package party

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/baardove/my-echo/server/internal/artwork"
	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
	"github.com/google/uuid"
)

//go:embed webui.html
var guestWebUIHTML []byte

// Wire models matching Sources/EchoCore/Party/PartyModeServer.swift & tvOS/Sources/Models/TVModels.swift

type TrackItem struct {
	ID          string  `json:"id"`
	Title       string  `json:"title"`
	Artist      string  `json:"artist"`
	Album       string  `json:"album"`
	Duration    float64 `json:"duration"`
	Year        *int    `json:"year,omitempty"`
	Format      string  `json:"format"`
	ArtworkHash *string `json:"artworkHash,omitempty"`
	InPlaylist  bool    `json:"inPlaylist"`
	InUpNext    bool    `json:"inUpNext"`
}

type QueueEntry struct {
	ID                 string     `json:"id"`
	Position           int        `json:"position"`
	Track              TrackItem  `json:"track"`
	IsCurrent          bool       `json:"isCurrent"`
	IsPlayed           bool       `json:"isPlayed"`
	AddedBy            *string    `json:"addedBy,omitempty"`
	AddedByGuestID     *string    `json:"addedByGuestID,omitempty"`
	AddedAt            *time.Time `json:"addedAt,omitempty"`
	PlannedPlayAt      *time.Time `json:"plannedPlayAt,omitempty"`
	HasVoiceIntro      bool       `json:"hasVoiceIntro"`
	VoiceIntroDuration *float64   `json:"voiceIntroDuration,omitempty"`
}

type GuestPresence struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	IsActive     bool      `json:"isActive"`
	RequestCount int       `json:"requestCount"`
	FirstSeenAt  time.Time `json:"firstSeenAt"`
	LastSeenAt   time.Time `json:"lastSeenAt"`
}

type ChatMessage struct {
	ID         string    `json:"id"`
	GuestID    *string   `json:"guestID,omitempty"`
	SenderName string    `json:"senderName"`
	IsHost     bool      `json:"isHost"`
	Text       string    `json:"text"`
	SentAt     time.Time `json:"sentAt"`
}

type PlaylistState struct {
	PlaylistName         string          `json:"playlistName"`
	ServerName           string          `json:"serverName"`
	IsPlaying            bool            `json:"isPlaying"`
	IsPlayingVoiceIntro  bool            `json:"isPlayingVoiceIntro"`
	CurrentTrackID       *string         `json:"currentTrackID,omitempty"`
	CurrentIndex         *int            `json:"currentIndex,omitempty"`
	CurrentTime          float64         `json:"currentTime"`
	Duration             float64         `json:"duration"`
	LibraryTotalCount    int             `json:"libraryTotalCount"`
	UpNextCount          int             `json:"upNextCount"`
	AllowReRequestPlayed bool            `json:"allowReRequestPlayed"`
	ChatEnabled          bool            `json:"chatEnabled"`
	StreamingEnabled     bool            `json:"streamingEnabled"`
	MaxIntroSeconds      int             `json:"maxIntroSeconds"`
	HTTPSPort            *int            `json:"httpsPort,omitempty"`
	StateUpdatedAt       time.Time       `json:"stateUpdatedAt"`
	Revision             int             `json:"revision"`
	Tracks               []QueueEntry    `json:"tracks"`
	Guests               []GuestPresence `json:"guests"`
	ChatMessages         []ChatMessage   `json:"chatMessages"`
	// Extended server-hosted fields (backward-compatible)
	ActiveRendererID   string `json:"activeRendererID,omitempty"`
	ActiveRendererName string `json:"activeRendererName,omitempty"`
}

type Status struct {
	IsLive       bool    `json:"isLive"`
	Port         *int    `json:"port,omitempty"`
	LanIP        *string `json:"lanIP,omitempty"`
	JoinURL      *string `json:"joinURL,omitempty"`
	PlaylistName string  `json:"playlistName"`
}

type Envelope struct {
	Status Status         `json:"status"`
	State  *PlaylistState `json:"state,omitempty"`
}

type SearchResponse struct {
	Query        string      `json:"query"`
	Scope        string      `json:"scope"`
	Sort         string      `json:"sort"`
	TotalMatches int         `json:"totalMatches"`
	Offset       int         `json:"offset"`
	Limit        int         `json:"limit"`
	HasMore      bool        `json:"hasMore"`
	Tracks       []TrackItem `json:"tracks"`
}

type TrackInfoResponse struct {
	OK                    bool     `json:"ok"`
	TrackID               string   `json:"trackId"`
	Title                 string   `json:"title"`
	Artist                string   `json:"artist"`
	Album                 string   `json:"album"`
	Year                  *int     `json:"year,omitempty"`
	Duration              float64  `json:"duration"`
	Format                string   `json:"format"`
	ArtworkHash           *string  `json:"artworkHash,omitempty"`
	AddedBy               *string  `json:"addedBy,omitempty"`
	ReleaseDate           *string  `json:"releaseDate,omitempty"`
	Label                 *string  `json:"label,omitempty"`
	Genres                []string `json:"genres"`
	AlbumFans             *int     `json:"albumFans,omitempty"`
	AlbumSummary          *string  `json:"albumSummary,omitempty"`
	AlbumTrackCount       int      `json:"albumTrackCount"`
	ArtistSummary         *string  `json:"artistSummary,omitempty"`
	ArtistFans            *int     `json:"artistFans,omitempty"`
	ArtistAlbumCount      *int     `json:"artistAlbumCount,omitempty"`
	ArtistImageURL        *string  `json:"artistImageURL,omitempty"`
	LocalArtistAlbumCount int      `json:"localArtistAlbumCount"`
	LocalArtistTrackCount int      `json:"localArtistTrackCount"`
}

type persistedQueueItem struct {
	TrackID            string    `json:"trackId"`
	AddedBy            string    `json:"addedBy,omitempty"`
	AddedByGuestID     string    `json:"addedByGuestId,omitempty"`
	AddedAt            time.Time `json:"addedAt"`
	IntroPath          string    `json:"introPath,omitempty"`
	VoiceIntroDuration float64   `json:"voiceIntroDuration,omitempty"`
}

type persistedSession struct {
	IsLive               bool                 `json:"isLive"`
	PlaylistName         string               `json:"playlistName"`
	IsPlaying            bool                 `json:"isPlaying"`
	CurrentIndex         int                  `json:"currentIndex"`
	CurrentTime          float64              `json:"currentTime"`
	AllowReRequestPlayed bool                 `json:"allowReRequestPlayed"`
	ChatEnabled          bool                 `json:"chatEnabled"`
	StreamingEnabled     bool                 `json:"streamingEnabled"`
	MaxIntroSeconds      int                  `json:"maxIntroSeconds"`
	Queue                []persistedQueueItem `json:"queue"`
	Chat                 []ChatMessage        `json:"chat"`
}

type indexedEntry struct {
	rec              db.TrackRecord
	effectiveID      string
	fullPath         string
	displayArtist    string
	displayAlbum     string
	formatBadge      string
	titleLower       string
	artistLower      string
	albumArtistLower string
	albumLower       string
	genreLower       string
	yearString       string
	arrivalUnix      int64
}

type guestRec struct {
	ID           string
	Name         string
	RequestCount int
	FirstSeenAt  time.Time
	LastSeenAt   time.Time
}

// Engine manages server-hosted always-on Party Mode state, catalog search, voice intros, and renderer coordination.
type Engine struct {
	cfg      *config.Config
	store    *db.Store
	artCache *artwork.Cache

	mu                   sync.RWMutex
	isLive               bool
	playlistName         string
	isPlaying            bool
	isPlayingVoiceIntro  bool
	currentIndex         int
	currentTime          float64
	stateUpdatedAt       time.Time
	revision             int
	allowReRequestPlayed bool
	chatEnabled          bool
	streamingEnabled     bool
	maxIntroSeconds      int
	httpsPort            int

	activeRendererID   string
	activeRendererName string
	rendererLastSeen   time.Time

	queue  []persistedQueueItem
	chat   []ChatMessage
	guests map[string]*guestRec

	entries        []indexedEntry
	entryByID      map[string]int
	sortedByTitle  []int
	sortedByRecent []int
}

func NewEngine(cfg *config.Config, store *db.Store, artCache *artwork.Cache) *Engine {
	_ = os.MkdirAll(cfg.IntrosDir(), 0o755)
	e := &Engine{
		cfg:                  cfg,
		store:                store,
		artCache:             artCache,
		playlistName:         "Partymode",
		currentIndex:         -1,
		stateUpdatedAt:       time.Now().UTC(),
		allowReRequestPlayed: true,
		chatEnabled:          true,
		streamingEnabled:     true,
		maxIntroSeconds:      15,
		httpsPort:            cfg.PartyHTTPSPort,
		guests:               make(map[string]*guestRec),
		entryByID:            make(map[string]int),
	}
	e.loadPersisted()
	e.ReloadCatalog()
	return e
}

func (e *Engine) SetHTTPSPort(port int) {
	e.mu.Lock()
	e.httpsPort = port
	e.mu.Unlock()
}

func (e *Engine) ReloadCatalog() {
	tracks, err := e.store.LoadAllTracks()
	if err != nil {
		return
	}
	rootByLib := make(map[string]string, len(e.cfg.Libraries))
	for _, lib := range e.cfg.Libraries {
		rootByLib[lib.ID] = lib.Path
	}

	entries := make([]indexedEntry, 0, len(tracks))
	byID := make(map[string]int, len(tracks)*2)
	for _, tr := range tracks {
		root, ok := rootByLib[tr.LibraryID]
		if !ok {
			continue
		}
		dispArtist := strings.TrimSpace(tr.Artist)
		if dispArtist == "" {
			dispArtist = strings.TrimSpace(tr.AlbumArtist)
		}
		if dispArtist == "" {
			dispArtist = "Unknown Artist"
		}
		dispAlbum := strings.TrimSpace(tr.Album)
		if dispAlbum == "" {
			dispAlbum = "Unknown Album"
		}
		var genre string
		if tr.Genre != nil {
			genre = strings.ToLower(*tr.Genre)
		}
		var yearStr string
		if tr.Year != nil {
			yearStr = strconv.Itoa(*tr.Year)
		}
		arr := tr.ModifiedAt.Unix()
		if tr.AddedAt != nil && tr.AddedAt.Unix() > arr {
			arr = tr.AddedAt.Unix()
		}
		effID := tr.EffectiveID()
		idx := len(entries)
		entries = append(entries, indexedEntry{
			rec:              tr,
			effectiveID:      effID,
			fullPath:         filepath.Join(root, filepath.FromSlash(tr.RelativePath)),
			displayArtist:    dispArtist,
			displayAlbum:     dispAlbum,
			formatBadge:      formatDisplayBadge(tr.Format),
			titleLower:       strings.ToLower(tr.Title),
			artistLower:      strings.ToLower(dispArtist),
			albumArtistLower: strings.ToLower(tr.AlbumArtist),
			albumLower:       strings.ToLower(dispAlbum),
			genreLower:       genre,
			yearString:       yearStr,
			arrivalUnix:      arr,
		})
		byID[effID] = idx
		byID[tr.TrackKey] = idx
		if tr.LegacyID != "" {
			byID[tr.LegacyID] = idx
		}
	}

	byTitle := make([]int, len(entries))
	byRecent := make([]int, len(entries))
	for i := range entries {
		byTitle[i] = i
		byRecent[i] = i
	}
	sort.Slice(byTitle, func(i, j int) bool {
		a, b := entries[byTitle[i]], entries[byTitle[j]]
		if a.titleLower != b.titleLower {
			return a.titleLower < b.titleLower
		}
		if a.artistLower != b.artistLower {
			return a.artistLower < b.artistLower
		}
		return a.albumLower < b.albumLower
	})
	sort.Slice(byRecent, func(i, j int) bool {
		a, b := entries[byRecent[i]], entries[byRecent[j]]
		if a.arrivalUnix != b.arrivalUnix {
			return a.arrivalUnix > b.arrivalUnix
		}
		return a.titleLower < b.titleLower
	})

	e.mu.Lock()
	e.entries = entries
	e.entryByID = byID
	e.sortedByTitle = byTitle
	e.sortedByRecent = byRecent
	e.mu.Unlock()
}

func (e *Engine) loadPersisted() {
	raw, ok := e.store.GetKV("party.session")
	if !ok || raw == "" {
		return
	}
	var ps persistedSession
	if err := json.Unmarshal([]byte(raw), &ps); err != nil {
		return
	}
	e.isLive = ps.IsLive
	if ps.PlaylistName != "" {
		e.playlistName = ps.PlaylistName
	}
	e.isPlaying = ps.IsPlaying
	e.currentIndex = ps.CurrentIndex
	e.currentTime = ps.CurrentTime
	e.allowReRequestPlayed = ps.AllowReRequestPlayed
	e.chatEnabled = ps.ChatEnabled
	e.streamingEnabled = ps.StreamingEnabled
	if ps.MaxIntroSeconds > 0 {
		e.maxIntroSeconds = ps.MaxIntroSeconds
	}
	e.queue = ps.Queue
	e.chat = ps.Chat
}

func (e *Engine) persistLocked() {
	ps := persistedSession{
		IsLive:               e.isLive,
		PlaylistName:         e.playlistName,
		IsPlaying:            e.isPlaying,
		CurrentIndex:         e.currentIndex,
		CurrentTime:          e.currentTime,
		AllowReRequestPlayed: e.allowReRequestPlayed,
		ChatEnabled:          e.chatEnabled,
		StreamingEnabled:     e.streamingEnabled,
		MaxIntroSeconds:      e.maxIntroSeconds,
		Queue:                e.queue,
		Chat:                 e.chat,
	}
	if raw, err := json.Marshal(ps); err == nil {
		_ = e.store.SetKV("party.session", string(raw))
	}
}

func (e *Engine) Status() Status {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.statusLocked()
}

func (e *Engine) statusLocked() Status {
	host := e.cfg.EffectiveAdvertiseHost()
	port := e.cfg.PartyHTTPPort
	if !e.isLive {
		return Status{IsLive: false, PlaylistName: e.playlistName}
	}
	join := fmt.Sprintf("http://%s:%d", host, port)
	return Status{
		IsLive:       true,
		Port:         &port,
		LanIP:        &host,
		JoinURL:      &join,
		PlaylistName: e.playlistName,
	}
}

func (e *Engine) Envelope() Envelope {
	e.mu.RLock()
	defer e.mu.RUnlock()
	st := e.statusLocked()
	if !st.IsLive {
		return Envelope{Status: st, State: nil}
	}
	ps := e.snapshotStateLocked(time.Now().UTC())
	return Envelope{Status: st, State: &ps}
}

func (e *Engine) StartSession(rendererID, rendererName string) Envelope {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLive = true
	if rendererID != "" {
		e.activeRendererID = rendererID
		e.activeRendererName = rendererName
		e.rendererLastSeen = time.Now().UTC()
	}
	if e.currentIndex < 0 && len(e.queue) > 0 {
		e.currentIndex = 0
		e.currentTime = 0
	}
	e.stateUpdatedAt = time.Now().UTC()
	e.revision++
	e.persistLocked()
	ps := e.snapshotStateLocked(e.stateUpdatedAt)
	return Envelope{Status: e.statusLocked(), State: &ps}
}

func (e *Engine) StopSession() Envelope {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLive = false
	e.isPlaying = false
	e.isPlayingVoiceIntro = false
	e.activeRendererID = ""
	e.activeRendererName = ""
	e.stateUpdatedAt = time.Now().UTC()
	e.revision++
	e.persistLocked()
	return Envelope{Status: e.statusLocked(), State: nil}
}

// HandleHostCommand executes PIN-authenticated host commands (from Apple TV, Mac, or Admin Web UI).
func (e *Engine) HandleHostCommand(cmd map[string]any) (bool, string, PlaylistState) {
	action, _ := cmd["action"].(string)
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()

	switch action {
	case "play":
		if e.currentIndex < 0 && len(e.queue) > 0 {
			e.currentIndex = 0
		}
		if e.currentIndex >= 0 && e.currentIndex < len(e.queue) {
			e.isPlaying = true
			e.stateUpdatedAt = now
		}
	case "pause":
		e.advanceClockLocked(now)
		e.isPlaying = false
	case "togglePlayPause":
		e.advanceClockLocked(now)
		if e.isPlaying {
			e.isPlaying = false
		} else {
			if e.currentIndex < 0 && len(e.queue) > 0 {
				e.currentIndex = 0
			}
			if e.currentIndex >= 0 && e.currentIndex < len(e.queue) {
				e.isPlaying = true
			}
		}
	case "next":
		if e.currentIndex+1 < len(e.queue) {
			e.currentIndex++
			e.currentTime = 0
			e.isPlayingVoiceIntro = false
			e.isPlaying = true
			e.stateUpdatedAt = now
		} else {
			e.isPlaying = false
		}
	case "previous":
		if e.currentTime > 3 {
			e.currentTime = 0
		} else if e.currentIndex > 0 {
			e.currentIndex--
			e.currentTime = 0
		}
		e.isPlayingVoiceIntro = false
		e.stateUpdatedAt = now
	case "jumpToIndex":
		if f, ok := cmd["index"].(float64); ok {
			idx := int(f)
			if idx >= 0 && idx < len(e.queue) {
				e.currentIndex = idx
				e.currentTime = 0
				e.isPlaying = true
				e.isPlayingVoiceIntro = false
				e.stateUpdatedAt = now
			}
		}
	case "removeIndex":
		if f, ok := cmd["index"].(float64); ok {
			idx := int(f)
			if idx >= 0 && idx < len(e.queue) {
				if e.queue[idx].IntroPath != "" {
					_ = os.Remove(e.queue[idx].IntroPath)
				}
				e.queue = append(e.queue[:idx], e.queue[idx+1:]...)
				if idx < e.currentIndex {
					e.currentIndex--
				} else if idx == e.currentIndex {
					if e.currentIndex >= len(e.queue) {
						e.currentIndex = len(e.queue) - 1
						e.isPlaying = false
					}
					e.currentTime = 0
				}
			}
		}
	case "clearPlayed":
		if e.currentIndex > 0 && e.currentIndex < len(e.queue) {
			for i := 0; i < e.currentIndex; i++ {
				if e.queue[i].IntroPath != "" {
					_ = os.Remove(e.queue[i].IntroPath)
				}
			}
			e.queue = append([]persistedQueueItem(nil), e.queue[e.currentIndex:]...)
			e.currentIndex = 0
		}
	case "hostChat":
		if txt, ok := cmd["text"].(string); ok {
			clean := sanitizeChatText(txt)
			if clean != "" {
				sender := "Host"
				if n, ok := cmd["senderName"].(string); ok && strings.TrimSpace(n) != "" {
					sender = sanitizeGuestName(n)
				}
				e.chat = append(e.chat, ChatMessage{
					ID:         uuid.NewString(),
					SenderName: sender,
					IsHost:     true,
					Text:       clean,
					SentAt:     now,
				})
				if len(e.chat) > 200 {
					e.chat = e.chat[len(e.chat)-200:]
				}
			}
		}
	case "updateSettings":
		if v, ok := cmd["allowReRequestPlayed"].(bool); ok {
			e.allowReRequestPlayed = v
		}
		if v, ok := cmd["chatEnabled"].(bool); ok {
			e.chatEnabled = v
		}
		if v, ok := cmd["streamingEnabled"].(bool); ok {
			e.streamingEnabled = v
		}
		if v, ok := cmd["maxIntroSeconds"].(float64); ok && int(v) >= 5 && int(v) <= 60 {
			e.maxIntroSeconds = int(v)
		}
	default:
		return false, "Unknown host command: " + action, e.snapshotStateLocked(now)
	}

	e.revision++
	e.persistLocked()
	return true, "OK", e.snapshotStateLocked(now)
}

func (e *Engine) advanceClockLocked(now time.Time) {
	if !e.isPlaying || e.currentIndex < 0 || e.currentIndex >= len(e.queue) {
		e.stateUpdatedAt = now
		return
	}
	elapsed := now.Sub(e.stateUpdatedAt).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	dur := e.trackDurationLocked(e.queue[e.currentIndex].TrackID)
	nextTime := e.currentTime + elapsed
	if dur > 0 && nextTime > dur {
		nextTime = dur
	}
	e.currentTime = nextTime
	e.stateUpdatedAt = now
}

func (e *Engine) trackDurationLocked(trackID string) float64 {
	if idx, ok := e.entryByID[trackID]; ok {
		return e.entries[idx].rec.Duration
	}
	return 0
}

func (e *Engine) snapshotStateLocked(now time.Time) PlaylistState {
	curTime := e.currentTime
	var curTrackID *string
	var curIdxPtr *int
	var curDur float64
	if e.currentIndex >= 0 && e.currentIndex < len(e.queue) {
		ci := e.currentIndex
		curIdxPtr = &ci
		tid := e.queue[ci].TrackID
		curTrackID = &tid
		curDur = e.trackDurationLocked(tid)
		if e.isPlaying {
			elapsed := now.Sub(e.stateUpdatedAt).Seconds()
			if elapsed > 0 {
				curTime += elapsed
				if curDur > 0 && curTime > curDur {
					curTime = curDur
				}
			}
		}
	}

	inPlaylistSet := make(map[string]bool, len(e.queue))
	upNextSet := make(map[string]bool, len(e.queue))
	for i, qi := range e.queue {
		inPlaylistSet[qi.TrackID] = true
		if e.currentIndex < 0 || i >= e.currentIndex {
			upNextSet[qi.TrackID] = true
		}
	}

	// Build queue entries + live HH:mm scheduled play timestamps
	tracksOut := make([]QueueEntry, 0, len(e.queue))
	remainingCur := max(0.0, curDur-curTime)
	cursorTime := now.Add(time.Duration(remainingCur * float64(time.Second)))
	upNextCount := 0

	for i, qi := range e.queue {
		idx, ok := e.entryByID[qi.TrackID]
		if !ok {
			continue
		}
		ent := e.entries[idx]
		isCur := i == e.currentIndex
		isPlayed := e.currentIndex >= 0 && i < e.currentIndex
		var planned *time.Time
		if !isPlayed && !isCur {
			t := cursorTime
			planned = &t
			cursorTime = cursorTime.Add(time.Duration(ent.rec.Duration * float64(time.Second)))
			upNextCount++
		}
		var addedBy *string
		if qi.AddedBy != "" {
			v := qi.AddedBy
			addedBy = &v
		}
		var addedByGID *string
		if qi.AddedByGuestID != "" {
			v := qi.AddedByGuestID
			addedByGID = &v
		}
		addedAt := qi.AddedAt
		var introDur *float64
		hasIntro := qi.IntroPath != ""
		if hasIntro && qi.VoiceIntroDuration > 0 {
			d := qi.VoiceIntroDuration
			introDur = &d
		}

		tracksOut = append(tracksOut, QueueEntry{
			ID:       fmt.Sprintf("%d:%s", i, ent.effectiveID),
			Position: i,
			Track: TrackItem{
				ID:          ent.effectiveID,
				Title:       ent.rec.Title,
				Artist:      ent.displayArtist,
				Album:       ent.displayAlbum,
				Duration:    ent.rec.Duration,
				Year:        ent.rec.Year,
				Format:      ent.formatBadge,
				ArtworkHash: ent.rec.ArtworkHash,
				InPlaylist:  inPlaylistSet[ent.effectiveID],
				InUpNext:    upNextSet[ent.effectiveID],
			},
			IsCurrent:          isCur,
			IsPlayed:           isPlayed,
			AddedBy:            addedBy,
			AddedByGuestID:     addedByGID,
			AddedAt:            &addedAt,
			PlannedPlayAt:      planned,
			HasVoiceIntro:      hasIntro,
			VoiceIntroDuration: introDur,
		})
	}

	guestsOut := e.buildGuestsLocked(now)
	var httpsPortPtr *int
	if e.httpsPort > 0 {
		p := e.httpsPort
		httpsPortPtr = &p
	}

	chatOut := e.chat
	if chatOut == nil {
		chatOut = []ChatMessage{}
	}

	// Expire stale renderer lease after 20s
	rendererID := e.activeRendererID
	rendererName := e.activeRendererName
	if rendererID != "" && now.Sub(e.rendererLastSeen) > 20*time.Second {
		rendererID = ""
		rendererName = ""
	}

	return PlaylistState{
		PlaylistName:         e.playlistName,
		ServerName:           e.cfg.ServerName,
		IsPlaying:            e.isPlaying,
		IsPlayingVoiceIntro:  e.isPlayingVoiceIntro,
		CurrentTrackID:       curTrackID,
		CurrentIndex:         curIdxPtr,
		CurrentTime:          curTime,
		Duration:             curDur,
		LibraryTotalCount:    len(e.entries),
		UpNextCount:          upNextCount,
		AllowReRequestPlayed: e.allowReRequestPlayed,
		ChatEnabled:          e.chatEnabled,
		StreamingEnabled:     e.streamingEnabled,
		MaxIntroSeconds:      e.maxIntroSeconds,
		HTTPSPort:            httpsPortPtr,
		StateUpdatedAt:       now,
		Revision:             e.revision,
		Tracks:               tracksOut,
		Guests:               guestsOut,
		ChatMessages:         chatOut,
		ActiveRendererID:     rendererID,
		ActiveRendererName:   rendererName,
	}
}

func (e *Engine) buildGuestsLocked(now time.Time) []GuestPresence {
	out := make([]GuestPresence, 0, len(e.guests))
	for _, g := range e.guests {
		out = append(out, GuestPresence{
			ID:           g.ID,
			Name:         g.Name,
			IsActive:     now.Sub(g.LastSeenAt) <= 15*time.Second,
			RequestCount: g.RequestCount,
			FirstSeenAt:  g.FirstSeenAt,
			LastSeenAt:   g.LastSeenAt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsActive != out[j].IsActive {
			return out[i].IsActive
		}
		if !out[i].LastSeenAt.Equal(out[j].LastSeenAt) {
			return out[i].LastSeenAt.After(out[j].LastSeenAt)
		}
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})
	return out
}

func (e *Engine) touchGuestLocked(guestID, guestName string, incReq bool) {
	id := sanitizeGuestID(guestID)
	if id == "" {
		return
	}
	name := sanitizeGuestName(guestName)
	now := time.Now().UTC()
	if g, ok := e.guests[id]; ok {
		if name != "" {
			g.Name = name
		}
		if incReq {
			g.RequestCount++
		}
		g.LastSeenAt = now
	} else {
		if name == "" {
			name = "Guest"
		}
		req := 0
		if incReq {
			req = 1
		}
		e.guests[id] = &guestRec{
			ID:           id,
			Name:         name,
			RequestCount: req,
			FirstSeenAt:  now,
			LastSeenAt:   now,
		}
	}
}

// Handler returns the HTTP handler for the Party Mode Guest Web UI & APIs (served on :53333 HTTP & :53334 HTTPS).
func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", e.handleWebUI)
	mux.HandleFunc("/index.html", e.handleWebUI)
	mux.HandleFunc("/api/party/state", e.handleState)
	mux.HandleFunc("/api/party/tracks", e.handleTracks)
	mux.HandleFunc("/api/party/add", e.handleAdd)
	mux.HandleFunc("/api/party/guest/rename", e.handleRenameGuest)
	mux.HandleFunc("/api/party/chat", e.handleChat)
	mux.HandleFunc("/api/party/intro", e.handleIntro)
	mux.HandleFunc("/api/party/intro/delete", e.handleIntroDelete)
	mux.HandleFunc("/api/party/intro/", e.handleIntroStream)
	mux.HandleFunc("/api/party/stream/", e.handleStream)
	mux.HandleFunc("/api/party/track-info", e.handleTrackInfo)
	mux.HandleFunc("/api/party/artwork/", e.handleArtwork)
	// Authenticated Host & Renderer coordination endpoints
	mux.HandleFunc("/api/party/session/start", e.handleSessionStart)
	mux.HandleFunc("/api/party/session/stop", e.handleSessionStop)
	mux.HandleFunc("/api/party/host/command", e.handleHostCommandHTTP)
	mux.HandleFunc("/api/party/renderer/heartbeat", e.handleRendererHeartbeat)
	mux.HandleFunc("/api/party/renderer/ended", e.handleRendererEnded)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Echo-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (e *Engine) isAuthorized(r *http.Request) bool {
	tok := strings.TrimSpace(r.Header.Get("X-Echo-Token"))
	if tok == "" {
		tok = strings.TrimSpace(r.URL.Query().Get("token"))
	}
	return tok != "" && tok == e.cfg.PairingPIN
}

func (e *Engine) handleWebUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(guestWebUIHTML)
}

func (e *Engine) handleState(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	e.mu.Lock()
	e.touchGuestLocked(q.Get("guestId"), q.Get("guestName"), false)
	st := e.snapshotStateLocked(time.Now().UTC())
	e.mu.Unlock()
	writeJSON(w, http.StatusOK, st)
}

func (e *Engine) handleTracks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := strings.TrimSpace(q.Get("q"))
	scope := strings.ToLower(strings.TrimSpace(q.Get("scope")))
	if scope == "" {
		scope = "all"
	}
	sortMode := strings.ToLower(strings.TrimSpace(q.Get("sort")))
	if sortMode == "" {
		sortMode = "title"
	}
	seed, _ := strconv.ParseUint(q.Get("seed"), 10, 64)
	offset, _ := strconv.Atoi(q.Get("offset"))
	if offset < 0 {
		offset = 0
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > 150 {
		limit = 150
	}

	e.mu.RLock()
	inPl := make(map[string]bool, len(e.queue))
	upNext := make(map[string]bool, len(e.queue))
	for i, qi := range e.queue {
		inPl[qi.TrackID] = true
		if e.currentIndex < 0 || i >= e.currentIndex {
			upNext[qi.TrackID] = true
		}
	}
	resp := e.searchTracksLocked(query, scope, sortMode, seed, offset, limit, inPl, upNext)
	e.mu.RUnlock()
	writeJSON(w, http.StatusOK, resp)
}

func (e *Engine) handleAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONErr(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		TrackID   string `json:"trackId"`
		GuestName string `json:"guestName"`
		GuestID   string `json:"guestId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&req); err != nil || strings.TrimSpace(req.TrackID) == "" {
		writeJSONErr(w, http.StatusBadRequest, "Missing or invalid trackId")
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	guestName := sanitizeGuestName(req.GuestName)
	guestID := sanitizeGuestID(req.GuestID)

	idx, ok := e.entryByID[req.TrackID]
	if !ok {
		writeJSONErr(w, http.StatusNotFound, "Track not found in library")
		return
	}
	ent := e.entries[idx]

	// Check duplicate in Up Next (or entire playlist if re-request played disabled)
	for i, qi := range e.queue {
		if qi.TrackID == ent.effectiveID {
			isPlayed := e.currentIndex >= 0 && i < e.currentIndex
			if !isPlayed || !e.allowReRequestPlayed {
				st := e.snapshotStateLocked(now)
				writeJSON(w, http.StatusOK, map[string]any{
					"ok":            false,
					"alreadyQueued": true,
					"message":       "Already in the Party queue",
					"position":      i,
					"state":         st,
				})
				return
			}
		}
	}

	e.touchGuestLocked(guestID, guestName, true)
	if guestName == "" {
		guestName = "Guest"
	}
	pos := len(e.queue)
	e.queue = append(e.queue, persistedQueueItem{
		TrackID:        ent.effectiveID,
		AddedBy:        guestName,
		AddedByGuestID: guestID,
		AddedAt:        now,
	})
	if !e.isLive {
		e.isLive = true
	}
	if e.currentIndex < 0 {
		e.currentIndex = 0
		e.currentTime = 0
		e.isPlaying = true
		e.stateUpdatedAt = now
	}
	e.revision++
	e.persistLocked()
	st := e.snapshotStateLocked(now)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"alreadyQueued": false,
		"message":       fmt.Sprintf("Added \"%s\" to Party queue (#%d)", ent.rec.Title, pos+1),
		"position":      pos,
		"track": TrackItem{
			ID:          ent.effectiveID,
			Title:       ent.rec.Title,
			Artist:      ent.displayArtist,
			Album:       ent.displayAlbum,
			Duration:    ent.rec.Duration,
			Year:        ent.rec.Year,
			Format:      ent.formatBadge,
			ArtworkHash: ent.rec.ArtworkHash,
			InPlaylist:  true,
			InUpNext:    true,
		},
		"state": st,
	})
}

func (e *Engine) handleRenameGuest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONErr(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		GuestID   string `json:"guestId"`
		GuestName string `json:"guestName"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&req); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	gid := sanitizeGuestID(req.GuestID)
	if gid == "" {
		writeJSONErr(w, http.StatusBadRequest, "Invalid guestId")
		return
	}
	newName := sanitizeGuestName(req.GuestName)
	if newName == "" {
		newName = "Guest"
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	updated := 0
	for i := range e.queue {
		if e.queue[i].AddedByGuestID == gid {
			e.queue[i].AddedBy = newName
			updated++
		}
	}
	for i := range e.chat {
		if e.chat[i].GuestID != nil && *e.chat[i].GuestID == gid {
			e.chat[i].SenderName = newName
			updated++
		}
	}
	e.touchGuestLocked(gid, newName, false)
	e.revision++
	e.persistLocked()
	st := e.snapshotStateLocked(time.Now().UTC())
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"updatedCount": updated,
		"guestName":    newName,
		"state":        st,
	})
}

func (e *Engine) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONErr(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var req struct {
		GuestID   string `json:"guestId"`
		GuestName string `json:"guestName"`
		Text      string `json:"text"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&req); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	clean := sanitizeChatText(req.Text)
	if clean == "" {
		writeJSONErr(w, http.StatusBadRequest, "Message cannot be empty")
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.chatEnabled {
		writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "message": "Party chat is disabled by host"})
		return
	}
	gid := sanitizeGuestID(req.GuestID)
	gname := sanitizeGuestName(req.GuestName)
	if gname == "" {
		gname = "Guest"
	}
	e.touchGuestLocked(gid, gname, false)
	var gidPtr *string
	if gid != "" {
		v := gid
		gidPtr = &v
	}
	msg := ChatMessage{
		ID:         uuid.NewString(),
		GuestID:    gidPtr,
		SenderName: gname,
		IsHost:     false,
		Text:       clean,
		SentAt:     time.Now().UTC(),
	}
	e.chat = append(e.chat, msg)
	if len(e.chat) > 200 {
		e.chat = e.chat[len(e.chat)-200:]
	}
	e.revision++
	e.persistLocked()
	st := e.snapshotStateLocked(time.Now().UTC())
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"chatMessage": msg,
		"state":       st,
	})
}

func (e *Engine) handleIntro(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		e.handleIntroDelete(w, r)
		return
	}
	if r.Method != http.MethodPost {
		writeJSONErr(w, http.StatusMethodNotAllowed, "POST or DELETE required")
		return
	}
	q := r.URL.Query()
	trackID := strings.TrimSpace(q.Get("trackId"))
	if trackID == "" {
		writeJSONErr(w, http.StatusBadRequest, "Missing trackId")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 6_291_456+1))
	if err != nil || len(body) == 0 || len(body) > 6_291_456 {
		writeJSONErr(w, http.StatusRequestEntityTooLarge, "Voice intro must be between 1 byte and 6 MB")
		return
	}
	rawExt := strings.ToLower(strings.TrimSpace(q.Get("ext")))
	switch rawExt {
	case "wav", "m4a", "mp3", "aac", "caf", "webm", "ogg":
	default:
		rawExt = "wav"
	}

	e.mu.RLock()
	maxSec := e.maxIntroSeconds
	e.mu.RUnlock()

	tmpIn := filepath.Join(e.cfg.IntrosDir(), fmt.Sprintf("raw-%s-%d.%s", artwork.Hash([]byte(trackID)), time.Now().UnixNano(), rawExt))
	finalOut := filepath.Join(e.cfg.IntrosDir(), fmt.Sprintf("intro-%s-%d.m4a", artwork.Hash([]byte(trackID)), time.Now().UnixNano()))
	if err := os.WriteFile(tmpIn, body, 0o644); err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "Failed to save raw voice intro")
		return
	}
	defer os.Remove(tmpIn)

	dur, err := NormalizeVoiceIntro(context.Background(), tmpIn, finalOut, maxSec)
	if err != nil {
		_ = os.Remove(finalOut)
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.touchGuestLocked(q.Get("guestId"), q.Get("guestName"), false)
	attached := false
	for i := len(e.queue) - 1; i >= 0; i-- {
		if e.queue[i].TrackID == trackID && (e.currentIndex < 0 || i >= e.currentIndex) {
			if e.queue[i].IntroPath != "" {
				_ = os.Remove(e.queue[i].IntroPath)
			}
			e.queue[i].IntroPath = finalOut
			e.queue[i].VoiceIntroDuration = dur
			attached = true
			break
		}
	}
	if !attached {
		_ = os.Remove(finalOut)
		writeJSONErr(w, http.StatusNotFound, "Track is not currently queued in Party Mode")
		return
	}
	e.revision++
	e.persistLocked()
	st := e.snapshotStateLocked(time.Now().UTC())
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"message":  fmt.Sprintf("Voice intro saved (%.1fs)", dur),
		"duration": dur,
		"state":    st,
	})
}

func (e *Engine) handleIntroDelete(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimSpace(r.URL.Query().Get("trackId"))
	if trackID == "" {
		writeJSONErr(w, http.StatusBadRequest, "Missing trackId")
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	removed := false
	for i := range e.queue {
		if e.queue[i].TrackID == trackID && e.queue[i].IntroPath != "" {
			_ = os.Remove(e.queue[i].IntroPath)
			e.queue[i].IntroPath = ""
			e.queue[i].VoiceIntroDuration = 0
			removed = true
		}
	}
	if removed {
		e.revision++
		e.persistLocked()
	}
	st := e.snapshotStateLocked(time.Now().UTC())
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "Voice intro removed",
		"state":   st,
	})
}

func (e *Engine) handleIntroStream(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimPrefix(r.URL.Path, "/api/party/intro/")
	e.mu.RLock()
	var introPath string
	for i := len(e.queue) - 1; i >= 0; i-- {
		if e.queue[i].TrackID == trackID && e.queue[i].IntroPath != "" {
			introPath = e.queue[i].IntroPath
			break
		}
	}
	e.mu.RUnlock()
	if introPath == "" {
		writeJSONErr(w, http.StatusNotFound, "Voice intro not found")
		return
	}
	w.Header().Set("Content-Type", "audio/mp4")
	http.ServeFile(w, r, introPath)
}

func (e *Engine) handleStream(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimPrefix(r.URL.Path, "/api/party/stream/")
	e.mu.RLock()
	allowed := e.streamingEnabled
	inParty := false
	for _, qi := range e.queue {
		if qi.TrackID == trackID {
			inParty = true
			break
		}
	}
	idx, exists := e.entryByID[trackID]
	var fullPath string
	if exists {
		fullPath = e.entries[idx].fullPath
	}
	e.mu.RUnlock()

	if !allowed && !e.isAuthorized(r) {
		writeJSONErr(w, http.StatusForbidden, "Listen-along streaming is disabled by host")
		return
	}
	if !inParty || !exists {
		writeJSONErr(w, http.StatusNotFound, "Track not available in Party stream")
		return
	}
	w.Header().Set("Content-Type", mimeForPath(fullPath))
	http.ServeFile(w, r, fullPath)
}

func (e *Engine) handleTrackInfo(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimSpace(r.URL.Query().Get("trackId"))
	if trackID == "" {
		writeJSONErr(w, http.StatusBadRequest, "Missing trackId")
		return
	}
	e.mu.RLock()
	idx, ok := e.entryByID[trackID]
	if !ok {
		e.mu.RUnlock()
		writeJSONErr(w, http.StatusNotFound, "Track not found")
		return
	}
	ent := e.entries[idx]
	var addedBy *string
	for i := len(e.queue) - 1; i >= 0; i-- {
		if e.queue[i].TrackID == ent.effectiveID && e.queue[i].AddedBy != "" {
			v := e.queue[i].AddedBy
			addedBy = &v
			break
		}
	}
	albCount := 0
	artistAlbums := make(map[string]struct{})
	artistTracks := 0
	for _, other := range e.entries {
		if other.albumLower == ent.albumLower {
			albCount++
		}
		if other.artistLower == ent.artistLower {
			artistTracks++
			artistAlbums[other.albumLower] = struct{}{}
		}
	}
	e.mu.RUnlock()

	resp := TrackInfoResponse{
		OK:                    true,
		TrackID:               ent.effectiveID,
		Title:                 ent.rec.Title,
		Artist:                ent.displayArtist,
		Album:                 ent.displayAlbum,
		Year:                  ent.rec.Year,
		Duration:              ent.rec.Duration,
		Format:                ent.formatBadge,
		ArtworkHash:           ent.rec.ArtworkHash,
		AddedBy:               addedBy,
		Genres:                []string{},
		AlbumTrackCount:       albCount,
		LocalArtistAlbumCount: len(artistAlbums),
		LocalArtistTrackCount: artistTracks,
	}

	// Enrich with cached editorial if present
	if albEds, err := e.store.ListAlbumEditorial(); err == nil {
		for _, ed := range albEds {
			if strings.ToLower(ed.AlbumKey) == ent.albumLower {
				resp.ReleaseDate = ed.ReleaseDate
				resp.Label = ed.Label
				if len(ed.Genres) > 0 {
					resp.Genres = ed.Genres
				}
				resp.AlbumFans = ed.AlbumFans
				resp.AlbumSummary = ed.AlbumSummary
				resp.ArtistSummary = ed.ArtistSummary
				resp.ArtistFans = ed.ArtistFans
				resp.ArtistAlbumCount = ed.ArtistAlbumCount
				resp.ArtistImageURL = ed.ArtistImageURL
				break
			}
		}
	}
	if resp.ArtistSummary == nil {
		if artEds, err := e.store.ListArtistEditorial(); err == nil {
			for _, ed := range artEds {
				if strings.ToLower(ed.ArtistKey) == ent.artistLower {
					resp.ArtistSummary = ed.Summary
					resp.ArtistFans = ed.Fans
					resp.ArtistAlbumCount = ed.AlbumCount
					resp.ArtistImageURL = ed.ImageURL
					break
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (e *Engine) handleArtwork(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimPrefix(r.URL.Path, "/api/party/artwork/")
	preferFull := r.URL.Query().Get("full") == "1" || strings.EqualFold(r.URL.Query().Get("full"), "true")
	var data []byte
	var err error
	if preferFull {
		data, err = e.artCache.LoadOrig(hash)
	} else {
		data, err = e.artCache.LoadThumb(hash)
	}
	if err != nil {
		writeJSONErr(w, http.StatusNotFound, "Artwork not found")
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(data)
}

func (e *Engine) handleSessionStart(w http.ResponseWriter, r *http.Request) {
	if !e.isAuthorized(r) {
		writeJSONErr(w, http.StatusUnauthorized, "Invalid or missing X-Echo-Token")
		return
	}
	var req struct {
		RendererID   string `json:"rendererId"`
		RendererName string `json:"rendererName"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req)
	env := e.StartSession(req.RendererID, req.RendererName)
	writeJSON(w, http.StatusOK, env)
}

func (e *Engine) handleSessionStop(w http.ResponseWriter, r *http.Request) {
	if !e.isAuthorized(r) {
		writeJSONErr(w, http.StatusUnauthorized, "Invalid or missing X-Echo-Token")
		return
	}
	env := e.StopSession()
	writeJSON(w, http.StatusOK, env)
}

func (e *Engine) handleHostCommandHTTP(w http.ResponseWriter, r *http.Request) {
	if !e.isAuthorized(r) {
		writeJSONErr(w, http.StatusUnauthorized, "Invalid or missing X-Echo-Token")
		return
	}
	var cmd map[string]any
	if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&cmd); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "Invalid command JSON")
		return
	}
	ok, msg, st := e.HandleHostCommand(cmd)
	status := http.StatusOK
	if !ok {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]any{"ok": ok, "message": msg, "state": st})
}

func (e *Engine) handleRendererHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !e.isAuthorized(r) {
		writeJSONErr(w, http.StatusUnauthorized, "Invalid or missing X-Echo-Token")
		return
	}
	var req struct {
		RendererID          string  `json:"rendererId"`
		RendererName        string  `json:"rendererName"`
		TrackID             string  `json:"trackId"`
		CurrentTime         float64 `json:"currentTime"`
		IsPlaying           bool    `json:"isPlaying"`
		IsPlayingVoiceIntro bool    `json:"isPlayingVoiceIntro"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req); err != nil || req.RendererID == "" {
		writeJSONErr(w, http.StatusBadRequest, "Invalid heartbeat payload")
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	// Accept lease if no active renderer, lease expired, or same renderer
	if e.activeRendererID == "" || e.activeRendererID == req.RendererID || now.Sub(e.rendererLastSeen) > 20*time.Second {
		e.activeRendererID = req.RendererID
		e.activeRendererName = req.RendererName
		e.rendererLastSeen = now
		if e.currentIndex >= 0 && e.currentIndex < len(e.queue) && (req.TrackID == "" || e.queue[e.currentIndex].TrackID == req.TrackID) {
			e.currentTime = max(0, req.CurrentTime)
			e.isPlaying = req.IsPlaying
			e.isPlayingVoiceIntro = req.IsPlayingVoiceIntro
			e.stateUpdatedAt = now
		}
	}
	st := e.snapshotStateLocked(now)
	writeJSON(w, http.StatusOK, st)
}

func (e *Engine) handleRendererEnded(w http.ResponseWriter, r *http.Request) {
	if !e.isAuthorized(r) {
		writeJSONErr(w, http.StatusUnauthorized, "Invalid or missing X-Echo-Token")
		return
	}
	var req struct {
		RendererID string `json:"rendererId"`
		TrackID    string `json:"trackId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "Invalid ended payload")
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	if e.currentIndex >= 0 && e.currentIndex < len(e.queue) && e.queue[e.currentIndex].TrackID == req.TrackID {
		// Consume voice intro file once track finishes
		if e.queue[e.currentIndex].IntroPath != "" {
			_ = os.Remove(e.queue[e.currentIndex].IntroPath)
			e.queue[e.currentIndex].IntroPath = ""
		}
		if e.currentIndex+1 < len(e.queue) {
			e.currentIndex++
			e.currentTime = 0
			e.isPlaying = true
			e.isPlayingVoiceIntro = false
		} else {
			e.isPlaying = false
			e.isPlayingVoiceIntro = false
		}
		e.stateUpdatedAt = now
		e.revision++
		e.persistLocked()
	}
	st := e.snapshotStateLocked(now)
	writeJSON(w, http.StatusOK, st)
}

// --- Multi-Field Search (Matches SearchQueryParser + PartyTrackCatalog) ---

type searchClause struct {
	field    string
	text     string
	exclude  bool
	required bool
}

func parseSearchClauses(raw string) []searchClause {
	fields := strings.Fields(raw)
	var out []searchClause
	for _, tok := range fields {
		c := searchClause{}
		if strings.HasPrefix(tok, "-") {
			c.exclude = true
			tok = tok[1:]
		} else if strings.HasPrefix(tok, "+") {
			c.required = true
			tok = tok[1:]
		}
		if colon := strings.IndexByte(tok, ':'); colon > 0 {
			prefix := strings.ToLower(tok[:colon])
			val := strings.ToLower(strings.TrimSpace(tok[colon+1:]))
			switch prefix {
			case "artist", "track", "title", "album", "genre", "year", "format":
				if prefix == "title" {
					prefix = "track"
				}
				c.field = prefix
				c.text = val
			default:
				c.text = strings.ToLower(tok)
			}
		} else {
			c.text = strings.ToLower(tok)
		}
		if c.text != "" {
			out = append(out, c)
		}
	}
	return out
}

func entryMatchesField(ent indexedEntry, field, needle string) bool {
	switch field {
	case "track":
		return strings.Contains(ent.titleLower, needle)
	case "artist":
		return strings.Contains(ent.artistLower, needle) || strings.Contains(ent.albumArtistLower, needle)
	case "album":
		return strings.Contains(ent.albumLower, needle)
	case "genre":
		return strings.Contains(ent.genreLower, needle)
	case "year":
		return ent.yearString != "" && strings.Contains(ent.yearString, needle)
	case "format":
		return strings.Contains(strings.ToLower(ent.formatBadge), needle)
	default:
		return strings.Contains(ent.titleLower, needle) ||
			strings.Contains(ent.artistLower, needle) ||
			strings.Contains(ent.albumArtistLower, needle) ||
			strings.Contains(ent.albumLower, needle) ||
			strings.Contains(ent.genreLower, needle) ||
			(ent.yearString != "" && strings.Contains(ent.yearString, needle))
	}
}

func (e *Engine) searchTracksLocked(
	query, scope, sortMode string,
	seed uint64,
	offset, limit int,
	inPl, upNext map[string]bool,
) SearchResponse {
	clauses := parseSearchClauses(query)
	if len(clauses) == 0 {
		var ordered []int
		switch sortMode {
		case "recent":
			ordered = e.sortedByRecent
		case "shuffle":
			ordered = make([]int, len(e.entries))
			for i := range e.entries {
				ordered[i] = i
			}
			sort.Slice(ordered, func(i, j int) bool {
				return stableMixHash(e.entries[ordered[i]].effectiveID, seed) < stableMixHash(e.entries[ordered[j]].effectiveID, seed)
			})
		default:
			ordered = e.sortedByTitle
		}
		total := len(ordered)
		start := min(offset, total)
		end := min(start+limit, total)
		page := make([]TrackItem, 0, end-start)
		for _, idx := range ordered[start:end] {
			ent := e.entries[idx]
			page = append(page, TrackItem{
				ID:          ent.effectiveID,
				Title:       ent.rec.Title,
				Artist:      ent.displayArtist,
				Album:       ent.displayAlbum,
				Duration:    ent.rec.Duration,
				Year:        ent.rec.Year,
				Format:      ent.formatBadge,
				ArtworkHash: ent.rec.ArtworkHash,
				InPlaylist:  inPl[ent.effectiveID],
				InUpNext:    upNext[ent.effectiveID],
			})
		}
		return SearchResponse{
			Query:        "",
			Scope:        scope,
			Sort:         sortMode,
			TotalMatches: total,
			Offset:       start,
			Limit:        limit,
			HasMore:      end < total,
			Tracks:       page,
		}
	}

	type scored struct {
		idx   int
		score int
	}
	var matches []scored
	scopeField := ""
	if scope == "track" || scope == "artist" || scope == "album" {
		scopeField = scope
	}
	var posWords []string
	allNoField := true
	for _, c := range clauses {
		if !c.exclude {
			posWords = append(posWords, c.text)
			if c.field != "" {
				allNoField = false
			}
		}
	}
	fullPos := strings.Join(posWords, " ")

	for idx, ent := range e.entries {
		excluded := false
		for _, c := range clauses {
			if c.exclude && entryMatchesField(ent, c.field, c.text) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		if len(posWords) == 0 {
			matches = append(matches, scored{idx: idx, score: 100})
			continue
		}

		matched := true
		titleHits, artistHits, albumHits, bonus := 0, 0, 0, 0
		for _, c := range clauses {
			if c.exclude {
				continue
			}
			tf := c.field
			if tf == "" {
				tf = scopeField
			}
			if !entryMatchesField(ent, tf, c.text) {
				matched = false
				break
			}
			if c.field != "" {
				bonus += 40
			}
			if c.required {
				bonus += 25
			}
			if strings.Contains(ent.titleLower, c.text) {
				titleHits++
			}
			if strings.Contains(ent.artistLower, c.text) || strings.Contains(ent.albumArtistLower, c.text) {
				artistHits++
			}
			if strings.Contains(ent.albumLower, c.text) {
				albumHits++
			}
		}
		if !matched {
			continue
		}

		sc := 220 + bonus + titleHits*35 + artistHits*20 + albumHits*10
		if titleHits > 0 && artistHits > 0 {
			sc += 140
		}
		if allNoField && fullPos != "" {
			switch {
			case ent.titleLower == fullPos:
				sc = 1000 + bonus
			case strings.HasPrefix(ent.titleLower, fullPos):
				sc = 860 + bonus
			case strings.Contains(ent.titleLower, fullPos):
				sc = 720 + bonus
			case ent.artistLower == fullPos:
				sc = 540 + bonus
			case strings.HasPrefix(ent.artistLower, fullPos):
				sc = 480 + bonus
			}
		}
		matches = append(matches, scored{idx: idx, score: sc})
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].score != matches[j].score {
			return matches[i].score > matches[j].score
		}
		a, b := e.entries[matches[i].idx], e.entries[matches[j].idx]
		if a.titleLower != b.titleLower {
			return a.titleLower < b.titleLower
		}
		return a.artistLower < b.artistLower
	})

	total := len(matches)
	start := min(offset, total)
	end := min(start+limit, total)
	page := make([]TrackItem, 0, end-start)
	for _, m := range matches[start:end] {
		ent := e.entries[m.idx]
		page = append(page, TrackItem{
			ID:          ent.effectiveID,
			Title:       ent.rec.Title,
			Artist:      ent.displayArtist,
			Album:       ent.displayAlbum,
			Duration:    ent.rec.Duration,
			Year:        ent.rec.Year,
			Format:      ent.formatBadge,
			ArtworkHash: ent.rec.ArtworkHash,
			InPlaylist:  inPl[ent.effectiveID],
			InUpNext:    upNext[ent.effectiveID],
		})
	}
	return SearchResponse{
		Query:        query,
		Scope:        scope,
		Sort:         sortMode,
		TotalMatches: total,
		Offset:       start,
		Limit:        limit,
		HasMore:      end < total,
		Tracks:       page,
	}
}

func stableMixHash(id string, seed uint64) uint64 {
	h := uint64(14695981039346656037) ^ seed
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return h
}

func sanitizeGuestName(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		if r >= 32 && unicode.IsPrint(r) {
			b.WriteRune(r)
		}
	}
	s := strings.TrimSpace(b.String())
	runes := []rune(s)
	if len(runes) > 32 {
		return string(runes[:32])
	}
	return s
}

func sanitizeGuestID(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" || len(s) > 64 {
		return ""
	}
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '-' || r == '_') {
			return ""
		}
	}
	return s
}

func sanitizeChatText(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		if r >= 32 && unicode.IsPrint(r) {
			b.WriteRune(r)
		}
	}
	s := strings.TrimSpace(b.String())
	runes := []rune(s)
	if len(runes) > 280 {
		return string(runes[:280])
	}
	return s
}

func formatDisplayBadge(format string) string {
	switch strings.ToLower(format) {
	case "dsf", "dff":
		return "DSD"
	case "":
		return "AUDIO"
	default:
		return strings.ToUpper(format)
	}
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

func writeJSONErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
