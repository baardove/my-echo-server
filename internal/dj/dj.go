package dj

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/editorial"
)

// LidarrArrivalChecker lets the DJ know when an album was just imported by Lidarr.
type LidarrArrivalChecker interface {
	IsRecentLidarrAlbum(albumKey string) bool
	RecentAlbumKeys() []string
}

// TrackAnalyzer runs fast on-the-go BPM + vocal cue analysis for DJ rundown tracks.
type TrackAnalyzer interface {
	AnalyzeTrackOnDemand(ctx context.Context, trackID string) (db.TrackAnalysis, error)
}

type Engine struct {
	cfg       *config.Config
	store     *db.Store
	editorial *editorial.Fetcher
	gemini    *GeminiClient
	research  *ResearchCache
	lidarr    LidarrArrivalChecker
	analyzer  TrackAnalyzer

	mu            sync.RWMutex
	sessionID     string
	sourceLabel   string
	activeProfile *config.DJProfile
	currentIndex    int
	slots           []DJSlot
	updatedAt       time.Time
	playbackPosSec  float64
	playbackPlaying bool
	playbackSource  string
	playbackAt      time.Time
	audioByID       map[string]string // breakID -> audio file path
	recentFacts   []string          // avoid repeating identical talking angles back-to-back
	analyzeFailed map[string]bool   // avoid retrying broken files in a loop

	genWake     chan struct{}
	analyzeWake chan struct{}
	genOnce     sync.Once
}

const persistedSessionKVKey = "dj_active_session_v1"

type persistedSession struct {
	SessionID     string            `json:"sessionId"`
	SourceLabel   string            `json:"sourceLabel"`
	ActiveProfile *config.DJProfile `json:"activeProfile,omitempty"`
	CurrentIndex  int               `json:"currentIndex"`
	Slots         []DJSlot          `json:"slots"`
	UpdatedAt     time.Time         `json:"updatedAt"`
}

func NewEngine(cfg *config.Config, store *db.Store, ed *editorial.Fetcher) *Engine {
	_ = os.MkdirAll(cfg.DJCacheDir(), 0o755)
	eng := &Engine{
		cfg:           cfg,
		store:         store,
		editorial:     ed,
		gemini:        NewGeminiClient(),
		research:      NewResearchCache(),
		audioByID:     make(map[string]string),
		analyzeFailed: make(map[string]bool),
		updatedAt:     time.Now().UTC(),
		genWake:       make(chan struct{}, 4),
		analyzeWake:   make(chan struct{}, 4),
	}
	eng.restorePersistedSession()
	return eng
}

func (e *Engine) persistSessionLocked() {
	if e.store == nil {
		return
	}
	snap := persistedSession{
		SessionID:     e.sessionID,
		SourceLabel:   e.sourceLabel,
		ActiveProfile: e.activeProfile,
		CurrentIndex:  e.currentIndex,
		Slots:         e.slots,
		UpdatedAt:     e.updatedAt,
	}
	if raw, err := json.Marshal(snap); err == nil {
		_ = e.store.SetKV(persistedSessionKVKey, string(raw))
	}
}

func (e *Engine) restorePersistedSession() {
	if e.store == nil {
		return
	}
	raw, ok := e.store.GetKV(persistedSessionKVKey)
	if !ok || strings.TrimSpace(raw) == "" {
		return
	}
	var snap persistedSession
	if err := json.Unmarshal([]byte(raw), &snap); err != nil || len(snap.Slots) == 0 {
		return
	}
	// Re-link any cached audio files on disk and reset stuck "generating" breaks to "pending"
	relink := func(b *DJBreak) {
		if b == nil {
			return
		}
		if b.Status == StatusGenerating {
			b.Status = StatusPending
			return
		}
		if b.Status == StatusReady {
			cand := filepath.Join(e.cfg.DJCacheDir(), b.ID+".m4a")
			if _, err := os.Stat(cand); err == nil {
				b.AudioPath = cand
				e.audioByID[b.ID] = cand
			} else {
				b.Status = StatusPending
				b.AudioURL = ""
			}
		}
	}
	for i := range snap.Slots {
		relink(snap.Slots[i].Intro)
		relink(snap.Slots[i].Outro)
		relink(snap.Slots[i].Bridge)
	}
	e.sessionID = snap.SessionID
	e.sourceLabel = snap.SourceLabel
	e.activeProfile = snap.ActiveProfile
	e.currentIndex = snap.CurrentIndex
	if e.currentIndex < 0 || e.currentIndex >= len(snap.Slots) {
		e.currentIndex = 0
	}
	e.slots = snap.Slots
	if mergeAdjacentOutroIntro(e.slots, e.currentIndex+1) {
		e.persistSessionLocked()
	}
	e.updatedAt = snap.UpdatedAt
	log.Printf("[dj] restored persisted DJ session (%d slots, profile=%s, index=%d)", len(e.slots), e.sourceLabel, e.currentIndex)
}

func (e *Engine) SetLidarrChecker(l LidarrArrivalChecker) {
	e.mu.Lock()
	e.lidarr = l
	e.mu.Unlock()
}

func (e *Engine) SetAnalyzer(a TrackAnalyzer) {
	e.mu.Lock()
	e.analyzer = a
	e.mu.Unlock()
	e.wakeGenerator()
}

const djLookaheadWorkers = 3

func (e *Engine) StartWorker(ctx context.Context) {
	e.genOnce.Do(func() {
		for i := 0; i < djLookaheadWorkers; i++ {
			go e.lookaheadLoop(ctx)
		}
		go e.analyzeRundownLoop(ctx)
	})
}

func (e *Engine) wakeGenerator() {
	for i := 0; i < djLookaheadWorkers; i++ {
		select {
		case e.genWake <- struct{}{}:
		default:
		}
	}
	select {
	case e.analyzeWake <- struct{}{}:
	default:
	}
}

func (e *Engine) effectiveDJConfigLocked() config.DJConfig {
	djCfg := e.cfg.DJ
	config.NormalizeDJConfig(&djCfg)
	if p := e.activeProfile; p != nil {
		if strings.TrimSpace(p.StationName) != "" {
			djCfg.RadioStationName = strings.TrimSpace(p.StationName)
		}
		if strings.TrimSpace(p.Persona) != "" {
			djCfg.Persona = strings.TrimSpace(p.Persona)
		}
		if strings.TrimSpace(p.VoiceName) != "" {
			djCfg.VoiceName = strings.TrimSpace(p.VoiceName)
		}
		if strings.TrimSpace(p.Language) != "" {
			djCfg.Language = strings.TrimSpace(p.Language)
		}
		if p.TalkFrequency != nil && *p.TalkFrequency >= 0 && *p.TalkFrequency <= 1 {
			djCfg.TalkFrequency = *p.TalkFrequency
		}
		if p.CrossfadeSeconds != nil && *p.CrossfadeSeconds >= 0 && *p.CrossfadeSeconds <= 15 {
			djCfg.CrossfadeSeconds = *p.CrossfadeSeconds
		}
		if strings.TrimSpace(p.CustomPrompt) != "" {
			djCfg.CustomPrompt = strings.TrimSpace(p.CustomPrompt)
		}
	}
	return djCfg
}

func (e *Engine) State() SessionState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	djCfg := e.effectiveDJConfigLocked()

	slotsCopy := make([]DJSlot, len(e.slots))
	for i, s := range e.slots {
		cp := s
		if s.Intro != nil {
			b := *s.Intro
			cp.Intro = &b
		}
		if s.Outro != nil {
			b := *s.Outro
			cp.Outro = &b
		}
		if s.Bridge != nil {
			b := *s.Bridge
			cp.Bridge = &b
		}
		slotsCopy[i] = cp
	}

	profID, profName := "", ""
	if e.activeProfile != nil {
		profID = e.activeProfile.ID
		profName = e.activeProfile.Name
	}

	livePos := e.playbackPosSec
	livePlaying := e.playbackPlaying
	if livePlaying && !e.playbackAt.IsZero() {
		age := time.Since(e.playbackAt)
		if age > 10*time.Second {
			livePlaying = false
		} else {
			livePos += age.Seconds()
		}
	}

	return SessionState{
		SessionID:           e.sessionID,
		StationName:         djCfg.RadioStationName,
		SourceLabel:         e.sourceLabel,
		ProfileID:           profID,
		ProfileName:         profName,
		AutoRefill:          djCfg.AutoRefill,
		MinQueueAhead:       djCfg.MinQueueAhead,
		CurrentIndex:        e.currentIndex,
		PlaybackPositionSec: livePos,
		PlaybackPlaying:     livePlaying,
		PlaybackSource:      e.playbackSource,
		Configured:          strings.TrimSpace(djCfg.GeminiAPIKey) != "",
		LLMModel:         djCfg.LLMModel,
		TTSModel:         djCfg.TTSModel,
		VoiceName:        djCfg.VoiceName,
		Persona:          djCfg.Persona,
		Language:         djCfg.Language,
		TalkFrequency:    djCfg.TalkFrequency,
		CrossfadeSeconds: djCfg.CrossfadeSeconds,
		DuckVolume:       djCfg.DuckVolume,
		UpdatedAt:        e.updatedAt,
		Slots:            slotsCopy,
	}
}

func (e *Engine) AudioPathForBreak(breakID string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.audioByID[breakID]
	if ok && p != "" {
		return p, true
	}
	// Allow serving recently generated test voice clips too
	if isSafeID(breakID) {
		cand := filepath.Join(e.cfg.DJCacheDir(), breakID+".m4a")
		if _, err := os.Stat(cand); err == nil {
			return cand, true
		}
	}
	return "", false
}

// CreateSessionRequest defines how to build a live DJ radio playlist.
type CreateSessionRequest struct {
	Source    string            `json:"source"`    // "profile", "shuffle", "bpm", "rated", "recent", "lidarr", "genre", "tag", "artist", "album", "playlist", "tracks"
	Value     string            `json:"value"`     // profileId, genre name, tag, artistKey, albumKey, playlistID
	ProfileID string            `json:"profileId"` // optional DJ Radio Profile ID (also drives auto-refill!)
	Filter    *config.DJProfile `json:"filter"`    // optional ad-hoc Library filter used as live station profile
	TrackIDs  []string          `json:"trackIds"`  // explicit list for source="tracks"
	Count     int               `json:"count"`     // desired initial playlist length (default 8, always >= minQueueAhead+1)
	Seed      int64             `json:"seed"`      // optional RNG seed (0 = random)
}

func (e *Engine) findProfileByID(id string) *config.DJProfile {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	for _, p := range e.cfg.DJ.Profiles {
		if strings.EqualFold(p.ID, id) {
			cp := p
			return &cp
		}
	}
	return nil
}

func (e *Engine) CreateSession(req CreateSessionRequest) (SessionState, error) {
	allTracks, err := e.store.LoadAllTracks()
	if err != nil {
		return SessionState{}, err
	}
	if len(allTracks) == 0 {
		return SessionState{}, fmt.Errorf("no indexed tracks available yet")
	}

	djBase := e.cfg.DJ
	config.NormalizeDJConfig(&djBase)

	count := req.Count
	if count <= 0 {
		count = max(8, djBase.MinQueueAhead+3)
	}
	if count < djBase.MinQueueAhead+1 {
		count = djBase.MinQueueAhead + 1
	}
	if count > 50 {
		count = 50
	}

	seed := req.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := mathrand.New(mathrand.NewSource(seed))

	source := strings.ToLower(strings.TrimSpace(req.Source))
	if source == "" {
		if req.ProfileID != "" || req.Filter != nil {
			source = "profile"
		} else {
			source = "shuffle"
		}
	}

	var activeProf *config.DJProfile
	if req.Filter != nil {
		cp := *req.Filter
		if cp.Name == "" {
			cp.Name = "Custom Filter Radio"
		}
		if cp.ID == "" {
			cp.ID = "custom-filter"
		}
		activeProf = &cp
	} else if pid := strings.TrimSpace(req.ProfileID); pid != "" {
		activeProf = e.findProfileByID(pid)
	} else if source == "profile" && strings.TrimSpace(req.Value) != "" {
		activeProf = e.findProfileByID(req.Value)
	}
	if activeProf == nil && source == "profile" && len(djBase.Profiles) > 0 {
		cp := djBase.Profiles[0]
		activeProf = &cp
	}

	var picked []db.TrackRecord
	label := "Shuffle Radio"

	switch source {
	case "profile":
		picked = e.pickTracksForProfile(allTracks, activeProf, count, nil, rng)
		if activeProf != nil {
			label = "Profile · " + activeProf.Name
		}
		if len(picked) == 0 {
			return SessionState{}, fmt.Errorf("no library tracks match DJ profile %q (try widening genres/BPM/stars)", activeProf.Name)
		}

	case "tracks":
		byID := indexTracksByID(allTracks)
		for _, id := range req.TrackIDs {
			if tr, ok := byID[id]; ok {
				picked = append(picked, tr)
			}
		}
		label = "Custom DJ Set"

	case "playlist":
		pls, _ := e.store.ListPlaylists(false)
		byID := indexTracksByID(allTracks)
		for _, p := range pls {
			if p.ID == req.Value || strings.EqualFold(p.Name, req.Value) {
				label = "Playlist · " + p.Name
				for _, tid := range p.TrackIDs {
					if db.IsMissingTrackID(tid) {
						continue
					}
					if tr, ok := byID[tid]; ok {
						picked = append(picked, tr)
					}
				}
				break
			}
		}

	case "album":
		target := strings.ToLower(strings.TrimSpace(req.Value))
		for _, tr := range allTracks {
			if strings.ToLower(strings.TrimSpace(tr.Album)) == target {
				picked = append(picked, tr)
			}
		}
		sortAlbumTracks(picked)
		if len(picked) > 0 {
			label = "Album Spotlight · " + picked[0].Album
		}

	case "artist":
		target := strings.ToLower(strings.TrimSpace(req.Value))
		for _, tr := range allTracks {
			if strings.ToLower(strings.TrimSpace(tr.AlbumArtist)) == target || strings.ToLower(strings.TrimSpace(tr.Artist)) == target {
				picked = append(picked, tr)
			}
		}
		rng.Shuffle(len(picked), func(i, j int) { picked[i], picked[j] = picked[j], picked[i] })
		if len(picked) > count {
			picked = picked[:count]
		}
		if len(picked) > 0 {
			label = "Artist Radio · " + displayArtist(picked[0])
		}

	case "genre":
		target := strings.ToLower(strings.TrimSpace(req.Value))
		analysisMap, _ := e.store.ListTrackAnalysisMap()
		for _, tr := range allTracks {
			matched := tr.Genre != nil && strings.Contains(strings.ToLower(*tr.Genre), target)
			if !matched {
				if an, ok := analysisMap[tr.TrackKey]; ok && strings.Contains(strings.ToLower(an.Genre), target) {
					matched = true
				}
			}
			if matched {
				picked = append(picked, tr)
			}
		}
		picked = pickDiverseSubset(picked, count, rng)
		picked = orderByBPMFlow(picked, analysisMap)
		label = "Genre Mix · " + req.Value

	case "tag":
		target := strings.ToLower(strings.TrimSpace(req.Value))
		analysisMap, _ := e.store.ListTrackAnalysisMap()
		for _, tr := range allTracks {
			if an, ok := analysisMap[tr.TrackKey]; ok {
				for _, t := range an.Tags {
					if strings.EqualFold(t, target) || strings.Contains(strings.ToLower(t), target) {
						picked = append(picked, tr)
						break
					}
				}
			}
		}
		picked = pickDiverseSubset(picked, count, rng)
		picked = orderByBPMFlow(picked, analysisMap)
		label = "Tag Mix · " + req.Value

	case "bpm":
		analysisMap, _ := e.store.ListTrackAnalysisMap()
		var analyzed []db.TrackRecord
		for _, tr := range allTracks {
			if an, ok := analysisMap[tr.TrackKey]; ok && an.BPM > 40 {
				analyzed = append(analyzed, tr)
			}
		}
		if len(analyzed) >= 4 {
			picked = pickBPMStackedSet(analyzed, analysisMap, count, rng)
			label = "BPM Flow Mix"
		} else {
			picked = pickDiverseSubset(allTracks, count, rng)
			label = "BPM Flow Mix (Analyze tracks in Settings for exact BPM stacking)"
		}

	case "rated":
		stats, _ := e.store.ListTrackStats()
		highRated := make(map[string]bool)
		for _, st := range stats {
			if st.UserRating != nil && *st.UserRating >= 4 {
				highRated[st.TrackID] = true
			}
		}
		for _, tr := range allTracks {
			if highRated[tr.EffectiveID()] || highRated[tr.TrackKey] {
				picked = append(picked, tr)
			}
		}
		analysisMap, _ := e.store.ListTrackAnalysisMap()
		picked = pickDiverseSubset(picked, count, rng)
		picked = orderByBPMFlow(picked, analysisMap)
		label = "Top Rated (4–5★) Radio"

	case "lidarr":
		var recentKeys map[string]bool
		e.mu.RLock()
		if e.lidarr != nil {
			keys := e.lidarr.RecentAlbumKeys()
			recentKeys = make(map[string]bool, len(keys))
			for _, k := range keys {
				recentKeys[k] = true
			}
		}
		e.mu.RUnlock()
		if len(recentKeys) > 0 {
			for _, tr := range allTracks {
				if recentKeys[strings.ToLower(strings.TrimSpace(tr.Album))] {
					picked = append(picked, tr)
				}
			}
		}
		if len(picked) == 0 {
			// Fallback to most recently modified/added tracks
			picked = pickRecentTracks(allTracks, count, rng)
			label = "Fresh Arrivals Mix"
		} else {
			picked = pickDiverseSubset(picked, count, rng)
			label = "Fresh from Lidarr"
		}

	case "recent":
		picked = pickRecentTracks(allTracks, count, rng)
		label = "Recent Arrivals Radio"

	default: // "shuffle"
		picked = pickDiverseSubset(allTracks, count, rng)
		label = "Library Discovery Mix"
	}

	if len(picked) == 0 {
		return SessionState{}, fmt.Errorf("no matching tracks found for DJ source %q", source)
	}

	// If user launched genre/tag/rated/bpm without an explicit profile, synthesize a matching refill profile
	// so infinite Auto-Refill continues picking matching tracks automatically!
	if activeProf == nil {
		switch source {
		case "genre":
			activeProf = &config.DJProfile{ID: "auto-genre", Name: "Genre · " + req.Value, Genres: []string{req.Value}, PreferBPMFlow: true}
		case "tag":
			activeProf = &config.DJProfile{ID: "auto-tag", Name: "Tag · " + req.Value, Tags: []string{req.Value}, PreferBPMFlow: true}
		case "rated":
			activeProf = &config.DJProfile{ID: "auto-rated", Name: "Top Rated (4–5★)", MinRating: 4, PreferBPMFlow: true}
		case "bpm":
			activeProf = &config.DJProfile{ID: "auto-bpm", Name: "BPM Flow Mix", PreferBPMFlow: true}
		default:
			if p := e.findProfileByID(djBase.ActiveProfileID); p != nil {
				activeProf = p
			} else if len(djBase.Profiles) > 0 {
				cp := djBase.Profiles[0]
				activeProf = &cp
			}
		}
	}

	// Build editorial & deep audio analysis lookups
	edAlbList, _ := e.store.ListAlbumEditorial()
	hasEd := make(map[string]bool, len(edAlbList))
	for _, ed := range edAlbList {
		if (ed.AlbumSummary != nil && *ed.AlbumSummary != "") || (ed.ArtistSummary != nil && *ed.ArtistSummary != "") || ed.Label != nil {
			hasEd[strings.ToLower(ed.AlbumKey)] = true
		}
	}
	analysisMap, _ := e.store.ListTrackAnalysisMap()
	if source == "shuffle" || source == "recent" || source == "lidarr" {
		picked = orderByBPMFlow(picked, analysisMap)
	}

	slots := make([]DJSlot, 0, len(picked))
	e.mu.RLock()
	lidarrChecker := e.lidarr
	e.mu.RUnlock()

	for i, tr := range picked {
		info := toDJTrackInfo(tr, analysisMap)
		fromLidarr := lidarrChecker != nil && lidarrChecker.IsRecentLidarrAlbum(info.AlbumKey)
		slots = append(slots, DJSlot{
			ID:           randomID("slot"),
			Position:     i,
			Track:        info,
			Mode:         "auto",
			HasEditorial: hasEd[info.AlbumKey],
			FromLidarr:   fromLidarr,
		})
	}

	e.mu.Lock()
	e.sessionID = randomID("dj")
	e.sourceLabel = label
	e.activeProfile = activeProf
	e.currentIndex = 0
	e.slots = slots
	e.updatedAt = time.Now().UTC()
	e.recentFacts = nil
	effCfg := e.effectiveDJConfigLocked()
	everyTrack := activeProf.WantsIntroOutroEveryTrack()
	PlanOrganicCadence(e.slots, effCfg.TalkFrequency, everyTrack, rng)
	if activeProf != nil && e.findProfileByID(activeProf.ID) != nil {
		e.cfg.DJ.ActiveProfileID = activeProf.ID
		_ = e.cfg.Save()
	}
	e.persistSessionLocked()
	e.mu.Unlock()

	// Ensure minQueueAhead (default 3) tracks exist after current song if initial list was shorter
	e.EnsureQueueAhead()
	e.wakeGenerator()
	return e.State(), nil
}

// AppendTracks adds one or more tracks to the active live DJ playlist (creating a session if empty)
// and organically plans their intro/outro/bridge breaks.
func (e *Engine) AppendTracks(trackIDs []string) (SessionState, error) {
	if len(trackIDs) == 0 {
		return e.State(), nil
	}
	allTracks, err := e.store.LoadAllTracks()
	if err != nil {
		return SessionState{}, err
	}
	byID := indexTracksByID(allTracks)

	edAlbList, _ := e.store.ListAlbumEditorial()
	hasEd := make(map[string]bool, len(edAlbList))
	for _, ed := range edAlbList {
		if (ed.AlbumSummary != nil && *ed.AlbumSummary != "") || (ed.ArtistSummary != nil && *ed.ArtistSummary != "") || ed.Label != nil {
			hasEd[strings.ToLower(ed.AlbumKey)] = true
		}
	}

	analysisMap, _ := e.store.ListTrackAnalysisMap()
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))

	e.mu.Lock()
	if e.sessionID == "" {
		e.sessionID = randomID("dj")
		e.sourceLabel = "Custom DJ Mix"
		e.currentIndex = 0
	}
	lidarrChecker := e.lidarr
	startIdx := len(e.slots)
	for _, id := range trackIDs {
		tr, ok := byID[id]
		if !ok {
			continue
		}
		info := toDJTrackInfo(tr, analysisMap)
		e.slots = append(e.slots, DJSlot{
			ID:           randomID("slot"),
			Position:     len(e.slots),
			Track:        info,
			Mode:         "auto",
			HasEditorial: hasEd[info.AlbumKey],
			FromLidarr:   lidarrChecker != nil && lidarrChecker.IsRecentLidarrAlbum(info.AlbumKey),
		})
	}
	djCfg := e.effectiveDJConfigLocked()
	everyTrack := e.activeProfile.WantsIntroOutroEveryTrack()
	if len(e.slots) > startIdx {
		planTailCadence(e.slots, startIdx, djCfg.TalkFrequency, everyTrack, rng)
	}
	e.updatedAt = time.Now().UTC()
	e.mu.Unlock()

	e.wakeGenerator()
	return e.State(), nil
}

// InsertTracksAfterCurrent splices one or more tracks directly after the currently playing slot
// without disturbing the rest of the DJ playlist, returning the updated state and insertion index.
func (e *Engine) InsertTracksAfterCurrent(trackIDs []string) (SessionState, int, error) {
	return e.InsertTracksAfterCurrentAt(trackIDs, nil)
}

// InsertTracksAfterCurrentAt splices one or more tracks directly after afterIndex (or e.currentIndex if nil)
// without disturbing the rest of the DJ playlist, returning the updated state and insertion index.
func (e *Engine) InsertTracksAfterCurrentAt(trackIDs []string, afterIndex *int) (SessionState, int, error) {
	if len(trackIDs) == 0 {
		st := e.State()
		return st, st.CurrentIndex, nil
	}
	allTracks, err := e.store.LoadAllTracks()
	if err != nil {
		return SessionState{}, -1, err
	}
	byID := indexTracksByID(allTracks)

	edAlbList, _ := e.store.ListAlbumEditorial()
	hasEd := make(map[string]bool, len(edAlbList))
	for _, ed := range edAlbList {
		if (ed.AlbumSummary != nil && *ed.AlbumSummary != "") || (ed.ArtistSummary != nil && *ed.ArtistSummary != "") || ed.Label != nil {
			hasEd[strings.ToLower(ed.AlbumKey)] = true
		}
	}

	analysisMap, _ := e.store.ListTrackAnalysisMap()

	e.mu.Lock()
	if e.sessionID == "" {
		e.sessionID = randomID("dj")
		e.sourceLabel = "Custom DJ Mix"
		e.currentIndex = 0
	}
	lidarrChecker := e.lidarr
	var newSlots []DJSlot
	for _, id := range trackIDs {
		tr, ok := byID[id]
		if !ok {
			continue
		}
		info := toDJTrackInfo(tr, analysisMap)
		newSlots = append(newSlots, DJSlot{
			ID:           randomID("slot"),
			Track:        info,
			Mode:         "auto",
			HasEditorial: hasEd[info.AlbumKey],
			FromLidarr:   lidarrChecker != nil && lidarrChecker.IsRecentLidarrAlbum(info.AlbumKey),
		})
	}
	if len(newSlots) == 0 {
		e.mu.Unlock()
		st := e.State()
		return st, st.CurrentIndex, fmt.Errorf("requested track not found in library")
	}

	oldPrev := snapshotPrevTrackIDs(e.slots)
	insertIdx := 0
	if len(e.slots) > 0 {
		baseIdx := e.currentIndex
		if afterIndex != nil {
			baseIdx = *afterIndex
		}
		insertIdx = baseIdx + 1
		if insertIdx < 0 {
			insertIdx = 0
		}
		if insertIdx > len(e.slots) {
			insertIdx = len(e.slots)
		}
	}

	updated := make([]DJSlot, 0, len(e.slots)+len(newSlots))
	updated = append(updated, e.slots[:insertIdx]...)
	updated = append(updated, newSlots...)
	updated = append(updated, e.slots[insertIdx:]...)
	for i := range updated {
		updated[i].Position = i
	}
	e.slots = updated

	repairBridgesAfterReorder(e.slots, oldPrev, e.activeProfile.WantsIntroOutroEveryTrack())
	e.updatedAt = time.Now().UTC()
	e.persistSessionLocked()
	e.mu.Unlock()

	e.wakeGenerator()
	return e.State(), insertIdx, nil
}

func (e *Engine) SetCurrentIndex(idx int) SessionState {
	e.mu.Lock()
	if idx >= 0 && idx < len(e.slots) {
		if e.currentIndex != idx {
			e.playbackPosSec = 0
			e.playbackAt = time.Now().UTC()
		}
		e.currentIndex = idx
		e.updatedAt = time.Now().UTC()
		e.persistSessionLocked()
	}
	e.mu.Unlock()
	// Automatically top up the queue so at least MinQueueAhead (default 3) songs remain ahead for research + voice gen!
	e.EnsureQueueAhead()
	e.wakeGenerator()
	return e.State()
}

// ReportPlayback records the live playback clock (from browser host or MP3 broadcast stream)
// so all listeners and DLNA speakers stay synchronized in real time.
func (e *Engine) ReportPlayback(idx int, posSec float64, playing bool, source string) {
	e.mu.Lock()
	if idx >= 0 && idx < len(e.slots) {
		e.currentIndex = idx
	}
	if posSec < 0 {
		posSec = 0
	}
	e.playbackPosSec = posSec
	e.playbackPlaying = playing
	e.playbackSource = source
	e.playbackAt = time.Now().UTC()
	e.mu.Unlock()
}

// LivePlaybackClock returns the extrapolated live position for real-time stream synchronization.
func (e *Engine) LivePlaybackClock() (sessionID string, idx int, posSec float64, playing bool, source string, age time.Duration) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	sessionID = e.sessionID
	idx = e.currentIndex
	posSec = e.playbackPosSec
	playing = e.playbackPlaying
	source = e.playbackSource
	if !e.playbackAt.IsZero() {
		age = time.Since(e.playbackAt)
		if playing && age < 12*time.Second {
			posSec += age.Seconds()
		}
	} else {
		age = time.Hour
	}
	return
}

func (e *Engine) RemoveSlot(idx int) SessionState {
	e.mu.Lock()
	if idx >= 0 && idx < len(e.slots) {
		oldPrev := snapshotPrevTrackIDs(e.slots)
		e.slots = append(e.slots[:idx], e.slots[idx+1:]...)
		for i := range e.slots {
			e.slots[i].Position = i
		}
		if idx < e.currentIndex {
			e.currentIndex--
		} else if e.currentIndex >= len(e.slots) && len(e.slots) > 0 {
			e.currentIndex = len(e.slots) - 1
		}
		repairBridgesAfterReorder(e.slots, oldPrev, e.activeProfile.WantsIntroOutroEveryTrack())
		e.updatedAt = time.Now().UTC()
		e.persistSessionLocked()
	}
	e.mu.Unlock()
	e.EnsureQueueAhead()
	e.wakeGenerator()
	return e.State()
}

// MoveSlot reorders a track from fromIdx to toIdx, keeps currentIndex aligned with the playing track,
// preserves unaffected ready breaks, and regenerates any bridges whose neighbor tracks changed
// while enforcing all intro/outro invariants.
func (e *Engine) MoveSlot(fromIdx, toIdx int) (SessionState, error) {
	e.mu.Lock()
	n := len(e.slots)
	if fromIdx < 0 || fromIdx >= n || toIdx < 0 || toIdx >= n {
		e.mu.Unlock()
		return SessionState{}, fmt.Errorf("move indices (%d -> %d) out of range (len %d)", fromIdx, toIdx, n)
	}
	if fromIdx == toIdx {
		e.mu.Unlock()
		return e.State(), nil
	}

	oldPrev := snapshotPrevTrackIDs(e.slots)
	moved := e.slots[fromIdx]
	without := append(e.slots[:fromIdx], e.slots[fromIdx+1:]...)
	updated := make([]DJSlot, 0, n)
	updated = append(updated, without[:toIdx]...)
	updated = append(updated, moved)
	updated = append(updated, without[toIdx:]...)
	for i := range updated {
		updated[i].Position = i
	}
	e.slots = updated

	// Keep currentIndex pointing at the same playing track
	if e.currentIndex == fromIdx {
		e.currentIndex = toIdx
	} else if fromIdx < e.currentIndex && toIdx >= e.currentIndex {
		e.currentIndex--
	} else if fromIdx > e.currentIndex && toIdx <= e.currentIndex {
		e.currentIndex++
	}

	repairBridgesAfterReorder(e.slots, oldPrev, e.activeProfile.WantsIntroOutroEveryTrack())
	e.updatedAt = time.Now().UTC()
	e.persistSessionLocked()
	e.mu.Unlock()

	e.wakeGenerator()
	return e.State(), nil
}

func snapshotPrevTrackIDs(slots []DJSlot) map[string]string {
	m := make(map[string]string, len(slots))
	for i, s := range slots {
		if i == 0 {
			m[s.ID] = ""
		} else {
			m[s.ID] = slots[i-1].Track.ID
		}
	}
	return m
}

// repairBridgesAfterReorder regenerates bridges whose previous-track neighbor changed,
// resolves any double-talk conflicts, and (when everyTrack is true) ensures no track is left
// without either an intro or an outro while leaving unaffected ready breaks untouched.
func repairBridgesAfterReorder(slots []DJSlot, oldPrev map[string]string, everyTrack bool) {
	n := len(slots)
	if n == 0 {
		return
	}

	// 1. Slot 0 cannot have a bridge (no outgoing track before it)
	if slots[0].Bridge != nil {
		slots[0].Bridge = nil
	}
	if n > 1 && slots[0].Intro != nil && oldPrev[slots[0].ID] != "" {
		slots[0].Intro = nil
	}

	// 2. Any bridge whose previous neighbor changed (or moved into opening slot 1) must be regenerated!
	for i := 1; i < n; i++ {
		if slots[i].Bridge != nil {
			newPrevID := slots[i-1].Track.ID
			if oldPrev[slots[i].ID] != newPrevID {
				slots[i].Bridge = newPendingBreak(BreakBridge)
			}
		}
	}

	// 3. Enforce invariant: NO song has both an intro (Intro or Bridge) AND an outro (Outro or next slot's Bridge).
	for i := 0; i < n; i++ {
		hasIntro := slots[i].Intro != nil || slots[i].Bridge != nil
		nextBridges := i+1 < n && slots[i+1].Bridge != nil
		if slots[i].Outro != nil && nextBridges {
			slots[i].Outro = nil
		}
		if hasIntro && slots[i].Outro != nil {
			slots[i].Outro = nil
		}
		if hasIntro && nextBridges {
			slots[i+1].Bridge = nil
			slots[i+1].Intro = newPendingBreak(BreakIntro)
		}
	}

	// 4. Enforce everyTrack invariant: when enabled, every track gets either an intro or an outro (never 0!)
	if everyTrack && n >= 2 {
		if slots[0].Intro == nil && slots[0].Outro == nil && slots[1].Bridge == nil {
			if slots[1].Outro == nil && (n <= 2 || slots[2].Bridge == nil) {
				slots[1].Intro = nil
				slots[1].Bridge = newPendingBreak(BreakBridge)
			} else {
				slots[0].Outro = newPendingBreak(BreakOutro)
			}
		}
		for i := 0; i < n; i++ {
			hasIntro := slots[i].Intro != nil || slots[i].Bridge != nil
			hasOutro := slots[i].Outro != nil || (i+1 < n && slots[i+1].Bridge != nil)
			if hasIntro || hasOutro {
				continue
			}
			if i+1 < n {
				nextHasIntro := slots[i+1].Intro != nil || slots[i+1].Bridge != nil
				nextHasOutro := slots[i+1].Outro != nil || (i+2 < n && slots[i+2].Bridge != nil)
				if !nextHasIntro && !nextHasOutro {
					slots[i+1].Bridge = newPendingBreak(BreakBridge)
					continue
				}
				if !nextHasIntro {
					slots[i].Outro = newPendingBreak(BreakOutro)
					continue
				}
			}
			if i > 0 {
				prevHasIntro := slots[i-1].Intro != nil || slots[i-1].Bridge != nil
				prevHasOutro := slots[i-1].Outro != nil
				if prevHasOutro && !prevHasIntro {
					slots[i-1].Outro = nil
					slots[i].Bridge = newPendingBreak(BreakBridge)
					continue
				}
				slots[i].Intro = newPendingBreak(BreakIntro)
				continue
			}
			slots[i].Outro = newPendingBreak(BreakOutro)
		}
	}

	mergeAdjacentOutroIntro(slots, 1)
	refreshTransitionLabels(slots)
}

// EnsureQueueAhead guarantees at least cfg.DJ.MinQueueAhead (default 3) tracks are queued AFTER currentIndex
// whenever AutoRefill is enabled, giving the background worker plenty of time for editorial research & TTS generation!
func (e *Engine) EnsureQueueAhead() int {
	if e.store == nil {
		return 0
	}
	e.mu.RLock()
	djCfg := e.effectiveDJConfigLocked()
	if !djCfg.AutoRefill || len(e.slots) == 0 {
		e.mu.RUnlock()
		return 0
	}
	minAhead := djCfg.MinQueueAhead
	if minAhead < 1 {
		minAhead = 3
	}
	ahead := len(e.slots) - 1 - e.currentIndex
	if ahead >= minAhead {
		e.mu.RUnlock()
		return 0
	}
	needed := minAhead - ahead
	// Top up with a small cushion (at least needed, up to needed+2) so it doesn't thrash every skip
	toAdd := needed
	prof := e.activeProfile
	usedIDs := make(map[string]bool, len(e.slots))
	for _, s := range e.slots {
		usedIDs[s.Track.ID] = true
	}
	e.mu.RUnlock()

	allTracks, err := e.store.LoadAllTracks()
	if err != nil || len(allTracks) == 0 {
		return 0
	}
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	picked := e.pickTracksForProfile(allTracks, prof, toAdd, usedIDs, rng)
	if len(picked) == 0 && len(usedIDs) > 0 {
		// Allow recycling if library subset is smaller than the played history
		picked = e.pickTracksForProfile(allTracks, prof, toAdd, nil, rng)
	}
	if len(picked) == 0 {
		return 0
	}

	edAlbList, _ := e.store.ListAlbumEditorial()
	hasEd := make(map[string]bool, len(edAlbList))
	for _, ed := range edAlbList {
		if (ed.AlbumSummary != nil && *ed.AlbumSummary != "") || (ed.ArtistSummary != nil && *ed.ArtistSummary != "") || ed.Label != nil {
			hasEd[strings.ToLower(ed.AlbumKey)] = true
		}
	}
	analysisMap, _ := e.store.ListTrackAnalysisMap()

	e.mu.Lock()
	startIdx := len(e.slots)
	lidarrChecker := e.lidarr
	for _, tr := range picked {
		info := toDJTrackInfo(tr, analysisMap)
		e.slots = append(e.slots, DJSlot{
			ID:           randomID("slot"),
			Position:     len(e.slots),
			Track:        info,
			Mode:         "auto",
			HasEditorial: hasEd[info.AlbumKey],
			FromLidarr:   lidarrChecker != nil && lidarrChecker.IsRecentLidarrAlbum(info.AlbumKey),
		})
	}
	if len(e.slots) > startIdx {
		planTailCadence(e.slots, startIdx, djCfg.TalkFrequency, e.activeProfile.WantsIntroOutroEveryTrack(), rng)
		e.updatedAt = time.Now().UTC()
		e.persistSessionLocked()
	}
	added := len(e.slots) - startIdx
	e.mu.Unlock()
	if added > 0 {
		log.Printf("[dj] auto-refilled +%d track(s) to keep min %d ahead of current track", added, minAhead)
		e.wakeGenerator()
	}
	return added
}

// pickTracksForProfile filters tracks against a DJProfile (genres, excludeGenres, tags, eras, BPM, stars, year)
// and selects a diverse, optionally BPM-flow-ordered batch.
func (e *Engine) pickTracksForProfile(allTracks []db.TrackRecord, prof *config.DJProfile, count int, excludeTrackIDs map[string]bool, rng *mathrand.Rand) []db.TrackRecord {
	if count <= 0 || len(allTracks) == 0 {
		return nil
	}
	analysisMap, _ := e.store.ListTrackAnalysisMap()
	edList, _ := e.store.ListAlbumEditorial()
	edByAlb := make(map[string]db.AlbumEditorial, len(edList))
	for _, ed := range edList {
		edByAlb[strings.ToLower(ed.AlbumKey)] = ed
	}
	statsList, _ := e.store.ListTrackStats()
	trackRating := make(map[string]int, len(statsList))
	for _, st := range statsList {
		if st.UserRating != nil && *st.UserRating > 0 {
			trackRating[st.TrackID] = *st.UserRating
		}
	}
	albRatings, _ := e.store.ListAlbumRatings(false)
	albRating := make(map[string]int, len(albRatings))
	for _, ar := range albRatings {
		if ar.Rating > 0 {
			albRating[strings.ToLower(ar.AlbumKey)] = ar.Rating
		}
	}

	var pool []db.TrackRecord
	for _, tr := range allTracks {
		if excludeTrackIDs != nil && (excludeTrackIDs[tr.EffectiveID()] || excludeTrackIDs[tr.TrackKey]) {
			continue
		}
		if prof != nil && !MatchesProfile(tr, *prof, analysisMap[tr.TrackKey], edByAlb[strings.ToLower(strings.TrimSpace(tr.Album))], trackRating, albRating) {
			continue
		}
		pool = append(pool, tr)
	}

	if len(pool) == 0 {
		return nil
	}
	preferFlow := prof == nil || prof.PreferBPMFlow
	if preferFlow {
		var analyzedPool []db.TrackRecord
		for _, tr := range pool {
			if an, ok := analysisMap[tr.TrackKey]; ok && an.BPM > 40 {
				analyzedPool = append(analyzedPool, tr)
			}
		}
		if len(analyzedPool) >= count {
			return pickBPMStackedSet(analyzedPool, analysisMap, count, rng)
		}
		picked := pickDiverseSubset(pool, count, rng)
		return orderByBPMFlow(picked, analysisMap)
	}
	return pickDiverseSubset(pool, count, rng)
}

// MatchesProfile evaluates whether a track satisfies every constraint on a DJProfile.
func MatchesProfile(
	tr db.TrackRecord,
	prof config.DJProfile,
	an db.TrackAnalysis,
	ed db.AlbumEditorial,
	trackRating map[string]int,
	albRating map[string]int,
) bool {
	// Collect all genre strings for this track (ID3 + editorial + analysis)
	var genres []string
	if tr.Genre != nil && strings.TrimSpace(*tr.Genre) != "" {
		genres = append(genres, strings.ToLower(strings.TrimSpace(*tr.Genre)))
	}
	if an.Genre != "" {
		genres = append(genres, strings.ToLower(an.Genre))
	}
	for _, g := range ed.Genres {
		if strings.TrimSpace(g) != "" {
			genres = append(genres, strings.ToLower(strings.TrimSpace(g)))
		}
	}
	var tags []string
	for _, t := range an.Tags {
		tags = append(tags, strings.ToLower(strings.TrimSpace(t)))
	}

	// ExcludeGenres check
	for _, ex := range prof.ExcludeGenres {
		exL := strings.ToLower(strings.TrimSpace(ex))
		if exL == "" {
			continue
		}
		for _, g := range genres {
			if strings.Contains(g, exL) {
				return false
			}
		}
		for _, t := range tags {
			if strings.Contains(t, exL) {
				return false
			}
		}
	}

	// Search Keywords & ExcludeKeywords check across title, artist, album, folder path, genre, tags & editorial description
	if len(prof.Keywords) > 0 || len(prof.ExcludeKeywords) > 0 {
		var hb strings.Builder
		hb.WriteString(tr.Title)
		hb.WriteByte(' ')
		hb.WriteString(tr.Artist)
		hb.WriteByte(' ')
		hb.WriteString(tr.AlbumArtist)
		hb.WriteByte(' ')
		hb.WriteString(tr.Album)
		hb.WriteByte(' ')
		hb.WriteString(tr.RelativePath)
		for _, g := range genres {
			hb.WriteByte(' ')
			hb.WriteString(g)
		}
		for _, t := range tags {
			hb.WriteByte(' ')
			hb.WriteString(t)
		}
		if ed.AlbumSummary != nil {
			hb.WriteByte(' ')
			hb.WriteString(*ed.AlbumSummary)
		}
		if ed.ArtistSummary != nil {
			hb.WriteByte(' ')
			hb.WriteString(*ed.ArtistSummary)
		}
		if ed.Label != nil {
			hb.WriteByte(' ')
			hb.WriteString(*ed.Label)
		}
		haystack := strings.ToLower(hb.String())

		var includeKWs []string
		var excludeKWs []string
		for _, kw := range prof.ExcludeKeywords {
			k := strings.ToLower(strings.TrimSpace(kw))
			if k != "" {
				excludeKWs = append(excludeKWs, k)
			}
		}
		for _, kw := range prof.Keywords {
			k := strings.ToLower(strings.TrimSpace(kw))
			if k == "" {
				continue
			}
			if strings.HasPrefix(k, "-") && len(k) > 1 {
				excludeKWs = append(excludeKWs, strings.TrimSpace(k[1:]))
			} else {
				includeKWs = append(includeKWs, k)
			}
		}
		for _, ex := range excludeKWs {
			if ex != "" && strings.Contains(haystack, ex) {
				return false
			}
		}
		if len(includeKWs) > 0 {
			hit := false
			for _, inc := range includeKWs {
				if strings.Contains(haystack, inc) {
					hit = true
					break
				}
			}
			if !hit {
				return false
			}
		}
	}

	// Include Genres check (match any)
	if len(prof.Genres) > 0 {
		hit := false
		for _, want := range prof.Genres {
			wL := strings.ToLower(strings.TrimSpace(want))
			if wL == "" {
				continue
			}
			for _, g := range genres {
				if strings.Contains(g, wL) {
					hit = true
					break
				}
			}
			if !hit {
				for _, t := range tags {
					if strings.Contains(t, wL) {
						hit = true
						break
					}
				}
			}
			if hit {
				break
			}
		}
		if !hit {
			return false
		}
	}

	// Tags check (match any)
	if len(prof.Tags) > 0 {
		hit := false
		for _, want := range prof.Tags {
			wL := strings.ToLower(strings.TrimSpace(want))
			if wL == "" {
				continue
			}
			for _, t := range tags {
				if strings.Contains(t, wL) {
					hit = true
					break
				}
			}
			if !hit {
				for _, g := range genres {
					if strings.Contains(g, wL) {
						hit = true
						break
					}
				}
			}
			if hit {
				break
			}
		}
		if !hit {
			return false
		}
	}

	// Resolve effective release year (tag or editorial releaseDate)
	yr := 0
	if tr.Year != nil && *tr.Year > 0 {
		yr = *tr.Year
	} else if ed.ReleaseDate != nil && len(*ed.ReleaseDate) >= 4 {
		fmt.Sscanf((*ed.ReleaseDate)[:4], "%d", &yr)
	}

	// Era check (e.g. "1980s", "1990s", "2000s")
	if len(prof.Eras) > 0 {
		if yr <= 0 {
			return false
		}
		decade := fmt.Sprintf("%ds", (yr/10)*10)
		hit := false
		for _, era := range prof.Eras {
			if strings.EqualFold(strings.TrimSpace(era), decade) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}

	if prof.MinYear > 0 && (yr <= 0 || yr < prof.MinYear) {
		return false
	}
	if prof.MaxYear > 0 && (yr <= 0 || yr > prof.MaxYear) {
		return false
	}

	// Star rating check (track rating or album rating fallback)
	if prof.MinRating > 0 || prof.AvoidMaxRating > 0 {
		r := trackRating[tr.EffectiveID()]
		if r == 0 {
			r = trackRating[tr.TrackKey]
		}
		if r == 0 && albRating != nil {
			r = albRating[strings.ToLower(strings.TrimSpace(tr.Album))]
		}
		if prof.MinRating > 0 && r < prof.MinRating {
			return false
		}
		// AvoidMaxRating blocks low-rated songs (1..AvoidMaxRating) while ALLOWING unrated songs (0)!
		if prof.AvoidMaxRating > 0 && r > 0 && r <= prof.AvoidMaxRating {
			return false
		}
	}

	// BPM range check (if track is analyzed, enforce range; if unanalyzed and BPM range is strict, allow only if no genres/eras specified? Better: if BPM filter is set, require matching BPM when analyzed; if zero tracks analyzed yet, still allow fallback? No: enforce strictly when BPM > 0, and skip unanalyzed only when BPM filter is explicitly set AND analyzed tracks exist)
	if prof.MinBPM > 0 || prof.MaxBPM > 0 {
		if an.BPM > 0 {
			if prof.MinBPM > 0 && an.BPM < prof.MinBPM {
				return false
			}
			if prof.MaxBPM > 0 && an.BPM > prof.MaxBPM {
				return false
			}
		}
	}

	return true
}

func (e *Engine) ClearSession() SessionState {
	e.mu.Lock()
	e.sessionID = ""
	e.sourceLabel = ""
	e.currentIndex = 0
	e.slots = nil
	e.updatedAt = time.Now().UTC()
	e.persistSessionLocked()
	e.mu.Unlock()
	return e.State()
}

// UpdateSlotBreak lets the user regenerate a break or switch a slot's mode ("intro", "outro", "bridge", "none").
// Enforces the invariant that a single song does not have both its own intro and its own outro.
func (e *Engine) UpdateSlotBreak(slotIndex int, newMode string, regenerate bool) (SessionState, error) {
	e.mu.Lock()
	if slotIndex < 0 || slotIndex >= len(e.slots) {
		e.mu.Unlock()
		return SessionState{}, fmt.Errorf("slot index %d out of range", slotIndex)
	}

	mode := strings.ToLower(strings.TrimSpace(newMode))
	slot := &e.slots[slotIndex]

	if mode != "" && mode != "keep" {
		slot.Mode = mode
		switch mode {
		case "none":
			slot.Intro = nil
			slot.Outro = nil
			slot.Bridge = nil
		case "intro":
			slot.Intro = newPendingBreak(BreakIntro)
			slot.Outro = nil
			slot.Bridge = nil
			// Avoid stacking a standalone Outro immediately before this Intro
			if slotIndex > 0 && e.slots[slotIndex-1].Outro != nil {
				e.slots[slotIndex-1].Outro = nil
			}
			// If next slot was a bridge (which outro'd this slot), downgrade next slot's bridge to an intro
			if slotIndex+1 < len(e.slots) && e.slots[slotIndex+1].Bridge != nil {
				e.slots[slotIndex+1].Bridge = nil
				e.slots[slotIndex+1].Intro = newPendingBreak(BreakIntro)
			}
		case "outro":
			slot.Intro = nil
			slot.Bridge = nil
			slot.Outro = newPendingBreak(BreakOutro)
			// Avoid stacking a standalone Intro or Bridge immediately after this Outro
			if slotIndex+1 < len(e.slots) && e.slots[slotIndex+1].Bridge != nil {
				e.slots[slotIndex+1].Bridge = nil
			}
			if slotIndex+1 < len(e.slots) && e.slots[slotIndex+1].Intro != nil {
				e.slots[slotIndex+1].Intro = nil
			}
		case "bridge":
			if slotIndex == 0 {
				// Can't bridge before track 0; use intro
				slot.Intro = newPendingBreak(BreakIntro)
				slot.Outro = nil
				slot.Bridge = nil
			} else {
				// Clear prev slot's standalone outro/intro conflict: bridge outro's prev and intro's curr
				e.slots[slotIndex-1].Outro = nil
				slot.Intro = nil
				slot.Outro = nil
				slot.Bridge = newPendingBreak(BreakBridge)
				if slotIndex+1 < len(e.slots) && e.slots[slotIndex+1].Bridge != nil {
					e.slots[slotIndex+1].Bridge = nil
					e.slots[slotIndex+1].Intro = newPendingBreak(BreakIntro)
				}
			}
		}
		refreshTransitionLabels(e.slots)
	} else if regenerate {
		if slot.Bridge != nil {
			slot.Bridge = newPendingBreak(BreakBridge)
		} else if slot.Intro != nil {
			slot.Intro = newPendingBreak(BreakIntro)
		} else if slot.Outro != nil {
			slot.Outro = newPendingBreak(BreakOutro)
		} else {
			slot.Intro = newPendingBreak(BreakIntro)
			refreshTransitionLabels(e.slots)
		}
	}
	e.updatedAt = time.Now().UTC()
	e.mu.Unlock()

	e.wakeGenerator()
	return e.State(), nil
}

// PlanOrganicCadence assigns Intros, Outros, Out/Intro Bridges, and Seamless Crossfades across a playlist.
// Core invariants:
//   - No single song is EVER both intro'd (via Intro or Bridge) AND outro'd (via Outro or next slot's Bridge).
//   - When everyTrack is true (default), EVERY track receives EXACTLY ONE of {intro, outro} — meaning
//     two Seamless Crossfades NEVER appear back-to-back leaving the track between them without a DJ mention!
//   - Still weaves organically between Out/Intro bridges, standalone intros, standalone outros, and single crossfades.
func PlanOrganicCadence(slots []DJSlot, talkFreq float64, everyTrack bool, rng *mathrand.Rand) {
	if len(slots) == 0 {
		return
	}
	for i := range slots {
		slots[i].Intro = nil
		slots[i].Outro = nil
		slots[i].Bridge = nil
	}
	planTailCadence(slots, 0, talkFreq, everyTrack, rng)
}

func planTailCadence(slots []DJSlot, startIdx int, talkFreq float64, everyTrack bool, rng *mathrand.Rand) {
	if len(slots) == 0 {
		return
	}
	if talkFreq < 0 {
		talkFreq = 0
	}
	if talkFreq > 1 {
		talkFreq = 1
	}

	silentStreak := 0
	for i := startIdx; i < len(slots); i++ {
		if i == 0 {
			// Song #1 ALWAYS starts playing immediately without waiting for an intro!
			// Its back-announce is folded into the combined Opening Greeting + Outro + Next Intro on Song #2.
			continue
		}
		if i == 1 && (everyTrack || talkFreq > 0.05) {
			// Song #2 ALWAYS gets the combined Opening Greeting + Outro of Song #1 + Intro of Song #2!
			slots[1].Bridge = newPendingBreak(BreakBridge)
			continue
		}

		prev := &slots[i-1]
		curr := &slots[i]
		prevWasIntrod := prev.Intro != nil || prev.Bridge != nil
		prevAlreadyHasOutro := prev.Outro != nil
		prevNeedsOutro := everyTrack && !prevWasIntrod && !prevAlreadyHasOutro

		if prevNeedsOutro {
			// Previous song entered without an intro (via crossfade or after a standalone outro):
			// it MUST receive an outro here (either combined Out/Intro bridge or standalone Outro)
			// so no track is ever left stranded between two consecutive crossfades!
			silentStreak = 0
			if rng.Float64() < 0.56 {
				curr.Bridge = newPendingBreak(BreakBridge)
			} else {
				prev.Outro = newPendingBreak(BreakOutro)
			}
			continue
		}

		// Decide whether the DJ speaks in the transition between prev and curr
		effectiveProb := talkFreq
		if silentStreak >= 2 && talkFreq >= 0.35 {
			// Prevent overly long dead-air stretches
			effectiveProb = minFloat(0.95, talkFreq+0.35)
		} else if prevWasIntrod {
			// Encourage a single clean music crossfade after a talked-into song so the next song earns an outro/bridge!
			effectiveProb = talkFreq * 0.50
		}

		shouldTalk := rng.Float64() < effectiveProb
		if !shouldTalk {
			silentStreak++
			continue
		}
		silentStreak = 0

		if prevAlreadyHasOutro {
			// Previous song already has a standalone Outro (e.g. added while it was temporarily the last track).
			// NEVER stack a separate Intro right after it — merge them into one combined Out/Intro Bridge!
			prev.Outro = nil
			curr.Bridge = newPendingBreak(BreakBridge)
			continue
		}
		if prevWasIntrod {
			// Previous song was already intro'd -> NEVER also outro it!
			// So if we talk here, we intro the incoming song only.
			curr.Intro = newPendingBreak(BreakIntro)
			continue
		}

		// Previous song was NOT intro'd (only reached when everyTrack == false)
		r := rng.Float64()
		switch {
		case r < 0.45:
			// Out/Intro Bridge: outro for prev + intro for curr in one smooth radio sweep
			curr.Bridge = newPendingBreak(BreakBridge)
		case r < 0.82:
			// Outro only for prev; curr enters clean with no intro
			prev.Outro = newPendingBreak(BreakOutro)
		default:
			// Intro only for curr
			curr.Intro = newPendingBreak(BreakIntro)
		}
	}

	// Ensure last track gets an outro if everyTrack is enabled and it hasn't been intro'd/outro'd yet
	last := len(slots) - 1
	if last > 0 && slots[last].Intro == nil && slots[last].Bridge == nil && slots[last].Outro == nil {
		if everyTrack || (talkFreq >= 0.45 && rng.Float64() < 0.5) {
			slots[last].Outro = newPendingBreak(BreakOutro)
		}
	}

	mergeAdjacentOutroIntro(slots, startIdx)
	refreshTransitionLabels(slots)
}

// mergeAdjacentOutroIntro enforces that two adjacent tracks NEVER stack a standalone Outro
// immediately followed by a standalone Intro — instead combining them into one Out/Intro Bridge.
func mergeAdjacentOutroIntro(slots []DJSlot, minIndex int) bool {
	if minIndex < 1 {
		minIndex = 1
	}
	changed := false
	for i := minIndex; i < len(slots); i++ {
		if slots[i-1].Outro != nil && slots[i].Bridge != nil {
			slots[i-1].Outro = nil
			changed = true
		} else if slots[i-1].Outro != nil && slots[i].Intro != nil {
			slots[i-1].Outro = nil
			slots[i].Intro = nil
			slots[i].Bridge = newPendingBreak(BreakBridge)
			changed = true
		}
	}
	if changed {
		refreshTransitionLabels(slots)
	}
	return changed
}

func refreshTransitionLabels(slots []DJSlot) {
	for i := range slots {
		hasBridge := slots[i].Bridge != nil
		hasIntro := slots[i].Intro != nil
		hasOutro := slots[i].Outro != nil
		nextBridgesMe := i+1 < len(slots) && slots[i+1].Bridge != nil
		switch {
		case hasBridge && i == 1:
			slots[i].TransitionLabel = "OPENING OUT/INTRO"
		case hasBridge:
			slots[i].TransitionLabel = "OUT/INTRO"
		case hasIntro:
			slots[i].TransitionLabel = "INTRO"
		case hasOutro:
			slots[i].TransitionLabel = "OUTRO"
		case nextBridgesMe && i == 0:
			slots[i].TransitionLabel = "DIRECT START"
		case nextBridgesMe:
			slots[i].TransitionLabel = "CROSSFADE"
		default:
			slots[i].TransitionLabel = "CROSSFADE"
		}
	}
}

// TestVoice synthesizes a short studio-processed sample DJ intro using either saved or preview config.
func (e *Engine) TestVoice(ctx context.Context, override *config.DJConfig) (DJBreak, error) {
	djCfg := e.cfg.DJ
	if override != nil {
		if strings.TrimSpace(override.GeminiAPIKey) != "" {
			djCfg.GeminiAPIKey = strings.TrimSpace(override.GeminiAPIKey)
		}
		if strings.TrimSpace(override.LLMModel) != "" {
			djCfg.LLMModel = strings.TrimSpace(override.LLMModel)
		}
		if strings.TrimSpace(override.TTSModel) != "" {
			djCfg.TTSModel = strings.TrimSpace(override.TTSModel)
		}
		if strings.TrimSpace(override.VoiceName) != "" {
			djCfg.VoiceName = strings.TrimSpace(override.VoiceName)
		}
		if strings.TrimSpace(override.Persona) != "" {
			djCfg.Persona = strings.TrimSpace(override.Persona)
		}
		if strings.TrimSpace(override.Language) != "" {
			djCfg.Language = strings.TrimSpace(override.Language)
		}
		if strings.TrimSpace(override.RadioStationName) != "" {
			djCfg.RadioStationName = strings.TrimSpace(override.RadioStationName)
		}
	}
	config.NormalizeDJConfig(&djCfg)
	if strings.TrimSpace(djCfg.GeminiAPIKey) == "" {
		return DJBreak{}, fmt.Errorf("Enter your Gemini API key first to test the DJ voice")
	}

	sampleScript := testVoiceSample(djCfg.Language, djCfg.RadioStationName)

	breakID := randomID("testvoice")
	outPath := filepath.Join(e.cfg.DJCacheDir(), breakID+".m4a")
	dur, err := e.gemini.SynthesizeSpeech(ctx, djCfg, personaTTSStyle(djCfg.Persona, djCfg.Language), sampleScript, outPath)
	if err != nil {
		return DJBreak{}, err
	}

	now := time.Now().UTC()
	e.mu.Lock()
	e.audioByID[breakID] = outPath
	e.mu.Unlock()

	return DJBreak{
		ID:          breakID,
		Kind:        BreakIntro,
		Status:      StatusReady,
		Script:      sampleScript,
		AudioURL:    "/api/v2/dj/audio/" + breakID,
		AudioPath:   outPath,
		Duration:    dur,
		GeneratedAt: &now,
	}, nil
}

func (e *Engine) ListLiveGeminiModels(ctx context.Context, overrideKey string) ([]OptionItem, []OptionItem, error) {
	key := strings.TrimSpace(overrideKey)
	if key == "" {
		key = strings.TrimSpace(e.cfg.DJ.GeminiAPIKey)
	}
	return e.gemini.ListLiveModels(ctx, key)
}

// --- Background Lookahead Script + TTS Pre-Generation Worker ---

func (e *Engine) lookaheadLoop(ctx context.Context) {
	for {
		e.generatePendingLookahead(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.genWake:
		case <-time.After(15 * time.Second):
		}
	}
}

type pendingJob struct {
	sessionID string
	slotIdx   int
	breakID   string
	kind      BreakKind
	currTrack DJTrackInfo
	prevTrack *DJTrackInfo
	fromLidarr bool
}

func (e *Engine) generatePendingLookahead(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if strings.TrimSpace(e.cfg.DJ.GeminiAPIKey) == "" {
			return
		}

		job := e.pickNextPendingJob()
		if job == nil {
			return
		}

		e.runJob(ctx, *job)
	}
}

func (e *Engine) pickNextPendingJob() *pendingJob {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.slots) == 0 {
		return nil
	}
	// Pre-generate at least 5 upcoming slots ahead in parallel so bridges/intros are always
	// ready well before playback reaches them — even if the listener skips tracks.
	djCfg := e.effectiveDJConfigLocked()
	lookahead := djCfg.MinQueueAhead + 2
	if lookahead < 5 {
		lookahead = 5
	}
	cur := e.currentIndex
	if cur < 0 {
		cur = 0
	}
	if cur >= len(e.slots) {
		return nil
	}
	endIdx := cur + lookahead + 1
	if endIdx > len(e.slots) {
		endIdx = len(e.slots)
	}

	claimBridge := func(i int) *pendingJob {
		s := &e.slots[i]
		if s.Bridge == nil || s.Bridge.Status != StatusPending {
			return nil
		}
		s.Bridge.Status = StatusGenerating
		var prev *DJTrackInfo
		if i > 0 {
			p := e.slots[i-1].Track
			prev = &p
		}
		e.updatedAt = time.Now().UTC()
		return &pendingJob{
			sessionID: e.sessionID, slotIdx: i, breakID: s.Bridge.ID, kind: BreakBridge,
			currTrack: s.Track, prevTrack: prev, fromLidarr: s.FromLidarr,
		}
	}
	claimIntro := func(i int) *pendingJob {
		s := &e.slots[i]
		if s.Intro == nil || s.Intro.Status != StatusPending {
			return nil
		}
		s.Intro.Status = StatusGenerating
		e.updatedAt = time.Now().UTC()
		return &pendingJob{
			sessionID: e.sessionID, slotIdx: i, breakID: s.Intro.ID, kind: BreakIntro,
			currTrack: s.Track, fromLidarr: s.FromLidarr,
		}
	}
	claimOutro := func(i int) *pendingJob {
		s := &e.slots[i]
		if s.Outro == nil || s.Outro.Status != StatusPending {
			return nil
		}
		s.Outro.Status = StatusGenerating
		e.updatedAt = time.Now().UTC()
		return &pendingJob{
			sessionID: e.sessionID, slotIdx: i, breakID: s.Outro.ID, kind: BreakOutro,
			currTrack: s.Track, fromLidarr: s.FromLidarr,
		}
	}

	// Priority 1: Opening intro on slot 0 (before first track is underway) + current slot's OUTRO
	// (which plays at the end of the currently playing song!).
	if cur == 0 {
		if job := claimIntro(0); job != nil {
			return job
		}
	}
	if job := claimOutro(cur); job != nil {
		return job
	}

	// Priority 2: Upcoming slots cur+1 .. endIdx-1 (the breaks that are ACTUALLY about to play next!).
	// NEVER waste worker time generating cur's already-passed Bridge/Intro before cur+1 is ready!
	for i := cur + 1; i < endIdx; i++ {
		if job := claimBridge(i); job != nil {
			return job
		}
		if job := claimIntro(i); job != nil {
			return job
		}
		if job := claimOutro(i); job != nil {
			return job
		}
	}

	// Priority 3: Only after all upcoming lookahead slots are ready/generating, backfill current slot's
	// pre-break if still pending (useful if the listener replays the current slot).
	if job := claimBridge(cur); job != nil {
		return job
	}
	if job := claimIntro(cur); job != nil {
		return job
	}
	return nil
}

func (e *Engine) analyzeRundownLoop(ctx context.Context) {
	for {
		e.analyzeNextRundownTracks(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.analyzeWake:
		case <-time.After(15 * time.Second):
		}
	}
}

func (e *Engine) analyzeNextRundownTracks(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		e.mu.RLock()
		analyzer := e.analyzer
		sessID := e.sessionID
		var targetID string
		if analyzer != nil && len(e.slots) > 0 {
			// Prioritize from currentIndex forward first, then any earlier slots
			cur := e.currentIndex
			if cur < 0 || cur >= len(e.slots) {
				cur = 0
			}
			for pass := 0; pass < 2 && targetID == ""; pass++ {
				start, end := cur, len(e.slots)
				if pass == 1 {
					start, end = 0, cur
				}
				for i := start; i < end; i++ {
					tr := e.slots[i].Track
					if (!tr.HasAnalysis || tr.LoudnessDB == 0) && !e.analyzeFailed[tr.ID] {
						targetID = tr.ID
						break
					}
				}
			}
		}
		e.mu.RUnlock()

		if targetID == "" {
			return
		}
		res, err := analyzer.AnalyzeTrackOnDemand(ctx, targetID)
		e.mu.Lock()
		if err != nil {
			e.analyzeFailed[targetID] = true
			e.mu.Unlock()
			continue
		}
		if e.sessionID == sessID {
			updated := false
			for i := range e.slots {
				if e.slots[i].Track.ID == targetID {
					e.slots[i].Track.BPM = res.BPM
					e.slots[i].Track.VocalStartSec = res.VocalStartSec
					e.slots[i].Track.VocalEndSec = res.VocalEndSec
					e.slots[i].Track.Energy = res.Energy
					e.slots[i].Track.LoudnessDB = res.LoudnessDB
					e.slots[i].Track.PeakLinear = res.PeakLinear
					e.slots[i].Track.NormGainDB = res.NormGainDB
					e.slots[i].Track.HasAnalysis = true
					updated = true
				}
			}
			if updated {
				e.updatedAt = time.Now().UTC()
				e.persistSessionLocked()
			}
		}
		e.mu.Unlock()
	}
}

func (e *Engine) ensureTrackAnalyzed(ctx context.Context, tr *DJTrackInfo) {
	if tr == nil || (tr.HasAnalysis && tr.LoudnessDB != 0) || tr.ID == "" {
		return
	}
	e.mu.RLock()
	analyzer := e.analyzer
	failed := e.analyzeFailed[tr.ID]
	e.mu.RUnlock()
	if analyzer == nil || failed {
		return
	}
	res, err := analyzer.AnalyzeTrackOnDemand(ctx, tr.ID)
	if err != nil {
		e.mu.Lock()
		e.analyzeFailed[tr.ID] = true
		e.mu.Unlock()
		return
	}
	tr.BPM = res.BPM
	tr.VocalStartSec = res.VocalStartSec
	tr.VocalEndSec = res.VocalEndSec
	tr.Energy = res.Energy
	tr.LoudnessDB = res.LoudnessDB
	tr.PeakLinear = res.PeakLinear
	tr.NormGainDB = res.NormGainDB
	tr.HasAnalysis = true
	e.mu.Lock()
	for i := range e.slots {
		if e.slots[i].Track.ID == tr.ID {
			e.slots[i].Track.BPM = res.BPM
			e.slots[i].Track.VocalStartSec = res.VocalStartSec
			e.slots[i].Track.VocalEndSec = res.VocalEndSec
			e.slots[i].Track.Energy = res.Energy
			e.slots[i].Track.LoudnessDB = res.LoudnessDB
			e.slots[i].Track.PeakLinear = res.PeakLinear
			e.slots[i].Track.NormGainDB = res.NormGainDB
			e.slots[i].Track.HasAnalysis = true
		}
	}
	e.updatedAt = time.Now().UTC()
	e.persistSessionLocked()
	e.mu.Unlock()
}

func (e *Engine) runJob(ctx context.Context, job pendingJob) {
	e.mu.RLock()
	djCfg := e.effectiveDJConfigLocked()
	e.mu.RUnlock()

	// Ensure BPM & vocal entry/exit cues are analyzed on the go BEFORE crafting the DJ script!
	e.ensureTrackAnalyzed(ctx, &job.currTrack)
	if job.prevTrack != nil {
		e.ensureTrackAnalyzed(ctx, job.prevTrack)
	}

	isOpening := (job.slotIdx == 1 && job.kind == BreakBridge) || job.slotIdx == 0
	sysPrompt, userPrompt, factsUsed := e.BuildFactualPrompt(ctx, djCfg, job.kind, job.prevTrack, job.currTrack, isOpening, job.fromLidarr)

	llmStart := time.Now()
	script, usedSearch, err := e.gemini.GenerateScript(ctx, djCfg, sysPrompt, userPrompt)
	llmElapsed := time.Since(llmStart)
	if err != nil {
		log.Printf("[dj] LLM script error (%s): %v", job.breakID, err)
		e.finishJob(job, "", "", 0, factsUsed, err)
		return
	}
	if usedSearch {
		factsUsed = append([]string{"Google Search"}, factsUsed...)
	}

	outPath := filepath.Join(e.cfg.DJCacheDir(), job.breakID+".m4a")
	ttsStart := time.Now()
	dur, err := e.gemini.SynthesizeSpeech(ctx, djCfg, personaTTSStyle(djCfg.Persona, djCfg.Language), script, outPath)
	ttsElapsed := time.Since(ttsStart)
	if err != nil {
		log.Printf("[dj] TTS synth error (%s): %v", job.breakID, err)
		e.finishJob(job, script, "", 0, factsUsed, err)
		return
	}

	log.Printf("[dj] ready %s (slot #%d %s · %.1fs) [llm=%.1fs tts=%.1fs]: %s", job.kind, job.slotIdx+1, job.currTrack.Title, dur, llmElapsed.Seconds(), ttsElapsed.Seconds(), script)
	e.finishJob(job, script, outPath, dur, factsUsed, nil)
}

func (e *Engine) finishJob(job pendingJob, script, audioPath string, dur float64, facts []string, genErr error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sessionID != job.sessionID {
		return
	}
	var target *DJBreak
	for i := range e.slots {
		if e.slots[i].Bridge != nil && e.slots[i].Bridge.ID == job.breakID {
			target = e.slots[i].Bridge
			break
		}
		if e.slots[i].Intro != nil && e.slots[i].Intro.ID == job.breakID {
			target = e.slots[i].Intro
			break
		}
		if e.slots[i].Outro != nil && e.slots[i].Outro.ID == job.breakID {
			target = e.slots[i].Outro
			break
		}
	}
	if target == nil {
		return
	}
	now := time.Now().UTC()
	target.Script = script
	target.FactsUsed = facts
	if genErr != nil {
		target.Status = StatusError
		target.Error = genErr.Error()
	} else {
		target.Status = StatusReady
		target.Error = ""
		target.AudioPath = audioPath
		target.AudioURL = "/api/v2/dj/audio/" + job.breakID
		target.Duration = dur
		target.GeneratedAt = &now
		e.audioByID[job.breakID] = audioPath
		if script != "" {
			e.recentFacts = append(e.recentFacts, script)
			if len(e.recentFacts) > 4 {
				e.recentFacts = e.recentFacts[len(e.recentFacts)-4:]
			}
		}
	}
	e.updatedAt = now
	e.persistSessionLocked()
}

// BuildFactualPrompt constructs a strictly grounded LLM prompt using real track tags + Deezer/Wikipedia editorial.
func (e *Engine) BuildFactualPrompt(
	ctx context.Context,
	djCfg config.DJConfig,
	kind BreakKind,
	prevTrack *DJTrackInfo,
	currTrack DJTrackInfo,
	isOpening bool,
	fromLidarr bool,
) (systemPrompt, userPrompt string, factsSummary []string) {
	langNorm := strings.ToLower(strings.TrimSpace(djCfg.Language))
	langRule := fmt.Sprintf("STRICT LANGUAGE RULE: Write 100%% exclusively in natural, idiomatic %s — like a native %s FM radio host. NEVER mix in English filler words, slang, or English sentences; keep only proper nouns (artist names and original song/album titles) as-is. ALWAYS begin every script with at least 3–5 unmistakably %s words (never open with an English song title, artist name, or station name) so speech synthesis locks onto authentic %s pronunciation from the first syllable.", djCfg.Language, djCfg.Language, djCfg.Language, djCfg.Language)
	hostRef := "BBC Radio 6 Music / KEXP"
	switch langNorm {
	case "", "english":
		langRule = "Speak exclusively in natural, idiomatic English — write like a native FM radio host."
	case "norwegian":
		langRule = "STRENGT SPRÅKKRAV (NORSK BOKMÅL): Skriv 100 % på ekte, muntlig og idiomatisk norsk bokmål — som en innfødt norsk radioprogramleder på NRK P13 eller NRK P3. Bruk ALDRI engelske fyllord, slang eller anglisismer (ikke «vibe», «banger», «track», «next up», «stay tuned» — bruk naturlige norske ord som «låt», «plate», «album», «stemning», «produsert av», «neste låt ut»). Behold KUN egennavn (artistnavn og originale låt-/albumtitler) på originalspråket. VIKTIG FOR UTTALE: Start ALLTID hvert eneste innslag med minst 3–5 tydelig norske ord (f.eks. «Det var…», «Nå skal vi tilbake til…», «Vi går rett videre til…», «Her får du…») — start ALDRI en setning med en engelsk låttittel, et engelsk artistnavn eller et engelsk stasjonsnavn."
		hostRef = "NRK P13 / NRK P3"
	case "auto":
		langRule = "Match the primary language of the track/artist if obvious (e.g. Norwegian for Norwegian songs, Swedish for Swedish songs, German for German songs, otherwise English), and keep the entire break cleanly in that single language."
	}

	personaDesc := personaLLMGuidance(djCfg.Persona)
	custom := ""
	if strings.TrimSpace(djCfg.CustomPrompt) != "" {
		custom = "\nAdditional user host instructions: " + strings.TrimSpace(djCfg.CustomPrompt)
	}

	maxWords := djCfg.MaxScriptWords
	if maxWords < 15 {
		maxWords = 45
	}
	bridgeMax := int(float64(maxWords) * 1.45)
	now := time.Now()
	todayStr := now.Format("Monday, January 2, 2006")
	timeStr := now.Format("15:04 MST")
	todayYear := now.Year()
	partOfDay := "late night"
	switch h := now.Hour(); {
	case h >= 5 && h < 8:
		partOfDay = "early morning"
	case h >= 8 && h < 12:
		partOfDay = "morning"
	case h >= 12 && h < 17:
		partOfDay = "afternoon"
	case h >= 17 && h < 21:
		partOfDay = "evening"
	}
	systemPrompt = fmt.Sprintf(`You are the live music-journalist radio DJ hosting "%s".
CURRENT LOCAL DATE & TIME: %s at %s (%s, Current Year: %d).
Persona: %s
%s%s

CRITICAL RULES FOR RICH, NON-GENERIC SPOKEN RADIO SCRIPTS:
1. USE LIVE GOOGLE SEARCH + PROVIDED DOSSIER: You have Google Search enabled. Always look up real, specific, fascinating facts about the SONG, ALBUM, and ARTIST (e.g., who produced/co-wrote it, the story behind the lyrics or recording session, chart history, iconic instrumentation/samples, label/album milestone, or a surprising connection between outgoing and incoming artists). NEVER fabricate facts — every detail must come from the provided dossier or verified Google Search results.
2. STRICT TEMPORAL & TIME-OF-DAY AWARENESS (NO STALE "UPCOMING" PHRASING): Right now it is %s at %s (%s, %d). Keep any time-of-day or seasonal references accurate to THIS exact date and time (never say "good morning" at night, etc.). Online reviews, press releases, and search snippets were often written BEFORE or AT the time a record originally came out and may say "this upcoming album", "due out this fall", "her brand-new single", or "currently on tour". NEVER blindly repeat time-relative wording from older reviews! Always compare the track/album's release date against TODAY (%s). If the song is already in the listener's library or its release date is in the past, ALWAYS refer to it in the past tense (e.g. "from her 2025 album Here for It All", NEVER "from her upcoming album"). Only describe a release as "brand new" if it was released within the last 3 months of %s.
3. ZERO GENERIC FILLER: NEVER say empty clichés like "So glad you could join me", "Sit back and relax", "Turn up the volume", "Here's a great track", or "Let's dive into the music". Jump straight into an intriguing, concrete musical detail that makes the listener think "oh cool, I didn't know that!".
4. NATURAL SPOKEN DELIVERY: Write ONLY the exact spoken words — no markdown, no asterisks, no citations like [1], no URLs, no quotes around the whole script, no stage directions like [music fades], no emojis.
5. BROADCAST PACING:
   - INTRO or OUTRO: 2 vivid, flowing sentences (up to ~%d words, 8–15 seconds spoken) weaving at least 1–2 specific song/album/artist facts naturally.
   - OUT/INTRO: 2 to 3 connected sentences (up to ~%d words, 12–19 seconds spoken) naming a specific fact about the outgoing track first, then smoothly linking into a concrete fact about the incoming track.
6. Vary your structure every break so it sounds like an authentic, deeply knowledgeable human FM host (like %s).`,
		djCfg.RadioStationName, todayStr, timeStr, partOfDay, todayYear, personaDesc, langRule, custom, todayStr, timeStr, partOfDay, todayYear, todayStr, todayStr, maxWords, bridgeMax, hostRef)

	var sb strings.Builder
	switch kind {
	case BreakIntro:
		if isOpening {
			sb.WriteString(fmt.Sprintf("TASK: Write the OPENING INTRO for this %s radio set, introducing the first track below.\n\n", djCfg.RadioStationName))
		} else {
			sb.WriteString("TASK: Write a short, vivid TRACK INTRO leading into the incoming song below (it plays over the start ramp of the track).\n\n")
		}
		sb.WriteString("INCOMING TRACK FACTS:\n")
		block, tags := e.formatTrackFacts(ctx, currTrack, fromLidarr)
		sb.WriteString(block)
		factsSummary = append(factsSummary, tags...)

	case BreakOutro:
		sb.WriteString("TASK: Write a short, warm TRACK OUTRO back-announcing the song that is just finishing (do NOT announce a next song).\n\n")
		sb.WriteString("OUTGOING TRACK FACTS:\n")
		block, tags := e.formatTrackFacts(ctx, currTrack, fromLidarr)
		sb.WriteString(block)
		factsSummary = append(factsSummary, tags...)

	case BreakBridge:
		if isOpening {
			sb.WriteString(fmt.Sprintf("TASK: Write the OPENING BROADCAST BREAK for %s! Track #1 started directly into music, so this first DJ break combines THREE things seamlessly: (1) a sharp, stylish station greeting for %s, (2) a factual OUTRO back-announcing the opening track that just finished, and (3) a vivid factual INTRO segueing directly into the incoming second track.\n\n", djCfg.RadioStationName, djCfg.RadioStationName))
		} else {
			sb.WriteString("TASK: Write a smooth IN/OUTRO RADIO BRIDGE that back-announces the OUTGOING track and segues directly into the INCOMING track.\n\n")
		}
		if prevTrack != nil {
			sb.WriteString("OUTGOING TRACK (just finishing) FACTS:\n")
			pBlock, pTags := e.formatTrackFacts(ctx, *prevTrack, false)
			sb.WriteString(pBlock)
			sb.WriteString("\n")
			factsSummary = append(factsSummary, pTags...)
		}
		sb.WriteString("INCOMING TRACK (starting next) FACTS:\n")
		cBlock, cTags := e.formatTrackFacts(ctx, currTrack, fromLidarr)
		sb.WriteString(cBlock)
		factsSummary = append(factsSummary, cTags...)
	}

	e.mu.RLock()
	recent := append([]string(nil), e.recentFacts...)
	e.mu.RUnlock()
	if len(recent) > 0 {
		sb.WriteString("\nRECENT LINES YOU ALREADY SAID (do NOT repeat the same phrasing or openers):\n")
		for _, line := range recent {
			sb.WriteString("- " + line + "\n")
		}
	}

	if langNorm == "norwegian" {
		sb.WriteString("\nSPRÅKKRAV: Skriv hele innslaget utelukkende på naturlig, muntlig norsk bokmål (NRK-radiostil). Start ALLTID setningen med tydelig norske ord — aldri åpne med engelske låttitler, artistnavn eller engelske uttrykk.\n")
	} else if langNorm != "" && langNorm != "english" && langNorm != "auto" {
		sb.WriteString(fmt.Sprintf("\nLANGUAGE REQUIREMENT: Write the entire spoken break exclusively in natural, idiomatic %s, and ALWAYS begin with unmistakably %s words rather than an English title or phrase.\n", djCfg.Language, djCfg.Language))
	}

	return systemPrompt, sb.String(), factsSummary
}

func (e *Engine) formatTrackFacts(ctx context.Context, tr DJTrackInfo, fromLidarr bool) (string, []string) {
	var sb strings.Builder
	var tags []string

	sb.WriteString(fmt.Sprintf("- Song Title: %s\n", tr.Title))
	sb.WriteString(fmt.Sprintf("- Artist: %s\n", tr.Artist))
	sb.WriteString(fmt.Sprintf("- Album: %s\n", tr.Album))
	if tr.Year != nil && *tr.Year > 0 {
		ageYears := time.Now().Year() - *tr.Year
		if ageYears >= 2 {
			sb.WriteString(fmt.Sprintf("- Release Year: %d (already released %d years ago — NOT upcoming)\n", *tr.Year, ageYears))
		} else if ageYears == 1 {
			sb.WriteString(fmt.Sprintf("- Release Year: %d (already released last year — NOT upcoming)\n", *tr.Year))
		} else {
			sb.WriteString(fmt.Sprintf("- Release Year: %d (already released — NOT upcoming)\n", *tr.Year))
		}
		tags = append(tags, fmt.Sprintf("%d", *tr.Year))
	}
	if tr.TrackNumber != nil && *tr.TrackNumber > 0 {
		sb.WriteString(fmt.Sprintf("- Track Number on Album: #%d\n", *tr.TrackNumber))
	}
	if tr.Genre != nil && strings.TrimSpace(*tr.Genre) != "" {
		sb.WriteString(fmt.Sprintf("- Genre Tag: %s\n", strings.TrimSpace(*tr.Genre)))
		tags = append(tags, strings.TrimSpace(*tr.Genre))
	}
	if tr.IsHiRes && tr.FormatDetail != "" {
		sb.WriteString(fmt.Sprintf("- Audio Quality: Hi-Res %s %s\n", tr.Format, tr.FormatDetail))
	}
	if tr.HasAnalysis {
		if tr.BPM > 0 {
			sb.WriteString(fmt.Sprintf("- Tempo: %.0f BPM\n", tr.BPM))
			tags = append(tags, fmt.Sprintf("%.0f BPM", tr.BPM))
		}
		if tr.VocalStartSec >= 4.0 {
			sb.WriteString(fmt.Sprintf("- Broadcast Timing Cue: Instrumental intro before singing starts is %.1f seconds (keep spoken intro crisp so it finishes before vocals enter).\n", tr.VocalStartSec))
		} else if tr.VocalStartSec > 0 && tr.VocalStartSec < 4.0 {
			sb.WriteString("- Broadcast Timing Cue: Singing starts almost immediately (keep intro ultra-tight, ~15–22 words).\n")
		}
		if tr.Duration > 0 && tr.VocalEndSec > 0 && (tr.Duration-tr.VocalEndSec) >= 6.0 {
			sb.WriteString(fmt.Sprintf("- Broadcast Outro Cue: Clean instrumental outro tail is %.1f seconds after final vocals.\n", tr.Duration-tr.VocalEndSec))
		}
	}
	if fromLidarr {
		sb.WriteString("- Library Note: Freshly imported into the listener's collection via Lidarr.\n")
		tags = append(tags, "Fresh Lidarr Arrival")
	}

	// Lookup or fetch Deezer + Wikipedia Album/Artist editorial AND live Song-level research (Wikipedia + MusicBrainz) concurrently
	var albEd *db.AlbumEditorial
	var artEd *db.ArtistEditorial
	var songRes TrackResearch

	var rwg sync.WaitGroup
	if e.editorial != nil {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			fctx, cancel := context.WithTimeout(ctx, 6*time.Second)
			defer cancel()
			albEd = e.editorial.EnsureAlbum(fctx, tr.AlbumKey, tr.AlbumArtist, tr.Album)
			artEd = e.editorial.EnsureArtist(fctx, strings.ToLower(tr.AlbumArtist), tr.AlbumArtist)
		}()
	}
	if e.research != nil {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			fctx, cancel := context.WithTimeout(ctx, 6*time.Second)
			defer cancel()
			songRes = e.research.FetchTrackResearch(fctx, tr)
		}()
	}
	rwg.Wait()

	if songRes.SongSummary != "" {
		sb.WriteString(fmt.Sprintf("- Individual Song Background (%s): %s\n", songRes.SongSource, songRes.SongSummary))
		tags = append(tags, "Song Wiki")
	}
	if songRes.FirstRelease != "" {
		sb.WriteString(fmt.Sprintf("- First Recording Release Date (MusicBrainz): %s\n", songRes.FirstRelease))
	}
	if len(songRes.RecordingTags) > 0 {
		sb.WriteString(fmt.Sprintf("- Recording Style Tags (MusicBrainz): %s\n", strings.Join(songRes.RecordingTags, ", ")))
	}

	if albEd != nil {
		if albEd.ReleaseDate != nil && *albEd.ReleaseDate != "" {
			sb.WriteString(fmt.Sprintf("- Official Album Release Date: %s\n", *albEd.ReleaseDate))
			tags = append(tags, "Released "+*albEd.ReleaseDate)
		}
		if albEd.Label != nil && *albEd.Label != "" {
			sb.WriteString(fmt.Sprintf("- Record Label: %s\n", *albEd.Label))
			tags = append(tags, *albEd.Label)
		}
		if len(albEd.Genres) > 0 {
			sb.WriteString(fmt.Sprintf("- Editorial Genres: %s\n", strings.Join(albEd.Genres, ", ")))
		}
		if albEd.AlbumFans != nil && *albEd.AlbumFans > 0 {
			sb.WriteString(fmt.Sprintf("- Deezer Album Fans: %d\n", *albEd.AlbumFans))
		}
		if albEd.AlbumSummary != nil && strings.TrimSpace(*albEd.AlbumSummary) != "" {
			sb.WriteString(fmt.Sprintf("- Album Background (Wikipedia): %s\n", truncateText(*albEd.AlbumSummary, 520)))
			tags = append(tags, "Album Bio")
		}
		if albEd.ArtistSummary != nil && strings.TrimSpace(*albEd.ArtistSummary) != "" && (artEd == nil || artEd.Summary == nil) {
			sb.WriteString(fmt.Sprintf("- Artist Background (Wikipedia): %s\n", truncateText(*albEd.ArtistSummary, 440)))
			tags = append(tags, "Artist Bio")
		}
	}
	if artEd != nil {
		if artEd.AlbumCount != nil && *artEd.AlbumCount > 0 {
			sb.WriteString(fmt.Sprintf("- Artist Discography Size: %d albums\n", *artEd.AlbumCount))
		}
		if artEd.Summary != nil && strings.TrimSpace(*artEd.Summary) != "" {
			sb.WriteString(fmt.Sprintf("- Artist Background (Wikipedia): %s\n", truncateText(*artEd.Summary, 440)))
			tags = append(tags, "Artist Bio")
		}
	}
	sb.WriteString(fmt.Sprintf("- Search Hint: Use Google Search if helpful to find a standout verified fact about \"%s\" by %s (album \"%s\").\n", tr.Title, tr.Artist, tr.Album))

	return sb.String(), tags
}

// --- Helpers ---

func personaLLMGuidance(persona string) string {
	switch persona {
	case "upbeat-radio":
		return "Bright, energetic prime-time radio host — crisp, charismatic, enthusiastic, and tight."
	case "vinyl-nerd":
		return "Passionate vinyl crate-digger and music historian — loves sharing exact release dates, record labels, and musical craft without sounding robotic."
	case "party-hype":
		return "Fun weekend mix DJ — upbeat, welcoming, builds momentum while still weaving in a crisp music fact."
	case "nordic-chill":
		return "Calm, understated midnight Nordic radio host — warm, relaxed, unhurried, poetic yet factual."
	default:
		return "Warm, intimate late-night FM radio DJ — velvet tone, effortlessly knowledgeable, conversational and magnetic."
	}
}

// WesternEuropeanLanguages returns the default Western European + Auto language options for DJ Radio.
func WesternEuropeanLanguages() []string {
	return []string{
		"English",
		"Norwegian",
		"Swedish",
		"Danish",
		"Finnish",
		"German",
		"Dutch",
		"French",
		"Spanish",
		"Italian",
		"Portuguese",
		"Auto",
	}
}

// ExtendedWorldLanguages returns additional world languages shown when "Show more languages" is enabled.
func ExtendedWorldLanguages() []string {
	return []string{
		"Icelandic",
		"Irish",
		"Polish",
		"Czech",
		"Greek",
		"Turkish",
		"Ukrainian",
		"Brazilian Portuguese",
		"Latin American Spanish",
		"Japanese",
		"Korean",
		"Mandarin Chinese",
		"Arabic",
		"Hindi",
	}
}

// LanguageBCP47 maps a DJ language display name to its BCP-47 locale for Gemini TTS speechConfig.languageCode.
func LanguageBCP47(language string) string {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "english":
		return "en-US"
	case "norwegian":
		return "nb-NO"
	case "swedish":
		return "sv-SE"
	case "danish":
		return "da-DK"
	case "finnish":
		return "fi-FI"
	case "german":
		return "de-DE"
	case "dutch":
		return "nl-NL"
	case "french":
		return "fr-FR"
	case "spanish":
		return "es-ES"
	case "italian":
		return "it-IT"
	case "portuguese":
		return "pt-PT"
	case "brazilian portuguese":
		return "pt-BR"
	case "latin american spanish":
		return "es-419"
	case "icelandic":
		return "is-IS"
	case "irish":
		return "ga-IE"
	case "polish":
		return "pl-PL"
	case "czech":
		return "cs-CZ"
	case "greek":
		return "el-GR"
	case "turkish":
		return "tr-TR"
	case "ukrainian":
		return "uk-UA"
	case "japanese":
		return "ja-JP"
	case "korean":
		return "ko-KR"
	case "mandarin chinese":
		return "zh-CN"
	case "arabic":
		return "ar-XA"
	case "hindi":
		return "hi-IN"
	default:
		return ""
	}
}

func testVoiceSample(language, station string) string {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "norwegian":
		return fmt.Sprintf("Du lytter til %s, rett fra ditt eget musikkbibliotek. Len deg tilbake, skru opp lyden, så setter vi i gang neste låt.", station)
	case "swedish":
		return fmt.Sprintf("Du lyssnar på %s, direkt från ditt eget musikbibliotek. Luta dig tillbaka, höj volymen, så drar vi igång nästa låt.", station)
	case "danish":
		return fmt.Sprintf("Du lytter til %s, direkte fra dit eget musikbibliotek. Læn dig tilbage, skru op for lyden, og lad os tage næste nummer.", station)
	case "finnish":
		return fmt.Sprintf("Kuuntelet asemaa %s suoraan omasta musiikkikirjastostasi. Nojaa taaksepäin, nosta äänenvoimakkuutta ja sukelletaan seuraavaan kappaleeseen.", station)
	case "german":
		return fmt.Sprintf("Du hörst %s, verlustfrei direkt aus deiner eigenen Musikbibliothek. Lehn dich zurück, dreh auf und los geht es mit dem nächsten Titel.", station)
	case "dutch":
		return fmt.Sprintf("Je luistert naar %s, rechtstreeks vanuit je eigen muziekbibliotheek. Leun achterover, zet het volume wat harder en we gaan door naar het volgende nummer.", station)
	case "french":
		return fmt.Sprintf("Vous écoutez %s, diffusé directement depuis votre propre audiothèque. Installez-vous confortablement et plongeons dans le prochain morceau.", station)
	case "spanish", "latin american spanish":
		return fmt.Sprintf("Estás escuchando %s, transmitiendo directamente desde tu biblioteca musical. Ponte cómodo, sube el volumen y vamos con la siguiente canción.", station)
	case "italian":
		return fmt.Sprintf("Stai ascoltando %s, direttamente dalla tua libreria musicale. Mettiti comodo, alza il volume e partiamo con il prossimo brano.", station)
	case "portuguese", "brazilian portuguese":
		return fmt.Sprintf("Você está ouvindo a %s, direto da sua própria biblioteca musical. Relaxe, aumente o som e vamos para a próxima faixa.", station)
	default:
		return fmt.Sprintf("You're locked into %s, streaming lossless straight from your home library. Sit back, turn up the monitors, and let's dive into the next record.", station)
	}
}

func personaTTSStyle(persona, language string) string {
	lang := strings.ToLower(strings.TrimSpace(language))
	// Write TTS delivery directions natively in the target language using POSITIVE native-phonology cues
	// (avoiding negative mentions of "English accent" which can accidentally prime English phonology!).
	switch lang {
	case "norwegian":
		tone := "varm, rolig og naturlig norsk FM-radioprogramleder i studio"
		switch persona {
		case "upbeat-radio":
			tone = "karismatisk, opplagt og energisk norsk radioprogramleder på dagtid"
		case "vinyl-nerd":
			tone = "kunnskapsrik og entusiastisk norsk musikkjournalist og vinylentusiast"
		case "party-hype":
			tone = "livlig og smilende norsk helge-DJ med god flyt"
		case "nordic-chill":
			tone = "rolig, lun og dempet norsk nattradiovert med behagelig tempo"
		}
		return "Leveringsinstruks (les BARE brukerens tekst ordrett, aldri denne instruksen): " +
			"Du er en innfødt norsk radioprogramleder fra Østlandet (standard østlandsk bokmål, NRK P13/P3-stil). " +
			"Snakk 100 % autentisk, flytende norsk bokmål med ekte norsk tonelag, tydelig norsk rulle-r/flapp-r og rene norske vokaler (æ, ø, å). " +
			"Hold stødig norsk setningsmelodi og norsk uttale gjennom hele innslaget, også rett før og etter internasjonale artistnavn og låttitler. " +
			"Stil: " + tone + "."
	case "swedish":
		return "Leveransinstruktion (läs ENDAST användarens text ordagrant, aldrig denna instruktion): Du är en infödd svensk radiopratare (Sveriges Radio P3/P4-stil). Tala helt autentisk, flytande rikssvenska med äkta svensk språkmelodi, tonaccent och svenska vokaler (å, ä, ö) genom hela inslaget, som en varm och kunnig radiovärd i studio."
	case "danish":
		return "Leveringsinstruktion (læs KUN brugerens tekst ordret, aldrig denne instruktion): Du er en indfødt dansk radiovært (DR P3/P6 Beat-stil). Tal helt autentisk, flydende rigsdansk med ægte dansk sprogmelodi, stød og danske vokaler (æ, ø, å) gennem hele indslaget, som en varm og vidende radiovært i studiet."
	case "finnish":
		return "Esitysohje (lue VAIN käyttäjän teksti sanasta sanaan, älä koskaan tätä ohjetta): Olet syntyperäinen suomalainen radiojuontaja (Yle-tyyli). Puhu täysin luonnollista, sujuvaa yleissuomea aidolla suomalaisella ääntämyksellä, rytmillä ja vokaaleilla läpi koko juonnon, lämpimänä ja asiantuntevana radiojuontajana."
	case "german":
		return "Sprechanweisung (sprich NUR den Text des Nutzers wortwörtlich, niemals diese Anweisung): Du bist ein muttersprachlicher deutscher Radiomoderator. Sprich völlig authentisches, fließendes Hochdeutsch mit echter deutscher Aussprache und natürlicher deutscher Sprachmelodie durch den gesamten Beitrag, wie ein warmer, kenntnisreicher Moderator im Studio."
	case "dutch":
		return "Spreekinstructie (spreek ALLEEN de tekst van de gebruiker letterlijk uit, nooit deze instructie): Je bent een moedertaalspreker Nederlands (NPO Radio-stijl). Spreek volledig authentiek, vloeiend Nederlands met echte Nederlandse uitspraak en zinsmelodie door het hele fragment heen, als een warme, deskundige radiopresentator."
	case "french":
		return "Consigne vocale (prononce UNIQUEMENT le texte de l'utilisateur mot pour mot, jamais cette consigne) : Tu es un animateur radio francophone natif (style FIP / France Inter). Parle un français 100 % authentique et fluide avec une prononciation et une intonation françaises naturelles tout au long de l'intervention."
	case "spanish", "latin american spanish":
		return "Indicación de voz (lee SOLO el texto del usuario literalmente, nunca esta indicación): Eres un locutor de radio nativo hispanohablante. Habla un español 100 % auténtico y fluido con pronunciación y entonación nativas naturales durante toda la locución, como un locutor de radio FM cálido y conocedor."
	case "italian":
		return "Indicazione vocale (pronuncia SOLO il testo dell'utente parola per parola, mai questa indicazione): Sei uno speaker radiofonico madrelingua italiano. Parla un italiano 100 % autentico e fluente con pronuncia e intonazione italiane naturali per tutta la durata dell'intervento, come uno speaker FM caldo e appassionato."
	case "portuguese", "brazilian portuguese":
		return "Instrução vocal (fale APENAS o texto do usuário literalmente, nunca esta instrução): Você é um locutor de rádio nativo de língua portuguesa. Fale português 100 % autêntico e fluente com pronúncia e entonação nativas naturais durante toda a fala, como um locutor de rádio FM caloroso e experiente."
	}

	var base string
	switch persona {
	case "upbeat-radio":
		base = "Speak as a charismatic, upbeat prime-time FM radio DJ with crisp broadcast energy"
	case "vinyl-nerd":
		base = "Speak as a warm, passionate audiophile vinyl radio host sharing liner notes with genuine enthusiasm"
	case "party-hype":
		base = "Speak as a lively weekend radio mix DJ with infectious smile and rhythm"
	case "nordic-chill":
		base = "Speak as a calm, velvet-voiced late-night Nordic radio host with relaxed, intimate pacing"
	default:
		base = "Speak as a warm, smooth, magnetic late-night FM radio DJ in a studio booth"
	}
	if lang != "" && lang != "english" && lang != "auto" {
		return fmt.Sprintf("Voice delivery direction (speak ONLY the user's transcript verbatim; never read this instruction): Speak exclusively as a native %s radio broadcaster with 100%% authentic native %s phonology, rhythm, and cadence throughout the entire utterance. %s.", language, language, base)
	}
	if lang == "auto" {
		return fmt.Sprintf("Voice delivery direction (speak ONLY the user's transcript verbatim; never read this instruction): Match the transcript's language with 100%% authentic native phonology and cadence for that language. %s.", base)
	}
	return fmt.Sprintf("Voice delivery direction (speak ONLY the user's transcript verbatim; never read this instruction): %s.", base)
}

// personaTTSSpeechStyle returns a concise, native-language turn-level style descriptor for
// Gemini 3.8 TTS part.speechMetadata.style so regional accent and cadence lock reliably.
func personaTTSSpeechStyle(persona, language string) string {
	lang := strings.ToLower(strings.TrimSpace(language))
	switch lang {
	case "norwegian":
		tone := "varm, rolig og naturlig studioflyt"
		switch persona {
		case "upbeat-radio":
			tone = "opplagt, karismatisk og energisk dagsradio"
		case "vinyl-nerd":
			tone = "kunnskapsrik og entusiastisk musikkjournalist"
		case "party-hype":
			tone = "livlig og smilende helge-DJ"
		case "nordic-chill":
			tone = "rolig, lun og dempet nattradio"
		}
		return "Innfødt norsk radioprogramleder fra Østlandet (standard østlandsk bokmål, NRK P13/P3), ekte norsk tonelag og uttale; " + tone
	case "swedish":
		return "Infödd svensk radiopratare (rikssvenska, SR P3/P4), äkta svensk språkmelodi och varm studioröst"
	case "danish":
		return "Indfødt dansk radiovært (rigsdansk, DR P3/P6), ægte dansk udtale og varm studiestemme"
	case "finnish":
		return "Syntyperäinen suomalainen radiojuontaja (yleissuomi), aito suomalainen ääntämys ja lämmin studiotyyli"
	case "german":
		return "Muttersprachlicher deutscher Radiomoderator (Hochdeutsch), authentische deutsche Aussprache und warme Studiostimme"
	case "dutch":
		return "Moedertaalspreker Nederlandse radiopresentator (NPO-stijl), authentieke Nederlandse uitspraak en warme studiostem"
	case "french":
		return "Animateur radio francophone natif (style FIP), prononciation française authentique et voix studio chaleureuse"
	case "spanish", "latin american spanish":
		return "Locutor de radio nativo hispanohablante, pronunciación y entonación nativas auténticas, tono cálido de estudio FM"
	case "italian":
		return "Speaker radiofonico madrelingua italiano, pronuncia italiana autentica e voce calda da studio FM"
	case "portuguese", "brazilian portuguese":
		return "Locutor de rádio nativo de língua portuguesa, pronúncia nativa autêntica e voz calorosa de estúdio FM"
	case "", "english":
		switch persona {
		case "upbeat-radio":
			return "Charismatic, upbeat prime-time FM radio host with crisp broadcast energy"
		case "vinyl-nerd":
			return "Warm, passionate audiophile radio host sharing liner notes"
		case "party-hype":
			return "Lively weekend radio mix DJ with infectious rhythm"
		case "nordic-chill":
			return "Calm, velvet-voiced late-night radio host with relaxed pacing"
		default:
			return "Warm, smooth late-night FM radio host in a studio booth"
		}
	case "auto":
		return "Native FM radio host speaking with authentic native phonology for the transcript's language"
	default:
		return fmt.Sprintf("Native %s FM radio broadcaster with authentic %s pronunciation and warm studio delivery", language, language)
	}
}

func newPendingBreak(kind BreakKind) *DJBreak {
	return &DJBreak{
		ID:     randomID(string(kind)),
		Kind:   kind,
		Status: StatusPending,
	}
}

func toDJTrackInfo(tr db.TrackRecord, analysisMap map[string]db.TrackAnalysis) DJTrackInfo {
	artist := displayArtist(tr)
	albArtist := strings.TrimSpace(tr.AlbumArtist)
	if albArtist == "" {
		albArtist = artist
	}
	alb := strings.TrimSpace(tr.Album)
	if alb == "" {
		alb = "Unknown Album"
	}
	hiRes := strings.EqualFold(tr.Format, "dsf") || strings.EqualFold(tr.Format, "dff") ||
		(tr.BitDepth != nil && *tr.BitDepth >= 24) ||
		(tr.SampleRate != nil && *tr.SampleRate >= 88200)
	detail := ""
	if tr.BitDepth != nil && tr.SampleRate != nil && *tr.SampleRate > 0 {
		khz := float64(*tr.SampleRate) / 1000.0
		if khz == float64(int(khz)) {
			detail = fmt.Sprintf("%db/%dkHz", *tr.BitDepth, int(khz))
		} else {
			detail = fmt.Sprintf("%db/%.1fkHz", *tr.BitDepth, khz)
		}
	} else if tr.Bitrate != nil && *tr.Bitrate > 0 {
		detail = fmt.Sprintf("%dk", *tr.Bitrate)
	}
	info := DJTrackInfo{
		ID:           tr.EffectiveID(),
		Title:        tr.Title,
		Artist:       artist,
		AlbumArtist:  albArtist,
		Album:        alb,
		AlbumKey:     strings.ToLower(alb),
		Year:         tr.Year,
		Genre:        tr.Genre,
		TrackNumber:  tr.TrackNumber,
		Duration:     tr.Duration,
		Format:       strings.ToUpper(tr.Format),
		FormatDetail: detail,
		IsHiRes:      hiRes,
		ArtworkHash:  tr.ArtworkHash,
	}
	if analysisMap != nil {
		if an, ok := analysisMap[tr.TrackKey]; ok {
			info.BPM = an.BPM
			info.VocalStartSec = an.VocalStartSec
			info.VocalEndSec = an.VocalEndSec
			info.Energy = an.Energy
			info.LoudnessDB = an.LoudnessDB
			info.PeakLinear = an.PeakLinear
			info.NormGainDB = an.NormGainDB
			info.Tags = an.Tags
			info.HasAnalysis = true
		}
	}
	return info
}

// orderByBPMFlow reorders a selected batch using greedy nearest-BPM + genre affinity chaining
// so consecutive tracks feel smoothly stacked like a real DJ set.
func orderByBPMFlow(tracks []db.TrackRecord, analysisMap map[string]db.TrackAnalysis) []db.TrackRecord {
	if len(tracks) <= 2 || len(analysisMap) == 0 {
		return tracks
	}
	analyzedCount := 0
	for _, tr := range tracks {
		if an, ok := analysisMap[tr.TrackKey]; ok && an.BPM > 40 {
			analyzedCount++
		}
	}
	if analyzedCount < 3 {
		return tracks
	}

	used := make([]bool, len(tracks))
	out := make([]db.TrackRecord, 0, len(tracks))
	out = append(out, tracks[0])
	used[0] = true

	for len(out) < len(tracks) {
		prev := out[len(out)-1]
		prevAn, prevOK := analysisMap[prev.TrackKey]
		prevArtist := strings.ToLower(displayArtist(prev))
		bestIdx := -1
		bestCost := 1e9
		for i, cand := range tracks {
			if used[i] {
				continue
			}
			cost := 25.0
			if an, ok := analysisMap[cand.TrackKey]; ok && prevOK && an.BPM > 40 && prevAn.BPM > 40 {
				diff := an.BPM - prevAn.BPM
				if diff < 0 {
					diff = -diff
				}
				cost = diff
				if prevAn.Genre != "" && an.Genre == prevAn.Genre {
					cost -= 6.0
				}
			}
			if strings.ToLower(displayArtist(cand)) == prevArtist {
				cost += 30.0
			}
			if cost < bestCost {
				bestCost = cost
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		used[bestIdx] = true
		out = append(out, tracks[bestIdx])
	}
	return out
}

// pickBPMStackedSet picks a seed track from analyzed tracks and walks a smooth ±12 BPM / matching-vibe path.
func pickBPMStackedSet(pool []db.TrackRecord, analysisMap map[string]db.TrackAnalysis, count int, rng *mathrand.Rand) []db.TrackRecord {
	if len(pool) == 0 {
		return nil
	}
	cp := append([]db.TrackRecord(nil), pool...)
	rng.Shuffle(len(cp), func(i, j int) { cp[i], cp[j] = cp[j], cp[i] })

	used := make([]bool, len(cp))
	out := make([]db.TrackRecord, 0, min(count, len(cp)))
	out = append(out, cp[0])
	used[0] = true

	for len(out) < count && len(out) < len(cp) {
		prev := out[len(out)-1]
		prevAn := analysisMap[prev.TrackKey]
		prevArtist := strings.ToLower(displayArtist(prev))
		bestIdx := -1
		bestCost := 1e9
		// Sample up to 180 candidates for instant selection even on 50k track libraries
		checked := 0
		for i, cand := range cp {
			if used[i] {
				continue
			}
			checked++
			an := analysisMap[cand.TrackKey]
			diff := an.BPM - prevAn.BPM
			if diff < 0 {
				diff = -diff
			}
			eDiff := an.Energy - prevAn.Energy
			if eDiff < 0 {
				eDiff = -eDiff
			}
			cost := diff + eDiff*14.0
			if prevAn.Genre != "" && an.Genre == prevAn.Genre {
				cost -= 5.0
			}
			if strings.ToLower(displayArtist(cand)) == prevArtist {
				cost += 40.0
			}
			if cost < bestCost {
				bestCost = cost
				bestIdx = i
			}
			if checked >= 180 && bestCost <= 6.0 {
				break
			}
		}
		if bestIdx < 0 {
			break
		}
		used[bestIdx] = true
		out = append(out, cp[bestIdx])
	}
	return out
}

func displayArtist(tr db.TrackRecord) string {
	if s := strings.TrimSpace(tr.Artist); s != "" {
		return s
	}
	if s := strings.TrimSpace(tr.AlbumArtist); s != "" {
		return s
	}
	return "Unknown Artist"
}

func indexTracksByID(tracks []db.TrackRecord) map[string]db.TrackRecord {
	m := make(map[string]db.TrackRecord, len(tracks)*2)
	for _, tr := range tracks {
		m[tr.EffectiveID()] = tr
		m[tr.TrackKey] = tr
		if tr.LegacyID != "" {
			m[tr.LegacyID] = tr
		}
	}
	return m
}

func sortAlbumTracks(tracks []db.TrackRecord) {
	sort.Slice(tracks, func(i, j int) bool {
		di, dj := 1, 1
		if tracks[i].DiscNumber != nil {
			di = *tracks[i].DiscNumber
		}
		if tracks[j].DiscNumber != nil {
			dj = *tracks[j].DiscNumber
		}
		if di != dj {
			return di < dj
		}
		ti, tj := 999, 999
		if tracks[i].TrackNumber != nil {
			ti = *tracks[i].TrackNumber
		}
		if tracks[j].TrackNumber != nil {
			tj = *tracks[j].TrackNumber
		}
		if ti != tj {
			return ti < tj
		}
		return strings.ToLower(tracks[i].Title) < strings.ToLower(tracks[j].Title)
	})
}

func pickDiverseSubset(pool []db.TrackRecord, count int, rng *mathrand.Rand) []db.TrackRecord {
	if len(pool) == 0 {
		return nil
	}
	cp := append([]db.TrackRecord(nil), pool...)
	rng.Shuffle(len(cp), func(i, j int) { cp[i], cp[j] = cp[j], cp[i] })

	// Prefer avoiding back-to-back tracks from the same artist when library has variety
	out := make([]db.TrackRecord, 0, min(count, len(cp)))
	used := make([]bool, len(cp))
	lastArtist := ""
	for len(out) < count && len(out) < len(cp) {
		pickedIdx := -1
		for i, tr := range cp {
			if used[i] {
				continue
			}
			a := strings.ToLower(displayArtist(tr))
			if a != lastArtist || pickedIdx == -1 {
				pickedIdx = i
				if a != lastArtist {
					break
				}
			}
		}
		if pickedIdx < 0 {
			break
		}
		used[pickedIdx] = true
		out = append(out, cp[pickedIdx])
		lastArtist = strings.ToLower(displayArtist(cp[pickedIdx]))
	}
	return out
}

func pickRecentTracks(all []db.TrackRecord, count int, rng *mathrand.Rand) []db.TrackRecord {
	cp := append([]db.TrackRecord(nil), all...)
	sort.Slice(cp, func(i, j int) bool {
		ai, aj := cp[i].ModifiedAt.Unix(), cp[j].ModifiedAt.Unix()
		if cp[i].AddedAt != nil && cp[i].AddedAt.Unix() > ai {
			ai = cp[i].AddedAt.Unix()
		}
		if cp[j].AddedAt != nil && cp[j].AddedAt.Unix() > aj {
			aj = cp[j].AddedAt.Unix()
		}
		return ai > aj
	})
	poolSize := min(len(cp), max(count*4, 60))
	return pickDiverseSubset(cp[:poolSize], count, rng)
}

func truncateText(s string, maxChars int) string {
	s = strings.TrimSpace(s)
	if len(s) <= maxChars {
		return s
	}
	cut := s[:maxChars]
	if idx := strings.LastIndex(cut, "."); idx > maxChars/2 {
		return cut[:idx+1]
	}
	return cut + "..."
}

func randomID(prefix string) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

func isSafeID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
