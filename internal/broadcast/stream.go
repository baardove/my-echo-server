package broadcast

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/dj"
	"github.com/baardove/my-echo-server/internal/party"
)

// Hub manages continuous live HTTP/ICY MP3 radio streams for Live DJ Radio (/stream/dj.mp3)
// and Party Mode (/stream/party.mp3), enabling DLNA speakers, AVRs, TVs, VLC, and remote players
// to tune in directly. Encoding only runs while at least one listener is connected (0% idle CPU).
type Hub struct {
	cfg   *config.Config
	store *db.Store
	djEng *dj.Engine
	party *party.Engine

	djChan    *channel
	partyChan *channel
}

type StreamStats struct {
	DJListeners    int    `json:"djListeners"`
	DJNowPlaying   string `json:"djNowPlaying,omitempty"`
	PartyListeners int    `json:"partyListeners"`
	PartyNowPlay   string `json:"partyNowPlaying,omitempty"`
	DJStreamURL    string `json:"djStreamURL"`
	PartyStreamURL string `json:"partyStreamURL"`
}

// RadioTrackInfo holds safe public metadata for one track in the URL-only Radio DJ streaming player.
type RadioTrackInfo struct {
	ID          string  `json:"id"`
	Title       string  `json:"title"`
	Artist      string  `json:"artist"`
	Album       string  `json:"album"`
	Year        *int    `json:"year,omitempty"`
	Duration    float64 `json:"duration"`
	ArtworkHash string  `json:"artworkHash,omitempty"`
	StartedAt   string  `json:"startedAt,omitempty"`
}

// RadioNowPlaying is the scoped read-only status snapshot consumed by the URL-only Radio DJ streaming player.
type RadioNowPlaying struct {
	ServerName   string           `json:"serverName"`
	StationName  string           `json:"stationName"`
	ProfileID    string           `json:"profileId,omitempty"`
	ProfileName  string           `json:"profileName,omitempty"`
	Persona      string           `json:"persona,omitempty"`
	VoiceName    string           `json:"voiceName,omitempty"`
	IsLive       bool             `json:"isLive"`
	IsSpeaking   bool             `json:"isSpeaking"`
	BreakKind    string           `json:"breakKind,omitempty"`
	DJScript     string           `json:"djScript,omitempty"`
	Listeners    int              `json:"listeners"`
	CurrentTrack *RadioTrackInfo  `json:"currentTrack,omitempty"`
	UpNext       []RadioTrackInfo `json:"upNext"`
	UpdatedAt    string           `json:"updatedAt"`
}

type channel struct {
	name string
	hub  *Hub

	mu           sync.RWMutex
	clients      map[chan []byte]struct{}
	running      bool
	nowPlaying   string
	isSpeaking   bool
	breakKind    string
	djScript     string
	currentTrack *RadioTrackInfo
	cancel       context.CancelFunc
	idleTimer    *time.Timer
}

func NewHub(cfg *config.Config, store *db.Store, djEng *dj.Engine, partyEng *party.Engine) *Hub {
	h := &Hub{
		cfg:   cfg,
		store: store,
		djEng: djEng,
		party: partyEng,
	}
	h.djChan = &channel{name: "dj", hub: h, clients: make(map[chan []byte]struct{})}
	h.partyChan = &channel{name: "party", hub: h, clients: make(map[chan []byte]struct{})}
	return h
}

func (h *Hub) Stats() StreamStats {
	base := fmt.Sprintf("http://%s:%d", h.cfg.EffectiveAdvertiseHost(), h.cfg.APIPort)
	h.djChan.mu.RLock()
	djL, djNP := len(h.djChan.clients), h.djChan.nowPlaying
	h.djChan.mu.RUnlock()

	h.partyChan.mu.RLock()
	pL, pNP := len(h.partyChan.clients), h.partyChan.nowPlaying
	h.partyChan.mu.RUnlock()

	return StreamStats{
		DJListeners:    djL,
		DJNowPlaying:   djNP,
		PartyListeners: pL,
		PartyNowPlay:   pNP,
		DJStreamURL:    base + "/stream/dj.mp3",
		PartyStreamURL: base + "/stream/party.mp3",
	}
}

// RadioStatus returns safe, synchronized live metadata for the URL-only Radio DJ streaming player.
func (h *Hub) RadioStatus() RadioNowPlaying {
	station := strings.TrimSpace(h.cfg.DJ.RadioStationName)
	if station == "" {
		station = "Echo FM"
	}

	h.djChan.mu.RLock()
	listeners := len(h.djChan.clients)
	isLive := h.djChan.running && listeners > 0
	isSpeaking := h.djChan.isSpeaking
	breakKind := h.djChan.breakKind
	djScript := h.djChan.djScript
	var curCopy *RadioTrackInfo
	if h.djChan.currentTrack != nil {
		cp := *h.djChan.currentTrack
		curCopy = &cp
	}
	h.djChan.mu.RUnlock()

	out := RadioNowPlaying{
		ServerName:   h.cfg.ServerName,
		StationName:  station,
		Persona:      h.cfg.DJ.Persona,
		VoiceName:    h.cfg.DJ.VoiceName,
		IsLive:       isLive,
		IsSpeaking:   isSpeaking,
		BreakKind:    breakKind,
		DJScript:     djScript,
		Listeners:    listeners,
		CurrentTrack: curCopy,
		UpNext:       []RadioTrackInfo{},
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	if h.djEng != nil {
		st := h.djEng.State()
		if strings.TrimSpace(st.StationName) != "" {
			out.StationName = st.StationName
		}
		out.ProfileID = st.ProfileID
		out.ProfileName = st.ProfileName
		if st.Persona != "" {
			out.Persona = st.Persona
		}
		if st.VoiceName != "" {
			out.VoiceName = st.VoiceName
		}
		if len(st.Slots) > 0 {
			idx := st.CurrentIndex
			if idx < 0 || idx >= len(st.Slots) {
				idx = 0
			}
			tr := slotToRadioTrack(st.Slots[idx], "")
			if out.CurrentTrack == nil || out.CurrentTrack.ID != tr.ID {
				out.CurrentTrack = &tr
			}
			for i := idx + 1; i < len(st.Slots) && len(out.UpNext) < 4; i++ {
				out.UpNext = append(out.UpNext, slotToRadioTrack(st.Slots[i], ""))
			}
		}
	}
	return out
}

func slotToRadioTrack(slot dj.DJSlot, startedAt string) RadioTrackInfo {
	var art string
	if slot.Track.ArtworkHash != nil {
		art = *slot.Track.ArtworkHash
	}
	return RadioTrackInfo{
		ID:          slot.Track.ID,
		Title:       slot.Track.Title,
		Artist:      slot.Track.Artist,
		Album:       slot.Track.Album,
		Year:        slot.Track.Year,
		Duration:    slot.Track.Duration,
		ArtworkHash: art,
		StartedAt:   startedAt,
	}
}

// ServeDJM3U returns an .m3u playlist file pointing directly to the live Radio DJ MP3 stream.
func (h *Hub) ServeDJM3U(w http.ResponseWriter, r *http.Request, streamURL string) {
	station := h.cfg.DJ.RadioStationName
	if h.djEng != nil {
		if st := h.djEng.State(); strings.TrimSpace(st.StationName) != "" {
			station = st.StationName
		}
	}
	if station == "" {
		station = "My Echo DJ Radio"
	}
	w.Header().Set("Content-Type", "audio/x-mpegurl; charset=utf-8")
	w.Header().Set("Content-Disposition", `inline; filename="echo-radio-dj.m3u"`)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXTINF:-1,%s · Live AI DJ\n%s\n", station, streamURL)
}

func (h *Hub) ServeDJStream(w http.ResponseWriter, r *http.Request) {
	station := h.cfg.DJ.RadioStationName
	if station == "" {
		station = "My Echo DJ Radio"
	}
	h.djChan.serveHTTP(w, r, station+" · Live AI DJ")
}

func (h *Hub) ServePartyStream(w http.ResponseWriter, r *http.Request) {
	h.partyChan.serveHTTP(w, r, h.cfg.ServerName+" · Party Mode Live")
}

func (c *channel) serveHTTP(w http.ResponseWriter, r *http.Request, icyName string) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		http.Error(w, "ffmpeg not available for live streaming", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "audio/mpeg")
	w.Header().Set("Cache-Control", "no-store, no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Accept-Ranges", "none")
	w.Header().Set("transferMode.dlna.org", "Streaming")
	w.Header().Set("contentFeatures.dlna.org", "DLNA.ORG_PN=MP3;DLNA.ORG_OP=00;DLNA.ORG_CI=1;DLNA.ORG_FLAGS=01700000000000000000000000000000")
	w.Header().Set("icy-name", icyName)
	w.Header().Set("icy-br", "192")
	w.Header().Set("icy-pub", "0")

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	flusher, _ := w.(http.Flusher)
	ch := c.subscribe()
	defer c.unsubscribe(ch)

	w.WriteHeader(http.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-ch:
			if !ok {
				return
			}
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

func (c *channel) subscribe() chan []byte {
	// Tight 10-chunk buffer (~1-2s) keeps all live listeners and DLNA speakers tightly synchronized in real time.
	ch := make(chan []byte, 10)
	c.mu.Lock()
	c.clients[ch] = struct{}{}
	if c.idleTimer != nil {
		c.idleTimer.Stop()
		c.idleTimer = nil
	}
	if !c.running {
		c.running = true
		ctx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel
		go c.runLoop(ctx)
	}
	c.mu.Unlock()
	return ch
}

func (c *channel) unsubscribe(ch chan []byte) {
	c.mu.Lock()
	delete(c.clients, ch)
	close(ch)
	if len(c.clients) == 0 && c.running {
		if c.idleTimer != nil {
			c.idleTimer.Stop()
		}
		c.idleTimer = time.AfterFunc(15*time.Second, func() {
			c.mu.Lock()
			if len(c.clients) == 0 && c.cancel != nil {
				c.cancel()
				c.cancel = nil
				c.running = false
				c.nowPlaying = ""
			}
			c.mu.Unlock()
		})
	}
	c.mu.Unlock()
}

func (c *channel) broadcast(chunk []byte) {
	c.mu.RLock()
	for ch := range c.clients {
		select {
		case ch <- chunk:
		default:
			// Drop oldest frame for a slow client rather than blocking all listeners
		}
	}
	c.mu.RUnlock()
}

func (c *channel) setNowPlaying(title string) {
	c.mu.Lock()
	c.nowPlaying = title
	c.mu.Unlock()
}

func (c *channel) runLoop(ctx context.Context) {
	log.Printf("[broadcast] started live %s MP3 stream", c.name)
	defer log.Printf("[broadcast] stopped idle %s MP3 stream", c.name)

	for ctx.Err() == nil {
		var trackPath, voicePath, label, sessionID string
		var normGainDB, startSec float64
		var slotIdx int
		var advanceFn func(ctx context.Context)

		if c.name == "dj" {
			trackPath, voicePath, label, sessionID, slotIdx, normGainDB, startSec, advanceFn = c.nextDJItem()
		} else {
			trackPath, voicePath, label, normGainDB, advanceFn = c.nextPartyItem()
		}

		if trackPath == "" {
			// Stream gentle digital silence so DLNA receivers stay connected while queue populates
			c.setNowPlaying("Standby — waiting for tracks")
			c.streamSilence(ctx, 2*time.Second)
			continue
		}

		// Watch for external track skips / profile switches so the live stream cuts over within ~350ms!
		itemCtx, cancelItem := context.WithCancel(ctx)
		cutoverTriggered := false
		if c.name == "dj" && c.hub.djEng != nil {
			go func(expectedSess string, expectedIdx int) {
				tk := time.NewTicker(350 * time.Millisecond)
				defer tk.Stop()
				for {
					select {
					case <-itemCtx.Done():
						return
					case <-tk.C:
						st := c.hub.djEng.State()
						if st.SessionID != expectedSess || st.CurrentIndex != expectedIdx {
							cutoverTriggered = true
							cancelItem()
							return
						}
					}
				}
			}(sessionID, slotIdx)
		}

		c.setNowPlaying(label)
		if c.name == "dj" && c.hub.djEng != nil {
			st := c.hub.djEng.State()
			if slotIdx >= 0 && slotIdx < len(st.Slots) {
				tr := slotToRadioTrack(st.Slots[slotIdx], time.Now().UTC().Format(time.RFC3339))
				c.mu.Lock()
				c.currentTrack = &tr
				c.mu.Unlock()
			}
		}
		if voicePath != "" && startSec < 1.5 {
			// Stream the DJ voice break cleanly ahead of the song (only when starting from the beginning)
			var kind, script string
			if c.name == "dj" && c.hub.djEng != nil {
				st := c.hub.djEng.State()
				if slotIdx >= 0 && slotIdx < len(st.Slots) {
					sl := st.Slots[slotIdx]
					if sl.Bridge != nil && sl.Bridge.Status == dj.StatusReady {
						kind = string(dj.BreakBridge)
						script = sl.Bridge.Script
					} else if sl.Intro != nil && sl.Intro.Status == dj.StatusReady {
						kind = string(dj.BreakIntro)
						script = sl.Intro.Script
					}
				}
			}
			c.mu.Lock()
			c.isSpeaking = true
			c.breakKind = kind
			if script != "" {
				c.djScript = script
			}
			c.mu.Unlock()
			_ = c.streamFileAsMP3(itemCtx, voicePath, 0, 0, nil)
			c.mu.Lock()
			c.isSpeaking = false
			c.breakKind = ""
			c.mu.Unlock()
		}
		if itemCtx.Err() != nil {
			cancelItem()
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var onProgress func(elapsedSec float64)
		if c.name == "dj" && c.hub.djEng != nil {
			onProgress = func(elapsedSec float64) {
				c.hub.djEng.ReportPlayback(slotIdx, startSec+elapsedSec, true, "stream")
			}
		}
		_ = c.streamFileAsMP3(itemCtx, trackPath, normGainDB, startSec, onProgress)
		wasCutover := cutoverTriggered
		cancelItem()
		if ctx.Err() != nil {
			return
		}
		if !wasCutover && advanceFn != nil {
			advanceFn(ctx)
		}
	}
}

func (c *channel) nextDJItem() (trackPath, voicePath, label, sessionID string, slotIdx int, normGainDB, startSec float64, advance func(ctx context.Context)) {
	if c.hub.djEng == nil {
		return "", "", "", "", 0, 0, 0, nil
	}
	st := c.hub.djEng.State()
	if len(st.Slots) == 0 {
		// Automatically spin up the active DJ profile if a DLNA speaker tunes in while standby!
		profID := c.hub.cfg.DJ.ActiveProfileID
		if profID != "" {
			st, _ = c.hub.djEng.CreateSession(dj.CreateSessionRequest{Source: "profile", Value: profID, Count: 12})
		} else {
			st, _ = c.hub.djEng.CreateSession(dj.CreateSessionRequest{Source: "shuffle", Count: 12})
		}
		if len(st.Slots) == 0 {
			return "", "", "", "", 0, 0, 0, nil
		}
	}
	idx := st.CurrentIndex
	if idx < 0 || idx >= len(st.Slots) {
		idx = 0
	}
	slot := st.Slots[idx]
	tp := c.hub.resolveTrackPath(slot.Track.ID)
	if tp == "" {
		nextIdx := idx + 1
		if nextIdx < len(st.Slots) {
			c.hub.djEng.SetCurrentIndex(nextIdx)
		}
		return "", "", "", "", 0, 0, 0, nil
	}

	// If a browser DJ host is actively playing this exact track, seek straight to its live real-time position!
	clkSess, clkIdx, clkPos, clkPlaying, clkSrc, clkAge := c.hub.djEng.LivePlaybackClock()
	if clkSrc == "browser" && clkPlaying && clkAge < 8*time.Second && clkSess == st.SessionID && clkIdx == idx && clkPos > 1.5 {
		if slot.Track.Duration <= 0 || clkPos < slot.Track.Duration-3.0 {
			startSec = clkPos
		}
	}

	var vp string
	if startSec < 1.5 {
		if slot.Bridge != nil && slot.Bridge.Status == dj.StatusReady {
			if p, ok := c.hub.djEng.AudioPathForBreak(slot.Bridge.ID); ok {
				vp = p
			}
		} else if slot.Intro != nil && slot.Intro.Status == dj.StatusReady {
			if p, ok := c.hub.djEng.AudioPathForBreak(slot.Intro.ID); ok {
				vp = p
			}
		}
	}

	label = fmt.Sprintf("%s — %s", slot.Track.Artist, slot.Track.Title)
	normGainDB = c.hub.adjustedNormGain(slot.Track.NormGainDB)
	advance = func(advCtx context.Context) {
		// If current slot has a trailing standalone outro, play it right after the track!
		curState := c.hub.djEng.State()
		if curState.SessionID != st.SessionID || curState.CurrentIndex != idx {
			return
		}
		if idx < len(curState.Slots) && curState.Slots[idx].Outro != nil && curState.Slots[idx].Outro.Status == dj.StatusReady {
			if p, ok := c.hub.djEng.AudioPathForBreak(curState.Slots[idx].Outro.ID); ok {
				_ = c.streamFileAsMP3(advCtx, p, 0, 0, nil)
			}
		}
		nextIdx := idx + 1
		if nextIdx >= len(curState.Slots) {
			nextIdx = 0
		}
		c.hub.djEng.SetCurrentIndex(nextIdx)
	}
	return tp, vp, label, st.SessionID, idx, normGainDB, startSec, advance
}

func (c *channel) nextPartyItem() (trackPath, voicePath, label string, normGainDB float64, advance func(ctx context.Context)) {
	if c.hub.party == nil {
		return "", "", "", 0, nil
	}
	env := c.hub.party.Envelope()
	if !env.Status.IsLive || env.State == nil || len(env.State.Tracks) == 0 {
		return "", "", "", 0, nil
	}
	var cur *party.QueueEntry
	for i := range env.State.Tracks {
		if env.State.Tracks[i].IsCurrent {
			cur = &env.State.Tracks[i]
			break
		}
	}
	if cur == nil {
		for i := range env.State.Tracks {
			if !env.State.Tracks[i].IsPlayed {
				cur = &env.State.Tracks[i]
				break
			}
		}
	}
	if cur == nil {
		cur = &env.State.Tracks[0]
	}
	tp := c.hub.resolveTrackPath(cur.Track.ID)
	if tp == "" {
		return "", "", "", 0, nil
	}
	if c.hub.store != nil {
		if tr, err := c.hub.store.FindTrackByID(cur.Track.ID); err == nil {
			if an, ok := c.hub.store.GetTrackAnalysis(tr.TrackKey); ok {
				normGainDB = c.hub.adjustedNormGain(an.NormGainDB)
			}
		}
	}
	label = fmt.Sprintf("%s — %s", cur.Track.Artist, cur.Track.Title)
	return tp, "", label, normGainDB, nil
}

func (h *Hub) resolveTrackPath(trackID string) string {
	if h.store == nil || strings.TrimSpace(trackID) == "" {
		return ""
	}
	tr, err := h.store.FindTrackByID(trackID)
	if err != nil {
		return ""
	}
	lib, ok := h.cfg.LibraryByID(tr.LibraryID)
	if !ok {
		return ""
	}
	full := filepath.Join(lib.Path, filepath.FromSlash(tr.RelativePath))
	if _, err := os.Stat(full); err != nil {
		return ""
	}
	return full
}

func (h *Hub) adjustedNormGain(baseGainDB float64) float64 {
	targetDB, enabled := h.cfg.EffectiveNormTargetDB()
	if !enabled || baseGainDB == 0 {
		return 0
	}
	// Stored NormGainDB targets -14.0 dBFS; shift by (targetDB - (-14.0)) on the fly!
	g := baseGainDB + (targetDB + 14.0)
	if g < -12.0 {
		g = -12.0
	}
	if g > 6.0 {
		g = 6.0
	}
	return g
}

func (c *channel) streamFileAsMP3(ctx context.Context, path string, normGainDB, startSec float64, onProgress func(elapsedSec float64)) error {
	ffBin, err := exec.LookPath("ffmpeg")
	if err != nil {
		return err
	}
	args := []string{"-v", "error", "-re"}
	if startSec > 0.5 {
		args = append(args, "-ss", fmt.Sprintf("%.2f", startSec))
	}
	args = append(args, "-i", path, "-vn", "-ac", "2", "-ar", "44100")
	if normGainDB != 0 && normGainDB > -15 && normGainDB < 12 {
		args = append(args, "-af", fmt.Sprintf("volume=%.2fdB,alimiter=limit=0.95", normGainDB))
	}
	args = append(args, "-c:a", "libmp3lame", "-b:a", "192k", "-flush_packets", "1", "-f", "mp3", "pipe:1")
	cmd := exec.CommandContext(ctx, ffBin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	startedAt := time.Now()
	lastReport := time.Time{}
	buf := make([]byte, 2048)
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			cp := make([]byte, n)
			copy(cp, buf[:n])
			c.broadcast(cp)
			if onProgress != nil && time.Since(lastReport) >= time.Second {
				lastReport = time.Now()
				onProgress(time.Since(startedAt).Seconds())
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				_ = cmd.Process.Kill()
			}
			break
		}
	}
	return cmd.Wait()
}

func (c *channel) streamSilence(ctx context.Context, dur time.Duration) {
	ffBin, err := exec.LookPath("ffmpeg")
	if err != nil {
		select {
		case <-ctx.Done():
		case <-time.After(dur):
		}
		return
	}
	cmd := exec.CommandContext(ctx, ffBin,
		"-v", "error",
		"-re",
		"-f", "lavfi",
		"-i", "anullsrc=r=44100:cl=stereo",
		"-t", fmt.Sprintf("%.1f", dur.Seconds()),
		"-c:a", "libmp3lame",
		"-b:a", "192k",
		"-f", "mp3",
		"pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	if err := cmd.Start(); err != nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			cp := make([]byte, n)
			copy(cp, buf[:n])
			c.broadcast(cp)
		}
		if rerr != nil {
			break
		}
	}
	_ = cmd.Wait()
}
