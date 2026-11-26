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

	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
	"github.com/baardove/my-echo/server/internal/dj"
	"github.com/baardove/my-echo/server/internal/party"
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

type channel struct {
	name string
	hub  *Hub

	mu         sync.RWMutex
	clients    map[chan []byte]struct{}
	running    bool
	nowPlaying string
	cancel     context.CancelFunc
	idleTimer  *time.Timer
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
	ch := make(chan []byte, 48)
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
		var trackPath, voicePath, label string
		var normGainDB float64
		var advanceFn func()

		if c.name == "dj" {
			trackPath, voicePath, label, normGainDB, advanceFn = c.nextDJItem()
		} else {
			trackPath, voicePath, label, normGainDB, advanceFn = c.nextPartyItem()
		}

		if trackPath == "" {
			// Stream gentle digital silence so DLNA receivers stay connected while queue populates
			c.setNowPlaying("Standby — waiting for tracks")
			c.streamSilence(ctx, 2*time.Second)
			continue
		}

		c.setNowPlaying(label)
		if voicePath != "" {
			// Stream the DJ voice break or guest voice intro cleanly ahead of the song
			_ = c.streamFileAsMP3(ctx, voicePath, 0)
		}
		if ctx.Err() != nil {
			return
		}
		_ = c.streamFileAsMP3(ctx, trackPath, normGainDB)
		if ctx.Err() != nil {
			return
		}
		if advanceFn != nil {
			advanceFn()
		}
	}
}

func (c *channel) nextDJItem() (trackPath, voicePath, label string, normGainDB float64, advance func()) {
	if c.hub.djEng == nil {
		return "", "", "", 0, nil
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
			return "", "", "", 0, nil
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
		return "", "", "", 0, nil
	}

	var vp string
	if slot.Bridge != nil && slot.Bridge.Status == dj.StatusReady {
		if p, ok := c.hub.djEng.AudioPathForBreak(slot.Bridge.ID); ok {
			vp = p
		}
	} else if slot.Intro != nil && slot.Intro.Status == dj.StatusReady {
		if p, ok := c.hub.djEng.AudioPathForBreak(slot.Intro.ID); ok {
			vp = p
		}
	}

	label = fmt.Sprintf("%s — %s", slot.Track.Artist, slot.Track.Title)
	normGainDB = slot.Track.NormGainDB
	advance = func() {
		// If current slot has a trailing standalone outro, play it right after the track!
		curState := c.hub.djEng.State()
		if idx < len(curState.Slots) && curState.Slots[idx].Outro != nil && curState.Slots[idx].Outro.Status == dj.StatusReady {
			if p, ok := c.hub.djEng.AudioPathForBreak(curState.Slots[idx].Outro.ID); ok {
				_ = c.streamFileAsMP3(context.Background(), p, 0)
			}
		}
		nextIdx := idx + 1
		if nextIdx >= len(curState.Slots) {
			nextIdx = 0
		}
		c.hub.djEng.SetCurrentIndex(nextIdx)
	}
	return tp, vp, label, normGainDB, advance
}

func (c *channel) nextPartyItem() (trackPath, voicePath, label string, normGainDB float64, advance func()) {
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
				normGainDB = an.NormGainDB
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

func (c *channel) streamFileAsMP3(ctx context.Context, path string, normGainDB float64) error {
	ffBin, err := exec.LookPath("ffmpeg")
	if err != nil {
		return err
	}
	args := []string{"-v", "error", "-re", "-i", path, "-vn", "-ac", "2", "-ar", "44100"}
	if normGainDB != 0 && normGainDB > -15 && normGainDB < 12 {
		args = append(args, "-af", fmt.Sprintf("volume=%.2fdB,alimiter=limit=0.95", normGainDB))
	}
	args = append(args, "-c:a", "libmp3lame", "-b:a", "192k", "-f", "mp3", "pipe:1")
	cmd := exec.CommandContext(ctx, ffBin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	buf := make([]byte, 8192)
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			cp := make([]byte, n)
			copy(cp, buf[:n])
			c.broadcast(cp)
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
