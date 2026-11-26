package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
)

const (
	geminiBaseURL     = "https://generativelanguage.googleapis.com/v1beta/models"
	defaultLightModel = "gemini-3.5-flash-lite"
	doneKVKey         = "ai_metadata_backfill_done_v1"
	batchSize         = 14
)

type Status struct {
	IsRunning        bool   `json:"isRunning"`
	Model            string `json:"model"`
	APIKeyConfigured bool   `json:"apiKeyConfigured"`
	ProcessedItems   int    `json:"processedItems"`
	TotalItems       int    `json:"totalItems"`
	UpdatedTracks    int    `json:"updatedTracks"`
	UpdatedAlbums    int    `json:"updatedAlbums"`
	CurrentItem      string `json:"currentItem,omitempty"`
	LastError        string `json:"lastError,omitempty"`
}

// BackfillItem is the normalized structured response returned by the light Gemini model.
type BackfillItem struct {
	ID          string   `json:"id"`
	ReleaseDate string   `json:"releaseDate"` // YYYY-MM-DD or YYYY
	ReleaseYear int      `json:"releaseYear"`
	Genre       string   `json:"genre"`
	Genres      []string `json:"genres"`
	Category    string   `json:"category"`
	Tags        []string `json:"tags"`
	Label       string   `json:"label"`
	Description string   `json:"description"`
}

type workUnit struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"` // "album" | "track"
	Artist      string   `json:"artist"`
	Album       string   `json:"album"`
	TrackTitle  string   `json:"trackTitle,omitempty"`
	SampleSongs []string `json:"sampleTracks,omitempty"`
	ExistingYr  int      `json:"existingYear,omitempty"`
	ExistingGen string   `json:"existingGenre,omitempty"`

	// Internal references for applying updates
	albumKey string
	tracks   []db.TrackRecord
}

type Worker struct {
	cfg        *config.Config
	store      *db.Store
	httpClient *http.Client
	onUpdated  func()

	mu         sync.RWMutex
	status     Status
	doneMap    map[string]int64
	runCancel  context.CancelFunc
	forceNext  bool
	trigger    chan struct{}
}

func NewWorker(cfg *config.Config, store *db.Store, onUpdated func()) *Worker {
	w := &Worker{
		cfg:   cfg,
		store: store,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		onUpdated: onUpdated,
		doneMap:   make(map[string]int64),
		trigger:   make(chan struct{}, 2),
	}
	w.loadDoneMap()
	return w
}

func (w *Worker) EffectiveModel() string {
	if w.cfg != nil && strings.TrimSpace(w.cfg.MetadataBackfillModel) != "" {
		return strings.TrimSpace(w.cfg.MetadataBackfillModel)
	}
	return defaultLightModel
}

func (w *Worker) Status() Status {
	w.mu.RLock()
	st := w.status
	w.mu.RUnlock()
	st.Model = w.EffectiveModel()
	st.APIKeyConfigured = w.cfg != nil && strings.TrimSpace(w.cfg.DJ.GeminiAPIKey) != ""
	return st
}

func (w *Worker) TriggerRun(force bool) {
	w.mu.Lock()
	if force {
		w.forceNext = true
	}
	w.mu.Unlock()
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

func (w *Worker) StopCurrentRun() {
	w.mu.Lock()
	cancel := w.runCancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *Worker) StartBackgroundWorker(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.trigger:
				w.runPass(ctx)
			}
		}
	}()
}

func (w *Worker) loadDoneMap() {
	if w.store == nil {
		return
	}
	if raw, ok := w.store.GetKV(doneKVKey); ok && strings.TrimSpace(raw) != "" {
		var m map[string]int64
		if err := json.Unmarshal([]byte(raw), &m); err == nil && m != nil {
			w.mu.Lock()
			w.doneMap = m
			w.mu.Unlock()
		}
	}
}

func (w *Worker) saveDoneMap() {
	if w.store == nil {
		return
	}
	w.mu.RLock()
	cp := make(map[string]int64, len(w.doneMap))
	for k, v := range w.doneMap {
		cp[k] = v
	}
	w.mu.RUnlock()
	if raw, err := json.Marshal(cp); err == nil {
		_ = w.store.SetKV(doneKVKey, string(raw))
	}
}

func (w *Worker) runPass(parentCtx context.Context) {
	apiKey := ""
	if w.cfg != nil {
		apiKey = strings.TrimSpace(w.cfg.DJ.GeminiAPIKey)
	}
	if apiKey == "" {
		w.mu.Lock()
		w.status.IsRunning = false
		w.status.LastError = "Gemini API key is not configured (set it in Settings → AI Radio DJ)"
		w.mu.Unlock()
		return
	}
	if w.store == nil {
		return
	}

	w.mu.Lock()
	if w.status.IsRunning {
		w.mu.Unlock()
		return
	}
	force := w.forceNext
	w.forceNext = false
	runCtx, cancel := context.WithCancel(parentCtx)
	w.runCancel = cancel
	w.status.IsRunning = true
	w.status.LastError = ""
	w.status.CurrentItem = "Scanning library for missing metadata..."
	w.mu.Unlock()

	defer func() {
		cancel()
		w.mu.Lock()
		w.runCancel = nil
		w.status.IsRunning = false
		w.status.CurrentItem = ""
		w.mu.Unlock()
		w.saveDoneMap()
	}()

	tracks, err := w.store.LoadAllTracks()
	if err != nil || len(tracks) == 0 {
		return
	}
	edList, _ := w.store.ListAlbumEditorial()
	edByAlb := make(map[string]db.AlbumEditorial, len(edList))
	for _, e := range edList {
		edByAlb[e.AlbumKey] = e
	}
	anMap, _ := w.store.ListTrackAnalysisMap()

	units := w.buildWorkUnits(tracks, edByAlb, anMap, force)
	w.mu.Lock()
	w.status.TotalItems = len(units)
	w.status.ProcessedItems = 0
	w.mu.Unlock()

	if len(units) == 0 {
		w.mu.Lock()
		w.status.CurrentItem = "All albums & tracks already enriched"
		w.mu.Unlock()
		return
	}

	model := w.EffectiveModel()
	log.Printf("[backfill] starting light-model metadata backfill (%s) on %d units (force=%v)", model, len(units), force)

	changedAny := false
	for start := 0; start < len(units); start += batchSize {
		if runCtx.Err() != nil {
			log.Printf("[backfill] paused at %d/%d units", start, len(units))
			break
		}
		end := start + batchSize
		if end > len(units) {
			end = len(units)
		}
		batch := units[start:end]

		w.mu.Lock()
		if len(batch) > 0 {
			u0 := batch[0]
			w.status.CurrentItem = fmt.Sprintf("Batch %d–%d / %d (%s — %s)", start+1, end, len(units), u0.Artist, firstNonEmpty(u0.Album, u0.TrackTitle))
		}
		w.mu.Unlock()

		items, err := w.callGeminiBatch(runCtx, apiKey, model, batch)
		if err != nil {
			if runCtx.Err() != nil {
				break
			}
			errStr := err.Error()
			w.mu.Lock()
			w.status.LastError = errStr
			w.mu.Unlock()
			log.Printf("[backfill] batch %d-%d error: %v", start+1, end, err)
			// Back off gently on rate-limit / quota / transient server errors
			if strings.Contains(errStr, "429") || strings.Contains(errStr, "503") || strings.Contains(strings.ToLower(errStr), "quota") {
				select {
				case <-runCtx.Done():
					return
				case <-time.After(8 * time.Second):
				}
				// Retry once after backoff
				items, err = w.callGeminiBatch(runCtx, apiKey, model, batch)
				if err != nil {
					log.Printf("[backfill] retry failed, pausing run: %v", err)
					return
				}
				w.mu.Lock()
				w.status.LastError = ""
				w.mu.Unlock()
			} else {
				// Skip bad batch and continue gently
				continue
			}
		}

		byID := make(map[string]BackfillItem, len(items))
		for _, it := range items {
			byID[strings.TrimSpace(it.ID)] = it
		}

		updTracks, updAlbums := w.applyBatchResults(batch, byID, edByAlb, anMap)
		if updTracks > 0 || updAlbums > 0 {
			changedAny = true
		}

		nowUnix := time.Now().UTC().Unix()
		w.mu.Lock()
		for _, u := range batch {
			w.doneMap[u.ID] = nowUnix
		}
		w.status.ProcessedItems = end
		w.status.UpdatedTracks += updTracks
		w.status.UpdatedAlbums += updAlbums
		w.mu.Unlock()

		if end%(batchSize*4) == 0 {
			w.saveDoneMap()
			if changedAny && w.onUpdated != nil {
				w.onUpdated()
				changedAny = false
			}
		}

		// Gentle pacing so we stay well below API RPM limits
		select {
		case <-runCtx.Done():
			return
		case <-time.After(400 * time.Millisecond):
		}
	}

	w.saveDoneMap()
	if changedAny && w.onUpdated != nil {
		w.onUpdated()
	}
	log.Printf("[backfill] completed pass (%d/%d units processed)", w.Status().ProcessedItems, w.Status().TotalItems)
}

func (w *Worker) buildWorkUnits(
	tracks []db.TrackRecord,
	edByAlb map[string]db.AlbumEditorial,
	anMap map[string]db.TrackAnalysis,
	force bool,
) []workUnit {
	w.mu.RLock()
	doneSnap := make(map[string]int64, len(w.doneMap))
	for k, v := range w.doneMap {
		doneSnap[k] = v
	}
	w.mu.RUnlock()

	type albumBucket struct {
		key      string
		album    string
		artists  map[string]int
		tracks   []db.TrackRecord
		isCompil bool
	}
	buckets := make(map[string]*albumBucket)
	var singles []db.TrackRecord

	for _, tr := range tracks {
		alb := strings.TrimSpace(tr.Album)
		if alb == "" || strings.EqualFold(alb, "Unknown Album") || strings.EqualFold(alb, "Singles") {
			singles = append(singles, tr)
			continue
		}
		albKey := strings.ToLower(alb)
		b, ok := buckets[albKey]
		if !ok {
			b = &albumBucket{key: albKey, album: alb, artists: make(map[string]int)}
			buckets[albKey] = b
		}
		art := strings.TrimSpace(tr.AlbumArtist)
		if art == "" {
			art = strings.TrimSpace(tr.Artist)
		}
		b.artists[art]++
		if strings.EqualFold(art, "Various Artists") || strings.EqualFold(art, "VA") {
			b.isCompil = true
		}
		b.tracks = append(b.tracks, tr)
	}

	var units []workUnit
	// Sort album keys deterministically for stable progress/resume
	albKeys := make([]string, 0, len(buckets))
	for k := range buckets {
		albKeys = append(albKeys, k)
	}
	sort.Strings(albKeys)

	for _, k := range albKeys {
		b := buckets[k]
		// Compilations with many distinct artists are better backfilled per-track so each song gets its real year/genre/tags
		if b.isCompil || len(b.artists) >= 5 {
			singles = append(singles, b.tracks...)
			continue
		}
		unitID := "alb:" + k
		if !force && doneSnap[unitID] > 0 {
			continue
		}
		// Check if album already has complete metadata (editorial summary + year + genre + track tags)
		if !force && albumLooksComplete(b.tracks, edByAlb[k], anMap) {
			continue
		}
		topArtist := pickDominantArtist(b.artists)
		var samples []string
		existingYr := 0
		existingGen := ""
		for _, tr := range b.tracks {
			if len(samples) < 4 && strings.TrimSpace(tr.Title) != "" {
				samples = append(samples, strings.TrimSpace(tr.Title))
			}
			if existingYr == 0 && tr.Year != nil && *tr.Year > 0 {
				existingYr = *tr.Year
			}
			if existingGen == "" && tr.Genre != nil && !isWeakGenre(*tr.Genre) {
				existingGen = strings.TrimSpace(*tr.Genre)
			}
		}
		units = append(units, workUnit{
			ID:          unitID,
			Kind:        "album",
			Artist:      topArtist,
			Album:       b.album,
			SampleSongs: samples,
			ExistingYr:  existingYr,
			ExistingGen: existingGen,
			albumKey:    k,
			tracks:      b.tracks,
		})
	}

	// Individual tracks (singles, unknown album, or multi-artist compilations)
	sort.Slice(singles, func(i, j int) bool { return singles[i].TrackKey < singles[j].TrackKey })
	for _, tr := range singles {
		unitID := "trk:" + tr.TrackKey
		if !force && doneSnap[unitID] > 0 {
			continue
		}
		an := anMap[tr.TrackKey]
		hasYear := tr.Year != nil && *tr.Year > 0
		hasGen := (tr.Genre != nil && !isWeakGenre(*tr.Genre)) || !isWeakGenre(an.Genre)
		hasTags := len(an.Tags) >= 2
		if !force && hasYear && hasGen && hasTags {
			continue
		}
		yr := 0
		if tr.Year != nil {
			yr = *tr.Year
		}
		gen := ""
		if tr.Genre != nil {
			gen = strings.TrimSpace(*tr.Genre)
		}
		units = append(units, workUnit{
			ID:          unitID,
			Kind:        "track",
			Artist:      strings.TrimSpace(tr.Artist),
			Album:       strings.TrimSpace(tr.Album),
			TrackTitle:  strings.TrimSpace(tr.Title),
			ExistingYr:  yr,
			ExistingGen: gen,
			albumKey:    strings.ToLower(strings.TrimSpace(tr.Album)),
			tracks:      []db.TrackRecord{tr},
		})
	}

	return units
}

func albumLooksComplete(tracks []db.TrackRecord, ed db.AlbumEditorial, anMap map[string]db.TrackAnalysis) bool {
	hasSummary := ed.AlbumSummary != nil && len(strings.TrimSpace(*ed.AlbumSummary)) > 25
	hasDate := ed.ReleaseDate != nil && len(strings.TrimSpace(*ed.ReleaseDate)) >= 4
	hasGenre := len(ed.Genres) > 0
	allHaveTags := true
	for _, tr := range tracks {
		if tr.Year == nil || *tr.Year == 0 {
			hasDate = false
		}
		if tr.Genre == nil || isWeakGenre(*tr.Genre) {
			hasGenre = false
		}
		if len(anMap[tr.TrackKey].Tags) < 2 {
			allHaveTags = false
		}
	}
	return hasSummary && hasDate && hasGenre && allHaveTags
}

func (w *Worker) applyBatchResults(
	batch []workUnit,
	byID map[string]BackfillItem,
	edByAlb map[string]db.AlbumEditorial,
	anMap map[string]db.TrackAnalysis,
) (updatedTracks, updatedAlbums int) {
	var editorialUpdates []db.AlbumEditorial

	for _, u := range batch {
		res, ok := byID[u.ID]
		if !ok {
			continue
		}
		year := normalizeYear(res.ReleaseYear, res.ReleaseDate)
		primaryGenre := cleanGenre(res.Genre)
		if primaryGenre == "" && len(res.Genres) > 0 {
			primaryGenre = cleanGenre(res.Genres[0])
		}
		allGenres := mergeStringList(res.Genres, primaryGenre)
		tags := mergeStringList(res.Tags, res.Category)

		// 1. Backfill missing album editorial (releaseDate, genres, label, description) without overwriting rich existing info
		if u.albumKey != "" && !strings.EqualFold(u.Album, "Unknown Album") && u.Kind == "album" {
			ed, existed := edByAlb[u.albumKey]
			ed.AlbumKey = u.albumKey
			changedEd := !existed
			if (ed.ReleaseDate == nil || strings.TrimSpace(*ed.ReleaseDate) == "") && (strings.TrimSpace(res.ReleaseDate) != "" || year > 0) {
				rd := strings.TrimSpace(res.ReleaseDate)
				if rd == "" && year > 0 {
					rd = fmt.Sprintf("%04d", year)
				}
				ed.ReleaseDate = &rd
				changedEd = true
			}
			if (ed.Label == nil || strings.TrimSpace(*ed.Label) == "") && strings.TrimSpace(res.Label) != "" {
				lb := strings.TrimSpace(res.Label)
				ed.Label = &lb
				changedEd = true
			}
			if len(ed.Genres) == 0 && len(allGenres) > 0 {
				ed.Genres = allGenres
				changedEd = true
			}
			if (ed.AlbumSummary == nil || len(strings.TrimSpace(*ed.AlbumSummary)) < 20) && strings.TrimSpace(res.Description) != "" {
				desc := strings.TrimSpace(res.Description)
				ed.AlbumSummary = &desc
				changedEd = true
			}
			if changedEd {
				ed.FetchedAt = time.Now().UTC()
				edByAlb[u.albumKey] = ed
				editorialUpdates = append(editorialUpdates, ed)
				updatedAlbums++
			}
		}

		// 2. Backfill missing track year/genre in tracks table + merge category/tags/genre into track_analysis
		for _, tr := range u.tracks {
			trackUpdated := false
			var yrPtr *int
			if (tr.Year == nil || *tr.Year == 0) && year > 0 {
				y := year
				yrPtr = &y
			}
			var genPtr *string
			if (tr.Genre == nil || isWeakGenre(*tr.Genre)) && primaryGenre != "" {
				g := primaryGenre
				genPtr = &g
			}
			if yrPtr != nil || genPtr != nil {
				if changed, err := w.store.BackfillTrackMetadata(tr.TrackKey, yrPtr, genPtr); err == nil && changed {
					trackUpdated = true
				}
			}

			// Merge smart tags / category / genre into track_analysis while preserving existing BPM, vocal cues & loudness!
			if len(tags) > 0 || primaryGenre != "" {
				an, exists := anMap[tr.TrackKey]
				if !exists {
					an = db.TrackAnalysis{
						TrackKey:   tr.TrackKey,
						AnalyzedAt: time.Now().UTC(),
						Version:    1, // Keep Version < AnalysisVersion (2) so DSP audio analyzer still computes BPM/loudness/vocal cues!
					}
				}
				beforeTags := len(an.Tags)
				beforeGen := an.Genre
				if isWeakGenre(an.Genre) && primaryGenre != "" {
					an.Genre = primaryGenre
				}
				an.Tags = mergeStringList(an.Tags, tags...)
				if len(an.Tags) > 8 {
					an.Tags = an.Tags[:8]
				}
				if len(an.Tags) != beforeTags || an.Genre != beforeGen {
					if an.AnalyzedAt.IsZero() {
						an.AnalyzedAt = time.Now().UTC()
					}
					if err := w.store.UpsertTrackAnalysis(an); err == nil {
						anMap[tr.TrackKey] = an
						trackUpdated = true
					}
				}
			}
			if trackUpdated {
				updatedTracks++
			}
		}
	}

	if len(editorialUpdates) > 0 {
		_ = w.store.MergeAlbumEditorial(editorialUpdates)
	}
	return updatedTracks, updatedAlbums
}

func (w *Worker) callGeminiBatch(ctx context.Context, apiKey, model string, batch []workUnit) ([]BackfillItem, error) {
	endpoint := fmt.Sprintf("%s/%s:generateContent?key=%s", geminiBaseURL, url.PathEscape(model), url.QueryEscape(apiKey))

	reqBytes, _ := json.Marshal(batch)
	systemPrompt := `You are a precise music metadata archivist performing batch metadata backfill for a personal music library.
For each input item (album or track), return factual music metadata as a JSON array matching the input IDs 1:1.
Rules:
1. Return ONLY a valid JSON array (no markdown fences, no commentary).
2. Each object MUST have:
   - "id": exact input id string
   - "releaseDate": original release date "YYYY-MM-DD" or "YYYY" (empty string if genuinely unknown)
   - "releaseYear": integer year (e.g. 1994, or 0 if genuinely unknown)
   - "genre": concise primary genre (e.g. "Synthpop", "Classic Rock", "Jazz", "Christmas", "Electronic", "Pop", "Classical")
   - "genres": 1 to 3 relevant genre strings
   - "category": broad category or theme (e.g. "Holiday", "Soundtrack", "Dance", "Indie", "Acoustic", "Live", "Pop", "Rock", "Ambient", "Nordic")
   - "tags": 3 to 6 concise searchable vibe/theme tags (include seasonal/theme tags like "Christmas" or "Holiday" if applicable, language/region like "Norwegian" if relevant, plus mood/style tags like "Upbeat", "Chill", "80s", "Acoustic", "Ballad")
   - "label": record label if known (empty string if unsure)
   - "description": 1-2 factual sentences describing the album/song's musical style, origin, or notable context (empty string if obscure/unknown — never fabricate!)`

	userPrompt := "Backfill metadata for these music items:\n" + string(reqBytes)

	payload := map[string]any{
		"systemInstruction": map[string]any{
			"parts": []map[string]string{{"text": systemPrompt}},
		},
		"contents": []map[string]any{
			{
				"role":  "user",
				"parts": []map[string]string{{"text": userPrompt}},
			},
		},
		"generationConfig": map[string]any{
			"temperature":      0.2,
			"maxOutputTokens":  4096,
			"responseMimeType": "application/json",
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Gemini backfill request failed: %w", err)
	}
	defer resp.Body.Close()

	rawResp, _ := io.ReadAll(io.LimitReader(resp.Body, 4_000_000))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Gemini %s HTTP %d: %s", model, resp.StatusCode, extractErrText(rawResp))
	}

	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(rawResp, &parsed); err != nil {
		return nil, fmt.Errorf("decode Gemini backfill envelope: %w", err)
	}
	var textBuf strings.Builder
	for _, c := range parsed.Candidates {
		for _, p := range c.Content.Parts {
			textBuf.WriteString(p.Text)
		}
	}
	return ParseBackfillJSON(textBuf.String())
}

// ParseBackfillJSON extracts and decodes the JSON array of BackfillItem entries (tolerating optional markdown fences).
func ParseBackfillJSON(raw string) ([]BackfillItem, error) {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "```") {
		if nl := strings.IndexByte(s, '\n'); nl > 0 {
			s = s[nl+1:]
		}
		s = strings.TrimSuffix(strings.TrimSpace(s), "```")
		s = strings.TrimSpace(s)
	}
	// Handle wrapper object {"items": [...]} if model wrapped the array
	if strings.HasPrefix(s, "{") {
		var wrap struct {
			Items []BackfillItem `json:"items"`
		}
		if err := json.Unmarshal([]byte(s), &wrap); err == nil && len(wrap.Items) > 0 {
			return wrap.Items, nil
		}
	}
	var items []BackfillItem
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		return nil, fmt.Errorf("parse backfill JSON array: %w", err)
	}
	return items, nil
}

func extractErrText(raw []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &e); err == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	s := strings.TrimSpace(string(raw))
	if len(s) > 220 {
		s = s[:220]
	}
	return s
}

func normalizeYear(yr int, dateStr string) int {
	if yr >= 1900 && yr <= time.Now().Year()+1 {
		return yr
	}
	ds := strings.TrimSpace(dateStr)
	if len(ds) >= 4 {
		var parsed int
		if _, err := fmt.Sscanf(ds[:4], "%d", &parsed); err == nil && parsed >= 1900 && parsed <= time.Now().Year()+1 {
			return parsed
		}
	}
	return 0
}

func isWeakGenre(g string) bool {
	s := strings.ToLower(strings.TrimSpace(g))
	return s == "" || s == "unknown" || s == "other" || s == "misc" || s == "none"
}

func cleanGenre(g string) string {
	s := strings.TrimSpace(g)
	if isWeakGenre(s) {
		return ""
	}
	return s
}

func mergeStringList(base []string, extra ...string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(base)+len(extra))
	add := func(v string) {
		s := strings.TrimSpace(v)
		if s == "" || strings.EqualFold(s, "unknown") {
			return
		}
		k := strings.ToLower(s)
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, s)
	}
	for _, b := range base {
		add(b)
	}
	for _, e := range extra {
		add(e)
	}
	return out
}

func pickDominantArtist(counts map[string]int) string {
	best := ""
	bestN := -1
	for art, n := range counts {
		if n > bestN || (n == bestN && art < best) {
			best = art
			bestN = n
		}
	}
	return best
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return strings.TrimSpace(a)
	}
	return strings.TrimSpace(b)
}
