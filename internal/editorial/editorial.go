package editorial

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/baardove/my-echo-server/internal/artwork"
	"github.com/baardove/my-echo-server/internal/db"
	"golang.org/x/text/unicode/norm"
)

const (
	RefreshTTL         = 14 * 24 * time.Hour
	artworkAttemptKV   = "artwork_fetch_attempted_v1"
	maxCoverImageBytes = 10 * 1024 * 1024
)

type Status struct {
	IsEnriching     bool   `json:"isEnriching"`
	CachedAlbums    int    `json:"cachedAlbums"`
	TotalAlbums     int    `json:"totalAlbums"`
	CachedArtists   int    `json:"cachedArtists"`
	TotalArtists    int    `json:"totalArtists"`
	FetchedArtworks int    `json:"fetchedArtworks,omitempty"`
	CurrentItem     string `json:"currentItem,omitempty"`
}

type Fetcher struct {
	store           *db.Store
	artCache        *artwork.Cache
	httpClient      *http.Client
	onUpdated       func()
	onTracksUpdated func([]db.TrackRecord)
	deezerBaseURL   string
	itunesBaseURL   string

	inflightMu  sync.Mutex
	inflightAlb map[string]bool
	inflightArt map[string]bool

	attemptMu       sync.Mutex
	artworkAttempts map[string]int64

	statusMu sync.RWMutex
	status   Status
	trigger  chan struct{}
}

func NewFetcher(store *db.Store, onUpdated func()) *Fetcher {
	f := &Fetcher{
		store: store,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		onUpdated:       onUpdated,
		deezerBaseURL:   "https://api.deezer.com",
		itunesBaseURL:   "https://itunes.apple.com",
		inflightAlb:     make(map[string]bool),
		inflightArt:     make(map[string]bool),
		artworkAttempts: make(map[string]int64),
		trigger:         make(chan struct{}, 1),
	}
	f.loadArtworkAttempts()
	return f
}

// SetArtworkCache enables automatic fetching & caching of missing album cover art whenever metadata is pulled.
func (f *Fetcher) SetArtworkCache(cache *artwork.Cache, onTracksUpdated func([]db.TrackRecord)) {
	f.artCache = cache
	f.onTracksUpdated = onTracksUpdated
}

func (f *Fetcher) loadArtworkAttempts() {
	if f.store == nil {
		return
	}
	if raw, ok := f.store.GetKV(artworkAttemptKV); ok && strings.TrimSpace(raw) != "" {
		var m map[string]int64
		if json.Unmarshal([]byte(raw), &m) == nil && m != nil {
			f.attemptMu.Lock()
			f.artworkAttempts = m
			f.attemptMu.Unlock()
		}
	}
}

func (f *Fetcher) saveArtworkAttempts() {
	if f.store == nil {
		return
	}
	f.attemptMu.Lock()
	cp := make(map[string]int64, len(f.artworkAttempts))
	for k, v := range f.artworkAttempts {
		cp[k] = v
	}
	f.attemptMu.Unlock()
	if raw, err := json.Marshal(cp); err == nil {
		_ = f.store.SetKV(artworkAttemptKV, string(raw))
	}
}

func (f *Fetcher) wasArtworkRecentlyAttempted(key string) bool {
	f.attemptMu.Lock()
	ts := f.artworkAttempts[key]
	f.attemptMu.Unlock()
	if ts <= 0 {
		return false
	}
	return time.Since(time.Unix(ts, 0)) < RefreshTTL
}

func (f *Fetcher) markArtworkAttempted(key string) {
	f.attemptMu.Lock()
	f.artworkAttempts[key] = time.Now().UTC().Unix()
	f.attemptMu.Unlock()
}

func (f *Fetcher) Status() Status {
	f.statusMu.RLock()
	defer f.statusMu.RUnlock()
	return f.status
}

func (f *Fetcher) setStatus(st Status) {
	f.statusMu.Lock()
	f.status = st
	f.statusMu.Unlock()
}

// TriggerEnrichment wakes up the background enrichment worker after a library scan.
func (f *Fetcher) TriggerEnrichment() {
	select {
	case f.trigger <- struct{}{}:
	default:
	}
}

// StartBackgroundWorker gently enriches uncached or stale (>14d) albums and artists in SQLite,
// then triggers rebuilding `editorial.json.gz` alongside the compressed index shards.
func (f *Fetcher) StartBackgroundWorker(ctx context.Context) {
	go func() {
		// Brief startup delay so initial local disk scan finishes first
		select {
		case <-ctx.Done():
			return
		case <-time.After(4 * time.Second):
		}

		for {
			f.runEnrichmentPass(ctx)
			select {
			case <-ctx.Done():
				return
			case <-f.trigger:
			case <-time.After(6 * time.Hour):
			}
		}
	}()
}

func (f *Fetcher) runEnrichmentPass(ctx context.Context) {
	tracks, err := f.store.LoadAllTracks()
	if err != nil || len(tracks) == 0 {
		return
	}

	type albTarget struct {
		key    string
		title  string
		artist string
	}
	albumsMap := make(map[string]albTarget)
	artistsMap := make(map[string]string)
	missingArtByAlbum := make(map[string][]db.TrackRecord)
	var missingArtSingles []db.TrackRecord

	for _, tr := range tracks {
		missingArt := tr.ArtworkHash == nil || strings.TrimSpace(*tr.ArtworkHash) == ""
		albTitle := strings.TrimSpace(tr.Album)
		if albTitle == "" || strings.EqualFold(albTitle, "Unknown Album") {
			if missingArt && f.artCache != nil {
				missingArtSingles = append(missingArtSingles, tr)
			}
			continue
		}
		artist := strings.TrimSpace(tr.AlbumArtist)
		if artist == "" {
			artist = strings.TrimSpace(tr.Artist)
		}
		albKey := strings.ToLower(albTitle)
		if _, ok := albumsMap[albKey]; !ok {
			albumsMap[albKey] = albTarget{key: albKey, title: albTitle, artist: artist}
		}
		if missingArt && f.artCache != nil {
			missingArtByAlbum[albKey] = append(missingArtByAlbum[albKey], tr)
		}
		cleanArt := primaryArtistName(artist)
		if cleanArt != "" && !isVarious(cleanArt) {
			artistsMap[strings.ToLower(cleanArt)] = cleanArt
		}
	}

	existingAlbList, _ := f.store.ListAlbumEditorial()
	existingAlb := make(map[string]db.AlbumEditorial, len(existingAlbList))
	for _, item := range existingAlbList {
		existingAlb[strings.ToLower(item.AlbumKey)] = item
	}

	existingArtList, _ := f.store.ListArtistEditorial()
	existingArt := make(map[string]db.ArtistEditorial, len(existingArtList))
	for _, item := range existingArtList {
		existingArt[strings.ToLower(item.ArtistKey)] = item
	}

	var todoAlbums []albTarget
	todoAlbumSet := make(map[string]bool)
	for k, t := range albumsMap {
		if ex, ok := existingAlb[k]; !ok || time.Since(ex.FetchedAt) >= RefreshTTL {
			todoAlbums = append(todoAlbums, t)
			todoAlbumSet[k] = true
		}
	}
	var todoArtworkOnlyAlbums []albTarget
	if f.artCache != nil {
		for k, missingTracks := range missingArtByAlbum {
			if len(missingTracks) == 0 || todoAlbumSet[k] {
				continue
			}
			if f.wasArtworkRecentlyAttempted("alb:" + k) {
				continue
			}
			if t, ok := albumsMap[k]; ok {
				todoArtworkOnlyAlbums = append(todoArtworkOnlyAlbums, t)
			}
		}
	}
	var todoArtworkSingles []db.TrackRecord
	if f.artCache != nil {
		for _, tr := range missingArtSingles {
			if !f.wasArtworkRecentlyAttempted("trk:" + tr.TrackKey) {
				todoArtworkSingles = append(todoArtworkSingles, tr)
			}
		}
	}
	var todoArtists []struct{ key, name string }
	for k, name := range artistsMap {
		if ex, ok := existingArt[k]; !ok || time.Since(ex.FetchedAt) >= RefreshTTL {
			todoArtists = append(todoArtists, struct{ key, name string }{k, name})
		}
	}

	cachedAlbCount := len(existingAlb)
	cachedArtCount := len(existingArt)
	fetchedArtworks := 0
	hasWork := len(todoAlbums) > 0 || len(todoArtists) > 0 || len(todoArtworkOnlyAlbums) > 0 || len(todoArtworkSingles) > 0
	f.setStatus(Status{
		IsEnriching:   hasWork,
		CachedAlbums:  cachedAlbCount,
		TotalAlbums:   len(albumsMap),
		CachedArtists: cachedArtCount,
		TotalArtists:  len(artistsMap),
	})

	if !hasWork {
		if f.onUpdated != nil {
			f.onUpdated()
		}
		return
	}

	log.Printf("[editorial] enriching %d albums, %d artists, and %d missing-art items in background...",
		len(todoAlbums), len(todoArtists), len(todoArtworkOnlyAlbums)+len(todoArtworkSingles))
	dirtyCount := 0
	var pendingArtTracks []db.TrackRecord
	flushArtworkTracks := func() {
		if len(pendingArtTracks) == 0 {
			return
		}
		batch := pendingArtTracks
		pendingArtTracks = nil
		if f.onTracksUpdated != nil {
			f.onTracksUpdated(batch)
		}
	}

	for _, t := range todoAlbums {
		if ctx.Err() != nil {
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		}
		f.setStatus(Status{
			IsEnriching:     true,
			CachedAlbums:    cachedAlbCount,
			TotalAlbums:     len(albumsMap),
			CachedArtists:   cachedArtCount,
			TotalArtists:    len(artistsMap),
			FetchedArtworks: fetchedArtworks,
			CurrentItem:     fmt.Sprintf("%s — %s", t.artist, t.title),
		})

		albInfo, artInfo, coverURL := f.fetchAlbumAndArtistWithCover(ctx, t.key, t.artist, t.title)
		_ = f.store.MergeAlbumEditorial([]db.AlbumEditorial{albInfo})
		if _, existed := existingAlb[t.key]; !existed {
			cachedAlbCount++
			existingAlb[t.key] = albInfo
		}
		if artInfo != nil {
			_ = f.store.MergeArtistEditorial([]db.ArtistEditorial{*artInfo})
			if _, existed := existingArt[artInfo.ArtistKey]; !existed {
				cachedArtCount++
				existingArt[artInfo.ArtistKey] = *artInfo
			}
		}

		// If any tracks in this album lack embedded/folder cover art, fetch & cache it right away!
		if f.artCache != nil && len(missingArtByAlbum[t.key]) > 0 {
			f.markArtworkAttempted("alb:" + t.key)
			if updated := f.fillMissingArtworkForAlbumTracks(ctx, t.artist, t.title, missingArtByAlbum[t.key], coverURL); len(updated) > 0 {
				fetchedArtworks++
				pendingArtTracks = append(pendingArtTracks, updated...)
			}
		}

		dirtyCount++
		if dirtyCount%20 == 0 {
			flushArtworkTracks()
			f.saveArtworkAttempts()
			if f.onUpdated != nil {
				f.onUpdated()
			}
		}

		// Polite rate-limit so Deezer & Wikipedia APIs stay fast and happy
		select {
		case <-ctx.Done():
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		case <-time.After(1200 * time.Millisecond):
		}
	}

	// Fetch missing album artwork for albums whose editorial was already cached
	for _, t := range todoArtworkOnlyAlbums {
		if ctx.Err() != nil {
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		}
		f.setStatus(Status{
			IsEnriching:     true,
			CachedAlbums:    cachedAlbCount,
			TotalAlbums:     len(albumsMap),
			CachedArtists:   cachedArtCount,
			TotalArtists:    len(artistsMap),
			FetchedArtworks: fetchedArtworks,
			CurrentItem:     fmt.Sprintf("Cover art: %s — %s", t.artist, t.title),
		})
		f.markArtworkAttempted("alb:" + t.key)
		if updated := f.fillMissingArtworkForAlbumTracks(ctx, t.artist, t.title, missingArtByAlbum[t.key], ""); len(updated) > 0 {
			fetchedArtworks++
			pendingArtTracks = append(pendingArtTracks, updated...)
			if len(pendingArtTracks) >= 40 {
				flushArtworkTracks()
				f.saveArtworkAttempts()
			}
		}
		select {
		case <-ctx.Done():
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		case <-time.After(700 * time.Millisecond):
		}
	}

	// Fetch missing cover art for single tracks without an album
	for _, tr := range todoArtworkSingles {
		if ctx.Err() != nil {
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		}
		f.setStatus(Status{
			IsEnriching:     true,
			CachedAlbums:    cachedAlbCount,
			TotalAlbums:     len(albumsMap),
			CachedArtists:   cachedArtCount,
			TotalArtists:    len(artistsMap),
			FetchedArtworks: fetchedArtworks,
			CurrentItem:     fmt.Sprintf("Cover art: %s — %s", tr.Artist, tr.Title),
		})
		f.markArtworkAttempted("trk:" + tr.TrackKey)
		if updated := f.fillMissingArtworkForAlbumTracks(ctx, tr.Artist, "", []db.TrackRecord{tr}, ""); len(updated) > 0 {
			fetchedArtworks++
			pendingArtTracks = append(pendingArtTracks, updated...)
			if len(pendingArtTracks) >= 40 {
				flushArtworkTracks()
				f.saveArtworkAttempts()
			}
		}
		select {
		case <-ctx.Done():
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		case <-time.After(700 * time.Millisecond):
		}
	}

	// Fill any remaining artists that weren't already populated via their albums
	for _, a := range todoArtists {
		if ctx.Err() != nil {
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		}
		if ex, ok := existingArt[a.key]; ok && time.Since(ex.FetchedAt) < RefreshTTL {
			continue
		}
		f.setStatus(Status{
			IsEnriching:     true,
			CachedAlbums:    cachedAlbCount,
			TotalAlbums:     len(albumsMap),
			CachedArtists:   cachedArtCount,
			TotalArtists:    len(artistsMap),
			FetchedArtworks: fetchedArtworks,
			CurrentItem:     a.name,
		})
		artInfo := f.FetchArtistOnly(ctx, a.key, a.name)
		_ = f.store.MergeArtistEditorial([]db.ArtistEditorial{artInfo})
		cachedArtCount++
		existingArt[a.key] = artInfo
		dirtyCount++

		if dirtyCount%20 == 0 && f.onUpdated != nil {
			f.onUpdated()
		}
		select {
		case <-ctx.Done():
			flushArtworkTracks()
			f.saveArtworkAttempts()
			return
		case <-time.After(1000 * time.Millisecond):
		}
	}

	flushArtworkTracks()
	f.saveArtworkAttempts()

	f.setStatus(Status{
		IsEnriching:     false,
		CachedAlbums:    cachedAlbCount,
		TotalAlbums:     len(albumsMap),
		CachedArtists:   cachedArtCount,
		TotalArtists:    len(artistsMap),
		FetchedArtworks: fetchedArtworks,
	})

	if dirtyCount > 0 && f.onUpdated != nil {
		f.onUpdated()
	}
	log.Printf("[editorial] pass complete: %d/%d albums, %d/%d artists cached, %d missing covers fetched",
		cachedAlbCount, len(albumsMap), cachedArtCount, len(artistsMap), fetchedArtworks)
}

// EnsureAlbum returns cached album editorial if fresh, or fetches & caches it on demand (and fills missing album cover art).
func (f *Fetcher) EnsureAlbum(ctx context.Context, albumKey, artist, albumTitle string) *db.AlbumEditorial {
	key := strings.ToLower(strings.TrimSpace(albumKey))
	if key == "" {
		return nil
	}
	if list, err := f.store.ListAlbumEditorial(); err == nil {
		for _, item := range list {
			if strings.ToLower(item.AlbumKey) == key && time.Since(item.FetchedAt) < RefreshTTL {
				cp := item
				if f.artCache != nil && !f.wasArtworkRecentlyAttempted("alb:"+key) {
					go f.ensureMissingArtworkForAlbumKey(context.Background(), key, artist, albumTitle, "")
				}
				return &cp
			}
		}
	}

	f.inflightMu.Lock()
	if f.inflightAlb[key] {
		f.inflightMu.Unlock()
		return nil
	}
	f.inflightAlb[key] = true
	f.inflightMu.Unlock()
	defer func() {
		f.inflightMu.Lock()
		delete(f.inflightAlb, key)
		f.inflightMu.Unlock()
	}()

	alb, art, coverURL := f.fetchAlbumAndArtistWithCover(ctx, key, artist, albumTitle)
	_ = f.store.MergeAlbumEditorial([]db.AlbumEditorial{alb})
	if art != nil {
		_ = f.store.MergeArtistEditorial([]db.ArtistEditorial{*art})
	}
	if f.artCache != nil {
		go f.ensureMissingArtworkForAlbumKey(context.Background(), key, artist, albumTitle, coverURL)
	}
	if f.onUpdated != nil {
		go f.onUpdated()
	}
	return &alb
}

// EnsureArtist returns cached artist editorial if fresh, or fetches & caches it on demand.
func (f *Fetcher) EnsureArtist(ctx context.Context, artistKey, artistName string) *db.ArtistEditorial {
	key := strings.ToLower(strings.TrimSpace(artistKey))
	if key == "" || isVarious(key) {
		return nil
	}
	if list, err := f.store.ListArtistEditorial(); err == nil {
		for _, item := range list {
			if strings.ToLower(item.ArtistKey) == key && time.Since(item.FetchedAt) < RefreshTTL {
				cp := item
				return &cp
			}
		}
	}

	f.inflightMu.Lock()
	if f.inflightArt[key] {
		f.inflightMu.Unlock()
		return nil
	}
	f.inflightArt[key] = true
	f.inflightMu.Unlock()
	defer func() {
		f.inflightMu.Lock()
		delete(f.inflightArt, key)
		f.inflightMu.Unlock()
	}()

	art := f.FetchArtistOnly(ctx, key, artistName)
	_ = f.store.MergeArtistEditorial([]db.ArtistEditorial{art})
	if f.onUpdated != nil {
		go f.onUpdated()
	}
	return &art
}

// FetchAlbumAndArtist queries Deezer + Wikipedia concurrently (matching Echo's EditorialInfoFetcher).
func (f *Fetcher) FetchAlbumAndArtist(ctx context.Context, albumKey, artist, albumTitle string) (db.AlbumEditorial, *db.ArtistEditorial) {
	alb, art, _ := f.fetchAlbumAndArtistWithCover(ctx, albumKey, artist, albumTitle)
	return alb, art
}

func (f *Fetcher) fetchAlbumAndArtistWithCover(ctx context.Context, albumKey, artist, albumTitle string) (db.AlbumEditorial, *db.ArtistEditorial, string) {
	cleanArtist := primaryArtistName(artist)
	various := isVarious(cleanArtist)

	var dz deezerAlbumBundle
	var albSummary *string
	var artSummary *string

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		qArtist := cleanArtist
		if various {
			qArtist = ""
		}
		dz = f.fetchDeezerAlbumAndArtist(ctx, qArtist, albumTitle)
	}()
	go func() {
		defer wg.Done()
		cands := []string{
			fmt.Sprintf("%s (%s album)", albumTitle, cleanArtist),
			fmt.Sprintf("%s (album)", albumTitle),
			albumTitle,
		}
		hint := cleanArtist
		if various {
			hint = ""
			cands = []string{fmt.Sprintf("%s (album)", albumTitle), albumTitle}
		}
		albSummary = f.fetchWikipediaSummary(ctx, cands, true, hint)
	}()

	if !various && cleanArtist != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			artSummary = f.fetchWikipediaSummary(ctx, []string{
				cleanArtist + " (band)",
				cleanArtist + " (musician)",
				cleanArtist + " (singer)",
				cleanArtist,
			}, true, "")
		}()
	}
	wg.Wait()

	now := time.Now().UTC()
	genres := dz.genres
	if genres == nil {
		genres = []string{}
	}
	alb := db.AlbumEditorial{
		AlbumKey:         strings.ToLower(albumKey),
		ReleaseDate:      dz.releaseDate,
		Label:            dz.label,
		Genres:           genres,
		AlbumFans:        dz.albumFans,
		ArtistFans:       dz.artistFans,
		ArtistAlbumCount: dz.artistAlbumCount,
		AlbumSummary:     albSummary,
		ArtistSummary:    artSummary,
		ArtistImageURL:   dz.artistImageURL,
		FetchedAt:        now,
	}

	var artPtr *db.ArtistEditorial
	if !various && cleanArtist != "" {
		artPtr = &db.ArtistEditorial{
			ArtistKey:  strings.ToLower(cleanArtist),
			Summary:    artSummary,
			Fans:       dz.artistFans,
			AlbumCount: dz.artistAlbumCount,
			ImageURL:   dz.artistImageURL,
			FetchedAt:  now,
		}
	}
	coverURL := ""
	if dz.coverURL != nil {
		coverURL = *dz.coverURL
	}
	return alb, artPtr, coverURL
}

func (f *Fetcher) FetchArtistOnly(ctx context.Context, artistKey, artistName string) db.ArtistEditorial {
	cleanArtist := primaryArtistName(artistName)
	var fans, albCount *int
	var imgURL *string
	var summary *string

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		fans, albCount, imgURL = f.fetchDeezerArtistOnly(ctx, cleanArtist)
	}()
	go func() {
		defer wg.Done()
		summary = f.fetchWikipediaSummary(ctx, []string{
			cleanArtist + " (band)",
			cleanArtist + " (musician)",
			cleanArtist + " (singer)",
			cleanArtist,
		}, true, "")
	}()
	wg.Wait()

	return db.ArtistEditorial{
		ArtistKey:  strings.ToLower(artistKey),
		Summary:    summary,
		Fans:       fans,
		AlbumCount: albCount,
		ImageURL:   imgURL,
		FetchedAt:  time.Now().UTC(),
	}
}

// --- Deezer & iTunes Cover Art + Metadata API ---

type deezerAlbumBundle struct {
	releaseDate      *string
	label            *string
	genres           []string
	albumFans        *int
	artistFans       *int
	artistAlbumCount *int
	artistImageURL   *string
	coverURL         *string
}

func (f *Fetcher) fetchDeezerAlbumAndArtist(ctx context.Context, artist, album string) deezerAlbumBundle {
	var out deezerAlbumBundle
	queryRaw := album
	if artist != "" {
		queryRaw = artist + " " + album
	}
	u := strings.TrimRight(f.deezerBaseURL, "/") + "/search/album?q=" + url.QueryEscape(queryRaw) + "&limit=6"
	var searchResp struct {
		Data []struct {
			ID          int    `json:"id"`
			Title       string `json:"title"`
			CoverXL     string `json:"cover_xl"`
			CoverBig    string `json:"cover_big"`
			CoverMedium string `json:"cover_medium"`
			Artist      struct {
				Name string `json:"name"`
			} `json:"artist"`
		} `json:"data"`
	}
	if !f.getJSON(ctx, u, "MyEcho/1.0", &searchResp) || len(searchResp.Data) == 0 {
		return out
	}

	normAlb := normalize(album)
	normArt := normalize(artist)
	pickIdx := -1
	for i, item := range searchResp.Data {
		t := normalize(item.Title)
		a := normalize(item.Artist.Name)
		tOK := t == normAlb || strings.Contains(t, normAlb) || strings.Contains(normAlb, t)
		aOK := normArt == "" || a == normArt || strings.Contains(a, normArt) || strings.Contains(normArt, a)
		if tOK && aOK {
			pickIdx = i
			break
		}
	}
	if pickIdx < 0 {
		// Fall back to first hit for editorial metadata, but avoid attaching a mismatched cover
		pickIdx = 0
	} else {
		if c := firstNonEmptyURL(searchResp.Data[pickIdx].CoverXL, searchResp.Data[pickIdx].CoverBig, searchResp.Data[pickIdx].CoverMedium); c != "" {
			out.coverURL = &c
		}
	}
	albumID := searchResp.Data[pickIdx].ID
	matchedTightly := false
	{
		item := searchResp.Data[pickIdx]
		t := normalize(item.Title)
		a := normalize(item.Artist.Name)
		tOK := t == normAlb || strings.Contains(t, normAlb) || strings.Contains(normAlb, t)
		aOK := normArt == "" || a == normArt || strings.Contains(a, normArt) || strings.Contains(normArt, a)
		matchedTightly = tOK && aOK
	}

	var detail struct {
		ReleaseDate string `json:"release_date"`
		Label       string `json:"label"`
		Fans        int    `json:"fans"`
		CoverXL     string `json:"cover_xl"`
		CoverBig    string `json:"cover_big"`
		CoverMedium string `json:"cover_medium"`
		Genres      struct {
			Data []struct {
				Name string `json:"name"`
			} `json:"data"`
		} `json:"genres"`
		Artist struct {
			ID            int    `json:"id"`
			PictureMedium string `json:"picture_medium"`
		} `json:"artist"`
	}
	if !f.getJSON(ctx, fmt.Sprintf("%s/album/%d", strings.TrimRight(f.deezerBaseURL, "/"), albumID), "MyEcho/1.0", &detail) {
		return out
	}
	if matchedTightly {
		if c := firstNonEmptyURL(detail.CoverXL, detail.CoverBig, detail.CoverMedium); c != "" {
			out.coverURL = &c
		}
	}
	if detail.ReleaseDate != "" && detail.ReleaseDate != "0000-00-00" {
		v := detail.ReleaseDate
		out.releaseDate = &v
	}
	if lbl := strings.TrimSpace(detail.Label); lbl != "" {
		out.label = &lbl
	}
	if detail.Fans > 0 {
		v := detail.Fans
		out.albumFans = &v
	}
	for _, g := range detail.Genres.Data {
		if strings.TrimSpace(g.Name) != "" {
			out.genres = append(out.genres, g.Name)
		}
	}
	if detail.Artist.PictureMedium != "" {
		v := detail.Artist.PictureMedium
		out.artistImageURL = &v
	}
	if detail.Artist.ID > 0 {
		var aDetail struct {
			NbFan   int `json:"nb_fan"`
			NbAlbum int `json:"nb_album"`
		}
		if f.getJSON(ctx, fmt.Sprintf("%s/artist/%d", strings.TrimRight(f.deezerBaseURL, "/"), detail.Artist.ID), "MyEcho/1.0", &aDetail) {
			if aDetail.NbFan > 0 {
				v := aDetail.NbFan
				out.artistFans = &v
			}
			if aDetail.NbAlbum > 0 {
				v := aDetail.NbAlbum
				out.artistAlbumCount = &v
			}
		}
	}
	return out
}

func (f *Fetcher) fetchDeezerArtistOnly(ctx context.Context, artist string) (fans, albCount *int, imgURL *string) {
	u := strings.TrimRight(f.deezerBaseURL, "/") + "/search/artist?q=" + url.QueryEscape(artist) + "&limit=5"
	var resp struct {
		Data []struct {
			Name          string `json:"name"`
			NbFan         int    `json:"nb_fan"`
			NbAlbum       int    `json:"nb_album"`
			PictureMedium string `json:"picture_medium"`
		} `json:"data"`
	}
	if !f.getJSON(ctx, u, "MyEcho/1.0", &resp) || len(resp.Data) == 0 {
		return nil, nil, nil
	}
	normArt := normalize(artist)
	pick := resp.Data[0]
	for _, item := range resp.Data {
		if normalize(item.Name) == normArt {
			pick = item
			break
		}
	}
	if pick.NbFan > 0 {
		v := pick.NbFan
		fans = &v
	}
	if pick.NbAlbum > 0 {
		v := pick.NbAlbum
		albCount = &v
	}
	if pick.PictureMedium != "" {
		v := pick.PictureMedium
		imgURL = &v
	}
	return
}

// --- Wikipedia REST Summary API ---

var musicKeywords = []string{
	"album", "band", "singer", "musician", "music", "song", "record", "electronic",
	"rock", "pop", "hip hop", "jazz", "composer", "dj", "duo", "group", "rapper",
	"producer", "soundtrack", "ep", "studio", "trance", "house", "metal", "folk", "blues", "classical",
}

func (f *Fetcher) fetchWikipediaSummary(ctx context.Context, candidates []string, requireMusicHint bool, artistHint string) *string {
	normArt := normalize(artistHint)
	for _, raw := range candidates {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		slug := strings.ReplaceAll(trimmed, " ", "_")
		u := "https://en.wikipedia.org/api/rest_v1/page/summary/" + url.PathEscape(slug)
		var resp struct {
			Type    string `json:"type"`
			Extract string `json:"extract"`
		}
		if !f.getJSON(ctx, u, "MyEcho/1.0 (https://github.com/baardove/my-echo)", &resp) {
			continue
		}
		if resp.Type == "disambiguation" {
			continue
		}
		clean := strings.TrimSpace(resp.Extract)
		if len(clean) < 40 {
			continue
		}
		lower := strings.ToLower(clean)
		if requireMusicHint {
			hasHint := false
			for _, kw := range musicKeywords {
				if strings.Contains(lower, kw) {
					hasHint = true
					break
				}
			}
			if !hasHint {
				continue
			}
		}
		if normArt != "" {
			if !strings.Contains(strings.ToLower(trimmed), normArt) && !strings.Contains(normalize(clean), normArt) {
				continue
			}
		}
		return &clean
	}
	return nil
}

func (f *Fetcher) getJSON(ctx context.Context, rawURL, userAgent string, dst any) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return json.NewDecoder(resp.Body).Decode(dst) == nil
}

// --- Text Helpers ---

func primaryArtistName(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	lower := strings.ToLower(s)
	for _, sep := range []string{" feat. ", " feat ", " ft. ", " ft ", " featuring ", "; ", ", "} {
		if idx := strings.Index(lower, sep); idx > 0 {
			return strings.TrimSpace(s[:idx])
		}
	}
	return s
}

func isVarious(s string) bool {
	l := strings.ToLower(strings.TrimSpace(s))
	return l == "" || l == "various artists" || l == "various" || l == "va" || l == "v.a." || l == "unknown artist"
}

func normalize(s string) string {
	decomposed := norm.NFD.String(strings.ToLower(s))
	var b strings.Builder
	space := false
	for _, r := range decomposed {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			space = false
		} else if !space && b.Len() > 0 {
			b.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(b.String())
}

// --- Missing Album Cover Art Lookup, Download & Persistence ---

// EnsureMissingArtworkForTracks groups tracks lacking ArtworkHash by album (or single track),
// fetches cover art from Deezer (with iTunes Search API fallback), stores it in artwork.Cache,
// persists ArtworkHash in SQLite without overwriting existing artwork, and triggers onTracksUpdated.
func (f *Fetcher) EnsureMissingArtworkForTracks(ctx context.Context, tracks []db.TrackRecord) []db.TrackRecord {
	if f.artCache == nil || f.store == nil || len(tracks) == 0 {
		return nil
	}

	type group struct {
		key        string
		artist     string
		album      string
		trackTitle string
		tracks     []db.TrackRecord
	}
	groups := make(map[string]*group)
	var order []string

	for _, tr := range tracks {
		if tr.ArtworkHash != nil && strings.TrimSpace(*tr.ArtworkHash) != "" {
			continue
		}
		alb := strings.TrimSpace(tr.Album)
		art := strings.TrimSpace(tr.AlbumArtist)
		if art == "" {
			art = strings.TrimSpace(tr.Artist)
		}
		isSingle := alb == "" || strings.EqualFold(alb, "Unknown Album") || strings.EqualFold(alb, "Singles")
		var gk string
		if isSingle {
			gk = "trk:" + tr.TrackKey
		} else {
			gk = "alb:" + strings.ToLower(alb)
		}
		if f.wasArtworkRecentlyAttempted(gk) {
			continue
		}
		g, ok := groups[gk]
		if !ok {
			g = &group{
				key:        gk,
				artist:     art,
				album:      alb,
				trackTitle: strings.TrimSpace(tr.Title),
			}
			if isSingle {
				g.album = ""
			}
			groups[gk] = g
			order = append(order, gk)
		}
		g.tracks = append(g.tracks, tr)
	}

	var allUpdated []db.TrackRecord
	for _, gk := range order {
		if ctx.Err() != nil {
			break
		}
		g := groups[gk]
		f.markArtworkAttempted(gk)
		updated := f.fillMissingArtworkForAlbumTracks(ctx, g.artist, g.album, g.tracks, "")
		if len(updated) > 0 {
			allUpdated = append(allUpdated, updated...)
		}
	}

	if len(order) > 0 {
		f.saveArtworkAttempts()
	}
	if len(allUpdated) > 0 && f.onTracksUpdated != nil {
		f.onTracksUpdated(allUpdated)
	}
	return allUpdated
}

func (f *Fetcher) ensureMissingArtworkForAlbumKey(ctx context.Context, albumKey, artist, albumTitle, preferredCoverURL string) {
	if f.artCache == nil || f.store == nil || albumKey == "" {
		return
	}
	allTracks, err := f.store.LoadAllTracks()
	if err != nil {
		return
	}
	var missing []db.TrackRecord
	for _, tr := range allTracks {
		if strings.ToLower(strings.TrimSpace(tr.Album)) != albumKey {
			continue
		}
		if tr.ArtworkHash == nil || strings.TrimSpace(*tr.ArtworkHash) == "" {
			missing = append(missing, tr)
		}
	}
	if len(missing) == 0 {
		return
	}
	f.markArtworkAttempted("alb:" + albumKey)
	updated := f.fillMissingArtworkForAlbumTracks(ctx, artist, albumTitle, missing, preferredCoverURL)
	f.saveArtworkAttempts()
	if len(updated) > 0 && f.onTracksUpdated != nil {
		f.onTracksUpdated(updated)
	}
}

func (f *Fetcher) fillMissingArtworkForAlbumTracks(
	ctx context.Context,
	artist, albumTitle string,
	candidateTracks []db.TrackRecord,
	preferredCoverURL string,
) []db.TrackRecord {
	if f.artCache == nil || f.store == nil || len(candidateTracks) == 0 {
		return nil
	}
	var missingKeys []string
	var sampleTitle string
	for _, tr := range candidateTracks {
		if tr.ArtworkHash == nil || strings.TrimSpace(*tr.ArtworkHash) == "" {
			missingKeys = append(missingKeys, tr.TrackKey)
			if sampleTitle == "" {
				sampleTitle = strings.TrimSpace(tr.Title)
			}
		}
	}
	if len(missingKeys) == 0 {
		return nil
	}

	coverURL := strings.TrimSpace(preferredCoverURL)
	if coverURL == "" {
		coverURL = f.FetchCoverURL(ctx, artist, albumTitle, sampleTitle)
	}
	if coverURL == "" {
		return nil
	}

	hash := f.downloadAndStoreCover(ctx, coverURL)
	if hash == "" {
		return nil
	}

	updated, err := f.store.BackfillTrackArtwork(missingKeys, hash)
	if err != nil {
		log.Printf("[editorial] backfill artwork save error for %s — %s: %v", artist, albumTitle, err)
		return nil
	}
	if len(updated) > 0 {
		log.Printf("[editorial] fetched missing cover art (%s) for %d track(s): %s — %s",
			hash[:min(8, len(hash))], len(updated), artist, firstNonEmptyStr(albumTitle, sampleTitle))
	}
	return updated
}

// FetchCoverURL resolves a high-resolution album/single cover URL using Deezer first, then iTunes Search API.
func (f *Fetcher) FetchCoverURL(ctx context.Context, artist, albumTitle, trackTitle string) string {
	cleanArtist := primaryArtistName(artist)
	if isVarious(cleanArtist) {
		cleanArtist = ""
	}
	cleanAlbum := strings.TrimSpace(albumTitle)
	if strings.EqualFold(cleanAlbum, "Unknown Album") || strings.EqualFold(cleanAlbum, "Singles") {
		cleanAlbum = ""
	}
	cleanTrack := strings.TrimSpace(trackTitle)

	// 1. Deezer album search
	if cleanAlbum != "" {
		if u := f.fetchDeezerAlbumCoverURL(ctx, cleanArtist, cleanAlbum); u != "" {
			return u
		}
	}
	// 2. Deezer track search (great for singles or mismatched album names)
	if cleanTrack != "" {
		if u := f.fetchDeezerTrackCoverURL(ctx, cleanArtist, cleanTrack); u != "" {
			return u
		}
	}
	// 3. iTunes Search API fallback (album, then song)
	if cleanAlbum != "" {
		if u := f.fetchITunesCoverURL(ctx, cleanArtist, cleanAlbum, "album"); u != "" {
			return u
		}
	}
	if cleanTrack != "" {
		if u := f.fetchITunesCoverURL(ctx, cleanArtist, cleanTrack, "song"); u != "" {
			return u
		}
	}
	return ""
}

func (f *Fetcher) fetchDeezerAlbumCoverURL(ctx context.Context, artist, album string) string {
	queryRaw := album
	if artist != "" {
		queryRaw = artist + " " + album
	}
	u := strings.TrimRight(f.deezerBaseURL, "/") + "/search/album?q=" + url.QueryEscape(queryRaw) + "&limit=6"
	var resp struct {
		Data []struct {
			Title       string `json:"title"`
			CoverXL     string `json:"cover_xl"`
			CoverBig    string `json:"cover_big"`
			CoverMedium string `json:"cover_medium"`
			Artist      struct {
				Name string `json:"name"`
			} `json:"artist"`
		} `json:"data"`
	}
	if !f.getJSON(ctx, u, "MyEcho/1.0", &resp) || len(resp.Data) == 0 {
		return ""
	}
	normAlb := normalize(album)
	normArt := normalize(artist)
	for _, item := range resp.Data {
		t := normalize(item.Title)
		a := normalize(item.Artist.Name)
		tOK := normAlb != "" && (t == normAlb || strings.Contains(t, normAlb) || strings.Contains(normAlb, t))
		aOK := normArt == "" || a == normArt || strings.Contains(a, normArt) || strings.Contains(normArt, a)
		if tOK && aOK {
			if c := firstNonEmptyURL(item.CoverXL, item.CoverBig, item.CoverMedium); c != "" {
				return c
			}
		}
	}
	return ""
}

func (f *Fetcher) fetchDeezerTrackCoverURL(ctx context.Context, artist, trackTitle string) string {
	queryRaw := trackTitle
	if artist != "" {
		queryRaw = artist + " " + trackTitle
	}
	u := strings.TrimRight(f.deezerBaseURL, "/") + "/search/track?q=" + url.QueryEscape(queryRaw) + "&limit=6"
	var resp struct {
		Data []struct {
			Title  string `json:"title"`
			Artist struct {
				Name string `json:"name"`
			} `json:"artist"`
			Album struct {
				CoverXL     string `json:"cover_xl"`
				CoverBig    string `json:"cover_big"`
				CoverMedium string `json:"cover_medium"`
			} `json:"album"`
		} `json:"data"`
	}
	if !f.getJSON(ctx, u, "MyEcho/1.0", &resp) || len(resp.Data) == 0 {
		return ""
	}
	normTrk := normalize(trackTitle)
	normArt := normalize(artist)
	for _, item := range resp.Data {
		t := normalize(item.Title)
		a := normalize(item.Artist.Name)
		tOK := normTrk != "" && (t == normTrk || strings.Contains(t, normTrk) || strings.Contains(normTrk, t))
		aOK := normArt == "" || a == normArt || strings.Contains(a, normArt) || strings.Contains(normArt, a)
		if tOK && aOK {
			if c := firstNonEmptyURL(item.Album.CoverXL, item.Album.CoverBig, item.Album.CoverMedium); c != "" {
				return c
			}
		}
	}
	return ""
}

func (f *Fetcher) fetchITunesCoverURL(ctx context.Context, artist, title, entity string) string {
	queryRaw := title
	if artist != "" {
		queryRaw = artist + " " + title
	}
	u := fmt.Sprintf("%s/search?term=%s&media=music&entity=%s&limit=6",
		strings.TrimRight(f.itunesBaseURL, "/"), url.QueryEscape(queryRaw), url.QueryEscape(entity))
	var resp struct {
		Results []struct {
			ArtistName     string `json:"artistName"`
			CollectionName string `json:"collectionName"`
			TrackName      string `json:"trackName"`
			ArtworkURL100  string `json:"artworkUrl100"`
		} `json:"results"`
	}
	if !f.getJSON(ctx, u, "MyEcho/1.0", &resp) || len(resp.Results) == 0 {
		return ""
	}
	normTarget := normalize(title)
	normArt := normalize(artist)
	for _, item := range resp.Results {
		candTitle := item.CollectionName
		if entity == "song" && item.TrackName != "" {
			candTitle = item.TrackName
		}
		t := normalize(candTitle)
		a := normalize(item.ArtistName)
		tOK := normTarget != "" && (t == normTarget || strings.Contains(t, normTarget) || strings.Contains(normTarget, t))
		aOK := normArt == "" || a == normArt || strings.Contains(a, normArt) || strings.Contains(normArt, a)
		if tOK && aOK && strings.TrimSpace(item.ArtworkURL100) != "" {
			return strings.Replace(strings.TrimSpace(item.ArtworkURL100), "100x100bb", "600x600bb", 1)
		}
	}
	return ""
}

func (f *Fetcher) downloadAndStoreCover(ctx context.Context, rawURL string) string {
	if f.artCache == nil || strings.TrimSpace(rawURL) == "" {
		return ""
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "MyEcho/1.0")
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxCoverImageBytes))
	if err != nil || len(raw) == 0 {
		return ""
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		return ""
	}
	return f.artCache.Store(raw)
}

func firstNonEmptyURL(candidates ...string) string {
	for _, c := range candidates {
		s := strings.TrimSpace(c)
		if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
			return s
		}
	}
	return ""
}

func firstNonEmptyStr(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return strings.TrimSpace(a)
	}
	return strings.TrimSpace(b)
}
