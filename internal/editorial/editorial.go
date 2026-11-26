package editorial

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/baardove/my-echo/server/internal/db"
	"golang.org/x/text/unicode/norm"
)

const RefreshTTL = 14 * 24 * time.Hour

type Status struct {
	IsEnriching   bool   `json:"isEnriching"`
	CachedAlbums  int    `json:"cachedAlbums"`
	TotalAlbums   int    `json:"totalAlbums"`
	CachedArtists int    `json:"cachedArtists"`
	TotalArtists  int    `json:"totalArtists"`
	CurrentItem   string `json:"currentItem,omitempty"`
}

type Fetcher struct {
	store       *db.Store
	httpClient  *http.Client
	onUpdated   func()
	inflightMu  sync.Mutex
	inflightAlb map[string]bool
	inflightArt map[string]bool

	statusMu sync.RWMutex
	status   Status
	trigger  chan struct{}
}

func NewFetcher(store *db.Store, onUpdated func()) *Fetcher {
	return &Fetcher{
		store: store,
		httpClient: &http.Client{
			Timeout: 8 * time.Second,
		},
		onUpdated:   onUpdated,
		inflightAlb: make(map[string]bool),
		inflightArt: make(map[string]bool),
		trigger:     make(chan struct{}, 1),
	}
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

	for _, tr := range tracks {
		albTitle := strings.TrimSpace(tr.Album)
		if albTitle == "" || strings.EqualFold(albTitle, "Unknown Album") {
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
	for k, t := range albumsMap {
		if ex, ok := existingAlb[k]; !ok || time.Since(ex.FetchedAt) >= RefreshTTL {
			todoAlbums = append(todoAlbums, t)
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
	f.setStatus(Status{
		IsEnriching:   len(todoAlbums) > 0 || len(todoArtists) > 0,
		CachedAlbums:  cachedAlbCount,
		TotalAlbums:   len(albumsMap),
		CachedArtists: cachedArtCount,
		TotalArtists:  len(artistsMap),
	})

	if len(todoAlbums) == 0 && len(todoArtists) == 0 {
		if f.onUpdated != nil {
			f.onUpdated()
		}
		return
	}

	log.Printf("[editorial] enriching %d albums and %d artists in background...", len(todoAlbums), len(todoArtists))
	dirtyCount := 0

	for _, t := range todoAlbums {
		if ctx.Err() != nil {
			return
		}
		f.setStatus(Status{
			IsEnriching:   true,
			CachedAlbums:  cachedAlbCount,
			TotalAlbums:   len(albumsMap),
			CachedArtists: cachedArtCount,
			TotalArtists:  len(artistsMap),
			CurrentItem:   fmt.Sprintf("%s — %s", t.artist, t.title),
		})

		albInfo, artInfo := f.FetchAlbumAndArtist(ctx, t.key, t.artist, t.title)
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

		dirtyCount++
		if dirtyCount%20 == 0 && f.onUpdated != nil {
			f.onUpdated()
		}

		// Polite rate-limit so Deezer & Wikipedia APIs stay fast and happy
		select {
		case <-ctx.Done():
			return
		case <-time.After(1200 * time.Millisecond):
		}
	}

	// Fill any remaining artists that weren't already populated via their albums
	for _, a := range todoArtists {
		if ctx.Err() != nil {
			return
		}
		if ex, ok := existingArt[a.key]; ok && time.Since(ex.FetchedAt) < RefreshTTL {
			continue
		}
		f.setStatus(Status{
			IsEnriching:   true,
			CachedAlbums:  cachedAlbCount,
			TotalAlbums:   len(albumsMap),
			CachedArtists: cachedArtCount,
			TotalArtists:  len(artistsMap),
			CurrentItem:   a.name,
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
			return
		case <-time.After(1000 * time.Millisecond):
		}
	}

	f.setStatus(Status{
		IsEnriching:   false,
		CachedAlbums:  cachedAlbCount,
		TotalAlbums:   len(albumsMap),
		CachedArtists: cachedArtCount,
		TotalArtists:  len(artistsMap),
	})

	if dirtyCount > 0 && f.onUpdated != nil {
		f.onUpdated()
	}
	log.Printf("[editorial] pass complete: %d/%d albums, %d/%d artists cached", cachedAlbCount, len(albumsMap), cachedArtCount, len(artistsMap))
}

// EnsureAlbum returns cached album editorial if fresh, or fetches & caches it on demand.
func (f *Fetcher) EnsureAlbum(ctx context.Context, albumKey, artist, albumTitle string) *db.AlbumEditorial {
	key := strings.ToLower(strings.TrimSpace(albumKey))
	if key == "" {
		return nil
	}
	if list, err := f.store.ListAlbumEditorial(); err == nil {
		for _, item := range list {
			if strings.ToLower(item.AlbumKey) == key && time.Since(item.FetchedAt) < RefreshTTL {
				cp := item
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

	alb, art := f.FetchAlbumAndArtist(ctx, key, artist, albumTitle)
	_ = f.store.MergeAlbumEditorial([]db.AlbumEditorial{alb})
	if art != nil {
		_ = f.store.MergeArtistEditorial([]db.ArtistEditorial{*art})
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
	return alb, artPtr
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

// --- Deezer Public API ---

type deezerAlbumBundle struct {
	releaseDate      *string
	label            *string
	genres           []string
	albumFans        *int
	artistFans       *int
	artistAlbumCount *int
	artistImageURL   *string
}

func (f *Fetcher) fetchDeezerAlbumAndArtist(ctx context.Context, artist, album string) deezerAlbumBundle {
	var out deezerAlbumBundle
	queryRaw := album
	if artist != "" {
		queryRaw = artist + " " + album
	}
	u := "https://api.deezer.com/search/album?q=" + url.QueryEscape(queryRaw) + "&limit=6"
	var searchResp struct {
		Data []struct {
			ID     int    `json:"id"`
			Title  string `json:"title"`
			Artist struct {
				Name string `json:"name"`
			} `json:"artist"`
		} `json:"data"`
	}
	if !f.getJSON(ctx, u, "MyEcho/1.0", &searchResp) || len(searchResp.Data) == 0 {
		return out
	}

	normAlb := normalize(album)
	normArt := normalize(artist)
	albumID := searchResp.Data[0].ID
	for _, item := range searchResp.Data {
		t := normalize(item.Title)
		a := normalize(item.Artist.Name)
		tOK := t == normAlb || strings.Contains(t, normAlb) || strings.Contains(normAlb, t)
		aOK := normArt == "" || a == normArt || strings.Contains(a, normArt) || strings.Contains(normArt, a)
		if tOK && aOK {
			albumID = item.ID
			break
		}
	}

	var detail struct {
		ReleaseDate string `json:"release_date"`
		Label       string `json:"label"`
		Fans        int    `json:"fans"`
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
	if !f.getJSON(ctx, fmt.Sprintf("https://api.deezer.com/album/%d", albumID), "MyEcho/1.0", &detail) {
		return out
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
		if f.getJSON(ctx, fmt.Sprintf("https://api.deezer.com/artist/%d", detail.Artist.ID), "MyEcho/1.0", &aDetail) {
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
	u := "https://api.deezer.com/search/artist?q=" + url.QueryEscape(artist) + "&limit=5"
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
