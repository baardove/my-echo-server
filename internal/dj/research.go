package dj

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TrackResearch holds live-fetched factual background about an individual song (Wikipedia + MusicBrainz).
type TrackResearch struct {
	SongSummary   string
	SongSource    string
	FirstRelease  string
	RecordingTags []string
	FetchedAt     time.Time
}

type ResearchCache struct {
	mu         sync.RWMutex
	byTrackKey map[string]TrackResearch
	httpClient *http.Client
}

func NewResearchCache() *ResearchCache {
	return &ResearchCache{
		byTrackKey: make(map[string]TrackResearch),
		httpClient: &http.Client{Timeout: 6 * time.Second},
	}
}

// FetchTrackResearch searches Wikipedia (direct song article + full-text search) and MusicBrainz
// for concrete facts about the individual song, caching results in memory.
func (r *ResearchCache) FetchTrackResearch(ctx context.Context, tr DJTrackInfo) TrackResearch {
	cacheKey := strings.ToLower(strings.TrimSpace(tr.Artist) + "|" + strings.TrimSpace(tr.Title))
	r.mu.RLock()
	if cached, ok := r.byTrackKey[cacheKey]; ok && time.Since(cached.FetchedAt) < 24*time.Hour {
		r.mu.RUnlock()
		return cached
	}
	r.mu.RUnlock()

	var out TrackResearch
	out.FetchedAt = time.Now().UTC()

	var wg sync.WaitGroup
	wg.Add(2)

	// 1. Wikipedia Song Article & Search API
	go func() {
		defer wg.Done()
		summary, title := r.fetchWikipediaSongSummary(ctx, tr.Title, tr.Artist, tr.Album)
		if summary != "" {
			out.SongSummary = summary
			out.SongSource = "Wikipedia: " + title
		}
	}()

	// 2. MusicBrainz Recording Search (first release date & community genre/style tags)
	go func() {
		defer wg.Done()
		firstRel, tags := r.fetchMusicBrainzRecording(ctx, tr.Title, tr.Artist)
		out.FirstRelease = firstRel
		out.RecordingTags = tags
	}()

	wg.Wait()

	r.mu.Lock()
	r.byTrackKey[cacheKey] = out
	r.mu.Unlock()
	return out
}

func (r *ResearchCache) fetchWikipediaSongSummary(ctx context.Context, title, artist, album string) (string, string) {
	cleanTitle := stripTrackDecorations(title)
	cleanArtist := primaryArtist(artist)
	if cleanTitle == "" {
		return "", ""
	}

	// Fast path: direct Wikipedia song summary endpoints
	candidates := []string{
		fmt.Sprintf("%s (%s song)", cleanTitle, cleanArtist),
		fmt.Sprintf("%s (song)", cleanTitle),
	}
	for _, cand := range candidates {
		if ex, resolved := r.tryWikiSummary(ctx, cand, cleanArtist); ex != "" {
			return ex, resolved
		}
	}

	// Search path: Wikipedia OpenSearch / Query Search API for `"Title" "Artist" song`
	searchQ := fmt.Sprintf(`"%s" "%s" song`, cleanTitle, cleanArtist)
	u := "https://en.wikipedia.org/w/api.php?action=query&list=search&srlimit=3&format=json&srsearch=" + url.QueryEscape(searchQ)
	var sResp struct {
		Query struct {
			Search []struct {
				Title   string `json:"title"`
				Snippet string `json:"snippet"`
			} `json:"search"`
		} `json:"query"`
	}
	if !r.getJSON(ctx, u, &sResp) {
		return "", ""
	}
	lowTitle := strings.ToLower(cleanTitle)
	lowArtist := strings.ToLower(cleanArtist)
	for _, hit := range sResp.Query.Search {
		ht := strings.ToLower(hit.Title)
		if !strings.Contains(ht, lowTitle) && !strings.Contains(strings.ToLower(hit.Snippet), lowArtist) {
			continue
		}
		if ex, resolved := r.tryWikiSummary(ctx, hit.Title, cleanArtist); ex != "" {
			return ex, resolved
		}
	}
	return "", ""
}

func (r *ResearchCache) tryWikiSummary(ctx context.Context, pageTitle, artistHint string) (string, string) {
	slug := strings.ReplaceAll(strings.TrimSpace(pageTitle), " ", "_")
	u := "https://en.wikipedia.org/api/rest_v1/page/summary/" + url.PathEscape(slug)
	var resp struct {
		Type    string `json:"type"`
		Title   string `json:"title"`
		Extract string `json:"extract"`
	}
	if !r.getJSON(ctx, u, &resp) || resp.Type == "disambiguation" {
		return "", ""
	}
	ex := strings.TrimSpace(resp.Extract)
	if len(ex) < 50 {
		return "", ""
	}
	low := strings.ToLower(ex)
	if !strings.Contains(low, "song") && !strings.Contains(low, "single") && !strings.Contains(low, "track") && !strings.Contains(low, "recorded") && !strings.Contains(low, "album") && !strings.Contains(low, "music") {
		return "", ""
	}
	if artistHint != "" {
		// Verify the article actually references the artist (prevents wrong homonym songs!)
		artWords := strings.Fields(strings.ToLower(artistHint))
		matched := strings.Contains(low, strings.ToLower(artistHint))
		if !matched && len(artWords) > 0 && len(artWords[len(artWords)-1]) >= 4 {
			matched = strings.Contains(low, artWords[len(artWords)-1])
		}
		if !matched {
			return "", ""
		}
	}
	return truncateText(ex, 540), resp.Title
}

func (r *ResearchCache) fetchMusicBrainzRecording(ctx context.Context, title, artist string) (firstRelease string, tags []string) {
	cleanTitle := stripTrackDecorations(title)
	cleanArtist := primaryArtist(artist)
	if cleanTitle == "" || cleanArtist == "" {
		return "", nil
	}
	q := fmt.Sprintf(`recording:"%s" AND artist:"%s"`, cleanTitle, cleanArtist)
	u := "https://musicbrainz.org/ws/2/recording/?fmt=json&limit=2&query=" + url.QueryEscape(q)
	var resp struct {
		Recordings []struct {
			FirstReleaseDate string `json:"first-release-date"`
			Tags             []struct {
				Name  string `json:"name"`
				Count int    `json:"count"`
			} `json:"tags"`
		} `json:"recordings"`
	}
	if !r.getJSON(ctx, u, &resp) || len(resp.Recordings) == 0 {
		return "", nil
	}
	rec := resp.Recordings[0]
	firstRelease = strings.TrimSpace(rec.FirstReleaseDate)
	for _, t := range rec.Tags {
		if strings.TrimSpace(t.Name) != "" && len(tags) < 5 {
			tags = append(tags, strings.TrimSpace(t.Name))
		}
	}
	return firstRelease, tags
}

func (r *ResearchCache) getJSON(ctx context.Context, rawURL string, dst any) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "MyEchoServer/1.0 (https://github.com/baardove/my-echo)")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return json.NewDecoder(resp.Body).Decode(dst) == nil
}

func stripTrackDecorations(s string) string {
	s = strings.TrimSpace(s)
	for _, sep := range []string{" (Remaster", " - Remaster", " [Remaster", " (Live", " (Radio Edit", " (Deluxe", " (feat.", " (Feat."} {
		if idx := strings.Index(strings.ToLower(s), strings.ToLower(sep)); idx > 1 {
			s = strings.TrimSpace(s[:idx])
		}
	}
	return s
}

func primaryArtist(s string) string {
	s = strings.TrimSpace(s)
	for _, sep := range []string{" feat. ", " feat ", " ft. ", " & ", ", ", " / "} {
		if idx := strings.Index(strings.ToLower(s), sep); idx > 1 {
			return strings.TrimSpace(s[:idx])
		}
	}
	return s
}
