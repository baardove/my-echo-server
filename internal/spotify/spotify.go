package spotify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/baardove/my-echo-server/internal/db"
	"github.com/baardove/my-echo-server/internal/lidarr"
	"github.com/google/uuid"
	"golang.org/x/text/unicode/norm"
)

// TrackItem represents one track scraped from a public Spotify playlist.
type TrackItem struct {
	ID         string  `json:"id"`
	URI        string  `json:"uri"`
	Title      string  `json:"title"`
	Artist     string  `json:"artist"`
	Album      string  `json:"album,omitempty"`
	Duration   float64 `json:"duration"`
	IsExplicit bool    `json:"isExplicit,omitempty"`
	ArtworkURL string  `json:"artworkURL,omitempty"`
}

// PlaylistPayload holds the fetched metadata and track list from Spotify.
type PlaylistPayload struct {
	PlaylistID        string      `json:"playlistId"`
	SourceURL         string      `json:"sourceURL"`
	Name              string      `json:"name"`
	OwnerName         string      `json:"ownerName,omitempty"`
	CoverURL          string      `json:"coverURL,omitempty"`
	Tracks            []TrackItem `json:"tracks"`
	HitPublicEmbedCap bool        `json:"hitPublicEmbedCap"`
}

// MatchEntry represents one track in the preview match report (matched local track or retained missing track).
type MatchEntry struct {
	Index      int        `json:"index"`
	Matched    bool       `json:"matched"`
	Confidence int        `json:"confidence,omitempty"`
	Spotify    TrackItem  `json:"spotify"`
	LocalID    string     `json:"localId,omitempty"`
	LocalTitle string     `json:"localTitle,omitempty"`
	LocalArt   string     `json:"localArtist,omitempty"`
	LocalAlbum string     `json:"localAlbum,omitempty"`
	Format     string     `json:"format,omitempty"`
	ArtHash    *string    `json:"artworkHash,omitempty"`
	Duration   float64    `json:"duration,omitempty"`
}

// MatchReport summarizes matching a Spotify playlist against the server's library.
type MatchReport struct {
	Payload      PlaylistPayload `json:"payload"`
	Entries      []MatchEntry    `json:"entries"`
	MatchedCount int             `json:"matchedCount"`
	MissingCount int             `json:"missingCount"`
	TotalCount   int             `json:"totalCount"`
}

// LidarrSearchStatus exposes live progress for automatic Lidarr search on a playlist's missing tracks.
type LidarrSearchStatus struct {
	Running       bool   `json:"running"`
	PlaylistID    string `json:"playlistId,omitempty"`
	Done          int    `json:"done"`
	Total         int    `json:"total"`
	CurrentTrack  string `json:"currentTrack,omitempty"`
	AlbumsOrdered int    `json:"albumsOrdered"`
	TracksCovered int    `json:"tracksCovered"`
	NotFound      int    `json:"notFound"`
	Failed        int    `json:"failed"`
	LastSummary   string `json:"lastSummary,omitempty"`
}

// Importer handles zero-config Spotify public playlist fetching, local matching,
// artwork enrichment, in-place missing-track resolution, and automatic Lidarr ordering.
type Importer struct {
	store      *db.Store
	lidarrMgr  *lidarr.Manager
	httpClient *http.Client

	mu           sync.Mutex
	searchCancel context.CancelFunc
	searchStatus LidarrSearchStatus
}

func NewImporter(store *db.Store, lidarrMgr *lidarr.Manager) *Importer {
	return &Importer{
		store:     store,
		lidarrMgr: lidarrMgr,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (im *Importer) LidarrStatus() LidarrSearchStatus {
	im.mu.Lock()
	defer im.mu.Unlock()
	return im.searchStatus
}

func (im *Importer) StopLidarrSearch() {
	im.mu.Lock()
	cancel := im.searchCancel
	im.searchCancel = nil
	im.searchStatus.Running = false
	im.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// --- URL / URI Parsing ---

func isValidSpotifyID(s string) bool {
	if len(s) < 10 || len(s) > 40 {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func IsShortLink(raw string) bool {
	lower := strings.ToLower(strings.TrimSpace(raw))
	return strings.Contains(lower, "spotify.link/") || strings.Contains(lower, "spoti.fi/")
}

// ParsePlaylistID extracts a Spotify playlist ID from URLs, embed URLs, user URLs, URIs, or raw IDs.
func ParsePlaylistID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)

	// 1. spotify:playlist:<id> or spotify:user:<user>:playlist:<id>
	if strings.HasPrefix(lower, "spotify:") {
		parts := strings.Split(trimmed, ":")
		for i := 0; i < len(parts)-1; i++ {
			if strings.EqualFold(parts[i], "playlist") {
				cand := strings.TrimSpace(parts[i+1])
				if isValidSpotifyID(cand) {
					return cand
				}
			}
		}
		return ""
	}

	// 2. HTTP(S) URL
	candidateURL := trimmed
	if !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "https://") {
		if strings.Contains(lower, "spotify.com/") || strings.Contains(lower, "spotify.link/") || strings.Contains(lower, "spoti.fi/") {
			candidateURL = "https://" + trimmed
		} else if len(trimmed) >= 16 && isValidSpotifyID(trimmed) {
			return trimmed
		} else {
			return ""
		}
	}
	u, err := url.Parse(candidateURL)
	if err != nil {
		return ""
	}
	segs := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i < len(segs)-1; i++ {
		if strings.EqualFold(segs[i], "playlist") {
			cand := strings.TrimSpace(segs[i+1])
			if isValidSpotifyID(cand) {
				return cand
			}
		}
	}
	return ""
}

// FetchPlaylist fetches a public Spotify playlist via Spotify's zero-auth embed endpoint.
func (im *Importer) FetchPlaylist(ctx context.Context, rawInput string) (*PlaylistPayload, error) {
	trimmed := strings.TrimSpace(rawInput)
	if trimmed == "" {
		return nil, fmt.Errorf("enter a valid Spotify playlist link (e.g. https://open.spotify.com/playlist/...)")
	}

	playlistID := ParsePlaylistID(trimmed)
	sourceURL := trimmed

	if playlistID == "" && IsShortLink(trimmed) {
		normURL := trimmed
		if !strings.HasPrefix(strings.ToLower(normURL), "http") {
			normURL = "https://" + normURL
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, normURL, nil)
		if err == nil {
			req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15")
			if resp, err := im.httpClient.Do(req); err == nil {
				if resp.Request != nil && resp.Request.URL != nil {
					sourceURL = resp.Request.URL.String()
					playlistID = ParsePlaylistID(sourceURL)
				}
				resp.Body.Close()
			}
		}
	}

	if playlistID == "" {
		return nil, fmt.Errorf("could not find a Spotify playlist ID in that link — paste a URL like https://open.spotify.com/playlist/...")
	}

	canonicalURL := "https://open.spotify.com/playlist/" + playlistID
	if !strings.HasPrefix(strings.ToLower(sourceURL), "http") {
		sourceURL = canonicalURL
	}

	embedURL := "https://open.spotify.com/embed/playlist/" + playlistID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := im.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not reach Spotify: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("Spotify playlist not found or is private — make sure the playlist is set to Public in Spotify")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Spotify returned HTTP %d", resp.StatusCode)
	}

	rawHTML, err := io.ReadAll(io.LimitReader(resp.Body, 4_194_304))
	if err != nil {
		return nil, fmt.Errorf("failed reading Spotify response: %w", err)
	}

	return ParseEmbedHTML(string(rawHTML), playlistID, sourceURL)
}

var nextDataRe = regexp.MustCompile(`(?is)<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>`)

// ParseEmbedHTML extracts playlist metadata and tracks from Spotify embed __NEXT_DATA__ JSON.
func ParseEmbedHTML(html, playlistID, sourceURL string) (*PlaylistPayload, error) {
	m := nextDataRe.FindStringSubmatch(html)
	if len(m) < 2 {
		return nil, fmt.Errorf("Spotify playlist not found or is private — make sure the playlist is set to Public")
	}

	var root struct {
		Props struct {
			PageProps struct {
				State struct {
					Data struct {
						Entity struct {
							Name     string `json:"name"`
							Title    string `json:"title"`
							Subtitle string `json:"subtitle"`
							CoverArt struct {
								Sources []struct {
									URL string `json:"url"`
								} `json:"sources"`
							} `json:"coverArt"`
							VisualIdentity struct {
								Image []struct {
									URL string `json:"url"`
								} `json:"image"`
							} `json:"visualIdentity"`
							TrackList []struct {
								URI        string `json:"uri"`
								UID        string `json:"uid"`
								Title      string `json:"title"`
								Subtitle   string `json:"subtitle"`
								Duration   int64  `json:"duration"`
								IsExplicit bool   `json:"isExplicit"`
							} `json:"trackList"`
						} `json:"entity"`
					} `json:"data"`
				} `json:"state"`
			} `json:"pageProps"`
		} `json:"props"`
	}
	if err := json.Unmarshal([]byte(m[1]), &root); err != nil {
		return nil, fmt.Errorf("could not parse Spotify playlist JSON: %w", err)
	}

	ent := root.Props.PageProps.State.Data.Entity
	name := cleanSpotifyText(ent.Name)
	if name == "" {
		name = cleanSpotifyText(ent.Title)
	}
	if name == "" {
		name = "Spotify Playlist"
	}
	owner := cleanSpotifyText(ent.Subtitle)
	coverURL := ""
	if len(ent.CoverArt.Sources) > 0 {
		coverURL = ent.CoverArt.Sources[0].URL
	} else if len(ent.VisualIdentity.Image) > 0 {
		coverURL = ent.VisualIdentity.Image[0].URL
	}

	tracks := make([]TrackItem, 0, len(ent.TrackList))
	for idx, item := range ent.TrackList {
		title := cleanSpotifyText(item.Title)
		if title == "" {
			continue
		}
		artist := cleanSpotifyText(item.Subtitle)
		trackID := ""
		if strings.HasPrefix(item.URI, "spotify:track:") {
			trackID = strings.TrimPrefix(item.URI, "spotify:track:")
		} else if item.UID != "" {
			trackID = item.UID
		} else {
			trackID = fmt.Sprintf("sp-%s-%d", playlistID, idx)
		}
		uri := item.URI
		if uri == "" {
			uri = "spotify:track:" + trackID
		}
		tracks = append(tracks, TrackItem{
			ID:         trackID,
			URI:        uri,
			Title:      title,
			Artist:     artist,
			Duration:   float64(item.Duration) / 1000.0,
			IsExplicit: item.IsExplicit,
		})
	}
	if len(tracks) == 0 {
		return nil, fmt.Errorf("this Spotify playlist has no tracks or is private")
	}

	return &PlaylistPayload{
		PlaylistID:        playlistID,
		SourceURL:         sourceURL,
		Name:              name,
		OwnerName:         owner,
		CoverURL:          coverURL,
		Tracks:            tracks,
		HitPublicEmbedCap: len(tracks) >= 100,
	}, nil
}

func cleanSpotifyText(s string) string {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.ReplaceAll(s, "\u200b", "")
	return strings.TrimSpace(s)
}

// --- Local Library Matching & In-Place Resolution ---

var (
	spotifyDashSuffixRe  = regexp.MustCompile(`(?i)\s*-\s*(?:remaster(?:ed)?|20\d\d|19\d\d|deluxe|bonus|radio\s+edit|single\s+version|album\s+version|mono|stereo|live|acoustic|acap+el+a(?:\s+version)?|instrumental|from\s+["“].*|anniversary).*$`)
	spotifyParenSuffixRe = regexp.MustCompile(`(?i)\s*[\(\[]([^)\]]*(?:remaster|deluxe|bonus|radio\s+edit|album\s+version|single\s+version|explicit|clean|mono|stereo|anniversary|expanded|special\s+edition|feat\.?|ft\.?|featuring|from\s+["“])[^)\]]*)[\)\]]`)
	artistSplitRe        = regexp.MustCompile(`(?i)\s*(?:,|;|&|\+|\bfeat\.?\b|\bft\.?\b|\bfeaturing\b|\bwith\b|\bvs\.?\b|\bx\b)\s*`)
)

func StripSpotifyTitleSuffixes(raw string) string {
	s := cleanSpotifyText(raw)
	s = spotifyParenSuffixRe.ReplaceAllString(s, "")
	s = spotifyDashSuffixRe.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func StrictNormalize(raw string) string {
	s := norm.NFD.String(strings.ToLower(cleanSpotifyText(raw)))
	var b strings.Builder
	b.Grow(len(s))
	lastSpace := true
	for _, r := range s {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastSpace = false
		} else if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.TrimSpace(b.String())
}

func NormalizeArtistTokens(raw string) map[string]bool {
	cleaned := cleanSpotifyText(raw)
	out := make(map[string]bool)
	if cleaned == "" {
		return out
	}
	add := func(s string) {
		n := StrictNormalize(s)
		if n == "" {
			return
		}
		out[n] = true
		if strings.HasPrefix(n, "the ") && len(n) > 4 {
			out[strings.TrimPrefix(n, "the ")] = true
		}
		if strings.Contains(n, " and ") {
			out[strings.ReplaceAll(n, " and ", " ")] = true
		}
	}
	add(cleaned)
	for _, part := range artistSplitRe.Split(cleaned, -1) {
		add(part)
	}
	return out
}

type indexedTrack struct {
	track         db.TrackRecord
	strictTitle   string
	baseTitle     string
	strippedTitle string
	artistTokens  map[string]bool
	fullArtist    string
}

type libraryIndex struct {
	entries         []indexedTrack
	byBaseTitle     map[string][]int
	byStrippedTitle map[string][]int
	byArtistToken   map[string][]int
}

func buildLibraryIndex(tracks []db.TrackRecord) *libraryIndex {
	idx := &libraryIndex{
		entries:         make([]indexedTrack, 0, len(tracks)),
		byBaseTitle:     make(map[string][]int, len(tracks)),
		byStrippedTitle: make(map[string][]int, len(tracks)),
		byArtistToken:   make(map[string][]int, len(tracks)),
	}
	for i, tr := range tracks {
		strictT := StrictNormalize(tr.Title)
		baseT := lidarr.NormalizeForMatch(tr.Title)
		strippedT := lidarr.NormalizeForMatch(StripSpotifyTitleSuffixes(tr.Title))
		aTokens := NormalizeArtistTokens(tr.Artist)
		for k := range NormalizeArtistTokens(tr.AlbumArtist) {
			aTokens[k] = true
		}
		artDisplay := tr.Artist
		if strings.TrimSpace(artDisplay) == "" {
			artDisplay = tr.AlbumArtist
		}
		idx.entries = append(idx.entries, indexedTrack{
			track:         tr,
			strictTitle:   strictT,
			baseTitle:     baseT,
			strippedTitle: strippedT,
			artistTokens:  aTokens,
			fullArtist:    StrictNormalize(artDisplay),
		})
		if baseT != "" {
			idx.byBaseTitle[baseT] = append(idx.byBaseTitle[baseT], i)
		}
		if strippedT != "" && strippedT != baseT {
			idx.byStrippedTitle[strippedT] = append(idx.byStrippedTitle[strippedT], i)
		}
		for tok := range aTokens {
			idx.byArtistToken[tok] = append(idx.byArtistToken[tok], i)
		}
	}
	return idx
}

func (idx *libraryIndex) bestMatch(title, artist string, duration float64) (*db.TrackRecord, int) {
	spStrict := StrictNormalize(title)
	spBase := lidarr.NormalizeForMatch(title)
	spStripped := lidarr.NormalizeForMatch(StripSpotifyTitleSuffixes(title))
	spArtists := NormalizeArtistTokens(artist)
	spFullArtist := StrictNormalize(artist)

	if spBase == "" && spStrict == "" {
		return nil, 0
	}

	cands := make(map[int]struct{})
	for _, i := range idx.byBaseTitle[spBase] {
		cands[i] = struct{}{}
	}
	for _, i := range idx.byStrippedTitle[spBase] {
		cands[i] = struct{}{}
	}
	for _, i := range idx.byBaseTitle[spStripped] {
		cands[i] = struct{}{}
	}
	for _, i := range idx.byStrippedTitle[spStripped] {
		cands[i] = struct{}{}
	}
	for tok := range spArtists {
		for _, i := range idx.byArtistToken[tok] {
			cands[i] = struct{}{}
		}
	}

	var best *indexedTrack
	bestScore := 0

	for i := range cands {
		ent := &idx.entries[i]

		titleScore := 0
		if spStrict != "" && ent.strictTitle == spStrict {
			titleScore = 100
		} else if spBase != "" && ent.baseTitle == spBase {
			titleScore = 88
		} else if spStripped != "" && (ent.strippedTitle == spStripped || ent.baseTitle == spStripped) {
			titleScore = 76
		} else if len(spStripped) >= 5 && len(ent.strippedTitle) >= 5 {
			if strings.Contains(ent.strippedTitle, spStripped) || strings.Contains(spStripped, ent.strippedTitle) {
				minL := min(len(ent.strippedTitle), len(spStripped))
				maxL := max(len(ent.strippedTitle), len(spStripped))
				if float64(minL)/float64(maxL) >= 0.55 {
					titleScore = 58
				}
			}
		}
		if titleScore == 0 {
			continue
		}

		artistScore := 0
		if spFullArtist != "" && ent.fullArtist == spFullArtist {
			artistScore = 85
		} else {
			overlap := false
			for tok := range spArtists {
				if ent.artistTokens[tok] {
					overlap = true
					break
				}
			}
			if overlap {
				artistScore = 70
			} else {
				partial := false
				for a := range spArtists {
					if len(a) < 4 {
						continue
					}
					for b := range ent.artistTokens {
						if len(b) >= 4 && (strings.Contains(b, a) || strings.Contains(a, b)) {
							partial = true
							break
						}
					}
					if partial {
						break
					}
				}
				if partial {
					artistScore = 48
				}
			}
		}
		if artistScore == 0 {
			continue
		}

		durAdj := 0
		if duration > 10 && ent.track.Duration > 10 {
			diff := math.Abs(duration - ent.track.Duration)
			switch {
			case diff <= 5:
				durAdj = 20
			case diff <= 12:
				durAdj = 12
			case diff <= 25:
				durAdj = 4
			case diff > 90:
				durAdj = -20
			}
		}

		qualAdj := 0
		fmtLower := strings.ToLower(ent.track.Format)
		if fmtLower == "flac" || fmtLower == "alac" || fmtLower == "wav" || fmtLower == "aiff" || fmtLower == "dsf" || fmtLower == "dff" {
			qualAdj += 4
		}
		if ent.track.BitDepth != nil && *ent.track.BitDepth >= 24 {
			qualAdj += 2
		}

		total := titleScore + artistScore + durAdj + qualAdj
		if total > bestScore {
			bestScore = total
			best = ent
		}
	}

	if best != nil && bestScore >= 105 {
		t := best.track
		return &t, bestScore
	}
	return nil, 0
}

// MatchPlaylist matches a Spotify payload against local library tracks.
func MatchPlaylist(payload PlaylistPayload, tracks []db.TrackRecord) MatchReport {
	idx := buildLibraryIndex(tracks)
	entries := make([]MatchEntry, 0, len(payload.Tracks))
	matchedCount := 0
	missingCount := 0

	for i, sp := range payload.Tracks {
		if hit, score := idx.bestMatch(sp.Title, sp.Artist, sp.Duration); hit != nil {
			matchedCount++
			art := hit.Artist
			if strings.TrimSpace(art) == "" {
				art = hit.AlbumArtist
			}
			entries = append(entries, MatchEntry{
				Index:      i,
				Matched:    true,
				Confidence: score,
				Spotify:    sp,
				LocalID:    hit.EffectiveID(),
				LocalTitle: hit.Title,
				LocalArt:   art,
				LocalAlbum: hit.Album,
				Format:     strings.ToUpper(hit.Format),
				ArtHash:    hit.ArtworkHash,
				Duration:   hit.Duration,
			})
		} else {
			missingCount++
			entries = append(entries, MatchEntry{
				Index:    i,
				Matched:  false,
				Spotify:  sp,
				Duration: sp.Duration,
			})
		}
	}

	return MatchReport{
		Payload:      payload,
		Entries:      entries,
		MatchedCount: matchedCount,
		MissingCount: missingCount,
		TotalCount:   len(entries),
	}
}

// BuildSharedPlaylist builds a persisted db.SharedPlaylist preserving exact order with missing track placeholders.
func BuildSharedPlaylist(report MatchReport, customName, customDesc string) db.SharedPlaylist {
	name := strings.TrimSpace(customName)
	if name == "" {
		name = report.Payload.Name
	}
	if name == "" {
		name = "Spotify Playlist"
	}
	desc := strings.TrimSpace(customDesc)
	if desc == "" {
		if report.Payload.OwnerName != "" {
			desc = fmt.Sprintf("Imported from Spotify (by %s)", report.Payload.OwnerName)
		} else {
			desc = "Imported from Spotify"
		}
	}

	trackIDs := make([]string, 0, len(report.Entries))
	missingList := make([]db.MissingTrack, 0, report.MissingCount)
	usedMissing := make(map[string]bool)

	for _, e := range report.Entries {
		if e.Matched && e.LocalID != "" {
			trackIDs = append(trackIDs, e.LocalID)
			continue
		}
		mid := db.MissingTrackIDPrefix + "spotify:" + e.Spotify.ID
		if usedMissing[mid] {
			mid = fmt.Sprintf("%sspotify:%s:%d", db.MissingTrackIDPrefix, e.Spotify.ID, len(trackIDs))
		}
		usedMissing[mid] = true
		trackIDs = append(trackIDs, mid)
		missingList = append(missingList, db.MissingTrack{
			ID:             mid,
			Title:          e.Spotify.Title,
			Artist:         e.Spotify.Artist,
			Album:          e.Spotify.Album,
			Duration:       e.Spotify.Duration,
			SpotifyTrackID: e.Spotify.ID,
			SpotifyURI:     e.Spotify.URI,
			ArtworkURL:     e.Spotify.ArtworkURL,
			LidarrStatus:   "missing",
		})
	}

	now := time.Now().UTC()
	return db.SharedPlaylist{
		ID:                uuid.NewString(),
		Name:              name,
		Description:       desc,
		TrackIDs:          trackIDs,
		MissingTracks:     missingList,
		SpotifyPlaylistID: report.Payload.PlaylistID,
		SpotifySourceURL:  report.Payload.SourceURL,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

// ResolveMissingInPlaylist upgrades any newly available local tracks in-place (preserving exact slot order).
func ResolveMissingInPlaylist(pl db.SharedPlaylist, tracks []db.TrackRecord) (db.SharedPlaylist, int) {
	if len(pl.MissingTracks) == 0 {
		return pl, 0
	}
	idx := buildLibraryIndex(tracks)
	missingByID := make(map[string]db.MissingTrack, len(pl.MissingTracks))
	for _, m := range pl.MissingTracks {
		missingByID[m.ID] = m
	}

	resolved := make(map[string]bool)
	newTrackIDs := make([]string, 0, len(pl.TrackIDs))
	for _, tid := range pl.TrackIDs {
		if db.IsMissingTrackID(tid) {
			if m, ok := missingByID[tid]; ok {
				if hit, _ := idx.bestMatch(m.Title, m.Artist, m.Duration); hit != nil {
					newTrackIDs = append(newTrackIDs, hit.EffectiveID())
					resolved[tid] = true
					continue
				}
			}
		}
		newTrackIDs = append(newTrackIDs, tid)
	}
	if len(resolved) == 0 {
		return pl, 0
	}

	remaining := make([]db.MissingTrack, 0, len(pl.MissingTracks)-len(resolved))
	for _, m := range pl.MissingTracks {
		if !resolved[m.ID] {
			remaining = append(remaining, m)
		}
	}
	pl.TrackIDs = newTrackIDs
	pl.MissingTracks = remaining
	pl.UpdatedAt = time.Now().UTC()
	return pl, len(resolved)
}

// ResolveAllPlaylists scans all stored playlists and resolves any retained missing tracks against the current library.
func (im *Importer) ResolveAllPlaylists() int {
	pls, err := im.store.ListPlaylists(false)
	if err != nil {
		return 0
	}
	hasMissing := false
	for _, p := range pls {
		if len(p.MissingTracks) > 0 {
			hasMissing = true
			break
		}
	}
	if !hasMissing {
		return 0
	}
	tracks, err := im.store.LoadAllTracks()
	if err != nil || len(tracks) == 0 {
		return 0
	}
	total := 0
	for _, p := range pls {
		if len(p.MissingTracks) == 0 {
			continue
		}
		updated, n := ResolveMissingInPlaylist(p, tracks)
		if n > 0 {
			if err := im.store.SavePlaylist(updated); err == nil {
				total += n
				log.Printf("[spotify] auto-resolved %d missing track(s) in playlist '%s'", n, updated.Name)
			}
		}
	}
	return total
}

// --- Artwork & Album Enrichment ---

func (im *Importer) FetchTrackThumbnailURL(ctx context.Context, spotifyTrackID string) string {
	if strings.TrimSpace(spotifyTrackID) == "" {
		return ""
	}
	u := "https://open.spotify.com/oembed?url=" + url.QueryEscape("https://open.spotify.com/track/"+spotifyTrackID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "MyEchoServer/1.0")
	resp, err := im.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ""
	}
	var obj struct {
		ThumbnailURL string `json:"thumbnail_url"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&obj) == nil {
		return strings.TrimSpace(obj.ThumbnailURL)
	}
	return ""
}

// EnrichPlaylistArtworkAsync populates Spotify oEmbed thumbnails for missing tracks in the background.
func (im *Importer) EnrichPlaylistArtworkAsync(playlistID string) {
	go func() {
		pl, err := im.store.GetPlaylist(playlistID)
		if err != nil || pl == nil || len(pl.MissingTracks) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		var targets []db.MissingTrack
		for _, m := range pl.MissingTracks {
			if m.ArtworkURL == "" && m.SpotifyTrackID != "" {
				targets = append(targets, m)
			}
		}
		if len(targets) == 0 {
			return
		}

		resolved := make(map[string]string)
		var mu sync.Mutex
		sem := make(chan struct{}, 4)
		var wg sync.WaitGroup
		for _, item := range targets {
			wg.Add(1)
			sem <- struct{}{}
			go func(m db.MissingTrack) {
				defer wg.Done()
				defer func() { <-sem }()
				if thumb := im.FetchTrackThumbnailURL(ctx, m.SpotifyTrackID); thumb != "" {
					mu.Lock()
					resolved[m.ID] = thumb
					mu.Unlock()
				}
			}(item)
		}
		wg.Wait()
		if len(resolved) == 0 {
			return
		}

		fresh, err := im.store.GetPlaylist(playlistID)
		if err != nil || fresh == nil {
			return
		}
		changed := false
		for i := range fresh.MissingTracks {
			if fresh.MissingTracks[i].ArtworkURL == "" {
				if u, ok := resolved[fresh.MissingTracks[i].ID]; ok {
					fresh.MissingTracks[i].ArtworkURL = u
					changed = true
				}
			}
		}
		if changed {
			_ = im.store.SavePlaylist(*fresh)
		}
	}()
}

type resolvedAlbumInfo struct {
	AlbumTitle  string
	ArtistName  string
	ArtworkURL  string
	MBReleaseID string
}

func (im *Importer) resolveAlbumForMissingTrack(ctx context.Context, title, artist string) *resolvedAlbumInfo {
	primaryArtist := strings.TrimSpace(artist)
	if parts := artistSplitRe.Split(primaryArtist, -1); len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
		primaryArtist = strings.TrimSpace(parts[0])
	}
	cleanTitle := StripSpotifyTitleSuffixes(title)
	normTargetTitle := lidarr.NormalizeForMatch(cleanTitle)
	normTargetArtists := NormalizeArtistTokens(artist)

	type dzArtist struct {
		Name string `json:"name"`
	}
	type dzAlbum struct {
		Title       string `json:"title"`
		CoverMedium string `json:"cover_medium"`
		CoverBig    string `json:"cover_big"`
	}
	type dzTrack struct {
		Title      string   `json:"title"`
		TitleShort string   `json:"title_short"`
		Artist     dzArtist `json:"artist"`
		Album      *dzAlbum `json:"album"`
	}
	type dzResp struct {
		Data []dzTrack `json:"data"`
	}

	ca := strings.ReplaceAll(primaryArtist, `"`, "")
	ct := strings.ReplaceAll(cleanTitle, `"`, "")
	queries := []string{
		fmt.Sprintf(`artist:"%s" track:"%s"`, ca, ct),
		fmt.Sprintf(`%s %s`, ca, ct),
	}

	for _, q := range queries {
		u := "https://api.deezer.com/search/track?q=" + url.QueryEscape(q) + "&limit=10"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", "MyEchoServer/1.0")
		resp, err := im.httpClient.Do(req)
		if err != nil {
			continue
		}
		var decoded dzResp
		_ = json.NewDecoder(io.LimitReader(resp.Body, 524_288)).Decode(&decoded)
		resp.Body.Close()

		for _, item := range decoded.Data {
			if item.Album == nil || strings.TrimSpace(item.Album.Title) == "" {
				continue
			}
			tShort := lidarr.NormalizeForMatch(item.TitleShort)
			tFull := lidarr.NormalizeForMatch(item.Title)
			titleOK := tShort == normTargetTitle || tFull == normTargetTitle ||
				(len(normTargetTitle) >= 4 && (strings.Contains(tFull, normTargetTitle) || strings.Contains(normTargetTitle, tFull)))
			if !titleOK {
				continue
			}
			itemArtists := NormalizeArtistTokens(item.Artist.Name)
			artistOK := false
			for a := range normTargetArtists {
				if itemArtists[a] {
					artistOK = true
					break
				}
				for b := range itemArtists {
					if len(a) >= 4 && len(b) >= 4 && (strings.Contains(b, a) || strings.Contains(a, b)) {
						artistOK = true
						break
					}
				}
				if artistOK {
					break
				}
			}
			if !artistOK {
				continue
			}
			cover := item.Album.CoverMedium
			if cover == "" {
				cover = item.Album.CoverBig
			}
			return &resolvedAlbumInfo{
				AlbumTitle: item.Album.Title,
				ArtistName: item.Artist.Name,
				ArtworkURL: cover,
			}
		}
	}

	// Fallback: MusicBrainz recording search
	mbQ := fmt.Sprintf(`recording:"%s" AND artist:"%s"`, ct, ca)
	mbURL := "https://musicbrainz.org/ws/2/recording?query=" + url.QueryEscape(mbQ) + "&fmt=json&limit=5"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mbURL, nil)
	if err == nil {
		req.Header.Set("User-Agent", "MyEchoServer/1.4 (https://github.com/baardove/my-echo-server)")
		req.Header.Set("Accept", "application/json")
		if resp, err := im.httpClient.Do(req); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				var mb struct {
					Recordings []struct {
						Releases []struct {
							Title        string `json:"title"`
							ReleaseGroup struct {
								ID          string `json:"id"`
								Title       string `json:"title"`
								PrimaryType string `json:"primary-type"`
							} `json:"release-group"`
						} `json:"releases"`
					} `json:"recordings"`
				}
				if json.NewDecoder(io.LimitReader(resp.Body, 524_288)).Decode(&mb) == nil && len(mb.Recordings) > 0 {
					rels := mb.Recordings[0].Releases
					if len(rels) > 0 {
						pick := rels[0]
						for _, r := range rels {
							if strings.EqualFold(r.ReleaseGroup.PrimaryType, "album") {
								pick = r
								break
							}
						}
						albTitle := pick.ReleaseGroup.Title
						if albTitle == "" {
							albTitle = pick.Title
						}
						artURL := ""
						if pick.ReleaseGroup.ID != "" {
							artURL = "https://coverartarchive.org/release-group/" + pick.ReleaseGroup.ID + "/front-250"
						}
						return &resolvedAlbumInfo{
							AlbumTitle:  albTitle,
							ArtistName:  primaryArtist,
							ArtworkURL:  artURL,
							MBReleaseID: pick.ReleaseGroup.ID,
						}
					}
				}
			}
		}
	}
	return nil
}

// --- Automatic Lidarr Search & Ordering for Missing Tracks ---

// StartAutoLidarrSearch runs an async job to resolve parent albums and order missing tracks in Lidarr.
func (im *Importer) StartAutoLidarrSearch(playlistID string, missingTrackID string, onlyUnsearched bool) (LidarrSearchStatus, error) {
	if im.lidarrMgr == nil || !im.lidarrMgr.IsConfigured() {
		return LidarrSearchStatus{}, fmt.Errorf("Lidarr is not configured — add Lidarr URL and API Key in Settings")
	}
	pl, err := im.store.GetPlaylist(playlistID)
	if err != nil || pl == nil {
		return LidarrSearchStatus{}, fmt.Errorf("playlist not found")
	}

	var targets []db.MissingTrack
	activeIDs := make(map[string]bool, len(pl.TrackIDs))
	for _, tid := range pl.TrackIDs {
		if db.IsMissingTrackID(tid) {
			activeIDs[tid] = true
		}
	}
	for _, m := range pl.MissingTracks {
		if !activeIDs[m.ID] {
			continue
		}
		if missingTrackID != "" {
			if m.ID == missingTrackID {
				targets = append(targets, m)
			}
			continue
		}
		if onlyUnsearched && m.LidarrStatus == "ordered" {
			continue
		}
		targets = append(targets, m)
	}
	if len(targets) == 0 {
		return im.LidarrStatus(), fmt.Errorf("no matching missing tracks to search in Lidarr")
	}

	im.StopLidarrSearch()
	ctx, cancel := context.WithCancel(context.Background())

	im.mu.Lock()
	im.searchCancel = cancel
	im.searchStatus = LidarrSearchStatus{
		Running:      true,
		PlaylistID:   playlistID,
		Done:         0,
		Total:        len(targets),
		CurrentTrack: targets[0].Artist + " — " + targets[0].Title,
	}
	st := im.searchStatus
	im.mu.Unlock()

	go im.runLidarrSearchJob(ctx, playlistID, targets)
	return st, nil
}

func (im *Importer) runLidarrSearchJob(ctx context.Context, playlistID string, targets []db.MissingTrack) {
	defer func() {
		im.mu.Lock()
		im.searchCancel = nil
		im.searchStatus.Running = false
		im.mu.Unlock()
	}()

	orderedCache := make(map[string]lidarr.OrderResult)

	for i, item := range targets {
		if ctx.Err() != nil {
			break
		}
		label := strings.TrimSpace(item.Artist + " — " + item.Title)
		im.mu.Lock()
		im.searchStatus.Done = i
		im.searchStatus.CurrentTrack = label
		im.mu.Unlock()

		im.updateMissingTrackInDB(playlistID, item.ID, func(m *db.MissingTrack) {
			m.LidarrStatus = "searching"
			m.LidarrStatusDetail = "Resolving album & searching Lidarr..."
		})

		im.processOneMissingTrack(ctx, playlistID, item, orderedCache)

		im.mu.Lock()
		im.searchStatus.Done = i + 1
		im.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(350 * time.Millisecond):
		}
	}

	im.mu.Lock()
	st := im.searchStatus
	var parts []string
	if st.TracksCovered > 0 {
		parts = append(parts, fmt.Sprintf("ordered %d album(s) covering %d track(s)", st.AlbumsOrdered, st.TracksCovered))
	}
	if st.NotFound > 0 {
		parts = append(parts, fmt.Sprintf("%d not found", st.NotFound))
	}
	if st.Failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", st.Failed))
	}
	if len(parts) == 0 {
		im.searchStatus.LastSummary = "Lidarr search complete"
	} else {
		im.searchStatus.LastSummary = "Lidarr auto-search: " + strings.Join(parts, " · ")
	}
	im.mu.Unlock()
}

func (im *Importer) processOneMissingTrack(
	ctx context.Context,
	playlistID string,
	item db.MissingTrack,
	orderedCache map[string]lidarr.OrderResult,
) {
	primaryArtist := strings.TrimSpace(item.Artist)
	if parts := artistSplitRe.Split(primaryArtist, -1); len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
		primaryArtist = strings.TrimSpace(parts[0])
	}
	albumTitle := strings.TrimSpace(item.Album)
	resolvedArtist := primaryArtist
	artworkURL := item.ArtworkURL
	mbID := ""

	if albumTitle == "" {
		if info := im.resolveAlbumForMissingTrack(ctx, item.Title, item.Artist); info != nil {
			albumTitle = strings.TrimSpace(info.AlbumTitle)
			if info.ArtistName != "" {
				resolvedArtist = info.ArtistName
			}
			if artworkURL == "" && info.ArtworkURL != "" {
				artworkURL = info.ArtworkURL
			}
			mbID = info.MBReleaseID
		}
	}

	if albumTitle != "" {
		cacheKey := lidarr.NormalizeForMatch(resolvedArtist) + "::" + lidarr.NormalizeForMatch(albumTitle)
		if cached, ok := orderedCache[cacheKey]; ok {
			im.mu.Lock()
			im.searchStatus.TracksCovered++
			im.mu.Unlock()
			im.updateMissingTrackInDB(playlistID, item.ID, func(m *db.MissingTrack) {
				m.Album = albumTitle
				if m.ArtworkURL == "" && artworkURL != "" {
					m.ArtworkURL = artworkURL
				}
				m.LidarrStatus = "ordered"
				m.LidarrAlbumTitle = cached.AlbumTitle
				m.LidarrAlbumID = cached.LidarrAlbumID
				m.LidarrStatusDetail = fmt.Sprintf("Ordered in Lidarr (%s)", cached.AlbumTitle)
			})
			return
		}
	}

	// Build Lidarr OrderAlbumRequest (uses MBID if found, otherwise artist + album, or artist + track title for singles)
	orderReq := lidarr.OrderAlbumRequest{
		ForeignAlbumID: mbID,
		ArtistName:     resolvedArtist,
		Title:          albumTitle,
	}
	if orderReq.Title == "" {
		orderReq.Title = StripSpotifyTitleSuffixes(item.Title)
	}

	res, err := im.lidarrMgr.OrderAlbum(ctx, orderReq)
	if err != nil {
		errMsg := err.Error()
		isNotFound := strings.Contains(strings.ToLower(errMsg), "could not find") ||
			strings.Contains(strings.ToLower(errMsg), "not found")
		im.mu.Lock()
		if isNotFound {
			im.searchStatus.NotFound++
		} else {
			im.searchStatus.Failed++
		}
		im.mu.Unlock()
		im.updateMissingTrackInDB(playlistID, item.ID, func(m *db.MissingTrack) {
			if albumTitle != "" {
				m.Album = albumTitle
			}
			if m.ArtworkURL == "" && artworkURL != "" {
				m.ArtworkURL = artworkURL
			}
			if isNotFound {
				m.LidarrStatus = "notFound"
			} else {
				m.LidarrStatus = "failed"
			}
			m.LidarrStatusDetail = errMsg
		})
		return
	}

	// Cache ordered album so multiple missing tracks on the same album don't re-order it
	resolvedKey := lidarr.NormalizeForMatch(res.ArtistName) + "::" + lidarr.NormalizeForMatch(res.AlbumTitle)
	orderedCache[resolvedKey] = res
	if albumTitle != "" {
		altKey := lidarr.NormalizeForMatch(resolvedArtist) + "::" + lidarr.NormalizeForMatch(albumTitle)
		orderedCache[altKey] = res
	}

	im.mu.Lock()
	im.searchStatus.AlbumsOrdered++
	im.searchStatus.TracksCovered++
	im.mu.Unlock()

	im.updateMissingTrackInDB(playlistID, item.ID, func(m *db.MissingTrack) {
		if albumTitle != "" {
			m.Album = albumTitle
		} else if res.AlbumTitle != "" {
			m.Album = res.AlbumTitle
		}
		if m.ArtworkURL == "" && artworkURL != "" {
			m.ArtworkURL = artworkURL
		}
		m.LidarrStatus = "ordered"
		m.LidarrAlbumTitle = res.AlbumTitle
		m.LidarrAlbumID = res.LidarrAlbumID
		if res.AlreadyExisted {
			m.LidarrStatusDetail = fmt.Sprintf("Triggered Lidarr search (%s)", res.AlbumTitle)
		} else {
			m.LidarrStatusDetail = fmt.Sprintf("Ordered in Lidarr (%s)", res.AlbumTitle)
		}
	})
}

func (im *Importer) updateMissingTrackInDB(playlistID, missingID string, mutate func(*db.MissingTrack)) {
	pl, err := im.store.GetPlaylist(playlistID)
	if err != nil || pl == nil {
		return
	}
	now := time.Now().UTC()
	for i := range pl.MissingTracks {
		if pl.MissingTracks[i].ID == missingID {
			mutate(&pl.MissingTracks[i])
			pl.MissingTracks[i].LastSearchedAt = &now
			pl.UpdatedAt = now
			_ = im.store.SavePlaylist(*pl)
			return
		}
	}
}
