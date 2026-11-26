package lidarr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/google/uuid"
	"golang.org/x/text/unicode/norm"
)

// AlbumSearchResult mirrors the Mac app's LidarrAlbumSearchResult for web search & ordering.
type AlbumSearchResult struct {
	LidarrID        int             `json:"lidarrId,omitempty"`
	ForeignAlbumID  string          `json:"foreignAlbumId"`
	Title           string          `json:"title"`
	ArtistName      string          `json:"artistName"`
	ForeignArtistID string          `json:"foreignArtistId,omitempty"`
	ReleaseYear     int             `json:"releaseYear,omitempty"`
	ReleaseDate     string          `json:"releaseDate,omitempty"`
	AlbumType       string          `json:"albumType,omitempty"`
	CoverURL        string          `json:"coverURL,omitempty"`
	Monitored       bool            `json:"monitored"`
	TrackFileCount  int             `json:"trackFileCount"`
	TrackCount      int             `json:"trackCount"`
	PercentOfTracks float64         `json:"percentOfTracks"`
	IsInLidarr      bool            `json:"isInLidarr"`
	IsDownloaded    bool            `json:"isDownloaded"`
	InEchoLibrary   bool            `json:"inEchoLibrary"`
	RawLookup       json.RawMessage `json:"rawLookup,omitempty"`
}

// ArtistSearchResult mirrors the Mac app's LidarrArtistSearchResult.
type ArtistSearchResult struct {
	LidarrID        int    `json:"lidarrId,omitempty"`
	ForeignArtistID string `json:"foreignArtistId"`
	ArtistName      string `json:"artistName"`
	Overview        string `json:"overview,omitempty"`
	CoverURL        string `json:"coverURL,omitempty"`
	Monitored       bool   `json:"monitored"`
	AlbumCount      int    `json:"albumCount"`
	TrackFileCount  int    `json:"trackFileCount"`
}

// TrackMatch mirrors the Mac app's LidarrTrackMatch (via Deezer public catalog)
// so users can search tracks by an artist and order the parent album in Lidarr in one click.
type TrackMatch struct {
	ID            string `json:"id"`
	Title         string `json:"title"`
	ArtistName    string `json:"artistName"`
	AlbumTitle    string `json:"albumTitle"`
	Duration      int    `json:"duration"`
	CoverURL      string `json:"coverURL,omitempty"`
	Popularity    int    `json:"popularity,omitempty"`
	InEchoLibrary bool   `json:"inEchoLibrary"`
}

// OrderAlbumRequest carries either a Lidarr search result or a track's parent album info.
type OrderAlbumRequest struct {
	LidarrID        int             `json:"lidarrId,omitempty"`
	ForeignAlbumID  string          `json:"foreignAlbumId,omitempty"`
	Title           string          `json:"title"`
	ArtistName      string          `json:"artistName"`
	ForeignArtistID string          `json:"foreignArtistId,omitempty"`
	RawLookup       json.RawMessage `json:"rawLookup,omitempty"`
}

// OrderResult summarizes the outcome of ordering an album in Lidarr.
type OrderResult struct {
	AlbumTitle      string `json:"albumTitle"`
	ArtistName      string `json:"artistName"`
	LidarrAlbumID   int    `json:"lidarrAlbumId,omitempty"`
	AlreadyExisted  bool   `json:"alreadyExisted"`
	SearchTriggered bool   `json:"searchTriggered"`
	Message         string `json:"message"`
}

// QueueItem represents an active item in Lidarr's download queue.
type QueueItem struct {
	ID             int      `json:"id"`
	AlbumID        int      `json:"albumId,omitempty"`
	ArtistID       int      `json:"artistId,omitempty"`
	Title          string   `json:"title"`
	ArtistName     string   `json:"artistName,omitempty"`
	AlbumTitle     string   `json:"albumTitle,omitempty"`
	Status         string   `json:"status"`
	Phase          string   `json:"phase"`
	Progress       float64  `json:"progress"`
	TimeLeft       string   `json:"timeleft,omitempty"`
	QualityName    string   `json:"qualityName,omitempty"`
	DownloadClient string   `json:"downloadClient,omitempty"`
	StatusMessages []string `json:"statusMessages,omitempty"`
	ErrorMessage   string   `json:"errorMessage,omitempty"`
}

// IsConfigured reports whether Lidarr URL and API key are configured.
func (m *Manager) IsConfigured() bool {
	return strings.TrimSpace(m.cfg.LidarrURL) != "" && strings.TrimSpace(m.cfg.LidarrAPIKey) != ""
}

// SearchAlbums searches Lidarr for albums matching term (album title OR artist name),
// expanding matching artist discographies via Lidarr DB or MusicBrainz + Deezer just like the Mac app.
func (m *Manager) SearchAlbums(ctx context.Context, term string) ([]AlbumSearchResult, error) {
	trimmed := strings.TrimSpace(term)
	if trimmed == "" {
		return []AlbumSearchResult{}, nil
	}
	if !m.IsConfigured() {
		return nil, fmt.Errorf("Lidarr is not configured — add Lidarr URL and API Key in Settings")
	}

	var (
		directAlbums []AlbumSearchResult
		directErr    error
		artists      []ArtistSearchResult
		wg           sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		directAlbums, directErr = m.lookupAlbumsRaw(ctx, trimmed)
	}()
	go func() {
		defer wg.Done()
		artists, _ = m.searchArtists(ctx, trimmed)
	}()
	wg.Wait()

	if directErr != nil && len(artists) == 0 {
		return nil, directErr
	}

	strongArtists := SelectMatchingArtists(artists, trimmed)
	matchedArtistNames := make([]string, 0, len(strongArtists))
	for _, a := range strongArtists {
		matchedArtistNames = append(matchedArtistNames, a.ArtistName)
	}

	var artistDiscography []AlbumSearchResult
	if len(strongArtists) > 0 {
		limit := len(strongArtists)
		if limit > 2 {
			limit = 2
		}
		for _, artist := range strongArtists[:limit] {
			if artist.LidarrID > 0 {
				if localAlbs, err := m.fetchAlbumsForLidarrArtist(ctx, artist.LidarrID, artist.ArtistName); err == nil && len(localAlbs) > 0 {
					artistDiscography = append(artistDiscography, localAlbs...)
					continue
				}
			}
			remoteAlbs := m.fetchRemoteDiscographyAlbums(ctx, artist)
			artistDiscography = append(artistDiscography, remoteAlbs...)
		}
	} else if len(directAlbums) < 5 {
		fallback := m.fetchDeezerArtistDiscography(ctx, trimmed)
		artistDiscography = append(artistDiscography, fallback...)
	}

	return MergeAndRankAlbumResults(trimmed, directAlbums, artistDiscography, matchedArtistNames), nil
}

// SearchTracks resolves track, artist, or album queries into matching tracks via Deezer public catalog,
// so users can search by artist or song and order the parent album in Lidarr.
func (m *Manager) SearchTracks(ctx context.Context, term string) ([]TrackMatch, error) {
	trimmed := strings.TrimSpace(term)
	if trimmed == "" {
		return []TrackMatch{}, nil
	}

	type dzArtist struct {
		Name string `json:"name"`
	}
	type dzAlbum struct {
		Title       string `json:"title"`
		CoverMedium string `json:"cover_medium"`
		CoverBig    string `json:"cover_big"`
	}
	type dzTrack struct {
		ID       int      `json:"id"`
		Title    string   `json:"title"`
		Duration int      `json:"duration"`
		Rank     int      `json:"rank"`
		Artist   dzArtist `json:"artist"`
		Album    *dzAlbum `json:"album"`
	}
	type dzTrackResp struct {
		Data []dzTrack `json:"data"`
	}
	type dzAlbumHit struct {
		ID          int      `json:"id"`
		Title       string   `json:"title"`
		CoverMedium string   `json:"cover_medium"`
		CoverBig    string   `json:"cover_big"`
		Artist      dzArtist `json:"artist"`
	}
	type dzAlbumSearchResp struct {
		Data []dzAlbumHit `json:"data"`
	}

	mapTrack := func(item dzTrack, fallbackAlbum, fallbackCover string) TrackMatch {
		albTitle := fallbackAlbum
		cover := fallbackCover
		if item.Album != nil {
			if strings.TrimSpace(item.Album.Title) != "" {
				albTitle = item.Album.Title
			}
			if item.Album.CoverBig != "" {
				cover = item.Album.CoverBig
			} else if item.Album.CoverMedium != "" {
				cover = item.Album.CoverMedium
			}
		}
		return TrackMatch{
			ID:         strconv.Itoa(item.ID),
			Title:      item.Title,
			ArtistName: item.Artist.Name,
			AlbumTitle: albTitle,
			Duration:   item.Duration,
			CoverURL:   cover,
			Popularity: scoreFromDeezerRank(item.Rank),
		}
	}

	var (
		directTracks      []TrackMatch
		artistTopTracks   []TrackMatch
		matchedArtistName string
		albumTracks       []TrackMatch
		directErr         error
		wg                sync.WaitGroup
	)
	encoded := url.QueryEscape(trimmed)

	wg.Add(3)
	go func() {
		defer wg.Done()
		var resp dzTrackResp
		if err := m.externalGetJSON(ctx, "https://api.deezer.com/search/track?q="+encoded+"&limit=30", "MyEchoServer/1.0", &resp); err != nil {
			directErr = err
			return
		}
		for _, item := range resp.Data {
			directTracks = append(directTracks, mapTrack(item, "", ""))
		}
	}()
	go func() {
		defer wg.Done()
		art := m.findMatchingDeezerArtist(ctx, trimmed)
		if art == nil {
			return
		}
		matchedArtistName = art.Name
		var resp dzTrackResp
		u := fmt.Sprintf("https://api.deezer.com/artist/%d/top?limit=30", art.ID)
		if err := m.externalGetJSON(ctx, u, "MyEchoServer/1.0", &resp); err != nil {
			return
		}
		for _, item := range resp.Data {
			artistTopTracks = append(artistTopTracks, mapTrack(item, "", ""))
		}
	}()
	go func() {
		defer wg.Done()
		var sResp dzAlbumSearchResp
		if err := m.externalGetJSON(ctx, "https://api.deezer.com/search/album?q="+encoded+"&limit=3", "MyEchoServer/1.0", &sResp); err != nil {
			return
		}
		normQ := NormalizeForMatch(trimmed)
		var hit *dzAlbumHit
		for i := range sResp.Data {
			nt := NormalizeForMatch(sResp.Data[i].Title)
			if nt == normQ || strings.HasPrefix(nt, normQ) {
				hit = &sResp.Data[i]
				break
			}
		}
		if hit == nil {
			return
		}
		var tResp dzTrackResp
		u := fmt.Sprintf("https://api.deezer.com/album/%d/tracks?limit=25", hit.ID)
		if err := m.externalGetJSON(ctx, u, "MyEchoServer/1.0", &tResp); err != nil {
			return
		}
		fallbackCover := hit.CoverBig
		if fallbackCover == "" {
			fallbackCover = hit.CoverMedium
		}
		for _, item := range tResp.Data {
			albumTracks = append(albumTracks, mapTrack(item, hit.Title, fallbackCover))
		}
	}()
	wg.Wait()

	if directErr != nil && len(artistTopTracks) == 0 && len(albumTracks) == 0 {
		return nil, directErr
	}

	return MergeAndRankTrackResults(trimmed, directTracks, artistTopTracks, albumTracks, matchedArtistName), nil
}

// OrderAlbum adds/monitors an album in Lidarr and triggers an immediate AlbumSearch.
func (m *Manager) OrderAlbum(ctx context.Context, req OrderAlbumRequest) (OrderResult, error) {
	if !m.IsConfigured() {
		return OrderResult{}, fmt.Errorf("Lidarr is not configured — add Lidarr URL and API Key in Settings")
	}
	req.Title = strings.TrimSpace(req.Title)
	req.ArtistName = strings.TrimSpace(req.ArtistName)
	req.ForeignAlbumID = strings.TrimSpace(req.ForeignAlbumID)

	resolved := AlbumSearchResult{
		LidarrID:        req.LidarrID,
		ForeignAlbumID:  req.ForeignAlbumID,
		Title:           req.Title,
		ArtistName:      req.ArtistName,
		ForeignArtistID: req.ForeignArtistID,
		RawLookup:       req.RawLookup,
	}

	// 0. If this album came from discography expansion or a Deezer track match without Lidarr lookup JSON,
	// resolve the full Lidarr lookup object on-demand before adding.
	if resolved.LidarrID == 0 && len(resolved.RawLookup) == 0 {
		isMBID := resolved.ForeignAlbumID != "" && !strings.HasPrefix(resolved.ForeignAlbumID, "deezer-album:") && isValidUUID(resolved.ForeignAlbumID)
		if isMBID {
			if byID, err := m.lookupAlbumsRaw(ctx, "lidarr:"+resolved.ForeignAlbumID); err == nil && len(byID) > 0 {
				pick := byID[0]
				for _, c := range byID {
					if strings.EqualFold(c.ForeignAlbumID, resolved.ForeignAlbumID) {
						pick = c
						break
					}
				}
				resolved = pick
			}
		}
		if resolved.LidarrID == 0 && len(resolved.RawLookup) == 0 {
			query := strings.TrimSpace(resolved.ArtistName + " " + resolved.Title)
			if query == "" {
				return OrderResult{}, fmt.Errorf("missing album title or artist name")
			}
			candidates, err := m.lookupAlbumsRaw(ctx, query)
			if err != nil {
				return OrderResult{}, fmt.Errorf("Lidarr album lookup failed: %w", err)
			}
			if len(candidates) == 0 && resolved.Title != "" {
				candidates, _ = m.lookupAlbumsRaw(ctx, resolved.Title)
			}
			if len(candidates) == 0 {
				return OrderResult{}, fmt.Errorf("could not find '%s' in Lidarr metadata", query)
			}
			targetTitle := NormalizeForMatch(resolved.Title)
			targetArtist := NormalizeForMatch(resolved.ArtistName)
			pick := candidates[0]
			for _, c := range candidates {
				ct := NormalizeForMatch(c.Title)
				ca := NormalizeForMatch(c.ArtistName)
				tOK := targetTitle == "" || ct == targetTitle || strings.Contains(ct, targetTitle) || strings.Contains(targetTitle, ct)
				aOK := targetArtist == "" || strings.Contains(ca, targetArtist) || strings.Contains(targetArtist, ca)
				if tOK && aOK {
					pick = c
					break
				}
			}
			resolved = pick
		}
	}

	// 1. If already tracked in Lidarr, monitor & trigger AlbumSearch directly
	if resolved.LidarrID > 0 {
		if err := m.monitorAlbum(ctx, resolved.LidarrID, true); err != nil {
			return OrderResult{}, err
		}
		if err := m.triggerAlbumSearch(ctx, resolved.LidarrID); err != nil {
			return OrderResult{}, err
		}
		return OrderResult{
			AlbumTitle:      resolved.Title,
			ArtistName:      resolved.ArtistName,
			LidarrAlbumID:   resolved.LidarrID,
			AlreadyExisted:  true,
			SearchTriggered: true,
			Message:         fmt.Sprintf("Monitored & triggered Lidarr search for '%s'", resolved.Title),
		}, nil
	}

	// 2. Check if foreignAlbumId is already in Lidarr
	if resolved.ForeignAlbumID != "" && !strings.HasPrefix(resolved.ForeignAlbumID, "deezer-album:") {
		var existing []struct {
			ID int `json:"id"`
		}
		err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/album?foreignAlbumId="+url.QueryEscape(resolved.ForeignAlbumID), nil, &existing)
		if err == nil && len(existing) > 0 && existing[0].ID > 0 {
			id := existing[0].ID
			if err := m.monitorAlbum(ctx, id, true); err != nil {
				return OrderResult{}, err
			}
			if err := m.triggerAlbumSearch(ctx, id); err != nil {
				return OrderResult{}, err
			}
			return OrderResult{
				AlbumTitle:      resolved.Title,
				ArtistName:      resolved.ArtistName,
				LidarrAlbumID:   id,
				AlreadyExisted:  true,
				SearchTriggered: true,
				Message:         fmt.Sprintf("Monitored & triggered Lidarr search for '%s'", resolved.Title),
			}, nil
		}
	}

	// 3. Resolve root folder, quality profile, and metadata profile from Lidarr
	rootFolder, qualityID, metadataID, err := m.resolveLidarrProfiles(ctx)
	if err != nil {
		return OrderResult{}, err
	}

	// 4. Build add payload preserving all raw lookup fields & guarding nested artist options
	var obj map[string]any
	if err := json.Unmarshal(resolved.RawLookup, &obj); err != nil {
		return OrderResult{}, fmt.Errorf("invalid Lidarr album lookup payload: %w", err)
	}
	obj["monitored"] = true
	obj["addOptions"] = map[string]any{
		"searchForNewAlbum": true,
	}
	artistObj, _ := obj["artist"].(map[string]any)
	if artistObj == nil {
		artistObj = map[string]any{}
	}
	artistObj["qualityProfileId"] = qualityID
	artistObj["metadataProfileId"] = metadataID
	artistObj["rootFolderPath"] = rootFolder
	artistObj["monitored"] = true
	artistObj["addOptions"] = map[string]any{
		"monitor":                "none",
		"searchForMissingAlbums": false,
	}
	obj["artist"] = artistObj

	var created struct {
		ID int `json:"id"`
	}
	if err := m.lidarrReq(ctx, http.MethodPost, "/api/v1/album", obj, &created); err != nil {
		return OrderResult{}, fmt.Errorf("add album to Lidarr: %w", err)
	}

	return OrderResult{
		AlbumTitle:      resolved.Title,
		ArtistName:      resolved.ArtistName,
		LidarrAlbumID:   created.ID,
		AlreadyExisted:  false,
		SearchTriggered: true,
		Message:         fmt.Sprintf("Ordered '%s' by %s in Lidarr & started search!", resolved.Title, resolved.ArtistName),
	}, nil
}

// FetchQueue returns active items from Lidarr's download queue.
func (m *Manager) FetchQueue(ctx context.Context) ([]QueueItem, error) {
	if !m.IsConfigured() {
		return []QueueItem{}, nil
	}
	var resp struct {
		Records []struct {
			ID                    int     `json:"id"`
			AlbumID               int     `json:"albumId"`
			ArtistID              int     `json:"artistId"`
			Title                 string  `json:"title"`
			Status                string  `json:"status"`
			TrackedDownloadStatus string  `json:"trackedDownloadStatus"`
			TrackedDownloadState  string  `json:"trackedDownloadState"`
			Size                  float64 `json:"size"`
			SizeLeft              float64 `json:"sizeleft"`
			TimeLeft              string  `json:"timeleft"`
			DownloadClient        string  `json:"downloadClient"`
			ErrorMessage          string  `json:"errorMessage"`
			Artist                *struct {
				ID         int    `json:"id"`
				ArtistName string `json:"artistName"`
			} `json:"artist"`
			Album *struct {
				ID    int    `json:"id"`
				Title string `json:"title"`
			} `json:"album"`
			Quality *struct {
				Quality *struct {
					Name string `json:"name"`
				} `json:"quality"`
			} `json:"quality"`
			StatusMessages []struct {
				Title    string   `json:"title"`
				Messages []string `json:"messages"`
			} `json:"statusMessages"`
		} `json:"records"`
	}
	err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/queue?pageSize=40&includeUnknownArtistItems=true&includeArtist=true&includeAlbum=true", nil, &resp)
	if err != nil {
		return nil, err
	}
	out := make([]QueueItem, 0, len(resp.Records))
	for _, rec := range resp.Records {
		item := QueueItem{
			ID:             rec.ID,
			AlbumID:        rec.AlbumID,
			ArtistID:       rec.ArtistID,
			Title:          rec.Title,
			Status:         rec.Status,
			TimeLeft:       rec.TimeLeft,
			DownloadClient: rec.DownloadClient,
			ErrorMessage:   rec.ErrorMessage,
		}
		if rec.Artist != nil {
			item.ArtistName = rec.Artist.ArtistName
			if item.ArtistID == 0 {
				item.ArtistID = rec.Artist.ID
			}
		}
		if rec.Album != nil {
			item.AlbumTitle = rec.Album.Title
			if item.AlbumID == 0 {
				item.AlbumID = rec.Album.ID
			}
			if item.Title == "" {
				item.Title = rec.Album.Title
			}
		}
		if rec.Quality != nil && rec.Quality.Quality != nil {
			item.QualityName = rec.Quality.Quality.Name
		}
		for _, sm := range rec.StatusMessages {
			if len(sm.Messages) > 0 {
				for _, msg := range sm.Messages {
					if strings.TrimSpace(msg) != "" {
						item.StatusMessages = append(item.StatusMessages, msg)
					}
				}
			} else if strings.TrimSpace(sm.Title) != "" {
				item.StatusMessages = append(item.StatusMessages, sm.Title)
			}
		}
		if rec.Size > 0 {
			p := 1.0 - (rec.SizeLeft / rec.Size)
			if p < 0 {
				p = 0
			}
			if p > 1 {
				p = 1
			}
			item.Progress = p
		}
		item.Phase = computeQueuePhase(rec.Status, rec.TrackedDownloadStatus, rec.TrackedDownloadState, len(item.StatusMessages) > 0)
		out = append(out, item)
	}
	return out, nil
}

func computeQueuePhase(status, tdStatus, tdState string, hasMessages bool) string {
	st := strings.ToLower(strings.TrimSpace(status))
	tds := strings.ToLower(strings.TrimSpace(tdStatus))
	tdst := strings.ToLower(strings.TrimSpace(tdState))
	if st == "failed" || tds == "error" || tdst == "failed" || tdst == "failedpending" {
		return "FAILED"
	}
	if tds == "warning" || st == "warning" || (hasMessages && (tdst == "importpending" || tdst == "importblocked")) {
		return "WARNING"
	}
	if tdst == "importing" || tdst == "importpending" || st == "completed" {
		return "IMPORTING"
	}
	if st == "paused" {
		return "PAUSED"
	}
	if st == "downloading" || tdst == "downloading" {
		return "DOWNLOADING"
	}
	return "QUEUED"
}

// --- Internal Lidarr, MusicBrainz & Deezer Helpers ---

func (m *Manager) resolveLidarrProfiles(ctx context.Context) (rootFolder string, qualityID int, metadataID int, err error) {
	var rfs []struct {
		Path string `json:"path"`
	}
	if err = m.lidarrReq(ctx, http.MethodGet, "/api/v1/rootfolder", nil, &rfs); err != nil {
		return "", 0, 0, fmt.Errorf("fetch Lidarr root folders: %w", err)
	}
	if len(rfs) == 0 || strings.TrimSpace(rfs[0].Path) == "" {
		return "", 0, 0, fmt.Errorf("no Root Folder configured in Lidarr")
	}
	rootFolder = rfs[0].Path

	var qps []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err = m.lidarrReq(ctx, http.MethodGet, "/api/v1/qualityprofile", nil, &qps); err != nil {
		return "", 0, 0, fmt.Errorf("fetch Lidarr quality profiles: %w", err)
	}
	if len(qps) == 0 {
		return "", 0, 0, fmt.Errorf("no Quality Profile configured in Lidarr")
	}
	qualityID = qps[0].ID

	var mps []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err = m.lidarrReq(ctx, http.MethodGet, "/api/v1/metadataprofile", nil, &mps); err != nil {
		return "", 0, 0, fmt.Errorf("fetch Lidarr metadata profiles: %w", err)
	}
	if len(mps) == 0 {
		return "", 0, 0, fmt.Errorf("no Metadata Profile configured in Lidarr")
	}
	metadataID = mps[0].ID
	return rootFolder, qualityID, metadataID, nil
}

func (m *Manager) monitorAlbum(ctx context.Context, albumID int, monitored bool) error {
	return m.lidarrReq(ctx, http.MethodPut, "/api/v1/album/monitor", map[string]any{
		"albumIds":  []int{albumID},
		"monitored": monitored,
	}, nil)
}

func (m *Manager) triggerAlbumSearch(ctx context.Context, albumID int) error {
	return m.lidarrReq(ctx, http.MethodPost, "/api/v1/command", map[string]any{
		"name":     "AlbumSearch",
		"albumIds": []int{albumID},
	}, nil)
}

func (m *Manager) lookupAlbumsRaw(ctx context.Context, term string) ([]AlbumSearchResult, error) {
	var rawArr []json.RawMessage
	if err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/album/lookup?term="+url.QueryEscape(term), nil, &rawArr); err != nil {
		return nil, err
	}
	return m.parseAlbumRawSlice(rawArr, ""), nil
}

func (m *Manager) fetchAlbumsForLidarrArtist(ctx context.Context, artistID int, artistName string) ([]AlbumSearchResult, error) {
	var rawArr []json.RawMessage
	path := fmt.Sprintf("/api/v1/album?artistId=%d&includeAllArtistAlbums=true", artistID)
	if err := m.lidarrReq(ctx, http.MethodGet, path, nil, &rawArr); err != nil {
		return nil, err
	}
	return m.parseAlbumRawSlice(rawArr, artistName), nil
}

func (m *Manager) searchArtists(ctx context.Context, term string) ([]ArtistSearchResult, error) {
	var rawArr []json.RawMessage
	if err := m.lidarrReq(ctx, http.MethodGet, "/api/v1/artist/lookup?term="+url.QueryEscape(term), nil, &rawArr); err != nil {
		return nil, err
	}
	baseURL := strings.TrimRight(strings.TrimSpace(m.cfg.LidarrURL), "/")
	out := make([]ArtistSearchResult, 0, len(rawArr))
	for _, raw := range rawArr {
		var d struct {
			ID              int    `json:"id"`
			ForeignArtistID string `json:"foreignArtistId"`
			ArtistName      string `json:"artistName"`
			Overview        string `json:"overview"`
			Monitored       bool   `json:"monitored"`
			Statistics      *struct {
				AlbumCount     int `json:"albumCount"`
				TrackFileCount int `json:"trackFileCount"`
			} `json:"statistics"`
			Images []lidarrImage `json:"images"`
		}
		if err := json.Unmarshal(raw, &d); err != nil || d.ForeignArtistID == "" || d.ArtistName == "" {
			continue
		}
		item := ArtistSearchResult{
			LidarrID:        d.ID,
			ForeignArtistID: d.ForeignArtistID,
			ArtistName:      d.ArtistName,
			Overview:        d.Overview,
			CoverURL:        resolveLidarrImageURL(d.Images, baseURL),
			Monitored:       d.Monitored,
		}
		if d.Statistics != nil {
			item.AlbumCount = d.Statistics.AlbumCount
			item.TrackFileCount = d.Statistics.TrackFileCount
		}
		out = append(out, item)
	}
	return out, nil
}

type lidarrImage struct {
	CoverType string `json:"coverType"`
	RemoteURL string `json:"remoteUrl"`
	URL       string `json:"url"`
}

func resolveLidarrImageURL(images []lidarrImage, baseURL string) string {
	if len(images) == 0 {
		return ""
	}
	pick := images[0]
	for _, img := range images {
		if strings.EqualFold(img.CoverType, "cover") {
			pick = img
			break
		}
	}
	if pick.RemoteURL != "" {
		return pick.RemoteURL
	}
	if pick.URL != "" {
		if strings.HasPrefix(pick.URL, "http://") || strings.HasPrefix(pick.URL, "https://") {
			return pick.URL
		}
		if baseURL != "" {
			return baseURL + "/" + strings.TrimLeft(pick.URL, "/")
		}
	}
	return ""
}

func (m *Manager) parseAlbumRawSlice(rawArr []json.RawMessage, fallbackArtist string) []AlbumSearchResult {
	baseURL := strings.TrimRight(strings.TrimSpace(m.cfg.LidarrURL), "/")
	out := make([]AlbumSearchResult, 0, len(rawArr))
	for _, raw := range rawArr {
		var d struct {
			ID             int    `json:"id"`
			ForeignAlbumID string `json:"foreignAlbumId"`
			Title          string `json:"title"`
			ReleaseDate    string `json:"releaseDate"`
			AlbumType      string `json:"albumType"`
			Monitored      bool   `json:"monitored"`
			Artist         *struct {
				ArtistName      string `json:"artistName"`
				ForeignArtistID string `json:"foreignArtistId"`
			} `json:"artist"`
			Statistics *struct {
				TrackFileCount  int     `json:"trackFileCount"`
				TrackCount      int     `json:"trackCount"`
				PercentOfTracks float64 `json:"percentOfTracks"`
			} `json:"statistics"`
			Images []lidarrImage `json:"images"`
		}
		if err := json.Unmarshal(raw, &d); err != nil || d.ForeignAlbumID == "" || d.Title == "" {
			continue
		}
		artName := fallbackArtist
		foreignArtID := ""
		if d.Artist != nil {
			if strings.TrimSpace(d.Artist.ArtistName) != "" {
				artName = d.Artist.ArtistName
			}
			foreignArtID = d.Artist.ForeignArtistID
		}
		if artName == "" {
			artName = "Unknown Artist"
		}
		yr := 0
		if len(d.ReleaseDate) >= 4 {
			yr, _ = strconv.Atoi(d.ReleaseDate[:4])
		}
		item := AlbumSearchResult{
			LidarrID:        d.ID,
			ForeignAlbumID:  d.ForeignAlbumID,
			Title:           d.Title,
			ArtistName:      artName,
			ForeignArtistID: foreignArtID,
			ReleaseYear:     yr,
			ReleaseDate:     d.ReleaseDate,
			AlbumType:       d.AlbumType,
			CoverURL:        resolveLidarrImageURL(d.Images, baseURL),
			Monitored:       d.Monitored,
			IsInLidarr:      d.ID > 0,
			RawLookup:       raw,
		}
		if d.Statistics != nil {
			item.TrackFileCount = d.Statistics.TrackFileCount
			item.TrackCount = d.Statistics.TrackCount
			item.PercentOfTracks = d.Statistics.PercentOfTracks
			item.IsDownloaded = item.IsInLidarr && (item.PercentOfTracks >= 99.0 || (item.TrackCount > 0 && item.TrackFileCount >= item.TrackCount))
		}
		out = append(out, item)
	}
	return out
}

func (m *Manager) fetchRemoteDiscographyAlbums(ctx context.Context, artist ArtistSearchResult) []AlbumSearchResult {
	var (
		dzAlbums []AlbumSearchResult
		mbAlbums []AlbumSearchResult
		wg       sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		dzAlbums = m.fetchDeezerArtistDiscography(ctx, artist.ArtistName)
	}()
	go func() {
		defer wg.Done()
		mbAlbums = m.fetchMusicBrainzReleaseGroups(ctx, artist.ForeignArtistID, artist.ArtistName)
	}()
	wg.Wait()

	if len(mbAlbums) == 0 {
		return dzAlbums
	}

	dzCoverByNorm := make(map[string]string, len(dzAlbums))
	for _, d := range dzAlbums {
		k := NormalizeForMatch(d.Title)
		if d.CoverURL != "" && dzCoverByNorm[k] == "" {
			dzCoverByNorm[k] = d.CoverURL
		}
	}

	seen := make(map[string]bool, len(mbAlbums))
	enriched := make([]AlbumSearchResult, 0, len(mbAlbums)+len(dzAlbums))
	for _, alb := range mbAlbums {
		k := NormalizeForMatch(alb.Title)
		if cover, ok := dzCoverByNorm[k]; ok {
			alb.CoverURL = cover
		}
		seen[k] = true
		enriched = append(enriched, alb)
	}
	for _, d := range dzAlbums {
		k := NormalizeForMatch(d.Title)
		if k != "" && !seen[k] {
			seen[k] = true
			enriched = append(enriched, d)
		}
	}
	return enriched
}

func (m *Manager) fetchMusicBrainzReleaseGroups(ctx context.Context, foreignArtistID, artistName string) []AlbumSearchResult {
	if strings.TrimSpace(foreignArtistID) == "" {
		return nil
	}
	u := "https://musicbrainz.org/ws/2/release-group?artist=" + url.QueryEscape(foreignArtistID) + "&type=album%7Cep&fmt=json&limit=40"
	var resp struct {
		ReleaseGroups []struct {
			ID               string   `json:"id"`
			Title            string   `json:"title"`
			PrimaryType      string   `json:"primary-type"`
			SecondaryTypes   []string `json:"secondary-types"`
			FirstReleaseDate string   `json:"first-release-date"`
		} `json:"release-groups"`
	}
	if err := m.externalGetJSON(ctx, u, "MyEchoServer/1.0 (https://github.com/baardove/my-echo-server)", &resp); err != nil {
		return nil
	}
	var out []AlbumSearchResult
	for _, rg := range resp.ReleaseGroups {
		skip := false
		isComp := false
		isLive := false
		for _, st := range rg.SecondaryTypes {
			l := strings.ToLower(st)
			if l == "bootleg" || l == "interview" || l == "audiobook" {
				skip = true
				break
			}
			if l == "compilation" {
				isComp = true
			}
			if l == "live" {
				isLive = true
			}
		}
		if skip {
			continue
		}
		yr := 0
		if len(rg.FirstReleaseDate) >= 4 {
			yr, _ = strconv.Atoi(rg.FirstReleaseDate[:4])
		}
		typeLbl := rg.PrimaryType
		if typeLbl == "" {
			typeLbl = "Album"
		}
		if isComp {
			typeLbl = "Compilation"
		} else if isLive {
			typeLbl = "Live"
		}
		out = append(out, AlbumSearchResult{
			ForeignAlbumID:  rg.ID,
			Title:           rg.Title,
			ArtistName:      artistName,
			ForeignArtistID: foreignArtistID,
			ReleaseYear:     yr,
			ReleaseDate:     rg.FirstReleaseDate,
			AlbumType:       typeLbl,
			CoverURL:        fmt.Sprintf("https://coverartarchive.org/release-group/%s/front-250", rg.ID),
		})
	}
	return out
}

type deezerArtistHit struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (m *Manager) findMatchingDeezerArtist(ctx context.Context, term string) *deezerArtistHit {
	trimmed := strings.TrimSpace(term)
	if trimmed == "" {
		return nil
	}
	var resp struct {
		Data []deezerArtistHit `json:"data"`
	}
	u := "https://api.deezer.com/search/artist?q=" + url.QueryEscape(trimmed) + "&limit=5"
	if err := m.externalGetJSON(ctx, u, "MyEchoServer/1.0", &resp); err != nil || len(resp.Data) == 0 {
		return nil
	}
	normQ := NormalizeForMatch(trimmed)
	if normQ == "" {
		return nil
	}
	for i := range resp.Data {
		normName := NormalizeForMatch(resp.Data[i].Name)
		if normName == normQ || strings.HasPrefix(normName, normQ) || strings.HasPrefix(normQ, normName) || strings.Contains(normName, normQ) {
			cp := resp.Data[i]
			return &cp
		}
	}
	return nil
}

func (m *Manager) fetchDeezerArtistDiscography(ctx context.Context, artistName string) []AlbumSearchResult {
	art := m.findMatchingDeezerArtist(ctx, artistName)
	if art == nil {
		return nil
	}
	var resp struct {
		Data []struct {
			ID          int    `json:"id"`
			Title       string `json:"title"`
			CoverMedium string `json:"cover_medium"`
			CoverBig    string `json:"cover_big"`
			ReleaseDate string `json:"release_date"`
			RecordType  string `json:"record_type"`
		} `json:"data"`
	}
	u := fmt.Sprintf("https://api.deezer.com/artist/%d/albums?limit=35", art.ID)
	if err := m.externalGetJSON(ctx, u, "MyEchoServer/1.0", &resp); err != nil {
		return nil
	}
	out := make([]AlbumSearchResult, 0, len(resp.Data))
	for _, item := range resp.Data {
		yr := 0
		if len(item.ReleaseDate) >= 4 {
			yr, _ = strconv.Atoi(item.ReleaseDate[:4])
		}
		typeLbl := "Album"
		switch strings.ToLower(item.RecordType) {
		case "ep":
			typeLbl = "EP"
		case "single":
			typeLbl = "Single"
		case "compile":
			typeLbl = "Compilation"
		}
		cover := item.CoverBig
		if cover == "" {
			cover = item.CoverMedium
		}
		out = append(out, AlbumSearchResult{
			ForeignAlbumID: fmt.Sprintf("deezer-album:%d", item.ID),
			Title:          item.Title,
			ArtistName:     art.Name,
			ReleaseYear:    yr,
			ReleaseDate:    item.ReleaseDate,
			AlbumType:      typeLbl,
			CoverURL:       cover,
		})
	}
	return out
}

func (m *Manager) externalGetJSON(ctx context.Context, rawURL, userAgent string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2_097_152))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

// --- Pure Ranking & Normalization Helpers (exported for unit tests) ---

func SelectMatchingArtists(artists []ArtistSearchResult, query string) []ArtistSearchResult {
	normQ := NormalizeForMatch(query)
	if normQ == "" {
		return nil
	}
	tokens := strings.Fields(normQ)
	var out []ArtistSearchResult
	for _, a := range artists {
		normName := NormalizeForMatch(a.ArtistName)
		if normName == "" {
			continue
		}
		if normName == normQ || strings.HasPrefix(normName, normQ) || strings.HasPrefix(normQ, normName) {
			out = append(out, a)
			continue
		}
		if len(normQ) >= 3 && strings.Contains(normName, normQ) {
			out = append(out, a)
			continue
		}
		if len(tokens) >= 2 {
			all := true
			for _, tok := range tokens {
				if !strings.Contains(normName, tok) {
					all = false
					break
				}
			}
			if all {
				out = append(out, a)
			}
		}
	}
	return out
}

func MergeAndRankAlbumResults(query string, directAlbums, artistAlbums []AlbumSearchResult, matchedArtistNames []string) []AlbumSearchResult {
	normQ := NormalizeForMatch(query)
	normMatched := make(map[string]bool, len(matchedArtistNames))
	for _, n := range matchedArtistNames {
		if k := NormalizeForMatch(n); k != "" {
			normMatched[k] = true
		}
	}

	byKey := make(map[string]AlbumSearchResult)
	var order []string
	richness := func(a AlbumSearchResult) int {
		r := 0
		if a.IsInLidarr {
			r += 4
		}
		if len(a.RawLookup) > 0 {
			r += 2
		}
		if a.CoverURL != "" {
			r += 1
		}
		return r
	}
	ingest := func(item AlbumSearchResult) {
		nt := NormalizeForMatch(item.Title)
		na := NormalizeForMatch(item.ArtistName)
		key := strings.ToLower(item.ForeignAlbumID)
		if nt != "" {
			key = na + "::" + nt
		}
		if existing, ok := byKey[key]; ok {
			if richness(item) > richness(existing) {
				merged := item
				if merged.CoverURL == "" {
					merged.CoverURL = existing.CoverURL
				}
				if merged.ReleaseYear == 0 {
					merged.ReleaseYear = existing.ReleaseYear
				}
				byKey[key] = merged
			} else if existing.CoverURL == "" && item.CoverURL != "" {
				existing.CoverURL = item.CoverURL
				byKey[key] = existing
			}
		} else {
			byKey[key] = item
			order = append(order, key)
		}
	}

	for _, a := range directAlbums {
		ingest(a)
	}
	for _, a := range artistAlbums {
		ingest(a)
	}

	score := func(alb AlbumSearchResult) int {
		nt := NormalizeForMatch(alb.Title)
		na := NormalizeForMatch(alb.ArtistName)
		tl := strings.ToLower(alb.AlbumType)
		isStudio := tl == "" || tl == "album"
		isSingle := tl == "single"
		isComp := tl == "compilation" || tl == "live"

		s := 0
		if na == normQ || normMatched[na] {
			s += 900
		} else if normQ != "" && (strings.HasPrefix(na, normQ) || strings.Contains(na, normQ)) {
			s += 650
		}
		if nt == normQ {
			s += 1000
		} else if normQ != "" && strings.HasPrefix(nt, normQ) {
			s += 720
		} else if normQ != "" && strings.Contains(nt, normQ) {
			s += 520
		}
		if isStudio {
			s += 160
		} else if isComp {
			s += 40
		} else if isSingle {
			s -= 120
		}
		if alb.IsInLidarr {
			s += 30
		}
		if len(alb.RawLookup) > 0 {
			s += 15
		}
		return s
	}

	out := make([]AlbumSearchResult, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k])
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := score(out[i]), score(out[j])
		if si != sj {
			return si > sj
		}
		if out[i].ReleaseYear != out[j].ReleaseYear {
			return out[i].ReleaseYear > out[j].ReleaseYear
		}
		return strings.ToLower(out[i].Title) < strings.ToLower(out[j].Title)
	})
	return out
}

func MergeAndRankTrackResults(query string, directTracks, artistTopTracks, albumTracks []TrackMatch, matchedArtistName string) []TrackMatch {
	normQ := NormalizeForMatch(query)
	normArt := NormalizeForMatch(matchedArtistName)
	queryIsExactArtist := normArt != "" && (normArt == normQ || strings.HasPrefix(normArt, normQ))

	seenID := make(map[string]bool)
	seenKey := make(map[string]bool)
	var combined []TrackMatch
	ingest := func(list []TrackMatch) {
		for _, t := range list {
			if seenID[t.ID] {
				continue
			}
			k := NormalizeForMatch(t.ArtistName) + "::" + NormalizeForMatch(t.Title)
			if seenKey[k] {
				continue
			}
			seenID[t.ID] = true
			seenKey[k] = true
			combined = append(combined, t)
		}
	}

	if queryIsExactArtist {
		ingest(artistTopTracks)
		ingest(directTracks)
		ingest(albumTracks)
	} else {
		ingest(directTracks)
		ingest(albumTracks)
		ingest(artistTopTracks)
	}

	score := func(t TrackMatch) int {
		nt := NormalizeForMatch(t.Title)
		na := NormalizeForMatch(t.ArtistName)
		nal := NormalizeForMatch(t.AlbumTitle)
		s := t.Popularity
		if queryIsExactArtist && (na == normArt || strings.Contains(na, normArt)) {
			s += 900
		} else if na == normQ {
			s += 850
		} else if normQ != "" && strings.Contains(na, normQ) {
			s += 500
		}
		if nt == normQ {
			s += 950
		} else if normQ != "" && strings.HasPrefix(nt, normQ) {
			s += 700
		} else if normQ != "" && strings.Contains(nt, normQ) {
			s += 520
		}
		if nal == normQ {
			s += 620
		} else if normQ != "" && strings.Contains(nal, normQ) {
			s += 320
		}
		return s
	}

	sort.Slice(combined, func(i, j int) bool {
		si, sj := score(combined[i]), score(combined[j])
		if si != sj {
			return si > sj
		}
		return combined[i].Popularity > combined[j].Popularity
	})
	if len(combined) > 40 {
		combined = combined[:40]
	}
	if combined == nil {
		return []TrackMatch{}
	}
	return combined
}

func NormalizeForMatch(s string) string {
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

func scoreFromDeezerRank(rank int) int {
	if rank <= 0 {
		return 0
	}
	// Map Deezer's ~100k–1M rank logarithmically into 1..100 (matching EchoCore OnlinePopularityService)
	v := math.Log10(float64(rank))
	pct := int(math.Round(((v - 3.5) / 2.5) * 100.0))
	if pct < 1 {
		pct = 1
	}
	if pct > 100 {
		pct = 100
	}
	return pct
}

func isValidUUID(s string) bool {
	_, err := uuid.Parse(strings.TrimSpace(s))
	return err == nil
}
