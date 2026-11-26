package api

import (
	"encoding/json"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baardove/my-echo/server/internal/db"
	qrcode "github.com/skip2/go-qrcode"
)

type webTrackDTO struct {
	ID            string   `json:"id"`
	Title         string   `json:"title"`
	Artist        string   `json:"artist"`
	AlbumArtist   string   `json:"albumArtist"`
	Album         string   `json:"album"`
	AlbumKey      string   `json:"albumKey"`
	TrackNumber   *int     `json:"trackNumber,omitempty"`
	DiscNumber    *int     `json:"discNumber,omitempty"`
	Year          *int     `json:"year,omitempty"`
	Genre         *string  `json:"genre,omitempty"`
	Duration      float64  `json:"duration"`
	Format        string   `json:"format"`
	FormatDetail  string   `json:"formatDetail,omitempty"`
	IsHiRes       bool     `json:"isHiRes"`
	ArtworkHash   *string  `json:"artworkHash,omitempty"`
	ArrivalUnix   int64    `json:"arrivalUnix"`
	UserRating    int      `json:"userRating"`
	BPM           float64  `json:"bpm,omitempty"`
	VocalStartSec float64  `json:"vocalStartSec,omitempty"`
	VocalEndSec   float64  `json:"vocalEndSec,omitempty"`
	Energy        float64  `json:"energy,omitempty"`
	Tags          []string `json:"tags,omitempty"`
}

type webAlbumDTO struct {
	AlbumKey          string   `json:"albumKey"`
	Title             string   `json:"title"`
	Artist            string   `json:"artist"`
	Year              *int     `json:"year,omitempty"`
	ReleaseDate       string   `json:"releaseDate,omitempty"`
	TrackCount        int      `json:"trackCount"`
	TotalDuration     float64  `json:"totalDuration"`
	ArtworkHash       *string  `json:"artworkHash,omitempty"`
	LatestArrivalUnix int64    `json:"latestArrivalUnix"`
	UserRating        int      `json:"userRating"`
	Genres            []string `json:"genres,omitempty"`
	Tags              []string `json:"tags,omitempty"`
}

type webArtistDTO struct {
	ArtistKey   string  `json:"artistKey"`
	Name        string  `json:"name"`
	AlbumCount  int     `json:"albumCount"`
	TrackCount  int     `json:"trackCount"`
	ArtworkHash *string `json:"artworkHash,omitempty"`
}

type webPlaylistDTO struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	TrackCount  int           `json:"trackCount"`
	Tracks      []webTrackDTO `json:"tracks"`
}

func toWebTrack(tr db.TrackRecord) webTrackDTO {
	artist := strings.TrimSpace(tr.Artist)
	if artist == "" {
		artist = strings.TrimSpace(tr.AlbumArtist)
	}
	if artist == "" {
		artist = "Unknown Artist"
	}
	albArtist := strings.TrimSpace(tr.AlbumArtist)
	if albArtist == "" {
		albArtist = artist
	}
	alb := strings.TrimSpace(tr.Album)
	if alb == "" {
		alb = "Unknown Album"
	}
	arr := tr.ModifiedAt.Unix()
	if tr.AddedAt != nil && tr.AddedAt.Unix() > arr {
		arr = tr.AddedAt.Unix()
	}
	detail := formatQualityDetail(tr)
	hiRes := strings.EqualFold(tr.Format, "dsf") || strings.EqualFold(tr.Format, "dff") ||
		(tr.BitDepth != nil && *tr.BitDepth >= 24) ||
		(tr.SampleRate != nil && *tr.SampleRate >= 88200)
	return webTrackDTO{
		ID:           tr.EffectiveID(),
		Title:        tr.Title,
		Artist:       artist,
		AlbumArtist:  albArtist,
		Album:        alb,
		AlbumKey:     strings.ToLower(alb),
		TrackNumber:  tr.TrackNumber,
		DiscNumber:   tr.DiscNumber,
		Year:         tr.Year,
		Genre:        tr.Genre,
		Duration:     tr.Duration,
		Format:       strings.ToUpper(tr.Format),
		FormatDetail: detail,
		IsHiRes:      hiRes,
		ArtworkHash:  tr.ArtworkHash,
		ArrivalUnix:  arr,
	}
}

func formatQualityDetail(tr db.TrackRecord) string {
	f := strings.ToLower(tr.Format)
	if f == "dsf" || f == "dff" {
		if tr.SampleRate != nil && *tr.SampleRate >= 5644800 {
			return "DSD128"
		}
		return "DSD64"
	}
	if tr.BitDepth != nil && tr.SampleRate != nil && *tr.SampleRate > 0 {
		khz := float64(*tr.SampleRate) / 1000.0
		if khz == float64(int(khz)) {
			return fmt.Sprintf("%db/%dkHz", *tr.BitDepth, int(khz))
		}
		return fmt.Sprintf("%db/%.1fkHz", *tr.BitDepth, khz)
	}
	if tr.Bitrate != nil && *tr.Bitrate > 0 {
		return fmt.Sprintf("%dk", *tr.Bitrate)
	}
	return ""
}

func (s *Server) handleWebTracks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := strings.TrimSpace(q.Get("q"))
	scope := strings.ToLower(strings.TrimSpace(q.Get("scope")))
	sortMode := strings.ToLower(strings.TrimSpace(q.Get("sort")))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if offset < 0 {
		offset = 0
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 60
	}
	if limit > 200 {
		limit = 200
	}

	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	minRating, _ := strconv.Atoi(q.Get("minRating"))
	avoidMaxRating, _ := strconv.Atoi(q.Get("avoidMaxRating"))
	genreFilter := strings.ToLower(strings.TrimSpace(q.Get("genre")))
	tagFilter := strings.ToLower(strings.TrimSpace(q.Get("tag")))
	eraFilter := strings.ToLower(strings.TrimSpace(q.Get("era")))
	bpmMin, _ := strconv.ParseFloat(q.Get("bpmMin"), 64)
	bpmMax, _ := strconv.ParseFloat(q.Get("bpmMax"), 64)

	ratingMap := s.loadTrackRatingsMap()
	analysisMap, _ := s.store.ListTrackAnalysisMap()
	edGenresByAlb := s.loadAlbumEditorialGenresMap()

	var items []webTrackDTO
	tokens := parseSimpleTokens(query)
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		enrichTrackDTO(&dto, tr, ratingMap, analysisMap, edGenresByAlb)
		if minRating > 0 && dto.UserRating < minRating {
			continue
		}
		if avoidMaxRating > 0 && dto.UserRating > 0 && dto.UserRating <= avoidMaxRating {
			continue
		}
		if genreFilter != "" && !matchesAnyCSV(genreFilter, func(g string) bool { return trackMatchesGenre(dto, g) }) {
			continue
		}
		if tagFilter != "" && !matchesAnyCSV(tagFilter, func(t string) bool { return trackMatchesTag(dto, t) }) {
			continue
		}
		if eraFilter != "" && !yearMatchesEraCSV(dto.Year, "", eraFilter) {
			continue
		}
		if bpmMin > 0 && (dto.BPM <= 0 || dto.BPM < bpmMin) {
			continue
		}
		if bpmMax > 0 && (dto.BPM <= 0 || dto.BPM > bpmMax) {
			continue
		}
		if len(tokens) > 0 && !matchesWebTokens(dto, scope, tokens) {
			continue
		}
		items = append(items, dto)
	}

	switch sortMode {
	case "recent":
		sort.Slice(items, func(i, j int) bool {
			if items[i].ArrivalUnix != items[j].ArrivalUnix {
				return items[i].ArrivalUnix > items[j].ArrivalUnix
			}
			return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
		})
	case "release":
		sort.Slice(items, func(i, j int) bool {
			yi, yj := 0, 0
			if items[i].Year != nil {
				yi = *items[i].Year
			}
			if items[j].Year != nil {
				yj = *items[j].Year
			}
			if yi != yj {
				return yi > yj
			}
			if strings.ToLower(items[i].Album) != strings.ToLower(items[j].Album) {
				return strings.ToLower(items[i].Album) < strings.ToLower(items[j].Album)
			}
			return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
		})
	case "rating":
		sort.Slice(items, func(i, j int) bool {
			if items[i].UserRating != items[j].UserRating {
				return items[i].UserRating > items[j].UserRating
			}
			return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
		})
	case "bpm":
		sort.Slice(items, func(i, j int) bool {
			if items[i].BPM != items[j].BPM {
				return items[i].BPM > items[j].BPM
			}
			return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
		})
	case "artist":
		sort.Slice(items, func(i, j int) bool {
			ai, aj := strings.ToLower(items[i].Artist), strings.ToLower(items[j].Artist)
			if ai != aj {
				return ai < aj
			}
			return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
		})
	default:
		sort.Slice(items, func(i, j int) bool {
			ti, tj := strings.ToLower(items[i].Title), strings.ToLower(items[j].Title)
			if ti != tj {
				return ti < tj
			}
			return strings.ToLower(items[i].Artist) < strings.ToLower(items[j].Artist)
		})
	}

	total := len(items)
	start := min(offset, total)
	end := min(start+limit, total)
	page := items[start:end]
	if page == nil {
		page = []webTrackDTO{}
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"total":   total,
		"offset":  start,
		"limit":   limit,
		"hasMore": end < total,
		"tracks":  page,
	})
}

func (s *Server) handleWebAlbums(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := strings.ToLower(strings.TrimSpace(q.Get("q")))
	sortMode := strings.ToLower(strings.TrimSpace(q.Get("sort")))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if offset < 0 {
		offset = 0
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 48
	}
	if limit > 200 {
		limit = 200
	}

	minRating, _ := strconv.Atoi(q.Get("minRating"))
	avoidMaxRating, _ := strconv.Atoi(q.Get("avoidMaxRating"))
	genreFilter := strings.ToLower(strings.TrimSpace(q.Get("genre")))
	tagFilter := strings.ToLower(strings.TrimSpace(q.Get("tag")))
	eraFilter := strings.ToLower(strings.TrimSpace(q.Get("era")))

	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	albRatingMap := s.loadAlbumRatingsMap()
	trackRatingMap := s.loadTrackRatingsMap()
	analysisMap, _ := s.store.ListTrackAnalysisMap()

	byKey := make(map[string]*webAlbumDTO)
	artistCounts := make(map[string]map[string]int)
	albGenreSet := make(map[string]map[string]bool)
	albTagSet := make(map[string]map[string]bool)
	albMaxTrackRating := make(map[string]int)
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		key := dto.AlbumKey
		alb, ok := byKey[key]
		if !ok {
			alb = &webAlbumDTO{
				AlbumKey:          key,
				Title:             dto.Album,
				Artist:            dto.AlbumArtist,
				Year:              dto.Year,
				ArtworkHash:       dto.ArtworkHash,
				LatestArrivalUnix: dto.ArrivalUnix,
				UserRating:        albRatingMap[key],
			}
			byKey[key] = alb
			artistCounts[key] = make(map[string]int)
			albGenreSet[key] = make(map[string]bool)
			albTagSet[key] = make(map[string]bool)
		}
		alb.TrackCount++
		alb.TotalDuration += dto.Duration
		if alb.ArtworkHash == nil && dto.ArtworkHash != nil {
			alb.ArtworkHash = dto.ArtworkHash
		}
		if alb.Year == nil && dto.Year != nil {
			alb.Year = dto.Year
		}
		if dto.ArrivalUnix > alb.LatestArrivalUnix {
			alb.LatestArrivalUnix = dto.ArrivalUnix
		}
		artistCounts[key][dto.Artist]++
		if dto.Genre != nil && strings.TrimSpace(*dto.Genre) != "" {
			albGenreSet[key][strings.TrimSpace(*dto.Genre)] = true
		}
		if r := lookupTrackRating(trackRatingMap, tr); r > albMaxTrackRating[key] {
			albMaxTrackRating[key] = r
		}
		if an, ok := analysisMap[tr.TrackKey]; ok {
			if an.Genre != "" {
				albGenreSet[key][an.Genre] = true
			}
			for _, t := range an.Tags {
				albTagSet[key][t] = true
			}
		}
		if dto.Year != nil && *dto.Year >= 1950 {
			albTagSet[key][fmt.Sprintf("%ds", (*dto.Year/10)*10)] = true
		}
	}

	// Merge editorial release dates (YYYY-MM-DD) & genres for exact release sorting & filtering
	edReleaseByAlb := make(map[string]string)
	if eds, err := s.store.ListAlbumEditorial(); err == nil {
		for _, ed := range eds {
			k := strings.ToLower(ed.AlbumKey)
			if ed.ReleaseDate != nil && strings.TrimSpace(*ed.ReleaseDate) != "" {
				edReleaseByAlb[k] = strings.TrimSpace(*ed.ReleaseDate)
			}
			if gset, ok := albGenreSet[k]; ok {
				for _, g := range ed.Genres {
					if strings.TrimSpace(g) != "" {
						gset[strings.TrimSpace(g)] = true
					}
				}
			}
		}
	}

	var albums []webAlbumDTO
	for key, alb := range byKey {
		// Majority artist or Various Artists
		bestArtist := alb.Artist
		bestCnt := 0
		for name, cnt := range artistCounts[key] {
			if cnt > bestCnt {
				bestCnt = cnt
				bestArtist = name
			}
		}
		if bestCnt*2 < alb.TrackCount {
			alb.Artist = "Various Artists"
		} else if bestArtist != "" {
			alb.Artist = bestArtist
		}
		if rd, ok := edReleaseByAlb[key]; ok {
			alb.ReleaseDate = rd
			if alb.Year == nil && len(rd) >= 4 {
				if y, err := strconv.Atoi(rd[:4]); err == nil && y > 1900 {
					alb.Year = &y
				}
			}
		} else if alb.Year != nil && *alb.Year > 0 {
			alb.ReleaseDate = fmt.Sprintf("%04d-00-00", *alb.Year)
		}
		for g := range albGenreSet[key] {
			alb.Genres = append(alb.Genres, g)
		}
		sort.Strings(alb.Genres)
		for t := range albTagSet[key] {
			alb.Tags = append(alb.Tags, t)
		}
		sort.Strings(alb.Tags)

		// Effective star check: album rating OR highest rated track in album when filtering by minRating
		effRating := alb.UserRating
		if effRating == 0 {
			effRating = albMaxTrackRating[key]
		}
		if minRating > 0 && effRating < minRating {
			continue
		}
		if avoidMaxRating > 0 && effRating > 0 && effRating <= avoidMaxRating {
			continue
		}
		if genreFilter != "" && !matchesAnyCSV(genreFilter, func(want string) bool {
			for _, g := range alb.Genres {
				if strings.Contains(strings.ToLower(g), want) {
					return true
				}
			}
			return false
		}) {
			continue
		}
		if tagFilter != "" && !matchesAnyCSV(tagFilter, func(want string) bool {
			for _, t := range alb.Tags {
				if strings.Contains(strings.ToLower(t), want) {
					return true
				}
			}
			for _, g := range alb.Genres {
				if strings.Contains(strings.ToLower(g), want) {
					return true
				}
			}
			return false
		}) {
			continue
		}
		if eraFilter != "" && !yearMatchesEraCSV(alb.Year, alb.ReleaseDate, eraFilter) {
			continue
		}

		if query != "" && !strings.Contains(strings.ToLower(alb.Title), query) && !strings.Contains(strings.ToLower(alb.Artist), query) {
			continue
		}
		albums = append(albums, *alb)
	}

	switch sortMode {
	case "title":
		sort.Slice(albums, func(i, j int) bool {
			return strings.ToLower(albums[i].Title) < strings.ToLower(albums[j].Title)
		})
	case "release":
		// Strictly sort by Release Year descending first so Release Year group headers are 100% contiguous!
		sort.Slice(albums, func(i, j int) bool {
			yi, yj := albumReleaseYear(albums[i]), albumReleaseYear(albums[j])
			if yi != yj {
				return yi > yj
			}
			if albums[i].ReleaseDate != albums[j].ReleaseDate {
				return albums[i].ReleaseDate > albums[j].ReleaseDate
			}
			return strings.ToLower(albums[i].Title) < strings.ToLower(albums[j].Title)
		})
	case "rating":
		sort.Slice(albums, func(i, j int) bool {
			if albums[i].UserRating != albums[j].UserRating {
				return albums[i].UserRating > albums[j].UserRating
			}
			return strings.ToLower(albums[i].Title) < strings.ToLower(albums[j].Title)
		})
	default:
		sort.Slice(albums, func(i, j int) bool {
			if albums[i].LatestArrivalUnix != albums[j].LatestArrivalUnix {
				return albums[i].LatestArrivalUnix > albums[j].LatestArrivalUnix
			}
			return strings.ToLower(albums[i].Title) < strings.ToLower(albums[j].Title)
		})
	}

	total := len(albums)
	start := min(offset, total)
	end := min(start+limit, total)
	page := albums[start:end]
	if page == nil {
		page = []webAlbumDTO{}
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"total":   total,
		"offset":  start,
		"limit":   limit,
		"hasMore": end < total,
		"albums":  page,
	})
}

func (s *Server) handleWebAlbumDetail(w http.ResponseWriter, r *http.Request) {
	key := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("key")))
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing album key"})
		return
	}
	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	ratingMap := s.loadTrackRatingsMap()
	analysisMap, _ := s.store.ListTrackAnalysisMap()
	edGenresByAlb := s.loadAlbumEditorialGenresMap()
	albRatingMap := s.loadAlbumRatingsMap()

	var matched []webTrackDTO
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		if dto.AlbumKey == key {
			enrichTrackDTO(&dto, tr, ratingMap, analysisMap, edGenresByAlb)
			matched = append(matched, dto)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		di, dj := 1, 1
		if matched[i].DiscNumber != nil {
			di = *matched[i].DiscNumber
		}
		if matched[j].DiscNumber != nil {
			dj = *matched[j].DiscNumber
		}
		if di != dj {
			return di < dj
		}
		ti, tj := 999, 999
		if matched[i].TrackNumber != nil {
			ti = *matched[i].TrackNumber
		}
		if matched[j].TrackNumber != nil {
			tj = *matched[j].TrackNumber
		}
		if ti != tj {
			return ti < tj
		}
		return strings.ToLower(matched[i].Title) < strings.ToLower(matched[j].Title)
	})

	var editorial *db.AlbumEditorial
	if eds, err := s.store.ListAlbumEditorial(); err == nil {
		for _, ed := range eds {
			if strings.ToLower(ed.AlbumKey) == key {
				copyEd := ed
				editorial = &copyEd
				break
			}
		}
	}
	if editorial == nil && s.editorial != nil && len(matched) > 0 {
		editorial = s.editorial.EnsureAlbum(r.Context(), key, matched[0].AlbumArtist, matched[0].Album)
	}

	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"albumKey":   key,
		"userRating": albRatingMap[key],
		"tracks":     matched,
		"editorial":  editorial,
	})
}

func (s *Server) handleWebArtists(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := strings.ToLower(strings.TrimSpace(q.Get("q")))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if offset < 0 {
		offset = 0
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 60
	}
	if limit > 200 {
		limit = 200
	}

	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	type acc struct {
		dto    webArtistDTO
		albums map[string]struct{}
	}
	byKey := make(map[string]*acc)
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		name := dto.AlbumArtist
		if name == "" {
			name = dto.Artist
		}
		key := strings.ToLower(name)
		a, ok := byKey[key]
		if !ok {
			a = &acc{
				dto:    webArtistDTO{ArtistKey: key, Name: name, ArtworkHash: dto.ArtworkHash},
				albums: make(map[string]struct{}),
			}
			byKey[key] = a
		}
		a.dto.TrackCount++
		a.albums[dto.AlbumKey] = struct{}{}
		if a.dto.ArtworkHash == nil && dto.ArtworkHash != nil {
			a.dto.ArtworkHash = dto.ArtworkHash
		}
	}

	var out []webArtistDTO
	for _, a := range byKey {
		a.dto.AlbumCount = len(a.albums)
		if query != "" && !strings.Contains(strings.ToLower(a.dto.Name), query) {
			continue
		}
		out = append(out, a.dto)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})

	total := len(out)
	start := min(offset, total)
	end := min(start+limit, total)
	page := out[start:end]
	if page == nil {
		page = []webArtistDTO{}
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"total":   total,
		"offset":  start,
		"limit":   limit,
		"hasMore": end < total,
		"artists": page,
	})
}

func (s *Server) handleWebArtistDetail(w http.ResponseWriter, r *http.Request) {
	key := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("key")))
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing artist key"})
		return
	}
	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	ratingMap := s.loadTrackRatingsMap()
	analysisMap, _ := s.store.ListTrackAnalysisMap()
	edGenresByAlb := s.loadAlbumEditorialGenresMap()
	var matched []webTrackDTO
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		if strings.ToLower(dto.AlbumArtist) == key || strings.ToLower(dto.Artist) == key {
			enrichTrackDTO(&dto, tr, ratingMap, analysisMap, edGenresByAlb)
			matched = append(matched, dto)
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].AlbumKey != matched[j].AlbumKey {
			yi, yj := 0, 0
			if matched[i].Year != nil {
				yi = *matched[i].Year
			}
			if matched[j].Year != nil {
				yj = *matched[j].Year
			}
			if yi != yj {
				return yi > yj
			}
			return matched[i].AlbumKey < matched[j].AlbumKey
		}
		ti, tj := 999, 999
		if matched[i].TrackNumber != nil {
			ti = *matched[i].TrackNumber
		}
		if matched[j].TrackNumber != nil {
			tj = *matched[j].TrackNumber
		}
		return ti < tj
	})
	var artistEditorial *db.ArtistEditorial
	if eds, err := s.store.ListArtistEditorial(); err == nil {
		for _, ed := range eds {
			if strings.ToLower(ed.ArtistKey) == key {
				cp := ed
				artistEditorial = &cp
				break
			}
		}
	}
	if artistEditorial == nil && s.editorial != nil && len(matched) > 0 {
		artistEditorial = s.editorial.EnsureArtist(r.Context(), key, matched[0].AlbumArtist)
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"artistKey": key,
		"tracks":    matched,
		"editorial": artistEditorial,
	})
}

func (s *Server) handleWebPlaylists(w http.ResponseWriter, r *http.Request) {
	pls, err := s.store.ListPlaylists(false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	tracks, _ := s.store.LoadAllTracks()
	byID := make(map[string]webTrackDTO, len(tracks)*2)
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		byID[dto.ID] = dto
		byID[tr.TrackKey] = dto
		if tr.LegacyID != "" {
			byID[tr.LegacyID] = dto
		}
	}
	out := make([]webPlaylistDTO, 0, len(pls))
	for _, p := range pls {
		resolved := make([]webTrackDTO, 0, len(p.TrackIDs))
		for _, id := range p.TrackIDs {
			if t, ok := byID[id]; ok {
				resolved = append(resolved, t)
			}
		}
		out = append(out, webPlaylistDTO{
			ID:          p.ID,
			Name:        p.Name,
			Description: p.Description,
			TrackCount:  len(resolved),
			Tracks:      resolved,
		})
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{"playlists": out})
}

func (s *Server) handlePartyQR(w http.ResponseWriter, r *http.Request) {
	st := s.party.Status()
	targetURL := fmt.Sprintf("http://%s:%d", s.cfg.EffectiveAdvertiseHost(), s.cfg.PartyHTTPPort)
	if st.JoinURL != nil && *st.JoinURL != "" {
		targetURL = *st.JoinURL
	}
	if custom := strings.TrimSpace(r.URL.Query().Get("url")); custom != "" {
		targetURL = custom
	}
	pngBytes, err := qrcode.Encode(targetURL, qrcode.Medium, 320)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "QR generation failed"})
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(pngBytes)
}

// --- Simple multi-field token matcher for Web Browse ---

type simpleToken struct {
	field   string
	val     string
	exclude bool
}

func parseSimpleTokens(raw string) []simpleToken {
	var out []simpleToken
	for _, tok := range strings.Fields(raw) {
		t := simpleToken{}
		if strings.HasPrefix(tok, "-") {
			t.exclude = true
			tok = tok[1:]
		} else if strings.HasPrefix(tok, "+") {
			tok = tok[1:]
		}
		if c := strings.IndexByte(tok, ':'); c > 0 {
			f := strings.ToLower(tok[:c])
			v := strings.ToLower(strings.TrimSpace(tok[c+1:]))
			switch f {
			case "artist", "track", "title", "album", "genre", "year", "format", "tag", "stars", "rating":
				if f == "title" {
					f = "track"
				}
				if f == "rating" {
					f = "stars"
				}
				t.field = f
				t.val = v
			default:
				t.val = strings.ToLower(tok)
			}
		} else {
			t.val = strings.ToLower(tok)
		}
		if t.val != "" {
			out = append(out, t)
		}
	}
	return out
}

func matchesWebTokens(dto webTrackDTO, scope string, tokens []simpleToken) bool {
	titleL := strings.ToLower(dto.Title)
	artistL := strings.ToLower(dto.Artist)
	albArtistL := strings.ToLower(dto.AlbumArtist)
	albumL := strings.ToLower(dto.Album)
	genreL := ""
	if dto.Genre != nil {
		genreL = strings.ToLower(*dto.Genre)
	}
	yearS := ""
	if dto.Year != nil {
		yearS = strconv.Itoa(*dto.Year)
	}
	fmtL := strings.ToLower(dto.Format)

	matchField := func(field, needle string) bool {
		switch field {
		case "track":
			return strings.Contains(titleL, needle)
		case "artist":
			return strings.Contains(artistL, needle) || strings.Contains(albArtistL, needle)
		case "album":
			return strings.Contains(albumL, needle)
		case "genre":
			return trackMatchesGenre(dto, needle)
		case "tag":
			return trackMatchesTag(dto, needle)
		case "stars":
			minR, _ := strconv.Atoi(strings.TrimSuffix(needle, "+"))
			return dto.UserRating >= minR
		case "year":
			return yearS != "" && strings.Contains(yearS, needle)
		case "format":
			return strings.Contains(fmtL, needle)
		default:
			return strings.Contains(titleL, needle) ||
				strings.Contains(artistL, needle) ||
				strings.Contains(albArtistL, needle) ||
				strings.Contains(albumL, needle) ||
				strings.Contains(genreL, needle) ||
				trackMatchesTag(dto, needle) ||
				(yearS != "" && strings.Contains(yearS, needle))
		}
	}

	scopeField := ""
	if scope == "track" || scope == "artist" || scope == "album" {
		scopeField = scope
	}
	for _, tok := range tokens {
		f := tok.field
		if f == "" && !tok.exclude {
			f = scopeField
		}
		hit := matchField(f, tok.val)
		if tok.exclude && hit {
			return false
		}
		if !tok.exclude && !hit {
			return false
		}
	}
	return true
}

// --- Star Ratings & Advanced Search Facets ---

func (s *Server) handleWebRate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		TrackID  string `json:"trackId"`
		AlbumKey string `json:"albumKey"`
		Rating   int    `json:"rating"` // 0 clears rating, 1..5 sets stars
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16384)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	if req.Rating < 0 {
		req.Rating = 0
	}
	if req.Rating > 5 {
		req.Rating = 5
	}
	if strings.TrimSpace(req.TrackID) != "" {
		st, err := s.store.SetTrackRating(strings.TrimSpace(req.TrackID), req.Rating)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "trackId": st.TrackID, "rating": req.Rating})
		return
	}
	if strings.TrimSpace(req.AlbumKey) != "" {
		ar, err := s.store.SetAlbumRating(strings.ToLower(strings.TrimSpace(req.AlbumKey)), req.Rating)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "albumKey": ar.AlbumKey, "rating": ar.Rating})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Provide trackId or albumKey"})
}

func (s *Server) handleWebFacets(w http.ResponseWriter, r *http.Request) {
	tracks, _ := s.store.LoadAllTracks()
	analysisMap, _ := s.store.ListTrackAnalysisMap()
	edGenres := s.loadAlbumEditorialGenresMap()
	ratingMap := s.loadTrackRatingsMap()

	genreCnt := make(map[string]int)
	genreDisp := make(map[string]string)
	tagCnt := make(map[string]int)
	tagDisp := make(map[string]string)
	ratedTracks := 0

	addFacet := func(cnt map[string]int, disp map[string]string, val string) {
		val = strings.TrimSpace(val)
		if val == "" || strings.EqualFold(val, "unknown") {
			return
		}
		k := strings.ToLower(val)
		cnt[k]++
		if disp[k] == "" {
			disp[k] = val
		}
	}

	for _, tr := range tracks {
		if tr.Genre != nil {
			addFacet(genreCnt, genreDisp, *tr.Genre)
		}
		albKey := strings.ToLower(strings.TrimSpace(tr.Album))
		for _, g := range edGenres[albKey] {
			addFacet(genreCnt, genreDisp, g)
		}
		if tr.Year != nil && *tr.Year >= 1950 {
			addFacet(tagCnt, tagDisp, fmt.Sprintf("%ds", (*tr.Year/10)*10))
		}
		if an, ok := analysisMap[tr.TrackKey]; ok {
			if an.Genre != "" {
				addFacet(genreCnt, genreDisp, an.Genre)
			}
			for _, t := range an.Tags {
				addFacet(tagCnt, tagDisp, t)
			}
		}
		if lookupTrackRating(ratingMap, tr) > 0 {
			ratedTracks++
		}
	}

	topKeys := func(cnt map[string]int, disp map[string]string, limit int) []string {
		type kv struct {
			name string
			n    int
		}
		var arr []kv
		for k, n := range cnt {
			arr = append(arr, kv{name: disp[k], n: n})
		}
		sort.Slice(arr, func(i, j int) bool {
			if arr[i].n != arr[j].n {
				return arr[i].n > arr[j].n
			}
			return arr[i].name < arr[j].name
		})
		if len(arr) > limit {
			arr = arr[:limit]
		}
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			out = append(out, item.name)
		}
		return out
	}

	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"genres":        topKeys(genreCnt, genreDisp, 40),
		"tags":          topKeys(tagCnt, tagDisp, 36),
		"ratedTracks":   ratedTracks,
		"analyzedCount": len(analysisMap),
		"totalTracks":   len(tracks),
	})
}

func (s *Server) loadTrackRatingsMap() map[string]int {
	stats, err := s.store.ListTrackStats()
	if err != nil {
		return nil
	}
	m := make(map[string]int, len(stats))
	for _, st := range stats {
		if st.UserRating != nil && *st.UserRating > 0 {
			m[st.TrackID] = *st.UserRating
		}
	}
	return m
}

func (s *Server) loadAlbumRatingsMap() map[string]int {
	albs, err := s.store.ListAlbumRatings(false)
	if err != nil {
		return nil
	}
	m := make(map[string]int, len(albs))
	for _, a := range albs {
		if a.Rating > 0 {
			m[strings.ToLower(a.AlbumKey)] = a.Rating
		}
	}
	return m
}

func (s *Server) loadAlbumEditorialGenresMap() map[string][]string {
	eds, err := s.store.ListAlbumEditorial()
	if err != nil {
		return nil
	}
	m := make(map[string][]string, len(eds))
	for _, ed := range eds {
		if len(ed.Genres) > 0 {
			m[strings.ToLower(ed.AlbumKey)] = ed.Genres
		}
	}
	return m
}

func lookupTrackRating(ratingMap map[string]int, tr db.TrackRecord) int {
	if ratingMap == nil {
		return 0
	}
	if r := ratingMap[tr.EffectiveID()]; r > 0 {
		return r
	}
	if r := ratingMap[tr.TrackKey]; r > 0 {
		return r
	}
	if tr.LegacyID != "" {
		if r := ratingMap[tr.LegacyID]; r > 0 {
			return r
		}
	}
	return 0
}

func enrichTrackDTO(dto *webTrackDTO, tr db.TrackRecord, ratingMap map[string]int, analysisMap map[string]db.TrackAnalysis, edGenresByAlb map[string][]string) {
	dto.UserRating = lookupTrackRating(ratingMap, tr)
	seen := make(map[string]bool)
	addTag := func(t string) {
		t = strings.TrimSpace(t)
		if t == "" {
			return
		}
		k := strings.ToLower(t)
		if !seen[k] {
			seen[k] = true
			dto.Tags = append(dto.Tags, t)
		}
	}
	if an, ok := analysisMap[tr.TrackKey]; ok {
		dto.BPM = an.BPM
		dto.VocalStartSec = an.VocalStartSec
		dto.VocalEndSec = an.VocalEndSec
		dto.Energy = an.Energy
		if (dto.Genre == nil || *dto.Genre == "") && an.Genre != "" {
			g := an.Genre
			dto.Genre = &g
		}
		for _, t := range an.Tags {
			addTag(t)
		}
	}
	if edGenresByAlb != nil {
		for _, g := range edGenresByAlb[dto.AlbumKey] {
			if dto.Genre == nil || *dto.Genre == "" {
				cp := g
				dto.Genre = &cp
			}
			addTag(g)
		}
	}
	if dto.Year != nil && *dto.Year >= 1950 {
		addTag(fmt.Sprintf("%ds", (*dto.Year/10)*10))
	}
}

func trackMatchesGenre(dto webTrackDTO, needle string) bool {
	if dto.Genre != nil && strings.Contains(strings.ToLower(*dto.Genre), needle) {
		return true
	}
	for _, t := range dto.Tags {
		if strings.Contains(strings.ToLower(t), needle) {
			return true
		}
	}
	return false
}

func trackMatchesTag(dto webTrackDTO, needle string) bool {
	for _, t := range dto.Tags {
		if strings.Contains(strings.ToLower(t), needle) {
			return true
		}
	}
	return false
}

// handleWebRandomQueue returns a fresh random queue of tracks (optionally honoring active stars/genre/tag/bpm/query filters).
func (s *Server) handleWebRandomQueue(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	count, _ := strconv.Atoi(q.Get("count"))
	if count <= 0 {
		count = 35
	}
	if count > 150 {
		count = 150
	}
	query := strings.TrimSpace(q.Get("q"))
	minRating, _ := strconv.Atoi(q.Get("minRating"))
	avoidMaxRating, _ := strconv.Atoi(q.Get("avoidMaxRating"))
	genreFilter := strings.ToLower(strings.TrimSpace(q.Get("genre")))
	tagFilter := strings.ToLower(strings.TrimSpace(q.Get("tag")))
	eraFilter := strings.ToLower(strings.TrimSpace(q.Get("era")))
	bpmMin, _ := strconv.ParseFloat(q.Get("bpmMin"), 64)
	bpmMax, _ := strconv.ParseFloat(q.Get("bpmMax"), 64)

	tracks, err := s.store.LoadAllTracks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	ratingMap := s.loadTrackRatingsMap()
	analysisMap, _ := s.store.ListTrackAnalysisMap()
	edGenresByAlb := s.loadAlbumEditorialGenresMap()
	tokens := parseSimpleTokens(query)

	var pool []webTrackDTO
	for _, tr := range tracks {
		dto := toWebTrack(tr)
		enrichTrackDTO(&dto, tr, ratingMap, analysisMap, edGenresByAlb)
		if minRating > 0 && dto.UserRating < minRating {
			continue
		}
		if avoidMaxRating > 0 && dto.UserRating > 0 && dto.UserRating <= avoidMaxRating {
			continue
		}
		if genreFilter != "" && !matchesAnyCSV(genreFilter, func(g string) bool { return trackMatchesGenre(dto, g) }) {
			continue
		}
		if tagFilter != "" && !matchesAnyCSV(tagFilter, func(t string) bool { return trackMatchesTag(dto, t) }) {
			continue
		}
		if eraFilter != "" && !yearMatchesEraCSV(dto.Year, "", eraFilter) {
			continue
		}
		if bpmMin > 0 && (dto.BPM <= 0 || dto.BPM < bpmMin) {
			continue
		}
		if bpmMax > 0 && (dto.BPM <= 0 || dto.BPM > bpmMax) {
			continue
		}
		if len(tokens) > 0 && !matchesWebTokens(dto, "", tokens) {
			continue
		}
		pool = append(pool, dto)
	}

	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	if len(pool) > count {
		pool = pool[:count]
	}
	if pool == nil {
		pool = []webTrackDTO{}
	}
	writeMaybeGzipJSON(w, r, http.StatusOK, map[string]any{
		"count":  len(pool),
		"tracks": pool,
	})
}

func albumReleaseYear(alb webAlbumDTO) int {
	if alb.Year != nil && *alb.Year > 0 {
		return *alb.Year
	}
	if len(alb.ReleaseDate) >= 4 {
		if y, err := strconv.Atoi(alb.ReleaseDate[:4]); err == nil && y > 1800 {
			return y
		}
	}
	return 0
}

func matchesAnyCSV(csv string, fn func(string) bool) bool {
	for _, part := range strings.Split(csv, ",") {
		p := strings.ToLower(strings.TrimSpace(part))
		if p != "" && fn(p) {
			return true
		}
	}
	return false
}

func yearMatchesEraCSV(year *int, releaseDate string, eraCSV string) bool {
	yr := 0
	if year != nil && *year > 0 {
		yr = *year
	} else if len(releaseDate) >= 4 {
		if y, err := strconv.Atoi(releaseDate[:4]); err == nil && y > 1800 {
			yr = y
		}
	}
	if yr <= 0 {
		return false
	}
	decade := fmt.Sprintf("%ds", (yr/10)*10)
	return matchesAnyCSV(eraCSV, func(e string) bool {
		return strings.EqualFold(e, decade)
	})
}
