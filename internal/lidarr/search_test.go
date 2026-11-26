package lidarr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/baardove/my-echo-server/internal/config"
)

func TestMergeAndRankAlbumResults(t *testing.T) {
	direct := []AlbumSearchResult{
		{ForeignAlbumID: "mb-single", Title: "One Kiss", ArtistName: "Calvin Harris", AlbumType: "Single", ReleaseYear: 2018},
		{ForeignAlbumID: "mb-18m", Title: "18 Months", ArtistName: "Calvin Harris", AlbumType: "Album", ReleaseYear: 2012, RawLookup: json.RawMessage(`{"title":"18 Months"}`)},
	}
	artistAlbums := []AlbumSearchResult{
		{ForeignAlbumID: "deezer-album:1", Title: "18 Months", ArtistName: "Calvin Harris", AlbumType: "Album", ReleaseYear: 2012, CoverURL: "https://img/18m.jpg"},
		{ForeignAlbumID: "mb-motion", Title: "Motion", ArtistName: "Calvin Harris", AlbumType: "Album", ReleaseYear: 2014, IsInLidarr: true, LidarrID: 42},
	}

	got := MergeAndRankAlbumResults("Calvin Harris", direct, artistAlbums, []string{"Calvin Harris"})
	if len(got) != 3 {
		t.Fatalf("expected 3 deduplicated albums, got %d", len(got))
	}
	// Studio album already in Lidarr should outrank single
	if got[0].Title != "Motion" {
		t.Fatalf("expected 'Motion' first, got %q", got[0].Title)
	}
	// Deduplicated 18 Months should preserve RawLookup from direct AND coverURL from artistAlbums
	var found18 *AlbumSearchResult
	for i := range got {
		if got[i].Title == "18 Months" {
			found18 = &got[i]
		}
	}
	if found18 == nil || found18.CoverURL != "https://img/18m.jpg" || len(found18.RawLookup) == 0 {
		t.Fatalf("expected merged metadata on 18 Months, got %+v", found18)
	}
}

func TestMergeAndRankTrackResults(t *testing.T) {
	top := []TrackMatch{
		{ID: "1", Title: "Summer", ArtistName: "Calvin Harris", AlbumTitle: "Motion", Popularity: 88},
		{ID: "2", Title: "Feel So Close", ArtistName: "Calvin Harris", AlbumTitle: "18 Months", Popularity: 85},
	}
	direct := []TrackMatch{
		{ID: "1", Title: "Summer", ArtistName: "Calvin Harris", AlbumTitle: "Motion", Popularity: 88}, // dup ID
		{ID: "3", Title: "Summer", ArtistName: "Calvin Harris", AlbumTitle: "Motion (Deluxe)", Popularity: 70}, // dup artist+title
		{ID: "4", Title: "Other Song", ArtistName: "Someone Else", AlbumTitle: "Calvin Harris Tribute", Popularity: 60},
	}
	got := MergeAndRankTrackResults("Calvin Harris", direct, top, nil, "Calvin Harris")
	if len(got) != 3 {
		t.Fatalf("expected 3 deduplicated tracks, got %d", len(got))
	}
	if got[0].Title != "Summer" || got[1].Title != "Feel So Close" {
		t.Fatalf("expected artist top tracks ranked first, got %+v", got)
	}
}

func TestOrderAlbum_NewAndExisting(t *testing.T) {
	var postedAlbum map[string]any
	var monitoredIDs []int
	var searchedIDs []int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "secret-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/album/lookup":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"foreignAlbumId": "11111111-1111-1111-1111-111111111111",
				"title":          "Motion",
				"artist":         map[string]any{"artistName": "Calvin Harris", "foreignArtistId": "22222222-2222-2222-2222-222222222222"},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/album":
			_ = json.NewEncoder(w).Encode([]any{})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/rootfolder":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"path": "/music"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/qualityprofile":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 2, "name": "Lossless"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/metadataprofile":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "name": "Standard"}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/album":
			_ = json.NewDecoder(r.Body).Decode(&postedAlbum)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 77})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/album/monitor":
			var body struct {
				AlbumIDs []int `json:"albumIds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			monitoredIDs = body.AlbumIDs
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/command":
			var body struct {
				Name     string `json:"name"`
				AlbumIDs []int  `json:"albumIds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Name == "AlbumSearch" {
				searchedIDs = body.AlbumIDs
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfg := &config.Config{LidarrURL: srv.URL, LidarrAPIKey: "secret-key"}
	mgr := NewManager(cfg, nil, nil)

	// 1. Order brand-new album (resolves via lookup + adds with artist monitor=none safeguard)
	res, err := mgr.OrderAlbum(context.Background(), OrderAlbumRequest{
		Title:      "Motion",
		ArtistName: "Calvin Harris",
	})
	if err != nil {
		t.Fatalf("OrderAlbum new failed: %v", err)
	}
	if res.AlreadyExisted || res.LidarrAlbumID != 77 || !res.SearchTriggered {
		t.Fatalf("unexpected OrderResult: %+v", res)
	}
	artistMap, _ := postedAlbum["artist"].(map[string]any)
	if artistMap["rootFolderPath"] != "/music" {
		t.Fatalf("expected rootFolderPath /music, got %+v", artistMap)
	}
	addOpts, _ := artistMap["addOptions"].(map[string]any)
	if addOpts["monitor"] != "none" || addOpts["searchForMissingAlbums"] != false {
		t.Fatalf("expected nested artist monitor=none safeguard, got %+v", addOpts)
	}

	// 2. Order album already in Lidarr (monitors + triggers AlbumSearch)
	res2, err := mgr.OrderAlbum(context.Background(), OrderAlbumRequest{
		LidarrID:   99,
		Title:      "18 Months",
		ArtistName: "Calvin Harris",
	})
	if err != nil {
		t.Fatalf("OrderAlbum existing failed: %v", err)
	}
	if !res2.AlreadyExisted || len(monitoredIDs) != 1 || monitoredIDs[0] != 99 || len(searchedIDs) != 1 || searchedIDs[0] != 99 {
		t.Fatalf("expected monitor & AlbumSearch for id 99, got res=%+v mon=%v search=%v", res2, monitoredIDs, searchedIDs)
	}
	if !strings.Contains(res2.Message, "18 Months") {
		t.Fatalf("unexpected message: %q", res2.Message)
	}
}
