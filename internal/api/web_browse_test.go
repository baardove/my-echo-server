package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/baardove/my-echo-server/internal/db"
)

func TestArtistFieldMatchesKey(t *testing.T) {
	cases := []struct {
		raw  string
		key  string
		want bool
	}{
		{"Calvin Harris", "calvin harris", true},
		{"Calvin Harris & Dua Lipa", "calvin harris", true},
		{"Calvin Harris & Dua Lipa", "dua lipa", true},
		{"Calvin Harris feat. Rihanna", "rihanna", true},
		{"Rihanna (ft. Calvin Harris)", "calvin harris", true},
		{"Calvin Harris", "alvin", false},
		{"Various Artists", "calvin harris", false},
		{"", "calvin harris", false},
	}
	for _, tc := range cases {
		if got := artistFieldMatchesKey(tc.raw, tc.key); got != tc.want {
			t.Errorf("artistFieldMatchesKey(%q, %q) = %v, want %v", tc.raw, tc.key, got, tc.want)
		}
	}
}

func TestEnrichTrackDTOIncludesStatsAndPopularity(t *testing.T) {
	rating := 4
	pop := 84
	onlinePlays := 1250000
	listeners := 420000
	src := "lastfm"
	tr := db.TrackRecord{
		TrackKey: "trk-1",
		LegacyID: "leg-1",
		Title:    "Jump",
		Artist:   "Van Halen",
		Album:    "1984",
		Format:   "flac",
	}
	dto := toWebTrack(tr)
	statsMap := map[string]db.SharedTrackStats{
		"leg-1": {
			TrackID:          "leg-1",
			UserPlayCount:    17,
			UserRating:       &rating,
			OnlinePopularity: &pop,
			OnlinePlayCount:  &onlinePlays,
			OnlineListeners:  &listeners,
			OnlineSource:     &src,
		},
	}
	enrichTrackDTO(&dto, tr, statsMap, nil, nil)

	if dto.UserRating != 4 {
		t.Fatalf("UserRating = %d, want 4", dto.UserRating)
	}
	if dto.UserPlayCount != 17 {
		t.Fatalf("UserPlayCount = %d, want 17", dto.UserPlayCount)
	}
	if dto.OnlinePopularity != 84 {
		t.Fatalf("OnlinePopularity = %d, want 84", dto.OnlinePopularity)
	}
	if dto.OnlinePlayCount == nil || *dto.OnlinePlayCount != onlinePlays {
		t.Fatalf("OnlinePlayCount = %v, want %d", dto.OnlinePlayCount, onlinePlays)
	}
	if dto.OnlineListeners == nil || *dto.OnlineListeners != listeners {
		t.Fatalf("OnlineListeners = %v, want %d", dto.OnlineListeners, listeners)
	}
	if dto.OnlineSource == nil || *dto.OnlineSource != src {
		t.Fatalf("OnlineSource = %v, want %q", dto.OnlineSource, src)
	}
}

func TestHandleWebPlaylistReorder(t *testing.T) {
	store, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	pl := db.SharedPlaylist{
		ID:       "pl-1",
		Name:     "Mixtape",
		TrackIDs: []string{"t1", "missing:m1", "t2", "t3"},
		MissingTracks: []db.MissingTrack{
			{ID: "missing:m1", Title: "Missing Song", Artist: "Mystery"},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.SavePlaylist(pl); err != nil {
		t.Fatalf("SavePlaylist: %v", err)
	}

	srv := &Server{store: store}
	sendReorder := func(from, to int) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{
			"id":        "pl-1",
			"action":    "reorder",
			"fromIndex": from,
			"toIndex":   to,
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v2/web/playlists/update", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		srv.handleWebPlaylistUpdate(rr, req)
		return rr
	}

	// Move t1 (0) -> index 2 => [missing:m1, t2, t1, t3]
	if rr := sendReorder(0, 2); rr.Code != http.StatusOK {
		t.Fatalf("reorder forward status = %d, body = %s", rr.Code, rr.Body.String())
	}
	got, err := store.GetPlaylist("pl-1")
	if err != nil || got == nil {
		t.Fatalf("GetPlaylist: %v", err)
	}
	want1 := []string{"missing:m1", "t2", "t1", "t3"}
	for i := range want1 {
		if got.TrackIDs[i] != want1[i] {
			t.Fatalf("after forward reorder got %v, want %v", got.TrackIDs, want1)
		}
	}

	// Move t3 (3) -> index 0 => [t3, missing:m1, t2, t1]
	if rr := sendReorder(3, 0); rr.Code != http.StatusOK {
		t.Fatalf("reorder backward status = %d, body = %s", rr.Code, rr.Body.String())
	}
	got, _ = store.GetPlaylist("pl-1")
	want2 := []string{"t3", "missing:m1", "t2", "t1"}
	for i := range want2 {
		if got.TrackIDs[i] != want2[i] {
			t.Fatalf("after backward reorder got %v, want %v", got.TrackIDs, want2)
		}
	}

	// Out-of-bounds index should return 400
	if rr := sendReorder(-1, 2); rr.Code != http.StatusBadRequest {
		t.Fatalf("out-of-bounds reorder status = %d, want 400", rr.Code)
	}
	if rr := sendReorder(0, 99); rr.Code != http.StatusBadRequest {
		t.Fatalf("out-of-bounds toIndex status = %d, want 400", rr.Code)
	}
}
