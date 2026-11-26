package spotify

import (
	"testing"

	"github.com/baardove/my-echo-server/internal/db"
)

func TestParsePlaylistIDVariants(t *testing.T) {
	cases := map[string]string{
		"https://open.spotify.com/playlist/1L9eGmQJXFRN8oL22QtHkA?si=a81025ef1e6446f7": "1L9eGmQJXFRN8oL22QtHkA",
		"https://open.spotify.com/embed/playlist/1L9eGmQJXFRN8oL22QtHkA":               "1L9eGmQJXFRN8oL22QtHkA",
		"https://open.spotify.com/intl-no/playlist/1L9eGmQJXFRN8oL22QtHkA":             "1L9eGmQJXFRN8oL22QtHkA",
		"https://open.spotify.com/user/bardove/playlist/1L9eGmQJXFRN8oL22QtHkA":        "1L9eGmQJXFRN8oL22QtHkA",
		"spotify:playlist:1L9eGmQJXFRN8oL22QtHkA":                                      "1L9eGmQJXFRN8oL22QtHkA",
		"1L9eGmQJXFRN8oL22QtHkA":                                                       "1L9eGmQJXFRN8oL22QtHkA",
		"https://open.spotify.com/track/1x1PcpocUC1nUIs24G8Juf":                        "",
		"invalid": "",
	}
	for in, want := range cases {
		if got := ParsePlaylistID(in); got != want {
			t.Errorf("ParsePlaylistID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseEmbedMatchRetainAndResolveMissing(t *testing.T) {
	embedHTML := `<!doctype html><html><body>
	<script id="__NEXT_DATA__" type="application/json">{
	  "props":{"pageProps":{"state":{"data":{"entity":{
	    "type":"playlist","name":"!2025","subtitle":"Bård Ove Myhr",
	    "coverArt":{"sources":[{"url":"https://mosaic.scdn.co/640/test"}]},
	    "trackList":[
	      {"uri":"spotify:track:sp1","title":"Téir Abhaile Riu","subtitle":"Celtic Woman","duration":241000},
	      {"uri":"spotify:track:sp2","title":"Isle Of Hope, Isle Of Tears","subtitle":"Celtic Woman","duration":302000},
	      {"uri":"spotify:track:sp3","title":"May It Be - Acapella Version","subtitle":"Jared Halley,\u00a0Rachel Hardy","duration":196000}
	    ]
	  }}}}}
	}</script></body></html>`

	payload, err := ParseEmbedHTML(embedHTML, "1L9eGmQJXFRN8oL22QtHkA", "https://open.spotify.com/playlist/1L9eGmQJXFRN8oL22QtHkA")
	if err != nil {
		t.Fatalf("ParseEmbedHTML error: %v", err)
	}
	if payload.Name != "!2025" || len(payload.Tracks) != 3 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload.Tracks[2].Artist != "Jared Halley, Rachel Hardy" {
		t.Fatalf("expected NBSP normalized artist, got %q", payload.Tracks[2].Artist)
	}

	library := []db.TrackRecord{
		{TrackKey: "k1", LegacyID: "local-1", Title: "Teir Abhaile Riu", Artist: "Celtic Woman", Album: "Decade", Format: "flac", Duration: 241},
		{TrackKey: "k3", LegacyID: "local-3", Title: "May It Be", Artist: "Rachel Hardy", Album: "Covers", Format: "flac", Duration: 196},
	}

	report := MatchPlaylist(*payload, library)
	if report.MatchedCount != 2 || report.MissingCount != 1 {
		t.Fatalf("expected 2 matched / 1 missing, got %d / %d", report.MatchedCount, report.MissingCount)
	}

	pl := BuildSharedPlaylist(report, "", "")
	if len(pl.TrackIDs) != 3 || pl.TrackIDs[0] != "local-1" || !db.IsMissingTrackID(pl.TrackIDs[1]) || pl.TrackIDs[2] != "local-3" {
		t.Fatalf("unexpected trackIDs order: %+v", pl.TrackIDs)
	}
	if len(pl.PlayableTrackIDs()) != 2 {
		t.Fatalf("PlayableTrackIDs should skip missing tracks, got %+v", pl.PlayableTrackIDs())
	}

	// Simulate Lidarr downloading the missing track -> verify in-place upgrade at index 1
	library = append(library, db.TrackRecord{
		TrackKey: "k2", LegacyID: "local-2", Title: "Isle Of Hope, Isle Of Tears", Artist: "Celtic Woman", Album: "Voices of Angels", Format: "flac", Duration: 302,
	})
	resolved, count := ResolveMissingInPlaylist(pl, library)
	if count != 1 {
		t.Fatalf("expected 1 resolved track, got %d", count)
	}
	if resolved.TrackIDs[0] != "local-1" || resolved.TrackIDs[1] != "local-2" || resolved.TrackIDs[2] != "local-3" {
		t.Fatalf("expected exact in-place order, got %+v", resolved.TrackIDs)
	}
	if len(resolved.MissingTracks) != 0 {
		t.Fatalf("expected 0 remaining missing tracks, got %d", len(resolved.MissingTracks))
	}
}
