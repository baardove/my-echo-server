package backfill

import (
	"path/filepath"
	"testing"

	"github.com/baardove/my-echo-server/internal/config"
	"github.com/baardove/my-echo-server/internal/db"
)

func TestParseBackfillJSONAndApplyPreservesExistingAudioAnalysis(t *testing.T) {
	raw := "```json\n" + `[
		{
			"id": "alb:snowfall",
			"releaseDate": "1998-11-15",
			"releaseYear": 1998,
			"genre": "Christmas",
			"genres": ["Christmas", "Pop"],
			"category": "Holiday",
			"tags": ["Christmas", "Winter", "Warm", "Norwegian"],
			"label": "Kirkelig Kulturverksted",
			"description": "A beloved Norwegian holiday album blending acoustic folk and classic winter melodies."
		}
	]` + "\n```"

	items, err := ParseBackfillJSON(raw)
	if err != nil {
		t.Fatalf("ParseBackfillJSON failed: %v", err)
	}
	if len(items) != 1 || items[0].ReleaseYear != 1998 || items[0].Category != "Holiday" {
		t.Fatalf("unexpected parsed items: %+v", items)
	}

	store, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	// Seed a track row in SQLite + an existing audio analysis with BPM & loudness so we verify
	// backfill populates missing year/genre on tracks AND NEVER clobbers audio DSP fields!
	if err := store.UpsertLibrary("lib1", "MUSIC", "/music", 4); err != nil {
		t.Fatalf("seed lib: %v", err)
	}
	seedTr := db.TrackRecord{TrackKey: "k1", LibraryID: "lib1", RelativePath: "Sissel/Snowfall/01.flac", Title: "Silent Night", Artist: "Sissel", Album: "Snowfall"}
	if _, err := store.CommitScanDelta("lib1", []db.TrackRecord{seedTr}, nil, 1, 1000, 10); err != nil {
		t.Fatalf("seed track: %v", err)
	}
	if err := store.UpsertTrackAnalysis(db.TrackAnalysis{
		TrackKey:      "k1",
		BPM:           122,
		VocalStartSec: 8.5,
		LoudnessDB:    -14.2,
		NormGainDB:    0.2,
		Tags:          []string{"ExistingTag"},
		Version:       2,
	}); err != nil {
		t.Fatalf("seed analysis: %v", err)
	}

	w := NewWorker(&config.Config{}, store, nil)
	batch := []workUnit{
		{
			ID:       "alb:snowfall",
			Kind:     "album",
			Artist:   "Sissel",
			Album:    "Snowfall",
			albumKey: "snowfall",
			tracks:   []db.TrackRecord{seedTr},
		},
	}
	byID := map[string]BackfillItem{"alb:snowfall": items[0]}
	edMap := map[string]db.AlbumEditorial{}
	anMap, _ := store.ListTrackAnalysisMap()

	updT, updA := w.applyBatchResults(batch, byID, edMap, anMap)
	if updA != 1 || updT != 1 {
		t.Fatalf("expected album + track updates, got updA=%d updT=%d", updA, updT)
	}

	gotTr, err := store.FindTrackByID("k1")
	if err != nil || gotTr.Year == nil || *gotTr.Year != 1998 || gotTr.Genre == nil || *gotTr.Genre != "Christmas" {
		t.Fatalf("expected backfilled track year=1998 genre=Christmas, got %+v (err=%v)", gotTr, err)
	}

	edList, _ := store.ListAlbumEditorial()
	if len(edList) != 1 || edList[0].AlbumSummary == nil || *edList[0].AlbumSummary == "" {
		t.Fatalf("expected backfilled album editorial summary, got %+v", edList)
	}
	gotAn, ok := store.GetTrackAnalysis("k1")
	if !ok {
		t.Fatalf("missing track analysis after backfill")
	}
	if gotAn.BPM != 122 || gotAn.VocalStartSec != 8.5 || gotAn.Version != 2 {
		t.Fatalf("backfill clobbered existing DSP analysis: %+v", gotAn)
	}
	if gotAn.Genre != "Christmas" || len(gotAn.Tags) < 4 {
		t.Fatalf("expected merged genre & tags, got %+v", gotAn)
	}
}
