package editorial

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baardove/my-echo-server/internal/artwork"
	"github.com/baardove/my-echo-server/internal/db"
)

func makeTestPNG(t *testing.T, r, g, b uint8) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 32, 32))
	c := color.RGBA{R: r, G: g, B: b, A: 255}
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			img.SetRGBA(x, y, c)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func TestFetchMissingAlbumArtworkDeezerITunesAndPreserveExisting(t *testing.T) {
	deezerCover := makeTestPNG(t, 20, 220, 140)
	itunesCover := makeTestPNG(t, 220, 60, 180)
	singleCover := makeTestPNG(t, 60, 140, 240)

	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/search/album":
			q := strings.ToLower(r.URL.Query().Get("q"))
			if strings.Contains(q, "discovery") {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"data":[{"id":101,"title":"Discovery","cover_xl":"%s/img/deezer.png","artist":{"name":"Daft Punk"}}]}`, srvURL)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[]}`))
		case "/search/track":
			q := strings.ToLower(r.URL.Query().Get("q"))
			if strings.Contains(q, "midnight city") {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"data":[{"title":"Midnight City","artist":{"name":"M83"},"album":{"cover_big":"%s/img/single.png"}}]}`, srvURL)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[]}`))
		case "/search":
			q := strings.ToLower(r.URL.Query().Get("term"))
			if strings.Contains(q, "melodrama") {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"results":[{"artistName":"Lorde","collectionName":"Melodrama","artworkUrl100":"%s/img/itunes-100x100bb.png"}]}`, srvURL)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":[]}`))
		case "/img/deezer.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(deezerCover)
		case "/img/itunes-600x600bb.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(itunesCover)
		case "/img/single.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(singleCover)
		case "/img/invalid.html":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html><body>not an image at all, just an error page with plenty of bytes to pass length check</body></html>"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	srvURL = srv.URL

	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	artCache, err := artwork.NewCache(filepath.Join(tmpDir, "art"))
	if err != nil {
		t.Fatalf("init art cache: %v", err)
	}

	if err := store.UpsertLibrary("music", "MUSIC", "/music", 4); err != nil {
		t.Fatalf("upsert library: %v", err)
	}

	existingHash := "already_embedded_art_hash"
	tracks := []db.TrackRecord{
		// Track 1: Missing artwork on Discovery -> should fetch from Deezer
		{TrackKey: "t1", LibraryID: "music", RelativePath: "Daft Punk/Discovery/01.flac", Title: "One More Time", Artist: "Daft Punk", AlbumArtist: "Daft Punk", Album: "Discovery", ShardID: 0},
		// Track 2: Already has embedded artwork on Discovery -> must NOT be overwritten
		{TrackKey: "t2", LibraryID: "music", RelativePath: "Daft Punk/Discovery/02.flac", Title: "Aerodynamic", Artist: "Daft Punk", AlbumArtist: "Daft Punk", Album: "Discovery", ArtworkHash: &existingHash, ShardID: 0},
		// Track 3: Missing artwork on Melodrama -> Deezer empty, should fall back to iTunes 600x600
		{TrackKey: "t3", LibraryID: "music", RelativePath: "Lorde/Melodrama/01.flac", Title: "Green Light", Artist: "Lorde", AlbumArtist: "Lorde", Album: "Melodrama", ShardID: 1},
		// Track 4: Single with Unknown Album -> should fall back to Deezer track search
		{TrackKey: "t4", LibraryID: "music", RelativePath: "Singles/Midnight City.mp3", Title: "Midnight City", Artist: "M83", AlbumArtist: "M83", Album: "Unknown Album", ShardID: 2},
	}
	if _, err := store.CommitScanDelta("music", tracks, nil, len(tracks), 4000, 20); err != nil {
		t.Fatalf("seed tracks: %v", err)
	}

	var notified []db.TrackRecord
	f := NewFetcher(store, nil)
	f.deezerBaseURL = srvURL
	f.itunesBaseURL = srvURL
	f.SetArtworkCache(artCache, func(updated []db.TrackRecord) {
		notified = append(notified, updated...)
	})

	// Reject non-image HTML cleanly
	if badHash := f.downloadAndStoreCover(context.Background(), srvURL+"/img/invalid.html"); badHash != "" {
		t.Fatalf("expected empty hash for HTML response, got %q", badHash)
	}

	updated := f.EnsureMissingArtworkForTracks(context.Background(), tracks)
	if len(updated) != 3 {
		t.Fatalf("expected 3 tracks updated with missing artwork, got %d", len(updated))
	}
	if len(notified) != 3 {
		t.Fatalf("expected onTracksUpdated callback with 3 tracks, got %d", len(notified))
	}

	// Verify DB records & artwork thumbnails
	expectedDeezerHash := artwork.Hash(deezerCover)
	expectedITunesHash := artwork.Hash(itunesCover)
	expectedSingleHash := artwork.Hash(singleCover)

	got1, _ := store.FindTrackByID("t1")
	if got1.ArtworkHash == nil || *got1.ArtworkHash != expectedDeezerHash {
		t.Fatalf("t1 expected Deezer hash %s, got %v", expectedDeezerHash, got1.ArtworkHash)
	}
	if _, err := artCache.LoadThumb(expectedDeezerHash); err != nil {
		t.Fatalf("expected cached thumb for Deezer cover: %v", err)
	}

	got2, _ := store.FindTrackByID("t2")
	if got2.ArtworkHash == nil || *got2.ArtworkHash != existingHash {
		t.Fatalf("t2 existing artwork must be preserved, got %v", got2.ArtworkHash)
	}

	got3, _ := store.FindTrackByID("t3")
	if got3.ArtworkHash == nil || *got3.ArtworkHash != expectedITunesHash {
		t.Fatalf("t3 expected iTunes fallback hash %s, got %v", expectedITunesHash, got3.ArtworkHash)
	}

	got4, _ := store.FindTrackByID("t4")
	if got4.ArtworkHash == nil || *got4.ArtworkHash != expectedSingleHash {
		t.Fatalf("t4 expected single cover hash %s, got %v", expectedSingleHash, got4.ArtworkHash)
	}

	// Verify RecordTrackDeltaUpdates bumps library revision and logs upserts for Mac delta sync
	revs, dirtyShards, err := store.RecordTrackDeltaUpdates(updated, 20)
	if err != nil {
		t.Fatalf("RecordTrackDeltaUpdates failed: %v", err)
	}
	if revs["music"] != 2 {
		t.Fatalf("expected bumped revision 2, got %d", revs["music"])
	}
	if len(dirtyShards["music"]) != 3 {
		t.Fatalf("expected 3 dirty shards (0,1,2), got %v", dirtyShards["music"])
	}
	upsertedDelta, _, _, err := store.LoadDeltaSince("music", 1)
	if err != nil || len(upsertedDelta) != 3 {
		t.Fatalf("expected 3 delta upserts since rev 1, got %d (err=%v)", len(upsertedDelta), err)
	}
}
