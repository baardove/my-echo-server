package indexer_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/baardove/my-echo/server/internal/api"
	"github.com/baardove/my-echo/server/internal/artwork"
	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
	"github.com/baardove/my-echo/server/internal/indexer"
	"github.com/baardove/my-echo/server/internal/party"
	"github.com/baardove/my-echo/server/internal/snapshot"
)

func writeMinimalWAV(t *testing.T, path, title, artist, album string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	// Build LIST INFO chunk
	var info bytes.Buffer
	info.WriteString("INFO")
	writeInfoSub := func(id, val string) {
		b := append([]byte(val), 0)
		info.WriteString(id)
		_ = binary.Write(&info, binary.LittleEndian, uint32(len(b)))
		info.Write(b)
		if len(b)%2 == 1 {
			info.WriteByte(0)
		}
	}
	writeInfoSub("INAM", title)
	writeInfoSub("IART", artist)
	writeInfoSub("IPRD", album)

	// fmt chunk (16-bit 44.1kHz stereo PCM)
	var fmtChunk bytes.Buffer
	_ = binary.Write(&fmtChunk, binary.LittleEndian, uint16(1))       // PCM
	_ = binary.Write(&fmtChunk, binary.LittleEndian, uint16(2))       // stereo
	_ = binary.Write(&fmtChunk, binary.LittleEndian, uint32(44100))   // sample rate
	_ = binary.Write(&fmtChunk, binary.LittleEndian, uint32(176400))  // byte rate
	_ = binary.Write(&fmtChunk, binary.LittleEndian, uint16(4))       // block align
	_ = binary.Write(&fmtChunk, binary.LittleEndian, uint16(16))      // bits per sample

	// 1 second of silence
	pcm := make([]byte, 176400)

	var body bytes.Buffer
	body.WriteString("WAVE")
	body.WriteString("fmt ")
	_ = binary.Write(&body, binary.LittleEndian, uint32(fmtChunk.Len()))
	body.Write(fmtChunk.Bytes())
	body.WriteString("LIST")
	_ = binary.Write(&body, binary.LittleEndian, uint32(info.Len()))
	body.Write(info.Bytes())
	body.WriteString("data")
	_ = binary.Write(&body, binary.LittleEndian, uint32(len(pcm)))
	body.Write(pcm)

	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(body.Len()))
	buf.Write(body.Bytes())

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestIndexerShardsDeltasAndAPI(t *testing.T) {
	tmp := t.TempDir()
	musicDir := filepath.Join(tmp, "music")
	cfgDir := filepath.Join(tmp, "config")

	writeMinimalWAV(t, filepath.Join(musicDir, "Pink Floyd", "Dark Side", "04 - Time.wav"), "Time", "Pink Floyd", "Dark Side")
	writeMinimalWAV(t, filepath.Join(musicDir, "Daft Punk", "Discovery", "01 - One More Time.wav"), "One More Time", "Daft Punk", "Discovery")

	cfg := &config.Config{
		ConfigDir:      cfgDir,
		ServerName:     "Test Echo Server",
		PairingPIN:     "654321",
		APIPort:        48721,
		PartyHTTPPort:  53333,
		PartyHTTPSPort: 53334,
		ShardCount:     4,
		MaxDeltaRevs:   50,
		Libraries: []config.LibraryConfig{
			{
				ID:              "music",
				Name:            "MUSIC",
				Path:            musicDir,
				LegacyMacPrefix: "/Volumes/Media/MUSIC",
				ShardCount:      4,
			},
		},
	}

	store, err := db.Open(cfg.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	artCache, err := artwork.NewCache(cfg.ArtworkDir())
	if err != nil {
		t.Fatal(err)
	}
	snap, err := snapshot.NewBuilder(cfg.ShardsDir(), store)
	if err != nil {
		t.Fatal(err)
	}
	partyEng := party.NewEngine(cfg, store, artCache)
	idx := indexer.New(cfg, store, artCache, snap)
	idx.SetOnChanged(func() { partyEng.ReloadCatalog() })

	// 1. Initial scan
	sum1, err := idx.ScanLibrary(context.Background(), cfg.Libraries[0], "")
	if err != nil {
		t.Fatal(err)
	}
	if sum1.Added != 2 || sum1.NewRevision != 1 {
		t.Fatalf("expected 2 added rev 1, got +%d rev %d", sum1.Added, sum1.NewRevision)
	}

	// Verify StableTrackID parity for LegacyMacPrefix
	expectedLegacyID := indexer.StableTrackID("/Volumes/Media/MUSIC/Pink Floyd/Dark Side/04 - Time.wav")
	tr, err := store.FindTrackByID(expectedLegacyID)
	if err != nil || tr.Title != "Time" {
		t.Fatalf("legacy ID lookup failed: err=%v tr=%+v", err, tr)
	}

	// 2. Zero-change rescan should keep revision == 1
	sum2, err := idx.ScanLibrary(context.Background(), cfg.Libraries[0], "")
	if err != nil {
		t.Fatal(err)
	}
	if sum2.Added != 0 || sum2.Updated != 0 || sum2.Removed != 0 || sum2.NewRevision != 1 {
		t.Fatalf("expected zero-change rev 1, got %+v", sum2)
	}

	// 3. Add a 3rd track and delete 1st track -> revision 2 delta
	writeMinimalWAV(t, filepath.Join(musicDir, "Massive Attack", "Mezzanine", "03 - Teardrop.wav"), "Teardrop", "Massive Attack", "Mezzanine")
	_ = os.Remove(filepath.Join(musicDir, "Daft Punk", "Discovery", "01 - One More Time.wav"))

	sum3, err := idx.ScanLibrary(context.Background(), cfg.Libraries[0], "")
	if err != nil {
		t.Fatal(err)
	}
	if sum3.Added != 1 || sum3.Removed != 1 || sum3.NewRevision != 2 {
		t.Fatalf("expected +1 -1 rev 2, got %+v", sum3)
	}

	// 4. Test HTTP API: Manifest, Delta, Raw Gzip Shard SHA-256 verification, Catalog Full, and Party Mode
	srv := api.New(cfg, store, artCache, snap, idx, partyEng, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Auth failure check
	respUnauth, _ := http.Get(ts.URL + "/api/v2/index/manifest?libraryId=music")
	if respUnauth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without PIN, got %d", respUnauth.StatusCode)
	}
	respUnauth.Body.Close()

	// Manifest check
	req, _ := http.NewRequest("GET", ts.URL+"/api/v2/index/manifest?libraryId=music", nil)
	req.Header.Set("X-Echo-Token", "654321")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("manifest failed: err=%v status=%v", err, resp)
	}
	var manifest struct {
		Revision   int            `json:"revision"`
		TrackCount int            `json:"trackCount"`
		Shards     []db.ShardMeta `json:"shards"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&manifest)
	resp.Body.Close()
	if manifest.Revision != 2 || manifest.TrackCount != 2 || len(manifest.Shards) != 4 {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}

	// Verify raw gzip shard matches advertised SHA-256 and decodes NDJSON cleanly
	for _, sh := range manifest.Shards {
		sreq, _ := http.NewRequest("GET", ts.URL+"/api/v2/index/shards/music/"+string(rune('0'+sh.ShardID))+"?raw=1", nil)
		sreq.Header.Set("X-Echo-Token", "654321")
		sresp, err := http.DefaultClient.Do(sreq)
		if err != nil || sresp.StatusCode != 200 {
			t.Fatalf("shard %d failed: %v", sh.ShardID, err)
		}
		rawGZ, _ := io.ReadAll(sresp.Body)
		sresp.Body.Close()
		gotSum := sha256.Sum256(rawGZ)
		if hex.EncodeToString(gotSum[:]) != sh.SHA256 {
			t.Fatalf("shard %d sha256 mismatch", sh.ShardID)
		}
		gzr, err := gzip.NewReader(bytes.NewReader(rawGZ))
		if err != nil {
			t.Fatalf("gunzip shard %d failed: %v", sh.ShardID, err)
		}
		_, _ = io.ReadAll(gzr)
		_ = gzr.Close()
	}

	// Delta check from rev 1 -> rev 2
	dreq, _ := http.NewRequest("GET", ts.URL+"/api/v2/index/delta?libraryId=music&since=1", nil)
	dreq.Header.Set("X-Echo-Token", "654321")
	dresp, err := http.DefaultClient.Do(dreq)
	if err != nil || dresp.StatusCode != 200 {
		t.Fatalf("delta failed: %v", err)
	}
	var delta struct {
		ToRevision           int              `json:"toRevision"`
		RequiresShardSync    bool             `json:"requiresShardSync"`
		Upserted             []db.TrackRecord `json:"upserted"`
		RemovedRelativePaths []string         `json:"removedRelativePaths"`
	}
	_ = json.NewDecoder(dresp.Body).Decode(&delta)
	dresp.Body.Close()
	if delta.ToRevision != 2 || delta.RequiresShardSync || len(delta.Upserted) != 1 || len(delta.RemovedRelativePaths) != 1 {
		t.Fatalf("unexpected delta: %+v", delta)
	}
	if delta.Upserted[0].Title != "Teardrop" {
		t.Fatalf("expected Teardrop in delta, got %s", delta.Upserted[0].Title)
	}

	// Party Mode start + guest track add + queue check
	partyEng.StartSession("apple-tv-1", "Living Room TV")
	addBody := bytes.NewBufferString(`{"trackId":"` + expectedLegacyID + `","guestName":"Alice","guestId":"g1"}`)
	preq, _ := http.NewRequest("POST", ts.URL+"/api/party/add", addBody)
	presp, err := http.DefaultClient.Do(preq)
	if err != nil || presp.StatusCode != 200 {
		t.Fatalf("party add failed: %v", err)
	}
	presp.Body.Close()
	env := partyEng.Envelope()
	if !env.Status.IsLive || env.State == nil || len(env.State.Tracks) != 1 || env.State.Tracks[0].Track.Title != "Time" {
		t.Fatalf("unexpected party state: %+v", env)
	}
}
