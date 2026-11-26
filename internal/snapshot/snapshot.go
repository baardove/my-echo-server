package snapshot

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/baardove/my-echo/server/internal/db"
)

// ShareBeacon is written to <musicRoot>/.myecho/server.json so macOS & other LAN clients
// automatically discover My Echo Server on a mounted share (no PIN stored on share).
type ShareBeacon struct {
	ServerName string `json:"serverName"`
	ServerKind string `json:"serverKind"`
	LibraryID  string `json:"libraryId"`
	LibraryName string `json:"libraryName"`
	LANURL     string `json:"lanURL"`
	PartyURL   string `json:"partyURL"`
	APIPort    int    `json:"apiPort"`
	PartyPort  int    `json:"partyPort"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// EditorialMeta describes the pre-built compressed editorial.json.gz bundle shipped alongside index shards.
type EditorialMeta struct {
	SHA256          string `json:"sha256"`
	CompressedBytes int64  `json:"compressedBytes"`
	AlbumCount      int    `json:"albumCount"`
	ArtistCount     int    `json:"artistCount"`
}

type EditorialBundle struct {
	GeneratedAt     time.Time            `json:"generatedAt"`
	AlbumEditorial  []db.AlbumEditorial  `json:"albumEditorial"`
	ArtistEditorial []db.ArtistEditorial `json:"artistEditorial"`
}

// ShareManifest is written to <musicRoot>/.myecho/manifest.json alongside shards/shard-XXX.ndjson.gz & editorial.json.gz.
type ShareManifest struct {
	ServerName               string         `json:"serverName"`
	LibraryID                string         `json:"libraryId"`
	LibraryName              string         `json:"libraryName"`
	Revision                 int            `json:"revision"`
	ShardCount               int            `json:"shardCount"`
	TrackCount               int            `json:"trackCount"`
	TotalAudioBytes          int64          `json:"totalAudioBytes"`
	CompressedBytes          int64          `json:"compressedBytes"`
	UncompressedBytes        int64          `json:"uncompressedBytes"`
	Compression              string         `json:"compression"`
	GeneratedAt              time.Time      `json:"generatedAt"`
	Shards                   []db.ShardMeta `json:"shards"`
	EditorialSHA256          string         `json:"editorialSha256,omitempty"`
	EditorialCompressedBytes int64          `json:"editorialCompressedBytes,omitempty"`
	EditorialAlbumCount      int            `json:"editorialAlbumCount,omitempty"`
	EditorialArtistCount     int            `json:"editorialArtistCount,omitempty"`
}

// Builder writes pre-compressed deterministic NDJSON.gz shards to disk with SHA-256 verification.
type Builder struct {
	shardsDir string
	store     *db.Store
}

func NewBuilder(shardsDir string, store *db.Store) (*Builder, error) {
	if err := os.MkdirAll(shardsDir, 0o755); err != nil {
		return nil, err
	}
	return &Builder{shardsDir: shardsDir, store: store}, nil
}

func (b *Builder) ShardFilePath(libraryID string, shardID int) string {
	return filepath.Join(b.shardsDir, libraryID, fmt.Sprintf("shard-%03d.ndjson.gz", shardID))
}

// BuildShards rebuilds only the specified shard IDs for libraryID atomically.
func (b *Builder) BuildShards(libraryID string, shardIDs []int, revision int) error {
	libDir := filepath.Join(b.shardsDir, libraryID)
	if err := os.MkdirAll(libDir, 0o755); err != nil {
		return err
	}

	for _, shardID := range shardIDs {
		tracks, err := b.store.LoadTracksInShard(libraryID, shardID)
		if err != nil {
			return err
		}

		var rawBuf bytes.Buffer
		for _, tr := range tracks {
			line, err := json.Marshal(tr)
			if err != nil {
				return err
			}
			rawBuf.Write(line)
			rawBuf.WriteByte('\n')
		}

		var gzBuf bytes.Buffer
		gzw, err := gzip.NewWriterLevel(&gzBuf, gzip.DefaultCompression)
		if err != nil {
			return err
		}
		if _, err := gzw.Write(rawBuf.Bytes()); err != nil {
			_ = gzw.Close()
			return err
		}
		if err := gzw.Close(); err != nil {
			return err
		}

		gzBytes := gzBuf.Bytes()
		sum := sha256.Sum256(gzBytes)
		hashHex := hex.EncodeToString(sum[:])

		finalPath := b.ShardFilePath(libraryID, shardID)
		tmpPath := finalPath + ".tmp"
		if err := os.WriteFile(tmpPath, gzBytes, 0o644); err != nil {
			return err
		}
		if err := os.Rename(tmpPath, finalPath); err != nil {
			return err
		}

		if err := b.store.SaveShardMeta(db.ShardMeta{
			LibraryID:         libraryID,
			ShardID:           shardID,
			SHA256:            hashHex,
			TrackCount:        len(tracks),
			CompressedBytes:   int64(len(gzBytes)),
			UncompressedBytes: int64(rawBuf.Len()),
			UpdatedAtRevision: revision,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) ReadShardBytes(libraryID string, shardID int) ([]byte, error) {
	return os.ReadFile(b.ShardFilePath(libraryID, shardID))
}

func (b *Builder) EditorialBundlePath() string {
	return filepath.Join(b.shardsDir, "editorial.json.gz")
}

// BuildEditorialBundle writes a gzip-compressed snapshot of all cached album & artist editorial info
// to /config/shards/editorial.json.gz and persists its SHA-256 metadata in SQLite kv_store.
func (b *Builder) BuildEditorialBundle() (EditorialMeta, error) {
	albs, err := b.store.ListAlbumEditorial()
	if err != nil {
		return EditorialMeta{}, err
	}
	arts, err := b.store.ListArtistEditorial()
	if err != nil {
		return EditorialMeta{}, err
	}
	if albs == nil {
		albs = []db.AlbumEditorial{}
	}
	if arts == nil {
		arts = []db.ArtistEditorial{}
	}

	raw, err := json.Marshal(EditorialBundle{
		GeneratedAt:     time.Now().UTC(),
		AlbumEditorial:  albs,
		ArtistEditorial: arts,
	})
	if err != nil {
		return EditorialMeta{}, err
	}
	var gzBuf bytes.Buffer
	gzw, err := gzip.NewWriterLevel(&gzBuf, gzip.DefaultCompression)
	if err != nil {
		return EditorialMeta{}, err
	}
	if _, err := gzw.Write(raw); err != nil {
		_ = gzw.Close()
		return EditorialMeta{}, err
	}
	if err := gzw.Close(); err != nil {
		return EditorialMeta{}, err
	}

	gzBytes := gzBuf.Bytes()
	sum := sha256.Sum256(gzBytes)
	meta := EditorialMeta{
		SHA256:          hex.EncodeToString(sum[:]),
		CompressedBytes: int64(len(gzBytes)),
		AlbumCount:      len(albs),
		ArtistCount:     len(arts),
	}

	dst := b.EditorialBundlePath()
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, gzBytes, 0o644); err != nil {
		return EditorialMeta{}, err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return EditorialMeta{}, err
	}
	if rawMeta, err := json.Marshal(meta); err == nil {
		_ = b.store.SetKV("editorial.bundle.meta", string(rawMeta))
	}
	return meta, nil
}

func (b *Builder) GetEditorialMeta() EditorialMeta {
	if raw, ok := b.store.GetKV("editorial.bundle.meta"); ok {
		var m EditorialMeta
		if json.Unmarshal([]byte(raw), &m) == nil && m.SHA256 != "" {
			return m
		}
	}
	m, _ := b.BuildEditorialBundle()
	return m
}

func (b *Builder) ReadEditorialBundleBytes() ([]byte, error) {
	if data, err := os.ReadFile(b.EditorialBundlePath()); err == nil {
		return data, nil
	}
	if _, err := b.BuildEditorialBundle(); err != nil {
		return nil, err
	}
	return os.ReadFile(b.EditorialBundlePath())
}

// MirrorToShare writes <libraryRoot>/.myecho/server.json, manifest.json, editorial.json.gz, and updated compressed shards
// if the share is writable. Silently skips when mounted read-only (:ro).
func (b *Builder) MirrorToShare(libRoot string, beacon ShareBeacon, libMeta *db.LibraryMeta, dirtyShardIDs []int) {
	if libRoot == "" || libMeta == nil {
		return
	}
	dotDir := filepath.Join(libRoot, ".myecho")
	shardsOutDir := filepath.Join(dotDir, "shards")
	if err := os.MkdirAll(shardsOutDir, 0o755); err != nil {
		// Read-only mount is valid and expected when user chooses :ro
		log.Printf("[snapshot] .myecho mirror skipped for %s (not writable: %v)", libRoot, err)
		return
	}

	// Copy dirty shards (or any missing shards in .myecho/shards)
	allShards, err := b.store.ListShards(libMeta.ID)
	if err != nil {
		return
	}
	dirtySet := make(map[int]bool, len(dirtyShardIDs))
	for _, s := range dirtyShardIDs {
		dirtySet[s] = true
	}

	var compTotal, uncompTotal int64
	for _, sh := range allShards {
		compTotal += sh.CompressedBytes
		uncompTotal += sh.UncompressedBytes
		dst := filepath.Join(shardsOutDir, fmt.Sprintf("shard-%03d.ndjson.gz", sh.ShardID))
		_, statErr := os.Stat(dst)
		if dirtySet[sh.ShardID] || statErr != nil {
			if raw, rErr := b.ReadShardBytes(libMeta.ID, sh.ShardID); rErr == nil {
				tmp := dst + ".tmp"
				if os.WriteFile(tmp, raw, 0o644) == nil {
					_ = os.Rename(tmp, dst)
				}
			}
		}
	}

	// Mirror compressed editorial bundle alongside index shards
	edMeta := b.GetEditorialMeta()
	if edBytes, err := b.ReadEditorialBundleBytes(); err == nil {
		tmp := filepath.Join(dotDir, "editorial.json.gz.tmp")
		if os.WriteFile(tmp, edBytes, 0o644) == nil {
			_ = os.Rename(tmp, filepath.Join(dotDir, "editorial.json.gz"))
		}
	}

	if rawBeacon, err := json.MarshalIndent(beacon, "", "  "); err == nil {
		tmp := filepath.Join(dotDir, "server.json.tmp")
		if os.WriteFile(tmp, rawBeacon, 0o644) == nil {
			_ = os.Rename(tmp, filepath.Join(dotDir, "server.json"))
		}
	}

	manifest := ShareManifest{
		ServerName:               beacon.ServerName,
		LibraryID:                libMeta.ID,
		LibraryName:              libMeta.Name,
		Revision:                 libMeta.CurrentRevision,
		ShardCount:               libMeta.ShardCount,
		TrackCount:               libMeta.TrackCount,
		TotalAudioBytes:          libMeta.TotalBytes,
		CompressedBytes:          compTotal,
		UncompressedBytes:        uncompTotal,
		Compression:              "gzip",
		GeneratedAt:              time.Now().UTC(),
		Shards:                   allShards,
		EditorialSHA256:          edMeta.SHA256,
		EditorialCompressedBytes: edMeta.CompressedBytes,
		EditorialAlbumCount:      edMeta.AlbumCount,
		EditorialArtistCount:     edMeta.ArtistCount,
	}
	if rawMan, err := json.MarshalIndent(manifest, "", "  "); err == nil {
		tmp := filepath.Join(dotDir, "manifest.json.tmp")
		if os.WriteFile(tmp, rawMan, 0o644) == nil {
			_ = os.Rename(tmp, filepath.Join(dotDir, "manifest.json"))
		}
	}
	log.Printf("[snapshot] mirrored rev #%d (%d shards, %d KB gz + %d alb/%d art editorial) to %s", libMeta.CurrentRevision, len(allShards), compTotal/1024, edMeta.AlbumCount, edMeta.ArtistCount, dotDir)
}
