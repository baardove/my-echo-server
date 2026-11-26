package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// TrackRecord is the canonical indexed track stored on My Echo Server and distributed in shards/deltas.
type TrackRecord struct {
	TrackKey           string     `json:"trackKey"`
	LegacyID           string     `json:"legacyId,omitempty"`
	LibraryID          string     `json:"libraryId"`
	RelativePath       string     `json:"relativePath"`
	FileName           string     `json:"fileName"`
	FileSize           int64      `json:"fileSize"`
	ModifiedAt         time.Time  `json:"modifiedAt"`
	AddedAt            *time.Time `json:"addedAt,omitempty"`
	Format             string     `json:"format"`
	FileExtension      string     `json:"fileExtension"`
	Title              string     `json:"title"`
	Artist             string     `json:"artist"`
	AlbumArtist        string     `json:"albumArtist"`
	Album              string     `json:"album"`
	TrackNumber        *int       `json:"trackNumber,omitempty"`
	DiscNumber         *int       `json:"discNumber,omitempty"`
	Year               *int       `json:"year,omitempty"`
	Genre              *string    `json:"genre,omitempty"`
	Duration           float64    `json:"duration"`
	SampleRate         *int       `json:"sampleRate,omitempty"`
	BitDepth           *int       `json:"bitDepth,omitempty"`
	Bitrate            *int       `json:"bitrate,omitempty"`
	ArtworkHash        *string    `json:"artworkHash,omitempty"`
	HasEmbeddedArtwork bool       `json:"hasEmbeddedArtwork"`
	LyricsRelativePath *string    `json:"lyricsRelativePath,omitempty"`
	ShardID            int        `json:"shardId"`
}

// EffectiveID returns LegacyID if configured so existing Mac playlists/ratings match 1:1, else TrackKey.
func (t TrackRecord) EffectiveID() string {
	if t.LegacyID != "" {
		return t.LegacyID
	}
	return t.TrackKey
}

// ShardMeta describes one pre-built compressed index shard.
type ShardMeta struct {
	LibraryID         string `json:"libraryId"`
	ShardID           int    `json:"shardId"`
	SHA256            string `json:"sha256"`
	TrackCount        int    `json:"trackCount"`
	CompressedBytes   int64  `json:"compressedBytes"`
	UncompressedBytes int64  `json:"uncompressedBytes"`
	UpdatedAtRevision int    `json:"updatedAtRevision"`
}

// LibraryMeta tracks revision & counts for one mounted library.
type LibraryMeta struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	RootPath        string     `json:"rootPath"`
	ShardCount      int        `json:"shardCount"`
	CurrentRevision int        `json:"currentRevision"`
	MinDeltaRev     int        `json:"minDeltaRevision"`
	TrackCount      int        `json:"trackCount"`
	TotalBytes      int64      `json:"totalBytes"`
	LastScannedAt   *time.Time `json:"lastScannedAt,omitempty"`
}

// DeltaEntry captures one track change at a given revision.
type DeltaEntry struct {
	Revision     int          `json:"revision"`
	Op           string       `json:"op"` // "upsert" or "delete"
	TrackKey     string       `json:"trackKey"`
	RelativePath string       `json:"relativePath,omitempty"`
	Track        *TrackRecord `json:"track,omitempty"`
}

// SharedPlaylist is synced across Mac, Server, and Apple TV.
type SharedPlaylist struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	TrackIDs    []string  `json:"trackIDs"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	Deleted     bool      `json:"deleted,omitempty"`
}

// SharedTrackStats mirrors EchoCore TrackStats.
type SharedTrackStats struct {
	TrackID          string     `json:"trackID"`
	UserPlayCount    int        `json:"userPlayCount"`
	LastPlayedAt     *time.Time `json:"lastPlayedAt,omitempty"`
	UserRating       *int       `json:"userRating,omitempty"`
	OnlinePopularity *int       `json:"onlinePopularity,omitempty"`
	OnlinePlayCount  *int       `json:"onlinePlayCount,omitempty"`
	OnlineListeners  *int       `json:"onlineListeners,omitempty"`
	OnlineSource     *string    `json:"onlineSource,omitempty"`
	OnlineFetchedAt  *time.Time `json:"onlineFetchedAt,omitempty"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

// SharedAlbumRating mirrors EchoCore AlbumUserRating.
type SharedAlbumRating struct {
	AlbumKey  string    `json:"albumKey"`
	Rating    int       `json:"rating"`
	UpdatedAt time.Time `json:"updatedAt"`
	Deleted   bool      `json:"deleted,omitempty"`
}

// SharedPodcastFeed mirrors WirelessSyncPodcastFeed.
type SharedPodcastFeed struct {
	ID                    string    `json:"id"`
	Title                 string    `json:"title"`
	Author                string    `json:"author"`
	Description           string    `json:"description"`
	FeedURL               string    `json:"feedURL"`
	ArtworkURL            *string   `json:"artworkURL,omitempty"`
	ArtworkHash           *string   `json:"artworkHash,omitempty"`
	EpisodesToKeep        int       `json:"episodesToKeep"`
	IsSelectedForAutoSync bool      `json:"isSelectedForAutoSync"`
	UpdatedAt             time.Time `json:"updatedAt"`
	Deleted               bool      `json:"deleted,omitempty"`
}

// AlbumEditorial mirrors EchoCore AlbumEditorialInfo.
type AlbumEditorial struct {
	AlbumKey         string    `json:"albumKey"`
	ReleaseDate      *string   `json:"releaseDate,omitempty"`
	Label            *string   `json:"label,omitempty"`
	Genres           []string  `json:"genres"`
	AlbumFans        *int      `json:"albumFans,omitempty"`
	ArtistFans       *int      `json:"artistFans,omitempty"`
	ArtistAlbumCount *int      `json:"artistAlbumCount,omitempty"`
	AlbumSummary     *string   `json:"albumSummary,omitempty"`
	ArtistSummary    *string   `json:"artistSummary,omitempty"`
	ArtistImageURL   *string   `json:"artistImageURL,omitempty"`
	FetchedAt        time.Time `json:"fetchedAt"`
}

// ArtistEditorial mirrors EchoCore ArtistEditorialInfo.
type ArtistEditorial struct {
	ArtistKey  string    `json:"artistKey"`
	Summary    *string   `json:"summary,omitempty"`
	Fans       *int      `json:"fans,omitempty"`
	AlbumCount *int      `json:"albumCount,omitempty"`
	ImageURL   *string   `json:"imageURL,omitempty"`
	FetchedAt  time.Time `json:"fetchedAt"`
}

// TrackAnalysis stores optional deep audio DSP telemetry for DJ Mode & smart stacking:
// BPM, vocal/singing start & end cue points, loudness/peak for live normalization, energy, normalized genre, and smart tags.
type TrackAnalysis struct {
	TrackKey      string    `json:"trackKey"`
	BPM           float64   `json:"bpm"`
	VocalStartSec float64   `json:"vocalStartSec"`
	VocalEndSec   float64   `json:"vocalEndSec"`
	Energy        float64   `json:"energy"`
	LoudnessDB    float64   `json:"loudnessDB"`
	PeakLinear    float64   `json:"peakLinear"`
	NormGainDB    float64   `json:"normGainDB"`
	Genre         string    `json:"genre,omitempty"`
	Tags          []string  `json:"tags"`
	AnalyzedAt    time.Time `json:"analyzedAt"`
	Version       int       `json:"version"`
}

type Store struct {
	sql *sql.DB
	mu  sync.RWMutex
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)", path)
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1) // Serialize writes cleanly on SQLite WAL

	s := &Store{sql: conn}
	if err := s.migrate(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.sql.Close()
}

func (s *Store) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS libraries (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		root_path TEXT NOT NULL,
		shard_count INTEGER NOT NULL DEFAULT 16,
		current_revision INTEGER NOT NULL DEFAULT 0,
		min_delta_revision INTEGER NOT NULL DEFAULT 0,
		track_count INTEGER NOT NULL DEFAULT 0,
		total_bytes INTEGER NOT NULL DEFAULT 0,
		last_scanned_at INTEGER
	);

	CREATE TABLE IF NOT EXISTS tracks (
		track_key TEXT PRIMARY KEY,
		legacy_id TEXT,
		library_id TEXT NOT NULL,
		relative_path TEXT NOT NULL,
		shard_id INTEGER NOT NULL,
		file_size INTEGER NOT NULL,
		modified_at INTEGER NOT NULL,
		json TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_tracks_library ON tracks(library_id);
	CREATE INDEX IF NOT EXISTS idx_tracks_lib_rel ON tracks(library_id, relative_path);
	CREATE INDEX IF NOT EXISTS idx_tracks_lib_shard ON tracks(library_id, shard_id);
	CREATE INDEX IF NOT EXISTS idx_tracks_legacy ON tracks(legacy_id);

	CREATE TABLE IF NOT EXISTS shards (
		library_id TEXT NOT NULL,
		shard_id INTEGER NOT NULL,
		sha256 TEXT NOT NULL,
		track_count INTEGER NOT NULL,
		compressed_bytes INTEGER NOT NULL,
		uncompressed_bytes INTEGER NOT NULL,
		updated_at_rev INTEGER NOT NULL,
		PRIMARY KEY (library_id, shard_id)
	);

	CREATE TABLE IF NOT EXISTS index_changelog (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		library_id TEXT NOT NULL,
		revision INTEGER NOT NULL,
		op TEXT NOT NULL,
		track_key TEXT NOT NULL,
		relative_path TEXT NOT NULL DEFAULT '',
		track_json TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_changelog_lib_rev ON index_changelog(library_id, revision);

	CREATE TABLE IF NOT EXISTS playlists (
		id TEXT PRIMARY KEY,
		updated_at INTEGER NOT NULL,
		deleted INTEGER NOT NULL DEFAULT 0,
		json TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS track_stats (
		track_id TEXT PRIMARY KEY,
		updated_at INTEGER NOT NULL,
		json TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS album_ratings (
		album_key TEXT PRIMARY KEY,
		updated_at INTEGER NOT NULL,
		deleted INTEGER NOT NULL DEFAULT 0,
		json TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS podcasts (
		id TEXT PRIMARY KEY,
		updated_at INTEGER NOT NULL,
		deleted INTEGER NOT NULL DEFAULT 0,
		json TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS album_editorial (
		album_key TEXT PRIMARY KEY,
		fetched_at INTEGER NOT NULL,
		json TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS artist_editorial (
		artist_key TEXT PRIMARY KEY,
		fetched_at INTEGER NOT NULL,
		json TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS track_analysis (
		track_key TEXT PRIMARY KEY,
		bpm REAL NOT NULL DEFAULT 0,
		vocal_start_sec REAL NOT NULL DEFAULT 0,
		vocal_end_sec REAL NOT NULL DEFAULT 0,
		energy REAL NOT NULL DEFAULT 0,
		loudness_db REAL NOT NULL DEFAULT 0,
		peak_linear REAL NOT NULL DEFAULT 0,
		norm_gain_db REAL NOT NULL DEFAULT 0,
		genre TEXT NOT NULL DEFAULT '',
		tags_json TEXT NOT NULL DEFAULT '[]',
		analyzed_at INTEGER NOT NULL,
		version INTEGER NOT NULL DEFAULT 1
	);

	CREATE TABLE IF NOT EXISTS kv_store (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);
	`
	_, err := s.sql.Exec(schema)
	if err != nil {
		return err
	}
	// Idempotent column migrations for existing databases
	for _, stmt := range []string{
		`ALTER TABLE track_analysis ADD COLUMN loudness_db REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE track_analysis ADD COLUMN peak_linear REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE track_analysis ADD COLUMN norm_gain_db REAL NOT NULL DEFAULT 0`,
	} {
		_, _ = s.sql.Exec(stmt)
	}
	return nil
}

// UpsertLibrary ensures a library row exists.
func (s *Store) UpsertLibrary(id, name, rootPath string, shardCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sql.Exec(`
		INSERT INTO libraries (id, name, root_path, shard_count)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET name=excluded.name, root_path=excluded.root_path, shard_count=excluded.shard_count
	`, id, name, rootPath, shardCount)
	return err
}

func (s *Store) GetLibrary(id string) (*LibraryMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row := s.sql.QueryRow(`
		SELECT id, name, root_path, shard_count, current_revision, min_delta_revision, track_count, total_bytes, last_scanned_at
		FROM libraries WHERE id = ?
	`, id)
	var m LibraryMeta
	var lastScan sql.NullInt64
	if err := row.Scan(&m.ID, &m.Name, &m.RootPath, &m.ShardCount, &m.CurrentRevision, &m.MinDeltaRev, &m.TrackCount, &m.TotalBytes, &lastScan); err != nil {
		return nil, err
	}
	if lastScan.Valid {
		t := time.Unix(lastScan.Int64, 0).UTC()
		m.LastScannedAt = &t
	}
	return &m, nil
}

func (s *Store) ListLibraries() ([]LibraryMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`
		SELECT id, name, root_path, shard_count, current_revision, min_delta_revision, track_count, total_bytes, last_scanned_at
		FROM libraries ORDER BY name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LibraryMeta
	for rows.Next() {
		var m LibraryMeta
		var lastScan sql.NullInt64
		if err := rows.Scan(&m.ID, &m.Name, &m.RootPath, &m.ShardCount, &m.CurrentRevision, &m.MinDeltaRev, &m.TrackCount, &m.TotalBytes, &lastScan); err != nil {
			return nil, err
		}
		if lastScan.Valid {
			t := time.Unix(lastScan.Int64, 0).UTC()
			m.LastScannedAt = &t
		}
		out = append(out, m)
	}
	return out, nil
}

// LoadTracksByLibrary loads all cached tracks for incremental comparison.
func (s *Store) LoadTracksByLibrary(libraryID string) (map[string]TrackRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT relative_path, json FROM tracks WHERE library_id = ?`, libraryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]TrackRecord)
	for rows.Next() {
		var rel, raw string
		if err := rows.Scan(&rel, &raw); err != nil {
			return nil, err
		}
		var tr TrackRecord
		if err := json.Unmarshal([]byte(raw), &tr); err == nil {
			out[rel] = tr
		}
	}
	return out, nil
}

// LoadAllTracks returns every track across all libraries.
func (s *Store) LoadAllTracks() ([]TrackRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT json FROM tracks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TrackRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var tr TrackRecord
		if err := json.Unmarshal([]byte(raw), &tr); err == nil {
			out = append(out, tr)
		}
	}
	return out, nil
}

// LoadTracksInShard loads all tracks in a specific library shard, deterministically ordered.
func (s *Store) LoadTracksInShard(libraryID string, shardID int) ([]TrackRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT json FROM tracks WHERE library_id = ? AND shard_id = ? ORDER BY relative_path ASC`, libraryID, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TrackRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var tr TrackRecord
		if err := json.Unmarshal([]byte(raw), &tr); err == nil {
			out = append(out, tr)
		}
	}
	return out, nil
}

// FindTrackByID looks up a track by either track_key or legacy_id.
func (s *Store) FindTrackByID(id string) (*TrackRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row := s.sql.QueryRow(`SELECT json FROM tracks WHERE track_key = ? OR legacy_id = ? LIMIT 1`, id, id)
	var raw string
	if err := row.Scan(&raw); err != nil {
		return nil, err
	}
	var tr TrackRecord
	if err := json.Unmarshal([]byte(raw), &tr); err != nil {
		return nil, err
	}
	return &tr, nil
}

// CommitScanDelta atomically applies upserts and deletes, bumps library revision, and appends changelog entries.
func (s *Store) CommitScanDelta(
	libraryID string,
	upserted []TrackRecord,
	removed []TrackRecord,
	totalTracks int,
	totalBytes int64,
	maxDeltaRevs int,
) (newRevision int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var curRev int
	var minDelta int
	if err = tx.QueryRow(`SELECT current_revision, min_delta_revision FROM libraries WHERE id = ?`, libraryID).Scan(&curRev, &minDelta); err != nil {
		return 0, err
	}

	nowUnix := time.Now().UTC().Unix()
	hasChanges := len(upserted) > 0 || len(removed) > 0
	if !hasChanges {
		_, err = tx.Exec(`UPDATE libraries SET track_count = ?, total_bytes = ?, last_scanned_at = ? WHERE id = ?`,
			totalTracks, totalBytes, nowUnix, libraryID)
		if err != nil {
			return 0, err
		}
		if err = tx.Commit(); err != nil {
			return 0, err
		}
		return curRev, nil
	}

	newRevision = curRev + 1
	if minDelta == 0 {
		minDelta = 1
	}

	upsertStmt, err := tx.Prepare(`
		INSERT INTO tracks (track_key, legacy_id, library_id, relative_path, shard_id, file_size, modified_at, json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(track_key) DO UPDATE SET
			legacy_id=excluded.legacy_id,
			relative_path=excluded.relative_path,
			shard_id=excluded.shard_id,
			file_size=excluded.file_size,
			modified_at=excluded.modified_at,
			json=excluded.json
	`)
	if err != nil {
		return 0, err
	}
	defer upsertStmt.Close()

	logStmt, err := tx.Prepare(`
		INSERT INTO index_changelog (library_id, revision, op, track_key, relative_path, track_json)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}
	defer logStmt.Close()

	for _, tr := range upserted {
		raw, mErr := json.Marshal(tr)
		if mErr != nil {
			return 0, mErr
		}
		if _, err = upsertStmt.Exec(tr.TrackKey, tr.LegacyID, tr.LibraryID, tr.RelativePath, tr.ShardID, tr.FileSize, tr.ModifiedAt.Unix(), string(raw)); err != nil {
			return 0, err
		}
		if _, err = logStmt.Exec(libraryID, newRevision, "upsert", tr.TrackKey, tr.RelativePath, string(raw)); err != nil {
			return 0, err
		}
	}

	delStmt, err := tx.Prepare(`DELETE FROM tracks WHERE track_key = ?`)
	if err != nil {
		return 0, err
	}
	defer delStmt.Close()

	for _, tr := range removed {
		if _, err = delStmt.Exec(tr.TrackKey); err != nil {
			return 0, err
		}
		if _, err = logStmt.Exec(libraryID, newRevision, "delete", tr.TrackKey, tr.RelativePath, nil); err != nil {
			return 0, err
		}
	}

	// Prune changelog beyond retention window
	if maxDeltaRevs > 0 && newRevision-minDelta >= maxDeltaRevs {
		cutoff := newRevision - maxDeltaRevs + 1
		if cutoff > minDelta {
			minDelta = cutoff
			_, _ = tx.Exec(`DELETE FROM index_changelog WHERE library_id = ? AND revision < ?`, libraryID, minDelta)
		}
	}

	_, err = tx.Exec(`
		UPDATE libraries
		SET current_revision = ?, min_delta_revision = ?, track_count = ?, total_bytes = ?, last_scanned_at = ?
		WHERE id = ?
	`, newRevision, minDelta, totalTracks, totalBytes, nowUnix, libraryID)
	if err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return newRevision, nil
}

// SaveShardMeta persists shard checksum & size info.
func (s *Store) SaveShardMeta(meta ShardMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sql.Exec(`
		INSERT INTO shards (library_id, shard_id, sha256, track_count, compressed_bytes, uncompressed_bytes, updated_at_rev)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(library_id, shard_id) DO UPDATE SET
			sha256=excluded.sha256,
			track_count=excluded.track_count,
			compressed_bytes=excluded.compressed_bytes,
			uncompressed_bytes=excluded.uncompressed_bytes,
			updated_at_rev=excluded.updated_at_rev
	`, meta.LibraryID, meta.ShardID, meta.SHA256, meta.TrackCount, meta.CompressedBytes, meta.UncompressedBytes, meta.UpdatedAtRevision)
	return err
}

func (s *Store) ListShards(libraryID string) ([]ShardMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`
		SELECT library_id, shard_id, sha256, track_count, compressed_bytes, uncompressed_bytes, updated_at_rev
		FROM shards WHERE library_id = ? ORDER BY shard_id ASC
	`, libraryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShardMeta
	for rows.Next() {
		var m ShardMeta
		if err := rows.Scan(&m.LibraryID, &m.ShardID, &m.SHA256, &m.TrackCount, &m.CompressedBytes, &m.UncompressedBytes, &m.UpdatedAtRevision); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// LoadDeltaSince returns collapsed net upserts & deletes strictly after sinceRev.
func (s *Store) LoadDeltaSince(libraryID string, sinceRev int) (upserted []TrackRecord, removedKeys []string, removedRelPaths []string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`
		SELECT op, track_key, relative_path, track_json
		FROM index_changelog
		WHERE library_id = ? AND revision > ?
		ORDER BY id ASC
	`, libraryID, sinceRev)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	type state struct {
		op      string
		relPath string
		track   *TrackRecord
	}
	finalByKey := make(map[string]state)
	var order []string
	for rows.Next() {
		var op, key, rel string
		var raw sql.NullString
		if err := rows.Scan(&op, &key, &rel, &raw); err != nil {
			return nil, nil, nil, err
		}
		if _, seen := finalByKey[key]; !seen {
			order = append(order, key)
		}
		if op == "upsert" && raw.Valid {
			var tr TrackRecord
			if json.Unmarshal([]byte(raw.String), &tr) == nil {
				finalByKey[key] = state{op: "upsert", relPath: rel, track: &tr}
			}
		} else if op == "delete" {
			finalByKey[key] = state{op: "delete", relPath: rel}
		}
	}

	for _, key := range order {
		st := finalByKey[key]
		if st.op == "upsert" && st.track != nil {
			upserted = append(upserted, *st.track)
		} else if st.op == "delete" {
			removedKeys = append(removedKeys, key)
			if st.relPath != "" {
				removedRelPaths = append(removedRelPaths, st.relPath)
			}
		}
	}
	return upserted, removedKeys, removedRelPaths, nil
}

// --- Shared Library State (Playlists, Stats, Ratings, Podcasts, Editorial) ---

func (s *Store) ListPlaylists(includeDeleted bool) ([]SharedPlaylist, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := `SELECT json FROM playlists`
	if !includeDeleted {
		q += ` WHERE deleted = 0`
	}
	q += ` ORDER BY updated_at ASC`
	rows, err := s.sql.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SharedPlaylist
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var p SharedPlaylist
		if json.Unmarshal([]byte(raw), &p) == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *Store) MergePlaylists(incoming []SharedPlaylist) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, p := range incoming {
		var existingUpdated int64
		row := tx.QueryRow(`SELECT updated_at FROM playlists WHERE id = ?`, p.ID)
		if scanErr := row.Scan(&existingUpdated); scanErr == nil {
			if p.UpdatedAt.Unix() < existingUpdated {
				continue
			}
		}
		raw, _ := json.Marshal(p)
		del := 0
		if p.Deleted {
			del = 1
		}
		_, err = tx.Exec(`
			INSERT INTO playlists (id, updated_at, deleted, json) VALUES (?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, deleted=excluded.deleted, json=excluded.json
		`, p.ID, p.UpdatedAt.Unix(), del, string(raw))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListTrackStats() ([]SharedTrackStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT json FROM track_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SharedTrackStats
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item SharedTrackStats
		if json.Unmarshal([]byte(raw), &item) == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Store) MergeTrackStats(incoming []SharedTrackStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, item := range incoming {
		var existingRaw string
		if scanErr := tx.QueryRow(`SELECT json FROM track_stats WHERE track_id = ?`, item.TrackID).Scan(&existingRaw); scanErr == nil {
			var cur SharedTrackStats
			if json.Unmarshal([]byte(existingRaw), &cur) == nil {
				if cur.UserPlayCount > item.UserPlayCount {
					item.UserPlayCount = cur.UserPlayCount
				}
				if !cur.UpdatedAt.IsZero() && item.UpdatedAt.Before(cur.UpdatedAt) {
					item.UserRating = cur.UserRating
					item.UpdatedAt = cur.UpdatedAt
				}
			}
		}
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = time.Now().UTC()
		}
		raw, _ := json.Marshal(item)
		_, err = tx.Exec(`
			INSERT INTO track_stats (track_id, updated_at, json) VALUES (?, ?, ?)
			ON CONFLICT(track_id) DO UPDATE SET updated_at=excluded.updated_at, json=excluded.json
		`, item.TrackID, item.UpdatedAt.Unix(), string(raw))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListAlbumRatings(includeDeleted bool) ([]SharedAlbumRating, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := `SELECT json FROM album_ratings`
	if !includeDeleted {
		q += ` WHERE deleted = 0`
	}
	rows, err := s.sql.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SharedAlbumRating
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item SharedAlbumRating
		if json.Unmarshal([]byte(raw), &item) == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Store) MergeAlbumRatings(incoming []SharedAlbumRating) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, item := range incoming {
		var existingUpdated int64
		if scanErr := tx.QueryRow(`SELECT updated_at FROM album_ratings WHERE album_key = ?`, item.AlbumKey).Scan(&existingUpdated); scanErr == nil {
			if item.UpdatedAt.Unix() < existingUpdated {
				continue
			}
		}
		raw, _ := json.Marshal(item)
		del := 0
		if item.Deleted {
			del = 1
		}
		_, err = tx.Exec(`
			INSERT INTO album_ratings (album_key, updated_at, deleted, json) VALUES (?, ?, ?, ?)
			ON CONFLICT(album_key) DO UPDATE SET updated_at=excluded.updated_at, deleted=excluded.deleted, json=excluded.json
		`, item.AlbumKey, item.UpdatedAt.Unix(), del, string(raw))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListPodcasts(includeDeleted bool) ([]SharedPodcastFeed, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := `SELECT json FROM podcasts`
	if !includeDeleted {
		q += ` WHERE deleted = 0`
	}
	rows, err := s.sql.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SharedPodcastFeed
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item SharedPodcastFeed
		if json.Unmarshal([]byte(raw), &item) == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Store) MergePodcasts(incoming []SharedPodcastFeed) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, item := range incoming {
		var existingUpdated int64
		if scanErr := tx.QueryRow(`SELECT updated_at FROM podcasts WHERE id = ?`, item.ID).Scan(&existingUpdated); scanErr == nil {
			if item.UpdatedAt.Unix() < existingUpdated {
				continue
			}
		}
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = time.Now().UTC()
		}
		raw, _ := json.Marshal(item)
		del := 0
		if item.Deleted {
			del = 1
		}
		_, err = tx.Exec(`
			INSERT INTO podcasts (id, updated_at, deleted, json) VALUES (?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, deleted=excluded.deleted, json=excluded.json
		`, item.ID, item.UpdatedAt.Unix(), del, string(raw))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListAlbumEditorial() ([]AlbumEditorial, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT json FROM album_editorial`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AlbumEditorial
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item AlbumEditorial
		if json.Unmarshal([]byte(raw), &item) == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Store) MergeAlbumEditorial(incoming []AlbumEditorial) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, item := range incoming {
		var existingFetched int64
		if scanErr := tx.QueryRow(`SELECT fetched_at FROM album_editorial WHERE album_key = ?`, item.AlbumKey).Scan(&existingFetched); scanErr == nil {
			if item.FetchedAt.Unix() < existingFetched {
				continue
			}
		}
		raw, _ := json.Marshal(item)
		_, err = tx.Exec(`
			INSERT INTO album_editorial (album_key, fetched_at, json) VALUES (?, ?, ?)
			ON CONFLICT(album_key) DO UPDATE SET fetched_at=excluded.fetched_at, json=excluded.json
		`, item.AlbumKey, item.FetchedAt.Unix(), string(raw))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListArtistEditorial() ([]ArtistEditorial, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT json FROM artist_editorial`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArtistEditorial
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item ArtistEditorial
		if json.Unmarshal([]byte(raw), &item) == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *Store) MergeArtistEditorial(incoming []ArtistEditorial) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, item := range incoming {
		var existingFetched int64
		if scanErr := tx.QueryRow(`SELECT fetched_at FROM artist_editorial WHERE artist_key = ?`, item.ArtistKey).Scan(&existingFetched); scanErr == nil {
			if item.FetchedAt.Unix() < existingFetched {
				continue
			}
		}
		raw, _ := json.Marshal(item)
		_, err = tx.Exec(`
			INSERT INTO artist_editorial (artist_key, fetched_at, json) VALUES (?, ?, ?)
			ON CONFLICT(artist_key) DO UPDATE SET fetched_at=excluded.fetched_at, json=excluded.json
		`, item.ArtistKey, item.FetchedAt.Unix(), string(raw))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetKV(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var val string
	if err := s.sql.QueryRow(`SELECT value FROM kv_store WHERE key = ?`, key).Scan(&val); err != nil {
		return "", false
	}
	return val, true
}

func (s *Store) SetKV(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.sql.Exec(`INSERT INTO kv_store (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// --- Advanced Track Analysis (BPM, Vocal Boundaries, Smart Tags) ---

func (s *Store) UpsertTrackAnalysis(a TrackAnalysis) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.Tags == nil {
		a.Tags = []string{}
	}
	if a.AnalyzedAt.IsZero() {
		a.AnalyzedAt = time.Now().UTC()
	}
	if a.Version <= 0 {
		a.Version = 1
	}
	tagsRaw, _ := json.Marshal(a.Tags)
	_, err := s.sql.Exec(`
		INSERT INTO track_analysis (track_key, bpm, vocal_start_sec, vocal_end_sec, energy, loudness_db, peak_linear, norm_gain_db, genre, tags_json, analyzed_at, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(track_key) DO UPDATE SET
			bpm=excluded.bpm,
			vocal_start_sec=excluded.vocal_start_sec,
			vocal_end_sec=excluded.vocal_end_sec,
			energy=excluded.energy,
			loudness_db=excluded.loudness_db,
			peak_linear=excluded.peak_linear,
			norm_gain_db=excluded.norm_gain_db,
			genre=excluded.genre,
			tags_json=excluded.tags_json,
			analyzed_at=excluded.analyzed_at,
			version=excluded.version
	`, a.TrackKey, a.BPM, a.VocalStartSec, a.VocalEndSec, a.Energy, a.LoudnessDB, a.PeakLinear, a.NormGainDB, a.Genre, string(tagsRaw), a.AnalyzedAt.Unix(), a.Version)
	return err
}

func (s *Store) GetTrackAnalysis(trackKey string) (TrackAnalysis, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var a TrackAnalysis
	var tagsRaw string
	var ts int64
	err := s.sql.QueryRow(`SELECT track_key, bpm, vocal_start_sec, vocal_end_sec, energy, loudness_db, peak_linear, norm_gain_db, genre, tags_json, analyzed_at, version FROM track_analysis WHERE track_key=?`, trackKey).
		Scan(&a.TrackKey, &a.BPM, &a.VocalStartSec, &a.VocalEndSec, &a.Energy, &a.LoudnessDB, &a.PeakLinear, &a.NormGainDB, &a.Genre, &tagsRaw, &ts, &a.Version)
	if err != nil {
		return TrackAnalysis{}, false
	}
	_ = json.Unmarshal([]byte(tagsRaw), &a.Tags)
	if a.Tags == nil {
		a.Tags = []string{}
	}
	a.AnalyzedAt = time.Unix(ts, 0).UTC()
	return a, true
}

func (s *Store) ListTrackAnalysisMap() (map[string]TrackAnalysis, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows, err := s.sql.Query(`SELECT track_key, bpm, vocal_start_sec, vocal_end_sec, energy, loudness_db, peak_linear, norm_gain_db, genre, tags_json, analyzed_at, version FROM track_analysis`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]TrackAnalysis)
	for rows.Next() {
		var a TrackAnalysis
		var tagsRaw string
		var ts int64
		if err := rows.Scan(&a.TrackKey, &a.BPM, &a.VocalStartSec, &a.VocalEndSec, &a.Energy, &a.LoudnessDB, &a.PeakLinear, &a.NormGainDB, &a.Genre, &tagsRaw, &ts, &a.Version); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(tagsRaw), &a.Tags)
		if a.Tags == nil {
			a.Tags = []string{}
		}
		a.AnalyzedAt = time.Unix(ts, 0).UTC()
		out[a.TrackKey] = a
	}
	return out, nil
}

func (s *Store) SetTrackRating(trackID string, rating int) (SharedTrackStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var item SharedTrackStats
	var existingRaw string
	if err := s.sql.QueryRow(`SELECT json FROM track_stats WHERE track_id = ?`, trackID).Scan(&existingRaw); err == nil {
		_ = json.Unmarshal([]byte(existingRaw), &item)
	}
	item.TrackID = trackID
	if rating <= 0 {
		item.UserRating = nil
	} else {
		if rating > 5 {
			rating = 5
		}
		r := rating
		item.UserRating = &r
	}
	item.UpdatedAt = time.Now().UTC()
	raw, _ := json.Marshal(item)
	_, err := s.sql.Exec(`
		INSERT INTO track_stats (track_id, updated_at, json) VALUES (?, ?, ?)
		ON CONFLICT(track_id) DO UPDATE SET updated_at=excluded.updated_at, json=excluded.json
	`, item.TrackID, item.UpdatedAt.Unix(), string(raw))
	return item, err
}

func (s *Store) SetAlbumRating(albumKey string, rating int) (SharedAlbumRating, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	del := 0
	item := SharedAlbumRating{
		AlbumKey:  albumKey,
		Rating:    rating,
		UpdatedAt: now,
		Deleted:   rating <= 0,
	}
	if rating <= 0 {
		item.Rating = 0
		del = 1
	} else if item.Rating > 5 {
		item.Rating = 5
	}
	raw, _ := json.Marshal(item)
	_, err := s.sql.Exec(`
		INSERT INTO album_ratings (album_key, updated_at, deleted, json) VALUES (?, ?, ?, ?)
		ON CONFLICT(album_key) DO UPDATE SET updated_at=excluded.updated_at, deleted=excluded.deleted, json=excluded.json
	`, item.AlbumKey, item.UpdatedAt.Unix(), del, string(raw))
	return item, err
}
