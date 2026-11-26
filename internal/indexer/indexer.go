package indexer

import (
	"context"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baardove/my-echo/server/internal/artwork"
	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
	"github.com/baardove/my-echo/server/internal/snapshot"
	"golang.org/x/text/unicode/norm"
)

// ScanStatus represents real-time indexing progress for Admin UI and API.
type ScanStatus struct {
	IsScanning   bool       `json:"isScanning"`
	LibraryID    string     `json:"libraryId,omitempty"`
	LibraryName  string     `json:"libraryName,omitempty"`
	ScannedCount int        `json:"scannedCount"`
	TotalFound   int        `json:"totalFound"`
	CurrentFile  string     `json:"currentFile,omitempty"`
	LastResult   string     `json:"lastResult,omitempty"`
	LastScanAt   *time.Time `json:"lastScanAt,omitempty"`
}

type ScanSummary struct {
	LibraryID   string
	Added       int
	Updated     int
	Unchanged   int
	Removed     int
	Total       int
	NewRevision int
	DirtyShards []int
	Duration    time.Duration
}

type Indexer struct {
	cfg      *config.Config
	store    *db.Store
	artCache *artwork.Cache
	snap     *snapshot.Builder

	scanMu    sync.Mutex
	statusMu  sync.RWMutex
	status    ScanStatus
	onChanged func()
}

func New(cfg *config.Config, store *db.Store, artCache *artwork.Cache, snap *snapshot.Builder) *Indexer {
	return &Indexer{
		cfg:      cfg,
		store:    store,
		artCache: artCache,
		snap:     snap,
	}
}

func (idx *Indexer) SetOnChanged(fn func()) {
	idx.onChanged = fn
}

func (idx *Indexer) Status() ScanStatus {
	idx.statusMu.RLock()
	defer idx.statusMu.RUnlock()
	return idx.status
}

func (idx *Indexer) setStatus(st ScanStatus) {
	idx.statusMu.Lock()
	idx.status = st
	idx.statusMu.Unlock()
}

// ShardForPath computes deterministic shard bucket in [0, shardCount).
func ShardForPath(relPath string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(norm.NFC.String(relPath)))
	return int(h.Sum64() % uint64(shardCount))
}

// ScanAll scans every configured library sequentially.
func (idx *Indexer) ScanAll(ctx context.Context) ([]ScanSummary, error) {
	var out []ScanSummary
	for _, lib := range idx.cfg.Libraries {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		sum, err := idx.ScanLibrary(ctx, lib, "")
		if err != nil {
			log.Printf("[indexer] library %s scan error: %v", lib.ID, err)
			continue
		}
		out = append(out, sum)
	}
	return out, nil
}

// ScanLibrary incrementally indexes a library (or a targeted subfolder relative to library root, e.g., from Lidarr webhook).
func (idx *Indexer) ScanLibrary(ctx context.Context, lib config.LibraryConfig, subPath string) (ScanSummary, error) {
	idx.scanMu.Lock()
	defer idx.scanMu.Unlock()

	start := time.Now()
	if err := idx.store.UpsertLibrary(lib.ID, lib.Name, lib.Path, lib.ShardCount); err != nil {
		return ScanSummary{}, err
	}

	fi, err := os.Stat(lib.Path)
	if err != nil || !fi.IsDir() {
		return ScanSummary{}, fmt.Errorf("library root unavailable: %s", lib.Path)
	}

	existingByRel, err := idx.store.LoadTracksByLibrary(lib.ID)
	if err != nil {
		return ScanSummary{}, err
	}

	walkRoot := lib.Path
	isTargeted := false
	cleanSub := ""
	if strings.TrimSpace(subPath) != "" {
		cleanSub = filepath.Clean(strings.TrimPrefix(subPath, "/"))
		candidate := filepath.Join(lib.Path, cleanSub)
		if relCheck, rErr := filepath.Rel(lib.Path, candidate); rErr == nil && !strings.HasPrefix(relCheck, "..") {
			if sfi, sErr := os.Stat(candidate); sErr == nil && sfi.IsDir() {
				walkRoot = candidate
				isTargeted = true
			}
		}
	}

	idx.setStatus(ScanStatus{
		IsScanning:  true,
		LibraryID:   lib.ID,
		LibraryName: lib.Name,
		CurrentFile: "Walking directory tree...",
	})

	type discoveredFile struct {
		fullPath string
		relPath  string
		format   string
		ext      string
		size     int64
		modTime  time.Time
	}

	var discovered []discoveredFile
	seenRel := make(map[string]struct{})

	_ = filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		name := d.Name()
		if d.IsDir() {
			if path != walkRoot && (strings.HasPrefix(name, ".") || name == "#recycle" || name == "@eaDir" || name == "$RECYCLE.BIN") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "._") {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(name))
		format, ok := RecognizeFormat(ext)
		if !ok {
			return nil
		}
		rel, rErr := filepath.Rel(lib.Path, path)
		if rErr != nil {
			return nil
		}
		relSlash := norm.NFC.String(filepath.ToSlash(rel))
		info, iErr := d.Info()
		if iErr != nil {
			return nil
		}
		discovered = append(discovered, discoveredFile{
			fullPath: path,
			relPath:  relSlash,
			format:   format,
			ext:      strings.TrimPrefix(ext, "."),
			size:     info.Size(),
			modTime:  info.ModTime().UTC(),
		})
		seenRel[relSlash] = struct{}{}
		return nil
	})

	totalFound := len(discovered)
	var toParse []discoveredFile
	var unchangedCount int
	var totalBytes int64
	dirtyShardsSet := make(map[int]struct{})

	for _, df := range discovered {
		totalBytes += df.size
		if cached, ok := existingByRel[df.relPath]; ok {
			if cached.FileSize == df.size && math.Abs(df.modTime.Sub(cached.ModifiedAt).Seconds()) < 1.5 {
				unchangedCount++
				continue
			}
		}
		toParse = append(toParse, df)
	}

	// For targeted scan, keep existing tracks outside cleanSub in total counts
	if isTargeted {
		subPrefix := filepath.ToSlash(cleanSub) + "/"
		for rel, tr := range existingByRel {
			if !strings.HasPrefix(rel, subPrefix) && rel != filepath.ToSlash(cleanSub) {
				totalBytes += tr.FileSize
			}
		}
	}

	// Parse new or modified audio files concurrently with a bounded worker pool
	workers := min(max(runtime.NumCPU(), 2), 8)
	if len(toParse) < workers {
		workers = max(len(toParse), 1)
	}

	var parsedMu sync.Mutex
	var upserted []db.TrackRecord
	var addedCount, updatedCount int
	var scanned atomic.Int64
	scanned.Store(int64(unchangedCount))

	// Cache folder artwork per directory during this scan to avoid re-reading cover.jpg 15x per album
	var dirArtMu sync.Mutex
	dirArtHash := make(map[string]string)

	jobs := make(chan discoveredFile, workers*2)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for df := range jobs {
				if ctx.Err() != nil {
					return
				}
				tr := idx.buildTrackRecord(ctx, lib, df.fullPath, df.relPath, df.format, df.ext, df.size, df.modTime, &dirArtMu, dirArtHash)
				n := int(scanned.Add(1))
				if n%20 == 0 || n == totalFound {
					idx.setStatus(ScanStatus{
						IsScanning:   true,
						LibraryID:    lib.ID,
						LibraryName:  lib.Name,
						ScannedCount: n,
						TotalFound:   totalFound,
						CurrentFile:  filepath.Base(df.relPath),
					})
				}

				parsedMu.Lock()
				if _, existed := existingByRel[df.relPath]; existed {
					updatedCount++
				} else {
					addedCount++
				}
				upserted = append(upserted, tr)
				dirtyShardsSet[tr.ShardID] = struct{}{}
				parsedMu.Unlock()
			}
		}()
	}

	for _, df := range toParse {
		jobs <- df
	}
	close(jobs)
	wg.Wait()

	// Detect removed tracks
	var removed []db.TrackRecord
	subPrefix := filepath.ToSlash(cleanSub) + "/"
	for rel, old := range existingByRel {
		if isTargeted && !strings.HasPrefix(rel, subPrefix) && rel != filepath.ToSlash(cleanSub) {
			continue
		}
		if _, stillExists := seenRel[rel]; !stillExists {
			removed = append(removed, old)
			dirtyShardsSet[old.ShardID] = struct{}{}
		}
	}

	finalTrackCount := len(existingByRel) + addedCount - len(removed)
	newRev, err := idx.store.CommitScanDelta(lib.ID, upserted, removed, finalTrackCount, totalBytes, idx.cfg.MaxDeltaRevs)
	if err != nil {
		idx.setStatus(ScanStatus{IsScanning: false, LastResult: fmt.Sprintf("Error: %v", err)})
		return ScanSummary{}, err
	}

	// Ensure all shards exist on first run or rebuild dirty shards
	existingShards, _ := idx.store.ListShards(lib.ID)
	var dirtyShards []int
	if len(existingShards) < lib.ShardCount {
		for s := 0; s < lib.ShardCount; s++ {
			dirtyShards = append(dirtyShards, s)
		}
	} else {
		for s := range dirtyShardsSet {
			dirtyShards = append(dirtyShards, s)
		}
	}

	if len(dirtyShards) > 0 {
		if err := idx.snap.BuildShards(lib.ID, dirtyShards, newRev); err != nil {
			log.Printf("[snapshot] shard rebuild error for %s: %v", lib.ID, err)
		}
	}

	// Mirror compressed index & discovery beacon to <libraryRoot>/.myecho/ when share is writable
	if idx.cfg.MirrorToShare {
		if libMeta, mErr := idx.store.GetLibrary(lib.ID); mErr == nil {
			host := idx.cfg.EffectiveAdvertiseHost()
			idx.snap.MirrorToShare(lib.Path, snapshot.ShareBeacon{
				ServerName:  idx.cfg.ServerName,
				ServerKind:  "echo-server",
				LibraryID:   lib.ID,
				LibraryName: lib.Name,
				LANURL:      fmt.Sprintf("http://%s:%d", host, idx.cfg.APIPort),
				PartyURL:    fmt.Sprintf("http://%s:%d", host, idx.cfg.PartyHTTPPort),
				APIPort:     idx.cfg.APIPort,
				PartyPort:   idx.cfg.PartyHTTPPort,
				UpdatedAt:   time.Now().UTC(),
			}, libMeta, dirtyShards)
		}
	}

	now := time.Now().UTC()
	dur := time.Since(start)
	summaryMsg := fmt.Sprintf("%s: %d total (+%d, ~%d, -%d) rev #%d in %s",
		lib.Name, finalTrackCount, addedCount, updatedCount, len(removed), newRev, dur.Round(time.Millisecond))
	idx.setStatus(ScanStatus{
		IsScanning:   false,
		LibraryID:    lib.ID,
		LibraryName:  lib.Name,
		ScannedCount: totalFound,
		TotalFound:   totalFound,
		LastResult:   summaryMsg,
		LastScanAt:   &now,
	})
	log.Printf("[indexer] %s", summaryMsg)

	if (addedCount > 0 || updatedCount > 0 || len(removed) > 0) && idx.onChanged != nil {
		idx.onChanged()
	}

	return ScanSummary{
		LibraryID:   lib.ID,
		Added:       addedCount,
		Updated:     updatedCount,
		Unchanged:   unchangedCount,
		Removed:     len(removed),
		Total:       finalTrackCount,
		NewRevision: newRev,
		DirtyShards: dirtyShards,
		Duration:    dur,
	}, nil
}

func (idx *Indexer) buildTrackRecord(
	ctx context.Context,
	lib config.LibraryConfig,
	fullPath, relPath, format, ext string,
	size int64,
	modTime time.Time,
	dirArtMu *sync.Mutex,
	dirArtHash map[string]string,
) db.TrackRecord {
	meta := ParseAudioFile(ctx, fullPath, format)

	var artHash *string
	if len(meta.ArtworkData) > 0 {
		if h := idx.artCache.Store(meta.ArtworkData); h != "" {
			artHash = &h
		}
	} else {
		dir := filepath.Dir(fullPath)
		dirArtMu.Lock()
		cachedHash, seen := dirArtHash[dir]
		dirArtMu.Unlock()
		if !seen {
			if raw := FindFolderArtwork(dir); len(raw) > 0 {
				cachedHash = idx.artCache.Store(raw)
			}
			dirArtMu.Lock()
			dirArtHash[dir] = cachedHash
			dirArtMu.Unlock()
		}
		if cachedHash != "" {
			h := cachedHash
			artHash = &h
		}
	}

	// Detect .lrc lyrics sidecar
	var lyricsRel *string
	lrcFull := strings.TrimSuffix(fullPath, filepath.Ext(fullPath)) + ".lrc"
	if _, err := os.Stat(lrcFull); err == nil {
		lr := strings.TrimSuffix(relPath, filepath.Ext(relPath)) + ".lrc"
		lyricsRel = &lr
	}

	fbTitle, fbArtist, fbAlbum, fbTrackNum := InferFromPath(relPath)
	title := firstNonEmpty(meta.Title, fbTitle)
	artist := firstNonEmpty(meta.Artist, fbArtist)
	albumArtist := firstNonEmpty(meta.AlbumArtist, artist)
	album := firstNonEmpty(meta.Album, fbAlbum)
	trackNum := meta.TrackNumber
	if trackNum == nil {
		trackNum = fbTrackNum
	}

	var legacyID string
	if lib.LegacyMacPrefix != "" {
		legacyFull := strings.TrimRight(lib.LegacyMacPrefix, "/") + "/" + relPath
		legacyID = StableTrackID(legacyFull)
	}

	addedAt := modTime
	return db.TrackRecord{
		TrackKey:           TrackKey(lib.ID, relPath),
		LegacyID:           legacyID,
		LibraryID:          lib.ID,
		RelativePath:       relPath,
		FileName:           filepath.Base(relPath),
		FileSize:           size,
		ModifiedAt:         modTime,
		AddedAt:            &addedAt,
		Format:             format,
		FileExtension:      ext,
		Title:              title,
		Artist:             artist,
		AlbumArtist:        albumArtist,
		Album:              album,
		TrackNumber:        trackNum,
		DiscNumber:         meta.DiscNumber,
		Year:               meta.Year,
		Genre:              meta.Genre,
		Duration:           meta.Duration,
		SampleRate:         meta.SampleRate,
		BitDepth:           meta.BitDepth,
		Bitrate:            meta.Bitrate,
		ArtworkHash:        artHash,
		HasEmbeddedArtwork: meta.HasEmbeddedArtwork,
		LyricsRelativePath: lyricsRel,
		ShardID:            ShardForPath(relPath, lib.ShardCount),
	}
}
