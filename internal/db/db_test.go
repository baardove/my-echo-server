package db

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestPlaylistUUIDCanonicalizationAndDeduplication(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "echo.sqlite")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	lowerID := "550e8400-e29b-41d4-a716-446655440000"
	upperID := "550E8400-E29B-41D4-A716-446655440000"
	t1 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(10 * time.Second)

	// Simulate legacy duplicate rows inserted directly into SQLite (Go lowercase + Mac uppercase).
	pLower := SharedPlaylist{
		ID:        lowerID,
		Name:      "Funky",
		TrackIDs:  []string{"tr1", "tr2"},
		CreatedAt: t1,
		UpdatedAt: t1,
	}
	pUpper := SharedPlaylist{
		ID:        upperID,
		Name:      "Funky (Updated)",
		TrackIDs:  []string{"tr1", "tr2", "tr3"},
		CreatedAt: t1,
		UpdatedAt: t2,
	}
	rawLower, _ := json.Marshal(pLower)
	rawUpper, _ := json.Marshal(pUpper)

	if _, err := store.sql.Exec(`INSERT INTO playlists (id, updated_at, deleted, json) VALUES (?, ?, 0, ?)`, lowerID, t1.Unix(), string(rawLower)); err != nil {
		t.Fatalf("insert lower: %v", err)
	}
	if _, err := store.sql.Exec(`INSERT INTO playlists (id, updated_at, deleted, json) VALUES (?, ?, 0, ?)`, upperID, t2.Unix(), string(rawUpper)); err != nil {
		t.Fatalf("insert upper: %v", err)
	}

	// Also insert a tombstone tie pair where deleted row should beat live row at equal timestamp.
	delMixed := "11111111-2222-3333-4444-55555555555a"
	delUpper2 := "11111111-2222-3333-4444-55555555555A"
	pLive := SharedPlaylist{ID: delMixed, Name: "DeletedPl", TrackIDs: []string{"tr1"}, CreatedAt: t1, UpdatedAt: t1}
	pDead := SharedPlaylist{ID: delUpper2, Name: "", Deleted: true, CreatedAt: t1, UpdatedAt: t1}
	rawLive, _ := json.Marshal(pLive)
	rawDead, _ := json.Marshal(pDead)
	if _, err := store.sql.Exec(`INSERT INTO playlists (id, updated_at, deleted, json) VALUES (?, ?, 0, ?)`, delMixed, t1.Unix(), string(rawLive)); err != nil {
		t.Fatalf("insert live: %v", err)
	}
	if _, err := store.sql.Exec(`INSERT INTO playlists (id, updated_at, deleted, json) VALUES (?, ?, 1, ?)`, delUpper2, t1.Unix(), string(rawDead)); err != nil {
		t.Fatalf("insert dead: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open to run startup migration deduplication.
	store, err = Open(dbPath)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer store.Close()

	live, err := store.ListPlaylists(false)
	if err != nil {
		t.Fatalf("ListPlaylists(false): %v", err)
	}
	if len(live) != 1 {
		t.Fatalf("expected 1 live playlist after deduplication, got %d: %+v", len(live), live)
	}
	if live[0].ID != upperID {
		t.Fatalf("expected canonical uppercase ID %q, got %q", upperID, live[0].ID)
	}
	if live[0].Name != "Funky (Updated)" {
		t.Fatalf("expected newer playlist to win, got %q", live[0].Name)
	}

	all, err := store.ListPlaylists(true)
	if err != nil {
		t.Fatalf("ListPlaylists(true): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 total playlists (1 live + 1 tombstone), got %d", len(all))
	}

	// GetPlaylist should match regardless of UUID casing.
	got, err := store.GetPlaylist(lowerID)
	if err != nil || got == nil || got.ID != upperID {
		t.Fatalf("GetPlaylist(lowerID) = (%+v, %v), want ID=%q", got, err, upperID)
	}

	// Merging lowercase and uppercase updates should never create duplicates.
	t3 := t2.Add(30 * time.Second)
	if err := store.MergePlaylists([]SharedPlaylist{
		{ID: lowerID, Name: "Funky v3", TrackIDs: []string{"tr1"}, CreatedAt: t1, UpdatedAt: t3},
	}); err != nil {
		t.Fatalf("MergePlaylists(lower): %v", err)
	}
	if err := store.MergePlaylists([]SharedPlaylist{
		{ID: upperID, Name: "Funky v3", TrackIDs: []string{"tr1"}, CreatedAt: t1, UpdatedAt: t3},
	}); err != nil {
		t.Fatalf("MergePlaylists(upper): %v", err)
	}
	// Merging a live copy with equal timestamp as the tombstone must NOT resurrect it.
	if err := store.MergePlaylists([]SharedPlaylist{
		{ID: delMixed, Name: "Zombie", TrackIDs: []string{"tr1"}, CreatedAt: t1, UpdatedAt: t1},
	}); err != nil {
		t.Fatalf("MergePlaylists(zombie): %v", err)
	}

	live, err = store.ListPlaylists(false)
	if err != nil {
		t.Fatalf("ListPlaylists(false) after merge: %v", err)
	}
	if len(live) != 1 || live[0].Name != "Funky v3" || live[0].ID != upperID {
		t.Fatalf("unexpected playlists after merge: %+v", live)
	}

	// Non-UUID IDs should remain intact.
	if err := store.SavePlaylist(SharedPlaylist{ID: "pl-custom-1", Name: "Custom", CreatedAt: t1, UpdatedAt: t1}); err != nil {
		t.Fatalf("SavePlaylist(custom): %v", err)
	}
	custom, err := store.GetPlaylist("pl-custom-1")
	if err != nil || custom == nil || custom.ID != "pl-custom-1" {
		t.Fatalf("GetPlaylist(custom) = (%+v, %v)", custom, err)
	}
}
