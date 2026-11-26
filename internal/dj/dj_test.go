package dj

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
)

// Verifies the user's core DJ cadence rule:
// - Every song is intro'd OR outro'd, NEVER both on the same song!
// - Acts organically/randomly across the playlist with seamless crossfades, intros, outros, and in/outro bridges.
func TestPlanOrganicCadenceInvariant(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		rng := mathrand.New(mathrand.NewSource(seed))
		slots := make([]DJSlot, 20)
		for i := range slots {
			slots[i] = DJSlot{
				ID:       randomID("slot"),
				Position: i,
				Track: DJTrackInfo{
					ID:     randomID("tr"),
					Title:  "Track",
					Artist: "Artist",
					Album:  "Album",
				},
			}
		}

		PlanOrganicCadence(slots, 0.65, true, rng)

		if slots[0].Intro != nil || slots[0].Outro != nil || slots[0].Bridge != nil {
			t.Fatalf("seed=%d expected slot 0 to start directly with NO standalone intro/outro", seed)
		}
		if slots[1].Bridge == nil {
			t.Fatalf("seed=%d expected slot 1 to have the combined Opening Greeting + Outro + Intro bridge", seed)
		}

		introCnt, outroCnt, bridgeCnt, silentCnt := 0, 0, 0, 0
		for i := range slots {
			hasIntro := slots[i].Intro != nil || slots[i].Bridge != nil
			hasOutro := slots[i].Outro != nil || (i+1 < len(slots) && slots[i+1].Bridge != nil)

			if hasIntro && hasOutro {
				t.Fatalf("seed=%d slot=%d violated invariant: song has BOTH intro and outro (Intro=%v Bridge=%v Outro=%v NextBridge=%v)",
					seed, i, slots[i].Intro != nil, slots[i].Bridge != nil, slots[i].Outro != nil, i+1 < len(slots) && slots[i+1].Bridge != nil)
			}
			if !hasIntro && !hasOutro {
				t.Fatalf("seed=%d slot=%d violated everyTrack invariant: song has NEITHER intro nor outro (stranded between 2 crossfades!)", seed, i)
			}
			if slots[i].Intro != nil {
				introCnt++
			}
			if slots[i].Outro != nil {
				outroCnt++
			}
			if slots[i].Bridge != nil {
				bridgeCnt++
			}
			if slots[i].Intro == nil && slots[i].Outro == nil && slots[i].Bridge == nil {
				silentCnt++
			}
		}

		if bridgeCnt == 0 {
			t.Fatalf("seed=%d expected at least the opening bridge", seed)
		}
		if silentCnt == 0 && introCnt == 0 && outroCnt == 0 {
			t.Fatalf("seed=%d expected organic variety across transitions", seed)
		}
	}

	// Also verify optional relaxed mode (everyTrack=false) still respects no-double-talk invariant
	rng := mathrand.New(mathrand.NewSource(99))
	relaxed := make([]DJSlot, 12)
	for i := range relaxed {
		relaxed[i] = DJSlot{ID: randomID("slot"), Position: i, Track: DJTrackInfo{ID: randomID("tr"), Title: "T", Artist: "A", Album: "B"}}
	}
	PlanOrganicCadence(relaxed, 0.35, false, rng)
	for i := range relaxed {
		hasIntro := relaxed[i].Intro != nil || relaxed[i].Bridge != nil
		hasOutro := relaxed[i].Outro != nil || (i+1 < len(relaxed) && relaxed[i+1].Bridge != nil)
		if hasIntro && hasOutro {
			t.Fatalf("relaxed mode slot=%d violated no-double-talk invariant", i)
		}
	}
}

func TestBuildFactualPromptUsesRealMetadataAndVocalCues(t *testing.T) {
	cfg := &config.Config{
		ConfigDir: t.TempDir(),
		DJ:        config.DefaultDJConfig(),
	}
	eng := NewEngine(cfg, nil, nil)
	yr := 1994
	genre := "Eurodance"
	tr := DJTrackInfo{
		ID:            "t1",
		Title:         "Set the World on Fire",
		Artist:        "E-Type",
		AlbumArtist:   "E-Type",
		Album:         "Made in Sweden",
		AlbumKey:      "made in sweden",
		Year:          &yr,
		Genre:         &genre,
		Duration:      224,
		BPM:           136,
		VocalStartSec: 14.5,
		VocalEndSec:   208.0,
		HasAnalysis:   true,
	}

	sys, usr, facts := eng.BuildFactualPrompt(context.Background(), cfg.DJ, BreakIntro, nil, tr, true, true)
	if !strings.Contains(sys, "GOOGLE SEARCH") || !strings.Contains(sys, "Echo FM") || !strings.Contains(sys, "STRICT TEMPORAL AWARENESS") {
		t.Fatalf("missing Google Search / station name / temporal awareness guidance in system prompt")
	}
	if !strings.Contains(usr, "NOT upcoming") {
		t.Fatalf("expected user prompt to annotate past release year as NOT upcoming, got:\n%s", usr)
	}
	for _, want := range []string{"Set the World on Fire", "E-Type", "Made in Sweden", "1994", "136 BPM", "14.5 seconds", "Lidarr"} {
		if !strings.Contains(usr, want) {
			t.Fatalf("expected factual prompt to contain %q, got:\n%s", want, usr)
		}
	}
	if len(facts) == 0 {
		t.Fatalf("expected non-empty facts summary")
	}
}

func TestProcessBroadcastDJVoiceProducesValidAudio(t *testing.T) {
	// Generate 0.6s of synthetic 24kHz 16-bit mono PCM sine wave
	const sr = 24000
	samples := sr * 6 / 10
	pcm := make([]byte, samples*2)
	for i := 0; i < samples; i++ {
		val := int16(4000 * ((i % 60) - 30) / 30)
		pcm[i*2] = byte(val)
		pcm[i*2+1] = byte(val >> 8)
	}
	outPath := filepath.Join(t.TempDir(), "dj-test.m4a")
	dur, err := ProcessBroadcastDJVoice(context.Background(), pcm, sr, outPath)
	if err != nil {
		t.Fatalf("ProcessBroadcastDJVoice failed: %v", err)
	}
	fi, err := os.Stat(outPath)
	if err != nil || fi.Size() < 512 {
		t.Fatalf("expected non-empty processed audio file, err=%v size=%d", err, fi.Size())
	}
	if dur < 0.3 || dur > 2.0 {
		t.Fatalf("unexpected processed duration: %.2fs", dur)
	}
}

func TestMatchesProfileEraBPMStarsAndAutoRefill(t *testing.T) {
	yr94 := 1994
	yr22 := 2022
	gDance := "Eurodance"
	gMetal := "Metal"

	prof := config.DJProfile{
		ID:            "90s-dance",
		Name:          "90s Dance",
		Genres:        []string{"Dance"},
		ExcludeGenres: []string{"Metal"},
		Eras:          []string{"1990s"},
		MinBPM:        120,
		MaxBPM:        150,
		MinRating:     4,
	}

	trGood := db.TrackRecord{TrackKey: "k1", Title: "Hit", Album: "A1", Year: &yr94, Genre: &gDance}
	trWrongEra := db.TrackRecord{TrackKey: "k2", Title: "New", Album: "A2", Year: &yr22, Genre: &gDance}
	trExcluded := db.TrackRecord{TrackKey: "k3", Title: "Heavy", Album: "A3", Year: &yr94, Genre: &gMetal}

	anFast := db.TrackAnalysis{TrackKey: "k1", BPM: 136}
	anSlow := db.TrackAnalysis{TrackKey: "k1", BPM: 82}
	ratings := map[string]int{"k1": 5, "k2": 5, "k3": 5}

	if !MatchesProfile(trGood, prof, anFast, db.AlbumEditorial{}, ratings, nil) {
		t.Fatalf("expected trGood to match 90s-dance profile")
	}
	if MatchesProfile(trGood, prof, anSlow, db.AlbumEditorial{}, ratings, nil) {
		t.Fatalf("expected slow BPM track to be rejected by profile")
	}
	if MatchesProfile(trWrongEra, prof, anFast, db.AlbumEditorial{}, ratings, nil) {
		t.Fatalf("expected 2022 track to be rejected by 1990s era filter")
	}
	if MatchesProfile(trExcluded, prof, anFast, db.AlbumEditorial{}, ratings, nil) {
		t.Fatalf("expected excluded Metal genre to be rejected")
	}
	if MatchesProfile(trGood, prof, anFast, db.AlbumEditorial{}, map[string]int{"k1": 2}, nil) {
		t.Fatalf("expected 2-star track to be rejected by MinRating=4")
	}

	// AvoidMaxRating=2: allow unrated (0) & 3+ stars, reject 1-2 stars!
	avoidProf := config.DJProfile{ID: "avoid-bad", Name: "No Bad Songs", AvoidMaxRating: 2}
	if !MatchesProfile(trGood, avoidProf, anFast, db.AlbumEditorial{}, map[string]int{}, nil) {
		t.Fatalf("expected unrated song (0 stars) to be ALLOWED by AvoidMaxRating=2")
	}
	if !MatchesProfile(trGood, avoidProf, anFast, db.AlbumEditorial{}, map[string]int{"k1": 3}, nil) {
		t.Fatalf("expected 3-star song to be ALLOWED by AvoidMaxRating=2")
	}
	if MatchesProfile(trGood, avoidProf, anFast, db.AlbumEditorial{}, map[string]int{"k1": 1}, nil) {
		t.Fatalf("expected 1-star song to be BLOCKED by AvoidMaxRating=2")
	}
	if MatchesProfile(trGood, avoidProf, anFast, db.AlbumEditorial{}, map[string]int{"k1": 2}, nil) {
		t.Fatalf("expected 2-star song to be BLOCKED by AvoidMaxRating=2")
	}
}

func TestTrimTTSTailArtifactRemovesEOSDigitalBurst(t *testing.T) {
	const sr = 24000
	// Build 2.0s speech + 0.30s silence + 0.20s loud digital noise burst at EOF (exact Gemini 3.8 TTS EOS bug signature)
	speechSamples := sr * 20 / 10
	silenceSamples := sr * 3 / 10
	glitchSamples := sr * 2 / 10
	total := speechSamples + silenceSamples + glitchSamples
	pcm := make([]byte, total*2)

	rng := mathrand.New(mathrand.NewSource(42))
	for i := 0; i < total; i++ {
		var val int16
		if i < speechSamples {
			// Clean 300 Hz speech-like tone (-12 dB)
			val = int16(8000 * ((i % 80) - 40) / 40)
		} else if i < speechSamples+silenceSamples {
			// Deep silence (-70 dB)
			val = int16(rng.Intn(7) - 3)
		} else {
			// Corrupted EOS digital noise burst (-6 dB)
			val = int16(rng.Intn(30000) - 15000)
		}
		pcm[i*2] = byte(val)
		pcm[i*2+1] = byte(val >> 8)
	}

	cleaned := trimTTSTailArtifact(pcm, sr)
	cleanedSec := float64(len(cleaned)) / float64(sr*2)
	if cleanedSec < 1.95 || cleanedSec > 2.12 {
		t.Fatalf("expected EOS digital glitch to be trimmed cleanly around 2.04s, got %.3fs", cleanedSec)
	}

	// Verify clean speech with NO trailing glitch is NOT cut off
	cleanSpeech := pcm[:speechSamples*2]
	outClean := trimTTSTailArtifact(cleanSpeech, sr)
	if len(outClean) != len(cleanSpeech) {
		t.Fatalf("expected clean speech without glitch to keep full length (%d bytes), got %d bytes", len(cleanSpeech), len(outClean))
	}
}

func TestExtractWAVPCMStripsTrailingMetadataChunk(t *testing.T) {
	// Build a valid 24kHz mono WAV and append a fake 4KB trailing C2PA metadata chunk (like Gemini 3.8 TTS)
	pcm := make([]byte, 4800)
	for i := range pcm {
		pcm[i] = byte(i % 127)
	}
	wav := wrapPCMAsWAV(pcm, 24000, 1, 16)
	trailing := make([]byte, 8+4096)
	copy(trailing[0:4], "c2pa")
	trailing[4] = 0x00
	trailing[5] = 0x10 // 4096 little-endian
	for i := 8; i < len(trailing); i++ {
		trailing[i] = 0xFF // loud binary garbage if mistakenly decoded as PCM!
	}
	combined := append(wav, trailing...)

	gotPCM, gotSR, gotCh, ok := extractWAVPCM(combined)
	if !ok {
		t.Fatalf("expected WAV container to be recognized")
	}
	if gotSR != 24000 || gotCh != 1 {
		t.Fatalf("expected 24000Hz 1ch, got %dHz %dch", gotSR, gotCh)
	}
	if len(gotPCM) != len(pcm) {
		t.Fatalf("expected trailing c2pa chunk to be completely excluded (want %d bytes, got %d bytes)", len(pcm), len(gotPCM))
	}
}

func TestLooksCutOffMidSentence(t *testing.T) {
	if !looksCutOffMidSentence("That was Ike & Tina Turner. Up next, from German") {
		t.Fatalf("expected truncated mid-sentence script to be detected")
	}
	if looksCutOffMidSentence("Up next, ATB with The Autumn Leaves.") {
		t.Fatalf("expected complete sentence ending in period to pass")
	}
	if looksCutOffMidSentence(`Here is "The Autumn Leaves."`) {
		t.Fatalf("expected complete sentence ending in quote to pass")
	}
}

func TestMoveSlotRecreatesAffectedBridgesAndPreservesInvariants(t *testing.T) {
	cfg := &config.Config{
		ConfigDir: t.TempDir(),
		DJ:        config.DefaultDJConfig(),
	}
	eng := NewEngine(cfg, nil, nil)
	slots := make([]DJSlot, 6)
	for i := range slots {
		slots[i] = DJSlot{
			ID:       fmt.Sprintf("slot-%d", i),
			Position: i,
			Track: DJTrackInfo{
				ID:     fmt.Sprintf("tr-%d", i),
				Title:  fmt.Sprintf("Song %d", i),
				Artist: fmt.Sprintf("Artist %d", i),
			},
		}
	}
	// Set up ready bridges at slots[1] (tr-0 -> tr-1), slots[3] (tr-2 -> tr-3), slots[5] (tr-4 -> tr-5)
	slots[1].Bridge = &DJBreak{ID: "brk-1", Kind: BreakBridge, Status: StatusReady, Script: "0 to 1"}
	slots[3].Bridge = &DJBreak{ID: "brk-3", Kind: BreakBridge, Status: StatusReady, Script: "2 to 3"}
	slots[5].Bridge = &DJBreak{ID: "brk-5", Kind: BreakBridge, Status: StatusReady, Script: "4 to 5"}

	eng.mu.Lock()
	eng.sessionID = "sess-1"
	eng.slots = slots
	eng.currentIndex = 1 // tr-1 is playing
	eng.mu.Unlock()

	// Drag tr-4 (index 4) up to index 2 (between tr-1 and tr-2)
	st, err := eng.MoveSlot(4, 2)
	if err != nil {
		t.Fatalf("MoveSlot failed: %v", err)
	}
	// Verify order: tr-0, tr-1, tr-4, tr-2, tr-3, tr-5
	wantOrder := []string{"tr-0", "tr-1", "tr-4", "tr-2", "tr-3", "tr-5"}
	for i, want := range wantOrder {
		if st.Slots[i].Track.ID != want {
			t.Fatalf("pos %d: want %s, got %s", i, want, st.Slots[i].Track.ID)
		}
	}
	// Verify currentIndex stayed at 1 (tr-1)
	if st.CurrentIndex != 1 {
		t.Fatalf("expected currentIndex to remain 1, got %d", st.CurrentIndex)
	}
	// Unaffected bridge on tr-1 (tr-0 -> tr-1) must stay READY (not regenerated!)
	if st.Slots[1].Bridge == nil || st.Slots[1].Bridge.Status != StatusReady || st.Slots[1].Bridge.ID != "brk-1" {
		t.Fatalf("expected unaffected bridge brk-1 to remain READY, got %+v", st.Slots[1].Bridge)
	}
	// Affected bridge on tr-5 (formerly tr-4 -> tr-5, now tr-3 -> tr-5) must NOT keep stale brk-5!
	if st.Slots[5].Bridge != nil && st.Slots[5].Bridge.ID == "brk-5" {
		t.Fatalf("expected stale bridge brk-5 to be replaced after neighbor changed")
	}
	// Verify strict XOR invariant on EVERY track (exactly one of intro/outro, never 0, never 2!)
	for i := range st.Slots {
		hasIntro := st.Slots[i].Intro != nil || st.Slots[i].Bridge != nil
		hasOutro := st.Slots[i].Outro != nil || (i+1 < len(st.Slots) && st.Slots[i+1].Bridge != nil)
		if hasIntro == hasOutro {
			t.Fatalf("slot %d (%s) violated XOR invariant: hasIntro=%v hasOutro=%v", i, st.Slots[i].Track.ID, hasIntro, hasOutro)
		}
	}
}
