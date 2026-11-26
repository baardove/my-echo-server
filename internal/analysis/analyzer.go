package analysis

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo/server/internal/config"
	"github.com/baardove/my-echo/server/internal/db"
)

const (
	AnalysisSampleRate = 11025
	AnalysisVersion    = 2
	TargetLoudnessDB   = -14.0
)

type Status struct {
	Enabled       bool   `json:"enabled"`
	IsAnalyzing   bool   `json:"isAnalyzing"`
	AnalyzedCount int    `json:"analyzedCount"`
	TotalTracks   int    `json:"totalTracks"`
	CurrentTrack  string `json:"currentTrack,omitempty"`
	LastError     string `json:"lastError,omitempty"`
}

type Analyzer struct {
	cfg   *config.Config
	store *db.Store

	mu          sync.RWMutex
	status      Status
	cancelRun   context.CancelFunc
	trigger     chan struct{}
	workerOnce  sync.Once
}

func NewAnalyzer(cfg *config.Config, store *db.Store) *Analyzer {
	return &Analyzer{
		cfg:     cfg,
		store:   store,
		trigger: make(chan struct{}, 2),
	}
}

func (a *Analyzer) Status() Status {
	a.mu.RLock()
	st := a.status
	a.mu.RUnlock()
	st.Enabled = a.cfg.AdvancedAnalysisEnabled
	if st.TotalTracks == 0 {
		if libs, err := a.store.ListLibraries(); err == nil {
			for _, l := range libs {
				st.TotalTracks += l.TrackCount
			}
		}
	}
	if st.AnalyzedCount == 0 {
		if m, err := a.store.ListTrackAnalysisMap(); err == nil {
			st.AnalyzedCount = len(m)
		}
	}
	return st
}

func (a *Analyzer) StartWorker(ctx context.Context) {
	a.workerOnce.Do(func() {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			for {
				if a.cfg.AdvancedAnalysisEnabled {
					a.runPass(ctx)
				}
				select {
				case <-ctx.Done():
					return
				case <-a.trigger:
				case <-time.After(30 * time.Minute):
				}
			}
		}()
	})
}

func (a *Analyzer) TriggerRun() {
	select {
	case a.trigger <- struct{}{}:
	default:
	}
}

func (a *Analyzer) StopCurrentRun() {
	a.mu.Lock()
	if a.cancelRun != nil {
		a.cancelRun()
	}
	a.mu.Unlock()
}

func (a *Analyzer) runPass(parentCtx context.Context) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		a.mu.Lock()
		a.status.LastError = "ffmpeg not available in PATH"
		a.mu.Unlock()
		return
	}

	tracks, err := a.store.LoadAllTracks()
	if err != nil || len(tracks) == 0 {
		return
	}
	existing, _ := a.store.ListTrackAnalysisMap()
	edList, _ := a.store.ListAlbumEditorial()
	edByAlb := make(map[string]db.AlbumEditorial, len(edList))
	for _, ed := range edList {
		edByAlb[strings.ToLower(ed.AlbumKey)] = ed
	}

	var todo []db.TrackRecord
	for _, tr := range tracks {
		if ex, ok := existing[tr.TrackKey]; !ok || ex.Version < AnalysisVersion {
			todo = append(todo, tr)
		}
	}

	analyzedCnt := len(existing)
	a.mu.Lock()
	a.status = Status{
		Enabled:       a.cfg.AdvancedAnalysisEnabled,
		IsAnalyzing:   len(todo) > 0,
		AnalyzedCount: analyzedCnt,
		TotalTracks:   len(tracks),
	}
	if len(todo) == 0 {
		a.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(parentCtx)
	a.cancelRun = cancel
	a.mu.Unlock()
	defer func() {
		cancel()
		a.mu.Lock()
		a.cancelRun = nil
		a.status.IsAnalyzing = false
		a.status.CurrentTrack = ""
		a.mu.Unlock()
	}()

	log.Printf("[analysis] starting optional deep audio analysis on %d pending tracks...", len(todo))
	for _, tr := range todo {
		if runCtx.Err() != nil || !a.cfg.AdvancedAnalysisEnabled {
			return
		}
		lib, ok := a.cfg.LibraryByID(tr.LibraryID)
		if !ok {
			continue
		}
		fullPath := filepath.Join(lib.Path, filepath.FromSlash(tr.RelativePath))
		if _, err := os.Stat(fullPath); err != nil {
			continue
		}

		a.mu.Lock()
		a.status.CurrentTrack = fmt.Sprintf("%s — %s", tr.Artist, tr.Title)
		a.status.AnalyzedCount = analyzedCnt
		a.status.TotalTracks = len(tracks)
		a.mu.Unlock()

		var albEd *db.AlbumEditorial
		if ed, ok := edByAlb[strings.ToLower(strings.TrimSpace(tr.Album))]; ok {
			cp := ed
			albEd = &cp
		}

		res, err := AnalyzeTrackFile(runCtx, fullPath, tr, albEd)
		if err != nil {
			// Skip unreadable file without aborting pass
			continue
		}
		if err := a.store.UpsertTrackAnalysis(res); err == nil {
			analyzedCnt++
		}

		// Gentle CPU pacing so NAS stays responsive during playback
		select {
		case <-runCtx.Done():
			return
		case <-time.After(60 * time.Millisecond):
		}
	}
	log.Printf("[analysis] deep audio analysis pass complete: %d/%d tracks analyzed", analyzedCnt, len(tracks))
}

// AnalyzeTrackOnDemand analyzes a single track immediately (if not already cached in track_analysis)
// and persists BPM, vocal start/end cues, energy, canonical genre, and smart tags to SQLite.
// Used for fast on-the-go analysis of DJ rundown tracks without scanning the full library.
func (a *Analyzer) AnalyzeTrackOnDemand(ctx context.Context, trackID string) (db.TrackAnalysis, error) {
	if strings.TrimSpace(trackID) == "" || a.store == nil {
		return db.TrackAnalysis{}, fmt.Errorf("missing track ID")
	}
	tr, err := a.store.FindTrackByID(trackID)
	if err != nil {
		return db.TrackAnalysis{}, err
	}
	if cached, ok := a.store.GetTrackAnalysis(tr.TrackKey); ok && cached.Version >= AnalysisVersion && cached.BPM > 0 {
		return cached, nil
	}
	lib, ok := a.cfg.LibraryByID(tr.LibraryID)
	if !ok {
		return db.TrackAnalysis{}, fmt.Errorf("library %q not mounted", tr.LibraryID)
	}
	fullPath := filepath.Join(lib.Path, filepath.FromSlash(tr.RelativePath))
	if _, err := os.Stat(fullPath); err != nil {
		return db.TrackAnalysis{}, err
	}
	var albEd *db.AlbumEditorial
	if edList, err := a.store.ListAlbumEditorial(); err == nil {
		albKey := strings.ToLower(strings.TrimSpace(tr.Album))
		for i := range edList {
			if strings.ToLower(edList[i].AlbumKey) == albKey {
				cp := edList[i]
				albEd = &cp
				break
			}
		}
	}
	res, err := AnalyzeTrackFile(ctx, fullPath, *tr, albEd)
	if err != nil {
		return db.TrackAnalysis{}, err
	}
	if err := a.store.UpsertTrackAnalysis(res); err != nil {
		return res, err
	}
	a.mu.Lock()
	if m, err := a.store.ListTrackAnalysisMap(); err == nil {
		a.status.AnalyzedCount = len(m)
	}
	a.mu.Unlock()
	log.Printf("[analysis] on-the-go analyzed %q — %s: %.0f BPM, %.1f dB (gain %+.1f dB), vocal %.1fs–%.1fs", tr.Title, tr.Artist, res.BPM, res.LoudnessDB, res.NormGainDB, res.VocalStartSec, res.VocalEndSec)
	return res, nil
}

// AnalyzeTrackFile decodes stereo 11,025 Hz PCM via ffmpeg and runs BPM, loudness/peak, vocal-entry/exit, energy, and tag analysis.
func AnalyzeTrackFile(ctx context.Context, filePath string, tr db.TrackRecord, albEd *db.AlbumEditorial) (db.TrackAnalysis, error) {
	ffBin, err := exec.LookPath("ffmpeg")
	if err != nil {
		return db.TrackAnalysis{}, err
	}
	tctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	// Decode up to first 7 minutes at stereo 11025 Hz s16le (~18 MB max in RAM, completes in ~150ms)
	cmd := exec.CommandContext(tctx, ffBin,
		"-v", "error",
		"-i", filePath,
		"-t", "420",
		"-vn",
		"-ac", "2",
		"-ar", "11025",
		"-f", "s16le",
		"pipe:1",
	)
	raw, err := cmd.Output()
	if err != nil || len(raw) < AnalysisSampleRate*4 {
		return db.TrackAnalysis{}, fmt.Errorf("ffmpeg decode failed: %w", err)
	}

	left, right := decodeStereoS16LE(raw)
	dur := tr.Duration
	if dur <= 0 {
		dur = float64(len(left)) / float64(AnalysisSampleRate)
	}

	bpm, vocalStart, vocalEnd, energy := AnalyzeStereoPCM(left, right, AnalysisSampleRate, dur)
	loudnessDB, peakLinear, normGainDB := ComputeLoudnessAndGain(left, right, AnalysisSampleRate)
	canonGenre, tags := BuildGenreAndTags(tr, albEd, bpm, energy, vocalStart, vocalEnd, dur)

	return db.TrackAnalysis{
		TrackKey:      tr.TrackKey,
		BPM:           math.Round(bpm*10) / 10,
		VocalStartSec: math.Round(vocalStart*10) / 10,
		VocalEndSec:   math.Round(vocalEnd*10) / 10,
		Energy:        math.Round(energy*100) / 100,
		LoudnessDB:    math.Round(loudnessDB*10) / 10,
		PeakLinear:    math.Round(peakLinear*1000) / 1000,
		NormGainDB:    math.Round(normGainDB*10) / 10,
		Genre:         canonGenre,
		Tags:          tags,
		AnalyzedAt:    time.Now().UTC(),
		Version:       AnalysisVersion,
	}, nil
}

// ComputeLoudnessAndGain measures EBU R128-style 400ms gated integrated stereo loudness (dBFS),
// sample peak (0..1), and a peak-safe ReplayGain-style normalization adjustment targeting -14.0 dBFS.
func ComputeLoudnessAndGain(left, right []float64, sr int) (loudnessDB, peakLinear, normGainDB float64) {
	n := len(left)
	if n == 0 || sr <= 0 {
		return TargetLoudnessDB, 0, 0
	}
	if len(right) < n {
		n = len(right)
	}

	// Track true sample peak across both channels
	for i := 0; i < n; i++ {
		al := math.Abs(left[i])
		ar := math.Abs(right[i])
		if al > peakLinear {
			peakLinear = al
		}
		if ar > peakLinear {
			peakLinear = ar
		}
	}

	// 400ms overlapping (50% hop) stereo loudness blocks
	win := (sr * 4) / 10
	if win < 256 {
		win = 256
	}
	hop := win / 2
	var blockPowers []float64
	var absSum float64
	absCount := 0
	const absGatePower = 3.16227766e-6 // -55 dBFS
	for start := 0; start+win <= n; start += hop {
		var p float64
		for i := start; i < start+win; i++ {
			p += 0.5 * (left[i]*left[i] + right[i]*right[i])
		}
		p /= float64(win)
		if p >= absGatePower {
			blockPowers = append(blockPowers, p)
			absSum += p
			absCount++
		}
	}
	if absCount == 0 {
		return -40.0, peakLinear, 0
	}

	// Relative gate: -10 dB below ungated average power (0.1x power)
	relGate := (absSum / float64(absCount)) * 0.1
	var gatedSum float64
	gatedCount := 0
	for _, p := range blockPowers {
		if p >= relGate {
			gatedSum += p
			gatedCount++
		}
	}
	meanPower := absSum / float64(absCount)
	if gatedCount > 0 {
		meanPower = gatedSum / float64(gatedCount)
	}
	if meanPower < 1e-9 {
		return -45.0, peakLinear, 0
	}

	loudnessDB = 10.0 * math.Log10(meanPower)
	normGainDB = TargetLoudnessDB - loudnessDB
	if normGainDB < -10.0 {
		normGainDB = -10.0
	}
	if normGainDB > 6.0 {
		normGainDB = 6.0
	}
	// Peak-safe headroom clamp: ensure peakLinear * 10^(gain/20) <= 0.96
	if peakLinear > 0.01 {
		maxGainDB := 20.0 * math.Log10(0.96/peakLinear)
		if normGainDB > maxGainDB {
			normGainDB = maxGainDB
		}
	}
	return loudnessDB, peakLinear, normGainDB
}

// AnalyzeStereoPCM performs pure-Go signal analysis on stereo PCM samples (11025 Hz):
// 1. Estimates BPM via half-wave onset flux + tempo-prior autocorrelation (68–178 BPM).
// 2. Detects Vocal Start & Vocal End using center-channel (L+R) vocal-formant bandpass (300–3400 Hz)
//    vs side/bass energy, self-calibrated against the track's own mid-song vocal energy profile.
// 3. Computes normalized RMS energy (0.0..1.0).
func AnalyzeStereoPCM(left, right []float64, sr int, totalDuration float64) (bpm, vocalStartSec, vocalEndSec, energy float64) {
	n := len(left)
	if n == 0 || sr <= 0 {
		return 0, 0, totalDuration, 0
	}
	decodedDur := float64(n) / float64(sr)
	if totalDuration <= 0 {
		totalDuration = decodedDur
	}

	// Build mid (center-panned, where lead vocals sit) and side signals
	mid := make([]float64, n)
	side := make([]float64, n)
	var sumSq float64
	for i := 0; i < n; i++ {
		m := 0.5 * (left[i] + right[i])
		s := 0.5 * (left[i] - right[i])
		mid[i] = m
		side[i] = s
		sumSq += m * m
	}
	rms := math.Sqrt(sumSq / float64(n))
	// Map typical mastered music RMS (0.04..0.32) smoothly to 0..1
	energy = clamp((rms-0.025)/0.26, 0.05, 1.0)

	// 1. BPM Estimation on core section (skip first 10% and last 10%, max 75s)
	bpm = estimateBPM(mid, sr)

	// 2. Vocal Start / End Detection via 300–3400 Hz vocal-band biquad filter on Mid & Side
	midVocal := bandpassBiquad(mid, float64(sr), 1350.0, 0.65)
	sideVocal := bandpassBiquad(side, float64(sr), 1350.0, 0.65)

	winSamples := sr / 4 // 0.25s analysis frames
	if winSamples < 64 {
		winSamples = 64
	}
	numFrames := n / winSamples
	if numFrames < 8 {
		return bpm, 0, totalDuration, energy
	}

	vocalScore := make([]float64, numFrames)
	for f := 0; f < numFrames; f++ {
		start := f * winSamples
		end := start + winSamples
		var mE, sE, fullE float64
		for i := start; i < end; i++ {
			mv := midVocal[i]
			sv := sideVocal[i]
			mE += mv * mv
			sE += sv * sv
			fullE += mid[i] * mid[i]
		}
		mRMS := math.Sqrt(mE / float64(winSamples))
		sRMS := math.Sqrt(sE / float64(winSamples))
		fRMS := math.Sqrt(fullE / float64(winSamples))

		if fRMS < 0.008 {
			vocalScore[f] = 0
			continue
		}
		// Center-channel vocal dominance: lead singing concentrates energy in Mid 300-3400Hz vs Side & sub-bass
		centerDominance := mRMS / (sRMS + mRMS + 1e-6)
		bandShare := mRMS / (fRMS + 1e-6)
		vocalScore[f] = mRMS * (0.45 + 0.55*centerDominance) * clamp(bandShare*1.6, 0.2, 1.2)
	}

	// Self-calibrate threshold using 75th percentile of mid-song core (20%..80% of track)
	coreStart := numFrames / 5
	coreEnd := (numFrames * 4) / 5
	if coreEnd <= coreStart+2 {
		coreStart = 0
		coreEnd = numFrames
	}
	coreVals := append([]float64(nil), vocalScore[coreStart:coreEnd]...)
	sort.Float64s(coreVals)
	p75 := coreVals[int(float64(len(coreVals)-1)*0.75)]
	threshold := math.Max(0.012, p75*0.52)

	// Find Vocal Start: first sustained 3-frame (0.75s) vocal entry inside first 40% (max 55s)
	maxIntroFrame := min(numFrames-3, int(math.Min(55.0, decodedDur*0.40)/0.25))
	vocalStartFrame := 0
	foundStart := false
	for f := 0; f <= maxIntroFrame; f++ {
		if vocalScore[f] >= threshold && vocalScore[f+1] >= threshold*0.92 && vocalScore[f+2] >= threshold*0.88 {
			vocalStartFrame = f
			foundStart = true
			break
		}
	}
	if !foundStart {
		// Instrumental or very quiet vocals: safe default intro ramp of min(12s, 15% duration)
		vocalStartSec = math.Min(12.0, totalDuration*0.15)
	} else {
		vocalStartSec = float64(vocalStartFrame) * 0.25
	}

	// Find Vocal End: last sustained 3-frame vocal activity in final 40% (searching backward from end)
	minOutroFrame := max(2, int(float64(numFrames)*0.55))
	vocalEndFrame := numFrames - 1
	foundEnd := false
	for f := numFrames - 1; f >= minOutroFrame; f-- {
		if vocalScore[f] >= threshold*0.90 && vocalScore[f-1] >= threshold*0.85 {
			vocalEndFrame = f + 1
			foundEnd = true
			break
		}
	}
	if !foundEnd {
		vocalEndSec = math.Max(vocalStartSec+5, totalDuration-10.0)
	} else {
		// Scale to full track duration if track was longer than 7-minute decode cap
		rawEnd := float64(vocalEndFrame) * 0.25
		if totalDuration > decodedDur+1.0 {
			tailGap := decodedDur - rawEnd
			vocalEndSec = math.Max(0, totalDuration-tailGap)
		} else {
			vocalEndSec = math.Min(totalDuration, rawEnd)
		}
	}

	return bpm, vocalStartSec, vocalEndSec, energy
}

func estimateBPM(mono []float64, sr int) float64 {
	n := len(mono)
	start := n / 8
	end := n - n/8
	maxSamples := sr * 75
	if end-start > maxSamples {
		midPt := (start + end) / 2
		start = midPt - maxSamples/2
		end = start + maxSamples
	}
	if end-start < sr*6 {
		start = 0
		end = n
	}
	seg := mono[start:end]

	hop := 256
	frameSize := 512
	numFrames := (len(seg) - frameSize) / hop
	if numFrames < 100 {
		return 115.0
	}

	// Energy envelope + half-wave onset flux
	energies := make([]float64, numFrames)
	for f := 0; f < numFrames; f++ {
		off := f * hop
		var e float64
		for i := 0; i < frameSize; i++ {
			v := seg[off+i]
			e += v * v
		}
		energies[f] = math.Log1p(100.0 * math.Sqrt(e/float64(frameSize)))
	}

	onset := make([]float64, numFrames)
	var meanOnset float64
	for f := 1; f < numFrames; f++ {
		d := energies[f] - energies[f-1]
		if d > 0 {
			onset[f] = d
			meanOnset += d
		}
	}
	meanOnset /= float64(numFrames)
	for f := range onset {
		onset[f] = math.Max(0, onset[f]-meanOnset*0.75)
	}

	fps := float64(sr) / float64(hop)
	minLag := int(math.Floor(fps * 60.0 / 178.0))
	maxLag := int(math.Ceil(fps * 60.0 / 68.0))
	if minLag < 4 {
		minLag = 4
	}
	if maxLag >= numFrames/2 {
		maxLag = numFrames/2 - 1
	}

	scores := make([]float64, maxLag+1)
	bestLag := minLag
	bestScore := -1.0
	for lag := minLag; lag <= maxLag; lag++ {
		var acc float64
		limit := numFrames - lag
		for i := 0; i < limit; i++ {
			acc += onset[i] * onset[i+lag]
		}
		// Add harmonic reinforcement at 2x lag (beat + bar) if within bounds
		if lag*2 < numFrames {
			var acc2 float64
			lim2 := numFrames - lag*2
			for i := 0; i < lim2; i++ {
				acc2 += onset[i] * onset[i+lag*2]
			}
			acc += 0.35 * acc2
		}
		candBPM := fps * 60.0 / float64(lag)
		// Gentle Gaussian perceptual tempo prior centered at 118 BPM (sigma 42) to resolve half/double-time
		prior := math.Exp(-0.5 * math.Pow((candBPM-118.0)/42.0, 2))
		sc := (acc / float64(limit)) * (0.72 + 0.28*prior)
		scores[lag] = sc
		if sc > bestScore {
			bestScore = sc
			bestLag = lag
		}
	}

	// Parabolic interpolation around bestLag for sub-frame precision
	refinedLag := float64(bestLag)
	if bestLag > minLag && bestLag < maxLag {
		y0, y1, y2 := scores[bestLag-1], scores[bestLag], scores[bestLag+1]
		denom := (y0 - 2*y1 + y2)
		if math.Abs(denom) > 1e-12 {
			delta := 0.5 * (y0 - y2) / denom
			if delta > -0.5 && delta < 0.5 {
				refinedLag += delta
			}
		}
	}
	est := fps * 60.0 / refinedLag
	return clamp(est, 65.0, 185.0)
}

// bandpassBiquad implements a clean 2nd-order Butterworth-style constant-skirt bandpass filter.
func bandpassBiquad(x []float64, sr, f0, q float64) []float64 {
	w0 := 2.0 * math.Pi * f0 / sr
	alpha := math.Sin(w0) / (2.0 * q)
	b0 := alpha
	b1 := 0.0
	b2 := -alpha
	a0 := 1.0 + alpha
	a1 := -2.0 * math.Cos(w0)
	a2 := 1.0 - alpha

	b0 /= a0
	b1 /= a0
	b2 /= a0
	a1 /= a0
	a2 /= a0

	y := make([]float64, len(x))
	var x1, x2, y1, y2 float64
	for i, x0 := range x {
		y0 := b0*x0 + b1*x1 + b2*x2 - a1*y1 - a2*y2
		y[i] = y0
		x2, x1 = x1, x0
		y2, y1 = y1, y0
	}
	return y
}

// BuildGenreAndTags unifies ID3/FLAC genre tags + Deezer editorial genres + DSP BPM/energy/vocal cues into clean categories.
func BuildGenreAndTags(tr db.TrackRecord, albEd *db.AlbumEditorial, bpm, energy, vocalStart, vocalEnd, duration float64) (string, []string) {
	seen := make(map[string]bool)
	var tags []string
	addTag := func(t string) {
		t = strings.TrimSpace(t)
		if t == "" {
			return
		}
		k := strings.ToLower(t)
		if !seen[k] {
			seen[k] = true
			tags = append(tags, t)
		}
	}

	rawGenres := []string{}
	if tr.Genre != nil && strings.TrimSpace(*tr.Genre) != "" {
		rawGenres = append(rawGenres, strings.TrimSpace(*tr.Genre))
	}
	if albEd != nil {
		rawGenres = append(rawGenres, albEd.Genres...)
	}

	canonGenre := ""
	for _, rg := range rawGenres {
		cg := canonicalizeGenre(rg)
		if cg != "" {
			if canonGenre == "" {
				canonGenre = cg
			}
			addTag(cg)
		}
	}
	if canonGenre == "" {
		canonGenre = "Eclectic"
	}

	// Era / Decade
	if tr.Year != nil && *tr.Year >= 1950 && *tr.Year <= 2039 {
		decade := (*tr.Year / 10) * 10
		addTag(fmt.Sprintf("%ds", decade))
	}

	// Tempo Category
	switch {
	case bpm > 0 && bpm < 92:
		addTag("Slow Tempo")
	case bpm >= 92 && bpm < 116:
		addTag("Mid-Tempo")
	case bpm >= 116 && bpm < 134:
		addTag("Upbeat")
	case bpm >= 134:
		addTag("High BPM")
	}

	// Energy Category
	switch {
	case energy < 0.36:
		addTag("Chill")
	case energy < 0.66:
		addTag("Groovy")
	default:
		addTag("High Energy")
	}

	// DJ Vocal Cue Tags
	if vocalStart >= 10.0 {
		addTag("Long Intro Ramp")
	} else if vocalStart <= 3.0 {
		addTag("Instant Vocals")
	}
	if duration > 0 && (duration-vocalEnd) >= 10.0 {
		addTag("Clean Outro Tail")
	}

	return canonGenre, tags
}

func canonicalizeGenre(raw string) string {
	l := strings.ToLower(strings.TrimSpace(raw))
	if l == "" || l == "unknown" || l == "other" || l == "genre" {
		return ""
	}
	switch {
	case strings.Contains(l, "eurodance") || strings.Contains(l, "dance") || strings.Contains(l, "disco") || strings.Contains(l, "house") || strings.Contains(l, "trance") || strings.Contains(l, "techno") || strings.Contains(l, "edm") || strings.Contains(l, "synth"):
		return "Electronic / Dance"
	case strings.Contains(l, "electro") || strings.Contains(l, "ambient") || strings.Contains(l, "downtempo") || strings.Contains(l, "trip"):
		return "Electronic"
	case strings.Contains(l, "metal") || strings.Contains(l, "hardcore") || strings.Contains(l, "doom"):
		return "Metal"
	case strings.Contains(l, "punk") || strings.Contains(l, "grunge") || strings.Contains(l, "alt") || strings.Contains(l, "indie"):
		return "Indie / Alternative"
	case strings.Contains(l, "rock") || strings.Contains(l, "prog"):
		return "Rock"
	case strings.Contains(l, "hip") || strings.Contains(l, "rap"):
		return "Hip-Hop"
	case strings.Contains(l, "r&b") || strings.Contains(l, "rnb") || strings.Contains(l, "soul") || strings.Contains(l, "funk"):
		return "R&B / Soul"
	case strings.Contains(l, "jazz") || strings.Contains(l, "swing") || strings.Contains(l, "bossa"):
		return "Jazz"
	case strings.Contains(l, "blues"):
		return "Blues"
	case strings.Contains(l, "classical") || strings.Contains(l, "orchestr") || strings.Contains(l, "opera"):
		return "Classical"
	case strings.Contains(l, "soundtrack") || strings.Contains(l, "score") || strings.Contains(l, "film") || strings.Contains(l, "ost"):
		return "Soundtrack"
	case strings.Contains(l, "folk") || strings.Contains(l, "country") || strings.Contains(l, "americana") || strings.Contains(l, "acoustic") || strings.Contains(l, "singer"):
		return "Folk / Acoustic"
	case strings.Contains(l, "reggae") || strings.Contains(l, "dub") || strings.Contains(l, "ska"):
		return "Reggae / Dub"
	case strings.Contains(l, "pop"):
		return "Pop"
	default:
		// Title-case unknown specific genres cleanly
		return strings.Title(l) //nolint:staticcheck
	}
}

func decodeStereoS16LE(raw []byte) (left, right []float64) {
	frames := len(raw) / 4
	left = make([]float64, frames)
	right = make([]float64, frames)
	const scale = 1.0 / 32768.0
	for i := 0; i < frames; i++ {
		off := i * 4
		l := int16(binary.LittleEndian.Uint16(raw[off : off+2]))
		r := int16(binary.LittleEndian.Uint16(raw[off+2 : off+4]))
		left[i] = float64(l) * scale
		right[i] = float64(r) * scale
	}
	return left, right
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
