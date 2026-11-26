package analysis

import (
	"math"
	"testing"

	"github.com/baardove/my-echo/server/internal/db"
)

func TestAnalyzeStereoPCMDetectsBPMAndVocalBoundaries(t *testing.T) {
	const sr = AnalysisSampleRate
	const durSec = 40.0
	n := int(sr * durSec)
	left := make([]float64, n)
	right := make([]float64, n)

	// Synthesize a 120 BPM pulse track (2 beats/sec) with:
	// - 0..8s: pure bass/side synth intro (NO center vocals)
	// - 8..31s: strong center-panned 1000 Hz vocal formant singing
	// - 31..40s: clean instrumental outro tail (NO center vocals)
	const targetBPM = 120.0
	beatPeriod := 60.0 / targetBPM

	for i := 0; i < n; i++ {
		sec := float64(i) / float64(sr)
		// Soft 120 BPM kick pulse across entire track
		phase := math.Mod(sec, beatPeriod)
		kick := 0.0
		if phase < 0.06 {
			env := math.Exp(-phase * 45.0)
			kick = 0.38 * env * math.Sin(2*math.Pi*95.0*sec)
		}
		// Wide stereo pad (opposite polarity on L/R -> cancels in mid, sits in side)
		pad := 0.14 * math.Sin(2*math.Pi*440.0*sec)

		// Center-panned singing from 8s to 31s (strong 1100 Hz + 1650 Hz formants in L+R)
		vocal := 0.0
		if sec >= 8.0 && sec <= 31.0 {
			syllable := 0.55 + 0.45*math.Sin(2*math.Pi*3.0*sec)
			vocal = 0.42 * syllable * (math.Sin(2*math.Pi*1100.0*sec) + 0.5*math.Sin(2*math.Pi*1650.0*sec))
		}

		left[i] = kick + pad + vocal
		right[i] = kick - pad + vocal
	}

	bpm, vocalStart, vocalEnd, energy := AnalyzeStereoPCM(left, right, sr, durSec)

	if math.Abs(bpm-targetBPM) > 4.0 {
		t.Fatalf("expected BPM near %.1f, got %.1f", targetBPM, bpm)
	}
	if vocalStart < 6.5 || vocalStart > 9.5 {
		t.Fatalf("expected vocalStart near 8.0s, got %.2fs", vocalStart)
	}
	if vocalEnd < 29.5 || vocalEnd > 32.5 {
		t.Fatalf("expected vocalEnd near 31.0s, got %.2fs", vocalEnd)
	}
	if energy <= 0.05 || energy > 1.0 {
		t.Fatalf("unexpected energy score: %.2f", energy)
	}

	yr := 1994
	g := "Eurodance"
	canon, tags := BuildGenreAndTags(db.TrackRecord{Year: &yr, Genre: &g}, nil, bpm, energy, vocalStart, vocalEnd, durSec)
	if canon != "Electronic / Dance" {
		t.Fatalf("expected canonical genre Electronic / Dance, got %q", canon)
	}
	if len(tags) < 3 {
		t.Fatalf("expected categorized tags, got %v", tags)
	}
}
