package party

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// NormalizeVoiceIntro runs a broadcast vocal DSP chain via ffmpeg matching EchoCore's VoiceIntroAudioNormalizer:
// High-pass 85Hz (rumble) -> De-mud 260Hz -3.5dB -> Presence 3.2kHz +3dB -> Air shelf 9kHz +2dB ->
// Dynamic range compression -> Loudness normalization (-14 LUFS) -> -1 dBFS peak limiter -> AAC M4A (universal Safari/Apple TV/macOS).
func NormalizeVoiceIntro(ctx context.Context, inputPath, outputPath string, maxSeconds int) (duration float64, err error) {
	if maxSeconds <= 0 {
		maxSeconds = 15
	}
	ffBin, lookErr := exec.LookPath("ffmpeg")
	if lookErr != nil {
		// Graceful fallback if ffmpeg is unavailable: copy raw bytes
		raw, rErr := os.ReadFile(inputPath)
		if rErr != nil {
			return 0, rErr
		}
		if wErr := os.WriteFile(outputPath, raw, 0o644); wErr != nil {
			return 0, wErr
		}
		return float64(min(maxSeconds, 5)), nil
	}

	tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	filterChain := strings.Join([]string{
		"highpass=f=85",
		"equalizer=f=260:t=q:w=1.2:g=-3.5",
		"equalizer=f=3200:t=q:w=1.4:g=3.0",
		"treble=f=9000:g=2.0",
		"acompressor=threshold=-20dB:ratio=3.5:attack=10:release=120:makeup=3",
		"loudnorm=I=-14:TP=-1.0:LRA=7",
		"alimiter=limit=0.891:attack=3:release=50",
	}, ",")

	cmd := exec.CommandContext(tctx, ffBin,
		"-y",
		"-v", "error",
		"-i", inputPath,
		"-t", strconv.Itoa(maxSeconds),
		"-ac", "1",
		"-ar", "44100",
		"-af", filterChain,
		"-c:a", "aac",
		"-b:a", "128k",
		"-movflags", "+faststart",
		outputPath,
	)
	if out, runErr := cmd.CombinedOutput(); runErr != nil {
		return 0, fmt.Errorf("ffmpeg vocal chain failed: %v (%s)", runErr, strings.TrimSpace(string(out)))
	}

	dur := probeDuration(ctx, outputPath)
	if dur <= 0 {
		dur = 3.0
	}
	return dur, nil
}

func probeDuration(ctx context.Context, path string) float64 {
	bin, err := exec.LookPath("ffprobe")
	if err != nil {
		return 0
	}
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(tctx, bin,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path,
	).Output()
	if err != nil {
		return 0
	}
	d, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}
