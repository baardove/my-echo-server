package dj

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/baardove/my-echo/server/internal/config"
)

const geminiBaseURL = "https://generativelanguage.googleapis.com/v1beta/models"

type GeminiClient struct {
	httpClient *http.Client
}

func NewGeminiClient() *GeminiClient {
	return &GeminiClient{
		httpClient: &http.Client{
			Timeout: 65 * time.Second,
		},
	}
}

// GenerateScript calls the configured Gemini LLM model with live Google Search Grounding
// to research and craft a specific, factual, engaging spoken DJ script.
// Automatically retries with a larger token budget if Gemini hits MAX_TOKENS due to thinking + search tokens,
// and falls back cleanly without search tool if a custom/legacy model does not support google_search.
func (g *GeminiClient) GenerateScript(ctx context.Context, djCfg config.DJConfig, systemPrompt, userPrompt string) (string, bool, error) {
	script, usedSearch, finishReason, err := g.callGenerateScript(ctx, djCfg, systemPrompt, userPrompt, true, 4096)
	if err != nil {
		// Graceful fallback without google_search tool in case an older/custom model rejects search grounding
		if strings.Contains(strings.ToLower(err.Error()), "tool") || strings.Contains(strings.ToLower(err.Error()), "google_search") || strings.Contains(strings.ToLower(err.Error()), "not supported") {
			s2, _, _, err2 := g.callGenerateScript(ctx, djCfg, systemPrompt, userPrompt, false, 4096)
			return s2, false, err2
		}
		return "", false, err
	}
	// If thinking + Google Search grounding exhausted maxOutputTokens and cut off mid-sentence, retry with 8192 tokens!
	if strings.EqualFold(finishReason, "MAX_TOKENS") || looksCutOffMidSentence(script) {
		if s2, used2, _, err2 := g.callGenerateScript(ctx, djCfg, systemPrompt, userPrompt, true, 8192); err2 == nil && s2 != "" {
			return s2, used2 || usedSearch, nil
		}
	}
	return script, usedSearch, nil
}

func looksCutOffMidSentence(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	last := s[len(s)-1]
	// Valid sentence endings (including closing quotes/ellipsis)
	switch last {
	case '.', '!', '?', '"', '\'':
		return false
	}
	if strings.HasSuffix(s, "”") || strings.HasSuffix(s, "’") || strings.HasSuffix(s, "…") {
		return false
	}
	return true
}

func (g *GeminiClient) callGenerateScript(ctx context.Context, djCfg config.DJConfig, systemPrompt, userPrompt string, enableSearch bool, maxTokens int) (string, bool, string, error) {
	apiKey := strings.TrimSpace(djCfg.GeminiAPIKey)
	if apiKey == "" {
		return "", false, "", fmt.Errorf("Gemini API key is not configured (set it in Settings → AI Radio DJ)")
	}
	model := strings.TrimSpace(djCfg.LLMModel)
	if model == "" {
		model = "gemini-3.8-flash"
	}
	if maxTokens < 2048 {
		maxTokens = 4096
	}

	endpoint := fmt.Sprintf("%s/%s:generateContent?key=%s", geminiBaseURL, url.PathEscape(model), url.QueryEscape(apiKey))
	genCfg := map[string]any{
		"temperature":     0.78,
		"maxOutputTokens": maxTokens,
	}
	payload := map[string]any{
		"systemInstruction": map[string]any{
			"parts": []map[string]string{{"text": systemPrompt}},
		},
		"contents": []map[string]any{
			{
				"role":  "user",
				"parts": []map[string]string{{"text": userPrompt}},
			},
		},
		"generationConfig": genCfg,
	}
	if enableSearch {
		payload["tools"] = []map[string]any{
			{"google_search": map[string]any{}},
		}
	}

	rawReq, err := json.Marshal(payload)
	if err != nil {
		return "", false, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(rawReq))
	if err != nil {
		return "", false, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", false, "", fmt.Errorf("Gemini LLM request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2_097_152))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, "", fmt.Errorf("Gemini LLM %s error (%d): %s", model, resp.StatusCode, extractGeminiError(body))
	}

	var parsed struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
			Content      struct {
				Parts []struct {
					Text    string `json:"text"`
					Thought bool   `json:"thought"`
				} `json:"parts"`
			} `json:"content"`
			GroundingMetadata *struct {
				WebSearchQueries []string `json:"webSearchQueries"`
			} `json:"groundingMetadata"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", false, "", fmt.Errorf("decode Gemini LLM response: %w", err)
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return "", false, "", fmt.Errorf("Gemini LLM returned empty script")
	}

	finishReason := parsed.Candidates[0].FinishReason
	usedSearch := parsed.Candidates[0].GroundingMetadata != nil && len(parsed.Candidates[0].GroundingMetadata.WebSearchQueries) > 0
	var sb strings.Builder
	for _, p := range parsed.Candidates[0].Content.Parts {
		if p.Thought {
			continue
		}
		if strings.TrimSpace(p.Text) != "" {
			if sb.Len() > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(strings.TrimSpace(p.Text))
		}
	}
	clean := sanitizeSpokenScript(sb.String())
	if clean == "" {
		return "", usedSearch, finishReason, fmt.Errorf("Gemini LLM produced blank script")
	}
	return clean, usedSearch, finishReason, nil
}

// SynthesizeSpeech calls the configured Gemini TTS model and processes raw 24kHz PCM through
// our studio FM broadcast vocal DSP chain into a fast-start M4A file.
// IMPORTANT: Delivery directions are ALWAYS kept strictly separate from the spoken transcript
// (passed via systemInstruction, NEVER prepended to contents text) so directions are NEVER read aloud.
func (g *GeminiClient) SynthesizeSpeech(ctx context.Context, djCfg config.DJConfig, styleDirective, scriptText, outputPath string) (float64, error) {
	rawAudio, mimeType, err := g.callTTS(ctx, djCfg, styleDirective, scriptText, true)
	if err != nil && strings.TrimSpace(styleDirective) != "" &&
		(strings.Contains(strings.ToLower(err.Error()), "system") || strings.Contains(strings.ToLower(err.Error()), "invalid") || strings.Contains(strings.ToLower(err.Error()), "400")) {
		// Graceful fallback if a specific TTS endpoint rejects systemInstruction: send ONLY pure verbatim text
		rawAudio, mimeType, err = g.callTTS(ctx, djCfg, "", scriptText, false)
	}
	if err != nil {
		return 0, err
	}
	// Gemini 3.8 TTS returns audio/wav with trailing non-audio RIFF chunks (e.g. C2PA/metadata).
	// Extract ONLY the exact `data` chunk PCM so headers & trailing metadata are NEVER played as digital noise!
	pcm := rawAudio
	sampleRate := parsePCMSampleRate(mimeType)
	channels := 1
	if wavPCM, wavSR, wavCh, ok := extractWAVPCM(rawAudio); ok {
		pcm = wavPCM
		if wavSR > 0 {
			sampleRate = wavSR
		}
		if wavCh > 0 {
			channels = wavCh
		}
	}
	return ProcessBroadcastDJVoiceWithChannels(ctx, pcm, sampleRate, channels, outputPath)
}

func (g *GeminiClient) callTTS(ctx context.Context, djCfg config.DJConfig, styleDirective, scriptText string, includeSystem bool) ([]byte, string, error) {
	apiKey := strings.TrimSpace(djCfg.GeminiAPIKey)
	if apiKey == "" {
		return nil, "", fmt.Errorf("Gemini API key is not configured (set it in Settings → AI Radio DJ)")
	}
	model := strings.TrimSpace(djCfg.TTSModel)
	if model == "" {
		model = "gemini-3.8-flash-tts"
	}
	voice := strings.TrimSpace(djCfg.VoiceName)
	if voice == "" {
		voice = "Kore"
	}

	// Contents holds ONLY the verbatim words to speak aloud — NEVER prefix conversational directions here!
	cleanTranscript := strings.TrimSpace(scriptText)
	if cleanTranscript == "" {
		return nil, "", fmt.Errorf("empty transcript for TTS")
	}

	endpoint := fmt.Sprintf("%s/%s:generateContent?key=%s", geminiBaseURL, url.PathEscape(model), url.QueryEscape(apiKey))
	payload := map[string]any{
		"contents": []map[string]any{
			{
				"role":  "user",
				"parts": []map[string]string{{"text": cleanTranscript}},
			},
		},
		"generationConfig": map[string]any{
			"responseModalities": []string{"AUDIO"},
			"speechConfig": map[string]any{
				"voiceConfig": map[string]any{
					"prebuiltVoiceConfig": map[string]string{
						"voiceName": voice,
					},
				},
			},
		},
	}
	if includeSystem && strings.TrimSpace(styleDirective) != "" {
		payload["systemInstruction"] = map[string]any{
			"parts": []map[string]string{{
				"text": fmt.Sprintf("Voice delivery direction (DO NOT read this instruction aloud; speak ONLY the user's transcript verbatim): %s", strings.TrimSpace(styleDirective)),
			}},
		}
	}

	rawReq, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(rawReq))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("Gemini TTS request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 25_000_000))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("Gemini TTS %s error (%d): %s", model, resp.StatusCode, extractGeminiError(body))
	}

	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					InlineData *struct {
						MimeType string `json:"mimeType"`
						Data     string `json:"data"`
					} `json:"inlineData"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, "", fmt.Errorf("decode Gemini TTS response: %w", err)
	}
	var b64Data, mimeType string
	for _, c := range parsed.Candidates {
		for _, p := range c.Content.Parts {
			if p.InlineData != nil && p.InlineData.Data != "" {
				b64Data = p.InlineData.Data
				mimeType = p.InlineData.MimeType
				break
			}
		}
		if b64Data != "" {
			break
		}
	}
	if b64Data == "" {
		return nil, "", fmt.Errorf("Gemini TTS returned no inline audio payload")
	}

	rawAudio, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		return nil, "", fmt.Errorf("decode Gemini TTS base64 audio: %w", err)
	}
	return rawAudio, mimeType, nil
}

// extractWAVPCM parses a RIFF/WAVE container and returns ONLY the exact PCM payload inside the
// `data` chunk (plus sample rate and channel count from `fmt `), cleanly ignoring the RIFF header
// and any trailing metadata chunks (such as C2PA / SynthID / LIST / JUNK chunks) that would
// otherwise sound like loud digital noise if decoded as raw s16le PCM.
func extractWAVPCM(raw []byte) (pcm []byte, sampleRate int, channels int, ok bool) {
	if len(raw) < 44 || string(raw[0:4]) != "RIFF" || string(raw[8:12]) != "WAVE" {
		return nil, 0, 0, false
	}
	pos := 12
	var foundData []byte
	for pos+8 <= len(raw) {
		chunkID := string(raw[pos : pos+4])
		chunkSize := int(binary.LittleEndian.Uint32(raw[pos+4 : pos+8]))
		dataStart := pos + 8
		if chunkSize < 0 || dataStart > len(raw) {
			break
		}
		dataEnd := dataStart + chunkSize
		if dataEnd > len(raw) {
			dataEnd = len(raw)
		}
		switch chunkID {
		case "fmt ":
			if dataEnd-dataStart >= 16 {
				channels = int(binary.LittleEndian.Uint16(raw[dataStart+2 : dataStart+4]))
				sampleRate = int(binary.LittleEndian.Uint32(raw[dataStart+4 : dataStart+8]))
			}
		case "data":
			foundData = raw[dataStart:dataEnd]
		}
		nextPos := dataStart + chunkSize
		if chunkSize%2 == 1 {
			nextPos++ // RIFF chunks are word-aligned
		}
		if nextPos <= pos {
			break
		}
		pos = nextPos
	}
	if len(foundData) == 0 {
		return nil, 0, 0, false
	}
	return foundData, sampleRate, channels, true
}

// ProcessBroadcastDJVoice transforms raw 16-bit signed LE mono PCM into a studio-grade FM radio DJ voice:
// - High-pass 75 Hz (sub-bass rumble cut)
// - Proximity warmth +2.8 dB @ 135 Hz (classic SM7B/RE20 broadcast mic body)
// - De-mud -3.0 dB @ 270 Hz (clears room over music beds during crossfades)
// - Broadcast presence +3.2 dB @ 3.1 kHz (crisp speech intelligibility)
// - FM Air shelf +2.4 dB @ 9.5 kHz (silky studio top-end)
// - Tight vocal compressor (threshold -21 dB, ratio 4.2:1, fast attack, smooth release)
// - Loudness normalization (-14 LUFS, -1.0 dBTP, tight LRA 5.5)
// - Brickwall peak limiter (-1 dBFS) + clickless fade-in -> 44.1 kHz AAC 160 kbps M4A (+faststart).
func ProcessBroadcastDJVoice(ctx context.Context, rawPCM []byte, sampleRate int, outputPath string) (float64, error) {
	return ProcessBroadcastDJVoiceWithChannels(ctx, rawPCM, sampleRate, 1, outputPath)
}

func ProcessBroadcastDJVoiceWithChannels(ctx context.Context, rawPCM []byte, sampleRate int, channels int, outputPath string) (float64, error) {
	if sampleRate <= 0 {
		sampleRate = 24000
	}
	if channels <= 0 || channels > 2 {
		channels = 1
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return 0, err
	}

	// If someone passes a raw WAV container directly, extract ONLY its PCM `data` chunk first
	if wavPCM, wavSR, wavCh, ok := extractWAVPCM(rawPCM); ok {
		rawPCM = wavPCM
		if wavSR > 0 {
			sampleRate = wavSR
		}
		if wavCh > 0 && wavCh <= 2 {
			channels = wavCh
		}
	}

	// Strip any Gemini TTS end-of-stream digital glitch/noise burst and apply clickless micro-fades
	cleanPCM := rawPCM
	if channels == 1 {
		cleanPCM = trimTTSTailArtifact(rawPCM, sampleRate)
	}
	cleanDur := float64(len(cleanPCM)) / float64(sampleRate*channels*2)

	ffBin, lookErr := exec.LookPath("ffmpeg")
	if lookErr != nil {
		// Graceful fallback (e.g. unit test machine without ffmpeg): write valid WAV container
		wav := wrapPCMAsWAV(cleanPCM, sampleRate, channels, 16)
		if err := os.WriteFile(outputPath, wav, 0o644); err != nil {
			return 0, err
		}
		if cleanDur <= 0 {
			cleanDur = 1.0
		}
		return cleanDur, nil
	}

	tmpPCM := outputPath + ".pcm"
	if err := os.WriteFile(tmpPCM, cleanPCM, 0o600); err != nil {
		return 0, err
	}
	defer os.Remove(tmpPCM)

	tctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	filters := []string{
		"highpass=f=75",
		"bass=g=2.8:f=135:w=0.6",
		"equalizer=f=270:t=q:w=1.2:g=-3.0",
		"equalizer=f=3100:t=q:w=1.3:g=3.2",
		"treble=f=9500:g=2.4",
		"acompressor=threshold=-21dB:ratio=4.2:attack=6:release=110:makeup=3.5",
		"loudnorm=I=-14:TP=-1.0:LRA=6.0",
		"alimiter=limit=0.891:attack=2:release=45",
		"afade=t=in:st=0:d=0.03",
	}
	if cleanDur > 0.25 {
		fadeOutStart := math.Max(0.05, cleanDur-0.07)
		filters = append(filters, fmt.Sprintf("afade=t=out:st=%.3f:d=0.07", fadeOutStart))
	}
	filterChain := strings.Join(filters, ",")

	tmpOut := outputPath + ".tmp.m4a"
	cmd := exec.CommandContext(tctx, ffBin,
		"-y",
		"-v", "error",
		"-f", "s16le",
		"-ar", strconv.Itoa(sampleRate),
		"-ac", strconv.Itoa(channels),
		"-i", tmpPCM,
		"-ac", "2",
		"-ar", "44100",
		"-af", filterChain,
		"-c:a", "aac",
		"-b:a", "160k",
		"-movflags", "+faststart",
		tmpOut,
	)
	if out, runErr := cmd.CombinedOutput(); runErr != nil {
		_ = os.Remove(tmpOut)
		return 0, fmt.Errorf("ffmpeg broadcast DJ DSP failed: %v (%s)", runErr, strings.TrimSpace(string(out)))
	}
	if err := os.Rename(tmpOut, outputPath); err != nil {
		_ = os.Remove(tmpOut)
		return 0, err
	}

	dur := probeAudioDuration(ctx, outputPath)
	if dur <= 0 {
		dur = cleanDur
	}
	if dur <= 0 {
		dur = 4.0
	}
	return dur, nil
}

// trimTTSTailArtifact removes trailing end-of-stream neural codec digital noise bursts
// (e.g. Gemini 3.8 TTS emitting ~150-300ms of digital noise after ~150-350ms of post-speech silence),
// trims excess dead silence, and applies clickless cosine micro-fades on the raw 16-bit LE PCM.
func trimTTSTailArtifact(rawPCM []byte, sampleRate int) []byte {
	evenBytes := len(rawPCM) &^ 1
	if evenBytes == 0 || sampleRate <= 0 {
		return rawPCM
	}
	totalSamples := evenBytes / 2
	frameLen := sampleRate / 50 // 20ms frames
	if frameLen <= 0 || totalSamples < frameLen*25 {
		// Shorter than 0.5s — just align to even bytes
		out := make([]byte, evenBytes)
		copy(out, rawPCM[:evenBytes])
		return out
	}

	samples := make([]int16, totalSamples)
	for i := 0; i < totalSamples; i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(rawPCM[i*2 : i*2+2]))
	}

	numFrames := totalSamples / frameLen
	rmsDB := make([]float64, numFrames)
	zcr := make([]float64, numFrames)
	for f := 0; f < numFrames; f++ {
		base := f * frameLen
		var sumSq float64
		crossings := 0
		prev := samples[base]
		for j := 0; j < frameLen; j++ {
			s := samples[base+j]
			v := float64(s) / 32768.0
			sumSq += v * v
			if j > 0 && ((s >= 0 && prev < 0) || (s < 0 && prev >= 0)) {
				crossings++
			}
			prev = s
		}
		rms := math.Sqrt(sumSq / float64(frameLen))
		if rms > 1e-7 {
			rmsDB[f] = 20.0 * math.Log10(rms)
		} else {
			rmsDB[f] = -120.0
		}
		zcr[f] = float64(crossings) / float64(frameLen)
	}

	endFrame := numFrames
	minSpeechFrames := 25 // never trim into the first 0.5s

	// Up to 2 passes: strip trailing dead silence, then detect & cut EOS digital burst after silence gap
	for pass := 0; pass < 2; pass++ {
		for endFrame > minSpeechFrames && rmsDB[endFrame-1] < -52.0 {
			endFrame--
		}
		if endFrame <= minSpeechFrames {
			break
		}

		// Measure active burst right at the end of the file
		burstEnd := endFrame
		burstStart := burstEnd
		for burstStart > minSpeechFrames && rmsDB[burstStart-1] >= -44.0 {
			burstStart--
		}
		burstFrames := burstEnd - burstStart

		// Case 1: Short trailing burst (<= 440ms / 22 frames) preceded by >= 100ms (5 frames) of silence (< -44 dB)
		if burstFrames > 0 && burstFrames <= 22 && burstStart > minSpeechFrames {
			gapEnd := burstStart
			gapStart := gapEnd
			for gapStart > minSpeechFrames && rmsDB[gapStart-1] < -44.0 {
				gapStart--
			}
			gapFrames := gapEnd - gapStart
			if gapFrames >= 5 {
				// Confirm real speech existed before the silence gap
				hasSpeechBefore := false
				checkFrom := gapStart - 25
				if checkFrom < 0 {
					checkFrom = 0
				}
				for k := checkFrom; k < gapStart; k++ {
					if rmsDB[k] > -34.0 {
						hasSpeechBefore = true
						break
					}
				}
				if hasSpeechBefore {
					endFrame = gapStart
					continue
				}
			}
		}

		// Case 2: Very high zero-crossing digital noise glitch right at the tail (<= 260ms / 13 frames)
		if burstFrames > 0 && burstFrames <= 13 && burstStart > minSpeechFrames {
			highZCRCount := 0
			for k := burstStart; k < burstEnd; k++ {
				if zcr[k] > 0.36 {
					highZCRCount++
				}
			}
			if highZCRCount >= (burstFrames+1)/2 && rmsDB[burstStart-1] < -38.0 {
				endFrame = burstStart
				continue
			}
		}
		break
	}

	// Keep a natural 60ms breath tail (3 frames) after last spoken frame, if available before any removed burst
	keepSamples := (endFrame + 3) * frameLen
	if keepSamples > totalSamples {
		keepSamples = totalSamples
	}
	// If we actually trimmed an artifact, don't reach back into the trimmed artifact!
	if endFrame < numFrames {
		maxSafe := (endFrame + 2) * frameLen
		if maxSafe < keepSamples {
			keepSamples = maxSafe
		}
	}
	if keepSamples < frameLen*10 {
		keepSamples = totalSamples
	}

	trimmed := samples[:keepSamples]

	// Apply smooth cosine micro-fade-in (15ms) & micro-fade-out (50ms) to guarantee zero clicks
	fadeIn := sampleRate / 66
	if fadeIn > len(trimmed)/4 {
		fadeIn = len(trimmed) / 4
	}
	for i := 0; i < fadeIn; i++ {
		gain := 0.5 * (1.0 - math.Cos(math.Pi*float64(i)/float64(fadeIn)))
		trimmed[i] = int16(math.Round(float64(trimmed[i]) * gain))
	}
	fadeOut := sampleRate / 20
	if fadeOut > len(trimmed)/4 {
		fadeOut = len(trimmed) / 4
	}
	for i := 0; i < fadeOut; i++ {
		idx := len(trimmed) - 1 - i
		gain := 0.5 * (1.0 - math.Cos(math.Pi*float64(i)/float64(fadeOut)))
		trimmed[idx] = int16(math.Round(float64(trimmed[idx]) * gain))
	}

	out := make([]byte, len(trimmed)*2)
	for i, s := range trimmed {
		binary.LittleEndian.PutUint16(out[i*2:i*2+2], uint16(s))
	}
	return out
}

func probeAudioDuration(ctx context.Context, path string) float64 {
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

func parsePCMSampleRate(mimeType string) int {
	for _, part := range strings.Split(mimeType, ";") {
		p := strings.TrimSpace(strings.ToLower(part))
		if strings.HasPrefix(p, "rate=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(p, "rate=")); err == nil && n >= 8000 && n <= 96000 {
				return n
			}
		}
	}
	return 24000
}

func wrapPCMAsWAV(pcm []byte, sampleRate, channels, bitsPerSample int) []byte {
	byteRate := sampleRate * channels * bitsPerSample / 8
	blockAlign := channels * bitsPerSample / 8
	dataSize := len(pcm)
	buf := bytes.NewBuffer(make([]byte, 0, 44+dataSize))
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, uint32(36+dataSize))
	buf.WriteString("WAVEfmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(buf, binary.LittleEndian, uint16(channels))
	_ = binary.Write(buf, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(buf, binary.LittleEndian, uint32(byteRate))
	_ = binary.Write(buf, binary.LittleEndian, uint16(blockAlign))
	_ = binary.Write(buf, binary.LittleEndian, uint16(bitsPerSample))
	buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, uint32(dataSize))
	buf.Write(pcm)
	return buf.Bytes()
}

func extractGeminiError(body []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 220 {
		return s[:220] + "..."
	}
	return s
}

func sanitizeSpokenScript(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"'`+"`")
	s = strings.ReplaceAll(s, "**", "")
	s = strings.ReplaceAll(s, "*", "")
	s = strings.ReplaceAll(s, "`", "")
	// Strip inline numeric search citation markers like [1], [2], [1, 2] so TTS never reads them aloud
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '[' {
			end := strings.IndexByte(s[i:], ']')
			if end > 1 && end <= 12 {
				inner := s[i+1 : i+end]
				allCite := true
				for _, r := range inner {
					if !((r >= '0' && r <= '9') || r == ',' || r == ' ' || r == '-') {
						allCite = false
						break
					}
				}
				if allCite {
					i += end
					continue
				}
			}
		}
		out.WriteByte(s[i])
	}
	s = strings.ReplaceAll(out.String(), "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	fields := strings.Fields(s)
	return strings.TrimSpace(strings.Join(fields, " "))
}

// ListLiveModels queries Google's Gemini v1beta/models endpoint with the configured API key
// and returns discovered TTS models and text LLM models merged with our curated defaults.
func (g *GeminiClient) ListLiveModels(ctx context.Context, apiKey string) (ttsModels []OptionItem, llmModels []OptionItem, err error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return AvailableTTSModels(), AvailableLLMModels(), fmt.Errorf("Gemini API key is required to query live models")
	}
	endpoint := fmt.Sprintf("%s?pageSize=200&key=%s", geminiBaseURL, url.QueryEscape(apiKey))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("query Gemini models failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4_194_304))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("Gemini models API error (%d): %s", resp.StatusCode, extractGeminiError(body))
	}

	var parsed struct {
		Models []struct {
			Name                       string   `json:"name"`
			DisplayName                string   `json:"displayName"`
			Description                string   `json:"description"`
			SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
		} `json:"models"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, nil, fmt.Errorf("decode Gemini models response: %w", err)
	}

	seenTTS := make(map[string]bool)
	seenLLM := make(map[string]bool)
	for _, m := range parsed.Models {
		supportsGen := false
		for _, method := range m.SupportedGenerationMethods {
			if method == "generateContent" {
				supportsGen = true
				break
			}
		}
		if !supportsGen {
			continue
		}
		id := strings.TrimPrefix(strings.TrimSpace(m.Name), "models/")
		low := strings.ToLower(id)
		if !strings.HasPrefix(low, "gemini-") {
			continue
		}
		// Skip image/video/embedding-only models
		if strings.Contains(low, "embedding") || strings.Contains(low, "imagen") || strings.Contains(low, "veo") || strings.Contains(low, "image") || strings.Contains(low, "robotics") {
			continue
		}
		label := strings.TrimSpace(m.DisplayName)
		if label == "" {
			label = id
		}
		desc := strings.TrimSpace(m.Description)
		if len(desc) > 90 {
			desc = desc[:90] + "…"
		}
		item := OptionItem{ID: id, Label: label, Description: desc}
		if strings.Contains(low, "tts") {
			if !seenTTS[low] {
				seenTTS[low] = true
				ttsModels = append(ttsModels, item)
			}
		} else if !strings.Contains(low, "native-audio") && !strings.Contains(low, "live") {
			if !seenLLM[low] {
				seenLLM[low] = true
				llmModels = append(llmModels, item)
			}
		}
	}

	// Ensure curated defaults stay present if not returned
	for _, def := range AvailableTTSModels() {
		if !seenTTS[strings.ToLower(def.ID)] {
			ttsModels = append(ttsModels, def)
		}
	}
	for _, def := range AvailableLLMModels() {
		if !seenLLM[strings.ToLower(def.ID)] {
			llmModels = append(llmModels, def)
		}
	}
	return ttsModels, llmModels, nil
}
