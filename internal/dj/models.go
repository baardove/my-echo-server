package dj

import "time"

type OptionItem struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Accent      string `json:"accent,omitempty"`
}

func AvailableTTSModels() []OptionItem {
	return []OptionItem{
		{
			ID:          "gemini-3.8-flash-tts",
			Label:       "Gemini 3.8 Flash TTS",
			Description: "Flagship studio-grade voice fidelity, expressive radio acting & natural pacing (recommended)",
		},
		{
			ID:          "gemini-3.8-flash-lite-tts",
			Label:       "Gemini 3.8 Flash-Lite TTS",
			Description: "Ultra-fast, low-latency & cost-efficient 3.8 voice synthesis",
		},
		{
			ID:          "gemini-3.1-flash-tts-preview",
			Label:       "Gemini 3.1 Flash TTS Preview",
			Description: "Fast controllable single/multi-speaker TTS preview",
		},
		{
			ID:          "gemini-2.5-flash-preview-tts",
			Label:       "Gemini 2.5 Flash TTS",
			Description: "Previous-gen fast voice synthesis",
		},
		{
			ID:          "gemini-2.5-pro-preview-tts",
			Label:       "Gemini 2.5 Pro TTS",
			Description: "Previous-gen expressive voice synthesis",
		},
	}
}

func AvailableLLMModels() []OptionItem {
	return []OptionItem{
		{
			ID:          "gemini-3.8-flash",
			Label:       "Gemini 3.8 Flash",
			Description: "Flagship fast & creative DJ script writer (recommended)",
		},
		{
			ID:          "gemini-3.5-flash-lite",
			Label:       "Gemini 3.5 Flash-Lite",
			Description: "Ultra-fast, lowest latency & token cost",
		},
		{
			ID:          "gemini-2.5-flash",
			Label:       "Gemini 2.5 Flash",
			Description: "Previous-gen balanced script writer",
		},
		{
			ID:          "gemini-2.5-pro",
			Label:       "Gemini 2.5 Pro",
			Description: "Previous-gen deep music journalism",
		},
	}
}

func AvailableVoices() []OptionItem {
	return []OptionItem{
		{ID: "Kore", Label: "Kore", Description: "Firm, poised & confident FM host"},
		{ID: "Charon", Label: "Charon", Description: "Deep, velvety late-night radio voice"},
		{ID: "Puck", Label: "Puck", Description: "Upbeat, charismatic & playful"},
		{ID: "Aoede", Label: "Aoede", Description: "Smooth, breezy & melodic"},
		{ID: "Fenrir", Label: "Fenrir", Description: "Energetic, punchy drive-time host"},
		{ID: "Zephyr", Label: "Zephyr", Description: "Bright, crisp & modern"},
		{ID: "Orus", Label: "Orus", Description: "Rich, grounded vinyl crate-digger"},
		{ID: "Sulafat", Label: "Sulafat", Description: "Warm, inviting storyteller"},
		{ID: "Alnilam", Label: "Alnilam", Description: "Steady, classic broadcaster"},
		{ID: "Leda", Label: "Leda", Description: "Lively, fresh indie discovery"},
		{ID: "Vindemiatrix", Label: "Vindemiatrix", Description: "Gentle, nocturnal Nordic chill"},
		{ID: "Sadachbia", Label: "Sadachbia", Description: "Lively weekend party host"},
	}
}

func AvailablePersonas() []OptionItem {
	return []OptionItem{
		{
			ID:          "late-night-fm",
			Label:       "Late Night FM",
			Description: "Warm, intimate FM radio host with velvet pacing and sharp musical insight",
		},
		{
			ID:          "upbeat-radio",
			Label:       "Prime-Time Radio",
			Description: "Bright, tight, charismatic broadcaster keeping energy high",
		},
		{
			ID:          "vinyl-nerd",
			Label:       "Vinyl Crate-Digger",
			Description: "Passionate music historian spotlighting release years, record labels & liner facts",
		},
		{
			ID:          "party-hype",
			Label:       "Weekend Mix Host",
			Description: "Infectious, punchy club & party transitions that keep the floor moving",
		},
		{
			ID:          "nordic-chill",
			Label:       "Nordic Midnight",
			Description: "Calm, understated, atmospheric host with minimal poetic delivery",
		},
	}
}

type BreakKind string

const (
	BreakNone   BreakKind = "none"
	BreakIntro  BreakKind = "intro"
	BreakOutro  BreakKind = "outro"
	BreakBridge BreakKind = "bridge" // Outgoing track outro + Incoming track intro in one sweep
)

type BreakStatus string

const (
	StatusPending    BreakStatus = "pending"
	StatusGenerating BreakStatus = "generating"
	StatusReady      BreakStatus = "ready"
	StatusError      BreakStatus = "error"
)

type DJBreak struct {
	ID          string      `json:"id"`
	Kind        BreakKind   `json:"kind"`
	Status      BreakStatus `json:"status"`
	Script      string      `json:"script,omitempty"`
	AudioURL    string      `json:"audioURL,omitempty"`
	AudioPath   string      `json:"-"`
	Duration    float64     `json:"duration"`
	Error       string      `json:"error,omitempty"`
	FactsUsed   []string    `json:"factsUsed,omitempty"`
	GeneratedAt *time.Time  `json:"generatedAt,omitempty"`
}

type DJTrackInfo struct {
	ID            string   `json:"id"`
	Title         string   `json:"title"`
	Artist        string   `json:"artist"`
	AlbumArtist   string   `json:"albumArtist"`
	Album         string   `json:"album"`
	AlbumKey      string   `json:"albumKey"`
	Year          *int     `json:"year,omitempty"`
	Genre         *string  `json:"genre,omitempty"`
	TrackNumber   *int     `json:"trackNumber,omitempty"`
	Duration      float64  `json:"duration"`
	Format        string   `json:"format"`
	FormatDetail  string   `json:"formatDetail,omitempty"`
	IsHiRes       bool     `json:"isHiRes"`
	ArtworkHash   *string  `json:"artworkHash,omitempty"`
	BPM           float64  `json:"bpm,omitempty"`
	VocalStartSec float64  `json:"vocalStartSec,omitempty"`
	VocalEndSec   float64  `json:"vocalEndSec,omitempty"`
	Energy        float64  `json:"energy,omitempty"`
	LoudnessDB    float64  `json:"loudnessDB,omitempty"`
	PeakLinear    float64  `json:"peakLinear,omitempty"`
	NormGainDB    float64  `json:"normGainDB,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	HasAnalysis   bool     `json:"hasAnalysis,omitempty"`
}

// DJSlot represents one song in the live DJ playlist along with its planned/generated talk breaks.
// Invariant: a single song is NEVER both intro'd (Intro or Bridge) AND outro'd (Outro or next slot's Bridge).
type DJSlot struct {
	ID              string      `json:"id"`
	Position        int         `json:"position"`
	Track           DJTrackInfo `json:"track"`
	Mode            string      `json:"mode"` // "auto" or user override: "intro", "outro", "bridge", "none"
	Intro           *DJBreak    `json:"intro,omitempty"`
	Outro           *DJBreak    `json:"outro,omitempty"`
	Bridge          *DJBreak    `json:"bridge,omitempty"` // Played crossing from prev slot into this slot
	TransitionLabel string      `json:"transitionLabel"`
	HasEditorial    bool        `json:"hasEditorial"`
	FromLidarr      bool        `json:"fromLidarr,omitempty"`
}

type SessionState struct {
	SessionID        string    `json:"sessionId"`
	StationName      string    `json:"stationName"`
	SourceLabel      string    `json:"sourceLabel"`
	ProfileID        string    `json:"profileId,omitempty"`
	ProfileName      string    `json:"profileName,omitempty"`
	AutoRefill       bool      `json:"autoRefill"`
	MinQueueAhead    int       `json:"minQueueAhead"`
	CurrentIndex     int       `json:"currentIndex"`
	Configured       bool      `json:"configured"`
	LLMModel         string    `json:"llmModel"`
	TTSModel         string    `json:"ttsModel"`
	VoiceName        string    `json:"voiceName"`
	Persona          string    `json:"persona"`
	Language         string    `json:"language"`
	TalkFrequency    float64   `json:"talkFrequency"`
	CrossfadeSeconds float64   `json:"crossfadeSeconds"`
	DuckVolume       float64   `json:"duckVolume"`
	UpdatedAt        time.Time `json:"updatedAt"`
	Slots            []DJSlot  `json:"slots"`
}
