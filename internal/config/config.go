package config

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// LibraryConfig describes one mounted music volume inside the container.
type LibraryConfig struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Path            string `json:"path"`
	LegacyMacPrefix string `json:"legacyMacPrefix,omitempty"`
	ShardCount      int    `json:"shardCount"`
}

// DJProfile defines a reusable AI Radio DJ station profile (music filters + optional host vibe).
type DJProfile struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Description      string   `json:"description,omitempty"`
	Genres           []string `json:"genres,omitempty"`
	ExcludeGenres    []string `json:"excludeGenres,omitempty"`
	Tags             []string `json:"tags,omitempty"`
	Eras             []string `json:"eras,omitempty"`
	MinBPM           float64  `json:"minBPM,omitempty"`
	MaxBPM           float64  `json:"maxBPM,omitempty"`
	MinRating        int      `json:"minRating,omitempty"`
	AvoidMaxRating   int      `json:"avoidMaxRating,omitempty"`
	MinYear          int      `json:"minYear,omitempty"`
	MaxYear          int      `json:"maxYear,omitempty"`
	PreferBPMFlow        bool     `json:"preferBPMFlow"`
	IntroOutroEveryTrack *bool    `json:"introOutroEveryTrack,omitempty"`
	StationName          string   `json:"stationName,omitempty"`
	Persona              string   `json:"persona,omitempty"`
	VoiceName            string   `json:"voiceName,omitempty"`
	Language             string   `json:"language,omitempty"`
	TalkFrequency        *float64 `json:"talkFrequency,omitempty"`
	CrossfadeSeconds     *float64 `json:"crossfadeSeconds,omitempty"`
	CustomPrompt         string   `json:"customPrompt,omitempty"`
}

// WantsIntroOutroEveryTrack returns true unless the profile explicitly disables it,
// ensuring every track gets either an intro or an outro (never stranded between two crossfades).
func (p *DJProfile) WantsIntroOutroEveryTrack() bool {
	if p == nil || p.IntroOutroEveryTrack == nil {
		return true
	}
	return *p.IntroOutroEveryTrack
}

// DJConfig holds AI Radio DJ (LLM + TTS + broadcast crossfade/ducking + profiles) settings.
type DJConfig struct {
	Provider         string      `json:"provider"`
	GeminiAPIKey     string      `json:"geminiAPIKey,omitempty"`
	LLMModel         string      `json:"llmModel"`
	TTSModel         string      `json:"ttsModel"`
	VoiceName        string      `json:"voiceName"`
	Persona          string      `json:"persona"`
	Language         string      `json:"language"`
	RadioStationName string      `json:"radioStationName"`
	TalkFrequency    float64     `json:"talkFrequency"`
	CrossfadeSeconds float64     `json:"crossfadeSeconds"`
	DuckVolume       float64     `json:"duckVolume"`
	AutoRefill       bool        `json:"autoRefill"`
	MinQueueAhead    int         `json:"minQueueAhead"`
	MaxScriptWords   int         `json:"maxScriptWords"`
	CustomPrompt     string      `json:"customPrompt,omitempty"`
	ActiveProfileID  string      `json:"activeProfileId,omitempty"`
	Profiles         []DJProfile `json:"profiles,omitempty"`
}

func DefaultDJProfiles() []DJProfile {
	return []DJProfile{
		{
			ID:            "all-discovery",
			Name:          "All-Library Discovery",
			Description:   "Endless mix across your entire library with smooth BPM flow",
			ExcludeGenres: []string{"Christmas"},
			PreferBPMFlow: true,
			StationName:   "Echo FM",
			Persona:       "late-night-fm",
		},
		{
			ID:            "late-night-chill",
			Name:          "Late Night Chill",
			Description:   "Mellow, atmospheric & slower grooves (65–114 BPM)",
			Genres:        []string{"Ambient", "Downtempo", "Jazz", "Folk", "Soul", "Acoustic", "Soft Rock", "New Age"},
			ExcludeGenres: []string{"Metal", "Hardcore", "Christmas"},
			MinBPM:        65,
			MaxBPM:        114,
			PreferBPMFlow: true,
			StationName:   "Echo Midnight",
			Persona:       "nordic-chill",
		},
		{
			ID:            "prime-favorites",
			Name:          "Top Rated Favorites (4–5★)",
			Description:   "Your starred 4 & 5-star library favorites",
			ExcludeGenres: []string{"Christmas"},
			MinRating:     4,
			PreferBPMFlow: true,
			StationName:   "Echo Gold",
			Persona:       "upbeat-radio",
		},
		{
			ID:            "high-energy-drive",
			Name:          "High Energy Drive",
			Description:   "Upbeat, dance & energetic tracks (116–168 BPM)",
			Genres:        []string{"Electronic", "Dance", "Pop", "Rock", "Synth", "Disco", "House", "Electro"},
			ExcludeGenres: []string{"Christmas", "Classical"},
			MinBPM:        116,
			MaxBPM:        168,
			PreferBPMFlow: true,
			StationName:   "Echo Pulse",
			Persona:       "party-hype",
		},
		{
			ID:            "80s-90s-throwback",
			Name:          "80s & 90s Throwback",
			Description:   "Classic tracks from the 1980s & 1990s with deep liner-note stories",
			ExcludeGenres: []string{"Christmas"},
			Eras:          []string{"1980s", "1990s"},
			PreferBPMFlow: true,
			StationName:   "Echo Retro FM",
			Persona:       "vinyl-nerd",
		},
	}
}

func DefaultDJConfig() DJConfig {
	return DJConfig{
		Provider:         "gemini",
		LLMModel:         "gemini-3.8-flash",
		TTSModel:         "gemini-3.8-flash-tts",
		VoiceName:        "Kore",
		Persona:          "late-night-fm",
		Language:         "English",
		RadioStationName: "Echo FM",
		TalkFrequency:    0.65,
		CrossfadeSeconds: 5.0,
		DuckVolume:       0.35,
		AutoRefill:       true,
		MinQueueAhead:    3,
		MaxScriptWords:   38,
		ActiveProfileID:  "all-discovery",
		Profiles:         DefaultDJProfiles(),
	}
}

// RemoteConfig holds remote access settings (Cloudflare Tunnel + UPnP-IGD/NAT-PMP router auto-port-forwarding).
type RemoteConfig struct {
	// Cloudflare Tunnel (zero open router ports, automatic trusted HTTPS)
	CloudflareEnabled     bool   `json:"cloudflareEnabled"`
	CloudflareMode        string `json:"cloudflareMode"` // "quick" (trycloudflare.com) | "token" (named tunnel)
	CloudflareToken       string `json:"cloudflareToken,omitempty"`
	CloudflareExposeDJ    bool   `json:"cloudflareExposeDJ"`
	CloudflareExposeParty bool   `json:"cloudflareExposeParty"`

	// UPnP-IGD / NAT-PMP Router Auto-Port-Forwarding (off by default, guarded by remote PIN auth)
	UPnPEnabled          bool   `json:"upnpEnabled"`
	UPnPExposeParty      bool   `json:"upnpExposeParty"`
	UPnPExposeDJ         bool   `json:"upnpExposeDJ"`
	UPnPExternalParty    int    `json:"upnpExternalParty"` // 0 = same as PartyHTTPPort
	UPnPExternalDJ       int    `json:"upnpExternalDJ"`    // 0 = same as APIPort
	UPnPGatewayOverride  string `json:"upnpGatewayOverride,omitempty"`
	RequirePINOnRemote   bool   `json:"requirePINOnRemote"` // Always true by default for WAN security
}

// DLNAConfig holds DLNA / UPnP MediaServer advertisement & LAN speaker/AVR casting settings.
type DLNAConfig struct {
	Enabled      bool   `json:"enabled"`
	FriendlyName string `json:"friendlyName,omitempty"`
}

func DefaultRemoteConfig() RemoteConfig {
	return RemoteConfig{
		CloudflareMode:        "quick",
		CloudflareExposeDJ:    true,
		CloudflareExposeParty: true,
		UPnPExposeParty:       true,
		UPnPExposeDJ:          false,
		RequirePINOnRemote:    true,
	}
}

func DefaultDLNAConfig() DLNAConfig {
	return DLNAConfig{
		Enabled: true,
	}
}

// Config holds persisted + environment-overridden server configuration.
type Config struct {
	ConfigDir               string          `json:"-"`
	ServerName              string          `json:"serverName"`
	PairingPIN              string          `json:"pairingPIN"`
	APIPort                 int             `json:"apiPort"`
	PartyHTTPPort           int             `json:"partyHTTPPort"`
	PartyHTTPSPort          int             `json:"partyHTTPSPort"`
	AdvertiseHost           string          `json:"advertiseHost,omitempty"`
	ScanInterval            time.Duration   `json:"-"`
	ScanIntervalS           string          `json:"scanInterval"`
	ShardCount              int             `json:"shardCount"`
	MaxDeltaRevs            int             `json:"maxDeltaRevs"`
	EnableMDNS              bool            `json:"enableMDNS"`
	AutoFetchEditorial      bool            `json:"autoFetchEditorial"`
	MirrorToShare           bool            `json:"mirrorToShare"`
	LidarrURL               string          `json:"lidarrURL,omitempty"`
	LidarrAPIKey            string          `json:"lidarrAPIKey,omitempty"`
	LidarrPollEnabled       bool            `json:"lidarrPollEnabled"`
	AdvancedAnalysisEnabled bool            `json:"advancedAnalysisEnabled"`
	DJ                      DJConfig        `json:"dj"`
	Remote                  RemoteConfig    `json:"remote"`
	DLNA                    DLNAConfig      `json:"dlna"`
	Libraries               []LibraryConfig `json:"libraries"`
}

func Load() (*Config, error) {
	cfgDir := envOr("ECHO_CONFIG_DIR", "/config")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		return nil, fmt.Errorf("create config dir %s: %w", cfgDir, err)
	}

	cfg := &Config{
		ConfigDir:          cfgDir,
		ServerName:         "My Echo Server",
		APIPort:            48721,
		PartyHTTPPort:      53333,
		PartyHTTPSPort:     53334,
		ScanIntervalS:      "30m",
		ShardCount:         16,
		MaxDeltaRevs:       100,
		EnableMDNS:         true,
		AutoFetchEditorial: true,
		MirrorToShare:      true,
		LidarrPollEnabled:  true,
		DJ:                 DefaultDJConfig(),
		Remote:             DefaultRemoteConfig(),
		DLNA:               DefaultDLNAConfig(),
	}

	cfgFile := filepath.Join(cfgDir, "config.json")
	if data, err := os.ReadFile(cfgFile); err == nil {
		_ = json.Unmarshal(data, cfg)
	}

	// Environment overrides
	if v := strings.TrimSpace(os.Getenv("ECHO_SERVER_NAME")); v != "" {
		cfg.ServerName = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_PIN")); v != "" {
		cfg.PairingPIN = v
	}
	if v := envInt("ECHO_API_PORT", 0); v > 0 {
		cfg.APIPort = v
	}
	if v := envInt("ECHO_PARTY_PORT", 0); v > 0 {
		cfg.PartyHTTPPort = v
	}
	if v := envInt("ECHO_PARTY_HTTPS_PORT", 0); v > 0 {
		cfg.PartyHTTPSPort = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_ADVERTISE_HOST")); v != "" {
		cfg.AdvertiseHost = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_SCAN_INTERVAL")); v != "" {
		cfg.ScanIntervalS = v
	}
	if v := envInt("ECHO_SHARD_COUNT", 0); v >= 1 && v <= 256 {
		cfg.ShardCount = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_ENABLE_MDNS")); v != "" {
		cfg.EnableMDNS = v != "0" && strings.ToLower(v) != "false"
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_LIDARR_URL")); v != "" {
		cfg.LidarrURL = strings.TrimRight(v, "/")
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_LIDARR_API_KEY")); v != "" {
		cfg.LidarrAPIKey = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_GEMINI_API_KEY")); v != "" {
		cfg.DJ.GeminiAPIKey = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_DJ_LLM_MODEL")); v != "" {
		cfg.DJ.LLMModel = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_DJ_TTS_MODEL")); v != "" {
		cfg.DJ.TTSModel = v
	}
	if v := strings.TrimSpace(os.Getenv("ECHO_DJ_VOICE")); v != "" {
		cfg.DJ.VoiceName = v
	}
	NormalizeDJConfig(&cfg.DJ)

	// Parse libraries from ECHO_LIBRARIES (format: "id:Name:/path[|/Volumes/...],...") or default /music
	if rawLibs := strings.TrimSpace(os.Getenv("ECHO_LIBRARIES")); rawLibs != "" {
		cfg.Libraries = parseLibrariesEnv(rawLibs, cfg.ShardCount)
	} else if len(cfg.Libraries) == 0 {
		musicDir := envOr("ECHO_MUSIC_DIR", "/music")
		legacyPrefix := strings.TrimSpace(os.Getenv("ECHO_LEGACY_PATH_PREFIX"))
		cfg.Libraries = []LibraryConfig{
			{
				ID:              "music",
				Name:            envOr("ECHO_LIBRARY_NAME", "MUSIC"),
				Path:            musicDir,
				LegacyMacPrefix: legacyPrefix,
				ShardCount:      cfg.ShardCount,
			},
		}
	}

	for i := range cfg.Libraries {
		if cfg.Libraries[i].ShardCount <= 0 {
			cfg.Libraries[i].ShardCount = cfg.ShardCount
		}
		if cfg.Libraries[i].ID == "" {
			cfg.Libraries[i].ID = fmt.Sprintf("lib-%d", i+1)
		}
		if cfg.Libraries[i].Name == "" {
			cfg.Libraries[i].Name = filepath.Base(cfg.Libraries[i].Path)
		}
	}

	if strings.TrimSpace(cfg.PairingPIN) == "" {
		cfg.PairingPIN = generateSixDigitPIN()
	}

	if strings.ToLower(cfg.ScanIntervalS) == "manual" || cfg.ScanIntervalS == "0" {
		cfg.ScanInterval = 0
	} else if d, err := time.ParseDuration(cfg.ScanIntervalS); err == nil && d >= time.Minute {
		cfg.ScanInterval = d
	} else {
		cfg.ScanInterval = 30 * time.Minute
	}

	if cfg.MaxDeltaRevs < 10 {
		cfg.MaxDeltaRevs = 100
	}

	_ = cfg.Save()
	return cfg, nil
}

func (c *Config) Save() error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(c.ConfigDir, "config.json.tmp")
	final := filepath.Join(c.ConfigDir, "config.json")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

func (c *Config) EffectiveAdvertiseHost() string {
	if c.AdvertiseHost != "" {
		return c.AdvertiseHost
	}
	return DetectLANIP()
}

func (c *Config) DBPath() string {
	return filepath.Join(c.ConfigDir, "echo-server.sqlite")
}

func (c *Config) ArtworkDir() string {
	return filepath.Join(c.ConfigDir, "artwork")
}

func (c *Config) ShardsDir() string {
	return filepath.Join(c.ConfigDir, "shards")
}

func (c *Config) TLSDir() string {
	return filepath.Join(c.ConfigDir, "tls")
}

func (c *Config) IntrosDir() string {
	return filepath.Join(c.ConfigDir, "party-intros")
}

func (c *Config) DJCacheDir() string {
	return filepath.Join(c.ConfigDir, "dj-audio")
}

func NormalizeDJConfig(dj *DJConfig) {
	def := DefaultDJConfig()
	if strings.TrimSpace(dj.Provider) == "" {
		dj.Provider = def.Provider
	}
	if strings.TrimSpace(dj.LLMModel) == "" {
		dj.LLMModel = def.LLMModel
	}
	if strings.TrimSpace(dj.TTSModel) == "" {
		dj.TTSModel = def.TTSModel
	}
	if strings.TrimSpace(dj.VoiceName) == "" {
		dj.VoiceName = def.VoiceName
	}
	if strings.TrimSpace(dj.Persona) == "" {
		dj.Persona = def.Persona
	}
	if strings.TrimSpace(dj.Language) == "" {
		dj.Language = def.Language
	}
	if strings.TrimSpace(dj.RadioStationName) == "" {
		dj.RadioStationName = def.RadioStationName
	}
	if dj.TalkFrequency < 0 || dj.TalkFrequency > 1 {
		dj.TalkFrequency = def.TalkFrequency
	}
	if dj.CrossfadeSeconds < 0 || dj.CrossfadeSeconds > 15 {
		dj.CrossfadeSeconds = def.CrossfadeSeconds
	}
	if dj.DuckVolume < 0.1 || dj.DuckVolume > 0.85 {
		dj.DuckVolume = def.DuckVolume
	}
	if dj.MinQueueAhead < 1 {
		dj.MinQueueAhead = def.MinQueueAhead
	}
	if dj.MinQueueAhead > 12 {
		dj.MinQueueAhead = 12
	}
	if dj.MaxScriptWords < 15 || dj.MaxScriptWords > 120 {
		dj.MaxScriptWords = def.MaxScriptWords
	}
	if len(dj.Profiles) == 0 {
		dj.Profiles = DefaultDJProfiles()
		// Preserve AutoRefill default on migration from older config.json
		dj.AutoRefill = true
	}
	if strings.TrimSpace(dj.ActiveProfileID) == "" && len(dj.Profiles) > 0 {
		dj.ActiveProfileID = dj.Profiles[0].ID
	}
}

func (c *Config) LibraryByID(id string) (LibraryConfig, bool) {
	for _, lib := range c.Libraries {
		if lib.ID == id {
			return lib, true
		}
	}
	return LibraryConfig{}, false
}

func parseLibrariesEnv(raw string, defaultShards int) []LibraryConfig {
	var out []LibraryConfig
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		var legacyPrefix string
		if idx := strings.Index(entry, "|"); idx >= 0 {
			legacyPrefix = strings.TrimSpace(entry[idx+1:])
			entry = strings.TrimSpace(entry[:idx])
		}
		parts := strings.SplitN(entry, ":", 3)
		switch len(parts) {
		case 3:
			out = append(out, LibraryConfig{
				ID:              sanitizeSlug(parts[0]),
				Name:            strings.TrimSpace(parts[1]),
				Path:            strings.TrimSpace(parts[2]),
				LegacyMacPrefix: legacyPrefix,
				ShardCount:      defaultShards,
			})
		case 2:
			out = append(out, LibraryConfig{
				ID:              sanitizeSlug(parts[0]),
				Name:            strings.TrimSpace(parts[0]),
				Path:            strings.TrimSpace(parts[1]),
				LegacyMacPrefix: legacyPrefix,
				ShardCount:      defaultShards,
			})
		default:
			base := filepath.Base(entry)
			out = append(out, LibraryConfig{
				ID:              sanitizeSlug(base),
				Name:            base,
				Path:            entry,
				LegacyMacPrefix: legacyPrefix,
				ShardCount:      defaultShards,
			})
		}
	}
	return out
}

func sanitizeSlug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else if r == ' ' {
			b.WriteRune('-')
		}
	}
	if b.Len() == 0 {
		return "music"
	}
	return b.String()
}

func generateSixDigitPIN() string {
	n, err := rand.Int(rand.Reader, big.NewInt(900000))
	if err != nil {
		return "487210"
	}
	return fmt.Sprintf("%06d", n.Int64()+100000)
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// DetectLANIP returns the best non-loopback IPv4 LAN address.
func DetectLANIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}
	var fallback string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip4 := ip.To4()
			if ip4 == nil {
				continue
			}
			s := ip4.String()
			if strings.HasPrefix(s, "169.254.") {
				continue
			}
			if strings.HasPrefix(s, "192.168.") || strings.HasPrefix(s, "10.") || strings.HasPrefix(s, "172.") {
				return s
			}
			if fallback == "" {
				fallback = s
			}
		}
	}
	if fallback != "" {
		return fallback
	}
	return "127.0.0.1"
}
