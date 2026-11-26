# My Echo Server — Self-Hosted Hi-Fi Music Server, AI Radio DJ & Party Jukebox

**My Echo Server** is a lightweight, single-binary Go + Docker music server designed for **Unraid**, NAS boxes, and Linux homelabs. It works completely **standalone** in any web browser or DLNA speaker/TV, and also pairs seamlessly as the high-speed NAS companion server for [**My Echo**](https://github.com/baardove/my-echo) on **macOS, iOS, and Apple TV (`MyEchoTV`)**.

> **Companion Native Apps:** Looking for the native SwiftUI **macOS studio**, **DAP/FiiO sync engine**, **Winamp Mini Player**, **iPhone/iPad offline player**, and **Apple TV (`MyEchoTV`)** app? Check out the sister repository: [**baardove/my-echo**](https://github.com/baardove/my-echo).

---

## Highlights & Features

### 1. Standalone Dark-Neon Web Player & Fast Local NAS Indexing
- **Zero-Overhead Local Indexing:** Scans your NAS music share (`/music`) directly off local disks into an embedded **SQLite** database (`modernc.org/sqlite`, zero CGO) and partitioned gzip-compressed NDJSON shards + incremental delta changelogs.
- **Full Hi-Fi Format Support:** Streams lossless `FLAC`, `ALAC`, `WAV`, `MP3`, `M4A/AAC`, `OGG`, `OPUS`, `APE`, `AIFF`, and `DSD` with `HTTP 206` byte-range seeking and automatic 320 kbps MP3 fallback (`?transcode=1`) for codecs unsupported by a given browser.
- **On-the-Fly BPM & Loudness Normalization:** Automatically analyzes tracks for **BPM**, **true peak**, and **EBU R128-style gated loudness (`-14.0 dBFS` target)** in the background and on-demand for active rundowns. Applies smooth Web Audio API loudness normalization + soft peak limiting in the browser and `ffmpeg` loudness normalization + limiting on live DLNA/HTTP radio streams.
- **1–5 Star Ratings, Playlists & Editorial Notes:** Rate tracks, browse artists/albums/genres/playlists, and view artist bios & album editorial reviews right in the Web UI.

### 2. AI Radio DJ (Powered by Google Gemini)
- **Live Personalized Radio Shows:** Pick a mood, genre, artist seed, or custom prompt and let your AI DJ curate a track rundown from your own music library.
- **Fact-Grounded Banter & Natural Voices:** Uses **Gemini** with optional Google Search grounding and **Gemini TTS** to generate timely, music-knowledgeable voice intros, bridges, and outros tailored to the actual tracks playing — complete with smart intro/outro cadence, crossfading, ducking, and clickless audio fades.
- **Interactive Live Rundown:** Drag and drop upcoming tracks to reorder the show on the fly — affected DJ bridges automatically regenerate to match the new song transitions.
- **Customizable DJ Profiles:** Create multiple DJ personas with distinct voices, speaking styles, languages, and intro/outro cadence.

### 3. Party Mode Jukebox (Collaborative Guest Queue & Voice Intros)
- **Zero-Install Guest Web Jukebox (`:53333`):** Share a QR code or link so guests on any phone, tablet, or laptop can search your full library, queue songs, see live scheduled play times, chat in real time, and listen along.
- **Broadcast-Processed Mobile DJ Voice Intros (`:53334` HTTPS):** Guests can record a short voice intro right from mobile Safari/Chrome; the server runs it through an `ffmpeg` broadcast vocal chain (EQ, compression, normalization) and ducks it smoothly over the incoming track.

### 4. DLNA MediaServer & One-Click LAN Casting
- **Built-in DLNA MediaServer (SSDP):** Appears natively on your LAN as a UPnP/DLNA MediaServer exposing live **AI Radio DJ** (`/stream/dj.mp3`) and **Party Mode** (`/stream/party.mp3`) MP3 streams — with **0% idle CPU** (encoding spins up lazily only while a listener or speaker is connected).
- **One-Click Cast to Speakers & TVs:** Discover DLNA/UPnP renderers on your home network (Sonos, Denon/HEOS, Marantz, Yamaha, AVR receivers, smart TVs, WiiM, etc.) and cast live DJ Radio or Party Mode with one click from the Web UI.

### 5. Remote Access — Cloudflare Tunnel, UPnP/NAT-PMP & PIN Gate
- **Built-in Cloudflare Tunnel (`cloudflared`):** Expose your DJ Radio or Party Mode securely over HTTPS (`https://*.trycloudflare.com` quick tunnels or your own Named Tunnel token) in one click — no router port forwarding needed.
- **Automatic Router Port Forwarding (UPnP-IGD & NAT-PMP):** Pure-Go router mapping with automatic lease renewal and clean unmapping on shutdown when running with host networking.
- **Remote PIN Login Gate:** Local LAN browsers connect seamlessly, while external/tunnel visitors are protected by an HMAC-signed `HttpOnly` session cookie + rate-limited 6-digit PIN gate.

### 6. Lidarr Integration & Disk Spin-Down Friendly Scanning
- **In-App Lidarr Search & Ordering:** Search for missing artists/albums and request them directly through your Lidarr instance from the Web UI.
- **Targeted Webhook Indexing:** Set `ECHO_SCAN_INTERVAL=manual` (or `24h`) to keep Unraid array disks spun down, and point Lidarr's **On Import / On Upgrade** webhook at:
  ```text
  POST http://<SERVER-IP>:48721/api/v2/webhooks/lidarr?token=<PIN>
  ```
  for instant single-artist folder rescans.

### 7. Native Companion for My Echo (macOS, iOS & Apple TV)
When paired with [**My Echo**](https://github.com/baardove/my-echo):
- **macOS (`My Echo`):** Offloads slow SMB/Wi-Fi NAS indexing to local Unraid disks and fetches tiny gzip delta shards in milliseconds, with two-way 1–5 star rating sync.
- **Apple TV (`MyEchoTV`):** Streams your full lossless library, artwork, editorial notes, playlists, and Party Mode directly from your NAS even when your Mac is asleep or powered off.

---

## Quick Start

### Option 1: Docker Compose (Recommended for Linux / NAS)

```yaml
services:
  my-echo-server:
    image: ghcr.io/baardove/my-echo-server:latest
    container_name: my-echo-server
    restart: unless-stopped
    network_mode: host # Enables DLNA discovery/casting & UPnP/NAT-PMP port mapping
    environment:
      - PUID=99
      - PGID=100
      - TZ=Europe/Oslo
      - ECHO_SERVER_NAME=My Echo Server
      - ECHO_SCAN_INTERVAL=30m
    volumes:
      - /path/to/appdata/my-echo-server:/config
      - /path/to/your/music:/music:rw
```

Run it:
```bash
docker compose up -d
```

Then open the Web UI at:
```text
http://<SERVER-IP>:48721/
```
Check the container logs (or **Settings** on LAN) for your auto-generated **6-digit PIN** (or set `ECHO_PIN`).

### Option 2: Unraid

1. **Manual Template Install:** Copy [`templates/my-echo-server.xml`](templates/my-echo-server.xml) into `/boot/config/plugins/dockerMan/templates-user/` on your Unraid server.
2. In the Unraid WebGUI, go to **Docker → Add Container → Select a template → `my-echo-server`**.
3. Set your `/music` share path (e.g. `/mnt/user/Media/MUSIC`) and click **Apply**.
4. Keep **Network Type: `Host`** so DLNA discovery/casting and UPnP/NAT-PMP work out of the box.

### Option 3: Build From Source

```bash
git clone https://github.com/baardove/my-echo-server.git
cd my-echo-server
docker build -t my-echo-server:latest .
```

Or run locally with Go 1.24+ and `ffmpeg` installed:
```bash
go test ./...
go run ./cmd/echo-server
```

---

## Configuration (Environment Variables)

| Variable | Default | Description |
| --- | --- | --- |
| `ECHO_SERVER_NAME` | `Unraid Echo` | Display name shown in the Web UI, DLNA MediaServer, Party Mode, and My Echo clients. |
| `ECHO_ADVERTISE_HOST` | *(auto-detected)* | Explicit LAN or Tailscale IP used for DLNA URLs and Party Mode QR codes (useful in bridge mode or multi-NIC hosts). |
| `ECHO_PIN` | *(auto-generated)* | Fixed 6-digit PIN for remote Web UI login and Mac/Apple TV pairing. Stored in `/config/config.json` if omitted. |
| `ECHO_SCAN_INTERVAL` | `30m` | Incremental library scan interval (`30m`, `2h`, `24h`, or `manual` for webhook-only scanning). |
| `ECHO_LEGACY_PATH_PREFIX` | *(empty)* | Optional macOS SMB mount prefix (e.g. `/Volumes/Media/MUSIC`) so Track IDs match existing My Echo macOS playlists & ratings 1:1. |
| `ECHO_LIBRARY_NAME` | `MUSIC` | Logical library name exposed to clients. |
| `ECHO_SHARD_COUNT` | `16` | Number of gzip-compressed index shards for delta sync. |
| `PUID` / `PGID` | `99` / `100` | Container user/group IDs (defaults match Unraid `nobody:users`). |

Gemini API keys, DJ profiles, Lidarr credentials, Cloudflare Tunnel tokens, and DLNA/UPnP toggles can all be configured live in the **Settings** tab of the Web UI and are stored persistently in `/config/config.json`.

---

## Ports

| Port | Protocol | Purpose |
| --- | --- | --- |
| `48721` | TCP | Main API, Web Player, AI Radio DJ, Admin UI, `/stream/dj.mp3`, `/stream/party.mp3`, DLNA HTTP |
| `53333` | TCP | Party Mode Guest Web Jukebox (HTTP) |
| `53334` | TCP | Party Mode Mobile DJ Voice Intro Recorder (HTTPS) |
| `1900` | UDP | DLNA / UPnP SSDP multicast discovery (active automatically in `host` network mode) |

---

## Related Project

- [**My Echo (`baardove/my-echo`)**](https://github.com/baardove/my-echo) — Native Swift 6 / SwiftUI **macOS music & DAP sync studio** (FiiO / Snowsky / SD cards), floating Winamp Mini Player, Universal **iPhone & iPad** offline Hi-Fi player, and **Apple TV (`MyEchoTV`)** living-room client.

---

## License

Released under the [MIT License](LICENSE).
