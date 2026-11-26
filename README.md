# My Echo Server (Unraid / Docker Companion)

**My Echo Server** is an optional, lightweight (~45 MB) self-hosted Docker container for Unraid (or any Linux/Docker host) that:

1. **Offloads NAS Indexing:** Scans your Unraid music shares directly off local storage (read-only `/music:ro`) into **partitioned, gzip-compressed NDJSON shards + incremental delta changelogs** so My Echo on Mac fetches only KB-sized changes instead of walking SMB over Wi-Fi.
2. **Makes Apple TV (`MyEchoTV`) Independent of Mac:** Serves your full catalog, artwork, playlists, podcasts, editorial notes, and 1-hop lossless audio streaming (`HTTP 206`) directly from Unraid — even when your Mac is asleep or powered off.
3. **Hosts Always-On Party Mode:** Runs the collaborative Party Mode Guest Web Jukebox (`:53333` HTTP + `:53334` HTTPS for mobile DJ Voice Intro recording + `ffmpeg` broadcast vocal processing), with Apple TV acting as a native standalone audio renderer (including 40%-ducked voice intros!).

> **Standalone Mac Preserved:** My Echo on macOS requires zero server setup for local folders, external USB drives, or traditional SMB/NFS NAS shares.

---

## Quick Start on Unraid

### Option 1: Docker Compose (`Server/docker-compose.yml`)
```bash
cd Server
docker compose up -d --build
```

### Option 2: Unraid Template (`Server/unraid/my-echo-server.xml`)
Copy `Server/unraid/my-echo-server.xml` to `/boot/config/plugins/dockerMan/templates-user/` on Unraid, then add the container from **Docker → Add Container → Select a template → my-echo-server**.

Open the Admin WebUI at:
```text
http://<UNRAID-IP>:48721/admin
```
Note the auto-generated **6-digit Pairing PIN** (or set `ECHO_PIN`).

---

## Connecting Clients

- **My Echo on macOS (Optional):** Open **Settings (`⌘,`) → My Echo Server (Unraid / Docker)**, toggle ON, enter your Unraid LAN/Tailscale URL (`http://<UNRAID-IP>:48721`) + 6-digit PIN, click **Save & Test**, then click the server icon next to your NAS source in **Sources & NAS** to switch it to compressed delta indexing (reversible at any time).
- **My Echo on Apple TV (`MyEchoTV`):** Open **Servers**, enter `http://<UNRAID-IP>:48721` + 6-digit PIN, and tap **Connect & Load Full Library**. You can now stream lossless music and start **Party Mode** directly on Apple TV without your Mac running.
- **Lidarr Auto-Index Webhook (Unraid Disk-Spin-Down Friendly):** Set `ECHO_SCAN_INTERVAL=manual` (or `24h`) and add a Lidarr Webhook pointing to `POST http://<UNRAID-IP>:48721/api/v2/webhooks/lidarr?token=<PIN>` on **On Import / On Upgrade** for targeted single-artist folder scans.
