# zotify-wrapper

A small web UI (mirrors `yt-dlp-wrapper`) that queues Spotify URLs and downloads them
with [zotify](https://github.com/Googolplexed0/zotify), landing files on the NAS.

- **UI / port:** http://192.168.1.2:3106
- **Downloads to:** `/mnt/pi-nas/zotify` (change `NAS_MOUNT` in `.env` to point at the
  Plex/Lidarr library once you're happy — e.g. `/mnt/pi-nas/plex/music`)
- **Backend:** express + better-sqlite3, spawns the `zotify` CLI per job into an isolated
  `work/job-<id>` dir, then copies the album tree to the NAS.

## ⚠️ One-time setup: credentials.json (required)

Spotify killed username/password login for these tools (Aug 2025). You must generate a
reusable `credentials.json` **once** and drop it in `./credentials/`. Premium is what
unlocks `very_high` (320 kbps).

Easiest path — let zotify's own first-run login create it, using the Spotify **desktop
app** on the same LAN to authorise:

```bash
cd ~/zotify-wrapper
docker compose build          # build the image first (see below)

# Interactive one-shot: run zotify by hand inside the image to trigger login.
docker compose run --rm --entrypoint zotify app \
  -c /app/config/config.json \
  --credentials-location /app/credentials/credentials.json \
  https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT
```

When it prompts to log in / "connect a device": open the **Spotify desktop app**, click the
**Connect to a device** icon, and pick the device that appears (named like *librespot* /
*zotify*). That hands the reusable token to the container, which writes
`./credentials/credentials.json`. Ctrl-C once the test track downloads.

If that flow doesn't appear in this fork, fall back to
[librespot-auth](https://github.com/dspearson/librespot-auth) (see zotify issue #223) to
produce the blob, then copy it to `./credentials/credentials.json`.

Verify:

```bash
test -f credentials/credentials.json && echo "creds present"
```

## Run

```bash
cd ~/zotify-wrapper
cp .env.example .env          # optional, edit if you want
docker compose up -d --build
docker compose logs -f
```

Paste a track/album/playlist/artist URL in the UI and it queues. Until
`credentials.json` exists, every job fails fast with a clear message (by design).

## Glance widget

Add to `~/glance/config/glance.yml` (same pattern as the WhatsApp iframe):

```yaml
          - type: iframe
            title: Zotify
            source: http://192.168.1.2:3106
            height: 800
```

Then `cd ~/glance && docker compose restart`.

## Notes / knobs

- `config/config.json` — quality (`DOWNLOAD_QUALITY`), format (`DOWNLOAD_FORMAT`:
  `mp3` for max compatibility, `ogg` to keep Spotify's native codec lossless-of-source).
- `MAX_CONCURRENT=1` in `.env` — Spotify rate-limits hard; leave it at 1.
- Legal: this is against Spotify's ToS. Personal/testing use, your call.
