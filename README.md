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

- `config/config.json` — quality (`DOWNLOAD_QUALITY`), format (`DOWNLOAD_FORMAT`). Set to
  `ogg`: Spotify streams Ogg Vorbis, so zotify maps `ogg` to a ffmpeg stream **copy** —
  no re-encode, so the file is bit-identical to what Spotify sent (with `very_high` that's
  320 kbps Vorbis). `mp3` decodes and re-encodes, i.e. a generation of lossy loss, for
  players that can't read Vorbis. Every track is available as `ogg` — it's the source
  format, not an alternate one. (Podcast *episodes* are the exception: they aren't Vorbis,
  so `ogg` re-encodes them via `libvorbis`.)
- `MAX_CONCURRENT=1` in `.env` — Spotify rate-limits hard; leave it at 1. A job holds
  its slot for its *whole* lifecycle, download **and** the beets/NAS phase, so two
  albums never overlap.
- **Song archive** — `SONG_ARCHIVE_LOCATION` + `SKIP_PREVIOUSLY_DOWNLOADED` in
  `config/config.json` make zotify skip tracks it has already pulled, so re-queueing an
  album you own costs no bandwidth and no rate-limit exposure. zotify treats the archive
  as *disabled while the file is missing*, so `data/.song_archive` is the on/off switch:

  ```bash
  touch data/.song_archive     # enable
  rm    data/.song_archive     # disable (entries are kept if you just move it aside)
  ```

  Entries are `id⇥timestamp⇥artist⇥title⇥path`, appended per track as it lands. The
  wrapper prunes a job's entries whenever its files are discarded (cancel, failure,
  stall, retry) — otherwise a retry would skip those tracks and quietly produce a
  partial album. To force a re-download of something you deleted by hand, drop its
  line from `data/.song_archive`.
- Nothing is deleted from a job's staging dir until the audio has provably left it.
  beets exits 0 even when it imports nothing (e.g. it skips a directory), so a raw
  copy to the NAS is the fallback rather than an `rm -rf`.
- **Release pinning (`LIDARR_URL` / `LIDARR_API_KEY`)** — beets and Lidarr each run
  their own MusicBrainz match, and they regularly land on *different releases* of the
  same album (beets matched Clayman to the 12-track JP release while Lidarr monitored
  the 13-track US one). Lidarr then only matches the subset of tracks that line up, and
  Navidrome — which keys albums on `MusicBrainz Album Id` — draws the album twice. With
  these set, the wrapper asks Lidarr which release it monitors and passes it to
  `beet import -S <mbid>`, so both sides agree before a single tag is written. Matching
  is exact-then-unique-prefix on the album title, because Spotify drops subtitles
  ("Reroute To Remain" vs "Reroute to Remain: Fourteen Songs of Conscious Insanity").
  Entirely best-effort: Lidarr down, album not in Lidarr, or an ambiguous title all fall
  back to beets choosing on its own, exactly as before. Leave `LIDARR_URL` empty to
  disable. Use the LAN address — the two containers aren't on a shared docker network.
- **Partial imports are no longer copied in.** `match.max_rec.unmatched_tracks: strong`
  lets beets auto-accept a release with *fewer* tracks than were downloaded, orphaning
  the extras with nothing but zotify's bare Spotify tags. Dropping those next to
  properly tagged files is what splits an album in Navidrome. When beets tags some
  tracks but not all, the tagged ones are filed and the rest are **left in
  `work/job-<id>`** with a warning on the job in the UI, rather than landing in the
  library untagged.
- Legal: this is against Spotify's ToS. Personal/testing use, your call.
