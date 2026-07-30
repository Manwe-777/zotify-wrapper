import { spawn } from 'child_process';
import {
  mkdirSync, existsSync, statSync, rmSync, readdirSync,
  readFileSync, writeFileSync, createReadStream, createWriteStream,
} from 'fs';
import { join, relative, dirname, basename } from 'path';
import { pipeline } from 'stream/promises';

const WORK_DIR = process.env.WORK_DIR || '/app/work';
const NAS_DIR = process.env.NAS_DIR || '/nas/music';
const ZOTIFY_CONFIG = process.env.ZOTIFY_CONFIG || '/app/config/config.json';
const CREDENTIALS_FILE = process.env.CREDENTIALS_FILE || '/app/credentials/credentials.json';
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || '1', 10);
const STALL_TIMEOUT_MS = parseInt(process.env.STALL_TIMEOUT_MS || '600000', 10);
// zotify path template. Passed on the CLI (authoritative — get_output() returns the
// master OUTPUT first) so it applies to singles, albums and playlists alike.
// Tokens: {album_artist} {artist} {album} {year} {track_number}(zero-padded) {song_name}.
const OUTPUT_TEMPLATE =
  process.env.OUTPUT_TEMPLATE || '{album_artist}/{album} ({year})/{track_number}. {song_name}';
// beets = post-download MusicBrainz auto-tag + directory organizer. Set USE_BEETS=false
// to skip it and fall back to a plain copy into the library.
const USE_BEETS = (process.env.USE_BEETS ?? 'true') !== 'false';
const BEETS_CONFIG = process.env.BEETS_CONFIG || '/app/config/beets.yaml';
// zotify's global song archive: SONG_ARCHIVE_LOCATION from config.json + '/.song_archive'.
// It only becomes active once this file exists (zotify disables the archive when it's
// missing), so creating/deleting it is the on/off switch for skip-previously-downloaded.
const SONG_ARCHIVE = process.env.SONG_ARCHIVE || '/app/data/.song_archive';
// beets organises into this LOCAL dir (it must match `directory:` in beets.yaml),
// then we ship the result to the NAS ourselves in one sequential pass. beets does
// several whole-file passes per track — scrub strips tags, then it writes corrected
// tags and embeds the cover — and on CIFS each pass pays the SMB round-trip penalty:
// measured 31s/track to local disk vs minutes/track straight onto the NAS.
const LIB_STAGE = process.env.LIB_STAGE || '/app/work/.lib-stage';
// Lidarr, used read-only to ask which MusicBrainz *release* it monitors for an album.
// beets otherwise runs its own MB match with its own preferences (see `preferred:` in
// beets.yaml) and regularly lands on a different release than Lidarr — e.g. Clayman
// matched the 12-track JP release while Lidarr monitored the 13-track US one. Both
// sides then disagree about which tracks belong, Lidarr only matches the subset that
// lines up, and Navidrome (which keys albums on MusicBrainz Album Id) draws the album
// twice. Pinning beets to Lidarr's release removes the disagreement at the source.
// Reachable over the LAN, not the docker bridge — the containers aren't on one network.
const LIDARR_URL = (process.env.LIDARR_URL || '').replace(/\/+$/, '');
const LIDARR_API_KEY = process.env.LIDARR_API_KEY || '';
const LIDARR_TIMEOUT_MS = parseInt(process.env.LIDARR_TIMEOUT_MS || '8000', 10);

// zotify/tqdm emit progress on stderr with carriage returns, e.g. " 45%|███ | 12/26 ..."
const PCT_RE = /(\d{1,3})%\|/;
// Album-level tqdm shows a bare "3/11" count (byte bars show "2.1M/4.6M", so digits-only wins).
const TRACKS_RE = /\|\s*(\d+)\/(\d+)\s*[[\]]/;
const AUDIO_RE = /\.(mp3|ogg|m4a|flac|opus|aac|wav)$/i;

// "01. Next Year.mp3" -> { num: 1, name: "Next Year" }
function parseTrackName(file) {
  const b = basename(file).replace(/\.[^.]+$/, '');
  const m = b.match(/^(\d+)[.\-_ ]+\s*(.*)$/);
  return m ? { num: parseInt(m[1], 10), name: m[2] || b } : { num: null, name: b };
}

// From a relative path like "Album Artist/Album Name (2012)/01. Song.mp3"
// derive { artist, album, year } (best-effort; tolerates other templates).
function albumInfoFromRel(rel) {
  const parts = rel.split('/').filter(Boolean);
  if (parts.length >= 3) {
    const y = parts[parts.length - 2].match(/^(.*?)\s*\((\d{4})\)\s*$/);
    return {
      artist: parts[0],
      album: y ? y[1] : parts[parts.length - 2],
      year: y ? y[2] : null,
    };
  }
  if (parts.length === 2) return { artist: parts[0], album: null, year: null };
  return { artist: null, album: null, year: null };
}

// Build a human title from the files actually produced — reliable regardless of
// how zotify words its log lines (tqdm bars made log-parsing unreliable).
function titleFromFiles(files) {
  const audio = files.filter((f) => AUDIO_RE.test(f));
  if (audio.length === 1) return basename(audio[0]).replace(/\.[^.]+$/, '');
  if (audio.length > 1) return `${audio.length} tracks`;
  return files.length ? basename(files[0]) : 'download';
}

export class Downloader {
  constructor(db) {
    this.db = db;
    this.active = new Map();      // id -> live zotify child (download phase only)
    this.movers = new Map();      // id -> live beet child (moving phase only)
    // A job holds a concurrency slot for its WHOLE lifecycle — download *and* the
    // beets/NAS phase. `active` empties as soon as zotify exits, which is minutes
    // before the job is actually finished, so it can't be the thing tick() gates on.
    this.busy = new Set();
    this.tracksTotal = new Map(); // id -> total track count parsed from zotify output
    mkdirSync(WORK_DIR, { recursive: true });
    try { mkdirSync(NAS_DIR, { recursive: true }); } catch {}
    this.recoverOrphans();
  }

  recoverOrphans() {
    this.db.prepare(`
      UPDATE downloads SET status = 'queued', pid = NULL, updated_at = ?
      WHERE status IN ('downloading','moving')
    `).run(Date.now());
  }

  enqueue(url) {
    const now = Date.now();
    const info = this.db.prepare(`
      INSERT INTO downloads (url, status, created_at, updated_at)
      VALUES (?, 'queued', ?, ?)
    `).run(url, now, now);
    this.tick();
    return info.lastInsertRowid;
  }

  cancel(id) {
    const row = this.db.prepare(`SELECT * FROM downloads WHERE id = ?`).get(id);
    if (!row) return false;
    const proc = this.active.get(id);
    if (proc) { try { proc.kill('SIGTERM'); } catch {} }
    // A job in 'moving' has no zotify left — it's beets that has to be stopped.
    const mover = this.movers.get(id);
    if (mover) { try { mover.kill('SIGTERM'); } catch {} }
    this.db.prepare(`
      UPDATE downloads SET status = 'cancelled', updated_at = ?, pid = NULL WHERE id = ?
    `).run(Date.now(), id);
    this.active.delete(id);
    this.movers.delete(id);
    this.busy.delete(id);
    this._discardJob(id);
    this.tick();
    return true;
  }

  retry(id) {
    // Belt and braces: the failure paths already prune, but never let a retry
    // inherit archive entries — that's what silently yields a partial album.
    this._discardJob(id);
    this.db.prepare(`
      UPDATE downloads
      SET status = 'queued', progress = 0, error = NULL, pid = NULL, updated_at = ?
      WHERE id = ? AND status IN ('failed','cancelled','stalled')
    `).run(Date.now(), id);
    this.tick();
  }

  delete(id) {
    this.cancel(id);
    this._cleanupJobDir(id);
    this.db.prepare(`DELETE FROM downloads WHERE id = ?`).run(id);
  }

  tick() {
    if (this.busy.size >= MAX_CONCURRENT) return;
    // created_at is milliseconds, so a burst of pasted URLs can tie; id breaks it
    // so the queue is strictly FIFO in the order they were added.
    const next = this.db.prepare(`
      SELECT * FROM downloads WHERE status = 'queued'
      ORDER BY created_at ASC, id ASC LIMIT 1
    `).get();
    if (!next) return;
    this.start(next);
    if (this.busy.size < MAX_CONCURRENT) this.tick();
  }

  _jobDir(id) { return join(WORK_DIR, `job-${id}`); }

  _cleanupJobDir(id) {
    try { rmSync(this._jobDir(id), { recursive: true, force: true }); } catch {}
  }

  // zotify appends to the archive per track, the moment each file is written — so a
  // job that dies half-way leaves its finished tracks archived while we delete the
  // files. A retry would then skip exactly those tracks and hand back a partial
  // album. The archive records the staging path, so this job's entries are precisely
  // the lines pointing into its job dir.
  _pruneArchive(id) {
    if (!existsSync(SONG_ARCHIVE)) return 0;
    const prefix = `${this._jobDir(id)}/`;
    let lines;
    try { lines = readFileSync(SONG_ARCHIVE, 'utf8').split('\n'); } catch { return 0; }
    const keep = lines.filter((l) => !l.trim() || !l.split('\t').pop().startsWith(prefix));
    const dropped = lines.length - keep.length;
    if (dropped) {
      try { writeFileSync(SONG_ARCHIVE, keep.join('\n')); } catch { return 0; }
    }
    return dropped;
  }

  // The job's files are being thrown away without reaching the library, so its
  // archive entries have to go too — otherwise those tracks are unreachable forever.
  _discardJob(id) {
    const dropped = this._pruneArchive(id);
    if (dropped) console.log(`[${id}] dropped ${dropped} archive entries so a retry re-downloads them`);
    this._cleanupJobDir(id);
  }

  start(row) {
    const id = row.id;

    // Fail fast with a helpful message rather than letting zotify hang on a login prompt.
    if (!existsSync(CREDENTIALS_FILE)) {
      this.db.prepare(`
        UPDATE downloads SET status = 'failed', error = ?, updated_at = ?, pid = NULL WHERE id = ?
      `).run(
        'No credentials.json found — generate it once (see README) and mount it into /app/credentials.',
        Date.now(), id,
      );
      return;
    }

    const jobDir = this._jobDir(id);
    this._cleanupJobDir(id);
    mkdirSync(jobDir, { recursive: true });

    // Each job downloads into its own root so concurrent/overlapping jobs never
    // copy each other's half-written files to the NAS.
    const args = [
      '-c', ZOTIFY_CONFIG,
      '--root-path', jobDir,
      '--credentials-location', CREDENTIALS_FILE,
      '--output', OUTPUT_TEMPLATE,
      row.url,
    ];

    const proc = spawn('zotify', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    this.active.set(id, proc);
    this.busy.add(id); // released only once the job is fully filed (see close handler)
    this.db.prepare(`
      UPDATE downloads SET status = 'downloading', pid = ?, progress = 0, updated_at = ? WHERE id = ?
    `).run(proc.pid, Date.now(), id);

    let tail = '';        // rolling stderr/stdout tail for error reporting
    let lastBeat = Date.now();

    const handleLine = (line) => {
      tail = (tail + '\n' + line).slice(-4000);
      const now = Date.now();
      const c = line.match(TRACKS_RE);
      if (c) {
        const total = parseInt(c[2], 10);
        if (total > 0 && total < 2000) this.tracksTotal.set(id, total);
      }
      const p = line.match(PCT_RE);
      if (p) {
        const pct = Math.max(0, Math.min(100, parseInt(p[1], 10)));
        this.db.prepare(`UPDATE downloads SET progress = ?, updated_at = ? WHERE id = ?`)
          .run(pct, now, id);
        lastBeat = now;
      } else if (now - lastBeat > 5000) {
        // Heartbeat: real-time downloads emit output whose % we can't always parse.
        // Bump updated_at on any output so the stall-reaper only fires on true silence
        // (e.g. a process hung waiting on a prompt), not on a slow-but-live download.
        this.db.prepare(`UPDATE downloads SET updated_at = ? WHERE id = ?`).run(now, id);
        lastBeat = now;
      }
    };

    // tqdm uses \r as well as \n; split on both so progress updates flow.
    const wire = (stream) => {
      let buf = '';
      stream.on('data', (chunk) => {
        buf += chunk.toString();
        let m;
        while ((m = buf.search(/[\r\n]/)) >= 0) {
          const line = buf.slice(0, m);
          buf = buf.slice(m + 1);
          if (line.trim()) handleLine(line);
        }
      });
    };
    wire(proc.stdout);
    wire(proc.stderr);

    proc.on('close', async (code) => {
      // zotify is gone, but the job still owns its slot until it's filed into the
      // library — dropping `busy` here is what let the next album start early.
      this.active.delete(id);
      this.tracksTotal.delete(id);
      try {
        const current = this.db.prepare(`SELECT status FROM downloads WHERE id = ?`).get(id);
        if (current?.status === 'cancelled' || current?.status === 'stalled') {
          this._discardJob(id);
          return;
        }

        const produced = existsSync(jobDir) ? this._listFiles(jobDir) : [];
        if (code === 0 && produced.length) {
          try {
            const title = titleFromFiles(produced);
            const audio = produced.filter((f) => AUDIO_RE.test(f));
            // Size up now, before the files leave the staging dir.
            const bytes = audio.reduce((s, f) => {
              try { return s + statSync(f).size; } catch { return s; }
            }, 0);
            const info = audio[0]
              ? albumInfoFromRel(relative(jobDir, audio[0]))
              : { artist: null, album: null, year: null };
            const finalTracks = audio
              .map((f) => ({ ...parseTrackName(f), done: true }))
              .sort((a, b) => (a.num ?? 9999) - (b.num ?? 9999));
            await this.fileIntoLibrary(id, jobDir, info);
            this.db.prepare(`
              UPDATE downloads
              SET status = 'completed', progress = 100, file_path = ?, file_size = ?,
                  title = ?, album = ?, artist = ?, year = ?,
                  tracks_done = ?, tracks_total = ?, current_track = NULL, tracks_json = ?,
                  completed_at = ?, updated_at = ?, pid = NULL, eta = NULL, speed = NULL
              WHERE id = ?
            `).run(
              NAS_DIR, bytes, title, info.album, info.artist, info.year,
              audio.length, audio.length, JSON.stringify(finalTracks),
              Date.now(), Date.now(), id,
            );
          } catch (err) {
            this.db.prepare(`
              UPDATE downloads SET status = 'failed', error = ?, updated_at = ?, pid = NULL WHERE id = ?
            `).run(`move to NAS failed: ${err.message}`, Date.now(), id);
          }
        } else {
          const errMsg = tail.split('\n').filter(l => /error|invalid|premium|credential|failed/i.test(l))
            .slice(-4).join('\n').trim() || tail.slice(-500).trim() || `zotify exited ${code} with no files`;
          this.db.prepare(`
            UPDATE downloads SET status = 'failed', error = ?, updated_at = ?, pid = NULL WHERE id = ?
          `).run(errMsg, Date.now(), id);
          this._discardJob(id);
        }
      } finally {
        this.movers.delete(id);
        this.busy.delete(id);
        this.tick();
      }
    });

    proc.on('error', (err) => {
      this.active.delete(id);
      this.busy.delete(id);
      this.db.prepare(`
        UPDATE downloads SET status = 'failed', error = ?, updated_at = ?, pid = NULL WHERE id = ?
      `).run(`spawn error: ${err.message}`, Date.now(), id);
      this._discardJob(id);
      this.tick();
    });
  }

  // Scan every active job's staging dir and push live album/track metadata to the DB.
  scanActive() {
    for (const id of this.active.keys()) {
      try { this.scanJob(id); } catch {}
    }
  }

  scanJob(id) {
    const jobDir = this._jobDir(id);
    if (!existsSync(jobDir)) return;
    const files = this._listFiles(jobDir);
    const audio = files.filter((f) => AUDIO_RE.test(f));
    const tmp = files.filter((f) => /\.tmp$/i.test(f));
    const sample = audio[0] || tmp[0];
    const info = sample
      ? albumInfoFromRel(relative(jobDir, sample))
      : { artist: null, album: null, year: null };

    const doneTracks = audio.map((f) => ({ ...parseTrackName(f), done: true }));
    const curTracks = tmp.map((f) => ({ ...parseTrackName(f), done: false }));
    const all = [...doneTracks, ...curTracks]
      .sort((a, b) => (a.num ?? 9999) - (b.num ?? 9999));
    const total = this.tracksTotal.get(id) ?? null;

    this.db.prepare(`
      UPDATE downloads
      SET album = ?, artist = ?, year = ?, tracks_done = ?, tracks_total = ?,
          current_track = ?, tracks_json = ?
      WHERE id = ?
    `).run(
      info.album, info.artist, info.year, doneTracks.length, total,
      curTracks[0]?.name ?? null, JSON.stringify(all), id,
    );
  }

  // Recursively list all files under a directory (absolute paths).
  _listFiles(dir) {
    const out = [];
    for (const ent of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, ent.name);
      if (ent.isDirectory()) out.push(...this._listFiles(full));
      else if (ent.isFile()) out.push(full);
    }
    return out;
  }

  // Surface a problem on a job that still "succeeded". The UI renders `error`
  // whenever it's set, regardless of status, so this shows up without pretending
  // the tracks that did land were lost.
  _warn(id, msg) {
    console.log(`[${id}] ${msg}`);
    try {
      this.db.prepare(`UPDATE downloads SET error = ?, updated_at = ? WHERE id = ?`)
        .run(msg, Date.now(), id);
    } catch {}
  }

  // Ask Lidarr which release it monitors for this album, so beets can be pinned to it.
  // Best-effort by design: any miss (Lidarr down, album not in Lidarr, no monitored
  // release) returns null and beets picks for itself exactly as before.
  async _lidarrReleaseId(id, info) {
    if (!LIDARR_URL || !LIDARR_API_KEY || !info?.artist || !info?.album) return null;
    const norm = (s) => (s || '').toLowerCase().replace(/[^a-z0-9]+/g, '');
    try {
      const get = async (path) => {
        const r = await fetch(`${LIDARR_URL}${path}`, {
          headers: { 'X-Api-Key': LIDARR_API_KEY },
          signal: AbortSignal.timeout(LIDARR_TIMEOUT_MS),
        });
        if (!r.ok) throw new Error(`${path} -> HTTP ${r.status}`);
        return r.json();
      };
      const artist = (await get('/api/v1/artist'))
        .find((a) => norm(a.artistName) === norm(info.artist));
      if (!artist) return null;
      const albums = await get(`/api/v1/album?artistId=${artist.id}`);
      // Spotify and MusicBrainz disagree about subtitles — Spotify ships "Reroute To
      // Remain" where Lidarr has "Reroute to Remain: Fourteen Songs of Conscious
      // Insanity". Exact match first, then a prefix match, but only when it is
      // unambiguous: two candidates means we can't tell which, so let beets decide
      // rather than pin the wrong release.
      let album = albums.find((a) => norm(a.title) === norm(info.album));
      if (!album) {
        const want = norm(info.album);
        const near = albums.filter((a) => {
          const got = norm(a.title);
          return got.startsWith(want) || want.startsWith(got);
        });
        if (near.length === 1) album = near[0];
      }
      if (!album) return null;
      const rel = (album.releases || []).find((r) => r.monitored);
      if (!rel?.foreignReleaseId) return null;
      console.log(
        `[${id}] pinning beets to Lidarr's release ${rel.foreignReleaseId} ` +
        `(${rel.trackCount} tracks) for ${info.artist} - ${info.album}`,
      );
      return rel.foreignReleaseId;
    } catch (err) {
      console.log(`[${id}] Lidarr release lookup failed (${err.message}); letting beets choose`);
      return null;
    }
  }

  // Get the finished tracks into the library. With beets: MusicBrainz-tag + organize.
  // If beets is off or fails, fall back to a plain structure-preserving copy so
  // files are never stranded.
  async fileIntoLibrary(id, jobDir, info = {}) {
    // Clear pid: zotify has exited, and a dead pid left in the row would make the
    // stall-reaper SIGKILL whatever process the kernel has since recycled it onto.
    this.db.prepare(`UPDATE downloads SET status = 'moving', pid = NULL, updated_at = ? WHERE id = ?`)
      .run(Date.now(), id);
    if (USE_BEETS) {
      try {
        // Snapshot the staging library first: a previous run that crashed mid-flush
        // leaves files here, and counting those as "this job imported something"
        // would misread a total failure as a partial one.
        const before = new Set(existsSync(LIB_STAGE) ? this._listFiles(LIB_STAGE) : []);
        await this.beetsImport(id, jobDir, await this._lidarrReleaseId(id, info));
        // beets exits 0 even when it imported NOTHING — e.g. it skips any directory
        // its `ignore`/`ignore_hidden` rules match, which is what silently ate
        // "...Like Clockwork" (leading dots read as a hidden dir). So never take
        // exit 0 as proof the audio moved: only drop the staging dir once it's
        // actually empty of audio, and hand anything left over to the raw copy.
        const leftover = existsSync(jobDir)
          ? this._listFiles(jobDir).filter((f) => AUDIO_RE.test(f))
          : [];
        const staged = (existsSync(LIB_STAGE) ? this._listFiles(LIB_STAGE) : [])
          .filter((f) => AUDIO_RE.test(f) && !before.has(f));
        // PARTIAL import: beets tagged some tracks and left others behind, which is
        // what `max_rec.unmatched_tracks: strong` allows — it auto-accepts a release
        // with fewer tracks than we downloaded, orphaning the extras. Raw-copying
        // those extras drops files carrying nothing but zotify's bare Spotify tags
        // next to properly tagged ones, and that alone splits the album in Navidrome
        // (it's where "13. World of Promises.ogg" came from). Ship only what beets
        // tagged and leave the rest in the job dir for a human.
        if (leftover.length && staged.length) {
          this._warn(id,
            `beets matched a release missing ${leftover.length} of ` +
            `${leftover.length + staged.length} downloaded track(s): ` +
            `${leftover.map((f) => basename(f)).join(', ')}. ` +
            `Tagged tracks were filed; the unmatched ones were NOT copied to the ` +
            `library (they would split the album). They are in ${this._jobDir(id)}`);
          await this._flushLibStage(id);
          return;
        }
        if (leftover.length) {
          console.log(`[${id}] beets imported nothing (${leftover.length} track(s) still staged); copying as-is`);
          await this.moveToNas(id, jobDir);
        } else {
          this._cleanupJobDir(id); // beets took the audio; drop leftover .lrc/.log/.song_ids
        }
        await this._flushLibStage(id);
        return;
      } catch (err) {
        console.log(`[${id}] beets import failed (${err.message}); falling back to raw copy`);
        await this._flushLibStage(id); // it may have organised some tracks already
      }
    }
    await this.moveToNas(id, jobDir);
  }

  // Ship whatever beets organised into the local staging library over to the NAS in
  // one sequential copy. Self-healing: anything a crashed run left behind is picked
  // up by the next job rather than being stranded.
  async _flushLibStage(id) {
    if (!existsSync(LIB_STAGE)) return;
    const files = this._listFiles(LIB_STAGE);
    if (!files.length) {
      try { rmSync(LIB_STAGE, { recursive: true, force: true }); } catch {}
      return;
    }
    console.log(`[${id}] shipping ${files.length} file(s) from the local library to the NAS`);
    await this.moveToNas(id, LIB_STAGE);
  }

  // Run `beet import` over the staging dir. beets moves audio into `directory`
  // (the library) per its `paths`, writing corrected tags. Non-interactive.
  // `searchId` (when Lidarr supplied one) is passed as `-S/--search-id`, which makes
  // that MusicBrainz release the candidate beets scores first. It's a strong hint, not
  // a hard override: if the audio genuinely doesn't fit, beets still falls back to its
  // normal search rather than force-applying a wrong release.
  beetsImport(id, jobDir, searchId = null) {
    return new Promise((resolve, reject) => {
      const args = ['-c', BEETS_CONFIG, 'import', '-q'];
      if (searchId) args.push('-S', searchId);
      args.push(jobDir);
      const p = spawn('beet', args, { stdio: ['ignore', 'pipe', 'pipe'] });
      this.movers.set(id, p);
      let err = '';
      const beat = setInterval(() => {
        // keep the row fresh so the stall-reaper doesn't fire during a long MB lookup
        this.db.prepare(`UPDATE downloads SET updated_at = ? WHERE id = ? AND status = 'moving'`)
          .run(Date.now(), id);
      }, 10_000);
      p.stderr.on('data', (d) => { err = (err + d.toString()).slice(-1000); });
      p.stdout.on('data', () => {});
      p.on('close', (code) => {
        clearInterval(beat);
        this.movers.delete(id);
        code === 0 ? resolve() : reject(new Error(`beet exit ${code}: ${err.trim()}`));
      });
      p.on('error', (e) => { clearInterval(beat); this.movers.delete(id); reject(e); });
    });
  }

  // Copy a tree into NAS_DIR preserving album-folder structure, then drop the source.
  // Copy-then-unlink because CIFS can't rename across the mount boundary. Used both
  // for the raw fallback (source = the job dir) and to ship beets' organised output
  // (source = the local staging library).
  async moveToNas(id, srcDir) {
    // Clear pid: zotify has exited, and a dead pid left in the row would make the
    // stall-reaper SIGKILL whatever process the kernel has since recycled it onto.
    this.db.prepare(`UPDATE downloads SET status = 'moving', pid = NULL, updated_at = ? WHERE id = ?`)
      .run(Date.now(), id);
    const files = this._listFiles(srcDir);
    let bytes = 0;
    for (const src of files) {
      const rel = relative(srcDir, src);
      const dest = join(NAS_DIR, rel);
      mkdirSync(dirname(dest), { recursive: true });
      await pipeline(createReadStream(src), createWriteStream(dest));
      bytes += statSync(dest).size;
    }
    // Drop the source we were actually given, not the job dir — otherwise the
    // staging library would survive and be re-shipped on every subsequent job.
    try { rmSync(srcDir, { recursive: true, force: true }); } catch {}
    return { bytes, count: files.length };
  }

  reapStalled() {
    const cutoff = Date.now() - STALL_TIMEOUT_MS;
    const stalled = this.db.prepare(`
      SELECT id, pid FROM downloads
      WHERE status IN ('downloading','moving') AND updated_at < ?
    `).all(cutoff);
    for (const row of stalled) {
      const proc = this.active.get(row.id) || this.movers.get(row.id);
      if (proc) { try { proc.kill('SIGKILL'); } catch {} }
      else if (row.pid) { try { process.kill(row.pid, 'SIGKILL'); } catch {} }
      this.active.delete(row.id);
      this.movers.delete(row.id);
      this.busy.delete(row.id);
      this._discardJob(row.id);
      this.db.prepare(`
        UPDATE downloads SET status = 'stalled', error = 'no progress (possible auth prompt or rate-limit)', updated_at = ?, pid = NULL WHERE id = ?
      `).run(Date.now(), row.id);
    }
    if (stalled.length) this.tick();
    return stalled.length;
  }
}
