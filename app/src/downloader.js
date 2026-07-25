import { spawn } from 'child_process';
import {
  mkdirSync, existsSync, statSync, rmSync, readdirSync,
  createReadStream, createWriteStream,
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
    this.active = new Map();
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
    this.db.prepare(`
      UPDATE downloads SET status = 'cancelled', updated_at = ?, pid = NULL WHERE id = ?
    `).run(Date.now(), id);
    this.active.delete(id);
    this._cleanupJobDir(id);
    this.tick();
    return true;
  }

  retry(id) {
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
    if (this.active.size >= MAX_CONCURRENT) return;
    const next = this.db.prepare(`
      SELECT * FROM downloads WHERE status = 'queued'
      ORDER BY created_at ASC LIMIT 1
    `).get();
    if (!next) return;
    this.start(next);
    if (this.active.size < MAX_CONCURRENT) this.tick();
  }

  _jobDir(id) { return join(WORK_DIR, `job-${id}`); }

  _cleanupJobDir(id) {
    try { rmSync(this._jobDir(id), { recursive: true, force: true }); } catch {}
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
      this.active.delete(id);
      this.tracksTotal.delete(id);
      const current = this.db.prepare(`SELECT status FROM downloads WHERE id = ?`).get(id);
      if (current?.status === 'cancelled' || current?.status === 'stalled') {
        this._cleanupJobDir(id);
        this.tick();
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
          await this.fileIntoLibrary(id, jobDir);
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
        this._cleanupJobDir(id);
      }
      this.tick();
    });

    proc.on('error', (err) => {
      this.active.delete(id);
      this.db.prepare(`
        UPDATE downloads SET status = 'failed', error = ?, updated_at = ?, pid = NULL WHERE id = ?
      `).run(`spawn error: ${err.message}`, Date.now(), id);
      this._cleanupJobDir(id);
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

  // Get the finished tracks into the library. With beets: MusicBrainz-tag + organize.
  // If beets is off or fails, fall back to a plain structure-preserving copy so
  // files are never stranded.
  async fileIntoLibrary(id, jobDir) {
    this.db.prepare(`UPDATE downloads SET status = 'moving', updated_at = ? WHERE id = ?`)
      .run(Date.now(), id);
    if (USE_BEETS) {
      try {
        await this.beetsImport(id, jobDir);
        this._cleanupJobDir(id); // beets moved the audio; drop leftover .lrc/.log/.song_ids
        return;
      } catch (err) {
        console.log(`[${id}] beets import failed (${err.message}); falling back to raw copy`);
      }
    }
    await this.moveToNas(id, jobDir);
  }

  // Run `beet import` over the staging dir. beets moves audio into `directory`
  // (the library) per its `paths`, writing corrected tags. Non-interactive.
  beetsImport(id, jobDir) {
    return new Promise((resolve, reject) => {
      const args = ['-c', BEETS_CONFIG, 'import', '-q', jobDir];
      const p = spawn('beet', args, { stdio: ['ignore', 'pipe', 'pipe'] });
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
        code === 0 ? resolve() : reject(new Error(`beet exit ${code}: ${err.trim()}`));
      });
      p.on('error', (e) => { clearInterval(beat); reject(e); });
    });
  }

  // Copy the job tree into NAS_DIR preserving album-folder structure, then drop the local copy.
  // Copy-then-unlink because CIFS can't rename across the mount boundary.
  async moveToNas(id, jobDir) {
    this.db.prepare(`UPDATE downloads SET status = 'moving', updated_at = ? WHERE id = ?`)
      .run(Date.now(), id);
    const files = this._listFiles(jobDir);
    let bytes = 0;
    for (const src of files) {
      const rel = relative(jobDir, src);
      const dest = join(NAS_DIR, rel);
      mkdirSync(dirname(dest), { recursive: true });
      await pipeline(createReadStream(src), createWriteStream(dest));
      bytes += statSync(dest).size;
    }
    this._cleanupJobDir(id);
    return { bytes, count: files.length };
  }

  reapStalled() {
    const cutoff = Date.now() - STALL_TIMEOUT_MS;
    const stalled = this.db.prepare(`
      SELECT id, pid FROM downloads
      WHERE status IN ('downloading','moving') AND updated_at < ?
    `).all(cutoff);
    for (const row of stalled) {
      const proc = this.active.get(row.id);
      if (proc) { try { proc.kill('SIGKILL'); } catch {} }
      else if (row.pid) { try { process.kill(row.pid, 'SIGKILL'); } catch {} }
      this.active.delete(row.id);
      this._cleanupJobDir(row.id);
      this.db.prepare(`
        UPDATE downloads SET status = 'stalled', error = 'no progress (possible auth prompt or rate-limit)', updated_at = ?, pid = NULL WHERE id = ?
      `).run(Date.now(), row.id);
    }
    if (stalled.length) this.tick();
    return stalled.length;
  }
}
