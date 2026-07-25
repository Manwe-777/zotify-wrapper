import Database from 'better-sqlite3';
import { mkdirSync } from 'fs';
import { dirname } from 'path';

export function initDb(dbPath) {
  mkdirSync(dirname(dbPath), { recursive: true });

  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');

  db.exec(`
    CREATE TABLE IF NOT EXISTS downloads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      url TEXT NOT NULL,
      title TEXT,
      status TEXT NOT NULL DEFAULT 'queued',
      progress REAL DEFAULT 0,
      eta TEXT,
      speed TEXT,
      file_path TEXT,
      file_size INTEGER,
      error TEXT,
      pid INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      completed_at INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
    CREATE INDEX IF NOT EXISTS idx_downloads_created ON downloads(created_at DESC);
  `);

  // Migrations: add richer-metadata columns to existing DBs (idempotent).
  const cols = new Set(db.prepare(`PRAGMA table_info(downloads)`).all().map((c) => c.name));
  const addColumn = (name, decl) => {
    if (!cols.has(name)) db.exec(`ALTER TABLE downloads ADD COLUMN ${name} ${decl}`);
  };
  addColumn('album', 'TEXT');
  addColumn('artist', 'TEXT');
  addColumn('year', 'TEXT');
  addColumn('tracks_total', 'INTEGER');
  addColumn('tracks_done', 'INTEGER DEFAULT 0');
  addColumn('current_track', 'TEXT');
  addColumn('tracks_json', 'TEXT'); // JSON array of {num, name, done}

  return db;
}
