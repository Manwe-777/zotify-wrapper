import { Router } from 'express';

const router = Router();

function isValidUrl(s) {
  try { new URL(s); return true; } catch { return false; }
}

router.get('/downloads', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
  const rows = req.db.prepare(`
    SELECT id, url, title, status, progress, eta, speed, file_path, file_size,
           error, created_at, updated_at, completed_at,
           album, artist, year, tracks_total, tracks_done, current_track, tracks_json
    FROM downloads
    ORDER BY
      CASE status
        WHEN 'downloading' THEN 0
        WHEN 'moving' THEN 0
        WHEN 'queued' THEN 1
        WHEN 'failed' THEN 2
        WHEN 'stalled' THEN 2
        WHEN 'completed' THEN 3
        ELSE 4
      END,
      -- Queued rows list in the order they'll actually run (FIFO); everything else
      -- lists newest-first. NULL for non-queued rows, so it's a no-op there.
      CASE WHEN status = 'queued' THEN created_at END ASC,
      created_at DESC
    LIMIT ?
  `).all(limit);
  res.json({ downloads: rows });
});

router.post('/downloads', (req, res) => {
  const { url } = req.body || {};
  if (!url || !isValidUrl(url)) {
    return res.status(400).json({ error: 'valid url required' });
  }
  const id = req.downloader.enqueue(url.trim());
  res.json({ id });
});

router.post('/downloads/:id/cancel', (req, res) => {
  const ok = req.downloader.cancel(parseInt(req.params.id, 10));
  res.json({ ok });
});

router.post('/downloads/:id/retry', (req, res) => {
  req.downloader.retry(parseInt(req.params.id, 10));
  res.json({ ok: true });
});

router.delete('/downloads/:id', (req, res) => {
  req.downloader.delete(parseInt(req.params.id, 10));
  res.json({ ok: true });
});

router.post('/downloads/cleanup', (req, res) => {
  // Remove finished entries (completed/failed/cancelled/stalled).
  const info = req.db.prepare(`
    DELETE FROM downloads WHERE status IN ('completed','failed','cancelled','stalled')
  `).run();
  res.json({ removed: info.changes });
});

export default router;
