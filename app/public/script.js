const $ = (id) => document.getElementById(id);
const form = $('add-form');
const input = $('url-input');
const addBtn = $('add-btn');
const list = $('downloads');
const empty = $('empty');
const statusBar = $('status-bar');

// Track rows the user has expanded, preserved across the 2s auto-refresh.
const expanded = new Set();

const STATUS_LABEL = {
  queued: 'queued',
  downloading: 'downloading',
  moving: 'saving',
  completed: 'done',
  failed: 'failed',
  stalled: 'stalled',
  cancelled: 'cancelled',
};

function fmtSize(bytes) {
  if (!bytes) return '';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0, n = bytes;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(n < 10 ? 1 : 0)} ${u[i]}`;
}

function escape(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function safeTracks(json) {
  try { const t = JSON.parse(json || '[]'); return Array.isArray(t) ? t : []; }
  catch { return []; }
}

function trackList(tracks) {
  const items = tracks.map((t) => {
    const num = t.num != null ? String(t.num).padStart(2, '0') + '. ' : '';
    const icon = t.done ? '✓' : '↻';
    const cls = t.done ? 'done' : 'active';
    return `<li class="tk ${cls}"><span class="tk-ic">${icon}</span><span class="tk-nm">${escape(num + t.name)}</span></li>`;
  }).join('');
  return `<ul class="tracklist">${items}</ul>`;
}

function row(d) {
  const tracks = safeTracks(d.tracks_json);
  const hasTracks = tracks.length > 0;
  const isOpen = expanded.has(d.id);
  const mainTitle = d.album || d.title || d.url;
  const sub = [d.artist, d.year].filter(Boolean).join(' · ');

  const total = d.tracks_total || 0;
  const done = d.tracks_done || 0;
  const active = ['downloading', 'moving', 'queued'].includes(d.status);

  let pct = 0;
  if (total) pct = Math.min(100, Math.round((done / total) * 100));
  else if (d.status === 'completed') pct = 100;
  else pct = Math.max(0, Math.min(100, d.progress || 0));

  let meta = '';
  if (d.status === 'downloading') {
    meta = total ? `${done} of ${total} tracks` : (done ? `${done} downloaded` : 'starting…');
    if (d.current_track) meta += ` · ↓ ${escape(d.current_track)}`;
  } else if (d.status === 'moving') {
    meta = 'saving to NAS…';
  } else if (d.status === 'queued') {
    meta = 'queued';
  } else if (d.status === 'completed') {
    const n = done || tracks.length || 1;
    meta = `${n} track${n > 1 ? 's' : ''}`;
    if (d.file_size) meta += ` · ${fmtSize(d.file_size)}`;
  }

  const actions = [];
  if (active) actions.push(`<button class="icon-btn" data-act="cancel" data-id="${d.id}" title="Cancel">×</button>`);
  if (['failed', 'cancelled', 'stalled'].includes(d.status)) actions.push(`<button class="icon-btn" data-act="retry" data-id="${d.id}" title="Retry">↻</button>`);
  actions.push(`<button class="icon-btn danger" data-act="delete" data-id="${d.id}" title="Delete">🗑</button>`);

  const chev = hasTracks
    ? `<span class="chev ${isOpen ? 'open' : ''}">▸</span>`
    : `<span class="chev empty"></span>`;

  return `
    <div class="dl-row ${isOpen ? 'expanded' : ''}" data-status="${d.status}">
      <div class="dl-head" data-toggle="${d.id}" title="${escape(d.url)}">
        ${chev}
        <div class="dl-titles">
          <div class="dl-title">${escape(mainTitle)}</div>
          ${sub ? `<div class="dl-sub">${escape(sub)}</div>` : ''}
        </div>
        <div class="dl-actions">${actions.join('')}</div>
      </div>
      <div class="dl-meta">
        <span class="dl-status ${d.status}">${STATUS_LABEL[d.status] || d.status}</span>
        ${active ? `<div class="dl-bar"><div class="dl-bar-fill" style="width:${pct}%"></div></div>` : ''}
        <span class="dl-metatext">${meta}</span>
      </div>
      ${d.error ? `<div class="dl-error" title="${escape(d.error)}">${escape(d.error)}</div>` : ''}
      ${hasTracks && isOpen ? trackList(tracks) : ''}
    </div>
  `;
}

async function load() {
  try {
    const r = await fetch('/api/downloads?limit=50');
    const { downloads } = await r.json();
    if (!downloads.length) {
      list.innerHTML = '';
      empty.classList.remove('hidden');
    } else {
      empty.classList.add('hidden');
      list.innerHTML = downloads.map(row).join('');
    }
    const active = downloads.filter((d) => d.status === 'downloading' || d.status === 'moving').length;
    const queued = downloads.filter((d) => d.status === 'queued').length;
    statusBar.textContent = active || queued
      ? `${active} active${queued ? `, ${queued} queued` : ''}`
      : '';
  } catch (err) {
    statusBar.textContent = 'connection error';
  }
}

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const url = input.value.trim();
  if (!url) return;
  addBtn.disabled = true;
  try {
    const r = await fetch('/api/downloads', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }),
    });
    if (!r.ok) {
      const { error } = await r.json().catch(() => ({}));
      statusBar.textContent = error || 'failed to enqueue';
    } else {
      input.value = '';
      load();
    }
  } finally {
    addBtn.disabled = false;
  }
});

list.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-act]');
  if (btn) {
    const id = btn.dataset.id;
    const act = btn.dataset.act;
    const map = {
      cancel: { method: 'POST', url: `/api/downloads/${id}/cancel` },
      retry: { method: 'POST', url: `/api/downloads/${id}/retry` },
      delete: { method: 'DELETE', url: `/api/downloads/${id}` },
    };
    const cfg = map[act];
    if (!cfg) return;
    if (act === 'delete' && !confirm('Delete this entry?')) return;
    await fetch(cfg.url, { method: cfg.method });
    load();
    return;
  }
  // Toggle expand/collapse when clicking the row header (but not a button).
  const head = e.target.closest('.dl-head');
  if (head) {
    const id = parseInt(head.dataset.toggle, 10);
    if (expanded.has(id)) expanded.delete(id); else expanded.add(id);
    load();
  }
});

$('cleanup').addEventListener('click', async () => {
  await fetch('/api/downloads/cleanup', { method: 'POST' });
  load();
});

load();
setInterval(load, 2000);
