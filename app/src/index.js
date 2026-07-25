import express from 'express';
import cors from 'cors';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { initDb } from './db/index.js';
import { Downloader } from './downloader.js';
import downloadsRoutes from './routes/downloads.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

const app = express();
const PORT = process.env.PORT || 3106;

app.use(cors());
app.use(express.json({ limit: '1mb' }));

app.use((req, _res, next) => {
  if (req.url.startsWith('/api')) console.log(`${req.method} ${req.url}`);
  next();
});

const db = initDb(process.env.DB_PATH || './data/downloads.db');
const downloader = new Downloader(db);

app.use((req, _res, next) => {
  req.db = db;
  req.downloader = downloader;
  next();
});

app.use('/api', downloadsRoutes);
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.use(express.static(join(__dirname, '..', 'public')));

setInterval(() => downloader.reapStalled(), 30_000).unref();
setInterval(() => downloader.tick(), 5_000).unref();
setInterval(() => downloader.scanActive(), 2_000).unref();

app.listen(PORT, () => console.log(`zotify-wrapper on :${PORT}`));
