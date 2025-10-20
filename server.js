// server.js
// Video service – astats & cut
// Ejecuta con: node server.js
// Requiere: FFmpeg instalado en la imagen, Node 18+

import express from 'express';
import morgan from 'morgan';
import bodyParser from 'body-parser';
import { exec as execCb } from 'child_process';
import util from 'util';
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import http from 'http';
import https from 'https';

// Supabase (opcional para subir clips)
import pkg from '@supabase/supabase-js';
const { createClient } = pkg;

const exec = util.promisify(execCb);

const app = express();
app.use(bodyParser.json({ limit: '5mb' }));
app.use(morgan('tiny'));

const PORT = process.env.PORT || 8080;
const NODE_ENV = process.env.NODE_ENV || 'production';

// ======== Configuración Supabase opcional (para subir cortes) ========
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const STORAGE_BUCKET = process.env.STORAGE_BUCKET || 'clips';
let supabase = null;
if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
}

// ======== Utilidades ========

function tmpPath(ext = '') {
  const name = `tmp-${Date.now()}-${crypto.randomBytes(6).toString('hex')}${ext}`;
  return path.join(os.tmpdir(), name);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function clamp(n, a, b) {
  return Math.max(a, Math.min(b, n));
}

function quantile(arr, q) {
  if (!arr.length) return 0;
  const a = [...arr].sort((x, y) => x - y);
  const pos = clamp((a.length - 1) * q, 0, a.length - 1);
  const base = Math.floor(pos);
  const rest = pos - base;
  return a[base] + (a[base + 1] - a[base]) * rest || a[base];
}

// Descarga a /tmp cuando haga falta (normalmente FFmpeg puede leer HTTPS directo)
async function downloadToTmp(url, ext = '') {
  const file = tmpPath(ext);
  const proto = url.startsWith('https') ? https : http;
  await new Promise((resolve, reject) => {
    const req = proto.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 400) {
        reject(new Error(`HTTP ${res.statusCode} al descargar ${url}`));
        return;
      }
      const stream = fs.createWriteStream(file);
      res.pipe(stream);
      stream.on('finish', () => stream.close(resolve));
      stream.on('error', reject);
    });
    req.on('error', reject);
  });
  return file;
}

// Ejecuta FFmpeg y devuelve stdout/stderr truncados para logging controlado
async function run(cmd) {
  try {
    const { stdout, stderr } = await exec(cmd, { maxBuffer: 10 * 1024 * 1024 });
    return { ok: true, stdout, stderr, cmd };
  } catch (err) {
    const e = err;
    return {
      ok: false,
      stdout: e.stdout || '',
      stderr: e.stderr || (e.message || ''),
      cmd,
      error: { name: e.name, message: e.message, stack: e.stack },
    };
  }
}

// Parsea líneas de ametadata (RMS overall)
function parseRMS(metaText) {
  // Busca: lavfi.astats.Overall.RMS_level=-18.23
  const re = /lavfi\.astats\.Overall\.RMS_level=([-\d.]+)/g;
  const pts = [];
  let m;
  while ((m = re.exec(metaText)) !== null) {
    const v = Number(m[1]);
    if (Number.isFinite(v)) pts.push(v);
  }
  return pts;
}

// A partir de una secuencia de RMS y umbral, genera rangos [start,end] donde RMS >= TH
function rangesFromRMS(pts, fps, TH, pad = 0.8, mergeGap = 0.4, maxT = 1e9) {
  // pts mide cada frame/step; derivamos paso temporal aproximado.
  // Como usamos ametadata por frame de audio procesado, no hay fps exacto, pero
  // aproximamos a 100Hz si no podemos estimar. Aquí mejor derivar por longitud & maxT.
  const dt = clamp(maxT / Math.max(pts.length, 1), 0.005, 0.05); // 20–200 Hz
  const intervals = [];
  let curStart = null;

  for (let i = 0; i < pts.length; i++) {
    const t = i * dt;
    const voiced = pts[i] >= TH;
    if (voiced && curStart == null) curStart = t;
    if (!voiced && curStart != null) {
      intervals.push({ start: curStart, end: t });
      curStart = null;
    }
  }
  if (curStart != null) intervals.push({ start: curStart, end: Math.min(maxT, pts.length * dt) });

  // pad & merge
  const padded = intervals.map(({ start, end }) => ({
    start: clamp(start - pad, 0, maxT),
    end: clamp(end + pad, 0, maxT),
  }));
  if (!padded.length) return [];

  padded.sort((a, b) => a.start - b.start);
  const merged = [padded[0]];
  for (let i = 1; i < padded.length; i++) {
    const prev = merged[merged.length - 1];
    const cur = padded[i];
    if (cur.start - prev.end <= mergeGap) {
      prev.end = Math.max(prev.end, cur.end);
    } else {
      merged.push(cur);
    }
  }
  return merged;
}

// ======== Endpoints ========

app.get('/echo', (req, res) => {
  res.json({
    ok: true,
    where: 'echo',
    method: req.method,
    headers: req.headers,
    query: req.query,
    body: req.body,
    time: new Date().toISOString(),
  });
});

/**
 * POST /astats
 * body: {
 *   video_url: string (https firmado OK),
 *   max_seconds?: number (default 1200, máx 21600),
 *   base_db?: number (default -35),
 *   percentile?: number (0..1, default 0.6) — percentil de RMS como umbral
 *   pad?: number (s, default 1.2), merge_gap?: number (s, default 0.8)
 * }
 */
app.post('/astats', async (req, res) => {
  const t0 = Date.now();
  const reqId = crypto.randomBytes(4).toString('hex');
  const { video_url, max_seconds, base_db, percentile, pad, merge_gap } = req.body || {};
  const MAX_T = clamp(Number(max_seconds) || 1200, 1, 21600);
  const TH = Number(base_db) ?? -35;
  const PCTL = clamp(Number(percentile) || 0.6, 0.01, 0.99);
  const PAD = Number.isFinite(pad) ? Number(pad) : 1.2;
  const MERGE_GAP = Number.isFinite(merge_gap) ? Number(merge_gap) : 0.8;

  const reqInfo = { where: 'astats', reqId };

  if (!video_url || typeof video_url !== 'string') {
    return res.status(400).json({
      ok: false, ...reqInfo,
      error: { name: 'BadRequest', message: 'video_url es requerido' },
    });
  }

  // Usamos la URL directa (FFmpeg lee HTTPS). Añadimos rw_timeout para robustez.
  const filter =
    'aresample=16000:resampler=soxr:precision=16,' +
    'aformat=channel_layouts=mono,' +
    'astats=metadata=1:reset=1,' +
    'ametadata=print:key=lavfi.astats.Overall.RMS_level';

  const cmd =
    `ffmpeg -hide_banner -loglevel warning -y -t ${MAX_T} -rw_timeout 30000000 ` + // ~30s read timeout
    `-i "${video_url}" -vn -af "${filter}" -f null -`;

  const r = await run(cmd);

  // Parseo RMS
  const pts = parseRMS((r.stdout || '') + '\n' + (r.stderr || ''));
  let stderrExcerpt = (r.stderr || '').slice(0, 1500);

  if (!pts.length) {
    // Fallback a silencedetect
    const sdFilter =
      `aresample=16000:resampler=soxr:precision=16,` +
      `aformat=channel_layouts=mono,` +
      `silencedetect=noise=${TH}dB:d=0.2`;

    const cmd2 =
      `ffmpeg -hide_banner -loglevel warning -y -t ${MAX_T} -rw_timeout 30000000 ` +
      `-i "${video_url}" -vn -af "${sdFilter}" -f null -`;

    const r2 = await run(cmd2);
    const text = (r2.stdout || '') + '\n' + (r2.stderr || '');
    const noise = [];
    const re = /silence_(start|end):\s*([-\d.]+)/g;
    let m;
    while ((m = re.exec(text)) !== null) noise.push({ k: m[1], v: parseFloat(m[2]) });

    let last = 0;
    const ranges = [];
    for (let i = 0; i < noise.length; i++) {
      if (noise[i].k === 'start') {
        const end = clamp(noise[i].v, 0, MAX_T);
        if (end > last) ranges.push({ start: last, end });
      } else if (noise[i].k === 'end') {
        last = clamp(noise[i].v, 0, MAX_T);
      }
    }
    if (last < MAX_T) ranges.push({ start: last, end: MAX_T });

    return res.json({
      ok: true,
      ...reqInfo,
      threshold: Number(TH),
      ranges,
      limited: true,
      points: 0,
      method: 'silencedetect_fallback',
      stderr_excerpt: (r2.stderr || '').slice(0, 1500),
      took_ms: Date.now() - t0,
      mode: 'http',
    });
  }

  // Umbral por percentil sobre RMS
  const thAuto = quantile(pts, PCTL);
  const THRESH = Number.isFinite(TH) ? Math.max(TH, thAuto) : thAuto;

  const ranges = rangesFromRMS(pts, 100, THRESH, PAD, MERGE_GAP, MAX_T);

  return res.json({
    ok: true,
    ...reqInfo,
    threshold: Number(THRESH),
    ranges,
    limited: true,
    points: pts.length,
    method: 'astats_ametadata',
    stderr_excerpt: stderrExcerpt,
    took_ms: Date.now() - t0,
    mode: 'http',
  });
});

/**
 * POST /cut
 * body: {
 *   video_url: string,
 *   start_time: number (s),
 *   end_time: number (s),
 *   filters?: { format?: 'original', loudnorm?: boolean },
 *   output?: {
 *     container?: 'mp4'|'mov'|'mkv', video_codec?: string, audio_codec?: string,
 *     crf?: number, preset?: string, faststart?: boolean
 *   },
 *   upload?: { // opcional – si hay Supabase configurado se sube
 *     bucket?: string, pathPrefix?: string
 *   }
 * }
 */
app.post('/cut', async (req, res) => {
  const t0 = Date.now();
  const reqId = crypto.randomBytes(4).toString('hex');
  const reqInfo = { where: 'cut', reqId };

  const {
    video_url,
    start_time,
    end_time,
    filters = {},
    output = {},
    upload = {},
  } = req.body || {};

  if (!video_url || !Number.isFinite(Number(start_time)) || !Number.isFinite(Number(end_time))) {
    return res.status(400).json({
      ok: false,
      ...reqInfo,
      error: {
        name: 'BadRequest',
        message: 'video_url, start_time y end_time son requeridos',
      },
    });
  }

  const ss = Number(start_time);
  const to = Number(end_time);
  if (to <= ss) {
    return res.status(400).json({
      ok: false,
      ...reqInfo,
      error: { name: 'BadRequest', message: 'end_time debe ser mayor a start_time' },
    });
  }

  const dur = clamp(to - ss, 0.1, 21600);
  const container = output.container || 'mp4';
  const vcodec = output.video_codec || 'libx264';
  const acodec = output.audio_codec || 'aac';
  const crf = Number.isFinite(output.crf) ? output.crf : 23;
  const preset = output.preset || 'veryfast';
  const faststart = output.faststart !== false;

  // Cadena de filtros para salida (solo loudnorm si se pide)
  const af = [];
  if (filters.loudnorm) af.push('loudnorm=I=-16:TP=-1.5:LRA=11');

  const outFile = tmpPath(`.${container}`);
  const args = [
    '-hide_banner', '-loglevel', 'warning',
    '-y',
    // Corte preciso (re-encode): -ss DESPUÉS de -i para mayor precisión
    '-i', video_url,
    '-ss', ss.toString(),
    '-t', dur.toString(),
    '-map', '0',
    '-c:v', vcodec,
    '-preset', preset,
    '-crf', crf.toString(),
    '-c:a', acodec,
  ];

  if (af.length) {
    args.push('-af', af.join(','));
  }

  if (faststart && container === 'mp4') {
    args.push('-movflags', '+faststart');
  }

  args.push(outFile);

  const cmd = `ffmpeg ${args.map((a) => (a.includes(' ') ? `"${a}"` : a)).join(' ')}`;

  const r = await run(cmd);
  if (!r.ok || !fs.existsSync(outFile)) {
    return res.status(500).json({
      ok: false,
      ...reqInfo,
      error: r.error || { name: 'FFmpegError', message: 'ffmpeg falló' },
      stderr: (r.stderr || '').slice(0, 2000),
      cmd: r.cmd,
      took_ms: Date.now() - t0,
    });
  }

  // Si no hay Supabase configurado, devolvemos el archivo en base64 (o indicamos path temporal)
  if (!supabase) {
    const stats = fs.statSync(outFile);
    const base64 = fs.readFileSync(outFile).toString('base64');
    fs.unlink(outFile, () => {});
    return res.json({
      ok: true,
      ...reqInfo,
      size_bytes: stats.size,
      mime: container === 'mp4' ? 'video/mp4' : 'application/octet-stream',
      data_base64: base64,
      took_ms: Date.now() - t0,
    });
  }

  // Subida a Supabase
  const bucket = (upload && upload.bucket) || STORAGE_BUCKET || 'clips';
  const prefix = (upload && upload.pathPrefix) || 'cuts';
  const fileName = `${Date.now()}_${crypto.randomBytes(4).toString('hex')}.${container}`;
  const storagePath = `${prefix}/${fileName}`;

  const fileBuf = fs.readFileSync(outFile);
  fs.unlink(outFile, () => {});

  const { data, error } = await supabase
    .storage
    .from(bucket)
    .upload(storagePath, fileBuf, {
      contentType: container === 'mp4' ? 'video/mp4' : 'application/octet-stream',
      upsert: false,
    });

  if (error) {
    return res.status(500).json({
      ok: false,
      ...reqInfo,
      error: { name: 'StorageApiError', message: error.message },
      took_ms: Date.now() - t0,
    });
  }

  // Genera URL pública o firmada según tus reglas
  let publicUrl = null;
  try {
    const { data: pub } = supabase.storage.from(bucket).getPublicUrl(storagePath);
    publicUrl = pub?.publicUrl || null;
  } catch {
    // ignora
  }

  return res.json({
    ok: true,
    ...reqInfo,
    bucket,
    path: storagePath,
    public_url: publicUrl,
    took_ms: Date.now() - t0,
  });
});

// Salud
app.get('/healthz', (_req, res) => res.json({ ok: true, where: 'healthz', now: new Date().toISOString() }));

// Arranque
const server = app.listen(PORT, () => {
  console.log(`[video-svc] listening on :${PORT} (${NODE_ENV})`);
});

// Shutdown gracioso en Railway
const shutdown = async (sig) => {
  console.log(`[video-svc] received ${sig}, shutting down...`);
  server.close(() => process.exit(0));
  await sleep(2000);
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
