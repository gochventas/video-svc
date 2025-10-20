// server.js
// Video service – /astats (detectar tramos) & /cut (recortar)
// Node 18+. FFmpeg debe estar instalado. Imagen de Railway con ffmpeg recomendado.

import express from 'express';
import morgan from 'morgan';
import bodyParser from 'body-parser';
import { spawn } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import util from 'util';

// Supabase (opcional)
import pkg from '@supabase/supabase-js';
const { createClient } = pkg;

const app = express();
app.use(bodyParser.json({ limit: '20mb' }));
app.use(morgan('tiny'));

const PORT = process.env.PORT || 8080;
const NODE_ENV = process.env.NODE_ENV || 'production';

// ===== Supabase opcional para subir recortes =====
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const STORAGE_BUCKET = process.env.STORAGE_BUCKET || 'clips';
let supabase = null;
if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
}

// ===== Utils =====
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
function tmpPath(ext = '') {
  const name = `tmp-${Date.now()}-${crypto.randomBytes(6).toString('hex')}${ext}`;
  return path.join(os.tmpdir(), name);
}

// Ejecuta ffmpeg con spawn (streaming). Limita lo que guardamos de stderr/stdout.
function runFfmpeg(args, opts = {}) {
  const {
    maxMs = 15 * 60 * 1000,       // 15 min por seguridad
    stderrLimit = 200_000,        // guardamos hasta 200KB para diagnóstico
    stdoutLimit = 200_000,
    cwd,
    env,
  } = opts;

  return new Promise((resolve) => {
    const startedAt = Date.now();
    const child = spawn('ffmpeg', args, { cwd, env });
    let stdout = '';
    let stderr = '';
    let settled = false;

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        try { child.kill('SIGKILL'); } catch {}
        resolve({
          ok: false,
          code: null,
          signal: 'SIGKILL',
          stdout,
          stderr: stderr + '\n[runFfmpeg] timeout exceeded',
          args,
          took_ms: Date.now() - startedAt,
        });
      }
    }, maxMs);

    child.stdout.on('data', (d) => {
      if (stdout.length < stdoutLimit) stdout += d.toString();
    });
    child.stderr.on('data', (d) => {
      if (stderr.length < stderrLimit) stderr += d.toString();
    });
    child.on('error', (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        ok: false,
        code: null,
        signal: null,
        error: { name: err.name, message: err.message, stack: err.stack },
        stdout,
        stderr,
        args,
        took_ms: Date.now() - startedAt,
      });
    });
    child.on('close', (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        ok: code === 0,
        code,
        signal,
        stdout,
        stderr,
        args,
        took_ms: Date.now() - startedAt,
      });
    });
  });
}

// Une rangos cercanos
function mergeRanges(ranges, gap = 0.8, maxT = 1e12) {
  if (!ranges.length) return [];
  const arr = ranges
    .map((r) => ({
      start: clamp(Number(r.start) || 0, 0, maxT),
      end: clamp(Number(r.end) || 0, 0, maxT),
    }))
    .filter((r) => r.end > r.start)
    .sort((a, b) => a.start - b.start);

  const out = [arr[0]];
  for (let i = 1; i < arr.length; i++) {
    const prev = out[out.length - 1];
    const cur = arr[i];
    if (cur.start - prev.end <= gap) {
      prev.end = Math.max(prev.end, cur.end);
    } else {
      out.push(cur);
    }
  }
  return out;
}

// ===== Endpoints =====

app.get('/healthz', (_req, res) => {
  res.json({ ok: true, where: 'healthz', now: new Date().toISOString() });
});

app.all('/echo', (req, res) => {
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
 *   video_url: string (requerido, URL firmada OK),
 *   // parámetros típicos para silencedetect:
 *   base_db?: number (default -35) -> noise=THdB
 *   pad?: number (s, default 1.2)
 *   merge_gap?: number (s, default 0.8)
 *   max_seconds?: number (default 1800, cap 21600)
 *
 *   // Avanzado: si quieres RMS, añade { mode: "rms", percentile?: 0.6..0.95 }
 * }
 *
 * Devuelve: { ok, ranges:[{start,end}…], method, threshold, stderr_excerpt, took_ms }
 */
app.post('/astats', async (req, res) => {
  const t0 = Date.now();
  const reqId = crypto.randomBytes(4).toString('hex');
  const info = { where: 'astats', reqId };

  try {
    const {
      video_url,
      max_seconds,
      base_db,
      pad,
      merge_gap,
      mode,         // 'rms' para forzar RMS
      percentile,   // solo si mode='rms'
    } = req.body || {};

    if (!video_url || typeof video_url !== 'string') {
      return res.status(400).json({
        ok: false, ...info,
        error: { name: 'BadRequest', message: 'video_url es requerido' },
      });
    }

    const MAX_T = clamp(Number(max_seconds) || 1800, 1, 21600); // default 30 min
    const PAD = Number.isFinite(pad) ? Number(pad) : 1.2;
    const MERGE = Number.isFinite(merge_gap) ? Number(merge_gap) : 0.8;
    const TH = Number.isFinite(base_db) ? Number(base_db) : -35;

    // === 1) Ruta rápida y estable: silencedetect ===
    const sdArgs = [
      '-hide_banner', '-nostats', '-loglevel', 'error',
      '-y',
      '-t', String(MAX_T),
      '-rw_timeout', String(30_000_000), // ~30s
      '-i', video_url,
      '-vn',
      '-af', `aresample=16000:resampler=soxr:precision=16,aformat=channel_layouts=mono,silencedetect=noise=${TH}dB:d=0.2`,
      '-f', 'null', '-',
    ];

    let sd = await runFfmpeg(sdArgs, { maxMs: Math.min(MAX_T * 1000 + 60_000, 10 * 60_000) });
    // Parsear líneas silence_start/end
    const text = (sd.stdout || '') + '\n' + (sd.stderr || '');
    const events = [];
    const re = /silence_(start|end):\s*([-\d.]+)/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      events.push({ k: m[1], v: parseFloat(m[2]) });
    }

    let last = 0;
    const raw = [];
    for (const ev of events) {
      if (ev.k === 'start') {
        const end = clamp(ev.v, 0, MAX_T);
        if (end > last) raw.push({ start: last, end });
      } else if (ev.k === 'end') {
        last = clamp(ev.v, 0, MAX_T);
      }
    }
    if (last < MAX_T) raw.push({ start: last, end: MAX_T });

    const merged = mergeRanges(
      raw.map(r => ({ start: clamp(r.start - PAD, 0, MAX_T), end: clamp(r.end + PAD, 0, MAX_T) })),
      MERGE,
      MAX_T,
    );

    // Si el usuario quiere RMS explícito o no se detectó nada, intentamos RMS con prudencia
    if ((mode === 'rms' || merged.length === 0) && MAX_T <= 3600) {
      // NOTA: RMS solo si el tramo no es enorme (para evitar logs y 502)
      // Usamos ametadata pero con parámetros conservadores
      const rmsArgs = [
        '-hide_banner', '-nostats', '-loglevel', 'error',
        '-y',
        '-t', String(MAX_T),
        '-rw_timeout', String(30_000_000),
        '-i', video_url,
        '-vn',
        // astats + ametadata imprimen con alta frecuencia; usamos reset para bajar densidad
        '-af', 'aresample=16000:resampler=soxr:precision=16,aformat=channel_layouts=mono,astats=metadata=1:reset=0.5,ametadata=print:key=lavfi.astats.Overall.RMS_level',
        '-f', 'null', '-',
      ];
      const rms = await runFfmpeg(rmsArgs, { maxMs: Math.min(MAX_T * 1000 + 60_000, 10 * 60_000) });
      const blob = (rms.stdout || '') + '\n' + (rms.stderr || '');
      const reR = /lavfi\.astats\.Overall\.RMS_level=([-\d.]+)/g;
      const pts = [];
      let g;
      while ((g = reR.exec(blob)) !== null) {
        const v = Number(g[1]);
        if (Number.isFinite(v)) pts.push(v);
      }

      if (pts.length) {
        // Umbral por percentil (por defecto 0.6) o base_db si es mayor
        const q = clamp(Number(percentile) || 0.6, 0.05, 0.95);
        const a = [...pts].sort((x, y) => x - y);
        const pos = clamp((a.length - 1) * q, 0, a.length - 1);
        const base = Math.floor(pos);
        const rest = pos - base;
        const thAuto = a[base] + (a[base + 1] - a[base]) * rest || a[base];
        const THRESH = Number.isFinite(TH) ? Math.max(TH, thAuto) : thAuto;

        // dt aproximado a 0.05s
        const dt = clamp(MAX_T / Math.max(pts.length, 1), 0.02, 0.1);
        const seq = [];
        let cur = null;
        for (let i = 0; i < pts.length; i++) {
          const t = i * dt;
          const voiced = pts[i] >= THRESH;
          if (voiced && cur == null) cur = t;
          if (!voiced && cur != null) {
            seq.push({ start: cur, end: t });
            cur = null;
          }
        }
        if (cur != null) seq.push({ start: cur, end: Math.min(MAX_T, pts.length * dt) });

        const padded = seq.map(r => ({
          start: clamp(r.start - PAD, 0, MAX_T),
          end: clamp(r.end + PAD, 0, MAX_T),
        }));
        const mergedRms = mergeRanges(padded, MERGE, MAX_T);

        return res.json({
          ok: true, ...info,
          threshold: THRESH,
          method: 'astats_ametadata',
          points: pts.length,
          ranges: mergedRms,
          limited: true,
          stderr_excerpt: (rms.stderr || '').slice(0, 2000),
          took_ms: Date.now() - t0,
          mode: 'http',
        });
      }
    }

    // Respuesta final (silencedetect)
    return res.json({
      ok: true, ...info,
      threshold: TH,
      method: 'silencedetect',
      points: 0,
      ranges: merged,
      limited: true,
      stderr_excerpt: (sd.stderr || '').slice(0, 2000),
      took_ms: Date.now() - t0,
      mode: 'http',
    });
  } catch (err) {
    return res.status(500).json({
      ok: false, ...info,
      error: { name: err?.name || 'ServerError', message: err?.message || String(err) },
      took_ms: Date.now() - t0,
    });
  }
});

/**
 * POST /cut
 * body: {
 *   video_url: string,
 *   start_time: number,
 *   end_time: number,
 *   filters?: { loudnorm?: boolean },
 *   output?: { container?: 'mp4'|'mov'|'mkv', video_codec?: string, audio_codec?: string, crf?: number, preset?: string, faststart?: boolean },
 *   upload?: { bucket?: string, pathPrefix?: string } // si hay supabase
 * }
 */
app.post('/cut', async (req, res) => {
  const t0 = Date.now();
  const reqId = crypto.randomBytes(4).toString('hex');
  const info = { where: 'cut', reqId };

  try {
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
        ok: false, ...info,
        error: { name: 'BadRequest', message: 'video_url, start_time y end_time son requeridos' },
      });
    }
    const ss = Number(start_time);
    const to = Number(end_time);
    if (to <= ss) {
      return res.status(400).json({
        ok: false, ...info,
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

    const outFile = tmpPath(`.${container}`);

    const af = [];
    if (filters.loudnorm) af.push('loudnorm=I=-16:TP=-1.5:LRA=11');

    const args = [
      '-hide_banner', '-nostats', '-loglevel', 'error',
      '-y',
      '-i', video_url,
      '-ss', String(ss),
      '-t', String(dur),
      '-map', '0',
      '-c:v', vcodec,
      '-preset', preset,
      '-crf', String(crf),
      '-c:a', acodec,
    ];
    if (af.length) args.push('-af', af.join(','));
    if (faststart && container === 'mp4') {
      args.push('-movflags', '+faststart');
    }
    args.push(outFile);

    const r = await runFfmpeg(args, { maxMs: Math.min(dur * 1000 + 120_000, 20 * 60_000) });
    if (!r.ok || !fs.existsSync(outFile)) {
      return res.status(500).json({
        ok: false, ...info,
        error: { name: 'FFmpegError', message: 'ffmpeg falló' },
        stderr: (r.stderr || '').slice(0, 4000),
        args: r.args,
        took_ms: Date.now() - t0,
      });
    }

    // Subida opcional a Supabase
    if (!supabase) {
      const stats = fs.statSync(outFile);
      const base64 = fs.readFileSync(outFile).toString('base64');
      fs.unlink(outFile, () => {});
      return res.json({
        ok: true, ...info,
        size_bytes: stats.size,
        mime: container === 'mp4' ? 'video/mp4' : 'application/octet-stream',
        data_base64: base64,
        took_ms: Date.now() - t0,
      });
    }

    const bucket = upload.bucket || STORAGE_BUCKET || 'clips';
    const prefix = upload.pathPrefix || 'cuts';
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
        ok: false, ...info,
        error: { name: 'StorageApiError', message: error.message },
        took_ms: Date.now() - t0,
      });
    }

    let public_url = null;
    try {
      const { data: pub } = supabase.storage.from(bucket).getPublicUrl(storagePath);
      public_url = pub?.publicUrl || null;
    } catch {}

    return res.json({
      ok: true, ...info,
      bucket,
      path: storagePath,
      public_url,
      took_ms: Date.now() - t0,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false, ...info,
      error: { name: err?.name || 'ServerError', message: err?.message || String(err) },
      took_ms: Date.now() - t0,
    });
  }
});

// ===== Iniciar servidor =====
const server = app.listen(PORT, () => {
  console.log(`[video-svc] listening on :${PORT} (${NODE_ENV})`);
});

// Endurecer timeouts para Railway (evitar 502 por sockets colgados)
server.keepAliveTimeout = 65_000;
server.headersTimeout = 70_000;

const shutdown = async (sig) => {
  console.log(`[video-svc] received ${sig}, shutting down...`);
  server.close(() => process.exit(0));
  await sleep(2000);
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
