// server.js — video-svc (Express + FFmpeg + Supabase)
// Endpoints: /extract-audio, /astats, /cut

import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import tmp from "tmp";
import { v4 as uuidv4 } from "uuid";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ---------- Config Supabase (Variables en Railway) ----------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "video-results";
const SIGNED_URL_EXPIRES = Number(process.env.SIGNED_URL_EXPIRES || 604800); // 7 días

const supabase = (SUPABASE_URL && SUPABASE_KEY)
  ? createClient(SUPABASE_URL, SUPABASE_KEY)
  : null;

// ---------- Helpers ----------
async function uploadToSupabase(localPath, destKey, contentType) {
  if (!supabase) throw new Error("Supabase no está configurado");
  const fileBuffer = fs.readFileSync(localPath);

  const { error: upErr } = await supabase
    .storage
    .from(SUPABASE_BUCKET)
    .upload(destKey, fileBuffer, {
      contentType: contentType || "application/octet-stream",
      upsert: true,
    });

  if (upErr) throw upErr;

  // Si el bucket es público devuelve publicUrl; si es privado, generamos signedUrl
  const { data: pub } = supabase.storage.from(SUPABASE_BUCKET).getPublicUrl(destKey);
  if (pub?.publicUrl && !pub.publicUrl.includes("null")) return pub.publicUrl;

  const { data: signed, error: signErr } =
    await supabase.storage.from(SUPABASE_BUCKET).createSignedUrl(destKey, SIGNED_URL_EXPIRES);
  if (signErr) throw signErr;

  return signed.signedUrl;
}

async function downloadToTemp(url, postfix = ".mp4") {
  const f = tmp.fileSync({ postfix });
  const writer = fs.createWriteStream(f.name);

  const resp = await axios.get(url, {
    responseType: "stream",
    timeout: 300_000,               // 5 min
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    validateStatus: s => s >= 200 && s < 400,
  });

  await new Promise((ok, bad) => {
    resp.data.pipe(writer).on("finish", ok).on("error", bad);
  });

  return f; // { name, removeCallback() }
}

// Exec que devuelve STDERR/STDOUT completos si falla (para diagnosticar)
function execAsync(cmd, maxBuffer = 1024 * 1024 * 200) {
  return new Promise((resolve, reject) => {
    exec(cmd, { maxBuffer }, (err, stdout, stderr) => {
      if (err) {
        const msg = `CMD:\n${cmd}\n\nSTDERR:\n${stderr}\n\nSTDOUT:\n${stdout}`;
        return reject(new Error(msg));
      }
      resolve({ stdout, stderr });
    });
  });
}

// ---------- Health ----------
app.get("/", (_req, res) => res.send("video-svc up"));

// ---------- /extract-audio: WAV 16k mono ----------
app.post("/extract-audio", async (req, res) => {
  try {
    const { video_url } = req.body || {};
    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const tmpWav = tmp.fileSync({ postfix: ".wav" });

    await execAsync(`ffmpeg -hide_banner -y -i "${tmpVid.name}" -vn -ac 1 -ar 16000 "${tmpWav.name}"`);
    const url = await uploadToSupabase(tmpWav.name, `audio/${uuidv4()}_16k.wav`, "audio/wav");

    tmpVid.removeCallback(); tmpWav.removeCallback?.();
    return res.json({ ok: true, audio_url: url });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- Parse helpers (astats / silencedetect) ----------
function parseAmetadataPrint(raw) {
  // Busca pares pts_time + value:-XX.Y
  const lines = raw.split(/\r?\n/);
  const ptsRe = /pts_time:(\d+(?:\.\d+)?)/;
  const keyRe = /key:lavfi\.astats\.Overall\.RMS_level/;
  const valRe = /value:([-\d.]+)/;

  let t_current = null;
  const pts = []; // { t, rms }
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const mT = line.match(ptsRe);
    if (mT) { t_current = parseFloat(mT[1]); continue; }

    if (keyRe.test(line)) {
      const next = (i + 1 < lines.length) ? lines[i + 1] : "";
      const mV = (next.match(valRe) || line.match(valRe));
      if (mV && t_current !== null) {
        const rms = parseFloat(mV[1]);
        if (!Number.isNaN(rms)) pts.push({ t: t_current, rms });
      }
    }
  }
  return pts;
}

function buildRangesFromPoints(pts, { percentile = 0.80, base_db = -24, pad = 1.2, merge_gap = 1.0 }) {
  if (!pts.length) return { threshold: base_db, ranges: [], points: 0 };

  const sorted = [...pts].sort((a, b) => a.rms - b.rms);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * percentile)));
  const pxx = sorted[idx].rms;
  const TH = Math.max(pxx, base_db);

  let ranges = [], cur = null;
  for (const p of pts) {
    if (p.rms >= TH) {
      cur ? cur.end = p.t : cur = { start: p.t, end: p.t };
    } else if (cur) {
      ranges.push(cur);
      cur = null;
    }
  }
  if (cur) ranges.push(cur);

  ranges = ranges
    .map(r => ({ start: Math.max(0, r.start - pad), end: r.end + pad }))
    .sort((a, b) => a.start - b.start);

  const merged = [];
  for (const r of ranges) {
    const last = merged[merged.length - 1];
    if (!last || r.start - last.end > merge_gap) merged.push({ ...r });
    else last.end = Math.max(last.end, r.end);
  }
  return { threshold: TH, ranges: merged, points: pts.length };
}

function parseSilencedetect(raw) {
  // Captura "silence_start: X" y "silence_end: Y"
  const startRe = /silence_start:\s*([0-9.]+)/g;
  const endRe   = /silence_end:\s*([0-9.]+)/g;

  const starts = [];
  const ends = [];
  let m;
  while ((m = startRe.exec(raw)) !== null) starts.push(parseFloat(m[1]));
  while ((m = endRe.exec(raw))   !== null) ends.push(parseFloat(m[1]));

  return { starts, ends };
}

function invertSilenceToSound(starts, ends, totalDur) {
  // Asume silencio inicial si empieza con end sin start previo
  const intervals = [];
  let cursor = 0;

  for (let i = 0; i < Math.max(starts.length, ends.length); i++) {
    const s = starts[i];
    const e = ends[i];
    if (typeof s === "number") {
      // tramo de sonido antes del silencio
      if (s > cursor) intervals.push({ start: cursor, end: s });
      cursor = (typeof e === "number") ? e : s; // si no hay end aún, se ajustará más adelante
    } else if (typeof e === "number") {
      // silencio que termina sin inicio explícito: sonido previo
      if (e > cursor) intervals.push({ start: cursor, end: e });
      cursor = e;
    }
  }
  // tramo final
  if (totalDur && cursor < totalDur) intervals.push({ start: cursor, end: totalDur });

  // Limpieza de duplicados y orden
  return intervals
    .filter(r => r.end > r.start)
    .sort((a, b) => a.start - b.start);
}

// ---------- /astats: picos de energía (heurística de “risas”) ----------
app.post("/astats", async (req, res) => {
  try {
    const {
      video_url,
      max_seconds,
      percentile = 0.80,
      base_db = -24,
      pad = 1.2,
      merge_gap = 1.0,
      debug = false,
    } = req.body || {};

    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const timeLimit = (typeof max_seconds === "number" && max_seconds > 0) ? `-t ${max_seconds}` : "";

    // Verifica audio
    let durationSec = null;
    try {
      const probeDur = await execAsync(
        `ffprobe -v error -show_entries format=duration -of default=nokey=1:noprint_wrappers=1 "${tmpVid.name}"`
      );
      const d = parseFloat((probeDur.stdout || "").trim());
      if (!isNaN(d) && d > 0) durationSec = d;
      const probeAud = await execAsync(
        `ffprobe -v error -select_streams a:0 -show_entries stream=index,codec_name -of json "${tmpVid.name}"`
      );
      const hasAudio = /"streams"\s*:\s*\[/.test(probeAud.stdout) && !/"streams"\s*:\s*\[\s*\]/.test(probeAud.stdout);
      if (!hasAudio) {
        tmpVid.removeCallback();
        return res.status(400).json({ ok: false, error: "El archivo no contiene pista de audio." });
      }
    } catch (_) {}

    // 1) Camino principal: astats->metadata + ametadata=print a STDERR (más compatible)
    const cmd1 =
      `ffmpeg -hide_banner -i "${tmpVid.name}" ${timeLimit} -vn ` +
      `-af "aformat=channel_layouts=mono,aresample=async=1:first_pts=0,asetpts=N/SR*TB,` +
      `astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" ` +
      `-f null - 2>&1`;
    let raw1 = "";
    try {
      const { stdout } = await execAsync(cmd1);
      raw1 = stdout || "";
    } catch (e) {
      raw1 = (e.message || "");
    }

    let pts = parseAmetadataPrint(raw1);
    let built = buildRangesFromPoints(pts, { percentile, base_db, pad, merge_gap });

    // 2) Fallback: si no hay puntos, usa silencedetect (inversión de silencio → sonido)
    if (!built.points || built.points === 0) {
      const noise = Math.max(base_db, -28); // umbral sensato
      const cmd2 =
        `ffmpeg -hide_banner -i "${tmpVid.name}" ${timeLimit} -vn ` +
        `-af "aformat=channel_layouts=mono,aresample=async=1:first_pts=0,asetpts=N/SR*TB,` +
        `silencedetect=noise=${noise}dB:d=0.3" -f null - 2>&1`;
      let raw2 = "";
      try {
        const { stdout } = await execAsync(cmd2);
        raw2 = stdout || "";
      } catch (e2) {
        raw2 = (e2.message || "");
      }

      const { starts, ends } = parseSilencedetect(raw2);
      const rangesSound = invertSilenceToSound(starts, ends, durationSec);
      // Filtra sonidos largos razonables (0.6s–20s) como candidatos
      const filtered = rangesSound
        .map(r => ({ start: Math.max(0, r.start - pad), end: r.end + pad }))
        .filter(r => (r.end - r.start) >= 0.6 && (r.end - r.start) <= 20);

      tmpVid.removeCallback();
      return res.json({
        ok: true,
        threshold: base_db,
        ranges: filtered,
        limited: !!timeLimit,
        points: 0,
        method: "silencedetect_fallback",
        ...(debug ? { _debug: (raw2.slice(0, 2000)) } : {})
      });
    }

    tmpVid.removeCallback();
    return res.json({
      ok: true,
      threshold: built.threshold,
      ranges: built.ranges,
      limited: !!timeLimit,
      points: built.points,
      method: "astats_ametadata",
      ...(debug ? { _debug: (raw1.slice(0, 2000)) } : {})
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- /cut: recorta clip y sube a Supabase ----------
app.post("/cut", async (req, res) => {
  try {
    const {
      video_url, start_time, end_time,
      filters = { format: "original", captions_url: null, loudnorm: true },
      output = { container: "mp4", video_codec: "libx264", audio_codec: "aac", crf: 23, preset: "veryfast", faststart: true }
    } = req.body || {};

    if (!video_url || typeof start_time !== "number" || typeof end_time !== "number")
      return res.status(400).json({ ok: false, error: "video_url, start_time, end_time are required (numbers)" });
    if (end_time <= start_time)
      return res.status(400).json({ ok: false, error: "end_time must be > start_time" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const id = uuidv4();
    const out = `/tmp/clip_${id}.mp4`;
    const thumb = `/tmp/thumb_${id}.jpg`;

    // Formatos visuales
    let vf = "";
    if (filters.format === "vertical_9_16") {
      // Fondo blur 1080x1920 + primer plano 1080 px centrado
      vf = `-filter_complex "[0:v]scale=-2:1920,boxblur=luma_radius=20:luma_power=1[bg];[0:v]scale=-2:1080[fg];[bg][fg]overlay=(W-w)/2:(H-h)/2"`;
    } else if (filters.format === "square_1_1") {
      vf = `-vf "scale=1080:-2, pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black"`;
    }

    // Subtítulos opcionales
    if (filters.captions_url) {
      const subUrl = String(filters.captions_url).replace(/:/g, "\\:");
      if (!vf) vf = `-vf "subtitles='${subUrl}'"`;
      else vf = vf.replace(/"$/, `,subtitles='${subUrl}'"`);
    }

    // Audio loudness normalizado
    const loud = filters.loudnorm ? `-af "loudnorm=I=-16:TP=-1.5:LRA=11"` : "";

    const codecs =
      `-c:v ${output.video_codec || "libx264"} ` +
      `-preset ${output.preset || "veryfast"} ` +
      `-crf ${output.crf || 23} ` +
      `-c:a ${output.audio_codec || "aac"} ` +
      `${output.faststart !== false ? "-movflags +faststart" : ""}`;

    // Corte y thumbnail
    await execAsync(`ffmpeg -hide_banner -y -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" ${vf || ""} ${loud || ""} ${codecs} "${out}"`);
    await execAsync(`ffmpeg -hide_banner -y -ss ${start_time} -i "${tmpVid.name}" -frames:v 1 "${thumb}"`);

    const clipUrl = await uploadToSupabase(out, `clips/${id}.mp4`, "video/mp4");
    const thumbUrl = await uploadToSupabase(thumb, `clips/${id}.jpg`, "image/jpeg");

    const size_bytes = fs.statSync(out).size;
    const duration = end_time - start_time;

    // Limpieza
    fs.existsSync(out) && fs.unlinkSync(out);
    fs.existsSync(thumb) && fs.unlinkSync(thumb);
    tmpVid.removeCallback();

    return res.json({ ok: true, clip: { url: clipUrl, thumbnail_url: thumbUrl, duration, size_bytes } });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- Arranque ----------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`svc listening on ${PORT}`));
