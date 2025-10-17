// server.js
import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import tmp from "tmp";
import { v4 as uuidv4 } from "uuid";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ---- Supabase (se configura con variables en Railway) ----
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "video-results";
const SIGNED_URL_EXPIRES = Number(process.env.SIGNED_URL_EXPIRES || 604800);
const supabase = (SUPABASE_URL && SUPABASE_KEY) ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// -------- Utilidades --------

// Sube un archivo local a Supabase y devuelve URL pública o firmada
async function uploadToSupabase(localPath, destKey, contentType) {
  if (!supabase) throw new Error("Supabase not configured");
  const fileBuffer = fs.readFileSync(localPath);

  const { error: upErr } = await supabase
    .storage
    .from(SUPABASE_BUCKET)
    .upload(destKey, fileBuffer, { contentType: contentType || "application/octet-stream", upsert: true });

  if (upErr) throw upErr;

  // Si el bucket es público, devuelve publicUrl
  const { data: pub } = supabase.storage.from(SUPABASE_BUCKET).getPublicUrl(destKey);
  if (pub?.publicUrl && !pub.publicUrl.includes("null")) return pub.publicUrl;

  // Si es privado, devuelve signedUrl
  const { data: signed, error: signErr } =
    await supabase.storage.from(SUPABASE_BUCKET).createSignedUrl(destKey, SIGNED_URL_EXPIRES);
  if (signErr) throw signErr;
  return signed.signedUrl;
}

// Descarga una URL a archivo temporal
async function downloadToTemp(url, postfix = ".mp4") {
  const f = tmp.fileSync({ postfix });
  const writer = fs.createWriteStream(f.name);
  const resp = await axios.get(url, { responseType: "stream" });
  await new Promise((ok, bad) => {
    resp.data.pipe(writer).on("finish", ok).on("error", bad);
  });
  return f; // { name, removeCallback() }
}

// Ejecuta comando shell y devuelve stdout/stderr
function execAsync(cmd, maxBuffer = 1024 * 1024 * 300) {
  return new Promise((resolve, reject) => {
    exec(cmd, { maxBuffer }, (err, stdout, stderr) => {
      if (err) return reject(new Error(`CMD:\n${cmd}\n\nSTDERR:\n${stderr}\n\nSTDOUT:\n${stdout}`));
      resolve({ stdout, stderr });
    });
  });
}

// ---------- Endpoint opcional: extraer WAV 16k ----------
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

// ---------- Endpoint: astats (picos de energía; útil para risas) ----------
app.post("/astats", async (req, res) => {
  try {
    const {
      video_url,
      method = "astats",      // "astats" | "auto" | "silencedetect"
      max_seconds = 1200,     // ← default: analizar 20 minutos (1200s)
      percentile = 0.60,      // ← default: 0.60 (más sensible)
      base_db = -35,          // ← default: -35 dBFS (más sensible)
      pad = 1.2,
      merge_gap = 1.0,
      debug = false,
    } = req.body || {};

    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");

    // Construye -t si pidieron recortar el análisis (por defecto 1200s)
    const tFlag = typeof max_seconds === "number" && max_seconds > 0 ? `-t ${max_seconds}` : "";

    // Intento principal: ametadata con astats (RMS)
    const cmdAstats =
      `ffmpeg -hide_banner -y ${tFlag} -i "${tmpVid.name}" -vn ` +
      `-af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" -f null - 2>&1`;

    // Ejecuta y parsea ASTATS
    let usedMethod = "astats_ametadata";
    let stdout = "";
    let pts = [];
    try {
      const run = await execAsync(cmdAstats);
      stdout = (run.stdout || "") + (run.stderr || "");
      // Regex robusto: captura tiempo (pts_time o time) y valor RMS
      const re1 = /pts_time:(\d+(?:\.\d+)?)\s+.*?key=lavfi\.astats\.Overall\.RMS_level.*?value=([-\d.]+)/g;
      const re2 = /time:(\d+(?:\.\d+)?)\s+.*?Overall\.RMS_level.*?(-?\d+(?:\.\d+)?)/g;

      let m;
      while ((m = re1.exec(stdout)) !== null) pts.push({ t: parseFloat(m[1]), rms: parseFloat(m[2]) });
      if (pts.length === 0) {
        while ((m = re2.exec(stdout)) !== null) pts.push({ t: parseFloat(m[1]), rms: parseFloat(m[2]) });
      }

      // Si no hay puntos y el usuario permitió fallback (method !== "astats"), lanzamos para ir al fallback
      if (pts.length === 0 && method !== "astats") throw new Error("No RMS points; fallback allowed");

    } catch (e) {
      if (method === "astats") {
        // Usuario pidió astats estricto; devolvemos error con debug
        tmpVid.removeCallback();
        return res.status(500).json({
          ok: false,
          error: "ASTATS failed and fallback is disabled",
          _debug: debug ? { cmdAstats, err: e.message } : undefined,
        });
      }
      usedMethod = "silencedetect_fallback";
    }

    if (usedMethod === "astats_ametadata" && pts.length > 0) {
      // Calcula threshold dinámico
      const sorted = [...pts].sort((a, b) => a.rms - b.rms);
      const pIdx = Math.max(0, Math.min(sorted.length - 1, Math.floor(sorted.length * percentile)));
      const pVal = sorted[pIdx]?.rms ?? base_db;
      const TH = Math.max(pVal, base_db);

      // Construye rangos continuos donde RMS >= TH
      let ranges = [];
      let cur = null;
      for (const p of pts) {
        if (p.rms >= TH) {
          cur ? (cur.end = p.t) : (cur = { start: p.t, end: p.t });
        } else if (cur) {
          ranges.push(cur);
          cur = null;
        }
      }
      if (cur) ranges.push(cur);

      // Pad + merge
      ranges = ranges
        .map(r => ({ start: Math.max(0, r.start - pad), end: r.end + pad }))
        .sort((a, b) => a.start - b.start);

      const merged = [];
      for (const r of ranges) {
        const last = merged[merged.length - 1];
        if (!last || r.start - last.end > merge_gap) merged.push({ ...r });
        else last.end = Math.max(last.end, r.end);
      }

      tmpVid.removeCallback();
      return res.json({
        ok: true,
        threshold: TH,
        ranges: merged,
        limited: Boolean(max_seconds),
        points: pts.length,
        method: usedMethod,
        _debug: debug ? { cmdAstats, sampleOut: stdout.slice(0, 4000) } : undefined,
      });
    }

    // -------- Fallback: silencedetect (si ASTATS no produjo puntos) --------
    // silencedetect detecta silencios; invertimos para quedarnos con “zonas con sonido”
    const noiseTh = Math.abs(base_db); // p.ej., -35 ⇒ noise=35 dB
    const cmdSilence =
      `ffmpeg -hide_banner -y ${tFlag} -i "${tmpVid.name}" -af "silencedetect=noise=-${noiseTh}dB:d=0.3" -f null - 2>&1`;

    const run2 = await execAsync(cmdSilence);
    const out2 = (run2.stdout || "") + (run2.stderr || "");

    // Parsear silent_start / silent_end
    const sStart = [...out2.matchAll(/silence_start:\s*([0-9.]+)/g)].map(m => parseFloat(m[1]));
    const sEnd   = [...out2.matchAll(/silence_end:\s*([0-9.]+)/g)].map(m => parseFloat(m[1]));

    // Construimos “zonas con sonido” (no silencio)
    const sound = [];
    let cursor = 0;
    for (let i = 0; i < sStart.length; i++) {
      const st = sStart[i];
      const en = sEnd[i] ?? (typeof max_seconds === "number" ? max_seconds : null);
      // tramo con sonido antes del silencio
      if (st > cursor) sound.push({ start: cursor, end: st });
      cursor = en ?? cursor;
    }
    // tramo final con sonido
    const endCap = typeof max_seconds === "number" ? max_seconds : cursor;
    if (endCap > cursor) sound.push({ start: cursor, end: endCap });

    // Pad + merge a “sound”
    const padded = sound
      .map(r => ({ start: Math.max(0, r.start - pad), end: r.end + pad }))
      .sort((a, b) => a.start - b.start);

    const mergedSound = [];
    for (const r of padded) {
      const last = mergedSound[mergedSound.length - 1];
      if (!last || r.start - last.end > merge_gap) mergedSound.push({ ...r });
      else last.end = Math.max(last.end, r.end);
    }

    tmpVid.removeCallback();
    return res.json({
      ok: true,
      threshold: -noiseTh,
      ranges: mergedSound,
      limited: Boolean(max_seconds),
      points: 0,
      method: "silencedetect_fallback",
      _debug: debug ? { cmdSilence: cmdSilence, sampleOut: out2.slice(0, 4000) } : undefined,
    });

  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- Endpoint: cut (recorta clip y sube a Supabase) ----------
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

    // Filtros visuales simples (vertical/square) + normalización de audio opcional
    let vf = "";
    if (filters.format === "vertical_9_16") {
      vf = `-filter_complex "[0:v]scale=-2:1920,boxblur=luma_radius=20:luma_power=1[bg];[0:v]scale=-2:1080[fg];[bg][fg]overlay=(W-w)/2:(H-h)/2"`;
    } else if (filters.format === "square_1_1") {
      vf = `-vf "scale=1080:-2, pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black"`;
    }

    const loud = filters.loudnorm ? `-af "loudnorm=I=-16:TP=-1.5:LRA=11"` : "";
    const sub = filters.captions_url ? `-vf "subtitles='${filters.captions_url.replace(/:/g,"\\:")}'"` : "";
    const codecs = `-c:v ${output.video_codec||"libx264"} -preset ${output.preset||"veryfast"} -crf ${output.crf||23} -c:a ${output.audio_codec||"aac"} ${output.faststart!==false? "-movflags +faststart" : ""}`;

    await execAsync(`ffmpeg -hide_banner -y -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" ${vf||""} ${sub||""} ${loud||""} ${codecs} "${out}"`);
    await execAsync(`ffmpeg -hide_banner -y -ss ${start_time} -i "${tmpVid.name}" -frames:v 1 "${thumb}"`);

    const clipUrl = await uploadToSupabase(out, `clips/${id}.mp4`, "video/mp4");
    const thumbUrl = await uploadToSupabase(thumb, `clips/${id}.jpg`, "image/jpeg");

    const size_bytes = fs.statSync(out).size;
    const duration = end_time - start_time;

    fs.existsSync(out) && fs.unlinkSync(out);
    fs.existsSync(thumb) && fs.unlinkSync(thumb);
    tmpVid.removeCallback();

    return res.json({ ok: true, clip: { url: clipUrl, thumbnail_url: thumbUrl, duration, size_bytes } });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/", (_req, res) => res.send("video-svc up"));
app.listen(process.env.PORT || 3000, () => console.log("svc listening"));
