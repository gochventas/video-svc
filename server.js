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

// Exec que devuelve STDERR/STDOUT completos si falla
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

// ---------- /astats: picos de energía (heurística de “risas”) ----------
// Body opcional:
//   max_seconds -> analizar solo primeros N s
//   percentile  -> 0..1 (ej. 0.80=p80; baja a 0.70 para más sensibilidad)
//   base_db     -> dBFS base (ej. -24 más sensible que -18)
//   pad         -> expansión de rangos (s)
//   merge_gap   -> fusión de rangos cercanos (s)
app.post("/astats", async (req, res) => {
  try {
    const {
      video_url,
      max_seconds,
      percentile = 0.80,
      base_db = -24,
      pad = 1.2,
      merge_gap = 1.0,
    } = req.body || {};

    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const timeLimit = (typeof max_seconds === "number" && max_seconds > 0) ? `-t ${max_seconds}` : "";

    // --- Verifica que exista stream de audio antes de procesar ---
    try {
      const probe = await execAsync(
        `ffprobe -v error -select_streams a:0 -show_entries stream=index,codec_name -of json "${tmpVid.name}"`
      );
      const hasAudio = /"streams"\s*:\s*\[/.test(probe.stdout) && !/"streams"\s*:\s*\[\s*\]/.test(probe.stdout);
      if (!hasAudio) {
        tmpVid.removeCallback();
        return res.status(400).json({
          ok: false,
          error: "El archivo no contiene pista de audio (no se puede analizar).",
          detail: probe.stdout || probe.stderr || null
        });
      }
    } catch (ppErr) {
      tmpVid.removeCallback();
      return res.status(400).json({
        ok: false,
        error: "ffprobe falló al leer el audio del archivo.",
        detail: ppErr.message
      });
    }

    // astats + ametadata imprime pares (pts_time y luego RMS_level). ¡Sin 'random=0'!
    const cmd =
      `ffmpeg -hide_banner -i "${tmpVid.name}" ${timeLimit} -vn ` +
      `-af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" ` +
      `-f null - 2>&1`;
    const { stdout } = await execAsync(cmd);
    tmpVid.removeCallback();

    const lines = stdout.split(/\r?\n/);
    const ptsRe = /pts_time:(\d+(?:\.\d+)?)/;
    const keyRe = /key:lavfi\.astats\.Overall\.RMS_level/;
    const valRe = /value:([-\d.]+)/;

    let t_current = null;
    const pts = []; // { t, rms }

    for (const line of lines) {
      const mT = line.match(ptsRe);
      if (mT) { t_current = parseFloat(mT[1]); continue; }
      if (keyRe.test(line)) {
        const mV = line.match(valRe);
        if (mV && t_current !== null) {
          const rms = parseFloat(mV[1]);
          if (!Number.isNaN(rms)) pts.push({ t: t_current, rms });
        }
      }
    }

    if (!pts.length) {
      return res.json({ ok: true, threshold: base_db, ranges: [], limited: !!timeLimit, points: 0 });
    }

    // Umbral dinámico: percentil vs base_db (toma el más exigente)
    const sorted = [...pts].sort((a, b) => a.rms - b.rms);
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * percentile)));
    const pxx = sorted[idx].rms;
    const TH = Math.max(pxx, base_db);

    // Construcción de rangos continuos por encima del umbral
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

    // Expansión y merge
    ranges = ranges
      .map(r => ({ start: Math.max(0, r.start - pad), end: r.end + pad }))
      .sort((a, b) => a.start - b.start);

    const merged = [];
    for (const r of ranges) {
      const last = merged[merged.length - 1];
      if (!last || r.start - last.end > merge_gap) merged.push({ ...r });
      else last.end = Math.max(last.end, r.end);
    }

    return res.json({
      ok: true,
      threshold: TH,
      ranges: merged,
      limited: !!timeLimit,
      points: pts.length,
      params: { percentile, base_db, pad, merge_gap }
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
