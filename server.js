// server.js  — video-svc (Express + FFmpeg + Supabase)
// Endpoints: /extract-audio, /astats (con max_seconds), /cut

import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import tmp from "tmp";
import { v4 as uuidv4 } from "uuid";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ---------- Config Supabase (por Variables de Entorno en Railway) ----------
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

  // Si el bucket es público devolverá publicUrl; si es privado, generamos signedUrl
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

  // axios con timeouts altos y sin límite de tamaño
  const resp = await axios.get(url, {
    responseType: "stream",
    timeout: 300_000,               // 5 min
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    validateStatus: s => s >= 200 && s < 400, // seguir 3xx también
  });

  await new Promise((ok, bad) => {
    resp.data.pipe(writer).on("finish", ok).on("error", bad);
  });

  return f; // { name, removeCallback() }
}

function execAsync(cmd, maxBuffer = 1024 * 1024 * 200) {
  return new Promise((resolve, reject) => {
    exec(cmd, { maxBuffer }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      resolve({ stdout, stderr });
    });
  });
}

// ---------- Endpoint: salud ----------
app.get("/", (_req, res) => res.send("video-svc up"));

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

// ---------- Endpoint: astats (picos de energía; útil para “risas”) ----------
// ACEPTA opcionalmente { max_seconds: number } para responder rápido mientras iteras
app.post("/astats", async (req, res) => {
  try {
    const { video_url, max_seconds } = req.body || {};
    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const timeLimit = (typeof max_seconds === "number" && max_seconds > 0) ? `-t ${max_seconds}` : "";

    // Extrae RMS por bloque con astats y lo vuelca a stderr (capturado en stdout de exec)
    const cmd = `ffmpeg -hide_banner -i "${tmpVid.name}" ${timeLimit} -vn -af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" -f null - 2>&1`;
    const { stdout } = await execAsync(cmd);
    tmpVid.removeCallback();

    // Parseo pts_time + RMS_level
    const re = /pts_time:(\d+(?:\.\d+)?)\s+.*?key:lavfi\.astats\.Overall\.RMS_level\s+value:([-\d.]+)/g;
    let m, pts = [];
    while ((m = re.exec(stdout)) !== null) {
      const t = parseFloat(m[1]);
      const rms = parseFloat(m[2]);
      if (!Number.isNaN(t) && !Number.isNaN(rms)) pts.push({ t, rms });
    }

    if (!pts.length) return res.json({ ok: true, threshold: -18, ranges: [], limited: !!timeLimit });

    // Umbral: percentil 85 o -18 dBFS (el mayor)
    const sorted = [...pts].sort((a, b) => a.rms - b.rms);
    const p85 = sorted[Math.floor(sorted.length * 0.85)].rms;
    const TH = Math.max(p85, -18);

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

    // Expande bordes y fusiona huecos pequeños
    const PAD = 1.2, MERGE = 1.0;
    ranges = ranges
      .map(r => ({ start: Math.max(0, r.start - PAD), end: r.end + PAD }))
      .sort((a, b) => a.start - b.start);

    const merged = [];
    for (const r of ranges) {
      const last = merged[merged.length - 1];
      if (!last || r.start - last.end > MERGE) merged.push({ ...r });
      else last.end = Math.max(last.end, r.end);
    }

    return res.json({ ok: true, threshold: TH, ranges: merged, limited: !!timeLimit });
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

    // Video filters (formato) + audio (loudnorm) + subtítulos opcionales
    let vfParts = [];
    if (filters.format === "vertical_9_16") {
      // Fondo blur + primer plano centrado (1080x1920)
      vfParts.push(`[0:v]scale=-2:1920,boxblur=luma_radius=20:luma_power=1[bg]`);
      vfParts.push(`[0:v]scale=-2:1080[fg]`);
    } else if (filters.format === "square_1_1") {
      // 1080x1080 con padding
      vfParts.push(`scale=1080:-2, pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black`);
    }

    // Construye filtro de video final
    let vf = "";
    if (filters.format === "vertical_9_16") {
      vf = `-filter_complex "${vfParts[0]};${vfParts[1]};[bg][fg]overlay=(W-w)/2:(H-h)/2"`;
    } else if (filters.format === "square_1_1") {
      vf = `-vf "${vfParts.join(",")}"`;
    }

    // Subtítulos opcionales
    let sub = "";
    if (filters.captions_url) {
      const subUrl = String(filters.captions_url).replace(/:/g, "\\:");
      sub = vf ? `,subtitles='${subUrl}'` : `-vf "subtitles='${subUrl}'"`;
      // Si ya teníamos -vf/-filter_complex, añadimos el filtro al final
      if (vf.startsWith("-vf")) vf = vf.replace(/"$/, `${sub.replace('-vf ','').slice(4)}`); // hack simple
      else if (vf.startsWith("-filter_complex")) vf = vf.replace(/"$/, `${sub.replace('-vf ','').slice(4)}`);
      else vf = sub;
    }

    // Audio loudness normalizado
    const loud = filters.loudnorm ? `-af "loudnorm=I=-16:TP=-1.5:LRA=11"` : "";

    const codecs = `-c:v ${output.video_codec || "libx264"} -preset ${output.preset || "veryfast"} -crf ${output.crf || 23} -c:a ${output.audio_codec || "aac"} ${output.faststart !== false ? "-movflags +faststart" : ""}`;

    // Corte
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
