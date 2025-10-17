import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import tmp from "tmp";
import { v4 as uuidv4 } from "uuid";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ---- Supabase (configurado via variables de entorno en Railway) ----
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "video-results";
const SIGNED_URL_EXPIRES = Number(process.env.SIGNED_URL_EXPIRES || 604800);

const supabase =
  SUPABASE_URL && SUPABASE_KEY ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ---- Utilidades ----

// Sube un archivo local a Supabase y devuelve URL pública o firmada
async function uploadToSupabase(localPath, destKey, contentType) {
  if (!supabase) throw new Error("Supabase not configured");
  const fileBuffer = fs.readFileSync(localPath);

  const { error: upErr } = await supabase.storage
    .from(SUPABASE_BUCKET)
    .upload(destKey, fileBuffer, {
      contentType: contentType || "application/octet-stream",
      upsert: true,
    });

  if (upErr) throw upErr;

  // Si el bucket es público, devuelve publicUrl; si no, signedUrl
  const { data: pub } = supabase.storage.from(SUPABASE_BUCKET).getPublicUrl(destKey);
  if (pub?.publicUrl && !pub.publicUrl.includes("null")) return pub.publicUrl;

  const { data: signed, error: signErr } = await supabase.storage
    .from(SUPABASE_BUCKET)
    .createSignedUrl(destKey, SIGNED_URL_EXPIRES);
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
function execAsync(cmd, maxBuffer = 1024 * 1024 * 200) {
  return new Promise((resolve, reject) => {
    exec(cmd, { maxBuffer }, (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      resolve({ stdout, stderr });
    });
  });
}

// ---- Endpoints ----

// 1) Extraer WAV 16k mono del video (opcional)
app.post("/extract-audio", async (req, res) => {
  try {
    const { video_url } = req.body || {};
    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const tmpWav = tmp.fileSync({ postfix: ".wav" });

    await execAsync(
      `ffmpeg -hide_banner -y -i "${tmpVid.name}" -vn -ac 1 -ar 16000 "${tmpWav.name}"`
    );
    const url = await uploadToSupabase(
      tmpWav.name,
      `audio/${uuidv4()}_16k.wav`,
      "audio/wav"
    );

    tmpVid.removeCallback();
    tmpWav.removeCallback?.();
    return res.json({ ok: true, audio_url: url });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// 2) astats: detectar rangos con RMS alto (útil para risas)
app.post("/astats", async (req, res) => {
  try {
    const { video_url } = req.body || {};
    if (!video_url) return res.status(400).json({ ok: false, error: "video_url required" });

    const tmpVid = await downloadToTemp(video_url, ".mp4");

    const cmd =
      `ffmpeg -hide_banner -i "${tmpVid.name}" -vn ` +
      `-af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" ` +
      `-f null - 2>&1`;

    const { stdout } = await execAsync(cmd);
    tmpVid.removeCallback();

    // Parseo de pts_time + RMS_level
    const re =
      /pts_time:(\d+(?:\.\d+)?)\s+.*?key:lavfi\.astats\.Overall\.RMS_level\s+value:([-\d.]+)/g;
    let m;
    const pts = [];
    while ((m = re.exec(stdout)) !== null) {
      pts.push({ t: parseFloat(m[1]), rms: parseFloat(m[2]) });
    }
    if (!pts.length) return res.json({ ok: true, threshold: -18, ranges: [] });

    // Umbral dinámico: percentil 85 o -18 dBFS (lo que sea mayor)
    const sorted = [...pts].sort((a, b) => a.rms - b.rms);
    const p85 = sorted[Math.floor(sorted.length * 0.85)].rms;
    const TH = Math.max(p85, -18);

    // Construcción de rangos continuos
    const rangesRaw = [];
    let cur = null;
    for (const p of pts) {
      if (p.rms >= TH) {
        if (cur) cur.end = p.t;
        else cur = { start: p.t, end: p.t };
      } else if (cur) {
        rangesRaw.push(cur);
        cur = null;
      }
    }
    if (cur) rangesRaw.push(cur);

    // Padding y merge
    const PAD = 1.2;
    const MERGE = 1.0;
    const padded = rangesRaw
      .map((r) => ({ start: Math.max(0, r.start - PAD), end: r.end + PAD }))
      .sort((a, b) => a.start - b.start);

    const merged = [];
    for (const r of padded) {
      const last = merged[merged.length - 1];
      if (!last || r.start - last.end > MERGE) merged.push({ ...r });
      else last.end = Math.max(last.end, r.end);
    }

    return res.json({ ok: true, threshold: TH, ranges: merged });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// 3) cut: recortar clip (con filtros opcionales) y subir a Supabase
app.post("/cut", async (req, res) => {
  try {
    const {
      video_url,
      start_time,
      end_time,
      filters = { format: "original", captions_url: null, loudnorm: true },
      output = {
        container: "mp4",
        video_codec: "libx264",
        audio_codec: "aac",
        crf: 23,
        preset: "veryfast",
        faststart: true,
      },
    } = req.body || {};

    if (
      !video_url ||
      typeof start_time !== "number" ||
      typeof end_time !== "number"
    ) {
      return res
        .status(400)
        .json({ ok: false, error: "video_url, start_time, end_time are required (numbers)" });
    }
    if (end_time <= start_time) {
      return res.status(400).json({ ok: false, error: "end_time must be > start_time" });
    }

    const tmpVid = await downloadToTemp(video_url, ".mp4");
    const id = uuidv4();
    const outPath = `/tmp/clip_${id}.mp4`;
    const thumbPath = `/tmp/thumb_${id}.jpg`;

    // --- Construcción de filtros de video ---
    // vertical_9_16: blur de fondo + foreground centrado (1080x1920)
    // Si hay subtítulos, se aplican al foreground antes del overlay
    let vfArgs = "";         // usaremos -vf
    let fcArgs = "";         // o -filter_complex cuando haga falta

    const safeSubs = (p) =>
      (p || "").replace(/'/g, "\\'").replace(/:/g, "\\:"); // escapar ':' y comillas simples

    if (filters.format === "vertical_9_16") {
      const subChain = filters.captions_url
        ? `,subtitles='${safeSubs(filters.captions_url)}'`
        : "";
      fcArgs =
        `-filter_complex ` +
        `"[0:v]scale=-2:1920,boxblur=luma_radius=20:luma_power=1[bg];` +
        `[0:v]scale=-2:1080${subChain}[fg];` +
        `[bg][fg]overlay=(W-w)/2:(H-h)/2"`;
    } else if (filters.format === "square_1_1") {
      // 1080x1080 con padding
      vfArgs = `-vf "scale=1080:-2,pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black"`;
      if (filters.captions_url) {
        vfArgs = `-vf "scale=1080:-2,pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black,subtitles='${safeSubs(
          filters.captions_url
        )}'"`;
      }
    } else {
      // original; solo subtítulos si aplica
      if (filters.captions_url) {
        vfArgs = `-vf "subtitles='${safeSubs(filters.captions_url)}'"`;
      }
    }

    // Audio filter
    const afArgs = filters.loudnorm
      ? `-af "loudnorm=I=-16:TP=-1.5:LRA=11"`
      : "";

    // Codecs / contenedor
    const vCodec = output.video_codec || "libx264";
    const aCodec = output.audio_codec || "aac";
    const crf = Number(output.crf ?? 23);
    const preset = output.preset || "veryfast";
    const faststart = output.faststart !== false ? "-movflags +faststart" : "";
    const codecs = `-c:v ${vCodec} -preset ${preset} -crf ${crf} -c:a ${aCodec} ${faststart}`;

    // Recorte + filtros
    const cutCmd =
      `ffmpeg -hide_banner -y -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" ` +
      `${fcArgs || vfArgs || ""} ${afArgs} ${codecs} "${outPath}"`;

    await execAsync(cutCmd);

    // Thumbnail
    await execAsync(
      `ffmpeg -hide_banner -y -ss ${start_time} -i "${tmpVid.name}" -frames:v 1 "${thumbPath}"`
    );

    const clipUrl = await uploadToSupabase(outPath, `clips/${id}.mp4`, "video/mp4");
    const thumbUrl = await uploadToSupabase(thumbPath, `clips/${id}.jpg`, "image/jpeg");

    const size_bytes = fs.statSync(outPath).size;
    const duration = end_time - start_time;

    // Limpieza
    fs.existsSync(outPath) && fs.unlinkSync(outPath);
    fs.existsSync(thumbPath) && fs.unlinkSync(thumbPath);
    tmpVid.removeCallback();

    return res.json({
      ok: true,
      clip: { url: clipUrl, thumbnail_url: thumbUrl, duration, size_bytes },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/", (_req, res) => res.send("video-svc up"));
app.listen(process.env.PORT || 3000, () => console.log("svc listening"));
