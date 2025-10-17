import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import tmp from "tmp";
import { v4 as uuidv4 } from "uuid";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ---- Supabase (Railway Variables) ----
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "video-results";
const SIGNED_URL_EXPIRES = Number(process.env.SIGNED_URL_EXPIRES || 604800);
const supabase = (SUPABASE_URL && SUPABASE_KEY) ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ---------- Helpers ----------
function execAsync(cmd, maxBuffer = 1024 * 1024 * 200) {
  return new Promise((resolve, reject) => {
    exec(cmd, { maxBuffer }, (err, stdout, stderr) => {
      if (err) {
        const msg = `CMD:\n${cmd}\n\nSTDERR:\n${stderr}\n\nSTDOUT:\n${stdout}\n`;
        return reject(new Error(msg));
      }
      resolve({ stdout, stderr });
    });
  });
}

async function uploadToSupabase(localPath, destKey, contentType) {
  if (!supabase) throw new Error("Supabase not configured");
  const fileBuffer = fs.readFileSync(localPath);

  const { error: upErr } = await supabase
    .storage
    .from(SUPABASE_BUCKET)
    .upload(destKey, fileBuffer, {
      contentType: contentType || "application/octet-stream",
      upsert: true,
    });
  if (upErr) throw upErr;

  // Público si aplica
  const { data: pub } = supabase.storage.from(SUPABASE_BUCKET).getPublicUrl(destKey);
  if (pub?.publicUrl && !pub.publicUrl.includes("null")) return pub.publicUrl;

  // Si es privado, signed URL
  const { data: signed, error: signErr } =
    await supabase.storage.from(SUPABASE_BUCKET).createSignedUrl(destKey, SIGNED_URL_EXPIRES);
  if (signErr) throw signErr;
  return signed.signedUrl;
}

// --- Parseo robusto de URL de Supabase Storage ---
function parseSupabaseStorageUrl(url) {
  try {
    const u = new URL(url);
    // Debe contener "/storage/v1/object/"
    if (!u.pathname.includes("/storage/v1/object/")) return null;

    const parts = u.pathname.split("/").filter(Boolean); // e.g. ["storage","v1","object","sign","videos","folder","file.mp4"]
    const iObject = parts.findIndex(p => p === "object");
    if (iObject === -1) return null;

    const modeOrBucket = parts[iObject + 1]; // "public" | "sign" | "<bucket>"
    let bucket, startIdx;
    if (modeOrBucket === "public" || modeOrBucket === "sign") {
      bucket = parts[iObject + 2];
      startIdx = iObject + 3;
    } else {
      bucket = modeOrBucket;
      startIdx = iObject + 2;
    }
    if (!bucket) return null;
    const path = decodeURIComponent(parts.slice(startIdx).join("/"));
    if (!path) return null;

    return { bucket, path };
  } catch {
    return null;
  }
}

// --- Descarga HTTP genérica ---
async function downloadHttpToTemp(url, postfix = ".mp4") {
  const f = tmp.fileSync({ postfix });
  const writer = fs.createWriteStream(f.name);
  // Asegura URL escapada
  const safeUrl = url.replace(/\s/g, "%20");
  const resp = await axios.get(safeUrl, { responseType: "stream" });
  await new Promise((ok, bad) => {
    resp.data.pipe(writer).on("finish", ok).on("error", bad);
  });
  return f;
}

// --- Descarga desde Supabase por SDK (sin token) ---
async function downloadSupabaseToTemp(bucket, path, postfix = ".mp4") {
  if (!supabase) throw new Error("Supabase not configured");
  const { data, error } = await supabase.storage.from(bucket).download(path);
  if (error) throw error;
  const f = tmp.fileSync({ postfix });
  fs.writeFileSync(f.name, Buffer.from(await data.arrayBuffer()));
  return f;
}

/**
 * Descarga universal:
 *  - Si recibes { source: { bucket, path } }, usa SDK directo.
 *  - Si recibes video_url de Supabase (sign/public/object), usa SDK.
 *  - Si no, baja por HTTP normal.
 */
async function downloadToTempSmart({ video_url, source }, postfix = ".mp4") {
  if (source?.bucket && source?.path) {
    return downloadSupabaseToTemp(source.bucket, source.path, postfix);
  }
  if (video_url) {
    const parsed = parseSupabaseStorageUrl(video_url);
    if (parsed && supabase) {
      return downloadSupabaseToTemp(parsed.bucket, parsed.path, postfix);
    }
    return downloadHttpToTemp(video_url, postfix);
  }
  throw new Error("Provide either { video_url } or { source: { bucket, path } }");
}

// ---------- Endpoints ----------
app.post("/extract-audio", async (req, res) => {
  try {
    const { video_url, source } = req.body || {};
    if (!video_url && !source) {
      return res.status(400).json({ ok: false, error: "video_url OR source{bucket,path} required" });
    }

    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");
    const tmpWav = tmp.fileSync({ postfix: ".wav" });

    await execAsync(`ffmpeg -hide_banner -y -i "${tmpVid.name}" -vn -ac 1 -ar 16000 "${tmpWav.name}"`);
    const url = await uploadToSupabase(tmpWav.name, `audio/${uuidv4()}_16k.wav`, "audio/wav");

    tmpVid.removeCallback(); tmpWav.removeCallback?.();
    return res.json({ ok: true, audio_url: url });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/astats", async (req, res) => {
  try {
    const {
      video_url, source,
      max_seconds = 1200, // 20 min
      percentile = 0.60,
      base_db = -35,
      pad = 1.2,
      merge_gap = 1.0,
    } = req.body || {};

    if (!video_url && !source) {
      return res.status(400).json({ ok: false, error: "video_url OR source{bucket,path} required" });
    }

    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");

    let stdout = "", method = "astats_ametadata";
    const cmd = `ffmpeg -hide_banner -i "${tmpVid.name}" -vn -af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" -f null - 2>&1`;
    try {
      ({ stdout } = await execAsync(cmd));
    } catch {
      method = "silencedetect_fallback";
      const cmd2 = `ffmpeg -hide_banner -i "${tmpVid.name}" -af "silencedetect=noise=${base_db}dB:d=0.2" -f null - 2>&1`;
      ({ stdout } = await execAsync(cmd2));
    }

    const MAX_T = Number(max_seconds) > 0 ? Number(max_seconds) : Infinity;

    let pts = [];
    if (method === "astats_ametadata") {
      const rePts = /pts_time:(\d+(?:\.\d+)?)/g;
      const times = [];
      let m;
      while ((m = rePts.exec(stdout)) !== null) {
        const t = parseFloat(m[1]);
        if (t <= MAX_T) times.push(t);
      }

      const reRms = /key:lavfi\.astats\.Overall\.RMS_level\s+value:([-\d.]+)/g;
      const rms = [];
      while ((m = reRms.exec(stdout)) !== null) {
        rms.push(parseFloat(m[1]));
      }

      const N = Math.min(times.length, rms.length);
      for (let i = 0; i < N; i++) pts.push({ t: times[i], rms: rms[i] });

      if (pts.length === 0 && rms.length > 0) {
        const step = 0.5;
        for (let i = 0; i < rms.length; i++) {
          const t = i * step;
          if (t <= MAX_T) pts.push({ t, rms: rms[i] });
        }
      }
    } else {
      // Fallback: devolvemos "no silencios" sobre [0..MAX_T]
      const noise = [];
      const re = /silence_(start|end):\s*([-\d.]+)/g;
      let m;
      while ((m = re.exec(stdout)) !== null) noise.push({ k: m[1], v: parseFloat(m[2]) });

      let last = 0, ranges = [];
      for (let i = 0; i < noise.length; i++) {
        if (noise[i].k === "start") {
          const end = Math.min(noise[i].v, MAX_T);
          if (end > last) ranges.push({ start: last, end });
        } else if (noise[i].k === "end") {
          last = Math.min(noise[i].v, MAX_T);
        }
      }
      if (last < MAX_T) ranges.push({ start: last, end: MAX_T });

      tmpVid.removeCallback();
      return res.json({
        ok: true,
        threshold: base_db,
        ranges,
        limited: isFinite(MAX_T),
        points: 0,
        method,
      });
    }

    if (!pts.length) {
      tmpVid.removeCallback();
      return res.json({
        ok: true,
        threshold: base_db,
        ranges: [],
        limited: isFinite(MAX_T),
        points: 0,
        note: "No se detectaron líneas de RMS en el log.",
      });
    }

    const sorted = [...pts].map(p => p.rms).sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * percentile)));
    const pctl = sorted[idx];
    const TH = Math.max(pctl, base_db);

    const PAD = Number(pad) || 1.2;
    const MERGE = Number(merge_gap) || 1.0;
    let raw = [], cur = null;
    for (const p of pts) {
      if (p.t > MAX_T) break;
      if (p.rms >= TH) {
        cur ? cur.end = p.t : cur = { start: p.t, end: p.t };
      } else if (cur) {
        raw.push(cur);
        cur = null;
      }
    }
    if (cur) raw.push(cur);

    let ranges = raw.map(r => ({ start: Math.max(0, r.start - PAD), end: r.end + PAD }))
                    .sort((a, b) => a.start - b.start);

    let merged = [];
    for (const r of ranges) {
      const last = merged[merged.length - 1];
      if (!last || r.start - last.end > MERGE) merged.push({ ...r });
      else last.end = Math.max(last.end, r.end);
    }

    tmpVid.removeCallback();
    return res.json({
      ok: true,
      threshold: TH,
      ranges: merged,
      limited: isFinite(MAX_T),
      points: pts.length,
      method,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/cut", async (req, res) => {
  try {
    const {
      video_url, source,
      start_time, end_time,
      filters = { format: "original", captions_url: null, loudnorm: true },
      output = { container: "mp4", video_codec: "libx264", audio_codec: "aac", crf: 23, preset: "veryfast", faststart: true }
    } = req.body || {};

    if ((!video_url && !source) || typeof start_time !== "number" || typeof end_time !== "number") {
      return res.status(400).json({ ok: false, error: "Provide video_url OR source{bucket,path}, and numeric start_time/end_time" });
    }
    if (end_time <= start_time) {
      return res.status(400).json({ ok: false, error: "end_time must be > start_time" });
    }

    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");

    const id = uuidv4();
    const out = `/tmp/clip_${id}.mp4`;
    const thumb = `/tmp/thumb_${id}.jpg`;

    let vfParts = [];
    if (filters.format === "vertical_9_16") {
      vfParts.push("[0:v]scale=-2:1920,boxblur=luma_radius=20:luma_power=1[bg];[0:v]scale=-2:1080[fg];[bg][fg]overlay=(W-w)/2:(H-h)/2");
    } else if (filters.format === "square_1_1") {
      vfParts.push("scale=1080:-2, pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black");
    }
    if (filters.captions_url) {
      const safeSubs = String(filters.captions_url).replace(/:/g, "\\:");
      vfParts.push(`subtitles='${safeSubs}'`);
    }
    const vf = vfParts.length ? `-vf "${vfParts.join(',')}"` : "";
    const af = filters.loudnorm ? `-af "loudnorm=I=-16:TP=-1.5:LRA=11"` : "";
    const codecs = `-c:v ${output.video_codec||"libx264"} -preset ${output.preset||"veryfast"} -crf ${output.crf||23} -c:a ${output.audio_codec||"aac"} ${output.faststart!==false? "-movflags +faststart" : ""}`;

    await execAsync(`ffmpeg -hide_banner -y -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" ${vf} ${af} ${codecs} "${out}"`);
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
