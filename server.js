import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import tmp from "tmp";
import { v4 as uuidv4 } from "uuid";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json({ limit: "10mb" }));

// ========= ENV / SUPABASE =========
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // Service Role
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || "videos"; // <-- se mantiene "videos"
const SIGNED_URL_EXPIRES = Number(process.env.SIGNED_URL_EXPIRES || 60 * 60 * 12); // 12h
const supabase =
  SUPABASE_URL && SUPABASE_KEY ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ========= UTIL: LOG & ERRORES =========
function reqId() {
  return Math.random().toString(36).slice(2, 10);
}
function normalizeErr(e) {
  if (!e) return { name: "Error", message: "Unknown error" };
  const out = {
    name: e.name || "Error",
    message: e.message || "No message",
  };
  if (e.stack) out.stack = String(e.stack).split("\n").slice(0, 6).join("\n");
  if (e.response && e.response.data) out.inner = e.response.data; // axios
  if (e.error) out.inner = e.error; // libs varias
  if (e.code) out.code = e.code;
  return out;
}
function log(...args) {
  console.log(new Date().toISOString(), ...args);
}
function logReq(req, extra = {}) {
  const info = {
    id: req._id,
    method: req.method,
    path: req.path,
    query: req.query,
    hasBody: !!req.body,
    ...extra,
  };
  log("REQ", JSON.stringify(info));
}

app.use((req, _res, next) => {
  req._id = req._id || req.headers["x-request-id"] || reqId();
  next();
});

// ========= SHELL =========
function execAsync(cmd, maxBuffer = 1024 * 1024 * 300) {
  return new Promise((resolve, reject) => {
    exec(cmd, { maxBuffer }, (err, stdout, stderr) => {
      if (err) {
        return reject(
          new Error(
            JSON.stringify({
              cmd,
              stderr: String(stderr || "").slice(0, 8000),
              stdout: String(stdout || "").slice(0, 8000),
            })
          )
        );
      }
      resolve({ stdout, stderr });
    });
  });
}

// ========= SUPABASE UPLOAD =========
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

  // 1) Intento URL pública
  const { data: pub } = supabase.storage.from(SUPABASE_BUCKET).getPublicUrl(destKey);
  if (pub?.publicUrl && !pub.publicUrl.includes("null")) return pub.publicUrl;

  // 2) URL firmada
  const { data: signed, error: signErr } = await supabase.storage
    .from(SUPABASE_BUCKET)
    .createSignedUrl(destKey, SIGNED_URL_EXPIRES);
  if (signErr) throw signErr;
  return signed.signedUrl;
}

// ========= DOWNLOADS =========
async function downloadHttpToTemp(url, postfix = ".mp4") {
  const f = tmp.fileSync({ postfix });
  const writer = fs.createWriteStream(f.name);
  // Espacios a %20; dejamos el token tal cual.
  const safeUrl = url.replace(/\s/g, "%20");
  const resp = await axios.get(safeUrl, { responseType: "stream" });
  await new Promise((ok, bad) => {
    resp.data.pipe(writer).on("finish", ok).on("error", bad);
  });
  return f;
}

async function downloadSupabaseToTemp(bucket, path, postfix = ".mp4") {
  if (!supabase) throw new Error("Supabase not configured");

  // Validación ligera de existencia
  const dir = path.split("/").slice(0, -1).join("/") || "";
  const file = path.split("/").pop();
  const { data: meta, error: statErr } = await supabase.storage
    .from(bucket)
    .list(dir, { search: file });
  if (statErr) throw statErr;
  const found = Array.isArray(meta) && meta.some((x) => x.name === file);
  if (!found) {
    const e = new Error(`File not found in Supabase: bucket=${bucket}, path=${path}`);
    e.code = "SB_FILE_NOT_FOUND";
    throw e;
  }

  const { data, error } = await supabase.storage.from(bucket).download(path);
  if (error) throw error;
  const f = tmp.fileSync({ postfix });
  const arrBuf = await data.arrayBuffer();
  fs.writeFileSync(f.name, Buffer.from(arrBuf));
  return f;
}

/**
 * Descarga universal:
 *  - Si llega { source: { bucket, path } } => usa SDK (mismo proyecto).
 *  - Si llega video_url (firmada o pública) => SIEMPRE descarga por HTTP (NO SDK).
 */
async function downloadToTempSmart({ video_url, source }, postfix = ".mp4") {
  if (source?.bucket && source?.path) {
    return downloadSupabaseToTemp(source.bucket, source.path, postfix);
  }
  if (video_url) {
    // ¡IMPORTANTE! Tratar SIEMPRE video_url como HTTP (evita "signature verification failed").
    return downloadHttpToTemp(video_url, postfix);
  }
  throw new Error("Provide either { video_url } or { source: { bucket, path } }");
}

// ========= HELPERS AUDIO =========
async function hasAudioStream(filePath) {
  try {
    const { stdout, stderr } = await execAsync(
      `ffprobe -v error -select_streams a -show_entries stream=index -of csv=p=0 "${filePath}"`
    );
    const out = `${stdout}\n${stderr}`.trim();
    return out.length > 0; // si devuelve algo, hay al menos un stream de audio
  } catch {
    return false;
  }
}

// ========= ENDPOINTS =========
app.get("/", (_req, res) => res.send("video-svc up"));
app.post("/echo", (req, res) => res.json({ ok: true, echo: req.body || null }));

// --- EXTRAER WAV 16k ---
app.post("/extract-audio", async (req, res) => {
  const reqInfo = { where: "extract-audio", reqId: req._id };
  try {
    logReq(req, reqInfo);
    const { video_url, source } = req.body || {};
    if (!video_url && !source) {
      return res
        .status(400)
        .json({ ok: false, ...reqInfo, error: "video_url OR source{bucket,path} required" });
    }

    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");
    const hasAudio = await hasAudioStream(tmpVid.name);
    if (!hasAudio) {
      tmpVid.removeCallback();
      return res.json({
        ok: true,
        ...reqInfo,
        note: "El archivo no contiene pista de audio.",
        audio_url: null,
      });
    }

    // Cambiado a M4A (AAC) para reducir tamaño y compatibilidad
    const tmpM4a = tmp.fileSync({ postfix: ".m4a" });
    await execAsync(
      `ffmpeg -hide_banner -loglevel info -y -i "${tmpVid.name}" -map a:0 -vn -ac 1 -ar 16000 -b:a 64k "${tmpM4a.name}"`
    );
    const url = await uploadToSupabase(tmpM4a.name, `audio/${uuidv4()}_16k.m4a`, "audio/mp4");

    tmpVid.removeCallback();
    tmpM4a.removeCallback?.();
    return res.json({ ok: true, ...reqInfo, audio_url: url, format: "m4a", sample_rate: 16000 });
  } catch (e) {
    const err = normalizeErr(e);
    log("ERR", reqInfo, err);
    return res.status(500).json({
      ok: false,
      ...reqInfo,
      error: err,
      hint: "Verifica bucket/path o URL firmada y permisos.",
    });
  }
});

// --- NUEVO: EXTRAER AUDIO EN CHUNKS (< ~25MB c/u) ---
app.post("/extract-audio-chunks", async (req, res) => {
  const reqInfo = { where: "extract-audio-chunks", reqId: req._id };
  try {
    logReq(req, reqInfo);
    const {
      video_url,
      source,
      // Opcionales para control fino:
      target_mb = 24,              // tamaño objetivo por chunk
      audio_bitrate_kbps = 48,     // bitrate del audio AAC
      segment_seconds,             // si lo envías, se usa tal cual
    } = req.body || {};

    if (!video_url && !source) {
      return res
        .status(400)
        .json({ ok: false, ...reqInfo, error: "video_url OR source{bucket,path} required" });
    }

    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");
    const hasAudio = await hasAudioStream(tmpVid.name);
    if (!hasAudio) {
      tmpVid.removeCallback();
      return res.json({
        ok: true,
        ...reqInfo,
        note: "El archivo no contiene pista de audio.",
        chunks: [],
      });
    }

    // Cálculo automático de duración de chunk si no se envía segment_seconds:
    // size(MB) ≈ bitrate(kbps) * seconds / (8*1000)
    // seconds ≈ size(MB) * 8*1000 / bitrate(kbps)
    const autoSegSeconds = Math.max(
      60,
      Math.floor((Number(target_mb) * 8000) / Number(audio_bitrate_kbps))
    );
    const seg = Number(segment_seconds) > 0 ? Number(segment_seconds) : autoSegSeconds;

    const id = uuidv4();
    const workdir = `/tmp/chunks_${id}`;
    fs.mkdirSync(workdir, { recursive: true });
    const pattern = `${workdir}/part_%03d.m4a`;

    // Transcodifica + segmenta directo a M4A
    // -reset_timestamps 1 ayuda a evitar problemas de pts
    const cmd = [
      `ffmpeg -hide_banner -loglevel info -y`,
      `-i "${tmpVid.name}"`,
      `-map a:0 -vn -ac 1 -ar 16000 -b:a ${audio_bitrate_kbps}k`,
      `-f segment -segment_time ${seg} -reset_timestamps 1 -movflags +faststart`,
      `"${pattern}"`
    ].join(" ");
    await execAsync(cmd);

    // Subir todos los .m4a generados en orden
    const files = fs
      .readdirSync(workdir)
      .filter((f) => f.endsWith(".m4a"))
      .sort(); // part_000.m4a, part_001.m4a, ...

    const baseKey = `audio/chunks/${id}`;
    const chunks = [];
    for (const fname of files) {
      const full = `${workdir}/${fname}`;
      const key = `${baseKey}/${fname}`;
      const url = await uploadToSupabase(full, key, "audio/mp4");
      const size_bytes = fs.statSync(full).size;
      chunks.push({ url, path: key, size_bytes });
    }

    // Limpieza
    for (const fname of files) {
      const full = `${workdir}/${fname}`;
      fs.existsSync(full) && fs.unlinkSync(full);
    }
    fs.existsSync(workdir) && fs.rmdirSync(workdir);
    tmpVid.removeCallback();

    return res.json({
      ok: true,
      ...reqInfo,
      format: "m4a",
      sample_rate: 16000,
      bitrate_kbps: Number(audio_bitrate_kbps),
      segment_seconds: seg,
      count: chunks.length,
      chunks,
    });
  } catch (e) {
    const err = normalizeErr(e);
    log("ERR", reqInfo, err);
    return res.status(500).json({
      ok: false,
      ...reqInfo,
      error: err,
      hint:
        "Envía video_url firmada válida o source.bucket/path. Puedes ajustar target_mb, audio_bitrate_kbps o segment_seconds.",
    });
  }
});

// --- ASTATS (detección de energía / risas) ---
app.post("/astats", async (req, res) => {
  const reqInfo = { where: "astats", reqId: req._id };
  try {
    logReq(req, reqInfo);
    const {
      video_url,
      source,
      max_seconds = 1200, // 20 min
      percentile = 0.6,
      base_db = -35,
      pad = 1.2,
      merge_gap = 1.0,
    } = req.body || {};

    if (!video_url && !source) {
      return res
        .status(400)
        .json({ ok: false, ...reqInfo, error: "video_url OR source{bucket,path} required" });
    }

    const maxSec = Math.max(1, Number(max_seconds) || 1200);
    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");

    // Confirmar que hay audio
    const hasAudio = await hasAudioStream(tmpVid.name);
    if (!hasAudio) {
      tmpVid.removeCallback();
      return res.json({
        ok: true,
        ...reqInfo,
        threshold: Number(base_db),
        ranges: [],
        limited: true,
        points: 0,
        note: "No se encontró pista de audio en el video.",
        method: "no_audio",
        mode: source?.bucket ? "sdk" : "http",
      });
    }

    // Filtro optimizado: downsample + mono + astats (RMS) en los primeros maxSec
    const astatsFilter = [
      "aresample=16000:resampler=soxr:precision=16",
      "aformat=channel_layouts=mono",
      "astats=metadata=1:reset=1",
      "ametadata=mode=print:key=lavfi.astats.Overall.RMS_level:file=-",
    ].join(",");

    let out = "";
    let method = "astats_ametadata";

    // Intento 1: mapeando a:0
    let cmd = `ffmpeg -hide_banner -loglevel warning -y -t ${maxSec} -i "${tmpVid.name}" -map a:0 -vn -af "${astatsFilter}" -f null -`;
    try {
      const { stdout, stderr } = await execAsync(cmd);
      out = `${stdout}\n${stderr}`;
    } catch {
      // Intento 2: sin -map a:0 (por si el índice difiere)
      try {
        cmd = `ffmpeg -hide_banner -loglevel warning -y -t ${maxSec} -i "${tmpVid.name}" -vn -af "${astatsFilter}" -f null -`;
        const { stdout, stderr } = await execAsync(cmd);
        out = `${stdout}\n${stderr}`;
      } catch {
        // Fallback: silencedetect
        method = "silencedetect_fallback";
        const sdFilter = [
          "aresample=16000:resampler=soxr:precision=16",
          "aformat=channel_layouts=mono",
          `silencedetect=noise=${base_db}dB:d=0.2`,
        ].join(",");
        const cmd2 = `ffmpeg -hide_banner -loglevel warning -y -t ${maxSec} -i "${tmpVid.name}" -vn -af "${sdFilter}" -f null -`;
        const { stdout: sdOut, stderr: sdErr } = await execAsync(cmd2);
        out = `${sdOut}\n${sdErr}`;
      }
    }

    const MAX_T = maxSec;

    // Parseo
    let pts = [];
    if (method === "astats_ametadata") {
      // 1) Tiempos: pts_time aparece como "pts_time:..." o "pts_time=..."
      const rePts = /pts_time[:=]\s*(\d+(?:\.\d+)?)/g;
      const times = [];
      let m;
      while ((m = rePts.exec(out)) !== null) {
        const t = parseFloat(m[1]);
        if (t <= MAX_T) times.push(t);
      }

      // 2) RMS en dos formatos:
      const rms = [];
      const reRmsA = /key:lavfi\.astats\.Overall\.RMS_level\s+value:([-\d.]+)/g;
      while ((m = reRmsA.exec(out)) !== null) rms.push(parseFloat(m[1]));
      const reRmsB = /lavfi\.astats\.Overall\.RMS_level\s*=\s*([-\d.]+)/g;
      while ((m = reRmsB.exec(out)) !== null) rms.push(parseFloat(m[1]));

      const N = Math.min(times.length || Infinity, rms.length);
      if (N && times.length) {
        for (let i = 0; i < N; i++) pts.push({ t: times[i], rms: rms[i] });
      } else if (rms.length) {
        const step = 0.5;
        for (let i = 0; i < rms.length; i++) {
          const t = i * step;
          if (t <= MAX_T) pts.push({ t, rms: rms[i] });
        }
      }
    } else {
      // Fallback: construir "no-silencio" desde silencedetect
      const noise = [];
      const re = /silence_(start|end):\s*([-\d.]+)/g;
      let m;
      while ((m = re.exec(out)) !== null) noise.push({ k: m[1], v: parseFloat(m[2]) });

      let last = 0,
        ranges = [];
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
        ...reqInfo,
        threshold: Number(base_db),
        ranges,
        limited: true,
        points: 0,
        method,
        mode: source?.bucket ? "sdk" : "http",
      });
    }

    if (!pts.length) {
      tmpVid.removeCallback();
      return res.json({
        ok: true,
        ...reqInfo,
        threshold: Number(base_db),
        ranges: [],
        limited: true,
        points: 0,
        note: "No se detectaron líneas de RMS en el log.",
        method,
        mode: source?.bucket ? "sdk" : "http",
      });
    }

    // Umbral por percentil vs base_db
    const sorted = [...pts].map((p) => p.rms).sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * percentile)));
    const pctl = sorted[idx];
    const TH = Math.max(Number(pctl), Number(base_db));

    // Generar, expandir y fusionar rangos
    const PAD = Number(pad) || 1.2;
    const MERGE = Number(merge_gap) || 1.0;
    let raw = [],
      cur = null;
    for (const p of pts) {
      if (p.t > MAX_T) break;
      if (p.rms >= TH) (cur ? (cur.end = p.t) : (cur = { start: p.t, end: p.t }));
      else if (cur) {
        raw.push(cur);
        cur = null;
      }
    }
    if (cur) raw.push(cur);

    let ranges = raw
      .map((r) => ({ start: Math.max(0, r.start - PAD), end: r.end + PAD }))
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
      ...reqInfo,
      threshold: Number(TH),
      ranges: merged,
      limited: true,
      points: pts.length,
      method,
      mode: source?.bucket ? "sdk" : "http",
    });
  } catch (e) {
    const err = normalizeErr(e);
    log("ERR", reqInfo, err);
    return res.status(500).json({
      ok: false,
      ...reqInfo,
      error: err,
      hint:
        "Si usas source.bucket/path verifica que existan; si usas URL firmada, prueba /echo para confirmar el payload.",
    });
  }
});

// --- CUT (recorte y subida a Supabase) ---
app.post("/cut", async (req, res) => {
  const reqInfo = { where: "cut", reqId: req._id };
  try {
    logReq(req, reqInfo);
    const {
      video_url,
      source,
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

    if ((!video_url && !source) || typeof start_time !== "number" || typeof end_time !== "number") {
      return res.status(400).json({
        ok: false,
        ...reqInfo,
        error: "Provide video_url OR source{bucket,path}, and numeric start_time/end_time",
      });
    }
    if (end_time <= start_time) {
      return res.status(400).json({ ok: false, ...reqInfo, error: "end_time must be > start_time" });
    }

    // Descarga
    const tmpVid = await downloadToTempSmart({ video_url, source }, ".mp4");
    if (source?.bucket && source?.path)
      log("CUT using Supabase SDK:", source.bucket, source.path);

    const id = uuidv4();
    const out = `/tmp/clip_${id}.mp4`;
    const thumb = `/tmp/thumb_${id}.jpg`;

    // Video filters
    let vfParts = [];
    if (filters.format === "vertical_9_16") {
      vfParts.push(
        "[0:v]scale=-2:1920,boxblur=luma_radius=20:luma_power=1[bg];[0:v]scale=-2:1080[fg];[bg][fg]overlay=(W-w)/2:(H-h)/2"
      );
    } else if (filters.format === "square_1_1") {
      vfParts.push("scale=1080:-2, pad=1080:1080:(ow-iw)/2:(oh-ih)/2:black");
    }
    if (filters.captions_url) {
      const safeSubs = String(filters.captions_url).replace(/:/g, "\\:");
      vfParts.push(`subtitles='${safeSubs}'`);
    }
    const hasVideoFilters = vfParts.length > 0;
    const vf = hasVideoFilters ? `-vf "${vfParts.join(",")}"` : "";

    // Audio filter (siempre opcional)
    const af = filters.loudnorm ? `-af "loudnorm=I=-16:TP=-1.5:LRA=11"` : "";

    // Construcción de codecs:
    // - Si NO hay filtros de video => copiar video (-c:v copy) y solo re-codificar audio
    // - Si hay filtros de video => recodificar video como antes
    const faststartFlag = output.faststart !== false ? "-movflags +faststart" : "";
    let codecs;
    if (!hasVideoFilters) {
      codecs = `-c:v copy -c:a ${output.audio_codec || "aac"} ${faststartFlag}`;
    } else {
      codecs = `-c:v ${output.video_codec || "libx264"} -preset ${output.preset || "veryfast"} -crf ${output.crf || 23} -c:a ${output.audio_codec || "aac"} ${faststartFlag}`;
    }

    // Flags comunes para robustez en contenedores chicos
    const common = `-hide_banner -loglevel info -y -nostdin -threads 1`;

    // 1) Corte principal (intenta stream copy si se puede)
    const cmdMain = hasVideoFilters
      ? `ffmpeg ${common} -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" ${vf} ${af} ${codecs} "${out}"`
      : `ffmpeg ${common} -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" -map 0:v:0 -map a:0 ${af} ${codecs} "${out}"`;

    try {
      await execAsync(cmdMain);
    } catch (_e) {
      // Fallback: si falló (por ejemplo, keyframes poco favorables en copy), recodifica video completo
      const fallbackCodecs = `-c:v ${output.video_codec || "libx264"} -preset ${output.preset || "veryfast"} -crf ${output.crf || 23} -c:a ${output.audio_codec || "aac"} ${faststartFlag}`;
      const cmdFallback = `ffmpeg ${common} -ss ${start_time} -to ${end_time} -i "${tmpVid.name}" ${vf} ${af} ${fallbackCodecs} "${out}"`;
      await execAsync(cmdFallback);
    }

    // Thumbnail igual que antes (un frame a la hora de inicio)
    await execAsync(
      `ffmpeg ${common} -ss ${start_time} -i "${tmpVid.name}" -frames:v 1 "${thumb}"`
    );

    const clipUrl = await uploadToSupabase(out, `clips/${id}.mp4`, "video/mp4");
    const thumbUrl = await uploadToSupabase(thumb, `clips/${id}.jpg`, "image/jpeg");

    const size_bytes = fs.statSync(out).size;
    const duration = end_time - start_time;

    fs.existsSync(out) && fs.unlinkSync(out);
    fs.existsSync(thumb) && fs.unlinkSync(thumb);
    tmpVid.removeCallback();

    return res.json({
      ok: true,
      ...reqInfo,
      clip: { url: clipUrl, thumbnail_url: thumbUrl, duration, size_bytes },
    });
  } catch (e) {
    const err = normalizeErr(e);
    log("ERR", reqInfo, err);
    return res.status(500).json({
      ok: false,
      ...reqInfo,
      error: err,
      hint:
        "Usa /echo para ver el body que llega. Verifica bucket/path exactos y permisos del Service Role.",
    });
  }
});

app.listen(process.env.PORT || 3000, () => console.log("svc listening"));
