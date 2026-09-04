'use strict';

const crypto = require('crypto');
const { spawn } = require('child_process');
const express = require('express');
const fs = require('fs');
const http = require('http');
const https = require('https');
const multer = require('multer');
const net = require('net');
const path = require('path');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const { WebSocketServer } = require('ws');

function loadDotEnv(file) {
  if (!fs.existsSync(file)) return;
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)\s*$/);
    if (!match || process.env[match[1]] !== undefined) continue;
    process.env[match[1]] = match[2].replace(/^['"]|['"]$/g, '');
  }
}

loadDotEnv(path.join(__dirname, '.env'));

function fallbackNodeToken() {
  try {
    const secrets = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data', 'secrets.json'), 'utf8'));
    return secrets.nodeSecret || 'change-me';
  } catch {
    return 'change-me';
  }
}

const PORT = Number(process.env.PORT || 52000);
const NODE_TOKEN = process.env.NODE_TOKEN || fallbackNodeToken();
const ROLES = new Set(String(process.env.NODE_ROLES || 'sync,storage').split(',').map((role) => role.trim()).filter(Boolean));
const STORAGE_ROOT = process.env.VIDEO_STORAGE_ROOT || '/video52000/videos';
const UPLOAD_TMP_DIR = path.join(STORAGE_ROOT, '.uploads-tmp');
const MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB || 2048);
const TLS_CERT_PATH = process.env.TLS_CERT_PATH || '';
const TLS_KEY_PATH = process.env.TLS_KEY_PATH || '';
const SYNC_STATE_FILE = process.env.SYNC_STATE_FILE || path.join(__dirname, 'rooms.json');
const WS_HEARTBEAT_MS = 30000;
const WS_MAX_PAYLOAD = 256 * 1024;
const ROOM_EMPTY_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const ENDED_DEDUPE_WINDOW_MS = 2500;
const CHAT_BURST = 5;
const CHAT_REFILL_PER_SECOND = 1;
const TEXT_LIMITS = { title: 160, artist: 120, album: 120, sourceName: 180, coverUrl: 3000 };

// ---- Emby proxy config -----------------------------------------------------------------------
// The main site mints an encrypted grant with a key derived from the shared NODE_TOKEN; this node
// opens it to proxy an Emby stream so browsers never see the Emby token or address.
const EMBY_GRANT_KEY = crypto.createHash('sha256').update(NODE_TOKEN).digest();
const EMBY_CLIENT_NAME = 'PlayTogether';
const EMBY_CLIENT_VERSION = '1.0.0';
const EMBY_MAX_BITRATE = 40000000;
const EMBY_AUTH_QUERY_KEYS = new Set(['api_key', 'apikey', 'x-emby-token', 'x-mediabrowser-token', 'x-emby-authorization', 'authorization']);
const EMBY_SESSION_TTL_MS = 6 * 60 * 60 * 1000;
const EMBY_SEGMENT_TTL_MS = 120 * 1000;
const EMBY_SEGMENT_CACHE_MAX = 96 * 1024 * 1024;
const EMBY_PLAYLIST_TTL_MS = 1000;
const EMBY_DEVICE_PROFILE = {
  MaxStreamingBitrate: EMBY_MAX_BITRATE,
  MaxStaticBitrate: EMBY_MAX_BITRATE,
  DirectPlayProfiles: [
    { Container: 'mp4,m4v,mov', Type: 'Video', VideoCodec: 'h264,av1', AudioCodec: 'aac,mp3,opus,flac,ac3' },
    { Container: 'webm', Type: 'Video', VideoCodec: 'vp8,vp9,av1', AudioCodec: 'opus,vorbis' }
  ],
  TranscodingProfiles: [
    {
      Container: 'ts', Type: 'Video', Protocol: 'hls', VideoCodec: 'h264', AudioCodec: 'aac,mp3',
      Context: 'Streaming', MaxAudioChannels: '2', MinSegments: '1', BreakOnNonKeyFrames: true
    }
  ],
  SubtitleProfiles: [
    { Format: 'vtt', Method: 'External' },
    { Format: 'srt', Method: 'External' },
    { Format: 'subrip', Method: 'External' },
    { Format: 'ass', Method: 'External' },
    { Format: 'ssa', Method: 'External' },
    { Format: 'pgssub', Method: 'Encode' },
    { Format: 'dvdsub', Method: 'Encode' }
  ]
};
const embyHlsSessions = new Map();
const embySegmentCache = new Map();
const embyPlaylistCache = new Map();
const embyInflight = new Map();
let embySegmentBytes = 0;

fs.mkdirSync(STORAGE_ROOT, { recursive: true });
fs.mkdirSync(UPLOAD_TMP_DIR, { recursive: true });

const app = express();
const tlsEnabled = Boolean(TLS_CERT_PATH && TLS_KEY_PATH);
const downloadTasks = new Map();
const server = tlsEnabled
  ? https.createServer({
    cert: fs.readFileSync(TLS_CERT_PATH),
    key: fs.readFileSync(TLS_KEY_PATH)
  }, app)
  : http.createServer(app);
// Uploads stream to disk next to their destination so the final step is a rename, not a 2 GB buffer.
const upload = multer({
  storage: multer.diskStorage({
    destination: UPLOAD_TMP_DIR,
    filename: (req, file, cb) => cb(null, `upload_${crypto.randomBytes(10).toString('hex')}`)
  }),
  defParamCharset: 'utf8',
  limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024, files: 1 }
});

function now() {
  return new Date().toISOString();
}

async function moveFile(from, to) {
  try {
    await fs.promises.rename(from, to);
  } catch (error) {
    if (error.code !== 'EXDEV') throw error;
    await fs.promises.copyFile(from, to);
    await fs.promises.unlink(from);
  }
}

async function discardUpload(file) {
  if (file?.path) await fs.promises.unlink(file.path).catch(() => {});
}

async function pruneUploadTmpFiles() {
  const names = await fs.promises.readdir(UPLOAD_TMP_DIR).catch(() => []);
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const name of names) {
    const file = path.join(UPLOAD_TMP_DIR, name);
    const stat = await fs.promises.stat(file).catch(() => null);
    if (stat?.isFile() && stat.mtimeMs < cutoff) await fs.promises.unlink(file).catch(() => {});
  }
}

function isHiddenEntry(name) {
  return String(name || '').startsWith('.');
}

function cleanRelativePath(input) {
  const normalized = path.posix.normalize(`/${String(input || '').replace(/\\/g, '/')}`).replace(/^\/+/, '');
  if (!normalized || normalized === '.') return '';
  if (normalized.startsWith('../') || normalized === '..') {
    const err = new Error('路径不合法。');
    err.status = 400;
    throw err;
  }
  return normalized;
}

function fullStoragePath(root, rel) {
  const base = path.resolve(root);
  const full = path.resolve(base, rel);
  if (full !== base && !full.startsWith(`${base}${path.sep}`)) {
    const err = new Error('路径越界。');
    err.status = 400;
    throw err;
  }
  return full;
}

function encodePathForUrl(rel) {
  return cleanRelativePath(rel).split('/').filter(Boolean).map(encodeURIComponent).join('/');
}

function isVideoFile(name) {
  return /\.(mp4|webm|ogg|mov|m4v|mkv|m3u8)$/i.test(name);
}

function isAudioFile(name) {
  return /\.(mp3|m4a|aac|wav|flac|ogg|opus|weba)$/i.test(name);
}

function mediaTypeForFile(name) {
  if (isAudioFile(name)) return 'audio';
  if (isVideoFile(name)) return 'video';
  return '';
}

function randomId(prefix) {
  return `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
}

function normalizeDownloadUrl(value) {
  const text = String(value || '').trim();
  if (/^magnet:\?xt=/i.test(text)) return text.slice(0, 4096);
  let parsed;
  try {
    parsed = new URL(text);
  } catch {
    const err = new Error('下载地址必须是 http(s) 直链或 magnet 磁力链接。');
    err.status = 400;
    throw err;
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    const err = new Error('下载地址必须是 http(s) 直链或 magnet 磁力链接。');
    err.status = 400;
    throw err;
  }
  return parsed.toString();
}

function safeFilename(value, fallback = 'download.bin') {
  const name = path.basename(String(value || '').trim()).replace(/[<>:"/\\|?*\x00-\x1F]/g, '_').slice(0, 180);
  return name && name !== '.' && name !== '..' ? name : fallback;
}

function filenameFromUrl(url) {
  try {
    const parsed = new URL(url);
    return safeFilename(decodeURIComponent(path.basename(parsed.pathname || '')), 'download.bin');
  } catch {
    return 'download.bin';
  }
}

function filenameFromDisposition(value) {
  const match = String(value || '').match(/filename\*?=(?:UTF-8''|")?([^";]+)/i);
  if (!match) return '';
  try {
    return safeFilename(decodeURIComponent(match[1].replace(/^"|"$/g, '')));
  } catch {
    return safeFilename(match[1].replace(/^"|"$/g, ''));
  }
}

function parseByteUnit(value) {
  const match = String(value || '').trim().match(/^([\d.]+)\s*([kmgt]?i?b)?(?:\/s)?$/i);
  if (!match) return 0;
  const amount = Number(match[1]);
  if (!Number.isFinite(amount)) return 0;
  const unit = (match[2] || 'B').toUpperCase();
  const powers = { B: 0, KB: 1, KIB: 1, MB: 2, MIB: 2, GB: 3, GIB: 3, TB: 4, TIB: 4 };
  return Math.round(amount * (1024 ** (powers[unit] ?? 0)));
}

function updateDownloadBytes(task, downloadedBytes) {
  const nextBytes = Math.max(0, Number(downloadedBytes) || 0);
  const nowMs = Date.now();
  const previousBytes = Number(task.speedSampleBytes || 0);
  const previousAt = Number(task.speedSampleAt || nowMs);
  task.downloadedBytes = nextBytes;
  if (previousAt && nowMs > previousAt && nextBytes >= previousBytes) {
    const seconds = (nowMs - previousAt) / 1000;
    if (seconds >= 0.5) {
      task.speedBytesPerSecond = Math.round((nextBytes - previousBytes) / seconds);
      task.speedSampleBytes = nextBytes;
      task.speedSampleAt = nowMs;
    }
  } else {
    task.speedSampleBytes = nextBytes;
    task.speedSampleAt = nowMs;
  }
  if (task.size) {
    task.progressPercent = Math.max(0, Math.min(100, (nextBytes / task.size) * 100));
    if (task.speedBytesPerSecond > 0) {
      task.etaSeconds = Math.max(0, Math.round((task.size - nextBytes) / task.speedBytesPerSecond));
    }
  }
}

function parseAria2Progress(task, text) {
  const line = String(text || '');
  const progress = line.match(/([\d.]+\s*[KMGT]?i?B|\d+\s*B)\s*\/\s*([\d.]+\s*[KMGT]?i?B|\d+\s*B)\(([\d.]+)%\)/i);
  if (progress) {
    const downloaded = parseByteUnit(progress[1]);
    const total = parseByteUnit(progress[2]);
    if (total) task.size = total;
    if (downloaded) updateDownloadBytes(task, downloaded);
    task.progressPercent = Math.max(0, Math.min(100, Number(progress[3]) || task.progressPercent || 0));
  }
  const speed = line.match(/\bDL:([^\s\]]+)/i);
  if (speed) task.speedBytesPerSecond = parseByteUnit(speed[1]);
  const eta = line.match(/\bETA:([^\s\]]+)/i);
  if (eta) task.eta = eta[1];
}

function publicDownloadTask(task) {
  const { child, speedSampleBytes, speedSampleAt, ...rest } = task;
  if (rest.size && rest.downloadedBytes !== null && rest.downloadedBytes !== undefined) {
    rest.progressPercent = Math.max(0, Math.min(100, (Number(rest.downloadedBytes || 0) / Number(rest.size)) * 100));
  }
  return rest;
}

function appendDownloadLog(task, line) {
  const text = String(line || '').trim();
  if (!text) return;
  parseAria2Progress(task, text);
  task.logs.push(text.slice(0, 800));
  task.logs = task.logs.slice(-16);
  task.updatedAt = now();
}

function finishDownloadTask(task, status, error = '') {
  task.status = status;
  task.error = error;
  task.finishedAt = now();
  task.updatedAt = task.finishedAt;
  if (status === 'success') {
    if (task.size && (!task.downloadedBytes || task.downloadedBytes < task.size)) task.downloadedBytes = task.size;
    task.progressPercent = 100;
  }
  task.speedBytesPerSecond = 0;
  delete task.child;
}

async function runNodeHttpDownload(task) {
  task.method = 'node';
  task.status = 'running';
  task.updatedAt = now();
  const response = await fetch(task.url);
  if (!response.ok || !response.body) throw new Error(`直链下载失败：HTTP ${response.status}`);
  const headerName = filenameFromDisposition(response.headers.get('content-disposition'));
  const filename = safeFilename(task.filename || headerName || filenameFromUrl(task.url));
  const dirRel = cleanRelativePath(task.path || '');
  const dir = fullStoragePath(STORAGE_ROOT, dirRel);
  fs.mkdirSync(dir, { recursive: true });
  const file = fullStoragePath(STORAGE_ROOT, cleanRelativePath(path.posix.join(dirRel, filename)));
  task.filename = filename;
  task.size = Number(response.headers.get('content-length') || 0) || null;
  task.speedSampleBytes = task.downloadedBytes || 0;
  task.speedSampleAt = Date.now();
  let lastTick = 0;
  const stream = Readable.fromWeb(response.body);
  stream.on('data', (chunk) => {
    updateDownloadBytes(task, (task.downloadedBytes || 0) + chunk.length);
    if (Date.now() - lastTick > 1200) {
      task.updatedAt = now();
      lastTick = Date.now();
    }
  });
  await pipeline(stream, fs.createWriteStream(file));
  task.outputPath = cleanRelativePath(path.posix.join(dirRel, filename));
}

function runAria2Download(task) {
  return new Promise((resolve, reject) => {
    task.method = 'aria2c';
    task.status = 'running';
    task.updatedAt = now();
    const dirRel = cleanRelativePath(task.path || '');
    const dir = fullStoragePath(STORAGE_ROOT, dirRel);
    fs.mkdirSync(dir, { recursive: true });
    const isMagnet = /^magnet:/i.test(task.url);
    const args = [
      '--continue=true',
      '--max-connection-per-server=8',
      '--split=8',
      '--summary-interval=1',
      '--console-log-level=notice',
      '--seed-time=0',
      '--bt-save-metadata=true',
      '--follow-torrent=mem',
      '--dir', dir
    ];
    if (!isMagnet) {
      task.filename = safeFilename(task.filename || filenameFromUrl(task.url));
      args.push('--out', task.filename);
    }
    args.push(task.url);
    const child = spawn('aria2c', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    task.child = child;
    task.pid = child.pid;
    child.stdout.on('data', (chunk) => appendDownloadLog(task, chunk.toString().split(/\r?\n/).slice(-2).join(' ')));
    child.stderr.on('data', (chunk) => appendDownloadLog(task, chunk.toString().split(/\r?\n/).slice(-2).join(' ')));
    child.once('error', reject);
    child.once('close', (code) => {
      task.exitCode = code;
      if (code === 0) resolve();
      else reject(new Error(`aria2c 退出码 ${code}`));
    });
  });
}

async function runDownloadTask(task) {
  try {
    if (/^magnet:/i.test(task.url)) {
      await runAria2Download(task);
    } else {
      try {
        await runAria2Download(task);
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
        appendDownloadLog(task, 'aria2c 未安装，改用内置直链下载。');
        await runNodeHttpDownload(task);
      }
    }
    finishDownloadTask(task, 'success');
  } catch (error) {
    const message = error.code === 'ENOENT'
      ? '目标节点未安装 aria2c，磁力下载需要先执行节点更新。'
      : error.message;
    appendDownloadLog(task, message);
    finishDownloadTask(task, 'failed', message);
  }
}

function createDownloadTask(body) {
  const url = normalizeDownloadUrl(body.url);
  const task = {
    id: crypto.randomBytes(10).toString('hex'),
    url,
    type: /^magnet:/i.test(url) ? 'magnet' : 'direct',
    path: cleanRelativePath(body.path || ''),
    filename: body.filename ? safeFilename(body.filename) : '',
    status: 'queued',
    method: '',
    size: null,
    downloadedBytes: 0,
    progressPercent: 0,
    speedBytesPerSecond: 0,
    etaSeconds: null,
    eta: '',
    speedSampleBytes: 0,
    speedSampleAt: Date.now(),
    logs: [],
    error: '',
    createdAt: now(),
    updatedAt: now()
  };
  downloadTasks.set(task.id, task);
  while (downloadTasks.size > 40) {
    const oldest = Array.from(downloadTasks.values()).sort((a, b) => a.createdAt.localeCompare(b.createdAt))[0];
    if (!oldest || oldest.status === 'running') break;
    downloadTasks.delete(oldest.id);
  }
  setImmediate(() => runDownloadTask(task));
  return publicDownloadTask(task);
}

function requireNodeToken(req, res, next) {
  if (req.headers['x-node-token'] !== NODE_TOKEN) {
    return res.status(401).json({ error: '节点令牌无效。' });
  }
  next();
}

function requireRole(role) {
  return (req, res, next) => {
    if (!ROLES.has(role)) return res.status(404).json({ error: `此节点未启用 ${role} 角色。` });
    next();
  };
}

function verifyRoomToken(token) {
  const [body, sig] = String(token || '').split('.');
  if (!body || !sig) throw new Error('缺少同步令牌。');
  const expected = crypto.createHmac('sha256', NODE_TOKEN).update(body).digest('base64url');
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) throw new Error('同步令牌无效。');
  const payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  if (!payload.exp || payload.exp < Date.now()) throw new Error('同步令牌已过期。');
  if (!payload.roomId || !payload.userId || !payload.username) throw new Error('同步令牌内容不完整。');
  return payload;
}

// Listing is read-only: a missing directory is a 404, never created as a side effect of a GET.
async function listDir(rel) {
  const safeRel = cleanRelativePath(rel);
  const dir = fullStoragePath(STORAGE_ROOT, safeRel);
  let dirents;
  try {
    dirents = await fs.promises.readdir(dir, { withFileTypes: true });
  } catch (error) {
    if (error.code === 'ENOENT' || error.code === 'ENOTDIR') {
      const err = new Error('目录不存在。');
      err.status = 404;
      throw err;
    }
    throw error;
  }
  const entries = (await Promise.all(dirents.map(async (entry) => {
    if (isHiddenEntry(entry.name) || (!entry.isFile() && !entry.isDirectory())) return null;
    const stat = await fs.promises.stat(path.join(dir, entry.name)).catch(() => null);
    if (!stat) return null;
    const childRel = cleanRelativePath(path.posix.join(safeRel, entry.name));
    const mediaType = entry.isFile() ? mediaTypeForFile(entry.name) : '';
    const mediaUrl = entry.isFile() ? `/videos/${encodePathForUrl(childRel)}` : null;
    return {
      name: entry.name,
      path: childRel,
      type: entry.isDirectory() ? 'dir' : 'file',
      size: entry.isDirectory() ? null : stat.size,
      modifiedAt: stat.mtime.toISOString(),
      isVideo: entry.isFile() && isVideoFile(entry.name),
      isAudio: entry.isFile() && isAudioFile(entry.name),
      isMedia: Boolean(mediaType),
      mediaType,
      mediaUrl,
      videoUrl: mediaUrl
    };
  }))).filter(Boolean);
  entries.sort((a, b) => (a.type === b.type ? a.name.localeCompare(b.name) : a.type === 'dir' ? -1 : 1));
  return { path: safeRel, entries };
}

app.use(express.json({ limit: '2mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type,x-node-token,range,if-range');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    app: 'video52000-node',
    roles: Array.from(ROLES),
    port: PORT,
    protocol: tlsEnabled ? 'https' : 'http',
    storageRoot: ROLES.has('storage') ? STORAGE_ROOT : null,
    features: ['emby-proxy']
  });
});

app.use('/videos', requireRole('storage'), express.static(STORAGE_ROOT, {
  fallthrough: false,
  setHeaders(res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Length,Content-Range,Accept-Ranges,Content-Type');
    res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    res.setHeader('Accept-Ranges', 'bytes');
  }
}));

// ---- Emby proxy ------------------------------------------------------------------------------

function isPrivateIpAddress(address) {
  const version = net.isIP(address);
  if (!version) return false;
  if (version === 6) {
    const value = address.toLowerCase();
    return value === '::1' || value.startsWith('fc') || value.startsWith('fd') || value.startsWith('fe80:') || value === '::';
  }
  const parts = address.split('.').map((part) => Number(part));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part))) return true;
  const [a, b] = parts;
  return a === 10 || a === 127 || a === 0 || (a === 169 && b === 254)
    || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 100 && b >= 64 && b <= 127);
}

function isPrivateHost(hostname) {
  const host = String(hostname || '').toLowerCase();
  if (!host || host === 'localhost' || host.endsWith('.localhost')) return true;
  if (net.isIP(host)) return isPrivateIpAddress(host);
  return false;
}

function openEmbyGrant(token) {
  const [version, ivPart, tagPart, dataPart] = String(token || '').split('.');
  if (version !== 'v1' || !ivPart || !tagPart || !dataPart) {
    const err = new Error('播放凭证无效。');
    err.status = 401;
    err.code = 'grant_expired';
    throw err;
  }
  let payload;
  try {
    const decipher = crypto.createDecipheriv('aes-256-gcm', EMBY_GRANT_KEY, Buffer.from(ivPart, 'base64url'));
    decipher.setAuthTag(Buffer.from(tagPart, 'base64url'));
    const plain = Buffer.concat([decipher.update(Buffer.from(dataPart, 'base64url')), decipher.final()]).toString('utf8');
    payload = JSON.parse(plain);
  } catch {
    const err = new Error('播放凭证无效。');
    err.status = 401;
    err.code = 'grant_expired';
    throw err;
  }
  const [baseUrl, tok, deviceId, embyUserId, itemId, mediaSourceId, roomId, priv, exp] = payload;
  if (!exp || Number(exp) < Date.now()) {
    const err = new Error('播放凭证已过期。');
    err.status = 401;
    err.code = 'grant_expired';
    throw err;
  }
  return { baseUrl, token: tok, deviceId, embyUserId, itemId, mediaSourceId, roomId, priv: Number(priv) || 0, exp: Number(exp) };
}

function embyGrantHeaders(grant) {
  const parts = [
    `Client="${encodeURIComponent(EMBY_CLIENT_NAME)}"`,
    `Device="${encodeURIComponent(EMBY_CLIENT_NAME)}"`,
    `DeviceId="${encodeURIComponent(grant.deviceId || 'playtogether')}"`,
    `Version="${encodeURIComponent(EMBY_CLIENT_VERSION)}"`,
    `Token="${encodeURIComponent(grant.token || '')}"`
  ];
  const value = `MediaBrowser ${parts.join(', ')}`;
  return {
    Authorization: value,
    'X-Emby-Authorization': value,
    'X-Emby-Token': grant.token || '',
    Accept: '*/*'
  };
}

function embyBasePrefix(grant) {
  const base = new URL(grant.baseUrl);
  return `${base.pathname.replace(/\/+$/, '')}/Videos/${grant.itemId}/`;
}

function stripEmbyAuthQuery(searchParams) {
  for (const key of [...searchParams.keys()]) {
    if (EMBY_AUTH_QUERY_KEYS.has(key.toLowerCase())) searchParams.delete(key);
  }
}

// Build an upstream Emby URL for a path that must resolve under /Videos/{itemId}/ (the only tree a
// grant may reach). `relPath` is the untrusted segment; anything that normalises outside is refused.
function embyUpstreamUrl(grant, relPath, rawQuery) {
  const base = new URL(grant.baseUrl);
  const prefix = embyBasePrefix(grant);
  const joined = path.posix.normalize(`${prefix}${String(relPath || '').replace(/^\/+/, '')}`);
  if (!joined.toLowerCase().startsWith(prefix.toLowerCase())) {
    const err = new Error('路径越界。');
    err.status = 403;
    throw err;
  }
  const url = new URL(base.origin);
  url.pathname = joined;
  if (rawQuery) {
    const incoming = new URLSearchParams(rawQuery);
    stripEmbyAuthQuery(incoming);
    url.search = incoming.toString();
  }
  return url;
}

function rewriteEmbyUri(uri, upstreamUrl, grant) {
  let abs;
  try {
    abs = new URL(uri, upstreamUrl);
  } catch {
    return uri;
  }
  const prefix = embyBasePrefix(grant).toLowerCase();
  if (!abs.pathname.toLowerCase().startsWith(prefix)) return uri;
  const rel = abs.pathname.slice(embyBasePrefix(grant).length);
  const params = abs.searchParams;
  stripEmbyAuthQuery(params);
  const query = params.toString();
  return query ? `${rel}?${query}` : rel;
}

// Rewrites playlist URIs to relative paths under this grant so hls.js resolves them back through the
// node (never straight to Emby), and drops the api_key Emby stamps on every line.
function rewriteEmbyPlaylist(text, upstreamUrl, grant) {
  return String(text).split(/\r?\n/).map((line) => {
    if (!line) return line;
    if (line.startsWith('#')) {
      return line.replace(/URI="([^"]*)"/g, (match, uri) => `URI="${rewriteEmbyUri(uri, upstreamUrl, grant)}"`);
    }
    return rewriteEmbyUri(line, upstreamUrl, grant);
  }).join('\n');
}

function setEmbyCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length,Content-Range,Accept-Ranges,Content-Type');
  res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
  res.setHeader('Accept-Ranges', 'bytes');
}

// Byte streams (direct play, HLS segments served with Range) go through core http so there is no
// undici auto-decompression and no request-body timeout on a long-held connection.
function proxyEmbyStream(req, res, url, grant) {
  const lib = url.protocol === 'https:' ? https : http;
  const headers = { ...embyGrantHeaders(grant), 'Accept-Encoding': 'identity' };
  if (req.headers.range) headers.Range = req.headers.range;
  if (req.headers['if-range']) headers['If-Range'] = req.headers['if-range'];
  const upstream = lib.request(url, { method: 'GET', headers }, (up) => {
    clearTimeout(headerTimer);
    setEmbyCorsHeaders(res);
    res.statusCode = up.statusCode || 502;
    for (const name of ['content-type', 'content-length', 'content-range', 'accept-ranges', 'etag', 'last-modified', 'cache-control']) {
      if (up.headers[name]) res.setHeader(name, up.headers[name]);
    }
    if (!up.headers['accept-ranges']) res.setHeader('Accept-Ranges', 'bytes');
    up.pipe(res);
    up.on('error', () => res.destroy());
  });
  const headerTimer = setTimeout(() => upstream.destroy(new Error('timeout')), 15000);
  upstream.on('error', () => {
    clearTimeout(headerTimer);
    if (!res.headersSent) res.status(502).json({ error: '无法连接 Emby 服务器。' });
    else res.destroy();
  });
  res.on('close', () => {
    clearTimeout(headerTimer);
    upstream.destroy();
  });
  upstream.end();
}

async function embyUpstreamFetch(grant, url, { method = 'GET', body, timeout = 15000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, {
      method,
      headers: {
        ...embyGrantHeaders(grant),
        'Accept-Encoding': 'identity',
        ...(body ? { 'Content-Type': 'application/json' } : {})
      },
      body: body ? JSON.stringify(body) : undefined,
      redirect: 'manual',
      signal: controller.signal
    });
    return response;
  } finally {
    clearTimeout(timer);
  }
}

async function embyFetchText(grant, url) {
  const response = await embyUpstreamFetch(grant, url);
  if (response.status === 401 || response.status === 403) {
    const err = new Error('Emby 授权已失效。');
    err.status = 401;
    err.code = 'emby_unauthorized';
    throw err;
  }
  if (!response.ok) {
    const err = new Error(`Emby 返回 HTTP ${response.status}。`);
    err.status = 502;
    throw err;
  }
  return response.text();
}

async function embyFetchJson(grant, pathname, query, body) {
  const base = new URL(grant.baseUrl);
  const url = new URL(`${base.pathname.replace(/\/+$/, '')}/${pathname.replace(/^\/+/, '')}`, base.origin);
  for (const [key, value] of Object.entries(query || {})) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, value);
  }
  const response = await embyUpstreamFetch(grant, url, { method: body ? 'POST' : 'GET', body });
  if (response.status === 401 || response.status === 403) {
    const err = new Error('Emby 授权已失效。');
    err.status = 401;
    err.code = 'emby_unauthorized';
    throw err;
  }
  const text = await response.text();
  if (!response.ok) {
    const err = new Error(`Emby 返回 HTTP ${response.status}。`);
    err.status = 502;
    throw err;
  }
  return text ? JSON.parse(text) : {};
}

// One PlaybackInfo (and therefore one transcode session / PlaySessionId) is shared by the whole
// room, so Emby only ever counts a single concurrent stream per title.
async function embyHlsSession(grant) {
  const key = `${grant.baseUrl}|${grant.itemId}|${grant.mediaSourceId}`;
  const cached = embyHlsSessions.get(key);
  if (cached && Date.now() - cached.at < EMBY_SESSION_TTL_MS) return cached;
  const info = await embyFetchJson(
    grant,
    `Items/${encodeURIComponent(grant.itemId)}/PlaybackInfo`,
    { UserId: grant.embyUserId },
    {
      DeviceProfile: EMBY_DEVICE_PROFILE,
      EnableDirectPlay: false,
      EnableDirectStream: false,
      EnableTranscoding: true,
      MediaSourceId: grant.mediaSourceId,
      MaxStreamingBitrate: EMBY_MAX_BITRATE
    }
  );
  const source = (info.MediaSources || []).find((ms) => ms.Id === grant.mediaSourceId) || (info.MediaSources || [])[0];
  if (!source || !source.TranscodingUrl) {
    const err = new Error('Emby 未返回转码地址。');
    err.status = 502;
    throw err;
  }
  const session = { at: Date.now(), transcodingUrl: source.TranscodingUrl, playSessionId: info.PlaySessionId || '' };
  embyHlsSessions.set(key, session);
  return session;
}

function pruneEmbySegments() {
  const nowMs = Date.now();
  for (const [key, entry] of embySegmentCache) {
    if (nowMs - entry.at > EMBY_SEGMENT_TTL_MS) {
      embySegmentCache.delete(key);
      embySegmentBytes -= entry.buf.length;
    }
  }
  while (embySegmentBytes > EMBY_SEGMENT_CACHE_MAX && embySegmentCache.size) {
    const [key, entry] = embySegmentCache.entries().next().value;
    embySegmentCache.delete(key);
    embySegmentBytes -= entry.buf.length;
  }
}

// Concurrent requests for the same segment hit Emby once; a small LRU lets late-arriving members
// reuse it, keeping the shared transcode from being fetched N times.
async function embyFetchSegment(grant, url) {
  const key = url.toString();
  const cached = embySegmentCache.get(key);
  if (cached) {
    cached.at = Date.now();
    return cached;
  }
  if (embyInflight.has(key)) return embyInflight.get(key);
  const promise = (async () => {
    const response = await embyUpstreamFetch(grant, url, { timeout: 20000 });
    if (!response.ok) {
      const err = new Error(`Emby 返回 HTTP ${response.status}。`);
      err.status = response.status >= 400 && response.status < 500 ? response.status : 502;
      throw err;
    }
    const buf = Buffer.from(await response.arrayBuffer());
    const type = response.headers.get('content-type') || 'application/octet-stream';
    const entry = { at: Date.now(), buf, type };
    embySegmentCache.set(key, entry);
    embySegmentBytes += buf.length;
    pruneEmbySegments();
    return entry;
  })();
  embyInflight.set(key, promise);
  try {
    return await promise;
  } finally {
    embyInflight.delete(key);
  }
}

async function embyServePlaylist(grant, upstreamUrl, res) {
  const key = upstreamUrl.toString();
  const cached = embyPlaylistCache.get(key);
  let text;
  if (cached && Date.now() - cached.at < EMBY_PLAYLIST_TTL_MS) {
    text = cached.text;
  } else {
    text = rewriteEmbyPlaylist(await embyFetchText(grant, upstreamUrl), upstreamUrl, grant);
    embyPlaylistCache.set(key, { at: Date.now(), text });
    if (embyPlaylistCache.size > 200) embyPlaylistCache.delete(embyPlaylistCache.keys().next().value);
  }
  setEmbyCorsHeaders(res);
  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Cache-Control', 'private, max-age=1');
  res.send(text);
}

function embyGrantMiddleware(req, res, next) {
  try {
    req.embyGrant = openEmbyGrant(req.params.grant);
  } catch (error) {
    return res.status(error.status || 401).json({ error: error.message, code: error.code || 'grant_expired' });
  }
  try {
    if (req.embyGrant.priv === 0 && isPrivateHost(new URL(req.embyGrant.baseUrl).hostname)) {
      return res.status(403).json({ error: '目标地址不被允许。' });
    }
  } catch {
    return res.status(400).json({ error: 'Emby 地址不合法。' });
  }
  next();
}

app.get('/emby/:grant/stream', requireRole('storage'), embyGrantMiddleware, (req, res) => {
  const grant = req.embyGrant;
  const query = `Static=true&MediaSourceId=${encodeURIComponent(grant.mediaSourceId)}&DeviceId=${encodeURIComponent(grant.deviceId || '')}`;
  proxyEmbyStream(req, res, embyUpstreamUrl(grant, 'stream', query), grant);
});

app.get('/emby/:grant/hls/{*splat}', requireRole('storage'), embyGrantMiddleware, async (req, res) => {
  const grant = req.embyGrant;
  const rel = (Array.isArray(req.params.splat) ? req.params.splat.join('/') : String(req.params.splat || ''));
  const rawQuery = req.originalUrl.includes('?') ? req.originalUrl.slice(req.originalUrl.indexOf('?') + 1) : '';
  // Validate the untrusted path first so a "../other-item/master.m3u8" cannot ride the master branch.
  embyUpstreamUrl(grant, rel, '');
  if (/^master\.m3u8$/i.test(rel)) {
    const session = await embyHlsSession(grant);
    const upstreamUrl = new URL(session.transcodingUrl, new URL(grant.baseUrl).origin);
    // Emby stamps its own api_key on the transcoding URL; we authenticate by header instead so the
    // token never travels in a URL, not even server-to-server.
    stripEmbyAuthQuery(upstreamUrl.searchParams);
    return embyServePlaylist(grant, upstreamUrl, res);
  }
  const upstreamUrl = embyUpstreamUrl(grant, rel, rawQuery);
  if (/\.m3u8$/i.test(rel)) {
    return embyServePlaylist(grant, upstreamUrl, res);
  }
  try {
    const segment = await embyFetchSegment(grant, upstreamUrl);
    setEmbyCorsHeaders(res);
    res.setHeader('Content-Type', segment.type);
    res.setHeader('Cache-Control', 'private, max-age=3600');
    res.send(segment.buf);
  } catch (error) {
    // A dead PlaySessionId means the cached session is stale; drop it so the next master reloads.
    if (error.status >= 400 && error.status < 500) {
      embyHlsSessions.delete(`${grant.baseUrl}|${grant.itemId}|${grant.mediaSourceId}`);
    }
    throw error;
  }
});

app.get('/emby/:grant/sub/:index', requireRole('storage'), embyGrantMiddleware, async (req, res) => {
  const grant = req.embyGrant;
  const index = String(req.params.index).replace(/[^0-9]/g, '') || '0';
  const upstreamUrl = embyUpstreamUrl(grant, `${encodeURIComponent(grant.mediaSourceId)}/Subtitles/${index}/Stream.vtt`, '');
  const text = await embyFetchText(grant, upstreamUrl);
  setEmbyCorsHeaders(res);
  res.setHeader('Content-Type', 'text/vtt; charset=utf-8');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.send(text);
});

app.get('/emby/:grant/probe', requireRole('storage'), embyGrantMiddleware, async (req, res) => {
  const grant = req.embyGrant;
  try {
    await embyFetchJson(grant, `Users/${encodeURIComponent(grant.embyUserId)}/Items/${encodeURIComponent(grant.itemId)}`, {});
    res.json({ ok: true, mode: grant.mediaSourceId ? 'ready' : 'ready' });
  } catch (error) {
    res.status(error.status || 502).json({ error: error.message, code: error.code || 'emby_error' });
  }
});

app.get('/storage/list', requireRole('storage'), requireNodeToken, async (req, res) => {
  res.json(await listDir(req.query.path || ''));
});

app.post('/storage/mkdir', requireRole('storage'), requireNodeToken, (req, res) => {
  const rel = cleanRelativePath(req.body.path || '');
  if (!rel) return res.status(400).json({ error: '目录名不能为空。' });
  fs.mkdirSync(fullStoragePath(STORAGE_ROOT, rel), { recursive: true });
  res.json({ ok: true });
});

app.post('/storage/delete', requireRole('storage'), requireNodeToken, (req, res) => {
  const rel = cleanRelativePath(req.body.path || '');
  if (!rel) return res.status(400).json({ error: '不能删除根目录。' });
  fs.rmSync(fullStoragePath(STORAGE_ROOT, rel), { recursive: true, force: true });
  res.json({ ok: true });
});

app.post('/storage/upload', requireRole('storage'), requireNodeToken, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: '请选择文件。' });
    const dirRel = cleanRelativePath(req.body.path || '');
    const filename = safeFilename(req.file.originalname, 'video.bin');
    const targetRel = cleanRelativePath(path.posix.join(dirRel, filename));
    const dir = fullStoragePath(STORAGE_ROOT, dirRel);
    await fs.promises.mkdir(dir, { recursive: true });
    await moveFile(req.file.path, fullStoragePath(STORAGE_ROOT, targetRel));
    res.json({ ok: true, path: targetRel });
  } finally {
    await discardUpload(req.file);
  }
});

app.get('/storage/downloads', requireRole('storage'), requireNodeToken, (req, res) => {
  res.json({
    tasks: Array.from(downloadTasks.values())
      .map(publicDownloadTask)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
  });
});

app.post('/storage/downloads', requireRole('storage'), requireNodeToken, (req, res) => {
  res.json({ task: createDownloadTask(req.body || {}) });
});

app.delete('/storage/downloads/:id', requireRole('storage'), requireNodeToken, (req, res) => {
  const task = downloadTasks.get(req.params.id);
  if (!task) return res.status(404).json({ error: '下载任务不存在。' });
  if (task.status === 'running' && task.child) {
    task.child.kill('SIGTERM');
    finishDownloadTask(task, 'canceled', '已取消');
  } else {
    downloadTasks.delete(task.id);
  }
  res.json({ ok: true });
});

const rooms = new Map();
const wss = new WebSocketServer({ noServer: true, maxPayload: WS_MAX_PAYLOAD });
let roomStateFlushTimer = null;

function safeShortText(value, limit = 180) {
  return String(value || '').trim().slice(0, limit);
}

// Room state (queues, modes, last messages) is checkpointed to disk so a node restart or update
// does not wipe every room's queue. Writes are debounced; members are runtime-only.
function writeRoomsNow() {
  if (!ROLES.has('sync')) return;
  const snapshot = {};
  for (const [id, room] of rooms) {
    const current = computedState(room);
    snapshot[id] = {
      // Store the position as of now, not the base position of the last update, so a restart
      // resumes (paused) where playback actually was.
      state: { ...room.state, position: current.position, updatedAt: Date.now() },
      messages: room.messages.slice(-80),
      emptySince: room.members.size ? null : room.emptySince || Date.now()
    };
  }
  const tmp = `${SYNC_STATE_FILE}.${process.pid}.tmp`;
  try {
    fs.writeFileSync(tmp, JSON.stringify(snapshot));
    fs.renameSync(tmp, SYNC_STATE_FILE);
  } catch (error) {
    console.warn(`[sync] failed to persist room state: ${error.message}`);
  }
}

function persistRooms() {
  if (!ROLES.has('sync') || roomStateFlushTimer) return;
  roomStateFlushTimer = setTimeout(() => {
    roomStateFlushTimer = null;
    writeRoomsNow();
  }, 1500);
}

function anyRoomPlaying() {
  for (const room of rooms.values()) {
    if (room.members.size && room.state.isPlaying) return true;
  }
  return false;
}

function restoreRooms() {
  if (!ROLES.has('sync') || !fs.existsSync(SYNC_STATE_FILE)) return;
  try {
    const snapshot = JSON.parse(fs.readFileSync(SYNC_STATE_FILE, 'utf8'));
    for (const [id, saved] of Object.entries(snapshot || {})) {
      if (!id || !saved?.state) continue;
      const room = roomFor(id);
      room.state = {
        ...room.state,
        ...saved.state,
        isPlaying: false,
        position: Math.max(0, Number(saved.state.position) || 0),
        updatedAt: Date.now(),
        version: Number(saved.state.version || 0) + 1,
        updatedBy: 'system'
      };
      room.state.mediaQueues = normalizeQueues(room.state.mediaQueues);
      room.state.playbackModes = normalizePlaybackModes(room.state.playbackModes);
      room.messages = Array.isArray(saved.messages) ? saved.messages.slice(-80) : [];
      room.emptySince = Number(saved.emptySince) || Date.now();
    }
    console.log(`[sync] restored ${rooms.size} room(s) from ${SYNC_STATE_FILE}`);
  } catch (error) {
    console.warn(`[sync] ignoring unreadable ${SYNC_STATE_FILE}: ${error.message}`);
  }
}

function pruneEmptyRooms() {
  const cutoff = Date.now() - ROOM_EMPTY_TTL_MS;
  let removed = 0;
  for (const [id, room] of rooms) {
    if (!room.members.size && Number(room.emptySince || 0) < cutoff) {
      rooms.delete(id);
      removed += 1;
    }
  }
  if (removed) persistRooms();
}

function safeLyrics(input) {
  if (!input || typeof input !== 'object') return null;
  const lines = Array.isArray(input.lines)
    ? input.lines
      .map((line) => {
        const time = Number(line?.time);
        const text = safeShortText(line?.text, 240);
        if (!Number.isFinite(time) || time < 0 || !text) return null;
        return { time, text };
      })
      .filter(Boolean)
      .sort((a, b) => a.time - b.time)
      .slice(0, 600)
    : [];
  const text = safeShortText(input.text, 30000);
  if (!text && !lines.length) return null;
  return {
    type: lines.length ? 'synced' : 'plain',
    source: safeShortText(input.source || '', 40),
    text: text || lines.map((line) => line.text).join('\n').slice(0, 30000),
    lines
  };
}

function safeMediaType(value) {
  return value === 'audio' ? 'audio' : 'video';
}

function safePlaybackMode(value) {
  if (value === 'random' || value === 'repeat-one') return value;
  return 'sequence';
}

function emptyQueues() {
  return { video: [], audio: [] };
}

function normalizeQueues(value) {
  return {
    video: Array.isArray(value?.video) ? value.video : [],
    audio: Array.isArray(value?.audio) ? value.audio : []
  };
}

function normalizePlaybackModes(value) {
  return {
    video: safePlaybackMode(value?.video),
    audio: safePlaybackMode(value?.audio)
  };
}

function queueFor(room, mediaType = room.state.roomMode) {
  if (!room.state.mediaQueues) room.state.mediaQueues = emptyQueues();
  room.state.mediaQueues = normalizeQueues(room.state.mediaQueues);
  return room.state.mediaQueues[safeMediaType(mediaType)];
}

function playbackModeFor(room, mediaType = room.state.roomMode) {
  if (!room.state.playbackModes) room.state.playbackModes = normalizePlaybackModes(room.state.playbackModes);
  room.state.playbackModes = normalizePlaybackModes(room.state.playbackModes);
  return room.state.playbackModes[safeMediaType(mediaType)];
}

function legacyMusicQueue(room) {
  return queueFor(room, 'audio');
}

function safeInt(value) {
  const num = Number(value);
  return Number.isFinite(num) ? Math.trunc(num) : null;
}

// External-source metadata (currently Emby) rides along on the track so every member's client can
// fetch subtitles and show queued state without its own account. Only a fixed shape survives.
function sanitizeTrackSource(input) {
  if (!input || typeof input !== 'object' || input.kind !== 'emby') return undefined;
  const subtitles = Array.isArray(input.subtitles)
    ? input.subtitles.slice(0, 20).map((sub) => ({
      index: safeInt(sub.index) ?? 0,
      label: safeShortText(sub.label, 80),
      language: safeShortText(sub.language, 16),
      isDefault: Boolean(sub.isDefault)
    }))
    : [];
  return {
    kind: 'emby',
    serverId: safeShortText(input.serverId, 80),
    connectionId: safeShortText(input.connectionId, 80),
    itemId: safeShortText(input.itemId, 80),
    mediaSourceId: safeShortText(input.mediaSourceId, 80),
    nodeId: safeShortText(input.nodeId, 80),
    mode: input.mode === 'direct' ? 'direct' : 'hls',
    itemType: safeShortText(input.itemType, 40),
    seriesName: safeShortText(input.seriesName, 120),
    seasonName: safeShortText(input.seasonName, 120),
    indexNumber: safeInt(input.indexNumber),
    parentIndexNumber: safeInt(input.parentIndexNumber),
    year: safeInt(input.year),
    subtitles
  };
}

function sanitizeTrack(input = {}, fallbackType = 'audio') {
  const mediaUrl = safeShortText(input.mediaUrl || input.url || input.videoUrl, 3000);
  if (!mediaUrl) return null;
  const mediaType = safeMediaType(input.mediaType || fallbackType);
  const track = {
    id: safeShortText(input.id, 80) || randomId('track'),
    mediaUrl,
    mediaType,
    title: safeShortText(input.title || input.name || filenameFromUrl(mediaUrl) || (mediaType === 'audio' ? '未命名音乐' : '未命名视频'), TEXT_LIMITS.title),
    artist: safeShortText(input.artist, TEXT_LIMITS.artist),
    album: safeShortText(input.album, TEXT_LIMITS.album),
    coverUrl: safeShortText(input.coverUrl, TEXT_LIMITS.coverUrl),
    duration: Number.isFinite(Number(input.duration)) ? Math.max(0, Number(input.duration)) : null,
    lyrics: safeLyrics(input.lyrics),
    sourceName: safeShortText(input.sourceName || input.name, TEXT_LIMITS.sourceName),
    addedAt: now()
  };
  const source = sanitizeTrackSource(input.source);
  if (source) track.source = source;
  return track;
}

function currentTrack(room, mediaType = room.state.mediaType || room.state.roomMode) {
  return queueFor(room, mediaType).find((track) => track.id === room.state.currentTrackId) || null;
}

function setCurrentTrack(room, track, user, isPlaying = true) {
  const mediaType = safeMediaType(track?.mediaType || room.state.roomMode || 'video');
  room.state.roomMode = mediaType;
  room.state.mediaType = mediaType;
  room.state.mediaUrl = track?.mediaUrl || '';
  room.state.videoUrl = room.state.mediaUrl;
  room.state.currentTrackId = track?.id || '';
  room.state.mediaMeta = track ? {
    title: track.title,
    artist: track.artist,
    album: track.album,
    coverUrl: track.coverUrl,
    duration: track.duration,
    lyrics: track.lyrics || null,
    sourceName: track.sourceName
  } : null;
  room.state.position = 0;
  room.state.isPlaying = Boolean(track && isPlaying);
  room.state.updatedAt = Date.now();
  room.state.version = Number(room.state.version || 0) + 1;
  room.state.updatedBy = user?.username || 'system';
}

function setRoomMode(room, user, mode) {
  const roomMode = safeMediaType(mode);
  // Re-selecting the active mode must not rewind and pause the whole room.
  if (room.state.roomMode === roomMode && room.state.mediaType === roomMode) return false;
  room.state.roomMode = roomMode;
  room.state.mediaType = roomMode;
  const track = currentTrack(room, roomMode) || queueFor(room, roomMode)[0] || null;
  if (track) {
    setCurrentTrack(room, track, user, false);
  } else {
    room.state.mediaUrl = '';
    room.state.videoUrl = '';
    room.state.currentTrackId = '';
    room.state.mediaMeta = null;
    room.state.isPlaying = false;
    room.state.position = 0;
    room.state.updatedAt = Date.now();
    room.state.version = Number(room.state.version || 0) + 1;
    room.state.updatedBy = user?.username || 'system';
  }
  return true;
}

function broadcastState(room, reason = 'control') {
  broadcast(room, { type: 'state', state: computedState(room), reason });
  persistRooms();
}

function moveQueueItem(queue, id, direction) {
  const index = queue.findIndex((track) => track.id === id);
  if (index < 0) return false;
  const nextIndex = direction === 'up' ? index - 1 : index + 1;
  if (nextIndex < 0 || nextIndex >= queue.length) return false;
  const [item] = queue.splice(index, 1);
  queue.splice(nextIndex, 0, item);
  return true;
}

function handleQueueMessage(room, user, message) {
  const mediaType = safeMediaType(message.mediaType || room.state.roomMode || room.state.mediaType);
  const queue = queueFor(room, mediaType);
  if (message.type === 'queue_add') {
    const track = sanitizeTrack(message.track, mediaType);
    if (!track) return;
    queue.push(track);
    if (queue.length > 120) queue.splice(0, queue.length - 120);
    if (message.playNow) setCurrentTrack(room, track, user, true);
    else {
      touchRoomState(room, user);
    }
    broadcastState(room, message.playNow ? 'queue_play' : 'queue_add');
    return;
  }
  if (message.type === 'queue_play') {
    const track = queue.find((item) => item.id === String(message.trackId || ''));
    if (!track) return;
    setCurrentTrack(room, track, user, true);
    broadcastState(room, 'queue_play');
    return;
  }
  if (message.type === 'queue_update') {
    const track = queue.find((item) => item.id === String(message.trackId || ''));
    if (!track || !message.patch || typeof message.patch !== 'object') return;
    for (const field of ['title', 'artist', 'album', 'coverUrl', 'sourceName']) {
      if (field in message.patch) track[field] = safeShortText(message.patch[field], TEXT_LIMITS[field]);
    }
    if ('duration' in message.patch) track.duration = Number.isFinite(Number(message.patch.duration)) ? Math.max(0, Number(message.patch.duration)) : null;
    if ('lyrics' in message.patch) track.lyrics = safeLyrics(message.patch.lyrics);
    // A refreshed Emby grant re-points the same track; if it is the current one, update the live
    // media URL in place without rewinding (position and version are handled by touchRoomState).
    if ('mediaUrl' in message.patch) {
      const nextUrl = safeShortText(message.patch.mediaUrl, 3000);
      if (/^https?:\/\//i.test(nextUrl)) {
        track.mediaUrl = nextUrl;
        if (room.state.currentTrackId === track.id) {
          room.state.mediaUrl = nextUrl;
          room.state.videoUrl = nextUrl;
        }
      }
    }
    if (room.state.currentTrackId === track.id) {
      room.state.mediaMeta = {
        title: track.title,
        artist: track.artist,
        album: track.album,
        coverUrl: track.coverUrl,
        duration: track.duration,
        lyrics: track.lyrics || null,
        sourceName: track.sourceName
      };
    }
    touchRoomState(room, user);
    broadcastState(room, 'queue_update');
    return;
  }
  if (message.type === 'queue_remove') {
    const trackId = String(message.trackId || '');
    const index = queue.findIndex((track) => track.id === trackId);
    if (index < 0) return;
    const wasCurrent = room.state.mediaType === mediaType && room.state.currentTrackId === trackId;
    queue.splice(index, 1);
    if (wasCurrent) setCurrentTrack(room, queue[index] || queue[index - 1] || null, user, Boolean(queue.length));
    else {
      touchRoomState(room, user);
    }
    broadcastState(room, 'queue_remove');
    return;
  }
  if (message.type === 'queue_move') {
    if (!moveQueueItem(queue, String(message.trackId || ''), message.direction === 'up' ? 'up' : 'down')) return;
    touchRoomState(room, user);
    broadcastState(room, 'queue_move');
    return;
  }
  if (message.type === 'queue_clear') {
    queue.length = 0;
    if (room.state.mediaType === mediaType) setCurrentTrack(room, null, user, false);
    else {
      touchRoomState(room, user);
    }
    broadcastState(room, 'queue_clear');
    return;
  }
  if (message.type === 'queue_mode') {
    if (!room.state.playbackModes) room.state.playbackModes = normalizePlaybackModes(room.state.playbackModes);
    room.state.playbackModes[safeMediaType(mediaType)] = safePlaybackMode(message.playbackMode || message.mode);
    touchRoomState(room, user);
    broadcastState(room, 'queue_mode');
    return;
  }
  if (message.type === 'queue_reorder') {
    const order = Array.isArray(message.trackIds) ? message.trackIds.map(String) : [];
    if (!order.length) return;
    const byId = new Map(queue.map((track) => [track.id, track]));
    const reordered = order.map((id) => byId.get(id)).filter(Boolean);
    if (reordered.length !== queue.length) return;
    queue.splice(0, queue.length, ...reordered);
    touchRoomState(room, user);
    broadcastState(room, 'queue_reorder');
    return;
  }
  if (message.type === 'queue_next' || message.type === 'queue_previous') {
    if (!queue.length) return;
    const currentIndex = queue.findIndex((track) => track.id === room.state.currentTrackId);
    if (message.ended) {
      // Every member reports `ended` for the same track within a few ms of each other. Only the
      // first report for the track that is actually current may advance the queue; the rest are
      // duplicates and would otherwise skip tracks or re-trigger repeat-one.
      const endedTrackId = safeShortText(message.trackId, 80);
      if (endedTrackId && endedTrackId !== room.state.currentTrackId) return;
      if (room.state.mediaType !== mediaType) return;
      // A repeat-one loop keeps the same track current, so a second report for it inside the
      // window is still a duplicate; legacy reports without a trackId rely on the window alone.
      const sinceLastAdvance = Date.now() - Number(room.lastEndedAdvanceAt || 0);
      if (sinceLastAdvance < ENDED_DEDUPE_WINDOW_MS && (!endedTrackId || endedTrackId === room.lastEndedTrackId)) return;
      room.lastEndedAdvanceAt = Date.now();
      room.lastEndedTrackId = endedTrackId || room.state.currentTrackId;
      const mode = playbackModeFor(room, mediaType);
      if (mode === 'repeat-one' && currentIndex >= 0) {
        setCurrentTrack(room, queue[currentIndex], user, true);
        broadcastState(room, 'queue_loop');
        return;
      }
      if (queue.length <= 1) {
        pauseCurrentTrack(room, user);
        broadcastState(room, 'queue_end');
        return;
      }
      if (mode === 'random') {
        const candidates = queue.map((_, index) => index).filter((index) => index !== currentIndex);
        const nextRandomIndex = candidates[Math.floor(Math.random() * candidates.length)] ?? 0;
        setCurrentTrack(room, queue[nextRandomIndex], user, true);
        broadcastState(room, 'queue_random');
        return;
      }
      if (currentIndex < 0 || currentIndex >= queue.length - 1) {
        pauseCurrentTrack(room, user);
        broadcastState(room, 'queue_end');
        return;
      }
    }
    const nextIndex = message.type === 'queue_next'
      ? Math.min(queue.length - 1, currentIndex + 1)
      : Math.max(0, currentIndex - 1);
    const track = queue[nextIndex >= 0 ? nextIndex : 0];
    if (!track || track.id === room.state.currentTrackId) return;
    setCurrentTrack(room, track, user, true);
    broadcastState(room, message.type);
  }
}

function roomFor(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      id: roomId,
      state: {
        videoUrl: '',
        mediaUrl: '',
        mediaType: 'video',
        isPlaying: false,
        position: 0,
        currentTrackId: '',
        roomMode: 'video',
        mediaQueues: emptyQueues(),
        musicQueue: [],
        playbackModes: normalizePlaybackModes(),
        mediaMeta: null,
        updatedAt: Date.now(),
        version: 0,
        updatedBy: ''
      },
      members: new Map(),
      messages: [],
      emptySince: Date.now(),
      lastEndedAdvanceAt: 0
    });
  }
  return rooms.get(roomId);
}

function computedState(room) {
  const elapsed = room.state.isPlaying ? Math.max(0, (Date.now() - room.state.updatedAt) / 1000) : 0;
  room.state.mediaQueues = normalizeQueues(room.state.mediaQueues);
  room.state.musicQueue = legacyMusicQueue(room);
  room.state.playbackModes = normalizePlaybackModes(room.state.playbackModes);
  return {
    ...room.state,
    position: Math.max(0, room.state.position + elapsed),
    serverTime: Date.now()
  };
}

function touchRoomState(room, user) {
  const current = computedState(room);
  room.state.position = current.position;
  room.state.updatedAt = Date.now();
  room.state.version = Number(current.version || 0) + 1;
  room.state.updatedBy = user?.username || 'system';
}

function pauseCurrentTrack(room, user) {
  const current = computedState(room);
  room.state.position = current.position;
  room.state.isPlaying = false;
  room.state.updatedAt = Date.now();
  room.state.version = Number(current.version || 0) + 1;
  room.state.updatedBy = user?.username || 'system';
}

function distinctMembers(room) {
  const byUser = new Map();
  for (const member of room.members.values()) {
    byUser.set(member.userId, {
      userId: member.userId,
      username: member.username,
      joinedAt: member.joinedAt
    });
  }
  return Array.from(byUser.values()).sort((a, b) => a.username.localeCompare(b.username));
}

function send(ws, message) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(message));
}

function broadcast(room, message) {
  for (const member of room.members.values()) send(member.ws, message);
}

function broadcastMembers(room) {
  broadcast(room, { type: 'members', members: distinctMembers(room) });
}

function pauseRoomWhenEmpty(room) {
  const current = computedState(room);
  room.state = {
    ...room.state,
    videoUrl: current.mediaUrl || current.videoUrl || '',
    mediaUrl: current.mediaUrl || current.videoUrl || '',
    mediaType: safeMediaType(current.mediaType),
    roomMode: safeMediaType(current.roomMode || current.mediaType),
    isPlaying: false,
    position: current.position,
    updatedAt: Date.now(),
    version: current.version + 1,
    updatedBy: 'system'
  };
  room.emptySince = Date.now();
  persistRooms();
}

function takeChatToken(member) {
  const nowMs = Date.now();
  const elapsed = (nowMs - Number(member.chatRefillAt || nowMs)) / 1000;
  member.chatTokens = Math.min(CHAT_BURST, Number(member.chatTokens ?? CHAT_BURST) + elapsed * CHAT_REFILL_PER_SECOND);
  member.chatRefillAt = nowMs;
  if (member.chatTokens < 1) return false;
  member.chatTokens -= 1;
  return true;
}

function applyState(room, user, incoming) {
  const current = computedState(room);
  const mediaUrl = typeof incoming.mediaUrl === 'string'
    ? incoming.mediaUrl.trim().slice(0, 3000)
    : typeof incoming.videoUrl === 'string'
      ? incoming.videoUrl.trim().slice(0, 3000)
      : current.mediaUrl || current.videoUrl || '';
  const mediaType = safeMediaType(incoming.mediaType || current.mediaType);
  const roomMode = safeMediaType(incoming.roomMode || current.roomMode || mediaType);
  const position = Number.isFinite(Number(incoming.position)) ? Math.max(0, Number(incoming.position)) : current.position;
  const isPlaying = Boolean(incoming.isPlaying);
  const mediaMeta = incoming.mediaMeta && typeof incoming.mediaMeta === 'object' ? {
    title: safeShortText(incoming.mediaMeta.title, 160),
    artist: safeShortText(incoming.mediaMeta.artist, 120),
    album: safeShortText(incoming.mediaMeta.album, 120),
    coverUrl: safeShortText(incoming.mediaMeta.coverUrl, 3000),
    duration: Number.isFinite(Number(incoming.mediaMeta.duration)) ? Math.max(0, Number(incoming.mediaMeta.duration)) : null,
    lyrics: safeLyrics(incoming.mediaMeta.lyrics),
    sourceName: safeShortText(incoming.mediaMeta.sourceName, 180)
  } : current.mediaMeta || null;
  const currentTrackId = 'currentTrackId' in incoming ? safeShortText(incoming.currentTrackId, 80) : safeShortText(current.currentTrackId, 80);
  room.state = {
    ...room.state,
    videoUrl: mediaUrl,
    mediaUrl,
    mediaType,
    roomMode,
    playbackModes: normalizePlaybackModes(current.playbackModes),
    isPlaying,
    position,
    mediaMeta: mediaType === 'audio' ? mediaMeta : null,
    currentTrackId,
    updatedAt: Date.now(),
    version: current.version + 1,
    updatedBy: user.username
  };
  broadcastState(room, incoming.reason || 'control');
}

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname !== '/sync' || !ROLES.has('sync')) {
    socket.destroy();
    return;
  }
  let payload;
  try {
    payload = verifyRoomToken(url.searchParams.get('token'));
  } catch (error) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req, payload);
  });
});

wss.on('connection', (ws, req, payload) => {
  const room = roomFor(payload.roomId);
  const connectionId = crypto.randomBytes(12).toString('hex');
  const member = {
    connectionId,
    userId: payload.userId,
    username: payload.username,
    joinedAt: now(),
    chatTokens: CHAT_BURST,
    chatRefillAt: Date.now(),
    ws
  };
  room.members.set(connectionId, member);
  room.emptySince = null;
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  send(ws, {
    type: 'snapshot',
    roomId: room.id,
    state: computedState(room),
    members: distinctMembers(room),
    messages: room.messages.slice(-80)
  });
  broadcastMembers(room);

  ws.on('message', (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (message.type === 'state_update') {
      applyState(room, member, message.state || {});
      return;
    }
    if (message.type === 'room_mode') {
      if (setRoomMode(room, member, message.roomMode || message.mediaType)) broadcastState(room, 'room_mode');
      return;
    }
    if (/^queue_/.test(message.type || '')) {
      handleQueueMessage(room, member, message);
      return;
    }
    if (message.type === 'chat') {
      const text = String(message.text || '').trim().slice(0, 300);
      if (!text) return;
      if (!takeChatToken(member)) {
        send(ws, { type: 'notice', level: 'warn', text: '发言太快了，稍等一下。' });
        return;
      }
      const chat = {
        id: crypto.randomBytes(10).toString('hex'),
        userId: member.userId,
        username: member.username,
        text,
        createdAt: now()
      };
      room.messages.push(chat);
      room.messages = room.messages.slice(-160);
      // One message drives both the chat list and the danmaku layer on the client.
      broadcast(room, { type: 'chat', message: chat });
      persistRooms();
      return;
    }
    if (message.type === 'request_sync') {
      send(ws, { type: 'state_sync', state: computedState(room) });
      return;
    }
    if (message.type === 'latency_ping') {
      send(ws, {
        type: 'latency_pong',
        id: String(message.id || ''),
        clientTime: message.clientTime || null,
        serverTime: Date.now()
      });
    }
  });

  ws.on('close', () => {
    room.members.delete(connectionId);
    if (!room.members.size) {
      pauseRoomWhenEmpty(room);
      return;
    }
    broadcastMembers(room);
  });
});

setInterval(() => {
  for (const room of rooms.values()) {
    if (!room.members.size) continue;
    broadcast(room, { type: 'state_sync', state: computedState(room) });
  }
}, 1000);

// Heartbeat: a member whose socket silently died (mobile sleep, NAT timeout) is dropped within
// two intervals instead of lingering in the member list until the TCP stack gives up.
setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    try {
      client.ping();
    } catch {
      client.terminate();
    }
  }
}, WS_HEARTBEAT_MS).unref();

setInterval(pruneEmptyRooms, 10 * 60 * 1000).unref();
setInterval(pruneUploadTmpFiles, 6 * 60 * 60 * 1000).unref();
// While something is playing the live position moves without any state change, so checkpoint it
// regularly; a graceful stop (systemd restart, node update) flushes synchronously.
setInterval(() => {
  if (anyRoomPlaying()) writeRoomsNow();
}, 10 * 1000).unref();
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    clearTimeout(roomStateFlushTimer);
    roomStateFlushTimer = null;
    writeRoomsNow();
    process.exit(0);
  });
}
pruneUploadTmpFiles();
restoreRooms();

app.use((err, req, res, next) => {
  if (req.file) discardUpload(req.file);
  if (res.headersSent) return next(err);
  if (err instanceof multer.MulterError) {
    const message = err.code === 'LIMIT_FILE_SIZE' ? `文件超过上传上限 ${MAX_UPLOAD_MB} MB。` : `上传失败：${err.message}`;
    return res.status(413).json({ error: message });
  }
  res.status(err.status || 500).json({ error: err.message || '节点错误。' });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Video52000 node listening on ${tlsEnabled ? 'https' : 'http'}://0.0.0.0:${PORT}`);
  console.log(`Roles: ${Array.from(ROLES).join(', ') || 'none'}`);
  if (ROLES.has('storage')) console.log(`Storage root: ${STORAGE_ROOT}`);
});
