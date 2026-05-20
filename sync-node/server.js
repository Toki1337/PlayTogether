'use strict';

const crypto = require('crypto');
const { spawn } = require('child_process');
const express = require('express');
const fs = require('fs');
const http = require('http');
const https = require('https');
const multer = require('multer');
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
const MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB || 2048);
const TLS_CERT_PATH = process.env.TLS_CERT_PATH || '';
const TLS_KEY_PATH = process.env.TLS_KEY_PATH || '';

fs.mkdirSync(STORAGE_ROOT, { recursive: true });

const app = express();
const tlsEnabled = Boolean(TLS_CERT_PATH && TLS_KEY_PATH);
const downloadTasks = new Map();
const server = tlsEnabled
  ? https.createServer({
    cert: fs.readFileSync(TLS_CERT_PATH),
    key: fs.readFileSync(TLS_KEY_PATH)
  }, app)
  : http.createServer(app);
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024 }
});

function now() {
  return new Date().toISOString();
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

function listDir(rel) {
  const safeRel = cleanRelativePath(rel);
  const dir = fullStoragePath(STORAGE_ROOT, safeRel);
  fs.mkdirSync(dir, { recursive: true });
  const entries = fs.readdirSync(dir, { withFileTypes: true }).map((entry) => {
    const childRel = cleanRelativePath(path.posix.join(safeRel, entry.name));
    const stat = fs.statSync(path.join(dir, entry.name));
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
  });
  entries.sort((a, b) => (a.type === b.type ? a.name.localeCompare(b.name) : a.type === 'dir' ? -1 : 1));
  return { path: safeRel, entries };
}

app.use(express.json({ limit: '2mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type,x-node-token');
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
    storageRoot: ROLES.has('storage') ? STORAGE_ROOT : null
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

app.get('/storage/list', requireRole('storage'), requireNodeToken, (req, res) => {
  res.json(listDir(req.query.path || ''));
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

app.post('/storage/upload', requireRole('storage'), requireNodeToken, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: '请选择文件。' });
  const dirRel = cleanRelativePath(req.body.path || '');
  const filename = path.basename(req.file.originalname || 'video.bin');
  const dir = fullStoragePath(STORAGE_ROOT, dirRel);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, filename), req.file.buffer);
  res.json({ ok: true, path: cleanRelativePath(path.posix.join(dirRel, filename)) });
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
const wss = new WebSocketServer({ noServer: true });

function safeShortText(value, limit = 180) {
  return String(value || '').trim().slice(0, limit);
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

function sanitizeTrack(input = {}, fallbackType = 'audio') {
  const mediaUrl = safeShortText(input.mediaUrl || input.url || input.videoUrl, 3000);
  if (!mediaUrl) return null;
  const mediaType = safeMediaType(input.mediaType || fallbackType);
  return {
    id: safeShortText(input.id, 80) || randomId('track'),
    mediaUrl,
    mediaType,
    title: safeShortText(input.title || input.name || filenameFromUrl(mediaUrl) || (mediaType === 'audio' ? '未命名音乐' : '未命名视频'), 160),
    artist: safeShortText(input.artist, 120),
    album: safeShortText(input.album, 120),
    coverUrl: safeShortText(input.coverUrl, 3000),
    duration: Number.isFinite(Number(input.duration)) ? Math.max(0, Number(input.duration)) : null,
    lyrics: safeLyrics(input.lyrics),
    sourceName: safeShortText(input.sourceName || input.name, 180),
    addedAt: input.addedAt || now()
  };
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
}

function broadcastState(room, reason = 'control') {
  broadcast(room, { type: 'state', state: computedState(room), reason });
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
      if (field in message.patch) track[field] = safeShortText(message.patch[field], field === 'coverUrl' ? 3000 : 180);
    }
    if ('duration' in message.patch) track.duration = Number.isFinite(Number(message.patch.duration)) ? Math.max(0, Number(message.patch.duration)) : null;
    if ('lyrics' in message.patch) track.lyrics = safeLyrics(message.patch.lyrics);
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
      messages: []
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
    ws
  };
  room.members.set(connectionId, member);

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
      setRoomMode(room, member, message.roomMode || message.mediaType);
      broadcastState(room, 'room_mode');
      return;
    }
    if (/^queue_/.test(message.type || '')) {
      handleQueueMessage(room, member, message);
      return;
    }
    if (message.type === 'chat') {
      const text = String(message.text || '').trim().slice(0, 300);
      if (!text) return;
      const chat = {
        id: crypto.randomBytes(10).toString('hex'),
        userId: member.userId,
        username: member.username,
        text,
        createdAt: now()
      };
      room.messages.push(chat);
      room.messages = room.messages.slice(-160);
      broadcast(room, { type: 'chat', message: chat });
      broadcast(room, { type: 'danmaku', message: chat });
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

app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  res.status(err.status || 500).json({ error: err.message || '节点错误。' });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Video52000 node listening on ${tlsEnabled ? 'https' : 'http'}://0.0.0.0:${PORT}`);
  console.log(`Roles: ${Array.from(ROLES).join(', ') || 'none'}`);
  if (ROLES.has('storage')) console.log(`Storage root: ${STORAGE_ROOT}`);
});
