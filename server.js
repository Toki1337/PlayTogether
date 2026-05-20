'use strict';

const bcrypt = require('bcryptjs');
const { spawn } = require('child_process');
const crypto = require('crypto');
const dns = require('dns').promises;
const express = require('express');
const fs = require('fs');
const multer = require('multer');
const net = require('net');
const path = require('path');
const session = require('express-session');
const { Client } = require('ssh2');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const { domainToASCII } = require('url');
const { WebSocket } = require('ws');

const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
const SECRET_FILE = path.join(DATA_DIR, 'secrets.json');
const MEDIA_COVER_DIR = path.join(DATA_DIR, 'media-covers');
const PUBLIC_DIR = path.join(ROOT, 'public');
const STORAGE_ROOT = process.env.VIDEO_STORAGE_ROOT || '/video52000/videos';
const PORT = Number(process.env.PORT || 51999);
const DEFAULT_SYNC_PORT = Number(process.env.DEFAULT_SYNC_PORT || 52000);
const MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB || 2048);
const REMOTE_ACME = '/video52000/acme.sh';
const DEFAULT_ADMIN_USERNAME = String(process.env.DEFAULT_ADMIN_USERNAME || 'admin').trim() || 'admin';
const DEFAULT_ADMIN_PASSWORD = String(process.env.DEFAULT_ADMIN_PASSWORD || 'change-me-now');
const DEFAULT_ADMIN_DISPLAY_NAME = String(process.env.DEFAULT_ADMIN_DISPLAY_NAME || DEFAULT_ADMIN_USERNAME).trim() || DEFAULT_ADMIN_USERNAME;

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(MEDIA_COVER_DIR, { recursive: true });
fs.mkdirSync(STORAGE_ROOT, { recursive: true });

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024 }
});
const localDownloadTasks = new Map();
const MEDIA_METADATA_FETCH_LIMIT = 8 * 1024 * 1024;
const MEDIA_LRC_FETCH_LIMIT = 256 * 1024;
const MEDIA_LYRICS_TEXT_LIMIT = 30000;
const MEDIA_LYRICS_LINE_LIMIT = 600;
const MEDIA_COVER_TTL_MS = 30 * 60 * 1000;
const MEDIA_NETEASE_LYRIC_TTL_MS = 30 * 60 * 1000;
const mediaCoverCache = new Map();
const neteaseLyricsCache = new Map();
let musicMetadataImport = null;

function randomId(prefix) {
  return `${prefix}_${crypto.randomBytes(10).toString('hex')}`;
}

function now() {
  return new Date().toISOString();
}

function readJson(file, fallback) {
  if (!fs.existsSync(file)) return fallback;
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function writeJson(file, value) {
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value, null, 2));
  fs.renameSync(tmp, file);
}

function loadSecrets() {
  const existing = readJson(SECRET_FILE, null);
  if (existing?.sessionSecret && existing?.nodeSecret && existing?.encryptionKey) return existing;
  const generated = {
    sessionSecret: crypto.randomBytes(48).toString('hex'),
    nodeSecret: crypto.randomBytes(48).toString('hex'),
    encryptionKey: crypto.randomBytes(32).toString('hex'),
    createdAt: now()
  };
  writeJson(SECRET_FILE, generated);
  return generated;
}

const secrets = loadSecrets();

function loadDb() {
  return readJson(DB_FILE, {
    users: [],
    rooms: [],
    syncNodes: [],
    storageNodes: [],
    authMethods: [],
    dnsProviders: [],
    installJobs: [],
    updateJobs: []
  });
}

let db = loadDb();

function saveDb() {
  writeJson(DB_FILE, db);
}

function publicUser(user) {
  return user && {
    id: user.id,
    username: user.username,
    displayName: user.displayName || user.username,
    role: user.role,
    disabled: Boolean(user.disabled),
    createdAt: user.createdAt
  };
}

function ensureSeedData() {
  if (!Array.isArray(db.dnsProviders)) db.dnsProviders = [];
  if (!Array.isArray(db.authMethods)) db.authMethods = [];
  if (!Array.isArray(db.installJobs)) db.installJobs = [];
  if (!Array.isArray(db.updateJobs)) db.updateJobs = [];
  if (!Array.isArray(db.rooms)) db.rooms = [];
  if (!Array.isArray(db.users)) db.users = [];

  if (!db.users.some((user) => user.username.toLowerCase() === DEFAULT_ADMIN_USERNAME.toLowerCase())) {
    db.users.push({
      id: randomId('usr'),
      username: DEFAULT_ADMIN_USERNAME,
      displayName: DEFAULT_ADMIN_DISPLAY_NAME,
      passwordHash: bcrypt.hashSync(DEFAULT_ADMIN_PASSWORD, 12),
      role: 'admin',
      createdAt: now()
    });
  }

  if (!db.syncNodes.length) {
    db.syncNodes.push({
      id: 'sync_default_52000',
      name: '默认 sync node',
      url: '',
      port: DEFAULT_SYNC_PORT,
      enabled: true,
      isDefault: true,
      createdAt: now(),
      updatedAt: now(),
      lastStatus: null
    });
  }

  if (!db.storageNodes.length) {
    db.storageNodes.push({
      id: 'storage_local',
      name: '本机存储 /video52000/videos',
      type: 'local',
      url: '',
      path: STORAGE_ROOT,
      enabled: true,
      createdAt: now(),
      updatedAt: now(),
      lastStatus: null
    });
  }

  ensureDefaultSyncNode();
  saveDb();
}

function ensureDefaultSyncNode() {
  const enabled = db.syncNodes.filter((node) => node.enabled);
  if (!enabled.length) return;
  const current = enabled.find((node) => node.isDefault);
  if (!current) enabled[0].isDefault = true;
  const defaultId = enabled.find((node) => node.isDefault)?.id;
  db.syncNodes.forEach((node) => {
    node.isDefault = node.id === defaultId;
  });
}

ensureSeedData();

function normalizeUsername(username) {
  const value = String(username || '').trim();
  if (!/^[\p{L}\p{N}_-]{2,32}$/u.test(value)) {
    throw Object.assign(new Error('用户名需要 2-32 位，只能包含文字、数字、下划线或短横线。'), { status: 400 });
  }
  return value;
}

function assertPassword(password, confirmPassword, requireConfirm) {
  const value = String(password || '');
  if (value.length < 4 || value.length > 128) {
    throw Object.assign(new Error('密码需要 4-128 位。'), { status: 400 });
  }
  if (requireConfirm && value !== String(confirmPassword || '')) {
    throw Object.assign(new Error('两次输入的密码不一致。'), { status: 400 });
  }
  return value;
}

function getUserById(id) {
  return db.users.find((user) => user.id === id);
}

function requireAuth(req, res, next) {
  const user = getUserById(req.session.userId);
  if (!user) return res.status(401).json({ error: '请先登录。' });
  req.user = user;
  next();
}

function requireAdmin(req, res, next) {
  if (req.user?.role !== 'admin') return res.status(403).json({ error: '需要管理员权限。' });
  next();
}

function asyncRoute(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function requestOrigin(req) {
  const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'http').split(',')[0].trim();
  const host = String(req.headers['x-forwarded-host'] || req.get('host') || `127.0.0.1:${PORT}`).split(',')[0].trim();
  return `${proto}://${host}`;
}

function inferredNodeBase(req, port) {
  const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'http').split(',')[0].trim();
  const host = String(req.headers['x-forwarded-host'] || req.get('host') || `127.0.0.1:${PORT}`).split(',')[0].trim();
  const hostname = host.replace(/:\d+$/, '');
  return `${proto}://${hostname}:${port || DEFAULT_SYNC_PORT}`;
}

function normalizeBaseUrl(value) {
  const url = String(value || '').trim().replace(/\/+$/, '');
  if (!url) return '';
  if (!/^https?:\/\//i.test(url)) {
    throw Object.assign(new Error('节点地址必须以 http:// 或 https:// 开头。'), { status: 400 });
  }
  return url;
}

function cleanBindDomain(value) {
  return String(value || '')
    .trim()
    .replace(/^https?:\/\//i, '')
    .replace(/\/.*$/, '')
    .replace(/:\d+$/, '')
    .toLowerCase();
}

function acmeContactEmail(domain) {
  const asciiDomain = domainToASCII(cleanBindDomain(domain)).replace(/^\*\./, '');
  if (!asciiDomain || asciiDomain.endsWith('.example.com') || asciiDomain === 'example.com') {
    throw Object.assign(new Error('自动申请 SSL 需要使用真实绑定域名生成 acme.sh 账户邮箱。'), { status: 400 });
  }
  return `admin@${asciiDomain}`;
}

function normalizePemInput(value) {
  return String(value || '').trim().replace(/\r\n/g, '\n');
}

function validateTlsPem(cert, key) {
  if (!/-----BEGIN CERTIFICATE-----[\s\S]+-----END CERTIFICATE-----/.test(cert)) {
    throw Object.assign(new Error('SSL 证书必须是 PEM 格式的 fullchain/cert。'), { status: 400 });
  }
  if (!/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]+-----END [A-Z ]*PRIVATE KEY-----/.test(key)) {
    throw Object.assign(new Error('SSL 私钥必须是 PEM 格式的 private key。'), { status: 400 });
  }
}

function normalizeAcmeDnsProvider(value) {
  const provider = String(value || '').trim();
  if (!provider) return '';
  if (!/^dns_[A-Za-z0-9_]+$/.test(provider)) {
    throw Object.assign(new Error('DNS API 名称必须是 acme.sh 格式，例如 dns_cf、dns_ali、dns_dp。'), { status: 400 });
  }
  return provider;
}

function parseAcmeDnsEnv(value) {
  const text = String(value || '').replace(/\r\n/g, '\n').trim();
  if (!text) return [];
  return text.split('\n').map((line, index) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) return null;
    const eq = trimmed.indexOf('=');
    if (eq <= 0) {
      throw Object.assign(new Error(`DNS API 环境变量第 ${index + 1} 行格式错误，应为 KEY=VALUE。`), { status: 400 });
    }
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      throw Object.assign(new Error(`DNS API 环境变量 ${key} 名称不合法。`), { status: 400 });
    }
    if (!val) {
      throw Object.assign(new Error(`DNS API 环境变量 ${key} 的值不能为空。`), { status: 400 });
    }
    return [key, val];
  }).filter(Boolean);
}

function normalizeSslMode(value, hasManualCertificate) {
  const mode = String(value || '').trim();
  if (['file', 'dns', 'manual'].includes(mode)) return mode;
  return hasManualCertificate ? 'manual' : 'dns';
}

function boolFromBody(value, fallback = false) {
  if (value === undefined || value === null) return fallback;
  return Boolean(value);
}

function normalizeDnsProviderPayload(body, existing = null) {
  const type = body.type === 'huaweicloud_intl' ? 'huaweicloud_intl' : 'dnspod';
  const config = { ...(existing?.config || {}) };
  const secret = existing?.secretCipher ? JSON.parse(decryptSecret(existing.secretCipher) || '{}') : {};
  if (type === 'dnspod') {
    config.tokenId = String(body.tokenId ?? config.tokenId ?? '').trim();
    config.endpoint = String(body.endpoint ?? config.endpoint ?? 'https://api.dnspod.com').trim().replace(/\/+$/, '');
    config.zone = String(body.zone ?? config.zone ?? '').trim();
    if (body.token !== undefined && String(body.token || '').trim()) secret.token = String(body.token).trim();
    if (!config.tokenId) throw Object.assign(new Error('DNSPod 需要填写 Token ID。'), { status: 400 });
    if (!secret.token) throw Object.assign(new Error('DNSPod 需要填写 Token。'), { status: 400 });
  } else {
    config.username = String(body.username ?? config.username ?? '').trim();
    config.domainName = String(body.domainName ?? config.domainName ?? '').trim();
    config.projectName = String(body.projectName ?? config.projectName ?? '').trim();
    config.region = String(body.region ?? config.region ?? '').trim();
    config.iamEndpoint = String(body.iamEndpoint ?? config.iamEndpoint ?? 'https://iam.myhuaweicloud.com').trim().replace(/\/+$/, '');
    config.dnsEndpoint = String(body.dnsEndpoint ?? config.dnsEndpoint ?? '').trim().replace(/\/+$/, '');
    config.zone = String(body.zone ?? config.zone ?? '').trim();
    if (body.password !== undefined && String(body.password || '').trim()) secret.password = String(body.password);
    if (!config.username || !config.domainName || !config.projectName || !config.dnsEndpoint) {
      throw Object.assign(new Error('华为云国际站需要填写用户名、账号名、项目名和 DNS Endpoint。'), { status: 400 });
    }
    if (!secret.password) throw Object.assign(new Error('华为云国际站需要填写密码。'), { status: 400 });
  }
  return {
    type,
    config,
    secretCipher: encryptSecret(JSON.stringify(secret))
  };
}

function dnsProviderToAcme(provider) {
  const secret = JSON.parse(decryptSecret(provider.secretCipher) || '{}');
  if (provider.type === 'dnspod') {
    return {
      sslDnsProvider: 'dns_dp',
      sslDnsEnv: [
        ['DP_Id', provider.config.tokenId],
        ['DP_Key', secret.token]
      ]
    };
  }
  return {
    sslDnsProvider: 'dns_huaweicloud',
    sslDnsEnv: [
      ['HUAWEICLOUD_Username', provider.config.username],
      ['HUAWEICLOUD_Password', secret.password],
      ['HUAWEICLOUD_DomainName', provider.config.domainName]
    ]
  };
}

function buildInstallPublicUrl({ publicUrl, bindDomain, host, servicePort, useSsl }) {
  const existing = String(publicUrl || '').trim();
  if (existing) return normalizeBaseUrl(existing);
  const domain = cleanBindDomain(bindDomain);
  const target = domain || String(host || '').trim();
  if (!target) throw Object.assign(new Error('服务器 IP/域名不能为空。'), { status: 400 });
  const scheme = useSsl ? 'https' : 'http';
  return normalizeBaseUrl(`${scheme}://${target}:${Number(servicePort || DEFAULT_SYNC_PORT)}`);
}

async function resolveHostAddresses(host, label = '主机') {
  const value = cleanBindDomain(host) || String(host || '').trim();
  if (!value) return [];
  if (net.isIP(value)) return [value];
  try {
    return (await dns.lookup(value, { all: true })).map((entry) => entry.address);
  } catch (error) {
    throw Object.assign(new Error(`${label} ${value} 无法解析：${error.message}`), { status: 400 });
  }
}

async function assertBindDomainTargetsHost(bindDomain, host, servicePort = DEFAULT_SYNC_PORT) {
  const domain = cleanBindDomain(bindDomain);
  if (!domain) return;
  const route = await inspectBindDomainRoute(domain, host);
  if (route && !route.matched) {
    throw Object.assign(
      new Error(`绑定域名 ${domain} 当前解析到 ${route.domainAddresses.join(', ') || '空'}，不是安装服务器 ${host} (${route.hostAddresses.join(', ') || '无法解析'})。请先把域名 A/AAAA 记录解析到节点服务器 IP，并开放 ${servicePort} 端口。`),
      { status: 400 }
    );
  }
}

async function inspectBindDomainRoute(bindDomain, host) {
  const domain = cleanBindDomain(bindDomain);
  if (!domain) return null;
  const domainAddresses = await resolveHostAddresses(domain, '绑定域名');
  const hostAddresses = await resolveHostAddresses(host, '安装服务器');
  return {
    domain,
    host: String(host || '').trim(),
    domainAddresses,
    hostAddresses,
    matched: domainAddresses.some((address) => hostAddresses.includes(address))
  };
}

function domainRouteLog(route) {
  if (!route || route.matched) return '';
  return `绑定域名 ${route.domain} 解析到 ${route.domainAddresses.join(', ') || '空'}，与安装服务器 ${route.host} (${route.hostAddresses.join(', ') || '无法解析'}) 不一致；按反代/公开地址模式继续，最终以公开地址连通性测试为准`;
}

function sameBaseUrl(a, b) {
  return normalizeBaseUrl(a || '').toLowerCase() === normalizeBaseUrl(b || '').toLowerCase();
}

function toWsBase(url) {
  return url.replace(/^http:/i, 'ws:').replace(/^https:/i, 'wss:');
}

function safeSyncNode(req, node) {
  const url = node.url ? normalizeBaseUrl(node.url) : inferredNodeBase(req, node.port || DEFAULT_SYNC_PORT);
  return {
    id: node.id,
    name: node.name,
    url,
    wsUrl: `${toWsBase(url)}/sync`,
    port: node.port || DEFAULT_SYNC_PORT,
    enabled: Boolean(node.enabled),
    isDefault: Boolean(node.isDefault),
    lastStatus: node.lastStatus || null,
    updatedAt: node.updatedAt,
    install: node.install || null
  };
}

function safeStorageNode(req, node) {
  const url = node.url ? normalizeBaseUrl(node.url) : requestOrigin(req);
  return {
    id: node.id,
    name: node.name,
    type: node.type || 'remote',
    url,
    enabled: Boolean(node.enabled),
    path: node.path || '/video52000/videos',
    lastStatus: node.lastStatus || null,
    updatedAt: node.updatedAt,
    install: node.install || null
  };
}

function safeDnsProvider(provider) {
  return {
    id: provider.id,
    name: provider.name,
    type: provider.type,
    enabled: provider.enabled !== false,
    config: provider.config || {},
    hasSecret: Boolean(provider.secretCipher),
    lastStatus: provider.lastStatus || null,
    createdAt: provider.createdAt,
    updatedAt: provider.updatedAt
  };
}

function safeAdminUser(user) {
  return {
    ...publicUser(user),
    updatedAt: user.updatedAt || user.createdAt,
    roomCount: db.rooms.filter((room) => room.ownerId === user.id).length
  };
}

function safeAdminRoom(room) {
  const owner = db.users.find((user) => user.id === room.ownerId);
  const syncNode = db.syncNodes.find((node) => node.id === room.syncNodeId);
  return {
    ...room,
    ownerName: owner?.username || 'unknown',
    syncNodeName: syncNode?.name || '未知节点',
    syncNodeEnabled: Boolean(syncNode?.enabled)
  };
}

function signRoomToken(roomId, user, ttlMs = 24 * 60 * 60 * 1000) {
  const payload = {
    roomId,
    userId: user.id,
    username: user.displayName || user.username,
    exp: Date.now() + ttlMs
  };
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = crypto.createHmac('sha256', secrets.nodeSecret).update(body).digest('base64url');
  return `${body}.${sig}`;
}

function encryptSecret(value) {
  const iv = crypto.randomBytes(12);
  const key = Buffer.from(secrets.encryptionKey, 'hex');
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(String(value || ''), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64url')}.${tag.toString('base64url')}.${encrypted.toString('base64url')}`;
}

function decryptSecret(value) {
  const [ivPart, tagPart, dataPart] = String(value || '').split('.');
  if (!ivPart || !tagPart || !dataPart) return '';
  const key = Buffer.from(secrets.encryptionKey, 'hex');
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(ivPart, 'base64url'));
  decipher.setAuthTag(Buffer.from(tagPart, 'base64url'));
  return Buffer.concat([
    decipher.update(Buffer.from(dataPart, 'base64url')),
    decipher.final()
  ]).toString('utf8');
}

function cleanRelativePath(input) {
  const normalized = path.posix.normalize(`/${String(input || '').replace(/\\/g, '/')}`).replace(/^\/+/, '');
  if (!normalized || normalized === '.') return '';
  if (normalized.startsWith('../') || normalized === '..') {
    throw Object.assign(new Error('路径不合法。'), { status: 400 });
  }
  return normalized;
}

function fullStoragePath(root, rel) {
  const base = path.resolve(root);
  const full = path.resolve(base, rel);
  if (full !== base && !full.startsWith(`${base}${path.sep}`)) {
    throw Object.assign(new Error('路径越界。'), { status: 400 });
  }
  return full;
}

function encodePathForUrl(rel) {
  return cleanRelativePath(rel).split('/').filter(Boolean).map(encodeURIComponent).join('/');
}

function publicVideoUrl(req, node, rel) {
  const encoded = encodePathForUrl(rel);
  if ((node.type || 'remote') === 'local') return `/videos/${encoded}`;
  return `${safeStorageNode(req, node).url}/videos/${encoded}`;
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

function normalizeDownloadUrl(value) {
  const text = String(value || '').trim();
  if (/^magnet:\?xt=/i.test(text)) return text.slice(0, 4096);
  let parsed;
  try {
    parsed = new URL(text);
  } catch {
    throw Object.assign(new Error('下载地址必须是 http(s) 直链或 magnet 磁力链接。'), { status: 400 });
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw Object.assign(new Error('下载地址必须是 http(s) 直链或 magnet 磁力链接。'), { status: 400 });
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

function titleFromName(value) {
  const name = path.basename(String(value || '').split('?')[0] || '').replace(/\.[^.]+$/, '');
  return name || '未命名音乐';
}

async function loadMusicMetadata() {
  if (!musicMetadataImport) musicMetadataImport = import('music-metadata');
  return musicMetadataImport;
}

function cleanupMediaCoverCache() {
  const nowMs = Date.now();
  for (const [id, item] of mediaCoverCache) {
    if (!item || item.expiresAt <= nowMs) mediaCoverCache.delete(id);
  }
}

function mediaCoverExt(mime) {
  const value = String(mime || '').toLowerCase();
  if (value.includes('png')) return '.png';
  if (value.includes('webp')) return '.webp';
  if (value.includes('gif')) return '.gif';
  return '.jpg';
}

function mediaCoverMimeFromName(name) {
  const ext = path.extname(String(name || '')).toLowerCase();
  if (ext === '.png') return 'image/png';
  if (ext === '.webp') return 'image/webp';
  if (ext === '.gif') return 'image/gif';
  return 'image/jpeg';
}

function storeMediaCover(picture) {
  if (!picture?.data?.length) return '';
  cleanupMediaCoverCache();
  const data = Buffer.from(picture.data);
  const id = `${crypto.createHash('sha256').update(data).digest('hex')}${mediaCoverExt(picture.format)}`;
  const file = path.join(MEDIA_COVER_DIR, id);
  try {
    if (!fs.existsSync(file)) fs.writeFileSync(file, data);
  } catch {
    return '';
  }
  mediaCoverCache.set(id, {
    data,
    mime: picture.format || 'image/jpeg',
    expiresAt: Date.now() + MEDIA_COVER_TTL_MS
  });
  return `/api/media/covers/${encodeURIComponent(id)}`;
}

function clampLyricsText(value, limit = MEDIA_LYRICS_TEXT_LIMIT) {
  return String(value || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .trim()
    .slice(0, limit);
}

function normalizeLyricsLines(lines, timestampScale = 1) {
  if (!Array.isArray(lines)) return [];
  return lines
    .map((line) => {
      const rawTime = Number(line?.time ?? line?.timestamp);
      if (!Number.isFinite(rawTime)) return null;
      const text = clampLyricsText(line?.text, 240);
      if (!text) return null;
      return {
        time: Math.max(0, rawTime * timestampScale),
        text
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.time - b.time)
    .slice(0, MEDIA_LYRICS_LINE_LIMIT);
}

function parseLrcText(value, source = 'lrc') {
  const text = clampLyricsText(value);
  if (!text) return null;
  const timestampRe = /\[(\d{1,3}):(\d{2})(?:[.:](\d{1,3}))?\]/g;
  const lines = [];
  for (const rawLine of text.split('\n')) {
    const stamps = Array.from(rawLine.matchAll(timestampRe));
    if (!stamps.length) continue;
    const lyricText = clampLyricsText(rawLine.replace(timestampRe, ''), 240);
    if (!lyricText) continue;
    for (const stamp of stamps) {
      const minutes = Number(stamp[1]);
      const seconds = Number(stamp[2]);
      const fraction = stamp[3] || '0';
      const millis = fraction.length === 3 ? Number(fraction) : Number(fraction.padEnd(2, '0')) * 10;
      if (!Number.isFinite(minutes) || !Number.isFinite(seconds) || !Number.isFinite(millis)) continue;
      lines.push({ time: minutes * 60 + seconds + millis / 1000, text: lyricText });
    }
  }
  const normalizedLines = normalizeLyricsLines(lines);
  if (normalizedLines.length) {
    return {
      type: 'synced',
      source,
      text: normalizedLines.map((line) => line.text).join('\n').slice(0, MEDIA_LYRICS_TEXT_LIMIT),
      lines: normalizedLines
    };
  }
  const plain = clampLyricsText(text.replace(/^\[[a-z]+:.*\]\s*$/gim, ''));
  return plain ? { type: 'plain', source, text: plain, lines: [] } : null;
}

function normalizeLyricsTag(tag, source = 'embedded') {
  if (!tag) return null;
  if (typeof tag === 'string') {
    const text = clampLyricsText(tag);
    return text ? parseLrcText(text, source) || { type: 'plain', source, text, lines: [] } : null;
  }
  const syncedLines = normalizeLyricsLines(tag.syncText, 1 / 1000);
  if (syncedLines.length) {
    return {
      type: 'synced',
      source,
      text: clampLyricsText(tag.text || syncedLines.map((line) => line.text).join('\n')),
      lines: syncedLines
    };
  }
  const text = clampLyricsText(tag.text);
  if (!text) return null;
  return parseLrcText(text, source) || { type: 'plain', source, text, lines: [] };
}

function normalizeLyricsList(lyrics, source = 'embedded') {
  const values = Array.isArray(lyrics) ? lyrics : lyrics ? [lyrics] : [];
  let firstPlain = null;
  for (const item of values) {
    const normalized = normalizeLyricsTag(item, source);
    if (!normalized) continue;
    if (normalized.type === 'synced' && normalized.lines.length) return normalized;
    if (!firstPlain) firstPlain = normalized;
  }
  return firstPlain;
}

function collectTextValues(value, output = [], depth = 0) {
  if (output.length > 200 || depth > 5 || value === null || value === undefined) return output;
  if (typeof value === 'string' || typeof value === 'number') {
    const text = String(value).trim();
    if (text) output.push(text);
    return output;
  }
  if (Buffer.isBuffer(value) || ArrayBuffer.isView(value)) return output;
  if (Array.isArray(value)) {
    for (const item of value) collectTextValues(item, output, depth + 1);
    return output;
  }
  if (typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (key === 'picture' || key === 'data') continue;
      collectTextValues(item, output, depth + 1);
    }
  }
  return output;
}

function decodeNetease163Key(value) {
  const match = String(value || '').match(/163\s*key\s*\(Don't\s+modify\)\s*:\s*([A-Za-z0-9+/=]+)/i);
  if (!match) return null;
  try {
    const decipher = crypto.createDecipheriv('aes-128-ecb', Buffer.from("#14ljk_!\\]&0U<'("), null);
    decipher.setAutoPadding(true);
    const decrypted = Buffer.concat([
      decipher.update(Buffer.from(match[1].trim(), 'base64')),
      decipher.final()
    ]).toString('utf8');
    const jsonText = decrypted.replace(/^music:/, '').trim();
    return JSON.parse(jsonText);
  } catch {
    return null;
  }
}

function neteaseMetaFromParsed(parsed) {
  for (const value of collectTextValues({ native: parsed?.native, common: parsed?.common })) {
    const meta = decodeNetease163Key(value);
    if (meta?.musicId) return meta;
  }
  return null;
}

function cleanupNeteaseLyricsCache() {
  const nowMs = Date.now();
  for (const [id, item] of neteaseLyricsCache) {
    if (!item || item.expiresAt <= nowMs) neteaseLyricsCache.delete(id);
  }
}

async function fetchNeteaseLyrics(musicId) {
  const id = String(musicId || '').trim();
  if (!/^\d+$/.test(id)) return null;
  cleanupNeteaseLyricsCache();
  const cached = neteaseLyricsCache.get(id);
  if (cached) return cached.lyrics;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 7000);
  try {
    const response = await fetch(`https://music.163.com/api/song/lyric?id=${encodeURIComponent(id)}&lv=-1&kv=-1&tv=-1`, {
      headers: {
        accept: 'application/json,text/plain,*/*',
        referer: 'https://music.163.com/',
        'user-agent': 'Mozilla/5.0'
      },
      signal: controller.signal
    });
    if (!response.ok) return null;
    const body = await response.json().catch(() => null);
    const lyricText = body?.lrc?.lyric || body?.klyric?.lyric || '';
    const lyrics = parseLrcText(lyricText, 'netease');
    neteaseLyricsCache.set(id, {
      lyrics,
      expiresAt: Date.now() + MEDIA_NETEASE_LYRIC_TTL_MS
    });
    return lyrics;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function neteaseLyricsFromParsed(parsed) {
  return fetchNeteaseLyrics(neteaseMetaFromParsed(parsed)?.musicId);
}

function localLrcFileForMedia(localFile) {
  const parsed = path.parse(localFile);
  return path.join(parsed.dir, `${parsed.name}.lrc`);
}

async function readLocalLrc(localFile) {
  const lrcFile = localLrcFileForMedia(localFile);
  try {
    const stat = await fs.promises.stat(lrcFile);
    if (!stat.isFile() || stat.size > MEDIA_LRC_FETCH_LIMIT) return null;
    return parseLrcText(await fs.promises.readFile(lrcFile, 'utf8'), 'lrc');
  } catch {
    return null;
  }
}

function lrcUrlForMediaUrl(sourceUrl) {
  try {
    const parsed = new URL(sourceUrl);
    if (!/\.[^/]+$/.test(parsed.pathname)) return '';
    parsed.pathname = parsed.pathname.replace(/\.[^/.]+$/, '.lrc');
    parsed.search = '';
    return parsed.toString();
  } catch {
    return '';
  }
}

async function fetchLimitedText(urlString, limit = MEDIA_LRC_FETCH_LIMIT, redirects = 0) {
  if (redirects > 3) return '';
  const safeUrl = await assertPublicHttpUrl(urlString);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 7000);
  try {
    const response = await fetch(safeUrl, {
      headers: { range: `bytes=0-${limit - 1}` },
      redirect: 'manual',
      signal: controller.signal
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      const next = response.headers.get('location');
      return next ? fetchLimitedText(new URL(next, safeUrl).toString(), limit, redirects + 1) : '';
    }
    if (!response.ok && response.status !== 206) return '';
    return (await response.text()).slice(0, limit);
  } catch {
    return '';
  } finally {
    clearTimeout(timeout);
  }
}

async function readRemoteLrc(sourceUrl) {
  const lrcUrl = lrcUrlForMediaUrl(sourceUrl);
  if (!lrcUrl) return null;
  const text = await fetchLimitedText(lrcUrl);
  return parseLrcText(text, 'lrc');
}

function publicMetadataFromParsed(parsed, fallbackName, lyrics) {
  const common = parsed?.common || {};
  const format = parsed?.format || {};
  const normalizedLyrics = lyrics || normalizeLyricsList(common.lyrics, 'embedded');
  return {
    title: String(common.title || titleFromName(fallbackName)).slice(0, 160),
    artist: String(common.artist || '').slice(0, 120),
    album: String(common.album || '').slice(0, 120),
    duration: Number.isFinite(Number(format.duration)) ? Number(format.duration) : null,
    coverUrl: storeMediaCover((common.picture || [])[0]),
    sourceName: String(fallbackName || '').slice(0, 180),
    lyrics: normalizedLyrics || null
  };
}

function fallbackMediaMetadata(sourceName) {
  return {
    title: titleFromName(sourceName),
    artist: '',
    album: '',
    duration: null,
    coverUrl: '',
    sourceName: String(sourceName || '').slice(0, 180),
    lyrics: null
  };
}

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
  return a === 10
    || a === 127
    || a === 0
    || (a === 169 && b === 254)
    || (a === 172 && b >= 16 && b <= 31)
    || (a === 192 && b === 168)
    || (a === 100 && b >= 64 && b <= 127);
}

async function assertPublicHttpUrl(urlString) {
  let parsed;
  try {
    parsed = new URL(urlString);
  } catch {
    throw Object.assign(new Error('音频地址不合法。'), { status: 400 });
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw Object.assign(new Error('音频地址必须是 http(s)。'), { status: 400 });
  }
  const hostname = parsed.hostname.toLowerCase();
  if (!hostname || hostname === 'localhost' || hostname.endsWith('.localhost')) {
    throw Object.assign(new Error('音频地址不能指向本机。'), { status: 400 });
  }
  const addresses = net.isIP(hostname) ? [{ address: hostname }] : await dns.lookup(hostname, { all: true });
  if (!addresses.length || addresses.some((item) => isPrivateIpAddress(item.address))) {
    throw Object.assign(new Error('音频地址不能指向内网或本机地址。'), { status: 400 });
  }
  return parsed.toString();
}

async function fetchLimitedMediaBuffer(urlString, redirects = 0) {
  if (redirects > 3) throw Object.assign(new Error('音频地址跳转过多。'), { status: 400 });
  const safeUrl = await assertPublicHttpUrl(urlString);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);
  try {
    const response = await fetch(safeUrl, {
      headers: { range: `bytes=0-${MEDIA_METADATA_FETCH_LIMIT - 1}` },
      redirect: 'manual',
      signal: controller.signal
    });
    if ([301, 302, 303, 307, 308].includes(response.status)) {
      const next = response.headers.get('location');
      if (!next) throw Object.assign(new Error('音频地址跳转缺少 Location。'), { status: 400 });
      return fetchLimitedMediaBuffer(new URL(next, safeUrl).toString(), redirects + 1);
    }
    if (!response.ok && response.status !== 206) {
      throw Object.assign(new Error(`音频地址无法读取。HTTP ${response.status}`), { status: 400 });
    }
    if (!response.body) return { buffer: Buffer.alloc(0), contentType: response.headers.get('content-type') || '' };
    const chunks = [];
    let total = 0;
    for await (const chunk of response.body) {
      const buffer = Buffer.from(chunk);
      total += buffer.length;
      if (total > MEDIA_METADATA_FETCH_LIMIT) break;
      chunks.push(buffer);
    }
    return {
      buffer: Buffer.concat(chunks),
      contentType: response.headers.get('content-type') || ''
    };
  } finally {
    clearTimeout(timeout);
  }
}

function localVideoFileFromUrl(req, value) {
  const input = String(value || '').trim();
  if (!input) return null;
  let parsed;
  try {
    parsed = new URL(input, requestOrigin(req));
  } catch {
    return null;
  }
  if (parsed.origin !== requestOrigin(req) || !parsed.pathname.startsWith('/videos/')) return null;
  const rel = cleanRelativePath(decodeURIComponent(parsed.pathname.replace(/^\/videos\/?/, '')));
  return rel ? fullStoragePath(STORAGE_ROOT, rel) : null;
}

async function readMediaMetadata(req, sourceUrl, sourceName) {
  const fallback = fallbackMediaMetadata(sourceName || filenameFromUrl(sourceUrl));
  try {
    const metadata = await loadMusicMetadata();
    const localFile = localVideoFileFromUrl(req, sourceUrl);
    if (localFile && fs.existsSync(localFile) && fs.statSync(localFile).isFile()) {
      const parsed = await metadata.parseFile(localFile, { duration: true, skipCovers: false });
      const lyrics = normalizeLyricsList(parsed?.common?.lyrics, 'embedded') || await readLocalLrc(localFile) || await neteaseLyricsFromParsed(parsed);
      return publicMetadataFromParsed(parsed, sourceName || localFile, lyrics);
    }
    const remote = await fetchLimitedMediaBuffer(sourceUrl);
    if (!remote.buffer.length) return fallback;
    const parsed = await metadata.parseBuffer(remote.buffer, remote.contentType, { duration: true, skipCovers: false });
    const lyrics = normalizeLyricsList(parsed?.common?.lyrics, 'embedded') || await readRemoteLrc(sourceUrl) || await neteaseLyricsFromParsed(parsed);
    return publicMetadataFromParsed(parsed, sourceName || filenameFromUrl(sourceUrl), lyrics);
  } catch {
    return fallback;
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

async function runLocalHttpDownload(task, storageRoot) {
  task.method = 'node';
  task.status = 'running';
  task.updatedAt = now();
  const response = await fetch(task.url);
  if (!response.ok || !response.body) throw new Error(`直链下载失败：HTTP ${response.status}`);
  const headerName = filenameFromDisposition(response.headers.get('content-disposition'));
  const filename = safeFilename(task.filename || headerName || filenameFromUrl(task.url));
  const dirRel = cleanRelativePath(task.path || '');
  const dir = fullStoragePath(storageRoot, dirRel);
  fs.mkdirSync(dir, { recursive: true });
  const file = fullStoragePath(storageRoot, cleanRelativePath(path.posix.join(dirRel, filename)));
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

function runLocalAria2Download(task, storageRoot) {
  return new Promise((resolve, reject) => {
    task.method = 'aria2c';
    task.status = 'running';
    task.updatedAt = now();
    const dirRel = cleanRelativePath(task.path || '');
    const dir = fullStoragePath(storageRoot, dirRel);
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

async function runLocalDownloadTask(task, storageRoot) {
  try {
    if (/^magnet:/i.test(task.url)) {
      await runLocalAria2Download(task, storageRoot);
    } else {
      try {
        await runLocalAria2Download(task, storageRoot);
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
        appendDownloadLog(task, 'aria2c 未安装，改用内置直链下载。');
        await runLocalHttpDownload(task, storageRoot);
      }
    }
    finishDownloadTask(task, 'success');
  } catch (error) {
    const message = error.code === 'ENOENT'
      ? '本机未安装 aria2c，磁力下载需要先安装 aria2。'
      : error.message;
    appendDownloadLog(task, message);
    finishDownloadTask(task, 'failed', message);
  }
}

function createLocalDownloadTask(node, body) {
  const url = normalizeDownloadUrl(body.url);
  const task = {
    id: randomId('dl'),
    nodeId: node.id,
    nodeName: node.name,
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
  localDownloadTasks.set(task.id, task);
  while (localDownloadTasks.size > 40) {
    const oldest = Array.from(localDownloadTasks.values()).sort((a, b) => a.createdAt.localeCompare(b.createdAt))[0];
    if (!oldest || oldest.status === 'running') break;
    localDownloadTasks.delete(oldest.id);
  }
  setImmediate(() => runLocalDownloadTask(task, node.path || STORAGE_ROOT));
  return publicDownloadTask(task);
}

function listLocalStorage(req, node, rel) {
  const root = node.path || STORAGE_ROOT;
  const safeRel = cleanRelativePath(rel);
  const dir = fullStoragePath(root, safeRel);
  fs.mkdirSync(dir, { recursive: true });
  const entries = fs.readdirSync(dir, { withFileTypes: true }).map((entry) => {
    const childRel = cleanRelativePath(path.posix.join(safeRel, entry.name));
    const stat = fs.statSync(path.join(dir, entry.name));
    const mediaType = entry.isFile() ? mediaTypeForFile(entry.name) : '';
    const mediaUrl = entry.isFile() ? publicVideoUrl(req, node, childRel) : null;
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

async function fetchRemoteJson(node, endpoint, options = {}) {
  const url = `${normalizeBaseUrl(node.url)}${endpoint}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeout || 10000);
  try {
    const headers = {
      'x-node-token': secrets.nodeSecret,
      ...(options.headers || {})
    };
    const response = await fetch(url, { ...options, headers, signal: controller.signal });
    const text = await response.text();
    let body = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      const compact = text.replace(/\s+/g, ' ').trim().slice(0, 200);
      throw Object.assign(
        new Error(`远程节点返回了非 JSON 内容，可能节点地址没有指向 storage API 或反代返回了 HTML。HTTP ${response.status}: ${compact || response.statusText}`),
        { status: response.status || 502 }
      );
    }
    if (!response.ok) {
      throw Object.assign(new Error(body.error || `远程节点返回 HTTP ${response.status}`), { status: response.status });
    }
    return body;
  } finally {
    clearTimeout(timeout);
  }
}

async function listStorageNode(req, node, rel) {
  if ((node.type || 'remote') === 'local') return listLocalStorage(req, node, rel);
  const data = await fetchRemoteJson(node, `/storage/list?path=${encodeURIComponent(cleanRelativePath(rel))}`);
  return {
    path: data.path || '',
    entries: (data.entries || []).map((entry) => {
      const mediaType = entry.mediaType || (entry.type === 'file' ? mediaTypeForFile(entry.name || entry.path || '') : '');
      const mediaUrl = entry.type === 'file' ? `${safeStorageNode(req, node).url}/videos/${encodePathForUrl(entry.path)}` : null;
      return {
        ...entry,
        isVideo: Boolean(entry.isVideo || mediaType === 'video'),
        isAudio: Boolean(entry.isAudio || mediaType === 'audio'),
        isMedia: Boolean(mediaType),
        mediaType,
        mediaUrl,
        videoUrl: mediaUrl
      };
    })
  };
}

async function dnspodRequest(provider, action, params = {}) {
  const secret = JSON.parse(decryptSecret(provider.secretCipher) || '{}');
  const endpoint = (provider.config.endpoint || 'https://api.dnspod.com').replace(/\/+$/, '');
  const body = new URLSearchParams({
    login_token: `${provider.config.tokenId},${secret.token}`,
    format: 'json',
    ...params
  });
  const response = await fetch(`${endpoint}/${action}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
      'user-agent': 'VideoTogether/1.0 (admin@video.local)'
    },
    body
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.status?.code !== '1') {
    throw new Error(data.status?.message || `DNSPod HTTP ${response.status}`);
  }
  return data;
}

async function getHuaweiToken(provider) {
  const secret = JSON.parse(decryptSecret(provider.secretCipher) || '{}');
  const response = await fetch(`${provider.config.iamEndpoint}/v3/auth/tokens`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      auth: {
        identity: {
          methods: ['password'],
          password: {
            user: {
              name: provider.config.username,
              password: secret.password,
              domain: { name: provider.config.domainName }
            }
          }
        },
        scope: {
          project: { name: provider.config.projectName }
        }
      }
    })
  });
  const token = response.headers.get('x-subject-token');
  const body = await response.json().catch(() => ({}));
  if (!response.ok || !token) throw new Error(body.error?.message || `Huawei IAM HTTP ${response.status}`);
  return token;
}

async function huaweiDnsRequest(provider, pathName, options = {}) {
  const token = await getHuaweiToken(provider);
  const response = await fetch(`${provider.config.dnsEndpoint}${pathName}`, {
    ...options,
    headers: {
      'content-type': 'application/json',
      'X-Auth-Token': token,
      ...(options.headers || {})
    }
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.message || body.error?.message || `Huawei DNS HTTP ${response.status}`);
  return body;
}

async function testDnsProvider(provider) {
  if (provider.type === 'dnspod') {
    const data = await dnspodRequest(provider, 'Domain.List', { length: '5' });
    return { ok: true, domains: data.domains?.slice?.(0, 5) || [], raw: { domainCount: data.info?.domain_total || data.domains?.length || 0 } };
  }
  const data = await huaweiDnsRequest(provider, '/v2/zones?type=public&limit=5');
  return { ok: true, zones: data.zones || [], raw: { zoneCount: data.metadata?.total_count || data.zones?.length || 0 } };
}

function safeAuthMethod(method) {
  return {
    id: method.id,
    name: method.name,
    username: method.username,
    mode: method.mode,
    hasSecret: Boolean(method.secretCipher),
    createdAt: method.createdAt,
    updatedAt: method.updatedAt
  };
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function normalizeSshError(error, mode) {
  if (mode === 'password' && /All configured authentication methods failed/i.test(error.message)) {
    return new Error('SSH 认证失败：已尝试 password 和 keyboard-interactive，请确认目标机允许该用户用密码登录。');
  }
  return error;
}

function sshConnect({ host, port, username, mode, secret }) {
  return new Promise((resolve, reject) => {
    const conn = new Client();
    let settled = false;
    const config = {
      host,
      port: Number(port || 22),
      username,
      readyTimeout: 20000
    };
    if (mode === 'key') {
      config.privateKey = secret;
    } else {
      config.password = secret;
      config.tryKeyboard = true;
      conn.on('keyboard-interactive', (name, instructions, lang, prompts, finish) => {
        finish(prompts.map(() => secret));
      });
    }
    conn.on('error', (error) => {
      if (!settled) {
        settled = true;
        reject(normalizeSshError(error, mode));
      } else {
        console.warn(`[ssh] ${host}:${Number(port || 22)} ${error.message}`);
      }
    });
    conn.once('ready', () => {
      if (settled) return;
      settled = true;
      resolve(conn);
    });
    try {
      conn.connect(config);
    } catch (error) {
      if (!settled) {
        settled = true;
        reject(normalizeSshError(error, mode));
      }
    }
  });
}

function sshExec(conn, command, options = {}) {
  return new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    let finished = false;
    let timer;
    const cleanup = () => {
      clearTimeout(timer);
      conn.removeListener('error', onConnError);
    };
    const finishReject = (error) => {
      if (finished) return;
      finished = true;
      cleanup();
      reject(error);
    };
    const finishResolve = (result) => {
      if (finished) return;
      finished = true;
      cleanup();
      resolve(result);
    };
    const onConnError = (error) => {
      finishReject(new Error(`SSH 连接中断: ${error.message}`));
    };
    conn.once('error', onConnError);
    timer = setTimeout(() => finishReject(new Error(`SSH 命令超时: ${command}`)), options.timeout || 120000);
    conn.exec(command, (err, stream) => {
      if (err) {
        finishReject(err);
        return;
      }
      stream.on('error', finishReject);
      stream.on('close', (code) => {
        const result = { code, stdout, stderr };
        if (code && !options.allowFailure) {
          const message = stderr.trim() || stdout.trim() || `SSH 命令失败: ${command}`;
          finishReject(Object.assign(new Error(message), result));
        } else {
          finishResolve(result);
        }
      });
      stream.on('data', (data) => {
        stdout += data.toString();
      });
      stream.stderr.on('data', (data) => {
        stderr += data.toString();
      });
    });
  });
}

function sftpWrite(conn, remotePath, content, mode = 0o644) {
  return new Promise((resolve, reject) => {
    let finished = false;
    let sftpClient = null;
    const timer = setTimeout(() => finishReject(new Error(`SFTP 写入超时: ${remotePath}`)), 120000);
    const cleanup = () => {
      clearTimeout(timer);
      conn.removeListener('error', onConnError);
      if (sftpClient) sftpClient.end();
    };
    const finishReject = (error) => {
      if (finished) return;
      finished = true;
      cleanup();
      reject(error);
    };
    const finishResolve = () => {
      if (finished) return;
      finished = true;
      cleanup();
      resolve();
    };
    const onConnError = (error) => {
      finishReject(new Error(`SSH 连接中断: ${error.message}`));
    };
    conn.once('error', onConnError);
    conn.sftp((err, sftp) => {
      if (err) return finishReject(err);
      sftpClient = sftp;
      sftp.on('error', finishReject);
      sftp.writeFile(remotePath, content, { mode }, (writeErr) => {
        if (writeErr) finishReject(writeErr);
        else finishResolve();
      });
    });
  });
}

async function ensureRemoteCurl(conn) {
  await sshExec(conn, [
    'if command -v curl >/dev/null 2>&1; then',
    '  curl --version >/dev/null;',
    'elif command -v apt-get >/dev/null 2>&1; then',
    '  apt-get update && apt-get install -y ca-certificates curl;',
    'elif command -v dnf >/dev/null 2>&1; then',
    '  dnf install -y ca-certificates curl;',
    'elif command -v yum >/dev/null 2>&1; then',
    '  yum install -y ca-certificates curl;',
    'else',
    '  echo "Cannot install curl automatically on this OS" >&2; exit 14;',
    'fi'
  ].join(' '), { timeout: 600000 });
}

async function ensureRemoteNodeRuntime(conn) {
  await sshExec(conn, [
    'if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then',
    '  node -v && npm -v;',
    'elif command -v apt-get >/dev/null 2>&1; then',
    '  apt-get update && apt-get install -y ca-certificates curl gnupg;',
    '  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -;',
    '  apt-get install -y nodejs;',
    'elif command -v dnf >/dev/null 2>&1; then',
    '  dnf install -y nodejs npm;',
    'elif command -v yum >/dev/null 2>&1; then',
    '  curl -fsSL https://rpm.nodesource.com/setup_22.x | bash -;',
    '  yum install -y nodejs;',
    'else',
    '  echo "Cannot install Node.js automatically on this OS" >&2; exit 12;',
    'fi'
  ].join(' '), { timeout: 600000 });
}

async function ensureRemoteAria2(conn) {
  await sshExec(conn, [
    'if command -v aria2c >/dev/null 2>&1; then',
    '  aria2c --version >/dev/null;',
    'elif command -v apt-get >/dev/null 2>&1; then',
    '  apt-get update && apt-get install -y aria2;',
    'elif command -v dnf >/dev/null 2>&1; then',
    '  dnf install -y aria2;',
    'elif command -v yum >/dev/null 2>&1; then',
    '  yum install -y aria2;',
    'else',
    '  echo "Cannot install aria2 automatically on this OS" >&2; exit 13;',
    'fi'
  ].join(' '), { timeout: 600000 });
}

async function ensureRemoteCron(conn) {
  await sshExec(conn, [
    'if command -v crontab >/dev/null 2>&1; then',
    '  true;',
    'elif command -v apt-get >/dev/null 2>&1; then',
    '  apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y cron;',
    'elif command -v dnf >/dev/null 2>&1; then',
    '  dnf install -y cronie;',
    'elif command -v yum >/dev/null 2>&1; then',
    '  yum install -y cronie;',
    'else',
    '  echo "Cannot install crontab automatically on this OS" >&2; exit 16;',
    'fi;',
    'if command -v systemctl >/dev/null 2>&1; then',
    '  systemctl enable --now cron >/dev/null 2>&1 || systemctl enable --now crond >/dev/null 2>&1 || true;',
    'elif command -v service >/dev/null 2>&1; then',
    '  service cron start >/dev/null 2>&1 || service crond start >/dev/null 2>&1 || true;',
    'fi;',
    'command -v crontab >/dev/null 2>&1'
  ].join(' '), { timeout: 600000 });
}

async function ensureRemoteAcme(conn, requireCron = false, contactDomain = '') {
  await ensureRemoteCurl(conn);
  if (requireCron) await ensureRemoteCron(conn);
  const contactEmail = acmeContactEmail(contactDomain);
  const installArgs = [
    shellQuote(`email=${contactEmail}`),
    requireCron ? '' : '--force'
  ].filter(Boolean).join(' ');
  const command = [
    'mkdir -p /video52000;',
    `if [ ! -x ${shellQuote(REMOTE_ACME)} ]; then`,
    '  ACME_HOME="${HOME:-/root}/.acme.sh";',
    '  ACME="$ACME_HOME/acme.sh";',
    '  if [ ! -x "$ACME" ]; then',
    `    curl -fsSL https://get.acme.sh | sh -s ${installArgs};`,
    '  fi;',
    '  ACME_HOME="${HOME:-/root}/.acme.sh";',
    '  ACME="$ACME_HOME/acme.sh";',
    '  if [ ! -x "$ACME" ] && [ -x /root/.acme.sh/acme.sh ]; then ACME=/root/.acme.sh/acme.sh; fi;',
    '  if [ ! -x "$ACME" ]; then echo "acme.sh install failed: acme.sh executable not found" >&2; exit 15; fi;',
    `  ln -sf "$ACME" ${shellQuote(REMOTE_ACME)};`,
    'fi;',
    `CONTACT_EMAIL=${shellQuote(contactEmail)};`,
    'ACCOUNT_CONF="${HOME:-/root}/.acme.sh/account.conf";',
    'mkdir -p "$(dirname "$ACCOUNT_CONF")";',
    'touch "$ACCOUNT_CONF";',
    'if grep -q "^ACCOUNT_EMAIL=.*example\\.com" "$ACCOUNT_CONF"; then',
    '  sed -i "s|^ACCOUNT_EMAIL=.*|ACCOUNT_EMAIL=\'$CONTACT_EMAIL\'|" "$ACCOUNT_CONF";',
    'elif ! grep -q "^ACCOUNT_EMAIL=" "$ACCOUNT_CONF"; then',
    '  printf "\\nACCOUNT_EMAIL=\'%s\'\\n" "$CONTACT_EMAIL" >> "$ACCOUNT_CONF";',
    'fi;',
    `${shellQuote(REMOTE_ACME)} --set-default-ca --server letsencrypt >/dev/null;`,
    `${shellQuote(REMOTE_ACME)} --register-account --server letsencrypt >/dev/null`
  ].join(' ');
  await sshExec(conn, command, { timeout: 240000 });
}

function remoteNodeReloadCommand(installPath) {
  const appPath = installPath || '/video52000/app';
  return [
    'if command -v systemctl >/dev/null 2>&1 && [ -f /etc/systemd/system/video52000-node.service ]; then',
    '  systemctl restart video52000-node.service;',
    'elif [ -f /video52000/node.pid ]; then',
    '  kill "$(cat /video52000/node.pid)" >/dev/null 2>&1 || true;',
    `  cd ${shellQuote(appPath)} && (nohup /usr/bin/env node server.js >> /video52000/node.log 2>&1 & echo $! > /video52000/node.pid);`,
    'fi'
  ].join(' ');
}

async function ensureRemoteFileCertificate(conn, options, tlsDir) {
  const domain = options.bindDomain;
  const safeDomain = cleanBindDomain(domain);
  const asciiDomain = domainToASCII(safeDomain);
  if (!safeDomain || !asciiDomain) throw new Error('文件验证申请 SSL 需要填写绑定域名。');

  await ensureRemoteAcme(conn, Boolean(options.fileAutoRenew), asciiDomain);
  await ensureRemoteNodeRuntime(conn);
  await sshExec(conn, 'mkdir -p /video52000/acme-webroot/.well-known/acme-challenge /video52000/acme');

  const challengeServer = [
    "'use strict';",
    "const fs = require('fs');",
    "const http = require('http');",
    "const path = require('path');",
    "const root = '/video52000/acme-webroot/.well-known/acme-challenge';",
    'http.createServer((req, res) => {',
    "  const url = new URL(req.url, 'http://127.0.0.1');",
    "  if (!url.pathname.startsWith('/.well-known/acme-challenge/')) { res.writeHead(404); res.end('not found'); return; }",
    '  const token = path.basename(url.pathname);',
    "  if (!/^[A-Za-z0-9_-]+$/.test(token)) { res.writeHead(400); res.end('bad token'); return; }",
    "  fs.readFile(path.join(root, token), 'utf8', (err, body) => {",
    "    if (err) { res.writeHead(404); res.end('not found'); return; }",
    "    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });",
    '    res.end(body);',
    '  });',
    "}).listen(80, '0.0.0.0');",
    ''
  ].join('\n');
  await sftpWrite(conn, '/video52000/acme-http.js', challengeServer);

  const acmeService = [
    '[Unit]',
    'Description=Video52000 ACME HTTP file validation',
    'After=network.target',
    '',
    '[Service]',
    'Type=simple',
    'ExecStart=/usr/bin/env node /video52000/acme-http.js',
    'Restart=always',
    'RestartSec=2',
    '',
    '[Install]',
    'WantedBy=multi-user.target',
    ''
  ].join('\n');
  await sftpWrite(conn, '/tmp/video52000-acme-http.service', acmeService);
  const startResult = await sshExec(conn, [
    'if command -v systemctl >/dev/null 2>&1; then',
    '  mv /tmp/video52000-acme-http.service /etc/systemd/system/video52000-acme-http.service &&',
    '  systemctl daemon-reload && systemctl enable video52000-acme-http.service && systemctl restart video52000-acme-http.service;',
    'else',
    '  if [ -f /video52000/acme-http.pid ]; then kill "$(cat /video52000/acme-http.pid)" >/dev/null 2>&1 || true; fi;',
    '  cd /video52000 && (nohup /usr/bin/env node /video52000/acme-http.js >> /video52000/acme-http.log 2>&1 & echo $! > /video52000/acme-http.pid);',
    'fi'
  ].join(' '), { allowFailure: true, timeout: 120000 });
  if (startResult.code !== 0) {
    const detail = startResult.stderr || startResult.stdout || '';
    throw new Error(`文件验证服务启动失败：${detail.trim() || '请检查 80 端口是否被占用'}`);
  }

  const probeCommand = [
    'token="video52000-probe-$(date +%s)";',
    'mkdir -p /video52000/acme-webroot/.well-known/acme-challenge;',
    'printf ok > "/video52000/acme-webroot/.well-known/acme-challenge/$token";',
    'curl -fsS "http://127.0.0.1/.well-known/acme-challenge/$token" | grep -qx ok;',
    'rm -f "/video52000/acme-webroot/.well-known/acme-challenge/$token"'
  ].join(' ');
  const probe = await sshExec(conn, probeCommand, { allowFailure: true, timeout: 30000 });
  if (probe.code !== 0) {
    const detail = probe.stderr || probe.stdout || '';
    throw new Error(`文件验证本机测试失败：${detail.trim() || '80 端口没有返回验证文件'}`);
  }

  const reloadCmd = options.fileAutoRenew ? remoteNodeReloadCommand(options.installPath) : '';
  const issueCommand = [
    `${shellQuote(REMOTE_ACME)} --issue -d ${shellQuote(asciiDomain)} -w /video52000/acme-webroot --keylength ec-256 --force;`,
    `${shellQuote(REMOTE_ACME)} --install-cert -d ${shellQuote(asciiDomain)} --ecc --fullchain-file ${shellQuote(`${tlsDir}/fullchain.pem`)} --key-file ${shellQuote(`${tlsDir}/privkey.pem`)}${reloadCmd ? ` --reloadcmd ${shellQuote(reloadCmd)}` : ''};`,
    options.fileAutoRenew ? `${shellQuote(REMOTE_ACME)} --install-cronjob >/dev/null 2>&1 || true;` : '',
    options.fileAutoRenew ? '' : `${shellQuote(REMOTE_ACME)} --remove -d ${shellQuote(asciiDomain)} --ecc >/dev/null 2>&1 || true;`,
    `test -s ${shellQuote(`${tlsDir}/fullchain.pem`)} -a -s ${shellQuote(`${tlsDir}/privkey.pem`)}`
  ].filter(Boolean).join(' ');
  const result = await sshExec(conn, issueCommand, { allowFailure: true, timeout: 600000 });
  if (!options.fileAutoRenew) {
    await sshExec(conn, [
      'if command -v systemctl >/dev/null 2>&1 && [ -f /etc/systemd/system/video52000-acme-http.service ]; then',
      '  systemctl disable --now video52000-acme-http.service >/dev/null 2>&1 || true;',
      '  rm -f /etc/systemd/system/video52000-acme-http.service;',
      '  systemctl daemon-reload >/dev/null 2>&1 || true;',
      'fi;',
      'if [ -f /video52000/acme-http.pid ]; then kill "$(cat /video52000/acme-http.pid)" >/dev/null 2>&1 || true; rm -f /video52000/acme-http.pid; fi'
    ].join(' '), { allowFailure: true, timeout: 120000 });
  }
  if (result.code !== 0) {
    const detail = result.stderr || result.stdout || '';
    throw new Error(`文件验证自动申请 HTTPS 证书失败：${detail.trim() || '未生成证书文件'}`);
  }
}

async function ensureRemoteDnsCertificate(conn, options, tlsDir) {
  const domain = options.bindDomain;
  const safeDomain = cleanBindDomain(domain);
  const asciiDomain = domainToASCII(safeDomain);
  if (!safeDomain || !asciiDomain) throw new Error('自动申请 SSL 需要填写绑定域名。');
  if (!options.sslDnsProvider || !options.sslDnsEnv?.length) {
    throw new Error('自动申请 SSL 需要填写 DNS API 名称和环境变量。');
  }

  await ensureRemoteAcme(conn, Boolean(options.dnsAutoRenew), asciiDomain);
  const envFile = `${tlsDir}/acme-dns.env`;
  const envContent = `${options.sslDnsEnv.map(([key, value]) => `${key}=${shellQuote(value)}`).join('\n')}\n`;
  await sftpWrite(conn, envFile, envContent, 0o600);
  await sshExec(conn, 'mkdir -p /video52000/acme');

  const reloadCmd = options.dnsAutoRenew ? remoteNodeReloadCommand(options.installPath) : '';
  const issueCommand = [
    `set -a; . ${shellQuote(envFile)}; set +a;`,
    `${shellQuote(REMOTE_ACME)} --issue --dns ${shellQuote(options.sslDnsProvider)} -d ${shellQuote(asciiDomain)} --keylength ec-256 --force;`,
    `${shellQuote(REMOTE_ACME)} --install-cert -d ${shellQuote(asciiDomain)} --ecc --fullchain-file ${shellQuote(`${tlsDir}/fullchain.pem`)} --key-file ${shellQuote(`${tlsDir}/privkey.pem`)}${reloadCmd ? ` --reloadcmd ${shellQuote(reloadCmd)}` : ''};`,
    options.dnsAutoRenew ? `${shellQuote(REMOTE_ACME)} --install-cronjob >/dev/null 2>&1 || true;` : '',
    options.dnsAutoRenew ? '' : `${shellQuote(REMOTE_ACME)} --remove -d ${shellQuote(asciiDomain)} --ecc >/dev/null 2>&1 || true;`,
    `test -s ${shellQuote(`${tlsDir}/fullchain.pem`)} -a -s ${shellQuote(`${tlsDir}/privkey.pem`)}`
  ].filter(Boolean).join(' ');
  const result = await sshExec(conn, issueCommand, { allowFailure: true, timeout: 600000 });
  if (result.code !== 0) {
    const detail = result.stderr || result.stdout || '';
    throw new Error(`DNS-01 自动申请 HTTPS 证书失败：${detail.trim() || '未生成证书文件'}`);
  }
}

function updateJob(jobId, patch, logLine) {
  const job = db.installJobs.find((item) => item.id === jobId);
  if (!job) return;
  Object.assign(job, patch, { updatedAt: now() });
  if (logLine) job.logs.push(`[${new Date().toLocaleString('zh-CN', { hour12: false })}] ${logLine}`);
  saveDb();
}

function updateNodeUpdateJob(jobId, patch, logLine) {
  const job = db.updateJobs.find((item) => item.id === jobId);
  if (!job) return;
  Object.assign(job, patch, { updatedAt: now() });
  if (logLine) job.logs.push(`[${new Date().toLocaleString('zh-CN', { hour12: false })}] ${logLine}`);
  saveDb();
}

const FINISHED_JOB_STATUSES = new Set(['success', 'failed', 'canceled']);

function deleteFinishedJob(list, id, label) {
  const index = list.findIndex((job) => job.id === id);
  if (index === -1) {
    throw Object.assign(new Error(`${label}不存在。`), { status: 404 });
  }
  const job = list[index];
  if (!FINISHED_JOB_STATUSES.has(job.status)) {
    throw Object.assign(new Error('运行中或排队中的任务不能清除。'), { status: 409 });
  }
  list.splice(index, 1);
  saveDb();
  return job;
}

async function fetchNodeHealth(publicUrl) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetch(`${normalizeBaseUrl(publicUrl)}/health`, { signal: controller.signal });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || !body.ok) throw new Error(body.error || `HTTP ${response.status}`);
    return body;
  } catch (error) {
    if (error.name === 'AbortError' || /aborted/i.test(error.message)) {
      throw new Error(`公开地址测试超时：${normalizeBaseUrl(publicUrl)}/health 无法在 10 秒内响应。请检查 DNS、节点服务端口、防火墙以及节点地址协议是否正确。`);
    }
    const detail = [error.cause?.code, error.cause?.message, error.message].filter(Boolean).join(' ');
    if (/wrong version number|ERR_SSL_WRONG_VERSION_NUMBER/i.test(detail)) {
      throw new Error(`HTTPS 测试失败：${normalizeBaseUrl(publicUrl)} 的端口当前不是 TLS/HTTPS 服务，请确认安装时已写入证书和私钥。`);
    }
    if (/CERT_|certificate|self-signed|unable to verify/i.test(detail)) {
      throw new Error(`HTTPS 证书校验失败：${detail}`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function verifyInstalledSyncNode(publicUrl) {
  const health = await fetchNodeHealth(publicUrl);
  if (!Array.isArray(health.roles) || !health.roles.includes('sync')) {
    throw new Error('公开地址可访问，但节点未启用 sync 角色。');
  }
  await new Promise((resolve, reject) => {
    const token = signRoomToken('install-test', { id: 'installer', username: 'installer' }, 30000);
    const ws = new WebSocket(`${toWsBase(normalizeBaseUrl(publicUrl))}/sync?token=${encodeURIComponent(token)}`);
    const timer = setTimeout(() => {
      ws.close();
      reject(new Error('sync WebSocket 测试超时。'));
    }, 10000);
    ws.once('open', () => {
      clearTimeout(timer);
      ws.close();
      resolve();
    });
    ws.once('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
  return health;
}

async function verifyInstalledStorageNode(publicUrl) {
  const health = await fetchNodeHealth(publicUrl);
  if (!Array.isArray(health.roles) || !health.roles.includes('storage')) {
    throw new Error('公开地址可访问，但节点未启用 storage 角色。');
  }
  const data = await fetchRemoteJson({ url: publicUrl }, '/storage/list?path=', { timeout: 10000 });
  if (!Array.isArray(data.entries)) throw new Error('存储节点目录接口返回异常。');
  return health;
}

function upsertInstalledNodes(options, publicUrl, health) {
  const added = {};
  const baseName = options.nodeName || options.auth.host;
  const servicePort = Number(options.servicePort || DEFAULT_SYNC_PORT);
  const timestamp = now();
  const installInfo = {
    host: options.auth.host,
    sshPort: Number(options.auth.port || 22),
    authMethodId: options.authMethodId || null,
    installPath: options.installPath || '/video52000/app',
    videoRoot: options.videoRoot || '/video52000/videos',
    servicePort,
    publicUrl,
    bindDomain: options.bindDomain || '',
    domainRouteInfo: options.domainRouteInfo || null,
    useSsl: Boolean(options.useSsl),
    sslMode: options.sslMode || 'off',
    fileAutoRenew: Boolean(options.fileAutoRenew),
    dnsAutoRenew: Boolean(options.dnsAutoRenew),
    dnsProviderId: options.dnsProviderId || null,
    roles: [options.useSync && 'sync', options.useStorage && 'storage'].filter(Boolean),
    updatedAt: timestamp
  };

  if (options.useSync) {
    let node = db.syncNodes.find((item) => item.url && sameBaseUrl(item.url, publicUrl));
    if (!node) {
      node = {
        id: randomId('sync'),
        createdAt: timestamp
      };
      db.syncNodes.push(node);
    }
    if (options.makeDefaultSync) {
      db.syncNodes.forEach((item) => { item.isDefault = false; });
    }
    Object.assign(node, {
      name: `${baseName} sync`,
      url: publicUrl,
      port: servicePort,
      enabled: true,
      isDefault: Boolean(options.makeDefaultSync || node.isDefault),
      updatedAt: timestamp,
      install: installInfo,
      lastStatus: {
        ok: true,
        body: health,
        checkedAt: timestamp,
        source: 'installer'
      }
    });
    added.syncNodeId = node.id;
  }

  if (options.useStorage) {
    let node = db.storageNodes.find((item) => item.type !== 'local' && item.url && sameBaseUrl(item.url, publicUrl));
    if (!node) {
      node = {
        id: randomId('storage'),
        type: 'remote',
        createdAt: timestamp
      };
      db.storageNodes.push(node);
    }
    Object.assign(node, {
      name: `${baseName} storage`,
      type: 'remote',
      url: publicUrl,
      path: options.videoRoot || '/video52000/videos',
      enabled: true,
      updatedAt: timestamp,
      install: installInfo,
      lastStatus: {
        ok: true,
        body: health,
        checkedAt: timestamp,
        source: 'installer'
      }
    });
    added.storageNodeId = node.id;
  }

  ensureDefaultSyncNode();
  saveDb();
  return added;
}

function portFromPublicUrl(publicUrl, fallback = DEFAULT_SYNC_PORT) {
  try {
    const parsed = new URL(normalizeBaseUrl(publicUrl));
    if (parsed.port) return Number(parsed.port);
    return parsed.protocol === 'https:' ? 443 : 80;
  } catch {
    return fallback;
  }
}

function latestInstallJobForUrl(publicUrl) {
  return db.installJobs
    .filter((job) => job.status === 'success' && job.publicUrl && sameBaseUrl(job.publicUrl, publicUrl))
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0] || null;
}

function installedNodeTargets(req) {
  const byUrl = new Map();
  for (const node of db.syncNodes) {
    if (!node.url) continue;
    const url = safeSyncNode(req, node).url;
    const key = normalizeBaseUrl(url).toLowerCase();
    if (!byUrl.has(key)) byUrl.set(key, { publicUrl: url, syncNodeIds: [], storageNodeIds: [], names: [], roles: [] });
    const target = byUrl.get(key);
    target.syncNodeIds.push(node.id);
    target.names.push(node.name);
    if (!target.roles.includes('sync')) target.roles.push('sync');
  }
  for (const node of db.storageNodes) {
    if ((node.type || 'remote') === 'local' || !node.url) continue;
    const url = safeStorageNode(req, node).url;
    const key = normalizeBaseUrl(url).toLowerCase();
    if (!byUrl.has(key)) byUrl.set(key, { publicUrl: url, syncNodeIds: [], storageNodeIds: [], names: [], roles: [] });
    const target = byUrl.get(key);
    target.storageNodeIds.push(node.id);
    target.names.push(node.name);
    if (!target.roles.includes('storage')) target.roles.push('storage');
  }
  return Array.from(byUrl.values()).map((target) => {
    const firstNode = [
      ...target.syncNodeIds.map((id) => db.syncNodes.find((node) => node.id === id)),
      ...target.storageNodeIds.map((id) => db.storageNodes.find((node) => node.id === id))
    ].filter(Boolean)[0];
    const install = firstNode?.install || {};
    const job = latestInstallJobForUrl(target.publicUrl);
    const hasInstallSource = Boolean(install.host || job);
    const authMethodId = install.authMethodId || job?.authMethodId || (db.authMethods.length === 1 ? db.authMethods[0].id : null);
    return {
      ...target,
      name: Array.from(new Set(target.names)).join(' / '),
      host: install.host || job?.host || new URL(target.publicUrl).hostname,
      sshPort: Number(install.sshPort || job?.sshPort || 22),
      installPath: install.installPath || job?.installPath || '/video52000/app',
      videoRoot: install.videoRoot || job?.videoRoot || db.storageNodes.find((node) => target.storageNodeIds.includes(node.id))?.path || '/video52000/videos',
      servicePort: install.servicePort || job?.servicePort || portFromPublicUrl(target.publicUrl),
      bindDomain: install.bindDomain || job?.bindDomain || cleanBindDomain(target.publicUrl),
      domainRouteInfo: install.domainRouteInfo || job?.domainRouteInfo || null,
      useSsl: 'useSsl' in install ? Boolean(install.useSsl) : /^https:\/\//i.test(target.publicUrl),
      sslMode: install.sslMode || job?.sslMode || (/^https:\/\//i.test(target.publicUrl) ? 'manual' : 'off'),
      fileAutoRenew: Boolean(install.fileAutoRenew || job?.fileAutoRenew),
      dnsAutoRenew: Boolean(install.dnsAutoRenew || job?.dnsAutoRenew),
      dnsProviderId: install.dnsProviderId || job?.dnsProviderId || '',
      syncEnabled: target.syncNodeIds.some((id) => db.syncNodes.find((node) => node.id === id)?.enabled),
      storageEnabled: target.storageNodeIds.some((id) => db.storageNodes.find((node) => node.id === id)?.enabled),
      isDefault: target.syncNodeIds.some((id) => db.syncNodes.find((node) => node.id === id)?.isDefault),
      canUpdate: Boolean(hasInstallSource && authMethodId),
      authMethodId
    };
  });
}

function resolveNodeUpdateTarget(publicUrl) {
  const url = normalizeBaseUrl(publicUrl);
  const syncNodes = db.syncNodes.filter((node) => node.url && sameBaseUrl(node.url, url));
  const storageNodes = db.storageNodes.filter((node) => (node.type || 'remote') !== 'local' && node.url && sameBaseUrl(node.url, url));
  if (!syncNodes.length && !storageNodes.length) {
    throw Object.assign(new Error('请选择已经安装并有公开地址的远程节点。'), { status: 400 });
  }
  const firstNode = [...syncNodes, ...storageNodes][0];
  const install = firstNode.install || {};
  const job = latestInstallJobForUrl(url);
  if (!install.host && !job) {
    throw Object.assign(new Error('这个节点没有安装记录，不能自动更新。请先通过“安装节点”成功安装一次。'), { status: 400 });
  }
  const authMethodId = install.authMethodId || job?.authMethodId || (db.authMethods.length === 1 ? db.authMethods[0].id : null);
  const method = db.authMethods.find((item) => item.id === authMethodId);
  if (!method) {
    throw Object.assign(new Error('这个节点缺少安装时的 SSH 验证方式。请先通过安装节点成功安装一次，或保留唯一一个 SSH 验证方式后再更新。'), { status: 400 });
  }
  const parsed = new URL(url);
  const roles = [
    syncNodes.length && 'sync',
    storageNodes.length && 'storage'
  ].filter(Boolean);
  const storageNode = storageNodes[0];
  const servicePort = Number(install.servicePort || job?.servicePort || syncNodes[0]?.port || portFromPublicUrl(url));
  return {
    publicUrl: url,
    host: install.host || job?.host || parsed.hostname,
    sshPort: Number(install.sshPort || job?.sshPort || 22),
    authMethodId: method.id,
    auth: {
      host: install.host || job?.host || parsed.hostname,
      port: Number(install.sshPort || job?.sshPort || 22),
      username: method.username,
      mode: method.mode,
      secret: decryptSecret(method.secretCipher)
    },
    installPath: install.installPath || job?.installPath || '/video52000/app',
    videoRoot: install.videoRoot || job?.videoRoot || storageNode?.path || '/video52000/videos',
    servicePort,
    useSsl: /^https:\/\//i.test(url),
    bindDomain: install.bindDomain || job?.bindDomain || cleanBindDomain(url),
    domainRouteInfo: install.domainRouteInfo || job?.domainRouteInfo || null,
    sslMode: install.sslMode || job?.sslMode || (/^https:\/\//i.test(url) ? 'manual' : 'off'),
    fileAutoRenew: Boolean(install.fileAutoRenew || job?.fileAutoRenew),
    dnsAutoRenew: Boolean(install.dnsAutoRenew || job?.dnsAutoRenew),
    dnsProviderId: install.dnsProviderId || job?.dnsProviderId || null,
    roles,
    useSync: syncNodes.length > 0,
    useStorage: storageNodes.length > 0,
    nodeName: Array.from(new Set([...syncNodes, ...storageNodes].map((node) => node.name))).join(' / '),
    syncNodeIds: syncNodes.map((node) => node.id),
    storageNodeIds: storageNodes.map((node) => node.id)
  };
}

function markUpdatedNodes(target, health) {
  const timestamp = now();
  const installInfo = {
    host: target.host,
    sshPort: target.sshPort,
    authMethodId: target.authMethodId,
    installPath: target.installPath,
    videoRoot: target.videoRoot,
    servicePort: target.servicePort,
    publicUrl: target.publicUrl,
    bindDomain: target.bindDomain || '',
    domainRouteInfo: target.domainRouteInfo || null,
    useSsl: target.useSsl,
    sslMode: target.sslMode || (target.useSsl ? 'manual' : 'off'),
    fileAutoRenew: Boolean(target.fileAutoRenew),
    dnsAutoRenew: Boolean(target.dnsAutoRenew),
    dnsProviderId: target.dnsProviderId || null,
    roles: target.roles,
    updatedAt: timestamp
  };
  for (const node of db.syncNodes.filter((item) => target.syncNodeIds.includes(item.id))) {
    node.updatedAt = timestamp;
    node.install = installInfo;
    node.lastStatus = { ok: true, body: health, checkedAt: timestamp, source: 'updater' };
  }
  for (const node of db.storageNodes.filter((item) => target.storageNodeIds.includes(item.id))) {
    node.updatedAt = timestamp;
    node.install = installInfo;
    node.path = target.videoRoot || node.path;
    node.lastStatus = { ok: true, body: health, checkedAt: timestamp, source: 'updater' };
  }
  saveDb();
}

function targetRoleName(baseName, role, bothRoles) {
  const cleanName = String(baseName || '').trim() || 'node';
  return bothRoles ? `${cleanName} ${role}` : cleanName;
}

async function buildEditableNodeTarget(body) {
  const oldPublicUrl = normalizeBaseUrl(body.oldPublicUrl || body.publicUrl || '');
  const oldSyncNodes = oldPublicUrl ? db.syncNodes.filter((node) => node.url && sameBaseUrl(node.url, oldPublicUrl)) : [];
  const oldStorageNodes = oldPublicUrl ? db.storageNodes.filter((node) => (node.type || 'remote') !== 'local' && node.url && sameBaseUrl(node.url, oldPublicUrl)) : [];
  if (!oldSyncNodes.length && !oldStorageNodes.length) {
    throw Object.assign(new Error('请选择已经安装的远程节点。'), { status: 400 });
  }

  const useSync = Boolean(body.useSync);
  const useStorage = Boolean(body.useStorage);
  if (!useSync && !useStorage) throw Object.assign(new Error('至少启用 sync 或存储角色。'), { status: 400 });

  const method = db.authMethods.find((item) => item.id === body.authMethodId);
  if (!method) throw Object.assign(new Error('请选择 SSH 验证方式。'), { status: 400 });

  const host = String(body.host || '').trim();
  if (!host) throw Object.assign(new Error('服务器 IP/域名不能为空。'), { status: 400 });
  const sshPort = Number(body.sshPort || 22);
  const servicePort = Number(body.servicePort || DEFAULT_SYNC_PORT);
  const requestedPublicUrl = String(body.publicUrl || '').trim();
  const useSsl = Boolean(body.useSsl) || /^https:\/\//i.test(requestedPublicUrl);
  const bindDomain = cleanBindDomain(body.bindDomain || requestedPublicUrl || '');
  const domainRouteInfo = bindDomain ? await inspectBindDomainRoute(bindDomain, host) : null;

  const sslCert = normalizePemInput(body.sslCert);
  const sslKey = normalizePemInput(body.sslKey);
  const existingInstall = (oldSyncNodes[0] || oldStorageNodes[0])?.install || {};
  const sslMode = useSsl ? normalizeSslMode(body.sslMode || existingInstall.sslMode, Boolean(sslCert || sslKey)) : 'off';
  const fileAutoRenew = boolFromBody(body.fileAutoRenew, Boolean(existingInstall.fileAutoRenew));
  const dnsAutoRenew = boolFromBody(body.dnsAutoRenew, Boolean(existingInstall.dnsAutoRenew));
  let dnsProvider = null;
  let sslDnsProvider = normalizeAcmeDnsProvider(body.sslDnsProvider);
  let sslDnsEnv = parseAcmeDnsEnv(body.sslDnsEnv);
  if (useSsl && sslMode === 'dns' && body.dnsProviderId) {
    dnsProvider = db.dnsProviders.find((item) => item.id === body.dnsProviderId && item.enabled !== false);
    if (!dnsProvider) throw Object.assign(new Error('请选择可用的 DNS 验证方式。'), { status: 400 });
    const acme = dnsProviderToAcme(dnsProvider);
    sslDnsProvider = acme.sslDnsProvider;
    sslDnsEnv = acme.sslDnsEnv;
  }
  if (useSsl) {
    if (sslMode === 'manual') {
      if (sslCert || sslKey) {
        if (!sslCert || !sslKey) throw Object.assign(new Error('手动证书模式需要同时填写 fullchain PEM 和私钥 PEM。'), { status: 400 });
        validateTlsPem(sslCert, sslKey);
      }
    } else if (sslMode === 'dns') {
      if (!bindDomain) throw Object.assign(new Error('DNS 验证需要填写绑定域名。'), { status: 400 });
      if (!sslDnsProvider || !sslDnsEnv.length) {
        throw Object.assign(new Error('DNS 验证需要选择已保存的 DNS API，或手动填写 acme.sh DNS API 名称和环境变量。'), { status: 400 });
      }
    } else if (sslMode === 'file') {
      if (!bindDomain) throw Object.assign(new Error('文件验证需要填写绑定域名。'), { status: 400 });
    }
  }

  const publicUrl = buildInstallPublicUrl({
    publicUrl: requestedPublicUrl,
    bindDomain,
    host,
    servicePort,
    useSsl
  });
  const installPath = String(body.installPath || existingInstall.installPath || '/video52000/app');
  const videoRoot = String(body.videoRoot || existingInstall.videoRoot || oldStorageNodes[0]?.path || '/video52000/videos');
  const roles = [useSync && 'sync', useStorage && 'storage'].filter(Boolean);
  const nodeName = String(body.nodeName || '').trim() || 'node';
  const timestamp = now();
  const installInfo = {
    host,
    sshPort,
    authMethodId: method.id,
    installPath,
    videoRoot,
    servicePort,
    publicUrl,
    bindDomain,
    domainRouteInfo,
    useSsl,
    sslMode,
    fileAutoRenew: sslMode === 'file' ? fileAutoRenew : false,
    dnsAutoRenew: sslMode === 'dns' ? dnsAutoRenew : false,
    dnsProviderId: dnsProvider?.id || body.dnsProviderId || null,
    roles,
    updatedAt: timestamp
  };
  const bothRoles = useSync && useStorage;
  const enabled = body.enabled !== false;

  let syncNodes = oldSyncNodes;
  if (useSync && !syncNodes.length) {
    const node = { id: randomId('sync'), createdAt: timestamp, lastStatus: null };
    db.syncNodes.push(node);
    syncNodes = [node];
  }
  for (const node of syncNodes) {
    Object.assign(node, {
      name: targetRoleName(nodeName, 'sync', bothRoles),
      url: publicUrl,
      port: servicePort,
      enabled: useSync ? enabled : false,
      updatedAt: timestamp,
      install: installInfo
    });
    if (body.isDefault && useSync) {
      db.syncNodes.forEach((item) => { item.isDefault = false; });
      node.isDefault = true;
      node.enabled = true;
    } else if (!useSync) {
      node.isDefault = false;
    }
  }

  let storageNodes = oldStorageNodes;
  if (useStorage && !storageNodes.length) {
    const node = { id: randomId('storage'), type: 'remote', createdAt: timestamp, lastStatus: null };
    db.storageNodes.push(node);
    storageNodes = [node];
  }
  for (const node of storageNodes) {
    Object.assign(node, {
      name: targetRoleName(nodeName, 'storage', bothRoles),
      type: 'remote',
      url: publicUrl,
      path: videoRoot,
      enabled: useStorage ? enabled : false,
      updatedAt: timestamp,
      install: installInfo
    });
  }

  ensureDefaultSyncNode();
  saveDb();

  return {
    publicUrl,
    host,
    sshPort,
    authMethodId: method.id,
    auth: {
      host,
      port: sshPort,
      username: method.username,
      mode: method.mode,
      secret: decryptSecret(method.secretCipher)
    },
    installPath,
    videoRoot,
    servicePort,
    useSsl,
    bindDomain,
    domainRouteInfo,
    sslMode,
    fileAutoRenew,
    dnsAutoRenew,
    dnsProviderId: dnsProvider?.id || body.dnsProviderId || null,
    sslDnsProvider,
    sslDnsEnv,
    sslCert,
    sslKey,
    roles,
    useSync,
    useStorage,
    nodeName,
    syncNodeIds: syncNodes.map((node) => node.id),
    storageNodeIds: storageNodes.map((node) => node.id)
  };
}

async function runUpdateJob(jobId, target) {
  let conn;
  try {
    updateNodeUpdateJob(jobId, { status: 'running' }, '开始连接 SSH');
    conn = await sshConnect(target.auth);
    updateNodeUpdateJob(jobId, {}, 'SSH 连接成功');

    const installPath = target.installPath || '/video52000/app';
    const videoRoot = target.videoRoot || '/video52000/videos';
    const servicePort = Number(target.servicePort || DEFAULT_SYNC_PORT);
    const tlsDir = `${installPath.replace(/\/+$/, '')}/tls`;
    const routeNotice = domainRouteLog(target.domainRouteInfo);
    if (routeNotice) {
      updateNodeUpdateJob(jobId, {}, routeNotice);
      if (target.useSsl && target.sslMode === 'file') {
        updateNodeUpdateJob(jobId, {}, '注意：HTTP-01 文件验证仍要求公开域名的 80 端口能访问到节点的 ACME 临时验证服务；反代环境更建议使用 DNS 验证或手动证书');
      }
    }

    updateNodeUpdateJob(jobId, {}, '创建远程目录');
    await sshExec(conn, `mkdir -p ${shellQuote(installPath)} ${shellQuote(videoRoot)} /video52000 ${target.useSsl ? shellQuote(tlsDir) : ''}`);

    updateNodeUpdateJob(jobId, {}, '上传新版节点程序');
    const nodeServer = fs.readFileSync(path.join(ROOT, 'sync-node', 'server.js'));
    const nodePackage = fs.readFileSync(path.join(ROOT, 'sync-node', 'package.json'));
    const envLines = [
      `PORT=${servicePort}`,
      `NODE_TOKEN=${secrets.nodeSecret}`,
      `NODE_ROLES=${target.roles.join(',')}`,
      `VIDEO_STORAGE_ROOT=${videoRoot}`
    ];
    if (target.useSsl) {
      envLines.push(`TLS_CERT_PATH=${tlsDir}/fullchain.pem`);
      envLines.push(`TLS_KEY_PATH=${tlsDir}/privkey.pem`);
    }
    await sftpWrite(conn, `${installPath}/server.js`, nodeServer);
    await sftpWrite(conn, `${installPath}/package.json`, nodePackage);
    await sftpWrite(conn, `${installPath}/.env`, `${envLines.join('\n')}\n`, 0o600);

    updateNodeUpdateJob(jobId, {}, '检查 Node.js 运行环境');
    await ensureRemoteNodeRuntime(conn);
    if (target.useStorage) {
      updateNodeUpdateJob(jobId, {}, '检查离线下载组件 aria2');
      await ensureRemoteAria2(conn);
    }

    if (target.useSsl && target.sslMode === 'manual' && target.sslCert && target.sslKey) {
      updateNodeUpdateJob(jobId, {}, '写入 HTTPS 证书和私钥');
      await sftpWrite(conn, `${tlsDir}/fullchain.pem`, `${target.sslCert}\n`, 0o600);
      await sftpWrite(conn, `${tlsDir}/privkey.pem`, `${target.sslKey}\n`, 0o600);
    } else if (target.useSsl && target.sslMode === 'file') {
      updateNodeUpdateJob(jobId, {}, `HTTP-01 文件验证同步 HTTPS 证书: ${target.bindDomain}${target.fileAutoRenew ? '，启用自动续签' : '，不自动续签'}`);
      await ensureRemoteFileCertificate(conn, target, tlsDir);
    } else if (target.useSsl && target.sslMode === 'dns') {
      updateNodeUpdateJob(jobId, {}, `DNS-01 同步 HTTPS 证书: ${target.bindDomain}${target.dnsAutoRenew ? '，启用自动续签' : '，不自动续签'}`);
      await ensureRemoteDnsCertificate(conn, target, tlsDir);
    }

    updateNodeUpdateJob(jobId, {}, '安装远程 npm 依赖');
    await sshExec(conn, `cd ${shellQuote(installPath)} && npm_config_cache=/tmp/npm-cache npm install --omit=dev`, { timeout: 300000 });

    updateNodeUpdateJob(jobId, {}, '重启节点服务');
    const serviceName = 'video52000-node.service';
    const serviceFile = [
      '[Unit]',
      'Description=Video52000 sync/storage node',
      'After=network.target',
      '',
      '[Service]',
      'Type=simple',
      `WorkingDirectory=${installPath}`,
      `EnvironmentFile=${installPath}/.env`,
      `ExecStart=/usr/bin/env node ${installPath}/server.js`,
      'Restart=always',
      'RestartSec=3',
      '',
      '[Install]',
      'WantedBy=multi-user.target',
      ''
    ].join('\n');
    await sftpWrite(conn, `/tmp/${serviceName}`, serviceFile);
    const systemd = await sshExec(
      conn,
      `if command -v systemctl >/dev/null 2>&1; then mv /tmp/${serviceName} /etc/systemd/system/${serviceName} && systemctl daemon-reload && systemctl enable ${serviceName} && systemctl restart ${serviceName}; else exit 7; fi`,
      { allowFailure: true, timeout: 120000 }
    );
    if (systemd.code !== 0) {
      updateNodeUpdateJob(jobId, {}, 'systemd 不可用，使用 nohup 后台启动');
      await sshExec(conn, `if [ -f /video52000/node.pid ]; then kill "$(cat /video52000/node.pid)" >/dev/null 2>&1 || true; fi; cd ${shellQuote(installPath)} && (nohup /usr/bin/env node server.js >> /video52000/node.log 2>&1 & echo $! > /video52000/node.pid)`);
    }

    updateNodeUpdateJob(jobId, {}, '等待节点端口启动');
    const localHealthCheck = target.useSsl
      ? `node -e "require('https').get({hostname:'127.0.0.1',port:${servicePort},path:'/health',rejectUnauthorized:false},r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"`
      : `node -e "require('http').get('http://127.0.0.1:${servicePort}/health',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"`;
    await sshExec(conn, `for i in $(seq 1 20); do ${localHealthCheck} && exit 0; sleep 1; done; ${localHealthCheck}`, { timeout: 45000 });

    updateNodeUpdateJob(jobId, {}, '从主站测试公开节点地址');
    let health = await fetchNodeHealth(target.publicUrl);
    if (target.useSync) health = await verifyInstalledSyncNode(target.publicUrl);
    if (target.useStorage) health = await verifyInstalledStorageNode(target.publicUrl);

    markUpdatedNodes(target, health);
    updateNodeUpdateJob(jobId, { status: 'success', result: { publicUrl: target.publicUrl, roles: target.roles } }, '更新完成，节点已重新测试通过');
  } catch (error) {
    updateNodeUpdateJob(jobId, { status: 'failed', error: error.message }, `更新失败: ${error.message}`);
  } finally {
    if (conn) conn.end();
  }
}

async function runInstallJob(jobId, options) {
  let conn;
  try {
    updateJob(jobId, { status: 'running' }, '开始连接 SSH');
    conn = await sshConnect(options.auth);
    updateJob(jobId, {}, 'SSH 连接成功');
    const installPath = options.installPath || '/video52000/app';
    const videoRoot = options.videoRoot || '/video52000/videos';
    const servicePort = Number(options.servicePort || DEFAULT_SYNC_PORT);
    const tlsDir = `${installPath.replace(/\/+$/, '')}/tls`;
    const roles = [];
    if (options.useSync) roles.push('sync');
    if (options.useStorage) roles.push('storage');
    const routeNotice = domainRouteLog(options.domainRouteInfo);
    if (routeNotice) {
      updateJob(jobId, {}, routeNotice);
      if (options.useSsl && options.sslMode === 'file') {
        updateJob(jobId, {}, '注意：HTTP-01 文件验证仍要求公开域名的 80 端口能访问到节点的 ACME 临时验证服务；反代环境更建议使用 DNS 验证或手动证书');
      }
    }

    updateJob(jobId, {}, '创建远程目录');
    await sshExec(conn, `mkdir -p ${shellQuote(installPath)} ${shellQuote(videoRoot)} /video52000 ${options.useSsl ? shellQuote(tlsDir) : ''}`);

    updateJob(jobId, {}, '上传节点程序');
    const nodeServer = fs.readFileSync(path.join(ROOT, 'sync-node', 'server.js'));
    const nodePackage = fs.readFileSync(path.join(ROOT, 'sync-node', 'package.json'));
    const envLines = [
      `PORT=${servicePort}`,
      `NODE_TOKEN=${secrets.nodeSecret}`,
      `NODE_ROLES=${roles.join(',')}`,
      `VIDEO_STORAGE_ROOT=${videoRoot}`
    ];
    if (options.useSsl) {
      envLines.push(`TLS_CERT_PATH=${tlsDir}/fullchain.pem`);
      envLines.push(`TLS_KEY_PATH=${tlsDir}/privkey.pem`);
    }
    const env = `${envLines.join('\n')}\n`;
    await sftpWrite(conn, `${installPath}/server.js`, nodeServer);
    await sftpWrite(conn, `${installPath}/package.json`, nodePackage);
    await sftpWrite(conn, `${installPath}/.env`, env, 0o600);

    updateJob(jobId, {}, '检查 Node.js 运行环境');
    await ensureRemoteNodeRuntime(conn);
    if (options.useStorage) {
      updateJob(jobId, {}, '检查离线下载组件 aria2');
      await ensureRemoteAria2(conn);
    }

    if (options.useSsl && options.sslMode === 'manual') {
      updateJob(jobId, {}, '写入 HTTPS 证书和私钥');
      await sftpWrite(conn, `${tlsDir}/fullchain.pem`, `${options.sslCert}\n`, 0o600);
      await sftpWrite(conn, `${tlsDir}/privkey.pem`, `${options.sslKey}\n`, 0o600);
    } else if (options.useSsl && options.sslMode === 'file') {
      updateJob(jobId, {}, `HTTP-01 文件验证自动申请 HTTPS 证书: ${options.bindDomain}${options.fileAutoRenew ? '，启用自动续签' : '，不自动续签'}`);
      await ensureRemoteFileCertificate(conn, options, tlsDir);
    } else if (options.useSsl && options.sslMode === 'dns') {
      updateJob(jobId, {}, `DNS-01 自动申请 HTTPS 证书: ${options.bindDomain}${options.dnsAutoRenew ? '，启用自动续签' : '，不自动续签'}`);
      await ensureRemoteDnsCertificate(conn, options, tlsDir);
    }

    updateJob(jobId, {}, '安装远程 npm 依赖');
    await sshExec(conn, `cd ${shellQuote(installPath)} && npm_config_cache=/tmp/npm-cache npm install --omit=dev`, { timeout: 300000 });

    updateJob(jobId, {}, '注册并启动节点服务');
    const serviceName = 'video52000-node.service';
    const serviceFile = [
      '[Unit]',
      'Description=Video52000 sync/storage node',
      'After=network.target',
      '',
      '[Service]',
      'Type=simple',
      `WorkingDirectory=${installPath}`,
      `EnvironmentFile=${installPath}/.env`,
      `ExecStart=/usr/bin/env node ${installPath}/server.js`,
      'Restart=always',
      'RestartSec=3',
      '',
      '[Install]',
      'WantedBy=multi-user.target',
      ''
    ].join('\n');
    await sftpWrite(conn, `/tmp/${serviceName}`, serviceFile);
    const systemd = await sshExec(
      conn,
      `if command -v systemctl >/dev/null 2>&1; then mv /tmp/${serviceName} /etc/systemd/system/${serviceName} && systemctl daemon-reload && systemctl enable ${serviceName} && systemctl restart ${serviceName}; else exit 7; fi`,
      { allowFailure: true, timeout: 120000 }
    );
    if (systemd.code !== 0) {
      updateJob(jobId, {}, 'systemd 不可用，使用 nohup 后台启动');
      await sshExec(conn, `if [ -f /video52000/node.pid ]; then kill "$(cat /video52000/node.pid)" >/dev/null 2>&1 || true; fi; cd ${shellQuote(installPath)} && (nohup /usr/bin/env node server.js >> /video52000/node.log 2>&1 & echo $! > /video52000/node.pid)`);
    }

    const publicUrl = normalizeBaseUrl(options.publicUrl || `http://${options.auth.host}:${servicePort}`);
    updateJob(jobId, {}, '等待节点端口启动');
    const localHealthCheck = options.useSsl
      ? `node -e "require('https').get({hostname:'127.0.0.1',port:${servicePort},path:'/health',rejectUnauthorized:false},r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"`
      : `node -e "require('http').get('http://127.0.0.1:${servicePort}/health',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"`;
    await sshExec(conn, `for i in $(seq 1 20); do ${localHealthCheck} && exit 0; sleep 1; done; ${localHealthCheck}`, { timeout: 45000 });

    if (options.useSsl) {
      updateJob(jobId, {}, `使用端口直连 HTTPS: ${publicUrl}`);
    }

    updateJob(jobId, {}, '从主站测试公开节点地址');
    let health = await fetchNodeHealth(publicUrl);
    if (options.useSync) health = await verifyInstalledSyncNode(publicUrl);
    if (options.useStorage) health = await verifyInstalledStorageNode(publicUrl);

    updateJob(jobId, {}, '测试通过，自动写入节点列表');
    const added = upsertInstalledNodes({ ...options, videoRoot }, publicUrl, health);

    updateJob(jobId, { status: 'success', result: { publicUrl, roles, ...added } }, '安装完成，节点已自动添加或更新');
  } catch (error) {
    updateJob(jobId, { status: 'failed', error: error.message }, `安装失败: ${error.message}`);
  } finally {
    if (conn) conn.end();
  }
}

async function testSyncNode(req, node) {
  const resolved = safeSyncNode(req, node);
  const healthStartedAt = Date.now();
  const healthController = new AbortController();
  const healthTimer = setTimeout(() => healthController.abort(), 6000);
  const health = await fetch(`${resolved.url}/health`, { signal: healthController.signal }).finally(() => clearTimeout(healthTimer));
  const body = await health.json().catch(() => ({}));
  const httpMs = Date.now() - healthStartedAt;
  if (!health.ok) throw new Error(body.error || `HTTP ${health.status}`);

  let wsMs = 0;
  await new Promise((resolve, reject) => {
    const wsStartedAt = Date.now();
    const token = signRoomToken('test-room', req.user, 30000);
    const ws = new WebSocket(`${resolved.wsUrl}?token=${encodeURIComponent(token)}`);
    const timer = setTimeout(() => {
      ws.close();
      reject(new Error('WebSocket 测试超时'));
    }, 6000);
    ws.once('open', () => {
      wsMs = Date.now() - wsStartedAt;
      clearTimeout(timer);
      ws.close();
      resolve();
    });
    ws.once('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });

  return { body, timings: { httpMs, wsMs } };
}

const app = express();
app.set('trust proxy', true);
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(session({
  name: 'video_together_sid',
  secret: secrets.sessionSecret,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    maxAge: 30 * 24 * 60 * 60 * 1000
  }
}));

app.use('/videos', express.static(STORAGE_ROOT, {
  fallthrough: false,
  setHeaders(res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Length,Content-Range,Accept-Ranges,Content-Type');
    res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
    res.setHeader('Accept-Ranges', 'bytes');
  }
}));
app.use('/vendor/hls.js', express.static(path.join(ROOT, 'node_modules', 'hls.js', 'dist', 'hls.min.js')));
app.use('/vendor/artplayer.js', express.static(path.join(ROOT, 'node_modules', 'artplayer', 'dist', 'artplayer.js')));
app.use('/vendor', (req, res) => {
  res.status(404).type('text/plain').send('vendor asset not found');
});
app.use(express.static(PUBLIC_DIR));

app.post('/api/auth/register', asyncRoute(async (req, res) => {
  const username = normalizeUsername(req.body.username);
  const password = assertPassword(req.body.password, req.body.confirmPassword, true);
  if (db.users.some((user) => user.username.toLowerCase() === username.toLowerCase())) {
    return res.status(409).json({ error: '用户名已存在。' });
  }
  const user = {
    id: randomId('usr'),
    username,
    displayName: username,
    passwordHash: await bcrypt.hash(password, 12),
    role: 'user',
    createdAt: now()
  };
  db.users.push(user);
  saveDb();
  req.session.userId = user.id;
  res.json({ user: publicUser(user) });
}));

app.post('/api/auth/login', asyncRoute(async (req, res) => {
  const username = String(req.body.username || '').trim();
  const password = String(req.body.password || '');
  const user = db.users.find((item) => item.username.toLowerCase() === username.toLowerCase());
  if (!user || !(await bcrypt.compare(password, user.passwordHash))) {
    return res.status(401).json({ error: '用户名或密码错误。' });
  }
  if (user.disabled) return res.status(403).json({ error: '此用户已被禁用。' });
  req.session.userId = user.id;
  res.json({ user: publicUser(user) });
}));

app.post('/api/auth/logout', requireAuth, (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
});

app.get('/api/me', (req, res) => {
  const user = getUserById(req.session.userId);
  res.json({ user: publicUser(user) || null });
});

app.get('/api/config', requireAuth, (req, res) => {
  const enabledSyncNodes = db.syncNodes.filter((node) => node.enabled);
  const enabledStorageNodes = db.storageNodes.filter((node) => node.enabled);
  res.json({
    syncNodes: enabledSyncNodes.map((node) => safeSyncNode(req, node)),
    storageNodes: enabledStorageNodes.map((node) => safeStorageNode(req, node)),
    defaultSyncNodeId: enabledSyncNodes.find((node) => node.isDefault)?.id || null,
    stats: {
      rooms: db.rooms.length,
      syncNodes: enabledSyncNodes.length,
      storageNodes: enabledStorageNodes.length,
      installJobs: db.installJobs.filter((job) => ['queued', 'running'].includes(job.status)).length
    }
  });
});

app.get('/api/rooms', requireAuth, (req, res) => {
  res.json({
    rooms: db.rooms.map((room) => ({
      ...room,
      ownerName: db.users.find((user) => user.id === room.ownerId)?.username || 'unknown',
      syncNodeName: db.syncNodes.find((node) => node.id === room.syncNodeId)?.name || '未知节点',
      syncNodeEnabled: Boolean(db.syncNodes.find((node) => node.id === room.syncNodeId)?.enabled)
    })).sort((a, b) => b.createdAt.localeCompare(a.createdAt))
  });
});

app.post('/api/rooms', requireAuth, (req, res) => {
  const name = String(req.body.name || '').trim() || `${req.user.username} 的房间`;
  if (name.length > 60) return res.status(400).json({ error: '房间名不能超过 60 个字符。' });
  const defaultNode = db.syncNodes.find((node) => node.enabled && node.isDefault) || db.syncNodes.find((node) => node.enabled);
  const selected = req.body.syncNodeId
    ? db.syncNodes.find((node) => node.id === req.body.syncNodeId && node.enabled)
    : defaultNode;
  if (!selected) return res.status(400).json({ error: '没有可用的 sync node。' });
  const room = {
    id: randomId('room'),
    name,
    ownerId: req.user.id,
    syncNodeId: selected.id,
    createdAt: now(),
    updatedAt: now()
  };
  db.rooms.push(room);
  saveDb();
  res.json({ room });
});

app.post('/api/rooms/:id/join', requireAuth, (req, res) => {
  const room = db.rooms.find((item) => item.id === req.params.id);
  if (!room) return res.status(404).json({ error: '房间不存在。' });
  const syncNode = db.syncNodes.find((node) => node.id === room.syncNodeId && node.enabled)
    || db.syncNodes.find((node) => node.enabled && node.isDefault)
    || db.syncNodes.find((node) => node.enabled);
  if (!syncNode) return res.status(400).json({ error: '此房间没有可用 sync node。' });
  res.json({
    room,
    user: publicUser(req.user),
    token: signRoomToken(room.id, req.user),
    syncNode: safeSyncNode(req, syncNode),
    storageNodes: db.storageNodes.filter((node) => node.enabled).map((node) => safeStorageNode(req, node))
  });
});

app.get('/api/storage/nodes/:id/list', requireAuth, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id && item.enabled);
  if (!node) return res.status(404).json({ error: '存储节点不存在或未启用。' });
  res.json(await listStorageNode(req, node, req.query.path || ''));
}));

app.post('/api/media/metadata', requireAuth, asyncRoute(async (req, res) => {
  const url = String(req.body.url || '').trim();
  if (!url) return res.status(400).json({ error: '音频地址不能为空。' });
  res.json({ metadata: await readMediaMetadata(req, url, req.body.name || filenameFromUrl(url)) });
}));

app.get('/api/media/covers/:id', requireAuth, (req, res) => {
  cleanupMediaCoverCache();
  const id = path.basename(String(req.params.id || ''));
  const file = path.join(MEDIA_COVER_DIR, id);
  const cover = mediaCoverCache.get(id);
  if (cover) {
    res.setHeader('content-type', cover.mime);
    res.setHeader('cache-control', 'private, max-age=86400');
    return res.send(cover.data);
  }
  if (!id || !fs.existsSync(file) || !fs.statSync(file).isFile()) return res.status(404).end();
  res.setHeader('content-type', mediaCoverMimeFromName(id));
  res.setHeader('cache-control', 'private, max-age=1800');
  res.sendFile(file);
});

app.get('/api/admin/sync-nodes', requireAuth, requireAdmin, (req, res) => {
  res.json({ nodes: db.syncNodes.map((node) => safeSyncNode(req, node)) });
});

app.post('/api/admin/sync-nodes', requireAuth, requireAdmin, (req, res) => {
  const node = {
    id: randomId('sync'),
    name: String(req.body.name || '').trim() || 'sync node',
    url: normalizeBaseUrl(req.body.url || ''),
    port: Number(req.body.port || DEFAULT_SYNC_PORT),
    enabled: req.body.enabled !== false,
    isDefault: Boolean(req.body.isDefault),
    createdAt: now(),
    updatedAt: now(),
    lastStatus: null
  };
  if (node.isDefault) db.syncNodes.forEach((item) => { item.isDefault = false; });
  db.syncNodes.push(node);
  ensureDefaultSyncNode();
  saveDb();
  res.json({ node: safeSyncNode(req, node) });
});

app.patch('/api/admin/sync-nodes/:id', requireAuth, requireAdmin, (req, res) => {
  const node = db.syncNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: 'sync node 不存在。' });
  if ('name' in req.body) node.name = String(req.body.name || '').trim() || node.name;
  if ('url' in req.body) node.url = normalizeBaseUrl(req.body.url || '');
  if ('port' in req.body) node.port = Number(req.body.port || DEFAULT_SYNC_PORT);
  if ('enabled' in req.body) node.enabled = Boolean(req.body.enabled);
  if (req.body.isDefault) {
    db.syncNodes.forEach((item) => { item.isDefault = false; });
    node.isDefault = true;
    node.enabled = true;
  }
  node.updatedAt = now();
  ensureDefaultSyncNode();
  saveDb();
  res.json({ node: safeSyncNode(req, node) });
});

app.delete('/api/admin/sync-nodes/:id', requireAuth, requireAdmin, (req, res) => {
  db.syncNodes = db.syncNodes.filter((item) => item.id !== req.params.id);
  ensureDefaultSyncNode();
  saveDb();
  res.json({ ok: true });
});

app.post('/api/admin/sync-nodes/:id/test', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.syncNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: 'sync node 不存在。' });
  const startedAt = Date.now();
  const result = await testSyncNode(req, node);
  node.lastStatus = {
    ok: true,
    latencyMs: Date.now() - startedAt,
    httpMs: result.timings.httpMs,
    wsMs: result.timings.wsMs,
    body: result.body,
    checkedAt: now()
  };
  saveDb();
  res.json({ ok: true, status: node.lastStatus });
}));

app.get('/api/admin/sync-nodes/:id/browser-test', requireAuth, requireAdmin, (req, res) => {
  const node = db.syncNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: 'sync node 不存在。' });
  if (!node.enabled) return res.status(400).json({ error: 'sync node 已禁用。' });
  res.json({
    syncNode: safeSyncNode(req, node),
    token: signRoomToken(`latency-${req.user.id}`, req.user, 60000)
  });
});

app.post('/api/admin/sync-nodes/:id/browser-status', requireAuth, requireAdmin, (req, res) => {
  const node = db.syncNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: 'sync node 不存在。' });
  const ok = req.body.ok !== false;
  const httpMs = Number(req.body.httpMs || 0);
  const wsMs = Number(req.body.wsMs || 0);
  const latencyMs = Number(req.body.latencyMs || wsMs || httpMs || 0);
  node.lastStatus = {
    ok,
    latencyMs: Number.isFinite(latencyMs) ? Math.round(latencyMs) : 0,
    httpMs: Number.isFinite(httpMs) ? Math.round(httpMs) : 0,
    wsMs: Number.isFinite(wsMs) ? Math.round(wsMs) : 0,
    source: 'browser',
    error: ok ? null : String(req.body.error || '浏览器测速失败'),
    checkedAt: now()
  };
  saveDb();
  res.json({ ok: true, status: node.lastStatus });
});

app.get('/api/admin/storage-nodes', requireAuth, requireAdmin, (req, res) => {
  res.json({ nodes: db.storageNodes.map((node) => safeStorageNode(req, node)) });
});

app.post('/api/admin/storage-nodes', requireAuth, requireAdmin, (req, res) => {
  const node = {
    id: randomId('storage'),
    name: String(req.body.name || '').trim() || 'storage node',
    type: req.body.type === 'local' ? 'local' : 'remote',
    url: normalizeBaseUrl(req.body.url || ''),
    path: String(req.body.path || (req.body.type === 'local' ? STORAGE_ROOT : '/video52000/videos')),
    enabled: req.body.enabled !== false,
    createdAt: now(),
    updatedAt: now(),
    lastStatus: null
  };
  db.storageNodes.push(node);
  saveDb();
  res.json({ node: safeStorageNode(req, node) });
});

app.patch('/api/admin/storage-nodes/:id', requireAuth, requireAdmin, (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  if ('name' in req.body) node.name = String(req.body.name || '').trim() || node.name;
  if ('url' in req.body) node.url = normalizeBaseUrl(req.body.url || '');
  if ('path' in req.body) node.path = String(req.body.path || node.path);
  if ('enabled' in req.body) node.enabled = Boolean(req.body.enabled);
  node.updatedAt = now();
  saveDb();
  res.json({ node: safeStorageNode(req, node) });
});

app.delete('/api/admin/storage-nodes/:id', requireAuth, requireAdmin, (req, res) => {
  db.storageNodes = db.storageNodes.filter((item) => item.id !== req.params.id);
  saveDb();
  res.json({ ok: true });
});

app.get('/api/admin/storage/nodes/:id/list', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  res.json(await listStorageNode(req, node, req.query.path || ''));
}));

app.post('/api/admin/storage/nodes/:id/mkdir', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  const rel = cleanRelativePath(path.posix.join(req.body.path || '', req.body.name || ''));
  if (!rel) return res.status(400).json({ error: '目录名不能为空。' });
  if ((node.type || 'remote') === 'local') {
    fs.mkdirSync(fullStoragePath(node.path || STORAGE_ROOT, rel), { recursive: true });
    return res.json({ ok: true });
  }
  await fetchRemoteJson(node, '/storage/mkdir', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ path: rel })
  });
  res.json({ ok: true });
}));

app.post('/api/admin/storage/nodes/:id/upload', requireAuth, requireAdmin, upload.single('file'), asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  if (!req.file) return res.status(400).json({ error: '请选择文件。' });
  const dirRel = cleanRelativePath(req.body.path || '');
  const filename = path.basename(req.file.originalname || 'video.bin');
  if ((node.type || 'remote') === 'local') {
    const dir = fullStoragePath(node.path || STORAGE_ROOT, dirRel);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, filename), req.file.buffer);
    return res.json({ ok: true, path: cleanRelativePath(path.posix.join(dirRel, filename)) });
  }
  const form = new FormData();
  form.append('path', dirRel);
  form.append('file', new Blob([req.file.buffer], { type: req.file.mimetype }), filename);
  await fetchRemoteJson(node, '/storage/upload', { method: 'POST', body: form, headers: {} });
  res.json({ ok: true });
}));

app.delete('/api/admin/storage/nodes/:id/item', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  const rel = cleanRelativePath(req.query.path || req.body?.path || '');
  if (!rel) return res.status(400).json({ error: '不能删除根目录。' });
  if ((node.type || 'remote') === 'local') {
    fs.rmSync(fullStoragePath(node.path || STORAGE_ROOT, rel), { recursive: true, force: true });
    return res.json({ ok: true });
  }
  await fetchRemoteJson(node, '/storage/delete', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ path: rel })
  });
  res.json({ ok: true });
}));

app.get('/api/admin/storage/nodes/:id/downloads', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  if ((node.type || 'remote') === 'local') {
    return res.json({
      tasks: Array.from(localDownloadTasks.values())
        .filter((task) => task.nodeId === node.id)
        .map(publicDownloadTask)
        .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
    });
  }
  res.json(await fetchRemoteJson(node, '/storage/downloads', { timeout: 10000 }));
}));

app.post('/api/admin/storage/nodes/:id/downloads', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  const body = {
    url: normalizeDownloadUrl(req.body.url),
    path: cleanRelativePath(req.body.path || ''),
    filename: req.body.filename ? safeFilename(req.body.filename) : ''
  };
  if ((node.type || 'remote') === 'local') {
    return res.json({ task: createLocalDownloadTask(node, body) });
  }
  res.json(await fetchRemoteJson(node, '/storage/downloads', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    timeout: 15000
  }));
}));

app.delete('/api/admin/storage/nodes/:id/downloads/:taskId', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const node = db.storageNodes.find((item) => item.id === req.params.id);
  if (!node) return res.status(404).json({ error: '存储节点不存在。' });
  if ((node.type || 'remote') === 'local') {
    const task = localDownloadTasks.get(req.params.taskId);
    if (!task || task.nodeId !== node.id) return res.status(404).json({ error: '下载任务不存在。' });
    if (task.status === 'running' && task.child) {
      task.child.kill('SIGTERM');
      finishDownloadTask(task, 'canceled', '已取消');
    } else {
      localDownloadTasks.delete(task.id);
    }
    return res.json({ ok: true });
  }
  await fetchRemoteJson(node, `/storage/downloads/${encodeURIComponent(req.params.taskId)}`, { method: 'DELETE', timeout: 10000 });
  res.json({ ok: true });
}));

app.get('/api/admin/auth-methods', requireAuth, requireAdmin, (req, res) => {
  res.json({ methods: db.authMethods.map(safeAuthMethod) });
});

app.post('/api/admin/auth-methods', requireAuth, requireAdmin, (req, res) => {
  const mode = req.body.mode === 'key' ? 'key' : 'password';
  const secret = mode === 'key' ? req.body.privateKey : req.body.password;
  if (!String(secret || '').trim()) return res.status(400).json({ error: '验证密钥不能为空。' });
  const method = {
    id: randomId('auth'),
    name: String(req.body.name || '').trim() || 'SSH 验证方式',
    username: String(req.body.username || '').trim() || 'root',
    mode,
    secretCipher: encryptSecret(secret),
    createdAt: now(),
    updatedAt: now()
  };
  db.authMethods.push(method);
  saveDb();
  res.json({ method: safeAuthMethod(method) });
});

app.patch('/api/admin/auth-methods/:id', requireAuth, requireAdmin, (req, res) => {
  const method = db.authMethods.find((item) => item.id === req.params.id);
  if (!method) return res.status(404).json({ error: '验证方式不存在。' });
  if ('name' in req.body) method.name = String(req.body.name || '').trim() || method.name;
  if ('username' in req.body) method.username = String(req.body.username || '').trim() || method.username;
  if ('mode' in req.body) method.mode = req.body.mode === 'key' ? 'key' : 'password';
  const secret = method.mode === 'key' ? req.body.privateKey : req.body.password;
  if (String(secret || '').trim()) method.secretCipher = encryptSecret(secret);
  method.updatedAt = now();
  saveDb();
  res.json({ method: safeAuthMethod(method) });
});

app.delete('/api/admin/auth-methods/:id', requireAuth, requireAdmin, (req, res) => {
  db.authMethods = db.authMethods.filter((item) => item.id !== req.params.id);
  saveDb();
  res.json({ ok: true });
});

app.get('/api/admin/dns-providers', requireAuth, requireAdmin, (req, res) => {
  res.json({ providers: (db.dnsProviders || []).map(safeDnsProvider) });
});

app.post('/api/admin/dns-providers', requireAuth, requireAdmin, (req, res) => {
  const normalized = normalizeDnsProviderPayload(req.body);
  const provider = {
    id: randomId('dns'),
    name: String(req.body.name || '').trim() || (normalized.type === 'dnspod' ? 'DNSPod' : '华为云国际站'),
    enabled: req.body.enabled !== false,
    ...normalized,
    createdAt: now(),
    updatedAt: now(),
    lastStatus: null
  };
  db.dnsProviders.push(provider);
  saveDb();
  res.json({ provider: safeDnsProvider(provider) });
});

app.patch('/api/admin/dns-providers/:id', requireAuth, requireAdmin, (req, res) => {
  const provider = db.dnsProviders.find((item) => item.id === req.params.id);
  if (!provider) return res.status(404).json({ error: 'DNS 验证方式不存在。' });
  const normalized = normalizeDnsProviderPayload(req.body, provider);
  provider.name = String(req.body.name || '').trim() || provider.name;
  provider.enabled = req.body.enabled !== false;
  provider.type = normalized.type;
  provider.config = normalized.config;
  provider.secretCipher = normalized.secretCipher;
  provider.updatedAt = now();
  saveDb();
  res.json({ provider: safeDnsProvider(provider) });
});

app.delete('/api/admin/dns-providers/:id', requireAuth, requireAdmin, (req, res) => {
  db.dnsProviders = db.dnsProviders.filter((item) => item.id !== req.params.id);
  saveDb();
  res.json({ ok: true });
});

app.post('/api/admin/dns-providers/:id/test', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const provider = db.dnsProviders.find((item) => item.id === req.params.id);
  if (!provider) return res.status(404).json({ error: 'DNS 验证方式不存在。' });
  const startedAt = Date.now();
  const result = await testDnsProvider(provider);
  provider.lastStatus = {
    ok: true,
    latencyMs: Date.now() - startedAt,
    checkedAt: now(),
    result
  };
  saveDb();
  res.json({ ok: true, status: provider.lastStatus });
}));

app.get('/api/admin/users', requireAuth, requireAdmin, (req, res) => {
  res.json({ users: db.users.map(safeAdminUser).sort((a, b) => b.createdAt.localeCompare(a.createdAt)) });
});

app.patch('/api/admin/users/:id', requireAuth, requireAdmin, (req, res) => {
  const user = db.users.find((item) => item.id === req.params.id);
  if (!user) return res.status(404).json({ error: '用户不存在。' });
  if ('role' in req.body) user.role = req.body.role === 'admin' ? 'admin' : 'user';
  if ('disabled' in req.body) {
    if (user.id === req.user.id && req.body.disabled) return res.status(400).json({ error: '不能禁用当前登录管理员。' });
    user.disabled = Boolean(req.body.disabled);
  }
  user.updatedAt = now();
  saveDb();
  res.json({ user: safeAdminUser(user) });
});

app.delete('/api/admin/users/:id', requireAuth, requireAdmin, (req, res) => {
  if (req.params.id === req.user.id) return res.status(400).json({ error: '不能删除当前登录管理员。' });
  db.users = db.users.filter((item) => item.id !== req.params.id);
  db.rooms = db.rooms.filter((room) => room.ownerId !== req.params.id);
  saveDb();
  res.json({ ok: true });
});

app.get('/api/admin/rooms', requireAuth, requireAdmin, (req, res) => {
  res.json({ rooms: db.rooms.map(safeAdminRoom).sort((a, b) => b.createdAt.localeCompare(a.createdAt)) });
});

app.patch('/api/admin/rooms/:id', requireAuth, requireAdmin, (req, res) => {
  const room = db.rooms.find((item) => item.id === req.params.id);
  if (!room) return res.status(404).json({ error: '房间不存在。' });
  if ('name' in req.body) room.name = String(req.body.name || '').trim() || room.name;
  if ('syncNodeId' in req.body) {
    const node = db.syncNodes.find((item) => item.id === req.body.syncNodeId);
    if (!node) return res.status(400).json({ error: 'sync node 不存在。' });
    room.syncNodeId = node.id;
  }
  room.updatedAt = now();
  saveDb();
  res.json({ room: safeAdminRoom(room) });
});

app.delete('/api/admin/rooms/:id', requireAuth, requireAdmin, (req, res) => {
  db.rooms = db.rooms.filter((item) => item.id !== req.params.id);
  saveDb();
  res.json({ ok: true });
});

app.get('/api/admin/update-targets', requireAuth, requireAdmin, (req, res) => {
  res.json({ targets: installedNodeTargets(req) });
});

app.post('/api/admin/update-node', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const target = resolveNodeUpdateTarget(req.body.publicUrl);
  const job = {
    id: randomId('upd'),
    status: 'queued',
    publicUrl: target.publicUrl,
    host: target.host,
    roles: target.roles,
    nodeName: target.nodeName,
    logs: [],
    createdAt: now(),
    updatedAt: now()
  };
  db.updateJobs.unshift(job);
  db.updateJobs = db.updateJobs.slice(0, 30);
  saveDb();
  runUpdateJob(job.id, target);
  res.json({ job });
}));

app.patch('/api/admin/node-config', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const target = await buildEditableNodeTarget(req.body);
  const job = {
    id: randomId('upd'),
    status: 'queued',
    publicUrl: target.publicUrl,
    host: target.host,
    roles: target.roles,
    nodeName: target.nodeName,
    logs: [],
    createdAt: now(),
    updatedAt: now()
  };
  db.updateJobs.unshift(job);
  db.updateJobs = db.updateJobs.slice(0, 30);
  saveDb();
  runUpdateJob(job.id, target);
  res.json({ ok: true, job });
}));

app.get('/api/admin/update-jobs', requireAuth, requireAdmin, (req, res) => {
  res.json({ jobs: db.updateJobs || [] });
});

app.delete('/api/admin/update-jobs/:id', requireAuth, requireAdmin, (req, res) => {
  deleteFinishedJob(db.updateJobs || [], req.params.id, '更新任务');
  res.json({ ok: true });
});

app.post('/api/admin/install-node', requireAuth, requireAdmin, asyncRoute(async (req, res) => {
  const method = db.authMethods.find((item) => item.id === req.body.authMethodId);
  if (!method) return res.status(400).json({ error: '请选择 SSH 验证方式。' });
  const useSync = Boolean(req.body.useSync);
  const useStorage = Boolean(req.body.useStorage);
  if (!useSync && !useStorage) return res.status(400).json({ error: '至少选择 sync node 或存储节点中的一种角色。' });

  const host = String(req.body.host || '').trim();
  if (!host) return res.status(400).json({ error: '服务器 IP/域名不能为空。' });
  const servicePort = Number(req.body.servicePort || DEFAULT_SYNC_PORT);
  const requestedPublicUrl = String(req.body.publicUrl || '').trim();
  const useSsl = Boolean(req.body.useSsl) || /^https:\/\//i.test(requestedPublicUrl);
  const bindDomain = cleanBindDomain(req.body.bindDomain || req.body.publicUrl || '');
  const domainRouteInfo = bindDomain ? await inspectBindDomainRoute(bindDomain, host) : null;
  const sslCert = normalizePemInput(req.body.sslCert);
  const sslKey = normalizePemInput(req.body.sslKey);
  const sslMode = useSsl ? normalizeSslMode(req.body.sslMode, Boolean(sslCert || sslKey)) : 'off';
  const fileAutoRenew = boolFromBody(req.body.fileAutoRenew, true);
  const dnsAutoRenew = boolFromBody(req.body.dnsAutoRenew, true);
  let dnsProvider = null;
  let sslDnsProvider = normalizeAcmeDnsProvider(req.body.sslDnsProvider);
  let sslDnsEnv = parseAcmeDnsEnv(req.body.sslDnsEnv);
  if (useSsl && sslMode === 'dns' && req.body.dnsProviderId) {
    dnsProvider = db.dnsProviders.find((item) => item.id === req.body.dnsProviderId && item.enabled !== false);
    if (!dnsProvider) return res.status(400).json({ error: '请选择可用的 DNS 验证方式。' });
    const acme = dnsProviderToAcme(dnsProvider);
    sslDnsProvider = acme.sslDnsProvider;
    sslDnsEnv = acme.sslDnsEnv;
  }
  if (useSsl) {
    if (sslMode === 'manual') {
      if (!sslCert || !sslKey) return res.status(400).json({ error: '手动证书模式需要同时填写 fullchain PEM 和私钥 PEM。' });
      validateTlsPem(sslCert, sslKey);
    } else if (sslMode === 'dns') {
      if (!bindDomain) return res.status(400).json({ error: 'DNS 验证自动申请 HTTPS 证书需要填写绑定域名。' });
      if (!sslDnsProvider || !sslDnsEnv.length) {
        return res.status(400).json({ error: 'DNS 验证需要选择已保存的 DNS API，或手动填写 acme.sh DNS API 名称和环境变量。' });
      }
    } else if (sslMode === 'file') {
      if (!bindDomain) return res.status(400).json({ error: '文件验证自动申请 HTTPS 证书需要填写绑定域名。' });
    }
  }
  const publicUrl = buildInstallPublicUrl({
    publicUrl: requestedPublicUrl,
    bindDomain,
    host,
    servicePort,
    useSsl
  });
  const job = {
    id: randomId('job'),
    status: 'queued',
    host,
    bindDomain,
    domainRouteInfo,
    useSsl,
    sslMode,
    fileAutoRenew: sslMode === 'file' ? fileAutoRenew : false,
    dnsAutoRenew: sslMode === 'dns' ? dnsAutoRenew : false,
    dnsProviderId: dnsProvider?.id || null,
    authMethodId: method.id,
    sshPort: Number(req.body.sshPort || 22),
    installPath: String(req.body.installPath || '/video52000/app'),
    videoRoot: String(req.body.videoRoot || '/video52000/videos'),
    servicePort,
    publicUrl,
    roles: [useSync && 'sync', useStorage && 'storage'].filter(Boolean),
    logs: [],
    createdAt: now(),
    updatedAt: now()
  };
  db.installJobs.unshift(job);
  db.installJobs = db.installJobs.slice(0, 30);
  saveDb();

  runInstallJob(job.id, {
    auth: {
      host,
      port: Number(req.body.sshPort || 22),
      username: method.username,
      mode: method.mode,
      secret: decryptSecret(method.secretCipher)
    },
    authMethodId: method.id,
    nodeName: String(req.body.nodeName || '').trim(),
    publicUrl,
    bindDomain,
    domainRouteInfo,
    useSsl,
    sslMode,
    fileAutoRenew,
    dnsAutoRenew,
    installPath: String(req.body.installPath || '/video52000/app'),
    videoRoot: String(req.body.videoRoot || '/video52000/videos'),
    sslCert,
    sslKey,
    dnsProviderId: dnsProvider?.id || null,
    sslDnsProvider,
    sslDnsEnv,
    servicePort,
    makeDefaultSync: Boolean(req.body.makeDefaultSync),
    useSync,
    useStorage
  });

  res.json({ job });
}));

app.get('/api/admin/install-jobs', requireAuth, requireAdmin, (req, res) => {
  res.json({ jobs: db.installJobs });
});

app.delete('/api/admin/install-jobs/:id', requireAuth, requireAdmin, (req, res) => {
  deleteFinishedJob(db.installJobs || [], req.params.id, '安装任务');
  res.json({ ok: true });
});

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    app: 'video-together',
    storageRoot: STORAGE_ROOT,
    defaultSyncPort: DEFAULT_SYNC_PORT
  });
});

app.get(/.*/, (req, res) => {
  if (req.path.startsWith('/api/')) return res.status(404).json({ error: '接口不存在。' });
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  const status = err.status || err.statusCode || 500;
  res.status(status).json({ error: err.message || '服务器错误。' });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Video Together app listening on http://127.0.0.1:${PORT}`);
  console.log(`Local videos: ${STORAGE_ROOT}`);
});
