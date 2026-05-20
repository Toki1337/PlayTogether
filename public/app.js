const $ = (selector) => document.querySelector(selector);

const LOCAL_SEEK_LOCK_MS = 7000;
const LOCAL_SEEK_COMMIT_LOCK_MS = 6500;
const PROGRESS_SYNC_THRESHOLD_SECONDS = 5;

const state = {
  mode: 'login',
  user: null,
  config: null,
  rooms: [],
  currentRoom: null,
  currentVideoUrl: '',
  currentMediaUrl: '',
  currentMediaType: 'video',
  roomMode: 'video',
  currentMediaMeta: null,
  currentTrackId: '',
  musicQueue: [],
  mediaQueues: {
    video: [],
    audio: []
  },
  playbackModes: {
    video: 'sequence',
    audio: 'sequence'
  },
  ws: null,
  hls: null,
  artPlayer: null,
  media: null,
  audio: null,
  roomSession: null,
  reconnectTimer: null,
  reconnectAttempts: 0,
  latencyTimer: null,
  latencyPending: new Map(),
  suppressUntil: 0,
  mediaIntentUntil: 0,
  explicitPauseUntil: 0,
  localPlayUntil: 0,
  seekIntentUntil: 0,
  localSeekUntil: 0,
  programmaticSeekUntil: 0,
  pendingLocalSeekTarget: null,
  seekCommitTimer: null,
  pendingPauseTimer: null,
  latestRemoteState: null,
  catchupTimer: null,
  bufferingLocally: false,
  sourceLoading: false,
  pendingSeekTime: null,
  pendingAutoplay: false,
  lastSeekTarget: null,
  lastCatchupSeekAt: 0,
  lastLocalControlAt: 0,
  desiredPlaying: false,
  syncTimer: null,
  musicProgressTimer: null,
  musicLyricKey: '',
  musicMetadataInFlight: new Set(),
  musicMetadataNoLyrics: new Set(),
  musicMetadataNoCover: new Set(),
  musicCoverFailures: new Set(),
  mediaThumbCache: new Map(),
  storagePath: '',
  adminStoragePath: '',
  admin: {
    syncNodes: [],
    storageNodes: [],
    authMethods: [],
    dnsProviders: [],
    users: [],
    rooms: [],
    jobs: [],
    updateTargets: [],
    updateJobs: [],
    downloadTasks: []
  },
  adminPage: 'sync',
  installPollBusy: false,
  downloadPollBusy: false
};

async function api(url, options = {}) {
  const init = { ...options, headers: { ...(options.headers || {}) } };
  if (init.body && !(init.body instanceof FormData) && typeof init.body !== 'string') {
    init.headers['content-type'] = 'application/json';
    init.body = JSON.stringify(init.body);
  }
  const response = await fetch(url, init);
  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    const compact = text.replace(/\s+/g, ' ').trim().slice(0, 180);
    const hint = /^</.test(compact)
      ? '服务器返回了 HTML 页面，通常是反代限制、登录页、404/413/502 或节点地址没有指向 API。'
      : '服务器返回了非 JSON 内容。';
    throw new Error(`${hint} HTTP ${response.status}: ${compact || response.statusText}`);
  }
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}

function toast(message) {
  const el = $('#toast');
  el.textContent = message;
  el.classList.remove('hidden');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => el.classList.add('hidden'), 3200);
}

function setText(selector, value) {
  $(selector).textContent = value;
}

function setButtonBusy(button, busy, label) {
  if (!button) return;
  if (busy) {
    button.dataset.originalText = button.textContent;
    button.textContent = label || '处理中';
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
  } else {
    button.textContent = button.dataset.originalText || button.textContent;
    button.disabled = false;
    button.removeAttribute('aria-busy');
  }
}

async function withBusy(button, work, label) {
  try {
    setButtonBusy(button, true, label);
    return await work();
  } catch (error) {
    const messageTarget = button?.closest('form')?.querySelector('.form-message');
    if (messageTarget) messageTarget.textContent = error.message;
    toast(error.message);
    return null;
  } finally {
    setButtonBusy(button, false);
  }
}

function formatTime(value) {
  if (!value) return '-';
  return new Date(value).toLocaleString([], { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function statusClass(kind) {
  return `status-pill status-${kind || 'muted'}`;
}

function setSyncStatus(text, kind) {
  const el = $('#syncStatus');
  el.textContent = text;
  el.className = statusClass(kind);
}

function setLatencyStatus(ms) {
  const el = $('#syncLatency');
  if (!el) return;
  if (!Number.isFinite(ms)) {
    el.textContent = '同步延迟 -';
    el.className = statusClass('muted');
    return;
  }
  const rounded = Math.round(ms);
  el.textContent = `同步延迟 ${rounded}ms`;
  el.className = statusClass(rounded < 120 ? 'good' : rounded < 320 ? 'warn' : 'bad');
}

function setBufferStatus(text = '缓冲状态 -', kind = 'muted') {
  const el = $('#bufferStatus');
  if (!el) return;
  el.textContent = text;
  el.className = statusClass(kind);
}

function feedbackKindFromText(text = '') {
  if (/上一|上移/.test(text)) return 'prev';
  if (/下一|下移/.test(text)) return 'next';
  if (/加入|添加|创建/.test(text)) return 'add';
  if (/播放|进入/.test(text)) return 'play';
  if (/暂停/.test(text)) return 'pause';
  if (/清空|清除|清理|移除|删除|取消/.test(text)) return 'clear';
  return 'press';
}

function triggerButtonFeedback(button, kind = 'press') {
  if (!button || prefersReducedMotion()) return;
  const feedbackClass = `feedback-${kind}`;
  button.classList.remove('control-feedback', 'feedback-press', 'feedback-play', 'feedback-pause', 'feedback-next', 'feedback-prev', 'feedback-add', 'feedback-clear');
  void button.offsetWidth;
  button.classList.add('control-feedback', feedbackClass);
  window.setTimeout(() => {
    button.classList.remove('control-feedback', feedbackClass);
  }, 460);
}

function setPlaybackToggleButton(button, isPlaying) {
  if (!button) return;
  const icon = button.querySelector('.button-icon');
  button.classList.toggle('is-playing', isPlaying);
  button.classList.toggle('is-paused', !isPlaying);
  button.setAttribute('aria-label', isPlaying ? '暂停' : '播放');
  button.title = isPlaying ? '暂停' : '播放';
  if (icon) {
    icon.classList.toggle('icon-play', isPlaying);
    icon.classList.toggle('icon-pause', !isPlaying);
  }
}

function renderPlaybackControls() {
  const isPlaying = Boolean(state.currentMediaUrl && state.desiredPlaying);
  setPlaybackToggleButton($('#playBtn'), isPlaying);
  setPlaybackToggleButton($('#musicPlayBtn'), isPlaying && state.currentMediaType === 'audio');
}

function toggleCurrentPlayback(button) {
  const shouldPause = Boolean(state.currentMediaUrl && state.desiredPlaying);
  triggerButtonFeedback(button, shouldPause ? 'pause' : 'play');
  markMediaIntent();
  if (!state.currentMediaUrl) {
    renderPlaybackControls();
    return toast('请先选择媒体');
  }
  if (shouldPause) {
    commitLocalPause('pause');
    suppressLocalMediaEvents(500);
    pauseMedia();
  } else {
    commitLocalPlay('play');
    suppressLocalMediaEvents(500);
    playMedia().catch(() => scheduleCatchup('等待播放'));
  }
  renderPlaybackControls();
}

function renderEmpty(target, text) {
  const box = typeof target === 'string' ? $(target) : target;
  box.innerHTML = '';
  const empty = document.createElement('div');
  empty.className = 'empty-state';
  empty.textContent = text;
  box.appendChild(empty);
}

function cleanInstallDomain(value) {
  return String(value || '')
    .trim()
    .replace(/^https?:\/\//i, '')
    .replace(/\/.*$/, '')
    .replace(/:\d+$/, '');
}

function buildInstallPublicUrl() {
  const host = String($('#installHost').value || '').trim();
  const domain = cleanInstallDomain($('#installBindDomain').value);
  const useSsl = $('#installUseSsl').checked;
  const servicePort = Number($('#installServicePort').value || 52000);
  const target = domain || host;
  if (!target) return '';
  return `${useSsl ? 'https' : 'http'}://${target}:${servicePort}`;
}

function updateInstallPublicUrl() {
  const next = buildInstallPublicUrl();
  $('#installPublicUrl').value = next;
}

function updateInstallSslFields() {
  const useSsl = $('#installUseSsl')?.checked;
  const mode = $('#installSslMode')?.value || 'dns';
  document.querySelector('[data-ssl-panel]')?.classList.toggle('hidden', !useSsl);
  for (const field of document.querySelectorAll('[data-ssl-panel] [data-ssl-field], [data-ssl-panel] [data-ssl-mode]')) {
    const fieldMode = field.dataset.sslMode;
    field.classList.toggle('hidden', !useSsl || (fieldMode && fieldMode !== mode));
  }
}

function updateNodeConfigSslFields() {
  const useSsl = $('#nodeConfigUseSsl')?.checked;
  const mode = $('#nodeConfigSslMode')?.value || 'manual';
  document.querySelector('[data-node-config-ssl-panel]')?.classList.toggle('hidden', !useSsl);
  for (const field of document.querySelectorAll('[data-node-config-ssl-panel] [data-node-config-ssl-field], [data-node-config-ssl-panel] [data-node-config-ssl-mode]')) {
    const fieldMode = field.dataset.nodeConfigSslMode;
    field.classList.toggle('hidden', !useSsl || (fieldMode && fieldMode !== mode));
  }
}

function updateMetrics() {
  const stats = state.config?.stats || {};
  $('#metricRooms').textContent = stats.rooms ?? state.rooms.length;
  $('#metricSyncNodes').textContent = stats.syncNodes ?? state.config?.syncNodes?.length ?? 0;
  $('#metricStorageNodes').textContent = stats.storageNodes ?? state.config?.storageNodes?.length ?? 0;
  const activeUpdates = state.admin.updateJobs.filter((job) => ['queued', 'running'].includes(job.status)).length;
  $('#metricInstallJobs').textContent = (stats.installJobs ?? state.admin.jobs.filter((job) => ['queued', 'running'].includes(job.status)).length) + activeUpdates;
}

function setAuthMode(mode) {
  state.mode = mode;
  $('#loginTab').classList.toggle('active', mode === 'login');
  $('#registerTab').classList.toggle('active', mode === 'register');
  $('#confirmWrap').classList.toggle('hidden', mode === 'login');
  $('#authSubmit').textContent = mode === 'login' ? '登录' : '注册';
  $('#authPassword').autocomplete = mode === 'login' ? 'current-password' : 'new-password';
  $('#authMessage').textContent = '';
}

function showAuthenticated(user) {
  state.user = user;
  $('#authView').classList.add('hidden');
  $('#appView').classList.remove('hidden');
  $('#logoutBtn').classList.remove('hidden');
  $('#currentUser').textContent = user ? `${user.username} · ${user.role === 'admin' ? '管理员' : '用户'}` : '';
  $('#adminTab').classList.toggle('hidden', user?.role !== 'admin');
}

function showLoggedOut() {
  state.user = null;
  $('#authView').classList.remove('hidden');
  $('#appView').classList.add('hidden');
  $('#logoutBtn').classList.add('hidden');
  $('#currentUser').textContent = '';
}

function switchView(view) {
  for (const name of ['lobby', 'room', 'admin']) {
    $(`#${name}View`).classList.toggle('hidden', name !== view);
  }
  for (const button of document.querySelectorAll('.tab-button')) {
    button.classList.toggle('active', button.dataset.view === view);
  }
  const titles = { lobby: '房间大厅', room: state.currentRoom?.name || '观看房间', admin: '管理员后台' };
  $('#surfaceTitle').textContent = titles[view] || 'Video Together';
  if (view === 'admin') loadAdmin();
}

function switchAdminPage(page) {
  state.adminPage = page || 'sync';
  for (const panel of document.querySelectorAll('[data-admin-panel]')) {
    panel.classList.toggle('hidden', panel.dataset.adminPanel !== state.adminPage);
  }
  for (const button of document.querySelectorAll('[data-admin-page]')) {
    button.classList.toggle('active', button.dataset.adminPage === state.adminPage);
  }
}

async function loadConfig() {
  state.config = await api('/api/config');
  renderSyncSelect();
  renderStorageSelects();
  updateMetrics();
}

async function loadRooms() {
  const data = await api('/api/rooms');
  state.rooms = data.rooms || [];
  renderRooms();
  updateMetrics();
}

function renderSyncSelect() {
  const select = $('#roomSyncNode');
  select.innerHTML = '';
  const defaultOption = document.createElement('option');
  defaultOption.value = '';
  defaultOption.textContent = '默认 sync node';
  select.appendChild(defaultOption);
  for (const node of state.config?.syncNodes || []) {
    const option = document.createElement('option');
    option.value = node.id;
    option.textContent = `${node.name}${node.isDefault ? ' · 默认' : ''}`;
    select.appendChild(option);
  }
}

function renderRooms() {
  const list = $('#roomList');
  list.innerHTML = '';
  if (!state.rooms.length) {
    renderEmpty(list, '暂无房间');
    return;
  }
  for (const room of state.rooms) {
    const item = document.createElement('div');
    item.className = 'list-item room-item';
    const main = document.createElement('div');
    const title = document.createElement('p');
    title.className = 'item-title';
    title.textContent = room.name;
    const meta = document.createElement('p');
    meta.className = 'item-meta';
    meta.textContent = `${room.ownerName} · ${room.syncNodeName} · ${formatTime(room.createdAt)}`;
    const badge = document.createElement('span');
    badge.className = statusClass(room.syncNodeEnabled ? 'good' : 'bad');
    badge.textContent = room.syncNodeEnabled ? '可加入' : '节点停用';
    main.append(title, meta, badge);
    const join = document.createElement('button');
    join.className = 'primary-pill';
    join.type = 'button';
    join.textContent = '加入';
    join.disabled = !room.syncNodeEnabled;
    join.addEventListener('click', () => withBusy(join, () => joinRoom(room.id), '加入中'));
    item.append(main, join);
    list.appendChild(item);
  }
}

async function joinRoom(roomId) {
  const data = await api(`/api/rooms/${roomId}/join`, { method: 'POST' });
  state.currentRoom = data.room;
  state.config.storageNodes = data.storageNodes || state.config.storageNodes || [];
  $('#roomNameLabel').textContent = data.room.name;
  $('#videoUrlInput').value = state.currentMediaUrl || state.currentVideoUrl;
  renderStorageSelects();
  connectSync(data);
  switchView('room');
}

function connectSync(data) {
  if (state.ws) {
    state.ws._manualClose = true;
    state.ws.close();
  }
  clearInterval(state.syncTimer);
  clearTimeout(state.reconnectTimer);
  stopLatencyMonitor();
  state.roomSession = data;
  state.reconnectAttempts = 0;
  openRoomSocket(data);
}

function openRoomSocket(data) {
  const url = `${data.syncNode.wsUrl}?token=${encodeURIComponent(data.token)}`;
  const ws = new WebSocket(url);
  state.ws = ws;
  setSyncStatus('连接中', 'warn');
  ws.addEventListener('open', () => {
    state.reconnectAttempts = 0;
    setSyncStatus(`${data.syncNode.name} 已连接`, 'good');
    startLatencyMonitor();
    state.syncTimer = setInterval(() => {
      if (state.ws?.readyState === WebSocket.OPEN) state.ws.send(JSON.stringify({ type: 'request_sync' }));
    }, 1000);
  });
  ws.addEventListener('close', () => {
    clearInterval(state.syncTimer);
    stopLatencyMonitor();
    if (ws._manualClose || !state.roomSession) {
      setSyncStatus('已断开', 'muted');
      return;
    }
    state.reconnectAttempts += 1;
    if (state.reconnectAttempts > 6) {
      setSyncStatus('连接失败', 'bad');
      return;
    }
    setSyncStatus(`重连中 ${state.reconnectAttempts}/6`, 'warn');
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = setTimeout(() => openRoomSocket(state.roomSession), Math.min(12000, 1000 * state.reconnectAttempts));
  });
  ws.addEventListener('error', () => {
    setSyncStatus('连接异常', 'bad');
  });
  ws.addEventListener('message', (event) => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch {
      return;
    }
    if (message.type === 'snapshot') {
      renderMembers(message.members || []);
      renderMessages(message.messages || []);
      applyRemoteState(message.state, 'snapshot');
    }
    if (message.type === 'members') renderMembers(message.members || []);
    if (message.type === 'state' || message.type === 'state_sync') applyRemoteState(message.state, message.reason || message.type);
    if (message.type === 'chat') appendChat(message.message);
    if (message.type === 'danmaku') showDanmaku(message.message);
    if (message.type === 'latency_pong') handleLatencyPong(message);
  });
}

function startLatencyMonitor() {
  stopLatencyMonitor(false);
  sendLatencyPing();
  state.latencyTimer = setInterval(sendLatencyPing, 3000);
}

function stopLatencyMonitor(clearDisplay = true) {
  clearInterval(state.latencyTimer);
  state.latencyTimer = null;
  state.latencyPending.clear();
  if (clearDisplay) setLatencyStatus(null);
}

function sendLatencyPing() {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  const id = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  state.latencyPending.set(id, performance.now());
  try {
    state.ws.send(JSON.stringify({ type: 'latency_ping', id, clientTime: Date.now() }));
  } catch {
    state.latencyPending.delete(id);
  }
  setTimeout(() => {
    if (state.latencyPending.has(id)) {
      state.latencyPending.delete(id);
      setLatencyStatus(null);
    }
  }, 5000);
}

function handleLatencyPong(message) {
  const startedAt = state.latencyPending.get(message.id);
  if (!startedAt) return;
  state.latencyPending.delete(message.id);
  setLatencyStatus(performance.now() - startedAt);
}

function prepareMediaElement(media) {
  if (!media) return null;
  media.id = 'videoPlayer';
  media.preload = 'auto';
  media.autoplay = false;
  media.setAttribute('playsinline', '');
  media.setAttribute('webkit-playsinline', '');
  media.removeAttribute('crossorigin');
  return media;
}

function ensureVideoVisible() {
  const media = getMedia();
  const root = state.artPlayer?.template?.$player || document.querySelector('#artPlayerMount .artplayer');
  if (!media) return;
  media.style.display = 'block';
  media.style.visibility = 'visible';
  media.style.opacity = '1';
  media.style.objectFit = 'contain';
  media.setAttribute('playsinline', '');
  media.setAttribute('webkit-playsinline', '');
  root?.classList.toggle('is-video-ready', Boolean(media.videoWidth || media.videoHeight || !media.paused));
}

function getPlayerCore() {
  initVideoPlayer();
  return state.artPlayer || state.media;
}

function initVideoPlayer() {
  if (state.artPlayer || state.media) return state.artPlayer || state.media;
  const mount = $('#artPlayerMount');
  if (!mount) return null;
  if (window.Artplayer) {
    state.artPlayer = new window.Artplayer({
      container: mount,
      url: '',
      autoplay: false,
      autoSize: false,
      playsInline: true,
      lang: 'zh-cn',
      theme: '#0071e3',
      volume: 0.8,
      setting: true,
      hotkey: true,
      pip: true,
      mutex: false,
      fullscreen: true,
      fullscreenWeb: true,
      lock: true,
      playbackRate: true,
      aspectRatio: true,
      miniProgressBar: true,
      moreVideoAttr: {
        id: 'videoPlayer',
        preload: 'auto',
        playsInline: true,
        webkitPlaysInline: true
      },
      customType: {
        m3u8(video, url) {
          if (state.hls) {
            state.hls.destroy();
            state.hls = null;
          }
          if (window.Hls?.isSupported()) {
            state.hls = new Hls({
              enableWorker: true,
              maxBufferLength: 90,
              maxMaxBufferLength: 180,
              backBufferLength: 90,
              lowLatencyMode: false
            });
            state.hls.on(Hls.Events.ERROR, (event, data) => {
              if (data?.fatal) setBufferStatus('HLS 播放失败', 'bad');
            });
            state.hls.on(Hls.Events.MANIFEST_PARSED, () => finishSourcePreload());
            state.hls.loadSource(url);
            state.hls.attachMedia(video);
          } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
            video.src = url;
          }
        }
      }
    });
    state.media = prepareMediaElement(state.artPlayer.video || mount.querySelector('video'));
    attachDanmakuLayerToPlayer();
  }
  if (!state.media) {
    mount.innerHTML = '<video id="videoPlayer" controls playsinline preload="auto"></video>';
    state.media = prepareMediaElement(mount.querySelector('video'));
  }
  return state.media;
}

function attachDanmakuLayerToPlayer() {
  const layer = $('#danmakuLayer');
  const target = state.artPlayer?.template?.$layer;
  if (!layer || !target || layer.parentElement === target) return;
  target.appendChild(layer);
}

function getVideoMedia() {
  if (!state.media && state.artPlayer?.video) state.media = prepareMediaElement(state.artPlayer.video);
  return state.media || initVideoPlayer()?.video || state.media;
}

function getAudioMedia() {
  if (!state.audio) state.audio = $('#audioPlayer');
  return state.audio;
}

function getMedia() {
  return state.currentMediaType === 'audio' ? getAudioMedia() : getVideoMedia();
}

function getMediaTime() {
  const art = state.currentMediaType === 'video' ? state.artPlayer : null;
  const media = getMedia();
  return Number((art && Number.isFinite(Number(art.currentTime)) ? art.currentTime : media?.currentTime) || 0);
}

function getMediaPaused() {
  const media = getMedia();
  return !media || media.paused;
}

function getMediaEnded() {
  const media = getMedia();
  return Boolean(media?.ended);
}

function playMedia() {
  const media = getMedia();
  const art = state.currentMediaType === 'video' ? state.artPlayer || getPlayerCore() : null;
  if (!art?.play && !media?.play) return Promise.resolve();
  const result = art?.play ? art.play() : media.play();
  return result?.catch ? result : Promise.resolve();
}

function pauseMedia() {
  const art = state.currentMediaType === 'video' ? state.artPlayer || getPlayerCore() : null;
  if (art?.pause) art.pause();
  else getMedia()?.pause?.();
}

function loadMedia() {
  const media = getMedia();
  if (media?.load) media.load();
}

function setPlaybackRate(rate) {
  const art = state.currentMediaType === 'video' ? state.artPlayer || getPlayerCore() : null;
  const media = getMedia();
  try {
    if (art) art.playbackRate = rate;
    if (media) media.playbackRate = rate;
  } catch {}
}

function sendState(reason, overrides = {}) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  const position = Number(overrides.position);
  try {
    state.ws.send(JSON.stringify({
      type: 'state_update',
      state: {
        videoUrl: state.currentMediaUrl || state.currentVideoUrl,
        mediaUrl: state.currentMediaUrl || state.currentVideoUrl,
        mediaType: state.currentMediaType,
        roomMode: state.roomMode,
        currentTrackId: state.currentTrackId,
        mediaMeta: state.currentMediaMeta,
        playbackModes: normalizePlaybackModes(state.playbackModes),
        isPlaying: overrides.isPlaying ?? !getMediaPaused(),
        position: Number.isFinite(position) ? Math.max(0, position) : getMediaTime(),
        reason
      }
    }));
  } catch {
    setSyncStatus('同步发送失败', 'bad');
  }
}

function markMediaIntent(duration = 1800) {
  state.mediaIntentUntil = Math.max(state.mediaIntentUntil, Date.now() + duration);
}

function suppressLocalMediaEvents(duration = 1000) {
  state.suppressUntil = Math.max(state.suppressUntil, Date.now() + duration);
}

function hasRecentMediaIntent() {
  return Date.now() <= state.mediaIntentUntil;
}

function markExplicitPause(duration = 2500) {
  state.explicitPauseUntil = Date.now() + duration;
  state.localPlayUntil = 0;
}

function hasExplicitPauseIntent() {
  return Date.now() <= state.explicitPauseUntil;
}

function markLocalPlay(duration = 4000) {
  state.localPlayUntil = Date.now() + duration;
  state.explicitPauseUntil = 0;
}

function hasLocalPlayIntent() {
  return Date.now() <= state.localPlayUntil;
}

function markSeekIntent(duration = 1200) {
  state.seekIntentUntil = Math.max(state.seekIntentUntil, Date.now() + duration);
}

function hasSeekIntent() {
  return Date.now() <= state.seekIntentUntil;
}

function markLocalSeekIntent(duration = 3500) {
  const until = Date.now() + duration;
  markMediaIntent(duration);
  markSeekIntent(duration);
  state.localSeekUntil = Math.max(state.localSeekUntil, until);
}

function hasLocalSeekIntent() {
  return Date.now() <= state.localSeekUntil;
}

function markProgrammaticSeek(duration = 1400) {
  state.programmaticSeekUntil = Date.now() + duration;
}

function hasProgrammaticSeekIntent() {
  return Date.now() <= state.programmaticSeekUntil;
}

function rememberLocalSeekTarget(value) {
  const target = Number(value);
  if (Number.isFinite(target)) state.pendingLocalSeekTarget = Math.max(0, target);
}

function localSeekCommitPosition() {
  const target = Number(state.pendingLocalSeekTarget);
  return Number.isFinite(target) ? Math.max(0, target) : getMediaTime();
}

function localRoomStatePatch(isPlaying, position = getMediaTime()) {
  return {
    ...(state.latestRemoteState || {}),
    videoUrl: state.currentMediaUrl || state.currentVideoUrl,
    mediaUrl: state.currentMediaUrl || state.currentVideoUrl,
    mediaType: state.currentMediaType,
    roomMode: state.roomMode,
    currentTrackId: state.currentTrackId,
    mediaMeta: state.currentMediaMeta,
    playbackModes: normalizePlaybackModes(state.playbackModes),
    isPlaying,
    position,
    serverTime: Date.now()
  };
}

function commitLocalSeek(reason = 'seek', options = {}) {
  if (!state.currentMediaUrl || (!options.force && Date.now() <= state.suppressUntil)) return;
  const isPlaying = !getMediaPaused();
  const position = localSeekCommitPosition();
  state.lastLocalControlAt = Date.now();
  state.latestRemoteState = localRoomStatePatch(isPlaying, position);
  sendState(reason, { isPlaying, position });
}

function commitLocalPlay(reason = 'play') {
  if (!state.currentMediaUrl) return;
  state.lastLocalControlAt = Date.now();
  state.desiredPlaying = true;
  state.latestRemoteState = localRoomStatePatch(true);
  markLocalPlay();
  clearCatchupStatus(0);
  renderMediaQueue();
  renderPlaybackControls();
  sendState(reason, { isPlaying: true });
}

function commitLocalPause(reason = 'pause') {
  state.lastLocalControlAt = Date.now();
  state.desiredPlaying = false;
  state.latestRemoteState = localRoomStatePatch(false);
  markExplicitPause(6000);
  clearCatchupStatus(0);
  setPlaybackRate(1);
  renderMediaQueue();
  renderPlaybackControls();
  sendState(reason, { isPlaying: false });
}

function remoteTargetPosition(remote = state.latestRemoteState) {
  if (!remote) return 0;
  let position = Number(remote.position || 0);
  if (remote.isPlaying && Number.isFinite(Number(remote.serverTime))) {
    position += Math.max(0, (Date.now() - Number(remote.serverTime)) / 1000);
  }
  return Math.max(0, position);
}

function remoteProgressDrift(remote = state.latestRemoteState, media = getMedia()) {
  if (!remote || !media) return 0;
  const current = Number(media.currentTime || 0);
  return remoteTargetPosition(remote) - (Number.isFinite(current) ? current : 0);
}

function shouldSyncByProgress(remote = state.latestRemoteState, media = getMedia()) {
  return Math.abs(remoteProgressDrift(remote, media)) > PROGRESS_SYNC_THRESHOLD_SECONDS;
}

function bufferedAheadAt(media, time) {
  if (!media?.buffered) return 0;
  for (let index = 0; index < media.buffered.length; index += 1) {
    const start = media.buffered.start(index);
    const end = media.buffered.end(index);
    if (time >= start && time <= end) return Math.max(0, end - time);
  }
  return 0;
}

function hasBufferedTarget(media, target, minAhead = 0.75) {
  return bufferedAheadAt(media, target) >= minAhead;
}

function canApplyProgressSeek(media, target) {
  return hasBufferedTarget(media, target, 0.5) || mediaCanSeek(media) || media?.readyState >= 3;
}

function clearCatchupStatus(delay = 1800) {
  clearTimeout(state.catchupTimer);
  state.catchupTimer = null;
  state.bufferingLocally = false;
  setPlaybackRate(1);
  if (delay) {
    setTimeout(() => {
      if (!state.bufferingLocally) setBufferStatus('缓冲状态 -', 'muted');
    }, delay);
  } else {
    setBufferStatus('缓冲状态 -', 'muted');
  }
}

function mediaCanSeek(media = getMedia()) {
  if (!media) return false;
  if (media.readyState >= 1) return true;
  try {
    return media.seekable && media.seekable.length > 0;
  } catch {
    return false;
  }
}

function seekMediaTo(target, suppressDuration = 900) {
  const art = state.currentMediaType === 'video' ? state.artPlayer || getPlayerCore() : null;
  const media = getMedia();
  if ((!art && !media) || !Number.isFinite(target)) return;
  if (state.lastSeekTarget !== null && Math.abs(target - state.lastSeekTarget) < 0.6 && Date.now() - state.lastCatchupSeekAt < 5000) return;
  markProgrammaticSeek(Math.max(2500, suppressDuration + 2500));
  markSeekIntent();
  suppressLocalMediaEvents(suppressDuration);
  try {
    const nextTime = Math.max(0, target);
    if (art) art.currentTime = nextTime;
    else media.currentTime = nextTime;
    state.lastSeekTarget = Math.max(0, target);
    state.lastCatchupSeekAt = Date.now();
  } catch {}
}

function setPendingSeek(target) {
  if (!Number.isFinite(target)) return;
  state.pendingSeekTime = Math.max(0, target);
}

function applyPendingSeek() {
  if (state.pendingSeekTime === null || !mediaCanSeek()) return false;
  const target = state.pendingSeekTime;
  state.pendingSeekTime = null;
  seekMediaTo(target, 1200);
  return true;
}

function finishSourcePreload() {
  if (!state.sourceLoading && state.pendingSeekTime === null) return;
  applyPendingSeek();
  state.sourceLoading = false;
  if (state.pendingAutoplay && state.desiredPlaying && !hasExplicitPauseIntent()) {
    markLocalPlay();
    suppressLocalMediaEvents(900);
    playMedia().catch(() => {
      setBufferStatus('已预加载，点击播放继续同步', 'warn');
    });
  } else if (state.currentMediaUrl) {
    setBufferStatus('预加载完成', 'good');
  }
}

function runCatchup() {
  clearTimeout(state.catchupTimer);
  state.catchupTimer = null;
  const media = getMedia();
  const remote = state.latestRemoteState;
  if (hasLocalSeekIntent() || media?.seeking) {
    setBufferStatus('定位缓冲中', 'warn');
    return;
  }
  if (!media || !remote?.isPlaying || !state.currentMediaUrl || hasExplicitPauseIntent() || !state.desiredPlaying) {
    clearCatchupStatus();
    return;
  }
  const target = remoteTargetPosition(remote);
  const drift = remoteProgressDrift(remote, media);
  if (Math.abs(drift) <= PROGRESS_SYNC_THRESHOLD_SECONDS) {
    clearCatchupStatus();
    return;
  }
  if (canApplyProgressSeek(media, target)) {
    seekMediaTo(target, 900);
  }
  const ahead = bufferedAheadAt(media, media.currentTime || 0);
  if (media.readyState >= 3 || ahead >= 0.8) {
    markLocalPlay();
    suppressLocalMediaEvents(900);
    playMedia()
      .then(() => {
        const nextDrift = remoteTargetPosition(remote) - getMediaTime();
        setPlaybackRate(1);
        setBufferStatus(Math.abs(nextDrift) > PROGRESS_SYNC_THRESHOLD_SECONDS ? '已恢复，继续检测进度差' : '播放已同步', 'good');
        clearCatchupStatus(1800);
      })
      .catch(() => scheduleCatchup('等待浏览器允许播放'));
    return;
  }
  setBufferStatus(`本地缓冲中 · 已缓存 ${ahead.toFixed(1)}s`, 'warn');
  state.catchupTimer = setTimeout(runCatchup, 900);
}

function scheduleCatchup(reason = '缓冲') {
  const remote = state.latestRemoteState;
  if (!remote?.isPlaying || !state.currentMediaUrl || hasExplicitPauseIntent() || !state.desiredPlaying) return;
  const media = getMedia();
  if (!shouldSyncByProgress(remote, media)) return;
  if (hasLocalSeekIntent() || media?.seeking) {
    setBufferStatus('定位缓冲中', 'warn');
    return;
  }
  if (state.sourceLoading) {
    setBufferStatus('预加载中', 'warn');
    return;
  }
  state.bufferingLocally = true;
  setBufferStatus(`${reason}，恢复后自动追赶`, 'warn');
  clearTimeout(state.catchupTimer);
  state.catchupTimer = setTimeout(runCatchup, 350);
}

function normalizeMediaInput(value) {
  const url = String(value || '').trim();
  if (!url) return '';
  if (/^(https?:)?\/\//i.test(url)) return url.startsWith('//') ? `${location.protocol}${url}` : url;
  if (url.startsWith('/')) return url;
  return null;
}

function updateMediaModeUi() {
  const isAudioMode = state.roomMode === 'audio';
  $('.player-shell')?.classList.toggle('hidden', isAudioMode);
  $('#musicPanel')?.classList.toggle('hidden', !isAudioMode);
  $('.video-transport')?.classList.toggle('hidden', isAudioMode);
  $('#roomModeVideoBtn')?.classList.toggle('active', state.roomMode === 'video');
  $('#roomModeAudioBtn')?.classList.toggle('active', state.roomMode === 'audio');
  const input = $('#videoUrlInput');
  if (input) {
    input.placeholder = state.roomMode === 'audio' ? 'https://example.com/music.mp3' : 'https://example.com/video.mp4';
  }
  const title = $('#mediaQueueTitle');
  if (title) title.textContent = `${modeLabel()}队列`;
  const modeSelect = $('#mediaPlaybackMode');
  if (modeSelect) modeSelect.value = playbackModeFor(state.roomMode);
  renderMusicPanel();
  renderMediaQueue();
  renderPlaybackControls();
}

function stopInactiveMedia(nextType) {
  if (nextType === 'audio') {
    const art = state.artPlayer;
    if (art?.pause) art.pause();
    const video = getVideoMedia();
    if (video) {
      video.pause?.();
      video.removeAttribute('src');
      if (state.hls) {
        state.hls.destroy();
        state.hls = null;
      }
    }
    return;
  }
  const audio = getAudioMedia();
  if (audio) {
    audio.pause();
    audio.removeAttribute('src');
    audio.load();
  }
}

function setMediaSource(url, options = {}) {
  initVideoPlayer();
  const mediaType = normalizeMediaType(options.mediaType || state.roomMode);
  const previousMediaUrl = state.currentMediaUrl;
  stopInactiveMedia(mediaType);
  state.roomMode = mediaType;
  state.currentMediaType = mediaType;
  const art = mediaType === 'video' ? state.artPlayer : null;
  const player = mediaType === 'audio' ? getAudioMedia() : getVideoMedia();
  if (!player && !art) return;
  const targetTime = Number(options.targetTime);
  state.currentMediaUrl = url || '';
  state.currentVideoUrl = url || '';
  state.currentTrackId = options.currentTrackId || (url === previousMediaUrl ? state.currentTrackId : '') || '';
  state.currentMediaMeta = mediaType === 'audio'
    ? normalizeMusicMeta(options.meta || state.currentMediaMeta, { mediaUrl: url, title: fileTitleFromUrl(url) })
    : null;
  state.sourceLoading = Boolean(url);
  state.pendingAutoplay = Boolean(options.autoplay);
  state.pendingLocalSeekTarget = null;
  state.lastSeekTarget = null;
  state.lastCatchupSeekAt = 0;
  if (Number.isFinite(targetTime) && targetTime > 0) setPendingSeek(targetTime);
  else state.pendingSeekTime = null;
  updateMediaModeUi();
  if (state.hls) {
    state.hls.destroy();
    state.hls = null;
  }
  if (!url) {
    if (art?.reset) art.reset();
    else {
      player.removeAttribute('src');
      loadMedia();
    }
    state.sourceLoading = false;
    state.pendingAutoplay = false;
    state.pendingSeekTime = null;
    clearCatchupStatus(0);
    updateMediaModeUi();
    return;
  }
  player.preload = 'auto';
  if (mediaType === 'audio') {
    player.src = url;
    loadMedia();
    setBufferStatus('音乐预加载中', 'warn');
    updateMusicProgressUi();
    return;
  }
  const isHls = /\.m3u8(?:[?#]|$)/i.test(url);
  if (art) {
    art.type = isHls ? 'm3u8' : '';
    Promise.resolve(art.switchUrl(url))
      .then(() => {
        ensureVideoVisible();
        finishSourcePreload();
      })
      .catch(() => {
        state.sourceLoading = false;
        state.pendingAutoplay = false;
        setBufferStatus('视频加载失败', 'bad');
      });
  } else {
    player.src = url;
    loadMedia();
  }
  ensureVideoVisible();
  setBufferStatus('预加载中', 'warn');
}

function setVideoSource(url, options = {}) {
  setMediaSource(url, { ...options, mediaType: 'video' });
}

function setAudioSource(url, options = {}) {
  setMediaSource(url, { ...options, mediaType: 'audio' });
}

function currentTrackFromQueue(type = state.currentMediaType) {
  return queueFor(type).find((track) => track.id === state.currentTrackId) || null;
}

function coverImageValue(url) {
  return `url(${JSON.stringify(url)})`;
}

function preloadCoverImage(url) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(url);
    image.onerror = reject;
    image.src = url;
  });
}

function applyMusicCover(cover, meta) {
  if (!cover) return;
  const title = meta.title || '音';
  const fallbackText = title.slice(0, 1).toUpperCase();
  const coverUrl = meta.coverUrl || '';
  if (!coverUrl || state.musicCoverFailures.has(coverUrl)) {
    cover.classList.remove('has-cover');
    cover.dataset.coverUrl = coverUrl;
    cover.style.backgroundImage = '';
    cover.textContent = fallbackText;
    return;
  }
  if (cover.dataset.coverUrl === coverUrl && cover.classList.contains('has-cover')) return;
  cover.dataset.coverUrl = coverUrl;
  cover.classList.remove('has-cover');
  cover.style.backgroundImage = '';
  cover.textContent = fallbackText;
  preloadCoverImage(coverUrl)
    .then(() => {
      if (!cover.isConnected || cover.dataset.coverUrl !== coverUrl) return;
      state.musicCoverFailures.delete(coverUrl);
      cover.style.backgroundImage = coverImageValue(coverUrl);
      cover.textContent = '';
      cover.classList.add('has-cover');
    })
    .catch(() => {
      if (!cover.isConnected || cover.dataset.coverUrl !== coverUrl) return;
      state.musicCoverFailures.add(coverUrl);
      cover.classList.remove('has-cover');
      cover.style.backgroundImage = '';
      cover.textContent = fallbackText;
      ensureCurrentAudioMetadata(true);
    });
}

function renderMusicPanel() {
  const queueMeta = currentTrackFromQueue('audio');
  const meta = mergeMusicMeta(queueMeta, state.currentMediaMeta, { mediaUrl: state.currentMediaUrl });
  if (state.currentMediaType === 'audio' && state.currentMediaUrl) state.currentMediaMeta = meta;
  const title = $('#musicTitle');
  const artist = $('#musicArtist');
  const cover = $('#musicCover');
  if (title) title.textContent = state.currentMediaType === 'audio' && state.currentMediaUrl ? meta.title : '未选择音乐';
  if (artist) artist.textContent = state.currentMediaType === 'audio' && state.currentMediaUrl ? (meta.artist || meta.album || '未知艺术家') : '从媒体库或直链菜单添加歌曲';
  applyMusicCover(cover, meta);
  ensureCurrentAudioMetadata();
  updateMusicProgressUi();
  renderPlaybackControls();
}

function mediaQueueRenderKey(mediaType, queue) {
  return JSON.stringify({
    mediaType,
    currentMediaType: state.currentMediaType,
    currentTrackId: state.currentTrackId,
    desiredPlaying: Boolean(state.desiredPlaying),
  items: queue.map((track) => ({
      id: track.id,
      mediaUrl: track.mediaUrl,
      title: track.title,
      artist: track.artist,
      album: track.album,
      coverUrl: track.coverUrl,
      duration: track.duration
    }))
  });
}

function renderMediaQueue() {
  const list = $('#mediaQueueList');
  if (!list) return;
  const mediaType = state.roomMode;
  const queue = queueFor(mediaType);
  const renderKey = mediaQueueRenderKey(mediaType, queue);
  if (list.dataset.renderKey === renderKey && list.childElementCount) {
    updateStorageQueueButtons();
    return;
  }
  list.dataset.renderKey = renderKey;
  list.innerHTML = '';
  if (!queue.length) {
    renderEmpty(list, '队列为空');
    updateStorageQueueButtons();
    return;
  }
  for (const track of queue) {
    const isAudioQueue = mediaType === 'audio';
    const isCurrentTrack = track.id === state.currentTrackId && state.currentMediaType === mediaType;
    const isPlayingTrack = isCurrentTrack && state.desiredPlaying;
    const trackTitle = track.title || fileTitleFromUrl(track.mediaUrl);
    const trackArtist = track.artist || track.album || (isAudioQueue ? '未知艺术家' : modeLabel(mediaType));
    const row = document.createElement('div');
    row.className = [
      'music-queue-item',
      `${mediaType}-queue-item`,
      isCurrentTrack ? 'active' : '',
      isPlayingTrack ? 'playing' : ''
    ].filter(Boolean).join(' ');
    row.dataset.trackId = track.id;
    row.draggable = true;
    const handle = document.createElement('button');
    handle.className = 'queue-drag-handle';
    handle.type = 'button';
    handle.setAttribute('aria-label', `拖动 ${trackTitle}`);
    let cover = null;
    if (isAudioQueue) {
      cover = document.createElement('div');
      cover.className = 'queue-track-cover';
      cover.textContent = track.coverUrl ? '' : (trackTitle || '音').slice(0, 1).toUpperCase();
      cover.style.backgroundImage = track.coverUrl ? `url("${track.coverUrl}")` : '';
    }
    const main = document.createElement('button');
    main.className = 'music-queue-main';
    main.type = 'button';
    main.addEventListener('click', () => {
      triggerButtonFeedback(main, 'play');
      sendQueueMessage('queue_play', { mediaType, trackId: track.id });
    });
    const name = document.createElement('strong');
    name.textContent = trackTitle;
    const meta = document.createElement('span');
    meta.textContent = isAudioQueue
      ? trackArtist
      : [track.artist, formatClock(track.duration)].filter(Boolean).join(' · ') || modeLabel(mediaType);
    main.append(name, meta);
    if (isAudioQueue && isPlayingTrack) {
      const wave = document.createElement('span');
      wave.className = 'queue-playing-wave';
      wave.setAttribute('aria-hidden', 'true');
      for (let i = 0; i < 5; i += 1) wave.appendChild(document.createElement('i'));
      main.appendChild(wave);
    }
    const actions = document.createElement('div');
    actions.className = 'music-queue-actions';
    actions.append(
      actionButton('上移', 'pearl-button', () => sendQueueMessage('queue_move', { mediaType, trackId: track.id, direction: 'up' })),
      actionButton('下移', 'pearl-button', () => sendQueueMessage('queue_move', { mediaType, trackId: track.id, direction: 'down' })),
      actionButton('移除', 'pearl-button', () => sendQueueMessage('queue_remove', { mediaType, trackId: track.id }))
    );
    bindQueueDrag(row, handle, mediaType);
    row.append(handle);
    if (cover) row.append(cover);
    row.append(main, actions);
    list.appendChild(row);
  }
  updateStorageQueueButtons();
}

function queueTrackIdsFromDom() {
  return Array.from($('#mediaQueueList')?.querySelectorAll('.music-queue-item') || [])
    .map((item) => item.dataset.trackId)
    .filter(Boolean);
}

function sendQueueReorder(mediaType) {
  const trackIds = queueTrackIdsFromDom();
  if (trackIds.length) sendQueueMessage('queue_reorder', { mediaType, trackIds });
}

function placeDraggingItem(dragging, target, clientY) {
  if (!dragging || !target || dragging === target || !target.parentElement) return;
  const rect = target.getBoundingClientRect();
  const after = clientY > rect.top + rect.height / 2;
  target.parentElement.insertBefore(dragging, after ? target.nextSibling : target);
}

function bindQueueDrag(row, handle, mediaType) {
  row.addEventListener('dragstart', (event) => {
    if (!event.target.closest('.queue-drag-handle')) {
      event.preventDefault();
      return;
    }
    row.classList.add('dragging');
    event.dataTransfer.effectAllowed = 'move';
  });
  row.addEventListener('dragend', () => {
    if (!row.classList.contains('dragging')) return;
    row.classList.remove('dragging');
    sendQueueReorder(mediaType);
  });
  row.addEventListener('dragover', (event) => {
    const dragging = $('#mediaQueueList')?.querySelector('.music-queue-item.dragging');
    if (!dragging) return;
    event.preventDefault();
    placeDraggingItem(dragging, row, event.clientY);
  });
  handle.addEventListener('pointerdown', (event) => {
    if (event.button && event.button !== 0) return;
    const list = $('#mediaQueueList');
    if (!list) return;
    event.preventDefault();
    row.classList.add('dragging');
    handle.setPointerCapture?.(event.pointerId);
    let moved = false;
    const onMove = (moveEvent) => {
      moved = true;
      const target = document.elementFromPoint(moveEvent.clientX, moveEvent.clientY)?.closest('.music-queue-item');
      if (target && target.parentElement === list) placeDraggingItem(row, target, moveEvent.clientY);
    };
    const onUp = () => {
      handle.removeEventListener('pointermove', onMove);
      handle.removeEventListener('pointerup', onUp);
      handle.removeEventListener('pointercancel', onUp);
      row.classList.remove('dragging');
      if (moved) sendQueueReorder(mediaType);
    };
    handle.addEventListener('pointermove', onMove);
    handle.addEventListener('pointerup', onUp);
    handle.addEventListener('pointercancel', onUp);
  });
}

const MUSIC_PROGRESS_HEIGHT = 34;
const MUSIC_PROGRESS_CENTER = 17;

function prefersReducedMotion() {
  return Boolean(window.matchMedia?.('(prefers-reduced-motion: reduce)').matches);
}

function shouldAnimateMusicProgress() {
  return state.currentMediaType === 'audio'
    && Boolean(state.currentMediaUrl)
    && Boolean(state.desiredPlaying)
    && !prefersReducedMotion();
}

function syncMusicProgressFrame() {
  if (!shouldAnimateMusicProgress()) {
    if (state.musicProgressTimer) {
      cancelAnimationFrame(state.musicProgressTimer);
      state.musicProgressTimer = null;
    }
    return;
  }
  if (state.musicProgressTimer) return;
  state.musicProgressTimer = requestAnimationFrame(() => {
    state.musicProgressTimer = null;
    updateMusicProgressUi();
  });
}

function updateMusicProgressGraphic(progressWrap, progressValue) {
  const svg = progressWrap?.querySelector('.music-wave-svg');
  if (!svg) return;
  const width = Math.max(1, progressWrap.getBoundingClientRect().width || progressWrap.clientWidth || 0);
  const progressRatio = Math.max(0, Math.min(1, progressValue / 1000));
  const endX = width * progressRatio;
  svg.setAttribute('viewBox', `0 0 ${width.toFixed(2)} ${MUSIC_PROGRESS_HEIGHT}`);
  svg.querySelector('.music-wave-rest-line')?.setAttribute('d', `M ${endX.toFixed(2)} ${MUSIC_PROGRESS_CENTER} L ${width.toFixed(2)} ${MUSIC_PROGRESS_CENTER}`);
  svg.querySelector('.music-wave-played-line')?.setAttribute('d', endX > 0.5 ? `M 0 ${MUSIC_PROGRESS_CENTER} L ${endX.toFixed(2)} ${MUSIC_PROGRESS_CENTER}` : '');
  const thumb = svg.querySelector('.music-wave-thumb-dot');
  if (thumb) {
    thumb.setAttribute('cx', endX.toFixed(2));
    thumb.setAttribute('cy', MUSIC_PROGRESS_CENTER);
  }
}

function updateMusicProgressUi() {
  const media = state.currentMediaType === 'audio' ? getAudioMedia() : null;
  const progress = $('#musicProgress');
  if (!progress) return;
  const current = Number(media?.currentTime || 0);
  const duration = Number(media?.duration || state.currentMediaMeta?.duration || currentTrackFromQueue()?.duration || 0);
  const progressValue = duration > 0 ? Math.max(0, Math.min(1000, (current / duration) * 1000)) : 0;
  progress.value = String(Math.round(progressValue));
  const progressWrap = progress.closest('.music-wave-progress');
  if (progressWrap) {
    progressWrap.classList.toggle('is-playing', Boolean(state.currentMediaUrl && state.currentMediaType === 'audio' && state.desiredPlaying));
    updateMusicProgressGraphic(progressWrap, progressValue);
  }
  $('#musicCurrentTime').textContent = formatClock(current);
  $('#musicDuration').textContent = duration > 0 ? formatClock(duration) : '0:00';
  renderLyricsPanel();
  syncMusicProgressFrame();
}

async function buildMusicTrack(url, name = '') {
  const fallback = fallbackMusicMeta(url, name);
  let meta = fallback;
  try {
    const data = await api('/api/media/metadata', {
      method: 'POST',
      body: { url, name: name || fallback.sourceName || fallback.title }
    });
    meta = normalizeMusicMeta(data.metadata, { mediaUrl: url, title: fallback.title, name });
  } catch {
    meta = fallback;
  }
  return {
    id: `track_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`,
    mediaUrl: url,
    mediaType: 'audio',
    title: meta.title,
    artist: meta.artist,
    album: meta.album,
    coverUrl: meta.coverUrl,
    duration: meta.duration,
    lyrics: meta.lyrics || null,
    sourceName: meta.sourceName || name || fallback.sourceName,
    addedAt: new Date().toISOString()
  };
}

function buildFallbackTrack(url, options = {}) {
  const mediaType = normalizeMediaType(options.mediaType || state.roomMode);
  return {
    id: `track_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`,
    mediaUrl: url,
    mediaType,
    title: String(options.name || fileTitleFromUrl(url) || (mediaType === 'audio' ? '未命名音乐' : '未命名视频')).replace(/\.[^.]+$/, ''),
    artist: '',
    album: '',
    coverUrl: '',
    duration: null,
    lyrics: normalizeLyrics(options.lyrics),
    sourceName: options.name || fileTitleFromUrl(url),
    addedAt: new Date().toISOString()
  };
}

async function enrichAudioTrackMetadata(track) {
  if (!track || track.mediaType !== 'audio') return;
  try {
    const data = await api('/api/media/metadata', {
      method: 'POST',
      body: { url: track.mediaUrl, name: track.sourceName || track.title }
    });
    const meta = mergeMusicMeta(track, data.metadata, { mediaUrl: track.mediaUrl, title: track.title, name: track.sourceName || track.title });
    Object.assign(track, meta);
    sendQueueMessage('queue_update', {
      mediaType: 'audio',
      trackId: track.id,
      patch: meta
    });
  } catch {}
}

function ensureCurrentAudioMetadata(force = false) {
  if (state.currentMediaType !== 'audio' || !state.currentMediaUrl) return;
  const url = state.currentMediaUrl;
  const currentMeta = mergeMusicMeta(currentTrackFromQueue('audio'), state.currentMediaMeta, { mediaUrl: url });
  const coverFailed = currentMeta.coverUrl && state.musicCoverFailures.has(currentMeta.coverUrl);
  const lyricsDone = Boolean(currentMeta.lyrics) || state.musicMetadataNoLyrics.has(url);
  const coverDone = Boolean(currentMeta.coverUrl && !coverFailed) || state.musicMetadataNoCover.has(url);
  if (!force && lyricsDone && coverDone) return;
  if (state.musicMetadataInFlight.has(url)) return;
  state.musicMetadataInFlight.add(url);
  api('/api/media/metadata', {
    method: 'POST',
    body: {
      url,
      name: currentMeta.sourceName || currentMeta.title || fileTitleFromUrl(url)
    }
  }).then((data) => {
    if (state.currentMediaUrl !== url || state.currentMediaType !== 'audio') return;
    const meta = mergeMusicMeta(currentMeta, data.metadata, { mediaUrl: url, title: currentMeta.title, name: currentMeta.sourceName || currentMeta.title });
    if (!meta.lyrics) state.musicMetadataNoLyrics.add(url);
    else state.musicMetadataNoLyrics.delete(url);
    if (!meta.coverUrl) state.musicMetadataNoCover.add(url);
    else state.musicMetadataNoCover.delete(url);
    state.currentMediaMeta = meta;
    const track = currentTrackFromQueue('audio');
    if (track && track.mediaUrl === url) Object.assign(track, meta);
    const hasUsefulUpdate = meta.lyrics
      || meta.coverUrl !== currentMeta.coverUrl
      || meta.title !== currentMeta.title
      || meta.artist !== currentMeta.artist
      || meta.album !== currentMeta.album;
    if (track?.id && hasUsefulUpdate) {
      sendQueueMessage('queue_update', {
        mediaType: 'audio',
        trackId: track.id,
        patch: meta
      });
    }
    renderMusicPanel();
    renderMediaQueue();
  }).catch(() => {
    state.musicMetadataNoLyrics.add(url);
  }).finally(() => {
    state.musicMetadataInFlight.delete(url);
  });
}

function sendQueueMessage(type, payload = {}) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
    toast('房间未连接');
    return false;
  }
  try {
    state.ws.send(JSON.stringify({ type, ...payload }));
    return true;
  } catch {
    toast('队列同步发送失败');
    return false;
  }
}

function requestRoomMode(mediaType) {
  const roomMode = normalizeMediaType(mediaType);
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
    toast('房间未连接');
    return false;
  }
  const previousMode = state.roomMode;
  state.roomMode = roomMode;
  state.currentMediaType = roomMode;
  if (previousMode !== roomMode) stopInactiveMedia(roomMode);
  updateMediaModeUi();
  try {
    state.ws.send(JSON.stringify({ type: 'room_mode', roomMode }));
    return true;
  } catch {
    toast('房间模式同步失败');
    return false;
  }
}

async function addMediaTrack(url, options = {}) {
  const normalized = normalizeMediaInput(url);
  if (normalized === null || !normalized) {
    toast(`请输入 http(s) 或 /videos 开头的${modeLabel(options.mediaType || state.roomMode)}地址`);
    return false;
  }
  const mediaType = normalizeMediaType(options.mediaType || state.roomMode);
  const track = buildFallbackTrack(normalized, { ...options, mediaType, name: options.name || fileTitleFromUrl(normalized) });
  const sent = sendQueueMessage('queue_add', { mediaType, track, playNow: Boolean(options.playNow) });
  if (sent && mediaType === 'audio') enrichAudioTrackMetadata(track);
  return sent;
}

async function addMusicTrack(url, options = {}) {
  return addMediaTrack(url, { ...options, mediaType: 'audio' });
}

function applyRemoteState(remote, reason = 'state_sync') {
  if (!remote) return;
  const previousRemote = state.latestRemoteState;
  const previousVersion = Number(previousRemote?.version || 0);
  const remoteVersion = Number(remote.version || 0);
  const isNewerRemoteState = remoteVersion > previousVersion;
  const isStaleRemoteState = remoteVersion && previousVersion && remoteVersion < previousVersion;
  if (isStaleRemoteState) return;
  const remoteMediaUrl = remote.mediaUrl || remote.videoUrl || '';
  const remoteRoomMode = normalizeMediaType(remote.roomMode || remote.mediaType || state.roomMode);
  const remoteMediaType = normalizeMediaType(remote.mediaType || remoteRoomMode);
  state.roomMode = remoteRoomMode;
  state.mediaQueues = normalizeMediaQueues(remote.mediaQueues || { audio: remote.musicQueue || state.mediaQueues.audio, video: state.mediaQueues.video });
  state.playbackModes = normalizePlaybackModes(remote.playbackModes || state.playbackModes);
  state.musicQueue = state.mediaQueues.audio;
  state.currentTrackId = remote.currentTrackId || '';
  const localMeta = mergeMusicMeta(currentTrackFromQueue(remoteMediaType), state.currentMediaMeta, { mediaUrl: remoteMediaUrl });
  state.currentMediaMeta = remote.mediaMeta
    ? mergeMusicMeta(localMeta, remote.mediaMeta, { mediaUrl: remoteMediaUrl })
    : state.currentMediaMeta;
  updateMediaModeUi();
  const target = remoteTargetPosition(remote);
  if (remoteMediaUrl !== state.currentMediaUrl || remoteMediaType !== state.currentMediaType) {
    state.latestRemoteState = remote;
    state.desiredPlaying = Boolean(remote.isPlaying);
    suppressLocalMediaEvents(1200);
    setMediaSource(remoteMediaUrl, {
      mediaType: remoteMediaType,
      targetTime: target,
      autoplay: Boolean(remote.isPlaying),
      meta: remote.mediaMeta,
      currentTrackId: remote.currentTrackId
    });
    return;
  }
  const player = getMedia();
  if (!player) return;
  const isPeriodicSync = reason === 'state_sync';
  const drift = remoteProgressDrift(remote, player);
  if (isPeriodicSync && (hasLocalSeekIntent() || player.seeking) && !isNewerRemoteState) return;
  if (isPeriodicSync && !isNewerRemoteState && remote.isPlaying && hasExplicitPauseIntent()) return;
  if (isPeriodicSync && !isNewerRemoteState && !remote.isPlaying && hasLocalPlayIntent()) return;
  state.latestRemoteState = remote;
  const shouldApplyProgressSync = !isPeriodicSync || Math.abs(drift) > PROGRESS_SYNC_THRESHOLD_SECONDS;
  state.desiredPlaying = Boolean(remote.isPlaying);
  if (!remote.isPlaying) state.pendingAutoplay = false;
  renderMediaQueue();
  updateMusicProgressUi();
  renderPlaybackControls();
  if (!shouldApplyProgressSync) {
    clearCatchupStatus(0);
    if (!player.paused) setPlaybackRate(1);
  } else {
    const seekThreshold = isPeriodicSync ? PROGRESS_SYNC_THRESHOLD_SECONDS : 0.35;
    if (remote.isPlaying) {
      if (hasExplicitPauseIntent() && !isNewerRemoteState) return;
      if (state.sourceLoading) {
        setPendingSeek(target);
        state.pendingAutoplay = true;
        return;
      }
      applyPendingSeek();
      if (Math.abs(drift) > seekThreshold) {
        if (canApplyProgressSeek(player, target)) {
          seekMediaTo(target, 900);
        } else {
          scheduleCatchup('缓存追赶');
        }
      } else if (Math.abs(drift) < 0.45) {
        setPlaybackRate(1);
      }
    } else {
      clearCatchupStatus(0);
      state.pendingAutoplay = false;
      if (state.sourceLoading) {
        setPendingSeek(target);
        return;
      }
      applyPendingSeek();
      if (Number.isFinite(target) && Math.abs(drift) > seekThreshold) seekMediaTo(target, 900);
    }
  }
  if (remote.isPlaying && state.currentMediaUrl && player.paused) {
    if (hasExplicitPauseIntent() && !isNewerRemoteState) return;
    markLocalPlay();
    suppressLocalMediaEvents(1200);
    playMedia().catch(() => scheduleCatchup('等待播放'));
    return;
  }
  if (!remote.isPlaying && !player.paused) {
    state.desiredPlaying = false;
    renderMediaQueue();
    updateMusicProgressUi();
    renderPlaybackControls();
    const explicitRemotePause = reason === 'pause' || reason === 'snapshot' || isNewerRemoteState;
    if (explicitRemotePause || !hasLocalPlayIntent()) {
      suppressLocalMediaEvents(1200);
      pauseMedia();
      clearCatchupStatus();
    }
  }
}

function renderMembers(members) {
  const list = $('#memberList');
  list.innerHTML = '';
  $('#memberCount').textContent = String(members.length);
  if (!members.length) {
    renderEmpty(list, '暂无成员');
    return;
  }
  for (const member of members) {
    const chip = document.createElement('span');
    chip.className = 'member-chip';
    chip.textContent = member.username;
    list.appendChild(chip);
  }
}

function renderMessages(messages) {
  $('#chatMessages').innerHTML = '';
  for (const message of messages) appendChat(message);
}

function appendChat(message) {
  if (!message) return;
  const wrap = document.createElement('div');
  wrap.className = 'chat-message';
  const name = document.createElement('div');
  name.className = 'chat-name';
  name.textContent = message.username;
  const text = document.createElement('div');
  text.className = 'chat-text';
  text.textContent = message.text;
  wrap.append(name, text);
  const box = $('#chatMessages');
  box.appendChild(wrap);
  box.scrollTop = box.scrollHeight;
}

function showDanmaku(message) {
  attachDanmakuLayerToPlayer();
  const layer = $('#danmakuLayer');
  if (!layer) return;
  const el = document.createElement('div');
  el.className = 'danmaku';
  el.textContent = `${message.username}: ${message.text}`;
  el.style.top = `${12 + Math.random() * 64}%`;
  el.style.animationPlayState = 'paused';
  el.addEventListener('animationend', () => el.remove());
  layer.appendChild(el);
  const layerWidth = layer.getBoundingClientRect().width || window.innerWidth || 0;
  const textWidth = el.getBoundingClientRect().width || el.offsetWidth || 0;
  const distance = Math.max(320, Math.ceil(layerWidth + textWidth + 24));
  el.style.setProperty('--danmaku-distance', `-${distance}px`);
  requestAnimationFrame(() => {
    if (el.isConnected) el.style.animationPlayState = '';
  });
}

function sendChat(text) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return toast('房间未连接');
  state.ws.send(JSON.stringify({ type: 'chat', text }));
}

function renderStorageSelects() {
  const nodes = state.config?.storageNodes || [];
  for (const selector of ['#storageNodeSelect', '#adminStorageSelect', '#downloadStorageSelect']) {
    const select = $(selector);
    if (!select) continue;
    const current = select.value;
    select.innerHTML = '';
    for (const node of nodes) {
      const option = document.createElement('option');
      option.value = node.id;
      option.textContent = node.name;
      select.appendChild(option);
    }
    if (current && nodes.some((node) => node.id === current)) select.value = current;
  }
  if (!$('#storageNodeSelect').value && nodes[0]) $('#storageNodeSelect').value = nodes[0].id;
  if (!$('#adminStorageSelect').value && nodes[0]) $('#adminStorageSelect').value = nodes[0].id;
  if ($('#downloadStorageSelect') && !$('#downloadStorageSelect').value && nodes[0]) $('#downloadStorageSelect').value = nodes[0].id;
  if (!nodes.length) {
    $('#storageBrowserStatus').textContent = '没有可用存储节点';
    renderEmpty('#storageFileList', '暂无视频库');
    renderEmpty('#adminFileList', '暂无存储节点');
    renderEmpty('#downloadTaskList', '暂无存储节点');
    return;
  }
  loadStorageBrowser().catch((error) => {
    $('#storageBrowserStatus').textContent = error.message;
  });
}

async function loadStorageBrowser() {
  const nodeId = $('#storageNodeSelect').value;
  if (!nodeId) return;
  $('#storageBrowserStatus').textContent = '加载中';
  const data = await api(`/api/storage/nodes/${nodeId}/list?path=${encodeURIComponent(state.storagePath)}`);
  $('#storagePathLabel').textContent = `/${data.path || ''}`;
  $('#storageBrowserStatus').textContent = `${data.entries?.length || 0} 项`;
  renderFileGrid('#storageFileList', data.entries || [], {
    onDir: (entry) => {
      state.storagePath = entry.path;
      loadStorageBrowser().catch((error) => {
        $('#storageBrowserStatus').textContent = error.message;
      });
    },
    onFile: (entry) => {
      const mediaUrl = entry.mediaUrl || entry.videoUrl;
      if (!entry.isMedia && !entry.isVideo && !entry.isAudio) return toast('只能播放音视频文件');
      if (!mediaUrl) return toast('文件没有可播放地址');
      const mediaType = entry.mediaType || (entry.isAudio ? 'audio' : 'video');
      if (state.roomMode !== mediaType) requestRoomMode(mediaType);
      if (mediaType === 'audio') {
        addMusicTrack(mediaUrl, { name: entry.name, playNow: true }).catch((error) => toast(error.message));
        return;
      }
      addMediaTrack(mediaUrl, { mediaType: 'video', name: entry.name, playNow: true }).catch((error) => toast(error.message));
    },
    onQueue: async (entry) => {
      const mediaUrl = entry.mediaUrl || entry.videoUrl;
      if (!mediaUrl) return false;
      const mediaType = entry.mediaType || (entry.isAudio ? 'audio' : 'video');
      try {
        return await addMediaTrack(mediaUrl, { mediaType, name: entry.name, playNow: false });
      } catch (error) {
        toast(error.message);
        return false;
      }
    }
  });
}

function entryMediaUrl(entry) {
  return entry?.mediaUrl || entry?.videoUrl || '';
}

function entryMediaType(entry) {
  return normalizeMediaType(entry?.mediaType || (entry?.isAudio ? 'audio' : 'video'));
}

function comparableMediaUrl(value) {
  const normalized = normalizeMediaInput(value);
  const url = normalized === null ? String(value || '').trim() : normalized;
  try {
    return new URL(url, location.origin).toString();
  } catch {
    return url;
  }
}

function isMediaUrlQueued(url, mediaType) {
  const target = comparableMediaUrl(url);
  if (!target) return false;
  return queueFor(mediaType).some((track) => comparableMediaUrl(track.mediaUrl || track.videoUrl) === target);
}

function isMediaEntryQueued(entry) {
  return isMediaUrlQueued(entryMediaUrl(entry), entryMediaType(entry));
}

function setFileQueueButtonState(button, queued, animate = false) {
  if (!button) return;
  const wasQueued = button.classList.contains('is-queued');
  button.classList.remove('is-loading');
  button.disabled = Boolean(queued);
  button.classList.toggle('is-queued', Boolean(queued));
  if (!queued) button.classList.remove('queued-confirm');
  button.textContent = queued ? '✓' : '加入队列';
  button.setAttribute('aria-label', queued ? '已添加到队列' : '加入队列');
  if (queued && animate && !wasQueued) {
    button.classList.remove('queued-confirm');
    void button.offsetWidth;
    button.classList.add('queued-confirm');
    window.setTimeout(() => button.classList.remove('queued-confirm'), 720);
  }
}

function updateStorageQueueButtons() {
  for (const button of document.querySelectorAll('.file-action[data-media-url]')) {
    const mediaType = normalizeMediaType(button.dataset.mediaType);
    const queued = isMediaUrlQueued(button.dataset.mediaUrl, mediaType);
    setFileQueueButtonState(button, queued, false);
  }
}

function fileEntryLabel(entry) {
  if (entry.type === 'dir') return '目录';
  if (entry.isAudio) return '音乐';
  if (entry.isVideo) return '视频';
  return '文件';
}

function fileEntryMeta(entry) {
  if (entry.type !== 'file') return entry.path || '';
  const tags = [formatBytes(entry.size)];
  if (entry.isAudio) tags.push('音频');
  if (entry.isVideo) tags.push('视频');
  return tags.filter(Boolean).join(' · ');
}

function audioThumbMetadata(entry) {
  const mediaUrl = entryMediaUrl(entry);
  if (!mediaUrl) return Promise.resolve(null);
  const key = comparableMediaUrl(mediaUrl);
  if (!state.mediaThumbCache.has(key)) {
    const fallback = fallbackMusicMeta(mediaUrl, entry.name);
    state.mediaThumbCache.set(
      key,
      api('/api/media/metadata', {
        method: 'POST',
        body: { url: mediaUrl, name: entry.name || fallback.sourceName || fallback.title }
      })
        .then((data) => normalizeMusicMeta(data.metadata, { mediaUrl, title: fallback.title, name: entry.name }))
        .catch(() => fallback)
    );
  }
  return state.mediaThumbCache.get(key);
}

function hydrateFileThumbnail(entry, thumb) {
  if (!entry?.isAudio || !thumb) return;
  audioThumbMetadata(entry).then((meta) => {
    if (!thumb.isConnected || !meta?.coverUrl) return;
    const coverImage = `url(${JSON.stringify(meta.coverUrl)})`;
    preloadCoverImage(meta.coverUrl).then(() => {
      if (!thumb.isConnected) return;
      let cover = thumb.querySelector('.file-cover-art');
      if (!cover) {
        cover = document.createElement('span');
        cover.className = 'file-cover-art';
        cover.setAttribute('aria-hidden', 'true');
        thumb.appendChild(cover);
      }
      thumb.style.setProperty('--file-cover-url', coverImage);
      cover.style.backgroundImage = coverImage;
      thumb.classList.add('has-cover');
    }).catch(() => {});
  });
}

function createFileThumbnail(entry, mediaUrl, shouldHydrateAudio = false) {
  const thumb = document.createElement('span');
  thumb.className = [
    'file-thumb',
    entry.type === 'dir' ? 'dir' : '',
    entry.isAudio ? 'audio' : '',
    entry.isVideo ? 'video' : ''
  ].filter(Boolean).join(' ');
  const label = document.createElement('span');
  label.className = 'file-thumb-label';
  label.textContent = fileEntryLabel(entry);
  if (entry.type === 'file' && entry.isVideo && mediaUrl) {
    const preview = document.createElement('video');
    preview.src = mediaUrl;
    preview.preload = 'metadata';
    preview.muted = true;
    preview.playsInline = true;
    preview.tabIndex = -1;
    preview.setAttribute('aria-hidden', 'true');
    preview.addEventListener('loadeddata', () => thumb.classList.add('has-preview'), { once: true });
    preview.addEventListener('error', () => preview.remove(), { once: true });
    thumb.appendChild(preview);
  }
  thumb.appendChild(label);
  if (shouldHydrateAudio) hydrateFileThumbnail(entry, thumb);
  return thumb;
}

function renderFileGrid(selector, entries, handlers) {
  const grid = $(selector);
  if (!grid) return;
  grid.innerHTML = '';
  if (!entries.length) {
    renderEmpty(grid, '目录为空');
    return;
  }
  for (const entry of entries) {
    const mediaUrl = entryMediaUrl(entry);
    const isPlayableFile = entry.type === 'file' && (entry.isAudio || entry.isVideo || entry.isMedia);
    const wrap = document.createElement('div');
    wrap.className = 'file-entry';
    const button = document.createElement('button');
    button.className = ['file-button', isPlayableFile ? 'media-file-button' : ''].filter(Boolean).join(' ');
    button.type = 'button';
    const thumb = createFileThumbnail(entry, mediaUrl, Boolean(handlers.onQueue));
    const body = document.createElement('span');
    body.className = 'file-card-body';
    const name = document.createElement('strong');
    name.className = 'file-name';
    name.title = entry.name || '';
    name.textContent = entry.name || fileEntryLabel(entry);
    const meta = document.createElement('span');
    meta.className = 'file-meta';
    meta.textContent = fileEntryMeta(entry);
    body.append(name, meta);
    button.append(thumb, body);
    button.addEventListener('click', () => {
      if (entry.type === 'dir') handlers.onDir?.(entry);
      else handlers.onFile?.(entry);
    });
    wrap.appendChild(button);
    if (isPlayableFile && handlers.onQueue) {
      const mediaType = entryMediaType(entry);
      const queueButton = actionButton('', 'pearl-button file-action', async (event) => {
        event.stopPropagation();
        if (isMediaUrlQueued(mediaUrl, mediaType)) {
          setFileQueueButtonState(queueButton, true, false);
          return;
        }
        queueButton.disabled = true;
        queueButton.classList.add('is-loading');
        queueButton.textContent = '添加中';
        try {
          const added = await handlers.onQueue(entry);
          setFileQueueButtonState(queueButton, Boolean(added), Boolean(added));
        } catch (error) {
          toast(error.message);
          setFileQueueButtonState(queueButton, false, false);
        }
      });
      queueButton.dataset.mediaUrl = mediaUrl;
      queueButton.dataset.mediaType = mediaType;
      setFileQueueButtonState(queueButton, isMediaEntryQueued(entry), false);
      wrap.appendChild(queueButton);
    }
    grid.appendChild(wrap);
  }
}

function parentPath(rel) {
  const parts = String(rel || '').split('/').filter(Boolean);
  parts.pop();
  return parts.join('/');
}

function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(value >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatSpeed(bytesPerSecond) {
  const speed = Number(bytesPerSecond || 0);
  return speed > 0 ? `${formatBytes(speed)}/s` : '-';
}

function formatDuration(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value <= 0) return '';
  if (value < 60) return `${Math.round(value)}s`;
  const minutes = Math.floor(value / 60);
  const rest = Math.round(value % 60);
  if (minutes < 60) return `${minutes}m${rest ? `${rest}s` : ''}`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}h${mins ? `${mins}m` : ''}`;
}

function formatClock(seconds) {
  const value = Math.max(0, Number(seconds) || 0);
  const minutes = Math.floor(value / 60);
  const rest = Math.floor(value % 60);
  return `${minutes}:${String(rest).padStart(2, '0')}`;
}

function isAudioUrl(value) {
  return /\.(mp3|m4a|aac|wav|flac|ogg|opus|weba)(?:[?#]|$)/i.test(String(value || ''));
}

function isVideoUrl(value) {
  return /\.(mp4|webm|ogv|mov|m4v|mkv|m3u8)(?:[?#]|$)/i.test(String(value || ''));
}

function mediaTypeFromUrl(value, fallback = 'video') {
  if (isAudioUrl(value)) return 'audio';
  if (isVideoUrl(value)) return 'video';
  return fallback === 'audio' ? 'audio' : 'video';
}

function normalizeMediaType(value) {
  return value === 'audio' ? 'audio' : 'video';
}

function normalizePlaybackMode(value) {
  if (value === 'random' || value === 'repeat-one') return value;
  return 'sequence';
}

function normalizePlaybackModes(value = {}) {
  return {
    video: normalizePlaybackMode(value.video),
    audio: normalizePlaybackMode(value.audio)
  };
}

function playbackModeFor(type = state.roomMode) {
  state.playbackModes = normalizePlaybackModes(state.playbackModes);
  return state.playbackModes[normalizeMediaType(type)];
}

function modeLabel(value = state.roomMode) {
  return normalizeMediaType(value) === 'audio' ? '音乐' : '视频';
}

function normalizeMediaQueues(value = {}) {
  return {
    video: Array.isArray(value.video) ? value.video : [],
    audio: Array.isArray(value.audio) ? value.audio : Array.isArray(state.musicQueue) ? state.musicQueue : []
  };
}

function queueFor(type = state.roomMode) {
  const mediaType = normalizeMediaType(type);
  state.mediaQueues = normalizeMediaQueues(state.mediaQueues);
  return state.mediaQueues[mediaType];
}

function fileTitleFromUrl(value) {
  try {
    const url = new URL(String(value || ''), location.origin);
    const last = decodeURIComponent(url.pathname.split('/').filter(Boolean).pop() || '');
    return (last || '未命名音乐').replace(/\.[^.]+$/, '');
  } catch {
    return String(value || '未命名音乐').split('/').pop().replace(/\?.*$/, '').replace(/\.[^.]+$/, '') || '未命名音乐';
  }
}

function fallbackMusicMeta(url, name = '') {
  return {
    title: String(name || fileTitleFromUrl(url) || '未命名音乐').replace(/\.[^.]+$/, ''),
    artist: '',
    album: '',
    coverUrl: '',
    duration: null,
    sourceName: name || fileTitleFromUrl(url),
    lyrics: null
  };
}

function normalizeMusicMeta(meta, fallback = {}) {
  const base = fallbackMusicMeta(fallback.mediaUrl || '', fallback.name || fallback.title || '');
  return {
    title: String(meta?.title || base.title || '未命名音乐').slice(0, 160),
    artist: String(meta?.artist || base.artist || '').slice(0, 120),
    album: String(meta?.album || '').slice(0, 120),
    coverUrl: String(meta?.coverUrl || '').slice(0, 3000),
    duration: Number.isFinite(Number(meta?.duration)) ? Math.max(0, Number(meta.duration)) : null,
    sourceName: String(meta?.sourceName || base.sourceName || '').slice(0, 180),
    lyrics: normalizeLyrics(meta?.lyrics || base.lyrics)
  };
}

function mergeMusicMeta(existing, incoming, fallback = {}) {
  const current = normalizeMusicMeta(existing, fallback);
  const next = normalizeMusicMeta(incoming, {
    ...fallback,
    title: current.title,
    name: current.sourceName || current.title
  });
  return {
    title: next.title || current.title,
    artist: next.artist || current.artist,
    album: next.album || current.album,
    coverUrl: next.coverUrl || current.coverUrl,
    duration: next.duration ?? current.duration,
    sourceName: next.sourceName || current.sourceName,
    lyrics: next.lyrics || current.lyrics
  };
}

function normalizeLyrics(lyrics) {
  if (!lyrics || typeof lyrics !== 'object') return null;
  const lines = Array.isArray(lyrics.lines)
    ? lyrics.lines
      .map((line) => {
        const time = Number(line?.time);
        const text = String(line?.text || '').trim().slice(0, 240);
        if (!Number.isFinite(time) || time < 0 || !text) return null;
        return { time, text };
      })
      .filter(Boolean)
      .sort((a, b) => a.time - b.time)
      .slice(0, 600)
    : [];
  const text = String(lyrics.text || '').trim().slice(0, 30000);
  if (!text && !lines.length) return null;
  return {
    type: lines.length ? 'synced' : 'plain',
    source: String(lyrics.source || '').slice(0, 40),
    text: text || lines.map((line) => line.text).join('\n').slice(0, 30000),
    lines
  };
}

function activeLyricIndex(lyrics, currentTime) {
  const lines = lyrics?.lines || [];
  if (!lines.length) return -1;
  let active = -1;
  for (let index = 0; index < lines.length; index += 1) {
    if (lines[index].time > currentTime + 0.12) break;
    active = index;
  }
  return active;
}

function renderLyricsPanel() {
  const panel = $('#musicLyrics');
  if (!panel) return;
  const currentEl = $('#musicLyricsCurrent');
  const nextEl = $('#musicLyricsNext');
  const meta = state.currentMediaMeta || currentTrackFromQueue('audio');
  const lyrics = state.currentMediaType === 'audio' ? normalizeLyrics(meta?.lyrics) : null;
  panel.classList.toggle('hidden', !lyrics);
  if (!lyrics) {
    if (state.musicLyricKey === 'none') return;
    state.musicLyricKey = 'none';
    if (currentEl) currentEl.textContent = '';
    if (nextEl) nextEl.textContent = '';
    return;
  }
  if (lyrics.lines.length) {
    panel.classList.remove('plain');
    const index = activeLyricIndex(lyrics, Number(getAudioMedia()?.currentTime || 0));
    const currentLine = lyrics.lines[Math.max(0, index)] || lyrics.lines[0];
    const nextLine = lyrics.lines[Math.max(0, index) + 1] || null;
    const key = `synced:${index}:${currentLine?.time || 0}:${currentLine?.text || ''}:${nextLine?.text || ''}`;
    if (state.musicLyricKey === key) return;
    state.musicLyricKey = key;
    if (currentEl) currentEl.textContent = currentLine?.text || '...';
    if (nextEl) nextEl.textContent = nextLine?.text || '';
    return;
  }
  panel.classList.add('plain');
  const key = `plain:${lyrics.text.length}:${lyrics.text.slice(0, 40)}`;
  if (state.musicLyricKey === key) return;
  state.musicLyricKey = key;
  if (currentEl) currentEl.textContent = lyrics.text || '暂无歌词';
  if (nextEl) nextEl.textContent = '';
}

async function loadAdmin() {
  if (state.user?.role !== 'admin') return;
  if (!state.config) state.config = { syncNodes: [], storageNodes: [], stats: {} };
  const [syncNodes, storageNodes, authMethods, dnsProviders, users, rooms, jobs, updateTargets, updateJobs] = await Promise.all([
    api('/api/admin/sync-nodes'),
    api('/api/admin/storage-nodes'),
    api('/api/admin/auth-methods'),
    api('/api/admin/dns-providers'),
    api('/api/admin/users'),
    api('/api/admin/rooms'),
    api('/api/admin/install-jobs'),
    api('/api/admin/update-targets'),
    api('/api/admin/update-jobs')
  ]);
  state.admin.syncNodes = syncNodes.nodes || [];
  state.admin.storageNodes = storageNodes.nodes || [];
  state.admin.authMethods = authMethods.methods || [];
  state.admin.dnsProviders = dnsProviders.providers || [];
  state.admin.users = users.users || [];
  state.admin.rooms = rooms.rooms || [];
  state.admin.jobs = jobs.jobs || [];
  state.admin.updateTargets = updateTargets.targets || [];
  state.admin.updateJobs = updateJobs.jobs || [];
  state.config.syncNodes = state.admin.syncNodes.filter((node) => node.enabled);
  state.config.storageNodes = state.admin.storageNodes.filter((node) => node.enabled);
  renderAdminSyncNodes();
  renderAdminStorageNodes();
  renderAuthMethods();
  renderDnsProviders();
  renderAdminUsers();
  renderAdminRooms();
  renderInstallJobs();
  renderInstallDnsProviders();
  renderUpdateTargets();
  renderNodeConfigTargets();
  renderUpdateJobs();
  renderSyncSelect();
  renderStorageSelects();
  loadAdminStorageList().catch(() => {});
  loadDownloadTasks().catch(() => {});
  switchAdminPage(state.adminPage);
  updateMetrics();
}

function renderAdminSyncNodes() {
  const list = $('#syncNodeList');
  list.innerHTML = '';
  if (!state.admin.syncNodes.length) {
    renderEmpty(list, '暂无 sync node');
    return;
  }
  for (const node of state.admin.syncNodes) {
    const item = document.createElement('div');
    item.className = 'list-item';
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    main.querySelector('.item-title').textContent = `${node.name}${node.isDefault ? ' · 默认' : ''}`;
    const timing = formatSyncTiming(node.lastStatus);
    main.querySelector('.item-meta').textContent = `${node.url} · ${node.enabled ? '启用' : '禁用'}${timing}`;
    const badge = document.createElement('span');
    badge.className = statusClass(node.enabled ? (node.lastStatus?.ok ? 'good' : 'warn') : 'muted');
    badge.textContent = node.enabled ? (node.lastStatus?.ok ? '在线' : '待测试') : '禁用';
    main.appendChild(badge);
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(
      actionButton('编辑', 'secondary-pill', () => fillSyncNodeForm(node)),
      actionButton('完整配置', 'secondary-pill', () => {
        const target = state.admin.updateTargets.find((item) => item.syncNodeIds?.includes(node.id) || item.publicUrl === node.url);
        if (target) {
          $('#nodeConfigSelect').value = target.publicUrl;
          fillNodeConfigForm(target);
        }
      }),
      actionButton('用户测速', 'secondary-pill', (event) => testSyncNode(node.id, event.currentTarget)),
      actionButton('默认', 'pearl-button', () => saveSyncNode(node.id, { isDefault: true })),
      actionButton('删除', 'pearl-button', () => deleteSyncNode(node.id))
    );
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function formatSyncTiming(status) {
  if (!status) return '';
  if (!status.ok) return ` · ${status.error || '测试失败'}`;
  if (status.source === 'browser') {
    return ` · 用户 WS ${status.wsMs || status.latencyMs}ms${status.httpMs ? ` · HTTP ${status.httpMs}ms` : ' · HTTP 未测'}`;
  }
  return ` · 主站总 ${status.latencyMs}ms${status.httpMs ? ` · HTTP ${status.httpMs}ms` : ''}${status.wsMs ? ` · WS ${status.wsMs}ms` : ''}`;
}

function renderAdminStorageNodes() {
  const list = $('#storageAdminList');
  list.innerHTML = '';
  if (!state.admin.storageNodes.length) {
    renderEmpty(list, '暂无存储节点');
    return;
  }
  for (const node of state.admin.storageNodes) {
    const item = document.createElement('div');
    item.className = 'list-item';
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    main.querySelector('.item-title').textContent = node.name;
    main.querySelector('.item-meta').textContent = `${node.type} · ${node.url || 'same-origin'} · ${node.enabled ? '启用' : '禁用'}`;
    const badge = document.createElement('span');
    badge.className = statusClass(node.enabled ? 'good' : 'muted');
    badge.textContent = node.enabled ? '可用' : '禁用';
    main.appendChild(badge);
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(
      actionButton('编辑', 'secondary-pill', () => fillStorageNodeForm(node)),
      actionButton('完整配置', 'secondary-pill', () => {
        const target = state.admin.updateTargets.find((item) => item.storageNodeIds?.includes(node.id) || item.publicUrl === node.url);
        if (target) {
          switchAdminPage('sync');
          $('#nodeConfigSelect').value = target.publicUrl;
          fillNodeConfigForm(target);
        }
      }),
      actionButton('删除', 'pearl-button', () => deleteStorageNode(node.id))
    );
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function renderAuthMethods() {
  const list = $('#authMethodList');
  const installSelect = $('#installAuthMethod');
  const configSelect = $('#nodeConfigAuthMethod');
  list.innerHTML = '';
  installSelect.innerHTML = '';
  if (configSelect) configSelect.innerHTML = '';
  if (!state.admin.authMethods.length) {
    renderEmpty(list, '暂无 SSH 验证方式');
  }
  for (const method of state.admin.authMethods) {
    const option = document.createElement('option');
    option.value = method.id;
    option.textContent = `${method.name} · ${method.username}`;
    installSelect.appendChild(option);
    if (configSelect) configSelect.appendChild(option.cloneNode(true));
    const item = document.createElement('div');
    item.className = 'list-item';
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    main.querySelector('.item-title').textContent = method.name;
    main.querySelector('.item-meta').textContent = `${method.username} · ${method.mode === 'key' ? '密钥' : '密码'}`;
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(
      actionButton('编辑', 'secondary-pill', () => fillAuthMethodForm(method)),
      actionButton('删除', 'pearl-button', () => deleteAuthMethod(method.id))
    );
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function renderInstallDnsProviders() {
  for (const selector of ['#installDnsProvider', '#nodeConfigDnsProvider']) {
    const select = $(selector);
    if (!select) continue;
    const current = select.value;
    select.innerHTML = '<option value="">手动填写 DNS API / 证书</option>';
    for (const provider of state.admin.dnsProviders.filter((item) => item.enabled)) {
      const option = document.createElement('option');
      option.value = provider.id;
      option.textContent = `${provider.name} · ${provider.type === 'dnspod' ? 'DNSPod' : '华为云国际站'}`;
      select.appendChild(option);
    }
    select.value = current;
  }
}

function renderDnsProviders() {
  const list = $('#dnsProviderList');
  if (!list) return;
  list.innerHTML = '';
  if (!state.admin.dnsProviders.length) {
    renderEmpty(list, '暂无 DNS API');
    return;
  }
  for (const provider of state.admin.dnsProviders) {
    const item = document.createElement('div');
    item.className = 'list-item';
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    main.querySelector('.item-title').textContent = provider.name;
    const type = provider.type === 'dnspod' ? 'DNSPod' : '华为云国际站';
    const zone = provider.config?.zone || provider.config?.projectName || provider.config?.tokenId || '-';
    const status = provider.lastStatus?.ok ? ` · ${provider.lastStatus.latencyMs}ms` : provider.lastStatus?.error ? ` · ${provider.lastStatus.error}` : '';
    main.querySelector('.item-meta').textContent = `${type} · ${zone} · ${provider.enabled ? '启用' : '禁用'}${status}`;
    const badge = document.createElement('span');
    badge.className = statusClass(provider.enabled ? (provider.lastStatus?.ok ? 'good' : 'warn') : 'muted');
    badge.textContent = provider.enabled ? (provider.lastStatus?.ok ? '已测试' : '待测试') : '禁用';
    main.appendChild(badge);
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(
      actionButton('编辑', 'secondary-pill', () => fillDnsProviderForm(provider)),
      actionButton('测试', 'secondary-pill', (event) => testDnsProvider(provider.id, event.currentTarget)),
      actionButton('删除', 'pearl-button', () => deleteDnsProvider(provider.id))
    );
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function renderAdminUsers() {
  const list = $('#adminUserList');
  if (!list) return;
  list.innerHTML = '';
  if (!state.admin.users.length) {
    renderEmpty(list, '暂无用户');
    return;
  }
  for (const user of state.admin.users) {
    const item = document.createElement('div');
    item.className = 'list-item';
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    main.querySelector('.item-title').textContent = `${user.username}${user.disabled ? ' · 已禁用' : ''}`;
    main.querySelector('.item-meta').textContent = `${user.role} · 房间 ${user.roomCount || 0} · ${formatTime(user.createdAt)}`;
    const badge = document.createElement('span');
    badge.className = statusClass(user.disabled ? 'muted' : user.role === 'admin' ? 'good' : 'warn');
    badge.textContent = user.disabled ? '禁用' : user.role;
    main.appendChild(badge);
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(
      actionButton(user.role === 'admin' ? '设为用户' : '设为管理员', 'secondary-pill', () => updateAdminUser(user.id, { role: user.role === 'admin' ? 'user' : 'admin' })),
      actionButton(user.disabled ? '启用' : '禁用', 'secondary-pill', () => updateAdminUser(user.id, { disabled: !user.disabled })),
      actionButton('删除', 'pearl-button', () => deleteAdminUser(user.id))
    );
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function renderAdminRooms() {
  const list = $('#adminRoomList');
  if (!list) return;
  list.innerHTML = '';
  if (!state.admin.rooms.length) {
    renderEmpty(list, '暂无房间');
    return;
  }
  for (const room of state.admin.rooms) {
    const item = document.createElement('div');
    item.className = 'list-item';
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    main.querySelector('.item-title').textContent = room.name;
    main.querySelector('.item-meta').textContent = `${room.ownerName} · ${room.syncNodeName} · ${formatTime(room.createdAt)}`;
    const badge = document.createElement('span');
    badge.className = statusClass(room.syncNodeEnabled ? 'good' : 'warn');
    badge.textContent = room.syncNodeEnabled ? '可进入' : '节点不可用';
    main.appendChild(badge);
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(
      actionButton('进入', 'secondary-pill', () => joinRoom(room.id)),
      actionButton('删除', 'pearl-button', () => deleteAdminRoom(room.id))
    );
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function renderInstallJobs() {
  const list = $('#installJobs');
  list.innerHTML = '';
  if (!state.admin.jobs.length) {
    renderEmpty(list, '暂无安装任务');
    return;
  }
  for (const job of state.admin.jobs.slice(0, 8)) {
    list.appendChild(renderJobCard(job, 'install'));
  }
}

function isFinishedJob(job) {
  return ['success', 'failed', 'canceled'].includes(job?.status);
}

function jobStatusKind(status) {
  if (status === 'success') return 'good';
  if (status === 'failed') return 'bad';
  if (status === 'queued' || status === 'running') return 'warn';
  return 'muted';
}

function jobStatusText(status) {
  return {
    queued: '排队中',
    running: '运行中',
    success: 'success',
    failed: 'failed',
    canceled: '已取消'
  }[status] || status || 'unknown';
}

function jobSslLabel(job) {
  if (job?.type === 'update') return '更新';
  if (!job?.useSsl) return 'HTTP';
  const sslMode = job.sslMode === 'file' ? '文件验证' : job.sslMode === 'manual' ? '自行输入' : 'DNS 验证';
  return `SSL ${sslMode}`;
}

function jobRenewLabel(job) {
  if (job?.sslMode === 'file') return job.fileAutoRenew ? '文件自动续签' : '文件不自动续签';
  if (job?.sslMode === 'dns') return job.dnsAutoRenew ? 'DNS 自动续签' : 'DNS 不自动续签';
  return '';
}

function appendJobTag(target, text, kind = 'muted') {
  if (!text) return;
  const tag = document.createElement('span');
  tag.className = statusClass(kind);
  tag.textContent = text;
  target.appendChild(tag);
}

function renderJobCard(job, type) {
  const item = document.createElement('div');
  item.className = `list-item job-item job-${job.status || 'unknown'}`;
  const main = document.createElement('div');
  main.className = 'job-main';

  const header = document.createElement('div');
  header.className = 'job-header';
  const title = document.createElement('p');
  title.className = 'item-title';
  title.textContent = type === 'update' ? (job.nodeName || job.host || '节点更新') : (job.host || '节点安装');
  const tags = document.createElement('div');
  tags.className = 'job-tags';
  appendJobTag(tags, jobStatusText(job.status), jobStatusKind(job.status));
  for (const role of job.roles || []) appendJobTag(tags, role, 'muted');
  appendJobTag(tags, jobSslLabel(type === 'update' ? { ...job, type } : job), type === 'update' || job.useSsl ? 'warn' : 'muted');
  appendJobTag(tags, jobRenewLabel(job), 'muted');
  if (job.result?.syncNodeId) appendJobTag(tags, 'sync 已添加', 'good');
  if (job.result?.storageNodeId) appendJobTag(tags, '存储已添加', 'good');
  header.append(title, tags);
  main.appendChild(header);

  if (job.publicUrl) {
    const url = document.createElement('div');
    url.className = 'job-url';
    url.textContent = job.publicUrl;
    main.appendChild(url);
  }

  const latestLog = job.logs?.slice(-1)[0];
  if (latestLog) {
    const log = document.createElement('div');
    log.className = 'job-log';
    log.textContent = latestLog;
    main.appendChild(log);
  }

  if (job.error) {
    const error = document.createElement('div');
    error.className = 'job-error';
    error.textContent = job.error;
    main.appendChild(error);
  }

  const meta = document.createElement('p');
  meta.className = 'item-meta job-time';
  meta.textContent = `更新 ${formatTime(job.updatedAt || job.createdAt)}`;
  main.appendChild(meta);

  item.appendChild(main);
  if (isFinishedJob(job)) {
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster job-actions';
    cluster.append(actionButton('清除', 'pearl-button danger-button', (event) => {
      const clear = type === 'update' ? deleteUpdateJob : deleteInstallJob;
      clear(job.id, event.currentTarget);
    }));
    item.appendChild(cluster);
  }
  return item;
}

function renderUpdateTargets() {
  const select = $('#updateNodeSelect');
  if (!select) return;
  const current = select.value;
  select.innerHTML = '';
  const targets = (state.admin.updateTargets || []).filter((target) => target.canUpdate);
  if (!targets.length) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = '暂无可更新的远程节点';
    select.appendChild(option);
    return;
  }
  for (const target of targets) {
    const option = document.createElement('option');
    option.value = target.publicUrl;
    option.textContent = `${target.name || target.publicUrl} · ${target.roles.join('+')}`;
    select.appendChild(option);
  }
  if (current && targets.some((target) => target.publicUrl === current)) select.value = current;
  else if (targets[0]) select.value = targets[0].publicUrl;
}

function nodeConfigBaseName(target) {
  const first = target.names?.[0] || target.name || '';
  return String(first).replace(/\s+(sync|storage)$/i, '').trim() || target.host || 'node';
}

function renderNodeConfigTargets() {
  const select = $('#nodeConfigSelect');
  if (!select) return;
  const current = select.value;
  const targets = state.admin.updateTargets || [];
  select.innerHTML = '';
  if (!targets.length) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = '暂无可编辑的远程节点';
    select.appendChild(option);
    return;
  }
  for (const target of targets) {
    const option = document.createElement('option');
    option.value = target.publicUrl;
    option.textContent = `${target.name || target.publicUrl} · ${target.roles.join('+')}${target.canUpdate ? '' : ' · 缺少安装记录'}`;
    select.appendChild(option);
  }
  const nextValue = current && targets.some((target) => target.publicUrl === current) ? current : targets[0].publicUrl;
  select.value = nextValue;
  fillNodeConfigForm(targets.find((target) => target.publicUrl === nextValue));
}

function renderUpdateJobs() {
  const list = $('#updateJobs');
  if (!list) return;
  list.innerHTML = '';
  if (!state.admin.updateJobs.length) {
    renderEmpty(list, '暂无更新任务');
    return;
  }
  for (const job of state.admin.updateJobs.slice(0, 8)) {
    list.appendChild(renderJobCard(job, 'update'));
  }
}

function formatDownloadStatus(task) {
  const size = task.size ? ` / ${formatBytes(task.size)}` : '';
  const got = task.downloadedBytes ? `${formatBytes(task.downloadedBytes)}${size}` : task.method || task.type;
  const speed = task.status === 'running' ? `速度 ${formatSpeed(task.speedBytesPerSecond)}` : '';
  const eta = task.status === 'running' ? (task.eta || (task.etaSeconds ? `ETA ${formatDuration(task.etaSeconds)}` : '')) : '';
  return [task.status, got, speed, eta, task.error || task.logs?.slice(-1)[0]].filter(Boolean).join(' · ');
}

function downloadProgressPercent(task) {
  const parsed = Number(task.progressPercent);
  if (Number.isFinite(parsed) && parsed > 0) return Math.max(0, Math.min(100, parsed));
  if (task.size && task.downloadedBytes) return Math.max(0, Math.min(100, (Number(task.downloadedBytes) / Number(task.size)) * 100));
  return task.status === 'success' ? 100 : 0;
}

function renderDownloadTasks() {
  const list = $('#downloadTaskList');
  if (!list) return;
  list.innerHTML = '';
  if (!state.admin.downloadTasks.length) {
    renderEmpty(list, '暂无下载任务');
    return;
  }
  for (const task of state.admin.downloadTasks.slice(0, 12)) {
    const item = document.createElement('div');
    item.className = `list-item job-item job-${task.status}`;
    const main = document.createElement('div');
    main.innerHTML = `<p class="item-title"></p><p class="item-meta"></p>`;
    const title = task.filename || task.outputPath || (task.type === 'magnet' ? '磁力任务' : '直链任务');
    main.querySelector('.item-title').textContent = `${task.nodeName ? `${task.nodeName} · ` : ''}${title}`;
    main.querySelector('.item-meta').textContent = [`/${task.path || ''}`, formatDownloadStatus(task)].filter(Boolean).join(' · ');
    const progress = downloadProgressPercent(task);
    const progressWrap = document.createElement('div');
    progressWrap.className = 'download-progress';
    const progressBar = document.createElement('div');
    progressBar.className = 'download-progress-bar';
    progressBar.style.width = `${progress}%`;
    const progressMeta = document.createElement('div');
    progressMeta.className = 'download-progress-meta';
    const percentText = Number.isFinite(progress) ? `${progress.toFixed(progress > 0 && progress < 10 ? 1 : 0)}%` : '-';
    const etaText = task.eta || (task.etaSeconds ? `剩余 ${formatDuration(task.etaSeconds)}` : '');
    progressMeta.textContent = [
      percentText,
      task.status === 'running' ? `速度 ${formatSpeed(task.speedBytesPerSecond)}` : '',
      etaText
    ].filter(Boolean).join(' · ');
    progressWrap.append(progressBar, progressMeta);
    main.appendChild(progressWrap);
    const badge = document.createElement('span');
    badge.className = statusClass(task.status === 'success' ? 'good' : task.status === 'failed' ? 'bad' : task.status === 'running' ? 'warn' : 'muted');
    badge.textContent = task.status;
    main.appendChild(badge);
    const cluster = document.createElement('div');
    cluster.className = 'button-cluster';
    cluster.append(actionButton(task.status === 'running' ? '取消' : '清理', 'pearl-button', () => deleteDownloadTask(task.id)));
    item.append(main, cluster);
    list.appendChild(item);
  }
}

function actionButton(text, cls, handler) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = cls;
  button.textContent = text;
  button.addEventListener('click', (event) => {
    triggerButtonFeedback(button, feedbackKindFromText(text));
    handler?.(event);
  });
  return button;
}

function fillSyncNodeForm(node) {
  $('#syncNodeId').value = node.id;
  $('#syncNodeName').value = node.name;
  $('#syncNodeUrl').value = node.url;
  $('#syncNodePort').value = node.port || 52000;
  $('#syncNodeEnabled').checked = node.enabled;
  $('#syncNodeDefault').checked = node.isDefault;
}

function fillNodeConfigForm(target) {
  if (!target || !$('#nodeConfigForm')) return;
  $('#nodeConfigOldUrl').value = target.publicUrl || '';
  $('#nodeConfigName').value = nodeConfigBaseName(target);
  $('#nodeConfigPublicUrl').value = target.publicUrl || '';
  $('#nodeConfigHost').value = target.host || '';
  $('#nodeConfigSshPort').value = target.sshPort || 22;
  $('#nodeConfigAuthMethod').value = target.authMethodId || '';
  $('#nodeConfigBindDomain').value = target.bindDomain || cleanInstallDomain(target.publicUrl || '');
  $('#nodeConfigUseSsl').checked = Boolean(target.useSsl);
  $('#nodeConfigServicePort').value = target.servicePort || 52000;
  $('#nodeConfigSslMode').value = target.sslMode && target.sslMode !== 'off' ? target.sslMode : 'manual';
  $('#nodeConfigFileAutoRenew').checked = Boolean(target.fileAutoRenew);
  $('#nodeConfigDnsProvider').value = target.dnsProviderId || '';
  $('#nodeConfigDnsAutoRenew').checked = Boolean(target.dnsAutoRenew);
  $('#nodeConfigSslDnsProvider').value = '';
  $('#nodeConfigSslDnsEnv').value = '';
  $('#nodeConfigSslCert').value = '';
  $('#nodeConfigSslKey').value = '';
  $('#nodeConfigInstallPath').value = target.installPath || '/video52000/app';
  $('#nodeConfigVideoRoot').value = target.videoRoot || '/video52000/videos';
  $('#nodeConfigUseSync').checked = target.roles?.includes('sync');
  $('#nodeConfigUseStorage').checked = target.roles?.includes('storage');
  $('#nodeConfigEnabled').checked = Boolean(target.syncEnabled || target.storageEnabled);
  $('#nodeConfigDefault').checked = Boolean(target.isDefault);
  updateNodeConfigSslFields();
}

function buildNodeConfigPublicUrl() {
  const host = String($('#nodeConfigHost').value || '').trim();
  const domain = cleanInstallDomain($('#nodeConfigBindDomain').value);
  const useSsl = $('#nodeConfigUseSsl').checked;
  const servicePort = Number($('#nodeConfigServicePort').value || 52000);
  const target = domain || host;
  if (!target) return '';
  return `${useSsl ? 'https' : 'http'}://${target}:${servicePort}`;
}

function updateNodeConfigPublicUrl() {
  const next = buildNodeConfigPublicUrl();
  if (next) $('#nodeConfigPublicUrl').value = next;
}

function nodeConfigPayload() {
  return {
    oldPublicUrl: $('#nodeConfigOldUrl').value,
    nodeName: $('#nodeConfigName').value,
    publicUrl: $('#nodeConfigPublicUrl').value,
    host: $('#nodeConfigHost').value,
    sshPort: Number($('#nodeConfigSshPort').value || 22),
    authMethodId: $('#nodeConfigAuthMethod').value,
    bindDomain: cleanInstallDomain($('#nodeConfigBindDomain').value),
    useSsl: $('#nodeConfigUseSsl').checked,
    servicePort: Number($('#nodeConfigServicePort').value || 52000),
    sslMode: $('#nodeConfigSslMode').value,
    fileAutoRenew: $('#nodeConfigFileAutoRenew').checked,
    dnsProviderId: $('#nodeConfigDnsProvider').value,
    dnsAutoRenew: $('#nodeConfigDnsAutoRenew').checked,
    sslDnsProvider: $('#nodeConfigSslDnsProvider').value,
    sslDnsEnv: $('#nodeConfigSslDnsEnv').value,
    sslCert: $('#nodeConfigSslCert').value,
    sslKey: $('#nodeConfigSslKey').value,
    installPath: $('#nodeConfigInstallPath').value,
    videoRoot: $('#nodeConfigVideoRoot').value,
    useSync: $('#nodeConfigUseSync').checked,
    useStorage: $('#nodeConfigUseStorage').checked,
    enabled: $('#nodeConfigEnabled').checked,
    isDefault: $('#nodeConfigDefault').checked
  };
}

async function saveSyncNode(id, patch) {
  if (id) await api(`/api/admin/sync-nodes/${id}`, { method: 'PATCH', body: patch });
  else await api('/api/admin/sync-nodes', { method: 'POST', body: patch });
  toast('sync node 已保存');
  await loadAdmin();
  await loadConfig();
  return true;
}

function isMixedContentUrl(url) {
  return location.protocol === 'https:' && /^(http|ws):\/\//i.test(url);
}

function browserNetworkHint(url, protocol) {
  if (isMixedContentUrl(url)) {
    return `${protocol} 被浏览器拦截：当前页面是 HTTPS，但节点地址是 ${url.split(':')[0]}。请使用带可信证书的 HTTPS/WSS 端口直连地址，例如 https://域名:52000。`;
  }
  return `${protocol} 无法从当前浏览器直连。请检查本机网络、防火墙、端口 52000、CORS 或浏览器插件拦截。`;
}

async function measureHttpLatency(baseUrl) {
  const startedAt = performance.now();
  const url = `${baseUrl.replace(/\/+$/, '')}/health?client_ts=${Date.now()}`;
  let response;
  try {
    response = await fetch(url, { cache: 'no-store', mode: 'cors' });
  } catch {
    throw new Error(browserNetworkHint(url, 'HTTP'));
  }
  if (!response.ok) throw new Error(`HTTP 测试失败: ${response.status}`);
  await response.json().catch(() => ({}));
  return performance.now() - startedAt;
}

async function measureWsOpenLatency(wsUrl, token) {
  return new Promise((resolve, reject) => {
    const startedAt = performance.now();
    const url = `${wsUrl}?token=${encodeURIComponent(token)}`;
    if (isMixedContentUrl(wsUrl)) {
      reject(new Error(browserNetworkHint(wsUrl, 'WebSocket')));
      return;
    }
    const ws = new WebSocket(url);
    const timer = setTimeout(() => {
      ws.close();
      reject(new Error('WebSocket 测速超时'));
    }, 8000);
    ws.addEventListener('open', () => {
      clearTimeout(timer);
      const latency = performance.now() - startedAt;
      ws.close();
      resolve(latency);
    }, { once: true });
    ws.addEventListener('error', () => {
      clearTimeout(timer);
      reject(new Error(browserNetworkHint(wsUrl, 'WebSocket')));
    }, { once: true });
  });
}

async function testSyncNode(id, button) {
  await withBusy(button, async () => {
    const data = await api(`/api/admin/sync-nodes/${id}/browser-test`);
    let httpMs = 0;
    let httpError = null;
    try {
      httpMs = await measureHttpLatency(data.syncNode.url);
    } catch (error) {
      httpError = error;
    }
    let wsMs = 0;
    try {
      wsMs = await measureWsOpenLatency(data.syncNode.wsUrl, data.token);
    } catch (error) {
      await api(`/api/admin/sync-nodes/${id}/browser-status`, {
        method: 'POST',
        body: { ok: false, httpMs, wsMs: 0, latencyMs: 0, error: [error.message, httpError?.message].filter(Boolean).join('；') }
      });
      throw error;
    }
    await api(`/api/admin/sync-nodes/${id}/browser-status`, {
      method: 'POST',
      body: { ok: true, httpMs, wsMs, latencyMs: wsMs }
    });
    toast(`用户到 sync node：WS ${Math.round(wsMs)}ms${httpMs ? `，HTTP ${Math.round(httpMs)}ms` : '，HTTP 未测'}`);
    if (httpError) toast(httpError.message);
  }, '测速中');
  await loadAdmin();
}

async function deleteSyncNode(id) {
  await api(`/api/admin/sync-nodes/${id}`, { method: 'DELETE' });
  toast('sync node 已删除');
  await loadAdmin();
  await loadConfig();
}

function fillStorageNodeForm(node) {
  $('#storageAdminNodeId').value = node.id;
  $('#storageAdminName').value = node.name;
  $('#storageAdminType').value = node.type;
  $('#storageAdminUrl').value = node.url;
  $('#storageAdminPath').value = node.path;
  $('#storageAdminEnabled').checked = node.enabled;
}

async function deleteStorageNode(id) {
  await api(`/api/admin/storage-nodes/${id}`, { method: 'DELETE' });
  toast('存储节点已删除');
  await loadAdmin();
  await loadConfig();
}

function fillAuthMethodForm(method) {
  $('#authMethodId').value = method.id;
  $('#authMethodName').value = method.name;
  $('#authMethodUsername').value = method.username;
  $('#authMethodMode').value = method.mode;
  $('#authMethodSecret').value = '';
}

async function deleteAuthMethod(id) {
  await api(`/api/admin/auth-methods/${id}`, { method: 'DELETE' });
  toast('验证方式已删除');
  await loadAdmin();
}

function updateDnsProviderFields() {
  const type = $('#dnsProviderType')?.value || 'dnspod';
  document.querySelectorAll('[data-dns-type]').forEach((el) => {
    el.classList.toggle('hidden', el.dataset.dnsType !== type);
  });
}

function fillDnsProviderForm(provider) {
  $('#dnsProviderId').value = provider.id;
  $('#dnsProviderName').value = provider.name;
  $('#dnsProviderType').value = provider.type;
  $('#dnsProviderEnabled').checked = provider.enabled;
  $('#dnsDnspodTokenId').value = provider.config?.tokenId || '';
  $('#dnsDnspodToken').value = '';
  $('#dnsDnspodEndpoint').value = provider.config?.endpoint || 'https://api.dnspod.com';
  $('#dnsHuaweiUsername').value = provider.config?.username || '';
  $('#dnsHuaweiPassword').value = '';
  $('#dnsHuaweiDomainName').value = provider.config?.domainName || '';
  $('#dnsHuaweiProjectName').value = provider.config?.projectName || '';
  $('#dnsHuaweiRegion').value = provider.config?.region || '';
  $('#dnsHuaweiIamEndpoint').value = provider.config?.iamEndpoint || 'https://iam.myhuaweicloud.com';
  $('#dnsHuaweiDnsEndpoint').value = provider.config?.dnsEndpoint || '';
  updateDnsProviderFields();
}

function dnsProviderPayload() {
  return {
    name: $('#dnsProviderName').value,
    type: $('#dnsProviderType').value,
    enabled: $('#dnsProviderEnabled').checked,
    tokenId: $('#dnsDnspodTokenId').value,
    token: $('#dnsDnspodToken').value,
    endpoint: $('#dnsDnspodEndpoint').value,
    username: $('#dnsHuaweiUsername').value,
    password: $('#dnsHuaweiPassword').value,
    domainName: $('#dnsHuaweiDomainName').value,
    projectName: $('#dnsHuaweiProjectName').value,
    region: $('#dnsHuaweiRegion').value,
    iamEndpoint: $('#dnsHuaweiIamEndpoint').value,
    dnsEndpoint: $('#dnsHuaweiDnsEndpoint').value
  };
}

async function testDnsProvider(id, button) {
  await withBusy(button, async () => {
    const data = await api(`/api/admin/dns-providers/${id}/test`, { method: 'POST' });
    const result = data.status?.result?.raw;
    toast(`DNS API 测试通过${result?.zoneCount ? `，Zone ${result.zoneCount}` : result?.domainCount ? `，域名 ${result.domainCount}` : ''}`);
  }, '测试中');
  await loadAdmin();
}

async function deleteDnsProvider(id) {
  await api(`/api/admin/dns-providers/${id}`, { method: 'DELETE' });
  toast('DNS API 已删除');
  await loadAdmin();
}

async function updateAdminUser(id, patch) {
  await api(`/api/admin/users/${id}`, { method: 'PATCH', body: patch });
  toast('用户已更新');
  await loadAdmin();
}

async function deleteAdminUser(id) {
  if (!confirm('删除用户会同时删除其创建的房间，继续？')) return;
  await api(`/api/admin/users/${id}`, { method: 'DELETE' });
  toast('用户已删除');
  await loadAdmin();
  await loadRooms();
}

async function deleteAdminRoom(id) {
  if (!confirm('删除这个房间？')) return;
  await api(`/api/admin/rooms/${id}`, { method: 'DELETE' });
  toast('房间已删除');
  await loadAdmin();
  await loadRooms();
}

async function loadAdminStorageList() {
  const nodeId = $('#adminStorageSelect').value;
  if (!nodeId) {
    $('#adminStorageStatus').textContent = '没有可管理的存储节点';
    renderEmpty('#adminFileList', '暂无存储节点');
    return;
  }
  $('#adminStorageStatus').textContent = '加载中';
  const data = await api(`/api/admin/storage/nodes/${nodeId}/list?path=${encodeURIComponent(state.adminStoragePath)}`);
  $('#adminStoragePath').textContent = `/${data.path || ''}`;
  if ($('#downloadPath')) $('#downloadPath').placeholder = `/${data.path || ''}`;
  $('#adminStorageStatus').textContent = `${data.entries?.length || 0} 项`;
  renderFileGrid('#adminFileList', data.entries || [], {
    onDir: (entry) => {
      state.adminStoragePath = entry.path;
      loadAdminStorageList().catch((error) => {
        $('#adminStorageStatus').textContent = error.message;
      });
    },
    onFile: (entry) => deleteStorageItem(entry.path)
  });
}

async function deleteStorageItem(path) {
  if (!confirm(`删除 ${path}？`)) return;
  await api(`/api/admin/storage/nodes/${$('#adminStorageSelect').value}/item?path=${encodeURIComponent(path)}`, { method: 'DELETE' });
  toast('已删除');
  await loadAdminStorageList();
}

async function loadDownloadTasks() {
  const nodeId = $('#downloadStorageSelect')?.value || $('#adminStorageSelect')?.value;
  if (!nodeId) {
    state.admin.downloadTasks = [];
    renderDownloadTasks();
    return;
  }
  const data = await api(`/api/admin/storage/nodes/${nodeId}/downloads`);
  const node = state.config?.storageNodes?.find((item) => item.id === nodeId);
  state.admin.downloadTasks = (data.tasks || []).map((task) => ({
    ...task,
    nodeId,
    nodeName: task.nodeName || node?.name || ''
  }));
  renderDownloadTasks();
}

async function deleteDownloadTask(taskId) {
  const nodeId = $('#downloadStorageSelect')?.value || $('#adminStorageSelect')?.value;
  if (!nodeId || !taskId) return;
  await api(`/api/admin/storage/nodes/${nodeId}/downloads/${encodeURIComponent(taskId)}`, { method: 'DELETE' });
  await loadDownloadTasks();
}

async function deleteInstallJob(jobId, button) {
  if (!jobId) return;
  const deleted = await withBusy(button, () => api(`/api/admin/install-jobs/${encodeURIComponent(jobId)}`, { method: 'DELETE' }), '清除中');
  if (!deleted) return;
  state.admin.jobs = state.admin.jobs.filter((job) => job.id !== jobId);
  renderInstallJobs();
  updateMetrics();
  toast('安装任务已清除');
}

async function deleteUpdateJob(jobId, button) {
  if (!jobId) return;
  const deleted = await withBusy(button, () => api(`/api/admin/update-jobs/${encodeURIComponent(jobId)}`, { method: 'DELETE' }), '清除中');
  if (!deleted) return;
  state.admin.updateJobs = state.admin.updateJobs.filter((job) => job.id !== jobId);
  renderUpdateJobs();
  updateMetrics();
  toast('更新任务已清除');
}

function bindEvents() {
  $('#loginTab').addEventListener('click', () => setAuthMode('login'));
  $('#registerTab').addEventListener('click', () => setAuthMode('register'));
  $('#authForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const submitter = event.submitter || $('#authSubmit');
    try {
      const data = await withBusy(submitter, async () => {
        const endpoint = state.mode === 'login' ? '/api/auth/login' : '/api/auth/register';
        return api(endpoint, {
          method: 'POST',
          body: {
            username: $('#authUsername').value,
            password: $('#authPassword').value,
            confirmPassword: $('#authConfirm').value
          }
        });
      });
      if (!data) return;
      showAuthenticated(data.user);
      await loadConfig();
      await loadRooms();
      switchView('lobby');
    } catch (error) {
      $('#authMessage').textContent = error.message;
    }
  });
  $('#logoutBtn').addEventListener('click', async () => {
    await api('/api/auth/logout', { method: 'POST' });
    state.roomSession = null;
    stopLatencyMonitor();
    if (state.ws) {
      state.ws._manualClose = true;
      state.ws.close();
    }
    showLoggedOut();
  });
  document.querySelector('[data-action="home"]').addEventListener('click', () => {
    if (state.user) switchView('lobby');
  });
  for (const button of document.querySelectorAll('.tab-button[data-view]')) {
    button.addEventListener('click', () => switchView(button.dataset.view));
  }
  for (const button of document.querySelectorAll('[data-admin-page]')) {
    button.addEventListener('click', () => switchAdminPage(button.dataset.adminPage));
  }
  $('#refreshRoomsBtn').addEventListener('click', () => loadRooms().catch((error) => toast(error.message)));
  $('#createRoomForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = await withBusy(event.submitter, () => api('/api/rooms', {
      method: 'POST',
      body: { name: $('#roomName').value, syncNodeId: $('#roomSyncNode').value || null }
    }), '创建中');
    if (!data) return;
    $('#roomName').value = '';
    await loadRooms();
    await joinRoom(data.room.id);
  });

  initVideoPlayer();
  const player = getMedia();
  if (player) {
    const playerEventTicks = new Map();
    const bindPlayerEvent = (eventName, handler) => {
      const deduped = (...args) => {
        const now = Date.now();
        if (now - (playerEventTicks.get(eventName) || 0) < 40) return;
        playerEventTicks.set(eventName, now);
        handler(...args);
      };
      if (state.artPlayer?.on) state.artPlayer.on(`video:${eventName}`, deduped);
      player.addEventListener(eventName, deduped);
    };
    const markPlayerIntent = () => {
      markMediaIntent(2500);
      if (getMediaPaused()) markLocalPlay(2500);
      else markExplicitPause(2500);
    };
    const shell = state.artPlayer?.template?.$player || document.querySelector('.player-shell');
    for (const eventName of ['pointerdown', 'touchstart', 'keydown']) {
      shell?.addEventListener(eventName, markPlayerIntent);
    }
    for (const eventName of ['pointerdown', 'touchstart', 'keydown']) {
      player.addEventListener(eventName, markPlayerIntent);
    }
    bindPlayerEvent('seeking', () => {
      if (hasProgrammaticSeekIntent()) return;
      rememberLocalSeekTarget(getMediaTime());
      markLocalSeekIntent(LOCAL_SEEK_LOCK_MS);
      state.bufferingLocally = false;
      setPlaybackRate(1);
      clearCatchupStatus(0);
      setBufferStatus('正在定位', 'warn');
    });
    state.artPlayer?.on?.('seek', (currentTime, requestedTime) => {
      if (hasProgrammaticSeekIntent()) return;
      rememberLocalSeekTarget(Number.isFinite(Number(requestedTime)) ? requestedTime : currentTime);
      markLocalSeekIntent(LOCAL_SEEK_LOCK_MS);
      setPlaybackRate(1);
      setBufferStatus('正在定位', 'warn');
      clearTimeout(state.seekCommitTimer);
      state.seekCommitTimer = setTimeout(() => commitLocalSeek('seek', { force: true }), 180);
    });
    bindPlayerEvent('play', () => {
      ensureVideoVisible();
      clearTimeout(state.pendingPauseTimer);
      markLocalPlay();
      setBufferStatus('播放中', 'good');
      if (Date.now() > state.suppressUntil) commitLocalPlay('play');
      renderPlaybackControls();
    });
    bindPlayerEvent('pause', () => {
      clearTimeout(state.pendingPauseTimer);
      renderPlaybackControls();
      if (Date.now() <= state.suppressUntil) return;
      if (hasExplicitPauseIntent()) {
        commitLocalPause('pause');
        return;
      }
      if (state.latestRemoteState?.isPlaying && state.currentMediaUrl) {
        scheduleCatchup('本地卡顿');
        return;
      }
      if (hasLocalPlayIntent() && state.currentMediaUrl) return;
      if (!hasRecentMediaIntent()) return;
      state.pendingPauseTimer = setTimeout(() => {
        if (Date.now() <= state.suppressUntil) return;
        if (state.latestRemoteState?.isPlaying && state.currentMediaUrl) {
          scheduleCatchup('本地卡顿');
          return;
        }
        if (hasLocalPlayIntent() && state.currentMediaUrl) return;
        if (!getMediaPaused() || getMediaEnded() || hasSeekIntent()) return;
        sendState('pause');
      }, 260);
    });
    for (const eventName of ['waiting', 'stalled', 'suspend']) {
      bindPlayerEvent(eventName, () => {
        if (hasLocalSeekIntent()) {
          setBufferStatus('定位缓冲中', 'warn');
          return;
        }
        if (state.latestRemoteState?.isPlaying || hasLocalPlayIntent()) scheduleCatchup('本地缓冲');
      });
    }
    bindPlayerEvent('error', () => {
      const code = player.error?.code || state.artPlayer?.video?.error?.code;
      const labels = {
        1: '加载被中止',
        2: '网络加载失败',
        3: '解码失败',
        4: '浏览器不支持此音视频格式'
      };
      state.sourceLoading = false;
      state.pendingAutoplay = false;
      setBufferStatus(labels[code] || '视频加载失败', 'bad');
    });
    for (const eventName of ['loadedmetadata', 'loadeddata', 'canplay', 'canplaythrough', 'playing', 'progress']) {
      bindPlayerEvent(eventName, () => {
        ensureVideoVisible();
        finishSourcePreload();
        if (state.bufferingLocally) runCatchup();
        else if (state.currentMediaUrl && player.readyState >= 3) setBufferStatus('缓存可播放', 'good');
      });
    }
    bindPlayerEvent('seeked', () => {
      if (hasProgrammaticSeekIntent()) return;
      markLocalSeekIntent(LOCAL_SEEK_COMMIT_LOCK_MS);
      clearTimeout(state.seekCommitTimer);
      state.seekCommitTimer = setTimeout(() => {
        commitLocalSeek('seek', { force: true });
      }, 140);
    });
    bindPlayerEvent('ended', () => {
      if (state.currentMediaType === 'video') sendQueueMessage('queue_next', { mediaType: 'video', ended: true });
    });
  }
  const audioPlayer = getAudioMedia();
  if (audioPlayer) {
    const audioEventTicks = new Map();
    const bindAudioEvent = (eventName, handler) => {
      audioPlayer.addEventListener(eventName, (...args) => {
        const now = Date.now();
        if (now - (audioEventTicks.get(eventName) || 0) < 40) return;
        audioEventTicks.set(eventName, now);
        handler(...args);
      });
    };
    const markAudioIntent = () => {
      if (state.currentMediaType !== 'audio') return;
      markMediaIntent(2500);
      if (getMediaPaused()) markLocalPlay(2500);
      else markExplicitPause(2500);
    };
    for (const eventName of ['pointerdown', 'touchstart', 'keydown']) {
      audioPlayer.addEventListener(eventName, markAudioIntent);
      $('#musicPanel')?.addEventListener(eventName, markAudioIntent);
    }
    bindAudioEvent('seeking', () => {
      if (state.currentMediaType !== 'audio' || hasProgrammaticSeekIntent()) return;
      rememberLocalSeekTarget(getMediaTime());
      markLocalSeekIntent(LOCAL_SEEK_LOCK_MS);
      state.bufferingLocally = false;
      setPlaybackRate(1);
      clearCatchupStatus(0);
      setBufferStatus('正在定位', 'warn');
    });
    bindAudioEvent('play', () => {
      if (state.currentMediaType !== 'audio') return;
      clearTimeout(state.pendingPauseTimer);
      markLocalPlay();
      setBufferStatus('播放中', 'good');
      if (Date.now() > state.suppressUntil) commitLocalPlay('play');
      updateMusicProgressUi();
      renderMediaQueue();
      renderPlaybackControls();
    });
    bindAudioEvent('pause', () => {
      if (state.currentMediaType !== 'audio') return;
      clearTimeout(state.pendingPauseTimer);
      updateMusicProgressUi();
      renderMediaQueue();
      renderPlaybackControls();
      if (Date.now() <= state.suppressUntil) return;
      if (hasExplicitPauseIntent()) {
        commitLocalPause('pause');
        return;
      }
      if (state.latestRemoteState?.isPlaying && state.currentMediaUrl) {
        scheduleCatchup('本地卡顿');
        return;
      }
      if (hasLocalPlayIntent() && state.currentMediaUrl) return;
      if (!hasRecentMediaIntent()) return;
      state.pendingPauseTimer = setTimeout(() => {
        if (Date.now() <= state.suppressUntil) return;
        if (state.latestRemoteState?.isPlaying && state.currentMediaUrl) {
          scheduleCatchup('本地卡顿');
          return;
        }
        if (hasLocalPlayIntent() && state.currentMediaUrl) return;
        if (!getMediaPaused() || getMediaEnded() || hasSeekIntent()) return;
        sendState('pause');
      }, 260);
    });
    for (const eventName of ['waiting', 'stalled', 'suspend']) {
      bindAudioEvent(eventName, () => {
        if (state.currentMediaType !== 'audio') return;
        if (hasLocalSeekIntent()) {
          setBufferStatus('定位缓冲中', 'warn');
          return;
        }
        if (state.latestRemoteState?.isPlaying || hasLocalPlayIntent()) scheduleCatchup('本地缓冲');
      });
    }
    bindAudioEvent('error', () => {
      if (state.currentMediaType !== 'audio') return;
      state.sourceLoading = false;
      state.pendingAutoplay = false;
      setBufferStatus('浏览器不支持此音频格式或加载失败', 'bad');
    });
    for (const eventName of ['loadedmetadata', 'loadeddata', 'canplay', 'canplaythrough', 'playing', 'progress']) {
      bindAudioEvent(eventName, () => {
        if (state.currentMediaType !== 'audio') return;
        finishSourcePreload();
        updateMusicProgressUi();
        if (eventName === 'playing') renderMediaQueue();
        if (state.bufferingLocally) runCatchup();
        else if (state.currentMediaUrl && audioPlayer.readyState >= 3) setBufferStatus('缓存可播放', 'good');
      });
    }
    bindAudioEvent('timeupdate', updateMusicProgressUi);
    bindAudioEvent('seeked', () => {
      if (state.currentMediaType !== 'audio' || hasProgrammaticSeekIntent()) return;
      markLocalSeekIntent(LOCAL_SEEK_COMMIT_LOCK_MS);
      clearTimeout(state.seekCommitTimer);
      state.seekCommitTimer = setTimeout(() => commitLocalSeek('seek', { force: true }), 140);
      updateMusicProgressUi();
    });
    bindAudioEvent('ended', () => {
      if (state.currentMediaType === 'audio') sendQueueMessage('queue_next', { mediaType: 'audio', ended: true });
    });
  }
  $('#switchVideoBtn').addEventListener('click', (event) => {
    triggerButtonFeedback(event.currentTarget, 'play');
    markMediaIntent();
    const url = normalizeMediaInput($('#videoUrlInput').value);
    if (url === null) return toast(`请输入 http(s) 或 /videos 开头的${modeLabel()}地址`);
    if (!url) return toast(`请输入${modeLabel()}地址`);
    addMediaTrack(url, { mediaType: state.roomMode, playNow: true }).then((ok) => {
      if (ok) $('#videoUrlInput').value = '';
    }).catch((error) => toast(error.message));
  });
  $('#addMediaQueueBtn')?.addEventListener('click', async (event) => {
    const ok = await addMediaTrack($('#videoUrlInput').value, { mediaType: state.roomMode, playNow: false });
    if (ok) {
      triggerButtonFeedback(event.currentTarget, 'add');
      $('#videoUrlInput').value = '';
    }
  });
  $('#roomModeVideoBtn')?.addEventListener('click', () => requestRoomMode('video'));
  $('#roomModeAudioBtn')?.addEventListener('click', () => requestRoomMode('audio'));
  $('#playBtn')?.addEventListener('click', (event) => toggleCurrentPlayback(event.currentTarget));
  $('#musicPlayBtn')?.addEventListener('click', (event) => toggleCurrentPlayback(event.currentTarget));
  $('#musicPrevBtn')?.addEventListener('click', (event) => {
    triggerButtonFeedback(event.currentTarget, 'prev');
    sendQueueMessage('queue_previous', { mediaType: 'audio' });
  });
  $('#musicNextBtn')?.addEventListener('click', (event) => {
    triggerButtonFeedback(event.currentTarget, 'next');
    sendQueueMessage('queue_next', { mediaType: 'audio' });
  });
  $('#mediaPrevQueueBtn')?.addEventListener('click', (event) => {
    triggerButtonFeedback(event.currentTarget, 'prev');
    sendQueueMessage('queue_previous', { mediaType: state.roomMode });
  });
  $('#mediaNextQueueBtn')?.addEventListener('click', (event) => {
    triggerButtonFeedback(event.currentTarget, 'next');
    sendQueueMessage('queue_next', { mediaType: state.roomMode });
  });
  $('#mediaClearQueueBtn')?.addEventListener('click', (event) => {
    triggerButtonFeedback(event.currentTarget, 'clear');
    sendQueueMessage('queue_clear', { mediaType: state.roomMode });
  });
  $('#mediaPlaybackMode')?.addEventListener('change', (event) => {
    const mediaType = state.roomMode;
    const playbackMode = normalizePlaybackMode(event.target.value);
    state.playbackModes = normalizePlaybackModes(state.playbackModes);
    state.playbackModes[mediaType] = playbackMode;
    sendQueueMessage('queue_mode', { mediaType, playbackMode });
  });
  $('#musicProgress')?.addEventListener('input', () => {
    const media = getAudioMedia();
    const duration = Number(media?.duration || state.currentMediaMeta?.duration || currentTrackFromQueue()?.duration || 0);
    if (duration <= 0) return;
    const target = (Number($('#musicProgress').value || 0) / 1000) * duration;
    rememberLocalSeekTarget(target);
    markLocalSeekIntent(LOCAL_SEEK_LOCK_MS);
    try {
      media.currentTime = Math.max(0, target);
    } catch {}
    updateMusicProgressUi();
  });
  $('#musicProgress')?.addEventListener('change', () => commitLocalSeek('seek', { force: true }));
  window.addEventListener('resize', () => updateMusicProgressUi());
  $('#chatForm').addEventListener('submit', (event) => {
    event.preventDefault();
    const text = $('#chatInput').value.trim();
    if (!text) return;
    $('#chatInput').value = '';
    sendChat(text);
  });
  for (const button of document.querySelectorAll('[data-emoji]')) {
    button.addEventListener('click', () => {
      $('#chatInput').value += button.dataset.emoji;
      $('#chatInput').focus();
    });
  }
  $('#storageNodeSelect').addEventListener('change', () => {
    state.storagePath = '';
    loadStorageBrowser().catch((error) => toast(error.message));
  });
  $('#refreshStorageBrowserBtn').addEventListener('click', () => loadStorageBrowser().catch((error) => toast(error.message)));
  $('#storageBackBtn').addEventListener('click', () => {
    state.storagePath = parentPath(state.storagePath);
    loadStorageBrowser().catch((error) => toast(error.message));
  });

  $('#syncNodeForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const id = $('#syncNodeId').value;
    const saved = await withBusy(event.submitter, () => saveSyncNode(id, {
      name: $('#syncNodeName').value,
      url: $('#syncNodeUrl').value,
      port: Number($('#syncNodePort').value || 52000),
      enabled: $('#syncNodeEnabled').checked,
      isDefault: $('#syncNodeDefault').checked
    }), '保存中');
    if (!saved) return;
    event.target.reset();
    $('#syncNodeId').value = '';
    $('#syncNodePort').value = '52000';
    $('#syncNodeEnabled').checked = true;
  });
  $('#refreshSyncNodesBtn').addEventListener('click', () => loadAdmin().catch((error) => toast(error.message)));
  $('#authMethodForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const id = $('#authMethodId').value;
    const body = {
      name: $('#authMethodName').value,
      username: $('#authMethodUsername').value,
      mode: $('#authMethodMode').value
    };
    if ($('#authMethodMode').value === 'key') body.privateKey = $('#authMethodSecret').value;
    else body.password = $('#authMethodSecret').value;
    const saved = await withBusy(event.submitter, async () => {
      if (id) return api(`/api/admin/auth-methods/${id}`, { method: 'PATCH', body });
      return api('/api/admin/auth-methods', { method: 'POST', body });
    }, '保存中');
    if (!saved) return;
    toast('验证方式已保存');
    event.target.reset();
    $('#authMethodUsername').value = 'root';
    $('#authMethodId').value = '';
    await loadAdmin();
  });
  $('#dnsProviderType').addEventListener('change', updateDnsProviderFields);
  $('#dnsProviderForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const id = $('#dnsProviderId').value;
    const saved = await withBusy(event.submitter, async () => {
      if (id) return api(`/api/admin/dns-providers/${id}`, { method: 'PATCH', body: dnsProviderPayload() });
      return api('/api/admin/dns-providers', { method: 'POST', body: dnsProviderPayload() });
    }, '保存中');
    if (!saved) return;
    toast('DNS API 已保存');
    event.target.reset();
    $('#dnsProviderId').value = '';
    $('#dnsProviderEnabled').checked = true;
    $('#dnsDnspodEndpoint').value = 'https://api.dnspod.com';
    $('#dnsHuaweiIamEndpoint').value = 'https://iam.myhuaweicloud.com';
    updateDnsProviderFields();
    await loadAdmin();
  });
  $('#installNodeForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    updateInstallPublicUrl();
    const started = await withBusy(event.submitter, () => api('/api/admin/install-node', {
      method: 'POST',
      body: {
        nodeName: $('#installNodeName').value,
        host: $('#installHost').value,
        sshPort: Number($('#installSshPort').value || 22),
        authMethodId: $('#installAuthMethod').value,
        bindDomain: cleanInstallDomain($('#installBindDomain').value),
        useSsl: $('#installUseSsl').checked,
        sslMode: $('#installSslMode').value,
        publicUrl: $('#installPublicUrl').value,
        dnsProviderId: $('#installDnsProvider').value,
        fileAutoRenew: $('#installFileAutoRenew').checked,
        dnsAutoRenew: $('#installDnsAutoRenew').checked,
        sslDnsProvider: $('#installSslDnsProvider').value,
        sslDnsEnv: $('#installSslDnsEnv').value,
        sslCert: $('#installSslCert').value,
        sslKey: $('#installSslKey').value,
        servicePort: Number($('#installServicePort').value || 52000),
        installPath: $('#installPath').value,
        videoRoot: $('#installVideoRoot').value,
        makeDefaultSync: $('#installMakeDefaultSync').checked,
        useSync: $('#installUseSync').checked,
        useStorage: $('#installUseStorage').checked
      }
    }), '提交中');
    if (!started) return;
    toast('安装任务已开始');
    await loadAdmin();
  });
  for (const selector of ['#installHost', '#installBindDomain', '#installServicePort', '#installUseSsl']) {
    const el = $(selector);
    el.addEventListener(el.type === 'checkbox' ? 'change' : 'input', () => {
      updateInstallPublicUrl();
      updateInstallSslFields();
    });
  }
  $('#installSslMode').addEventListener('change', updateInstallSslFields);
  $('#nodeConfigSelect').addEventListener('change', () => {
    fillNodeConfigForm(state.admin.updateTargets.find((target) => target.publicUrl === $('#nodeConfigSelect').value));
  });
  for (const selector of ['#nodeConfigHost', '#nodeConfigBindDomain', '#nodeConfigServicePort', '#nodeConfigUseSsl']) {
    const el = $(selector);
    el.addEventListener(el.type === 'checkbox' ? 'change' : 'input', () => {
      updateNodeConfigPublicUrl();
      updateNodeConfigSslFields();
    });
  }
  $('#nodeConfigSslMode').addEventListener('change', updateNodeConfigSslFields);
  $('#refreshNodeConfigBtn').addEventListener('click', () => loadAdmin().catch((error) => toast(error.message)));
  $('#nodeConfigForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    updateNodeConfigPublicUrl();
    const saved = await withBusy(event.submitter, () => api('/api/admin/node-config', {
      method: 'PATCH',
      body: nodeConfigPayload()
    }), '同步中');
    if (!saved) return;
    toast('节点配置已保存，正在同步远端');
    await loadAdmin();
    await loadConfig();
  });
  $('#refreshUpdateTargetsBtn').addEventListener('click', () => loadAdmin().catch((error) => toast(error.message)));
  $('#updateNodeForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const publicUrl = $('#updateNodeSelect').value;
    if (!publicUrl) return toast('请选择可更新的节点');
    const started = await withBusy(event.submitter, () => api('/api/admin/update-node', {
      method: 'POST',
      body: { publicUrl }
    }), '更新中');
    if (!started) return;
    toast('节点更新任务已开始');
    await loadAdmin();
  });
  $('#storageNodeForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const id = $('#storageAdminNodeId').value;
    const body = {
      name: $('#storageAdminName').value,
      type: $('#storageAdminType').value,
      url: $('#storageAdminUrl').value,
      path: $('#storageAdminPath').value,
      enabled: $('#storageAdminEnabled').checked
    };
    const saved = await withBusy(event.submitter, async () => {
      if (id) return api(`/api/admin/storage-nodes/${id}`, { method: 'PATCH', body });
      return api('/api/admin/storage-nodes', { method: 'POST', body });
    }, '保存中');
    if (!saved) return;
    toast('存储节点已保存');
    event.target.reset();
    $('#storageAdminPath').value = '/video52000/videos';
    $('#storageAdminEnabled').checked = true;
    $('#storageAdminNodeId').value = '';
    await loadAdmin();
    await loadConfig();
  });
  $('#refreshStorageAdminBtn').addEventListener('click', () => loadAdmin().catch((error) => toast(error.message)));
  $('#adminStorageSelect').addEventListener('change', () => {
    state.adminStoragePath = '';
    $('#downloadStorageSelect').value = $('#adminStorageSelect').value;
    $('#downloadPath').value = '';
    loadAdminStorageList().catch((error) => toast(error.message));
    loadDownloadTasks().catch((error) => toast(error.message));
  });
  $('#refreshAdminStorageBtn').addEventListener('click', () => {
    loadAdminStorageList().catch((error) => toast(error.message));
    loadDownloadTasks().catch((error) => toast(error.message));
  });
  $('#adminStorageBackBtn').addEventListener('click', () => {
    state.adminStoragePath = parentPath(state.adminStoragePath);
    loadAdminStorageList().catch((error) => toast(error.message));
  });
  $('#mkdirForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const made = await withBusy(event.submitter, () => api(`/api/admin/storage/nodes/${$('#adminStorageSelect').value}/mkdir`, {
      method: 'POST',
      body: { path: state.adminStoragePath, name: $('#mkdirName').value }
    }), '新建中');
    if (!made) return;
    $('#mkdirName').value = '';
    await loadAdminStorageList();
  });
  $('#uploadForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const file = $('#uploadFile').files[0];
    if (!file) return;
    const form = new FormData();
    form.append('path', state.adminStoragePath);
    form.append('file', file);
    const uploaded = await withBusy(event.submitter, () => api(`/api/admin/storage/nodes/${$('#adminStorageSelect').value}/upload`, { method: 'POST', body: form }), '上传中');
    if (!uploaded) return;
    $('#uploadFile').value = '';
    toast('上传完成');
    await loadAdminStorageList();
  });
  $('#downloadStorageSelect').addEventListener('change', () => {
    $('#adminStorageSelect').value = $('#downloadStorageSelect').value;
    state.adminStoragePath = '';
    loadAdminStorageList().catch((error) => toast(error.message));
    loadDownloadTasks().catch((error) => toast(error.message));
  });
  $('#refreshDownloadTasksBtn').addEventListener('click', () => loadDownloadTasks().catch((error) => toast(error.message)));
  $('#downloadForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const nodeId = $('#downloadStorageSelect').value;
    if (!nodeId) return toast('请选择存储节点');
    const pathValue = $('#downloadPath').value.trim().replace(/^\/+/, '') || state.adminStoragePath;
    const started = await withBusy(event.submitter, () => api(`/api/admin/storage/nodes/${nodeId}/downloads`, {
      method: 'POST',
      body: {
        url: $('#downloadUrl').value,
        path: pathValue,
        filename: $('#downloadFilename').value
      }
    }), '提交中');
    if (!started) return;
    $('#downloadUrl').value = '';
    $('#downloadFilename').value = '';
    toast('下载任务已创建');
    await loadDownloadTasks();
  });
}

async function init() {
  bindEvents();
  setAuthMode('login');
  updateDnsProviderFields();
  updateInstallSslFields();
  updateNodeConfigSslFields();
  switchAdminPage('sync');
  try {
    const data = await api('/api/me');
    if (!data.user) return showLoggedOut();
    showAuthenticated(data.user);
    await loadConfig();
    await loadRooms();
    switchView('lobby');
  } catch {
    showLoggedOut();
  }
}

setInterval(() => {
  if (state.user?.role === 'admin' && !$('#adminView').classList.contains('hidden') && !state.installPollBusy) {
    state.installPollBusy = true;
    Promise.all([
      api('/api/admin/install-jobs'),
      api('/api/admin/update-jobs')
    ]).then(async ([data, updateData]) => {
      const previous = new Map(state.admin.jobs.map((job) => [job.id, job.status]));
      const previousUpdates = new Map(state.admin.updateJobs.map((job) => [job.id, job.status]));
      state.admin.jobs = data.jobs || [];
      state.admin.updateJobs = updateData.jobs || [];
      renderInstallJobs();
      renderUpdateJobs();
      const finished = state.admin.jobs.find((job) => {
        const oldStatus = previous.get(job.id);
        return oldStatus && oldStatus !== job.status && ['success', 'failed'].includes(job.status);
      });
      const finishedUpdate = state.admin.updateJobs.find((job) => {
        const oldStatus = previousUpdates.get(job.id);
        return oldStatus && oldStatus !== job.status && ['success', 'failed'].includes(job.status);
      });
      if (finished || finishedUpdate) {
        const done = finished || finishedUpdate;
        toast(done.status === 'success' ? '节点任务完成，列表已自动刷新' : `节点任务失败：${done.error || '请查看任务日志'}`);
        await loadAdmin();
        await loadConfig();
        await loadRooms();
      }
    }).catch(() => {}).finally(() => {
      state.installPollBusy = false;
    });
  }
  if (state.user?.role === 'admin' && !$('#adminView').classList.contains('hidden') && state.adminPage === 'storage' && !state.downloadPollBusy) {
    state.downloadPollBusy = true;
    loadDownloadTasks().catch(() => {}).finally(() => {
      state.downloadPollBusy = false;
    });
  }
}, 5000);

init();
