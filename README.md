# PlayTogether

![PlayTogether hero](docs/images/playtogether-hero.png)

PlayTogether 是一个自托管的多人同步观影和一起听音乐 Web 应用。主站负责用户、房间、后台、媒体库和节点管理；sync/storage node 负责房间状态同步、聊天弹幕、队列同步、目录浏览、上传和离线下载。

这个仓库是脱敏后的可维护版本，不包含真实用户、房间、IP、域名、SSH 凭据、DNS 密钥、SSL 证书、日志或生产数据库。

## 功能

- 房间内同步播放视频和音乐，支持播放、暂停、拖动进度和新成员追赶。
- 同步状态栏：一眼看到连接、延迟、与房间的进度差，以及是谁刚做了什么；小漂移用播放速率微调追平，不会跳帧，超过阈值才定位；「同步」按钮可随时手动对齐，断线时变为「重连」。同步算法面向真实网络设计（延迟不同、抖动、中途卡顿），见下文「同步算法」。
- 视频队列和音乐队列分离，支持上一项、下一项、移除、清空、拖动排序、顺序播放、随机播放和单曲循环；多名成员同时播完一首不会跳曲。
- 音乐面板支持封面、作者、时长和歌词解析。
- 视频播放器基于 Artplayer，支持网页全屏、原生全屏、弹幕层和移动端锁。
- 房间聊天消息可同步显示为弹幕。
- 房间地址可分享和刷新（`#room/<id>`），断线后自动重连。
- 后台可管理用户、房间、同步节点、存储节点、SSH 凭据、DNS 凭据、节点安装和节点更新。
- 存储节点支持目录浏览、上传（流式落盘，保留 UTF-8 文件名）、删除和离线下载。
- 节点会把房间队列和聊天记录落盘，重启或更新后不丢失。

## 目录

```text
.
├── public/              # 前端静态页面、样式和浏览器逻辑
├── server.js            # 主站 Express 服务
├── sync-node/server.js  # 同步/存储节点服务
├── scripts/             # 节点安装脚本
├── data/*.example.json  # 脱敏示例数据
└── docs/images/         # README 图片资产
```

## 快速启动

需要 Node.js 22 或更新版本。

```bash
git clone https://github.com/Toki1337/PlayTogether.git
cd PlayTogether
cp .env.example .env
npm install
set -a
. ./.env
set +a
npm start
```

主站默认监听：

```text
http://127.0.0.1:51999
```

首次启动会自动生成 `data/secrets.json` 和 `data/db.json`；登录会话保存在 `data/sessions.json`，重启不会掉线。上传文件先写入媒体目录下的 `.uploads-tmp`，完成后再移动到目标位置。默认管理员由 `.env` 控制：

```text
DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=change-me-now
```

项目不会自动读取 `.env` 文件；上面的 `set -a; . ./.env; set +a` 用于把示例配置导入当前 shell。使用 systemd、PM2、Docker 或面板部署时，请把这些变量配置到对应的运行环境里。

生产环境请在第一次启动前修改默认密码，或启动后立刻修改管理员账户。

## 启动本机 sync/storage 节点

主站和本机节点可以分开启动。先启动主站，然后在另一个终端运行：

```bash
NODE_TOKEN="$(node -e "console.log(require('./data/secrets.json').nodeSecret)")" \
NODE_ROLES=sync,storage \
VIDEO_STORAGE_ROOT=/video52000/videos \
npm run start:sync
```

默认节点端口是 `52000`。后台添加同步节点时可填写：

```text
http://127.0.0.1:52000
```

## 远程节点

后台支持通过 SSH 安装或更新远程 sync/storage 节点。远程节点需要：

- 可 SSH 登录的服务器。
- 可访问的节点服务端口，默认 `52000`。
- 如果直接启用 HTTPS/WSS，需要有效域名和证书。
- 如果使用文件验证申请证书，需要公网 `80` 端口可达。
- 如果使用 DNS 验证，需要在后台配置 DNS API。

不要把真实 SSH 密码、私钥、DNS Token、证书 PEM 或生产节点地址提交到仓库。它们只应保存在运行环境的 `data/db.json`、`data/secrets.json` 或服务器环境变量里。

## Emby 来源

除了存储节点里的文件，任何用户都可以在房间的「媒体库」面板里选择「连接 Emby 服务器…」，用自己的 Emby / Jellyfin 账号登录后直接浏览影库（电影、剧集 → 季 → 集、搜索），把影片加入房间队列。

- **连接者授权全房间**：其他成员不需要 Emby 账号。Emby 的访问令牌用 AES-256-GCM 加密保存在主站 `data/db.json` 里，密码不落盘；浏览影库只有连接者本人可以。
- **播放由存储节点转发**：加入队列时主站把「服务器地址 + 令牌 + 影片」加密成一枚播放凭证放进播放地址，远程存储节点解开凭证后向 Emby 拉流并转发给浏览器。浏览器和房间成员只见到节点地址，看不到 Emby 地址和令牌。主站不代理播放流量。
- **Emby 侧只计一路播放**：所有成员共用连接者的一个设备会话；HLS 转码时节点对同一影片只申请一次转码会话并合并分片请求，Emby 后台只会看到一个设备、一路转码。
- **直连或转码**：mp4 / webm 直连（浏览器可解码时），mkv、HEVC 等交给 Emby 转成 HLS。HEVC 默认不直连，可在 `server.js` 与 `sync-node/server.js` 顶部的 `EMBY_DEVICE_PROFILE` 调整。
- **节点需要更新**：只有包含 `emby-proxy` 特性的存储节点（`/health` 会列出）才能转发，旧节点请在后台「更新节点」或重新运行 `scripts/install-node.sh`。凭证密钥来自节点共享的 `NODE_TOKEN`，任何已安装的存储节点都能解开凭证。
- **内网地址**：普通用户只能连接公网可达的 Emby；管理员可以连接内网地址（例如家里的 NAS），但此时存储节点也必须能访问到该地址。
- 不需要新的环境变量。

## 同步算法

同步分三层。节点只描述一条「时间线」，不转述任何人的播放头；每个客户端自己把播放器贴到这条时间线上。

- **时钟**：客户端用 NTP 式四时间戳估计节点时间。连接后先突发 4 个 ping 锁定，之后 1.5–3 秒一次；只采信往返时间接近最小值的样本，偏移取中位数，抖动取往返时间的绝对中位差。本机墙钟跳变会清空样本；重连同一节点时保留估计，换节点才重来。
- **时间线**：房间状态 `{ position, updatedAt, isPlaying }` 里的 `updatedAt` 是锚点，`当前位置 = position + (节点时间 − 锚点)`，锚点允许在未来。播放、拖动和换曲都是**预约起播**：节点把锚点定在 `成员最大往返时间 / 2 + 250 ms`（换曲 `+ 600 ms`）之后，成员各自先定位到目标帧、到点同时起播，延迟不同的成员不再有人先跑。控制消息是语义化的：`play` / `pause` 是状态转换（房间已在播放时再点播放不会改动房间，落后的人无法把大家拉回去），只有 `seek` 携带位置，`pause` 按发送者的节点时间冻结时间线，上行延迟不会让房间倒退。
- **控制器**：客户端每 250 ms 本地采样漂移并做指数平滑，不依赖心跳，抖动不会混进漂移里。阈值随抖动自适应（视频微调 0.25–0.6 s、定位 2–3.5 s；音乐微调 0.3–0.6 s、定位 2 s），倍率上限视频 10%（大漂移 20%）、音乐 8%，前方缓冲不足时加速上限降到 3%。硬定位需要连续两个采样超阈值并有 2 秒冷却。

卡顿时**房间从不等待**：卡住的成员在成员列表上显示状态点，恢复后若目标位置已缓冲就直接跳回时间线，否则微调追平。30 秒内卡顿 3 次进入弱网模式：不再硬跳（每次跳转都会落在未缓冲处再次卡住），定位阈值放宽到 5 秒，状态栏显示「缓冲中 · 网络不稳」，只靠微调收敛。

其他细节：

- 心跳只带时间线字段（播放中 1 秒一次、暂停时 5 秒一次），队列、歌词等只在显式事件和快照里发送。
- 广播带发起者标识，发起者收到自己的回声只采纳锚点，不会重复执行。
- 程序性播放 / 定位不会提交到房间；只有带指针或键盘意图的操作才算用户操作。耳机线控、系统媒体键触发的播放没有页面意图，不会改动房间状态，这是已知边界。
- 自动跳下一曲只信任接近曲目末尾的 `ended`（曲目时长由播放端上报），落后成员迟到的 `ended` 不会让单曲循环重启两次。
- **节点需要一起更新**：新客户端配旧节点仍能播放，但没有预约起播和语义化控制；旧客户端配新节点也能工作（心跳字段缺失时保留本地队列）。

## 环境变量

主站：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `51999` | 主站监听端口 |
| `DEFAULT_SYNC_PORT` | `52000` | 默认同步节点端口 |
| `VIDEO_STORAGE_ROOT` | `/video52000/videos` | 本机媒体目录 |
| `MAX_UPLOAD_MB` | `2048` | 单文件上传大小限制 |
| `DEFAULT_ADMIN_USERNAME` | `admin` | 首次启动创建的管理员用户名 |
| `DEFAULT_ADMIN_PASSWORD` | `change-me-now` | 首次启动创建的管理员密码 |

sync/storage 节点：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `52000` | 节点监听端口 |
| `NODE_TOKEN` | 无 | 主站生成的节点访问令牌 |
| `NODE_ROLES` | `sync,storage` | `sync`、`storage` 或 `sync,storage` |
| `VIDEO_STORAGE_ROOT` | `/video52000/videos` | 节点媒体目录 |
| `SYNC_STATE_FILE` | `sync-node/rooms.json` | 房间状态落盘文件 |
| `TLS_CERT_PATH` | 无 | 可选，HTTPS 证书路径 |
| `TLS_KEY_PATH` | 无 | 可选，HTTPS 私钥路径 |

## 脱敏和发布规则

仓库 `.gitignore` 默认排除：

- `data/*.json`，但保留 `data/*.example.json`。
- `.env` 和本地环境变量文件。
- `node_modules/`、日志文件、上传目录和媒体目录。
- SSL/TLS 证书、私钥和 CSR 文件。
- `.well-known/`、`.user.ini`、`.htaccess` 等服务器本地文件。

发布前建议执行：

```bash
grep -RInE '([0-9]{1,3}\.){3}[0-9]{1,3}|BEGIN .*PRIVATE KEY|passwordHash|secretCipher' \
  --exclude-dir=node_modules --exclude-dir=.git .
```

只保留示例地址、示例密钥占位符和通用配置。

## 检查

```bash
node --check server.js
node --check sync-node/server.js
node --check public/app.js
npm run check
```

## 许可证

MIT
