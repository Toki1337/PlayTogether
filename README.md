# PlayTogether

![PlayTogether hero](docs/images/playtogether-hero.png)

PlayTogether 是一个自托管的多人同步观影和一起听音乐 Web 应用。主站负责用户、房间、后台、媒体库和节点管理；sync/storage node 负责房间状态同步、聊天弹幕、队列同步、目录浏览、上传和离线下载。

这个仓库是脱敏后的可维护版本，不包含真实用户、房间、IP、域名、SSH 凭据、DNS 密钥、SSL 证书、日志或生产数据库。

## 功能

- 房间内同步播放视频和音乐，支持播放、暂停、拖动进度和新成员追赶。
- 视频队列和音乐队列分离，支持上一项、下一项、移除、清空、拖动排序、顺序播放、随机播放和单曲循环。
- 音乐面板支持封面、作者、时长和歌词解析。
- 视频播放器基于 Artplayer，支持网页全屏、原生全屏、弹幕层和移动端锁。
- 房间聊天消息可同步显示为弹幕。
- 后台可管理用户、房间、同步节点、存储节点、SSH 凭据、DNS 凭据、节点安装和节点更新。
- 存储节点支持目录浏览、上传、删除和离线下载。

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

首次启动会自动生成 `data/secrets.json` 和 `data/db.json`。默认管理员由 `.env` 控制：

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
