# RustDesk Server 定制版（hbbs / hbbr）

基于 [rustdesk-server](https://github.com/rustdesk/rustdesk-server)（经 [lejianwen/rustdesk-server](https://github.com/lejianwen/rustdesk-server) 分支）的定制版本，面向**多站点、多中继的私有化部署**。

配套组件：客户端 [rustdesk_private](https://github.com/laiyouxing/rustdesk_private) · API 服务 [rustdesk-api](https://github.com/laiyouxing/rustdesk-api) · 管理前端 [rustdesk-api-web](https://github.com/laiyouxing/rustdesk-api-web) · 总仓库 [rustdesk-custom](https://github.com/laiyouxing/rustdesk-custom)

## 相对上游的改动

### 集群 agent（核心特性）

hbbs / hbbr 内置节点 agent，把「节点注册 → 负载上报 → 加权调度」打通，**未配置 `RD_API_SERVER` 时整块功能关闭，行为与原生一致**。

- 启动注册（失败自动重试）+ 每 15 秒心跳（peers / sessions / uptime / version）
- hbbs 每 60 秒拉取调度结果并**热更新中继列表**（不重启、不断连），返回空列表时保留原配置
- 节点接入鉴权：`X-Node-Secret`，后台可热轮换（节点 60 秒内自动切换，旧密钥 7 天宽限）
- 平台配置热更新（中继列表 / 限速 / 降级阈值），30 秒内生效

### 指标采集（CPU / 内存 / 带宽）

| 平台 | CPU | 内存 | 带宽 |
|---|---|---|---|
| Linux | `/proc/stat` | `/proc/meminfo` | hbbr：中继自身吞吐；hbbs：`/proc/net/dev` |
| Windows | `GetSystemTimes` | `GlobalMemoryStatusEx` | 中继自身吞吐 |

带宽优先使用**中继自身转发的字节数**（比主机网卡总量更准确），累计字节差 ÷ 采样间隔 × 8 得到 bit/s。首次采样只建立基线。

### 安全与稳定性修复

- **CVE-2026-30784**：UDP 反射放大（校验注册前置条件）
- **i32 溢出**导致的"假在线"（peers 计数溢出后设备显示异常）
- 默认带宽上调，避免大流量会话被误限速

### Windows 构建

CI 产出 `hbbs.exe` / `hbbr.exe`，可用 NSSM 注册为服务（见下文）。

## 端口

| 服务 | 端口 | 用途 |
|---|---|---|
| hbbs | 21115 | NAT 类型测试 |
| hbbs | 21116（TCP + UDP） | ID 注册与心跳 / UDP 打洞 |
| hbbs | 21118 | WebSocket（客户端 ≥ 1.4.1） |
| hbbr | 21117 | 中继（TCP） |
| hbbr | 21119 | WebSocket 中继 |

## 环境变量

| 变量 | 说明 |
|---|---|
| `RD_API_SERVER` | api-server 地址。**同机部署必须用内网/回环地址**（公网地址在本机无 NAT 回环，会 Connection refused）|
| `RD_NODE_KEY` | RustDesk key，与 `-k` 相同 |
| `RD_NODE_SECRET` | 节点接入密钥，服务端强制校验后必填 |
| `RD_NODE_ADDR` | 节点对外地址（公网 `IP:端口`）。留空则自动探测，建议显式填写 |
| `RD_NODE_LAN_ADDR` | 内网地址，**必须带端口**；填错会导致探测失败、`rtt_ms = -1` |
| `RD_NODE_REGION` | 节点地域（省级 code，如 `GX` / `GD`） |
| `RD_RELAY_PULL` | hbbs 是否拉取调度列表（默认 true），hbbr 默认 false |

> 中继容器里**不要**配 `RD_NODE_LAN_ADDR`（api-server 在别的机器上探测不到容器内网 IP）。

## 部署

### Linux systemd

```bash
# hbbs
hbbs -r "<中继地址列表>" --mask "<内网网段>" --must-login Y

# hbbr（-k 必须与 hbbs 一致）
hbbr -k "<KEY>"
```

接入集群时在 service 里加 `Environment="RD_API_SERVER=..."` 等变量，重启后日志应出现：

```
[node-agent] 节点已注册: hbbr@<地址>:21117
```

### Docker

```bash
docker pull ghcr.io/laiyouxing/rustdesk-server-hbbs:latest-amd64
docker pull ghcr.io/laiyouxing/rustdesk-server-hbbr:latest-amd64
```

镜像基于 `scratch`（二进制是 musl 静态链接），体积 6~10MB，不依赖 Docker Hub。中继必须 `--network host`。

### Windows（NSSM）

```cmd
nssm install RustDeskHbbr "C:\rustdesk-server\hbbr.exe" "-k <KEY>"
nssm set RustDeskHbbr AppDirectory "C:\rustdesk-server"
nssm set RustDeskHbbr AppEnvironmentExtra ^
 "RD_API_SERVER=https://<api-server>" "RD_NODE_KEY=<KEY>" ^
 "RD_NODE_SECRET=<节点密钥>" "RD_NODE_ADDR=<公网IP>:21117" ^
 "RD_NODE_REGION=GD"
nssm start RustDeskHbbr
```

放行 `21117` / `21119`（TCP）。`AppDirectory` 必须可写 —— agent 会在那里缓存 `.node_secret`。

## 构建产物

| 产物 | 位置 |
|---|---|
| Linux 静态二进制 | Release 资产的 `hbbs` / `hbbr` / `rustdesk-utils` |
| Windows 二进制 | Release 资产的 `hbbs.exe` / `hbbr.exe` |
| Docker 镜像 | `ghcr.io/laiyouxing/rustdesk-server-hbbs:{tag}-amd64`、`-hbbr:{tag}-amd64` |
| 其它架构镜像 | amd64 / arm64v8 / armv7 / i386 |

推 `X.Y.Z` 格式的 tag 即触发构建（`docker-publish.yaml`），同时产出镜像与 Release 资产。

## 上游既有特性（保留）

- `--must-login Y`：强制登录才能建立连接（**只拦打洞请求，不影响注册**）
- `RUSTDESK_API_JWT_KEY`：设置后通过 JWT 校验 token 合法性
- 支持 client websocket（客户端 ≥ 1.4.1）
- IPv6 / 中继前打洞（Phase3 已在客户端侧移除，服务端保持兼容）

## 文档

运维细节、密钥轮换顺序、iKuai 路由器容器管理、各类排障记录统一放在总仓库的 [AGENTS.md](https://github.com/laiyouxing/rustdesk-custom/blob/main/AGENTS.md)。
