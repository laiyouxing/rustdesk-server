//! 集群节点 agent：向 api-server 注册、上报心跳指标、拉取按调度分数排序的中继列表。
//!
//! 设计目标（配合 api-server 的 `/api/node/*` 接口）：
//!   - **零侵入**：不改变 hbbs/hbbr 的对外协议，只作为旁路线程运行；
//!   - **可关闭**：未配置 `RD_API_SERVER` 时完全不影响原有行为；
//!   - **公网 + 双密钥**：请求携带 `X-Rustdesk-Key`（与 api-server 一致）与
//!     `X-Node-Secret`（节点专用密钥，见 `RD_NODE_SECRET`），后者才是真正的鉴权——
//!     rustdesk key 会下发到每个客户端，等于公开信息，不能单独用来证明"我是节点"；
//!   - **热更新**：hbbs 拉到新的中继列表后通过内部通道替换，无需重启。
//!
//! 说明：hbbs 走 lib crate、hbbr 是独立 binary，两者都 include 本模块。
//! 因此这里**不直接引用** `rendezvous_server` / `relay_server`，而是通过
//! 「函数指针回调」解耦：
//!   - hbbs 启动时 `set_relay_apply(rendezvous_server::apply_relay_list)`
//!   - hbbr 启动时 `set_limits_apply(relay_server::apply_limits)`
//!   - 指标**不再用共享原子量镜像**（历史上 ACTIVE_CONNS 只增不减）：
//!     各进程用 `set_conns_getter` / `set_peers_getter` 注册自己的取值函数，
//!     心跳时实时调用，谁维护谁负责。
//!
//! 环境变量（下划线与短横线两种写法都认，见 `env_get`）：
//!   RD_API_SERVER    api-server 地址，例如 https://183.24.70.210:21120
//!   RD_NODE_KEY      rustdesk key（与 hbbs/hbbr 的 -k 相同）
//!   RD_NODE_SECRET   节点专用密钥，与 api-server 的 node.secret 一致；
//!                    为空则不带 X-Node-Secret 请求头
//!   RD_NODE_ADDR     本节点对外地址 host:port（必须是公网 IPv4）
//!   RD_NODE_LAN_ADDR 本节点内网地址（可选，供同内网客户端直连；**必须带端口**）
//!   RD_NODE_REGION   地域标识（可选，省级 code，例如 GX）
//!   RD_NODE_NAME     节点显示名（可选）
//!   RD_RELAY_PULL    是否拉取中继列表；hbbs 默认 true、hbbr 默认 false
//!
//! 节点密钥的来源与轮换（`X-Node-Secret`）：
//!   - 启动时优先级：`.node_secret` 缓存文件（工作目录 / 二进制所在目录）> `RD_NODE_SECRET`；
//!     两者都没有则不带该请求头（服务端未启用密钥时仍可工作）。
//!   - 运行中每 60 秒调一次 `GET /api/node/secret`（需带当前或宽限期内的上一把密钥）：
//!     服务端返回的密钥与本机不同 → 原子写缓存（先写 `.node_secret.tmp` 再 rename）
//!     → 更新内存中的当前密钥 → info 日志「节点密钥已更新（服务端下发）」。
//!   - 服务端未配置 node.secret 时该接口仍返回 200，但 `secret` 为空串：
//!     **忽略空串、保持本地密钥不变**（否则会把已落盘的密钥清空）。
//!   - 该接口返回 401 表示本机密钥已超出服务端宽限期（current 与 rotated_at+7 天内的
//!     prev 都不匹配）：warn 提示检查 api-server 配置，继续用旧密钥重试，不退出线程。
//!   - 服务端同一时刻只保留一把 prev，因此后台连续轮换的间隔应大于本机 60 秒轮询周期。
//!   - 密钥明文**不写日志**；缓存文件在 unix 上以 0600 权限落盘。

use hbb_common::log;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

/// 中继列表热更新回调（仅 hbbs 注册）
type RelayApply = fn(Vec<String>) -> bool;
/// 限速热更新回调（仅 hbbr 注册）。
///
/// 参数为 `Option<usize>`：`None` = 保持不变，`Some(0)` = 不限制（解除限速）。
type LimitsApply = fn(Option<usize>, Option<usize>, Option<usize>, Option<usize>, Option<usize>);
/// 指标取值回调（只能注册无捕获的 fn）。
type CountGetter = fn() -> usize;

static RELAY_APPLY: once_cell::sync::OnceCell<RelayApply> = once_cell::sync::OnceCell::new();
static LIMITS_APPLY: once_cell::sync::OnceCell<LimitsApply> = once_cell::sync::OnceCell::new();
/// hbbr 活跃连接数 getter（由 relay_server 注册）
static CONNS_GETTER: once_cell::sync::OnceCell<CountGetter> = once_cell::sync::OnceCell::new();
/// hbbs 在线 peer 数 getter（由 rendezvous_server 注册）
static PEERS_GETTER: once_cell::sync::OnceCell<CountGetter> = once_cell::sync::OnceCell::new();

/// 平台（api-server）下发的中继列表。
///
/// 优先级：平台下发 > 调度列表 > 本地 -r 启动参数。
/// Some(..) 表示平台已接管中继列表，此时调度拉取到的列表不再应用，
/// 避免两者每 30/60 秒互相覆盖（此前是"谁后跑谁生效"，结果不确定）。
/// 平台配置里去掉 relay_servers 后自动交回调度控制。
static PLATFORM_RELAYS: once_cell::sync::Lazy<std::sync::Mutex<Option<Vec<String>>>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(None));

static STARTED: std::sync::Once = std::sync::Once::new();

/// 心跳间隔
const HEARTBEAT_SECS: u64 = 15;
/// 拉取中继列表间隔
const RELAY_PULL_SECS: u64 = 60;
/// 拉取 api-server 下发配置的间隔
const CONFIG_PULL_SECS: u64 = 30;
/// 首轮拉取延迟：node_agent::start() 早于 RendezvousServer::start()（hbbs）
/// 与 relay io_loop（hbbr），立即拉取必然失败，等它们就绪再拉。
const FIRST_PULL_DELAY_SECS: u64 = 10;
/// 地址探测失败后的重试间隔
const ADDR_DETECT_RETRY_SECS: u64 = 300;
/// 节点密钥轮询间隔（复用现有定时循环，不新增高频请求）
const SECRET_PULL_SECS: u64 = 60;
/// 节点密钥缓存文件名：落在工作目录（systemd 的 WorkingDirectory）或二进制所在目录，
/// 与其它运行时旁路文件一致的落盘方式。
const NODE_SECRET_FILE: &str = ".node_secret";

/// 注册中继列表热更新回调（hbbs 启动时调用）。
///
/// hbbr 不注册该回调，因此在 hbbr 二进制里是 dead_code，显式允许。
#[allow(dead_code)]
pub fn set_relay_apply(f: RelayApply) {
    let _ = RELAY_APPLY.set(f);
}

/// 注册限速热更新回调（hbbr 启动时调用）。
pub fn set_limits_apply(f: LimitsApply) {
    let _ = LIMITS_APPLY.set(f);
}

/// 注册活跃连接数取值回调（hbbr 启动时传入 `relay_server::active_conns`）。
///
/// 替代原先「relay_server 往 node_agent::ACTIVE_CONNS 里镜像」的写法：
/// 镜像只在 accept 时 +1、连接关闭时漏减，导致 sessions 只增不减。
pub fn set_conns_getter(f: CountGetter) {
    let _ = CONNS_GETTER.set(f);
}

/// 注册在线 peer 数取值回调（hbbs 启动时传入 `rendezvous_server::online_peers`）。
///
/// 原实现的 `ONLINE_PEERS` 从未被写入，hbbs 心跳的 peers 恒为 0。
/// hbbr 不注册该回调，因此在 hbbr 二进制里是 dead_code，显式允许。
#[allow(dead_code)]
pub fn set_peers_getter(f: CountGetter) {
    let _ = PEERS_GETTER.set(f);
}

/// 读取一个 RD_* 环境变量，兼容下划线与短横线两种写法。
///
/// 为什么两种都要认：hbbs 的 `common::init_args()` 会用 `arg_name()` 把 .env 的键
/// 规范成「大写 + 短横线」（`RD_API_SERVER` → `RD-API-SERVER`），而 hbbr 直接加载
/// .env、systemd/docker 的 `Environment=` 又用下划线形式。历史 bug：hbbs 上这些
/// 变量因此全部读不到，agent 静默不启动。
fn env_get(key: &str) -> Option<String> {
    let dashes = key.replace('_', "-");
    let mut names = vec![
        key.to_string(),
        key.to_uppercase(),
        dashes.clone(),
        dashes.to_uppercase(),
    ];
    names.dedup();
    for name in names {
        if let Ok(v) = std::env::var(&name) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn env_or(k: &str, d: &str) -> String {
    env_get(k).unwrap_or_else(|| d.to_string())
}

fn env_flag(k: &str, d: bool) -> bool {
    match env_get(k) {
        Some(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "y" | "yes"),
        None => d,
    }
}

/// 节点密钥缓存文件的候选路径，优先级：当前工作目录 → 二进制所在目录。
///
/// systemd 部署的 WorkingDirectory 是 `/var/lib/rustdesk-server/`，二进制却在
/// 别的目录（如 `/home/.../amd64/`）：两处都认（读时按顺序找），写时写到读到
/// 的那一份；都没有文件时默认写工作目录，失败再退回二进制目录。
fn node_secret_paths() -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        paths.push(cwd.join(NODE_SECRET_FILE));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let p = dir.join(NODE_SECRET_FILE);
            if !paths.contains(&p) {
                paths.push(p);
            }
        }
    }
    paths
}

/// 判断一个字符串是否“像可用的密钥”：非空、不含空白、长度合理。
///
/// 用于挡掉：缓存文件被截断/被编辑器写成多行、服务端下发空串把密钥清空。
/// 不校验字符集——密钥格式由 api-server 决定，agent 不做多余假设。
fn is_plausible_secret(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(|c| c.is_whitespace())
}

/// 读取本地缓存的节点密钥，返回（缓存文件路径, 密钥）。
fn read_secret_file() -> Option<(std::path::PathBuf, String)> {
    for path in node_secret_paths() {
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                let secret = raw.trim().to_string();
                if is_plausible_secret(&secret) {
                    return Some((path, secret));
                }
                log::warn!(
                    "[node-agent] 节点密钥缓存 {:?} 内容非法（空/含空白/过长），已忽略",
                    path
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => log::warn!("[node-agent] 读取节点密钥缓存 {:?} 失败: {}", path, e),
        }
    }
    None
}

/// 启动时的节点密钥来源：`.node_secret` 缓存文件 > `RD_NODE_SECRET` 环境变量 > 空。
///
/// 缓存文件优先于环境变量：管理员在后台轮换密钥后，节点把新密钥落盘，
/// 之后重启进程也不会退回 systemd 里的旧环境变量值。
///
/// 返回值第二项是后续写缓存要用的路径：读到文件则为该文件，否则为候选里的
/// 第一项（工作目录）。
fn load_node_secret() -> (String, std::path::PathBuf) {
    let candidates = node_secret_paths();
    if let Some((path, secret)) = read_secret_file() {
        log::info!("[node-agent] 节点密钥来自缓存文件 {:?}", path);
        return (secret, path);
    }
    let path = candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| std::path::PathBuf::from(NODE_SECRET_FILE));
    let secret = env_or("RD_NODE_SECRET", "");
    if !secret.is_empty() {
        log::info!("[node-agent] 节点密钥来自环境变量 RD_NODE_SECRET");
    }
    (secret, path)
}

/// 原子写节点密钥缓存：先写 `.node_secret.tmp`，再 rename 覆盖正式文件。
///
/// POSIX 上 rename 覆盖是原子的（读方要么看到旧内容、要么看到新内容，不会读到
/// 半个密钥）；Windows 上 rename 不能覆盖已存在文件，先删再改名（开发机场景，
/// 非原子）。unix 上把临时文件权限收紧为 0600——密钥不该让同机其他用户可读。
fn save_secret_atomic(path: &std::path::Path, secret: &str) -> bool {
    use std::io::Write;
    let mut tmp_os = path.as_os_str().to_os_string();
    tmp_os.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp_os);
    let mut file = match std::fs::File::create(&tmp) {
        Ok(f) => f,
        Err(e) => {
            log::warn!("[node-agent] 创建节点密钥临时文件 {:?} 失败: {}", tmp, e);
            return false;
        }
    };
    let write_res = file
        .write_all(format!("{}\n", secret).as_bytes())
        .and_then(|_| file.sync_all());
    if let Err(e) = write_res {
        log::warn!("[node-agent] 写节点密钥临时文件 {:?} 失败: {}", tmp, e);
        let _ = std::fs::remove_file(&tmp);
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        // Windows：rename 不能覆盖已存在文件，先删再改名
        let _ = std::fs::remove_file(path);
        if let Err(e2) = std::fs::rename(&tmp, path) {
            log::warn!(
                "[node-agent] 节点密钥缓存落盘失败 {:?}: {}（重试: {}）",
                path,
                e,
                e2
            );
            let _ = std::fs::remove_file(&tmp);
            return false;
        }
    }
    true
}

/// 写密钥缓存：优先写已确定的路径，失败则尝试其它候选路径（工作目录 ↔ 二进制目录），
/// 成功时把 `cache_path` 切到该路径，后续持续写到同一位置。
fn persist_secret(cache_path: &mut std::path::PathBuf, secret: &str) -> bool {
    if save_secret_atomic(cache_path, secret) {
        return true;
    }
    for path in node_secret_paths() {
        if path == *cache_path {
            continue;
        }
        if save_secret_atomic(&path, secret) {
            log::info!("[node-agent] 节点密钥缓存改写到 {:?}", path);
            *cache_path = path;
            return true;
        }
    }
    false
}

/// 启动节点 agent（每个进程最多一次）。`kind` 为 "hbbs" 或 "hbbr"。
///
/// **不阻塞调用方**：公网地址探测（最多 5s）在 run() 的后台线程里做，
/// 历史上它在启动主线程上跑，最坏拖慢服务启动 5 秒。
pub fn start(kind: &'static str) {
    STARTED.call_once(|| {
        let api = env_or("RD_API_SERVER", "");
        if api.is_empty() {
            log::info!("[node-agent] 未配置 RD_API_SERVER，节点上报功能未启用");
            return;
        }
        let key = env_or("RD_NODE_KEY", "");
        // 密钥来源：缓存文件 > 环境变量（详见 load_node_secret 的说明）
        let (secret, secret_path) = load_node_secret();
        let addr = env_or("RD_NODE_ADDR", "");
        let lan_addr = env_or("RD_NODE_LAN_ADDR", "");
        let region = env_or("RD_NODE_REGION", "");
        let name = env_or("RD_NODE_NAME", "");
        let pull_relays = env_flag("RD_RELAY_PULL", kind == "hbbs");
        // hbbr 的 -p 参数实际监听 port+1（见 hbbr.rs），对外中继端口仍是 21117
        let default_port: u16 = if kind == "hbbr" { 21117 } else { 21116 };
        log::info!(
            "[node-agent] 启动: kind={} addr={} api={} region={} pull_relays={} node_secret={}",
            kind,
            if addr.is_empty() {
                "(未配置，后台探测)"
            } else {
                addr.as_str()
            },
            api,
            region,
            pull_relays,
            if secret.is_empty() { "未启用" } else { "已配置" },
        );
        std::thread::spawn(move || {
            run(
                kind,
                &api,
                &key,
                secret,
                secret_path,
                &addr,
                &lan_addr,
                &region,
                &name,
                pull_relays,
                default_port,
            )
        });
    });
}

/// 解析一个候选对外地址字符串：`IPv4:port`，或裸 `IPv4`（补默认端口）。
///
/// 只接受 IPv4：api-server / 客户端的中继地址都按 `host:port` 处理，
/// IPv6 需要方括号且下游未验证，直接拒绝比注册一个连不上的地址好。
fn parse_node_addr(raw: &str, default_port: u16) -> Option<(String, Ipv4Addr)> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let with_port = if raw.contains(':') {
        raw.to_string()
    } else {
        format!("{}:{}", raw, default_port)
    };
    match with_port.parse::<SocketAddr>() {
        Ok(SocketAddr::V4(v4)) => Some((with_port, *v4.ip())),
        Ok(SocketAddr::V6(_)) => {
            log::warn!("[node-agent] 只支持 IPv4 对外地址，已忽略 {:?}", raw);
            None
        }
        Err(_) => {
            log::warn!(
                "[node-agent] {:?} 不是合法的 IPv4 host:port（示例 203.0.113.10:21117）",
                raw
            );
            None
        }
    }
}

/// 拒绝把回环 / 私网 / 链路本地 / 保留地址当作「对外地址」注册。
///
/// 不直接用 `Ipv4Addr::is_private()`：这里手写网段判断，顺带覆盖 CGNAT
/// （100.64/10，运营商大内网常见）与保留测试网段，且不依赖较新的 std API。
fn is_forbidden_public_ip(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
        || o[0] == 0
        || o[0] == 10
        || (o[0] == 172 && (16..=31).contains(&o[1]))
        || (o[0] == 192 && o[1] == 168)
        || (o[0] == 100 && (64..=127).contains(&o[1]))
        || (o[0] == 198 && (o[1] == 18 || o[1] == 19))
}

/// 选择可注册的对外地址（在 agent 线程里调用，失败只 warn、不阻塞服务）。
///
/// 优先级：
///   1. 显式 `RD_NODE_ADDR`：必须是公网 IPv4（回环/私网一律拒绝，私网请填 RD_NODE_LAN_ADDR）；
///   2. 自动探测公网 IP（`RD_PUBLIC_IP_URL`，默认 ifconfig.me/ip），只接受可解析的 IPv4；
///   3. 探测失败退回 `RD_NODE_LAN_ADDR`（纯内网/同 VPN 部署时仍可用）；
///   4. 都没有 → `None`：调用方**暂不注册**并打 warn。
///
/// 历史缺陷：探测失败会兜底 `127.0.0.1:21117`，api-server 里因此出现一台
/// addr=127.0.0.1 的"幽灵中继"，客户端拿到后永远连不上。
fn resolve_node_addr(explicit: &str, lan_addr: &str, default_port: u16) -> Option<String> {
    if !explicit.trim().is_empty() {
        match parse_node_addr(explicit, default_port) {
            Some((addr, ip)) => {
                if is_forbidden_public_ip(ip) {
                    log::warn!(
                        "[node-agent] RD_NODE_ADDR={} 是回环/私网等不可用地址，不作为对外地址注册；\
                         如需上报内网地址请改配 RD_NODE_LAN_ADDR（必须带端口）",
                        addr
                    );
                } else {
                    log::info!("[node-agent] 使用显式配置的对外地址 addr={}", addr);
                    return Some(addr);
                }
            }
            None => log::warn!("[node-agent] RD_NODE_ADDR={:?} 不可用，尝试自动探测", explicit.trim()),
        }
    }
    if let Some(addr) = detect_public_addr(default_port) {
        log::info!("[node-agent] 公网地址探测成功 addr={}", addr);
        return Some(addr);
    }
    log::warn!("[node-agent] 公网地址探测失败（RD_PUBLIC_IP_URL 不可达或返回非 IPv4）");
    if !lan_addr.trim().is_empty() {
        match parse_node_addr(lan_addr, default_port) {
            Some((addr, ip)) if ip.is_loopback() || ip.is_unspecified() => {
                log::warn!("[node-agent] RD_NODE_LAN_ADDR={} 是回环地址，已忽略", addr);
            }
            Some((addr, _)) => {
                log::warn!(
                    "[node-agent] 对外地址退回 RD_NODE_LAN_ADDR={}（公网探测失败，外部客户端可能连不上）",
                    addr
                );
                return Some(addr);
            }
            None => log::warn!(
                "[node-agent] RD_NODE_LAN_ADDR={:?} 不是合法的 IPv4 host:port（必须带端口，如 10.126.126.5:21117）",
                lan_addr.trim()
            ),
        }
    }
    None
}

/// 探测本机公网 IP（只返回 IPv4）。
///
/// 说明：agent 与 api-server 同机时走 127.0.0.1，api-server 只能看到回环地址，
/// 因此无法由 api-server 反推公网 IP，这里改用外部回显服务探测。
/// 最多阻塞 5s，只在 agent 后台线程里调用。
fn detect_public_addr(port: u16) -> Option<String> {
    let url = env_or("RD_PUBLIC_IP_URL", "https://ifconfig.me/ip");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let ip = client
        .get(&url)
        .send()
        .ok()?
        .text()
        .ok()?
        .trim()
        .to_string();
    if ip.is_empty() || ip.len() > 45 {
        return None;
    }
    let v4: Ipv4Addr = match ip.parse() {
        Ok(v4) => v4,
        Err(_) => {
            log::warn!("[node-agent] 公网探测返回非 IPv4 地址 {:?}，已忽略", ip);
            return None;
        }
    };
    if is_forbidden_public_ip(v4) {
        log::warn!("[node-agent] 公网探测返回不可用地址 {}，已忽略", v4);
        return None;
    }
    Some(format!("{}:{}", v4, port))
}

fn build_client() -> Option<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        // api-server 多用自签名证书（IP:端口），与客户端行为保持一致
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(6))
        .build()
        .ok()
}

/// 给节点接口请求统一加鉴权头：rustdesk key + 节点专用密钥。
///
/// `X-Node-Secret` 是 api-server 的 `node.secret`；为空则不带该 header
/// （未配置时 api-server 只告警放行，兼容升级）。
fn with_auth(
    mut req: reqwest::blocking::RequestBuilder,
    key: &str,
    secret: &str,
) -> reqwest::blocking::RequestBuilder {
    if !key.is_empty() {
        req = req.header("X-Rustdesk-Key", key);
    }
    if !secret.is_empty() {
        req = req.header("X-Node-Secret", secret);
    }
    req
}

fn post_json(
    client: &reqwest::blocking::Client,
    url: &str,
    key: &str,
    secret: &str,
    body: &serde_json::Value,
) -> bool {
    let req = with_auth(client.post(url).json(body), key, secret);
    match req.send() {
        Ok(resp) => {
            let ok = resp.status().is_success();
            if !ok {
                log::warn!("[node-agent] POST {} 返回 {}", url, resp.status());
            }
            ok
        }
        Err(e) => {
            log::warn!("[node-agent] POST {} 失败: {}", url, e);
            false
        }
    }
}

/// 读取配置里的整数项；缺省 / `null` 返回 None（= 保持不变），`0` = 不限制。
///
/// 兼容 32 这类整数与 32.0 这类浮点；负数 / NaN / 无穷视为非法，返回 None（忽略）。
fn cfg_u64(cfg: &serde_json::Value, key: &str) -> Option<u64> {
    let v = cfg.get(key)?;
    if v.is_null() {
        return None;
    }
    v.as_u64().or_else(|| {
        v.as_f64().and_then(|f| {
            if f.is_finite() && f >= 0.0 {
                Some(f as u64)
            } else {
                log::warn!("[node-agent] 配置项 {}={} 非法（应为非负数），已忽略", key, f);
                None
            }
        })
    })
}

/// 读取配置里的浮点项；缺省 / `null` 返回 None（= 保持不变）。
fn cfg_f64(cfg: &serde_json::Value, key: &str) -> Option<f64> {
    let v = cfg.get(key)?;
    if v.is_null() {
        return None;
    }
    v.as_f64()
}

/// 校验并换算 `downgrade_threshold` 的量纲。
///
/// 约定 api-server 传 **0~1 的比例**（0.66 = 66%）。误传百分数（如 66）时
/// 直接 ×100 会变成 6600%，自动降级永不触发；所以这里做量纲校验：
///   - 0~1：按比例换算成百分比整数；
///   - 1~100：按百分数解释（兼容旧/误配置），并告警；
///   - >100 或 <0：非法，告警并忽略（保持原值）。
///
/// 0 表示关闭自动降级（阈值 0 会让降级条件恒真，故单独语义化）。
fn normalize_threshold(x: f64) -> Option<usize> {
    if !x.is_finite() || x < 0.0 {
        log::warn!("[node-agent] downgrade_threshold={} 非法，已忽略", x);
        return None;
    }
    if x > 100.0 {
        log::warn!(
            "[node-agent] downgrade_threshold={} 超出 0~1 约定（疑似量纲错误），已忽略；传 0 表示关闭自动降级",
            x
        );
        return None;
    }
    if x > 1.0 {
        log::warn!(
            "[node-agent] downgrade_threshold={} 超出 0~1 约定，按百分数 {}% 解释",
            x,
            x.round() as usize
        );
        return Some(x.round() as usize);
    }
    Some((x * 100.0).round() as usize)
}

#[allow(clippy::too_many_arguments)]
fn run(
    kind: &'static str,
    api: &str,
    key: &str,
    mut secret: String,
    mut secret_path: std::path::PathBuf,
    explicit_addr: &str,
    lan_addr: &str,
    region: &str,
    name: &str,
    pull_relays: bool,
    default_port: u16,
) {
    let Some(client) = build_client() else {
        log::error!("[node-agent] 无法创建 HTTP client，节点上报已停用");
        return;
    };
    let started = Instant::now();
    // 地址探测最多阻塞 5s，但只阻塞本线程，不影响 hbbs/hbbr 启动。
    let mut addr = resolve_node_addr(explicit_addr, lan_addr, default_port);
    let mut last_detect = Instant::now();
    if addr.is_none() {
        log::warn!(
            "[node-agent] 暂无可用的对外地址，暂不注册；{}s 后重试探测（可用 RD_NODE_ADDR / RD_NODE_LAN_ADDR 显式指定）",
            ADDR_DETECT_RETRY_SECS
        );
    }
    let mut node_id = addr.as_ref().map(|a| format!("{}@{}", kind, a));
    let mut registered = false;
    let mut last_cfg: Option<Instant> = None;
    let mut last_pull: Option<Instant> = None;
    let mut last_secret: Option<Instant> = None;

    loop {
        // 地址未知（探测失败）时按固定间隔重试；探测在后台线程，失败不影响服务
        if addr.is_none() && last_detect.elapsed() >= Duration::from_secs(ADDR_DETECT_RETRY_SECS) {
            last_detect = Instant::now();
            addr = resolve_node_addr(explicit_addr, lan_addr, default_port);
            if let Some(a) = addr.as_ref() {
                log::info!("[node-agent] 地址探测成功，开始注册 addr={}", a);
                node_id = Some(format!("{}@{}", kind, a));
            } else {
                log::warn!(
                    "[node-agent] 仍无可用对外地址，继续跳过注册（{}s 后重试）",
                    ADDR_DETECT_RETRY_SECS
                );
            }
        }

        let (addr, node_id) = match (addr.as_deref(), node_id.as_deref()) {
            (Some(a), Some(id)) => (a, id),
            _ => {
                // 不注册 / 不心跳：api-server 的心跳接口要求节点已注册
                // （service/node.go: "节点未注册"），用空地址注册更会污染节点表。
                std::thread::sleep(Duration::from_secs(HEARTBEAT_SECS));
                continue;
            }
        };

        if !registered {
            let body = serde_json::json!({
                "node_id": node_id,
                "kind": kind,
                "name": name,
                "addr": addr,
                "lan_addr": lan_addr,
                "region": region,
                "version": crate::version::VERSION,
            });
            registered = post_json(
                &client,
                &format!("{}/api/node/register", api),
                key,
                &secret,
                &body,
            );
            if registered {
                log::info!("[node-agent] 节点已注册: {}", node_id);
            }
        }

        if registered {
            // 指标：hbbs 上报在线 peer 数，hbbr 上报活跃连接数。
            // 由各进程注册的 getter 实时取值（不再用只增不减的镜像原子量）。
            let peers = PEERS_GETTER.get().map_or(0, |f| f());
            let sessions = CONNS_GETTER.get().map_or(0, |f| f());
            let hb = serde_json::json!({
                "node_id": node_id,
                "peers": peers,
                "sessions": sessions,
                "bw_in": 0,
                "bw_out": 0,
                "cpu_pct": 0,
                "mem_pct": 0,
                "uptime_sec": started.elapsed().as_secs(),
                "version": crate::version::VERSION,
            });
            if !post_json(
                &client,
                &format!("{}/api/node/heartbeat", api),
                key,
                &secret,
                &hb,
            ) {
                // 心跳失败多为 api-server 重启或网络抖动：下一轮重新注册
                registered = false;
            }
        }

        let now = Instant::now();
        // 首轮延迟 FIRST_PULL_DELAY_SECS：node_agent::start() 早于
        // RendezvousServer::start() / relay io_loop，立即拉取必然失败。
        let cfg_due = last_cfg
            .map_or(started.elapsed() >= Duration::from_secs(FIRST_PULL_DELAY_SECS), |t| {
                now.duration_since(t) >= Duration::from_secs(CONFIG_PULL_SECS)
            });
        if registered && cfg_due {
            last_cfg = Some(now);
            pull_and_apply_config(&client, api, key, &secret, node_id);
        }

        let pull_due = last_pull.map_or(
            started.elapsed() >= Duration::from_secs(FIRST_PULL_DELAY_SECS),
            |t| now.duration_since(t) >= Duration::from_secs(RELAY_PULL_SECS),
        );
        if pull_relays && pull_due {
            last_pull = Some(now);
            pull_and_apply_relays(&client, api, key, &secret);
        }

        // 节点密钥轮询：每 60s 一次（复用本定时循环，不新增线程/高频请求）。
        // 服务端在后台轮换密钥后，节点在这里自动换成新密钥并落盘，无需改 systemd、无需重启。
        // 与是否已注册无关：注册本身也需要正确密钥，密钥失效时要能尽快恢复。
        let secret_due = last_secret.map_or(
            started.elapsed() >= Duration::from_secs(FIRST_PULL_DELAY_SECS),
            |t| now.duration_since(t) >= Duration::from_secs(SECRET_PULL_SECS),
        );
        if secret_due {
            last_secret = Some(now);
            pull_and_apply_secret(&client, api, key, &mut secret, &mut secret_path);
        }

        std::thread::sleep(Duration::from_secs(HEARTBEAT_SECS));
    }
}

/// `GET /api/node/secret` 的一次拉取结果。
enum SecretFetch {
    /// 服务端密钥与本机一致，无需变更
    Unchanged,
    /// 已更新内存中的密钥（并尽量落盘）
    Updated,
    /// 401：本机密钥已超出服务端宽限期
    Unauthorized,
    /// 其它失败（网络 / 限流 / 响应格式异常）
    Failed,
}

/// 拉取服务端当前节点密钥并按需落盘、更新内存（每 60s 一次，同一个 std::thread 内串行调用）。
///
/// 契约（与 api-server 严格一致）：
///   - 请求头 `X-Node-Secret` = 本机当前密钥（服务端在宽限期内也接受上一把）；
///   - 200：`{"code":0,"data":{"secret":"<当前密钥>","rotated_at":<unix秒>}}`；
///   - 401：本机密钥已不被接受（宽限期已过）→ warn 后继续用旧密钥重试，**不退出线程**。
///
/// 安全：全程不打印密钥明文，只打印「已更新」与缓存文件路径。
fn pull_and_apply_secret(
    client: &reqwest::blocking::Client,
    api: &str,
    key: &str,
    current: &mut String,
    cache_path: &mut std::path::PathBuf,
) -> SecretFetch {
    let url = format!("{}/api/node/secret", api);
    let req = with_auth(client.get(&url), key, current);
    let resp = match req.send() {
        Ok(r) => r,
        Err(e) => {
            log::debug!("[node-agent] 拉取节点密钥失败: {}", e);
            return SecretFetch::Failed;
        }
    };
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        // 服务端已配置 node.secret，且本机密钥既不是 current、也不在 7 天宽限期内的 prev
        log::warn!(
            "[node-agent] 节点密钥已失效，请检查 api-server 配置（宽限期已过，GET /api/node/secret 返回 401）；继续用旧密钥重试"
        );
        return SecretFetch::Unauthorized;
    }
    if !status.is_success() {
        log::warn!("[node-agent] 拉取节点密钥返回 {}", status);
        return SecretFetch::Failed;
    }
    let body: serde_json::Value = match resp.json() {
        Ok(v) => v,
        Err(e) => {
            log::warn!("[node-agent] 节点密钥响应解析失败: {}", e);
            return SecretFetch::Failed;
        }
    };
    // 全站响应格式 {code, message, data}；code != 0 视为业务失败
    if let Some(code) = body["code"].as_i64() {
        if code != 0 {
            log::warn!(
                "[node-agent] 拉取节点密钥业务失败: code={} msg={}",
                code,
                body["message"].as_str().unwrap_or("")
            );
            return SecretFetch::Failed;
        }
    }
    let Some(secret) = body["data"]["secret"].as_str().map(str::trim) else {
        log::warn!("[node-agent] 节点密钥响应缺少 data.secret，已忽略");
        return SecretFetch::Failed;
    };
    // 服务端未配置 node.secret 时仍返回 200，但 secret 为空串：
    // 必须**保持本地密钥不变**，否则会把已落盘/环境变量里的密钥清空。
    if secret.is_empty() {
        log::debug!("[node-agent] 服务端未配置节点密钥（下发空串），保持本地密钥不变");
        return SecretFetch::Unchanged;
    }
    if !is_plausible_secret(secret) {
        log::warn!("[node-agent] 服务端下发的节点密钥格式非法（含空白/过长），已忽略");
        return SecretFetch::Failed;
    }
    if secret == current.as_str() {
        return SecretFetch::Unchanged;
    }
    // 落盘失败也更新内存：先保证本次运行可用，落盘问题只影响重启后的初值
    if persist_secret(cache_path, secret) {
        log::info!(
            "[node-agent] 节点密钥已更新（服务端下发），缓存文件 {:?}",
            cache_path
        );
    } else {
        log::warn!("[node-agent] 节点密钥已更新（服务端下发），但缓存落盘失败，仅在内存生效");
    }
    *current = secret.to_string();
    SecretFetch::Updated
}

/// 拉取调度排序后的中继列表并热更新到 hbbs。
fn pull_and_apply_relays(client: &reqwest::blocking::Client, api: &str, key: &str, secret: &str) {
    let url = format!("{}/api/node/relays", api);
    let req = with_auth(client.get(&url), key, secret);
    let resp = match req.send() {
        Ok(r) => r,
        Err(e) => {
            log::warn!("[node-agent] 拉取中继列表失败: {}", e);
            return;
        }
    };
    let v: serde_json::Value = match resp.json() {
        Ok(v) => v,
        Err(e) => {
            log::warn!("[node-agent] 中继列表解析失败: {}", e);
            return;
        }
    };
    let Some(list) = v["data"]["relays"].as_array() else {
        log::warn!("[node-agent] 中继列表格式异常");
        return;
    };
    let addrs: Vec<String> = list
        .iter()
        .filter_map(|x| x["addr"].as_str().map(|s| s.to_string()))
        .filter(|s| !s.is_empty())
        .collect();
    if addrs.is_empty() {
        // 全部节点离线/下线时不覆盖现有配置，避免把中继列表清空导致客户端无中继可用
        log::warn!("[node-agent] 调度返回空列表，保留现有中继配置");
        return;
    }
    // 平台下发优先：平台已指定中继列表时，调度结果不再覆盖
    if PLATFORM_RELAYS.lock().map(|g| g.is_some()).unwrap_or(false) {
        log::debug!("[node-agent] 平台配置已接管中继列表，忽略调度结果");
        return;
    }
    match RELAY_APPLY.get() {
        Some(f) => {
            if f(addrs.clone()) {
                log::info!("[node-agent] 已热更新中继列表（调度）: {:?}", addrs);
            } else {
                log::warn!("[node-agent] 中继列表热更新失败");
            }
        }
        None => log::info!("[node-agent] 本进程未注册中继热更新回调，跳过"),
    }
}

/// 拉取 api-server 下发的节点配置并热应用。
///
/// 支持热改的项：
///   - `relay_servers`（hbbs）→ 通过内部通道替换中继列表
///   - `limit_speed_mbps` / `single_bandwidth_mbps` / `total_bandwidth_mbps` /
///     `downgrade_threshold` / `downgrade_start_check_sec`（hbbr）→ 更新运行时参数
///
/// 量纲约定（与 api-server 严格一致）：
///   - 三个 bandwidth 是 **Mbit/s 整数**；
///   - `downgrade_threshold` 是 **0~1 的比例浮点**；
///   - `downgrade_start_check_sec` 是**秒整数**；
///   - 键缺省 / `null` = 保持不变；`0` = 不限制（解除限速）。
///
/// 其余启动参数（key / port / mask / must_login）**无法热改**，由后台仅做展示，
/// 这里遇到会打印提示而不是假装成功。
fn pull_and_apply_config(
    client: &reqwest::blocking::Client,
    api: &str,
    key: &str,
    secret: &str,
    node_id: &str,
) {
    let url = format!("{}/api/node/config?node_id={}", api, node_id);
    let req = with_auth(client.get(&url), key, secret);
    let resp = match req.send() {
        Ok(r) => r,
        Err(e) => {
            log::debug!("[node-agent] 拉取节点配置失败: {}", e);
            return;
        }
    };
    let v: serde_json::Value = match resp.json() {
        Ok(v) => v,
        Err(_) => return,
    };
    let cfg_str = v["data"]["config"].as_str().unwrap_or("");
    if cfg_str.trim().is_empty() {
        return;
    }
    let cfg: serde_json::Value = match serde_json::from_str(cfg_str) {
        Ok(c) => c,
        Err(e) => {
            log::warn!("[node-agent] 节点配置 JSON 非法: {}", e);
            return;
        }
    };

    // 1) 中继列表（hbbs 热更新）
    // 平台配置里没有 relay_servers（或为空）时，交回调度列表控制
    let has_platform_relays = cfg["relay_servers"]
        .as_array()
        .map(|a| {
            a.iter()
                .any(|x| x.as_str().map(|s| !s.is_empty()).unwrap_or(false))
        })
        .unwrap_or(false);
    if !has_platform_relays {
        if let Ok(mut g) = PLATFORM_RELAYS.lock() {
            if g.is_some() {
                *g = None;
                log::info!("[node-agent] 平台配置已移除中继列表，交回调度控制");
            }
        }
    }
    if let Some(list) = cfg["relay_servers"].as_array() {
        let addrs: Vec<String> = list
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .filter(|s| !s.is_empty())
            .collect();
        if !addrs.is_empty() {
            // 平台接管：记录并应用，此后调度列表不再覆盖
            if let Ok(mut g) = PLATFORM_RELAYS.lock() {
                *g = Some(addrs.clone());
            }
            if let Some(f) = RELAY_APPLY.get() {
                if f(addrs.clone()) {
                    log::info!(
                        "[node-agent] 已按平台下发更新中继列表（平台优先，调度不再覆盖）: {:?}",
                        addrs
                    );
                }
            }
        }
    }

    // 2) hbbr 限速与降级（热更新）
    if let Some(f) = LIMITS_APPLY.get() {
        // Mbit/s 整数 → bit/s；32 位平台上 saturating，避免乘法溢出回绕
        let mbps = |k: &str| -> Option<usize> {
            cfg_u64(&cfg, k).map(|x| {
                usize::try_from(x)
                    .unwrap_or(usize::MAX)
                    .saturating_mul(1024 * 1024)
            })
        };
        let limit = mbps("limit_speed_mbps");
        let single = mbps("single_bandwidth_mbps");
        let total = mbps("total_bandwidth_mbps");
        let threshold = cfg_f64(&cfg, "downgrade_threshold").and_then(normalize_threshold);
        let check_ms = cfg_u64(&cfg, "downgrade_start_check_sec").map(|x| {
            usize::try_from(x)
                .unwrap_or(usize::MAX)
                .saturating_mul(1000)
        });
        if limit.is_some() || single.is_some() || total.is_some() || threshold.is_some()
            || check_ms.is_some()
        {
            f(limit, single, total, threshold, check_ms);
        }
    }

    // 3) 启动参数类：无法热改，明确提示而不是静默忽略
    for k in ["key", "port", "mask", "must_login"] {
        if cfg.get(k).map(|x| !x.is_null()).unwrap_or(false) {
            log::warn!(
                "[node-agent] 配置项 {} 属于启动参数，热更新不生效，需重启进程后由启动参数决定",
                k
            );
        }
    }
}
