//! 集群节点 agent：向 api-server 注册、上报心跳指标、拉取按调度分数排序的中继列表。
//!
//! 设计目标（配合 api-server 的 `/api/node/*` 接口）：
//!   - **零侵入**：不改变 hbbs/hbbr 的对外协议，只作为旁路线程运行；
//!   - **可关闭**：未配置 `RD_API_SERVER` 时完全不影响原有行为；
//!   - **公网 + 密钥**：请求携带 `X-Rustdesk-Key`（与 api-server 一致），
//!     复用 api-server 的 ProcessMonitorAuth 鉴权；
//!   - **热更新**：hbbs 拉到新的中继列表后通过内部通道替换，无需重启。
//!
//! 说明：hbbs 走 lib crate、hbbr 是独立 binary，两者都 include 本模块。
//! 因此这里**不直接引用** `rendezvous_server` / `relay_server`，
//! 而是通过「函数指针回调 + 共享原子计数」解耦：
//!   - hbbs 启动时 `set_relay_apply(rendezvous_server::apply_relay_list)`
//!   - hbbr 在 relay_server 里更新 `ACTIVE_CONNS`
//!
//! 环境变量：
//!   RD_API_SERVER    api-server 地址，例如 https://183.24.70.210:21120
//!   RD_NODE_KEY      rustdesk key（与 hbbs/hbbr 的 -k 相同）
//!   RD_NODE_ADDR     本节点对外地址 host:port
//!   RD_NODE_LAN_ADDR 本节点内网地址（可选，供同内网客户端直连）
//!   RD_NODE_REGION   地域标识（可选，例如 gz / hc）
//!   RD_NODE_NAME     节点显示名（可选）
//!   RD_RELAY_PULL    是否拉取中继列表；hbbs 默认 true、hbbr 默认 false

use hbb_common::log;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// hbbs 在线 peer 数（由 rendezvous_server 维护）
pub static ONLINE_PEERS: AtomicUsize = AtomicUsize::new(0);
/// hbbr 活跃连接数（由 relay_server 维护）
pub static ACTIVE_CONNS: AtomicUsize = AtomicUsize::new(0);

/// 中继列表热更新回调（仅 hbbs 注册）
type RelayApply = fn(Vec<String>) -> bool;
/// 限速热更新回调（仅 hbbr 注册）
type LimitsApply = fn(usize, usize, usize, usize, usize);
static RELAY_APPLY: once_cell::sync::OnceCell<RelayApply> = once_cell::sync::OnceCell::new();
static LIMITS_APPLY: once_cell::sync::OnceCell<LimitsApply> = once_cell::sync::OnceCell::new();

static STARTED: std::sync::Once = std::sync::Once::new();

/// 心跳间隔
const HEARTBEAT_SECS: u64 = 15;
/// 拉取中继列表间隔
const RELAY_PULL_SECS: u64 = 60;
/// 拉取 api-server 下发配置的间隔
const CONFIG_PULL_SECS: u64 = 30;

/// 注册中继列表热更新回调（hbbs 启动时调用）。
pub fn set_relay_apply(f: RelayApply) {
    let _ = RELAY_APPLY.set(f);
}

/// 注册限速热更新回调（hbbr 启动时调用）。
pub fn set_limits_apply(f: LimitsApply) {
    let _ = LIMITS_APPLY.set(f);
}

fn env_or(k: &str, d: &str) -> String {
    match std::env::var(k) {
        Ok(v) => {
            let v = v.trim().to_string();
            if v.is_empty() {
                d.to_string()
            } else {
                v
            }
        }
        Err(_) => d.to_string(),
    }
}

fn env_flag(k: &str, d: bool) -> bool {
    match std::env::var(k) {
        Ok(v) => matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "y" | "yes"),
        Err(_) => d,
    }
}

/// 启动节点 agent（每个进程最多一次）。`kind` 为 "hbbs" 或 "hbbr"。
pub fn start(kind: &'static str, default_addr: &'static str) {
    STARTED.call_once(|| {
        let api = env_or("RD_API_SERVER", "");
        if api.is_empty() {
            log::info!("[node-agent] 未配置 RD_API_SERVER，节点上报功能未启用");
            return;
        }
        let key = env_or("RD_NODE_KEY", "");
        let default_port: u16 = if kind == "hbbr" { 21117 } else { 21116 };
        let mut addr = env_or("RD_NODE_ADDR", "");
        if addr.is_empty() {
            // 未显式配置：尝试自动探测公网 IP（避免手工填错，之前就填错过一次）
            addr = detect_public_addr(default_port).unwrap_or_else(|| default_addr.to_string());
            log::info!("[node-agent] RD_NODE_ADDR 未配置，自动探测得到 addr={}", addr);
        }
        let lan_addr = env_or("RD_NODE_LAN_ADDR", "");
        let region = env_or("RD_NODE_REGION", "");
        let name = env_or("RD_NODE_NAME", "");
        let pull_relays = env_flag("RD_RELAY_PULL", kind == "hbbs");
        log::info!(
            "[node-agent] 启动: kind={} addr={} api={} region={} pull_relays={}",
            kind,
            addr,
            api,
            region,
            pull_relays
        );
        std::thread::spawn(move || {
            run(kind, &api, &key, &addr, &lan_addr, &region, &name, pull_relays)
        });
    });
}

/// 探测本机公网 IP（用于在未显式配置 RD_NODE_ADDR 时自动生成对外地址）。
///
/// 说明：agent 与 api-server 同机时走 127.0.0.1，api-server 只能看到回环地址，
/// 因此无法由 api-server 反推公网 IP，这里改用外部回显服务探测。
/// 探测失败则退回 RD_NODE_ADDR（若配了）或内网地址，绝不阻塞启动。
fn detect_public_addr(port: u16) -> Option<String> {
    let url = env_or("RD_PUBLIC_IP_URL", "https://ifconfig.me/ip");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let ip = client.get(&url).send().ok()?.text().ok()?.trim().to_string();
    if ip.is_empty() || ip.len() > 45 || ip.contains('<') {
        return None;
    }
    Some(format!("{}:{}", ip, port))
}
fn build_client() -> Option<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        // api-server 多用自签名证书（IP:端口），与客户端行为保持一致
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(6))
        .build()
        .ok()
}

fn post_json(
    client: &reqwest::blocking::Client,
    url: &str,
    key: &str,
    body: &serde_json::Value,
) -> bool {
    let mut req = client.post(url).json(body);
    if !key.is_empty() {
        req = req.header("X-Rustdesk-Key", key);
    }
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

fn run(
    kind: &'static str,
    api: &str,
    key: &str,
    addr: &str,
    lan_addr: &str,
    region: &str,
    name: &str,
    pull_relays: bool,
) {
    let Some(client) = build_client() else {
        log::error!("[node-agent] 无法创建 HTTP client，节点上报已停用");
        return;
    };
    let node_id = format!("{}@{}", kind, addr);
    let started = Instant::now();
    let mut registered = false;
    let mut last_pull = Instant::now() - Duration::from_secs(RELAY_PULL_SECS);
    let mut last_cfg = Instant::now() - Duration::from_secs(CONFIG_PULL_SECS);

    loop {
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
            registered = post_json(&client, &format!("{}/api/node/register", api), key, &body);
            if registered {
                log::info!("[node-agent] 节点已注册: {}", node_id);
            }
        }

        // 指标：hbbs 上报在线 peer 数，hbbr 上报活跃连接数
        let (peers, sessions) = if kind == "hbbr" {
            (0usize, ACTIVE_CONNS.load(Ordering::Relaxed))
        } else {
            (ONLINE_PEERS.load(Ordering::Relaxed), 0usize)
        };

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
        if !post_json(&client, &format!("{}/api/node/heartbeat", api), key, &hb) {
            // 心跳失败多为 api-server 重启或网络抖动：下一轮重新注册
            registered = false;
        }

        // 每 30 秒拉一次 api-server 下发的配置并热应用（中继列表 / 限速）
        if last_cfg.elapsed() >= Duration::from_secs(CONFIG_PULL_SECS) {
            last_cfg = Instant::now();
            pull_and_apply_config(&client, api, key, &node_id);
        }

        if pull_relays && last_pull.elapsed() >= Duration::from_secs(RELAY_PULL_SECS) {
            last_pull = Instant::now();
            pull_and_apply_relays(&client, api, key, "");
        }

        std::thread::sleep(Duration::from_secs(HEARTBEAT_SECS));
    }
}

/// 拉取调度排序后的中继列表并热更新到 hbbs。
fn pull_and_apply_relays(client: &reqwest::blocking::Client, api: &str, key: &str, region: &str) {
    let mut url = format!("{}/api/node/relays", api);
    if !region.is_empty() {
        url.push_str(&format!("?region={}", region));
    }
    let mut req = client.get(&url);
    if !key.is_empty() {
        req = req.header("X-Rustdesk-Key", key);
    }
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
    match RELAY_APPLY.get() {
        Some(f) => {
            if f(addrs.clone()) {
                log::info!("[node-agent] 已热更新中继列表: {:?}", addrs);
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
///     `downgrade_threshold` / `downgrade_start_check_sec`（hbbr）→ 更新运行时原子量
///
/// 其余启动参数（key / port / mask / must_login）**无法热改**，由后台仅做展示，
/// 这里遇到会打印提示而不是假装成功。
fn pull_and_apply_config(
    client: &reqwest::blocking::Client,
    api: &str,
    key: &str,
    node_id: &str,
) {
    let url = format!("{}/api/node/config?node_id={}", api, node_id);
    let mut req = client.get(&url);
    if !key.is_empty() {
        req = req.header("X-Rustdesk-Key", key);
    }
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
    if let Some(list) = cfg["relay_servers"].as_array() {
        let addrs: Vec<String> = list
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .filter(|s| !s.is_empty())
            .collect();
        if !addrs.is_empty() {
            if let Some(f) = RELAY_APPLY.get() {
                if f(addrs.clone()) {
                    log::info!("[node-agent] 已按平台下发更新中继列表: {:?}", addrs);
                }
            }
        }
    }

    // 2) hbbr 限速与降级（热更新）
    if let Some(f) = LIMITS_APPLY.get() {
        let mbps = |k: &str| -> usize {
            cfg[k].as_u64().unwrap_or(0) as usize * 1024 * 1024
        };
        let limit = mbps("limit_speed_mbps");
        let single = mbps("single_bandwidth_mbps");
        let total = mbps("total_bandwidth_mbps");
        let threshold = cfg["downgrade_threshold"]
            .as_f64()
            .map(|x| (x * 100.0).round() as usize)
            .unwrap_or(0);
        let check_ms = cfg["downgrade_start_check_sec"]
            .as_u64()
            .map(|x| (x * 1000) as usize)
            .unwrap_or(0);
        if limit + single + total + threshold + check_ms > 0 {
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