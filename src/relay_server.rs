use async_speed_limit::Limiter;
use async_trait::async_trait;
use hbb_common::{
    allow_err, bail,
    bytes::{Bytes, BytesMut},
    futures_util::{sink::SinkExt, stream::StreamExt},
    log,
    protobuf::Message as _,
    rendezvous_proto::*,
    sleep,
    tcp::{listen_any, FramedStream},
    timeout,
    tokio::{
        self,
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Mutex, RwLock},
        time::{interval, Duration},
    },
    ResultType,
};
use sodiumoxide::crypto::sign;
use std::{
    collections::{HashMap, HashSet},
    io::prelude::*,
    io::Error,
    net::SocketAddr,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

type Usage = (usize, usize, usize, usize);

lazy_static::lazy_static! {
    static ref PEERS: Mutex<HashMap<String, Box<dyn StreamTrait>>> = Default::default();
    static ref USAGE: RwLock<HashMap<String, Usage>> = Default::default();
    static ref BLACKLIST: RwLock<HashSet<String>> = Default::default();
    static ref BLOCKLIST: RwLock<HashSet<String>> = Default::default();
}

static DOWNGRADE_THRESHOLD_100: AtomicUsize = AtomicUsize::new(66); // 0.66
static DOWNGRADE_START_CHECK: AtomicUsize = AtomicUsize::new(1_800_000); // in ms
/// 黑名单（人工封禁的 IP）连接限速，bit/s；0 = 不限制。
/// 可由 api-server 下发 `limit_speed_mbps` 覆盖。
static LIMIT_SPEED: AtomicUsize = AtomicUsize::new(32 * 1024 * 1024);
/// 总带宽限速，bit/s；0 = 不限制。真实落点是 [`LIMITER`]（本文件 io_loop 里初始化）。
static TOTAL_BANDWIDTH: AtomicUsize = AtomicUsize::new(1024 * 1024 * 1024);
/// 单连接限速，bit/s；0 = 不限制。
/// **只在连接建立时读取一次，因此热更新只对新连接生效**（见 [`relay`]）。
static SINGLE_BANDWIDTH: AtomicUsize = AtomicUsize::new(128 * 1024 * 1024);
/// 自动降级连接的固定惩罚带宽（bit/s），与 `LIMIT_SPEED` 解耦。
///
/// 上游把默认 LIMIT_SPEED 从 4Mb/s 上调到 32Mb/s（commit `1d6c53d higher default
/// bandwidth`），而平台下发的 `limit_speed_mbps` 还可能被管理员填成节点总带宽：
/// 若降级直接复用 LIMIT_SPEED，降级惩罚会越来越弱、甚至完全失效。
/// 这里固定为 4Mb/s，保证"超量降速"始终有实际惩罚。
const DOWNGRADE_LIMIT_SPEED: usize = 4 * 1024 * 1024; // 4 Mb/s

/// 中继单向写入的超时时间（毫秒）。
///
/// `relay()` 里的转发是写在 `select!` 分支体里的 `.await`：一旦接收方不读
/// （窗口塞满、进程卡死、socket 半开），这里就会永久阻塞，**且不会再回到
/// select 去轮询 30 秒空闲定时器** —— 于是这条中继既不发数据、也永远不超时，
/// 两端 socket 都关不掉，表现为对端一直卡在"已连接但不出画面"。
/// 2026-11-04 故障 id=234485258 就是这种半死会话：控制端窗口 `win 515`
/// 几乎不 ACK，对端仍在以 ~200KB/s 灌视频，hbbr 日志里这条中继永远不 close。
///
/// 取 15 秒：正常 RDP/桌面视频的单帧远小于这个量级，只有在真正"写不进去"
/// 时才会命中，不会误杀慢链路（慢链路的写仍会在数十毫秒内完成）。
const RELAY_WRITE_TIMEOUT_MS: u64 = 15_000;

/// 全局总带宽限速器。
///
/// 以前 Limiter 只在 io_loop 里 `new` 一次、apply_limits 只改原子量，
/// 导致 api-server 下发的 total_bandwidth **假生效**（日志显示已应用，实际流量不受限）。
/// 现在 io_loop 把实例存进来，apply_limits 直接 `set_speed_limit`。
static LIMITER: once_cell::sync::OnceCell<Limiter> = once_cell::sync::OnceCell::new();

/// bit/s → async_speed_limit 的速度参数；**0 表示不限制**（该 crate 用 INFINITY 表达）。
fn speed_limit(bit_per_sec: usize) -> f64 {
    if bit_per_sec == 0 {
        f64::INFINITY
    } else {
        bit_per_sec as f64
    }
}

/// 日志用：bit/s → "32Mb/s" / "不限制"。
fn fmt_speed(bit_per_sec: usize) -> String {
    if bit_per_sec == 0 {
        "不限制".to_owned()
    } else {
        format!("{}Mb/s", bit_per_sec / 1024 / 1024)
    }
}

/// 供集群 agent 热更新限速与降级参数（api-server 下发，无需重启）。
///
/// 语义与 api-server 约定严格一致：`None` = 保持不变，`Some(0)` = 不限制（解除限速）。
/// 参数单位：bandwidth 为 bit/s；downgrade_threshold_100 为百分比整数（66 = 0.66）；
/// downgrade_start_check_ms 为毫秒。
///
/// 生效范围：
///   - 总带宽 → 立即生效（直接改 [`LIMITER`] 的速度限制）；
///   - 单连接 / 黑名单 / 降级限速 → **只对新建立的连接生效**（连接建立时读取原子量）。
pub fn apply_limits(
    limit_speed: Option<usize>,
    single_bandwidth: Option<usize>,
    total_bandwidth: Option<usize>,
    downgrade_threshold_100: Option<usize>,
    downgrade_start_check_ms: Option<usize>,
) {
    if let Some(v) = limit_speed {
        LIMIT_SPEED.store(v, Ordering::Relaxed);
    }
    if let Some(v) = single_bandwidth {
        SINGLE_BANDWIDTH.store(v, Ordering::Relaxed);
    }
    if let Some(v) = total_bandwidth {
        TOTAL_BANDWIDTH.store(v, Ordering::Relaxed);
        match LIMITER.get() {
            Some(limiter) => limiter.set_speed_limit(speed_limit(v)),
            // 启动早期（io_loop 还没跑）收到配置：记下原子量，io_loop 初始化时按它建 Limiter
            None => log::warn!(
                "[node-agent] 总带宽限速器尚未初始化（relay io_loop 未启动），配置已记录，启动时生效"
            ),
        }
    }
    if let Some(v) = downgrade_threshold_100 {
        DOWNGRADE_THRESHOLD_100.store(v, Ordering::Relaxed);
    }
    if let Some(v) = downgrade_start_check_ms {
        DOWNGRADE_START_CHECK.store(v, Ordering::Relaxed);
    }
    // 打印"实际生效值"（0 显示为不限制），而不是回读原子量后除以 1024/1024 得到迷惑的 0Mb/s
    log::info!(
        "[node-agent] 已应用限速配置（实际生效值）: limit_speed={} single_bandwidth={} total_bandwidth={} downgrade_threshold={}% downgrade_start_check={}ms",
        fmt_speed(LIMIT_SPEED.load(Ordering::Relaxed)),
        fmt_speed(SINGLE_BANDWIDTH.load(Ordering::Relaxed)),
        fmt_speed(TOTAL_BANDWIDTH.load(Ordering::Relaxed)),
        DOWNGRADE_THRESHOLD_100.load(Ordering::Relaxed),
        DOWNGRADE_START_CHECK.load(Ordering::Relaxed),
    );
}
const BLACKLIST_FILE: &str = "blacklist.txt";
const BLOCKLIST_FILE: &str = "blocklist.txt";

#[tokio::main(flavor = "multi_thread")]
pub async fn start(port: &str, key: &str) -> ResultType<()> {
    let key = get_server_sk(key);
    if let Ok(mut file) = std::fs::File::open(BLACKLIST_FILE) {
        let mut contents = String::new();
        if file.read_to_string(&mut contents).is_ok() {
            for x in contents.split('\n') {
                if let Some(ip) = x.trim().split(' ').next() {
                    BLACKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
    }
    log::info!(
        "#blacklist({}): {}",
        BLACKLIST_FILE,
        BLACKLIST.read().await.len()
    );
    if let Ok(mut file) = std::fs::File::open(BLOCKLIST_FILE) {
        let mut contents = String::new();
        if file.read_to_string(&mut contents).is_ok() {
            for x in contents.split('\n') {
                if let Some(ip) = x.trim().split(' ').next() {
                    BLOCKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
    }
    log::info!(
        "#blocklist({}): {}",
        BLOCKLIST_FILE,
        BLOCKLIST.read().await.len()
    );
    let port: u16 = port.parse()?;
    log::info!("Listening on tcp :{}", port);
    let port2 = port + 2;
    log::info!("Listening on websocket :{}", port2);
    let main_task = async move {
        loop {
            log::info!("Start");
            io_loop(listen_any(port).await?, listen_any(port2).await?, &key).await;
        }
    };
    let listen_signal = crate::common::listen_signal();
    tokio::select!(
        res = main_task => res,
        res = listen_signal => res,
    )
}

fn check_params() {
    let tmp = std::env::var("DOWNGRADE_THRESHOLD")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        DOWNGRADE_THRESHOLD_100.store((tmp * 100.) as _, Ordering::SeqCst);
    }
    log::info!(
        "DOWNGRADE_THRESHOLD: {}",
        DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst) as f64 / 100.
    );
    let tmp = std::env::var("DOWNGRADE_START_CHECK")
        .map(|x| x.parse::<usize>().unwrap_or(0))
        .unwrap_or(0);
    if tmp > 0 {
        DOWNGRADE_START_CHECK.store(tmp * 1000, Ordering::SeqCst);
    }
    log::info!(
        "DOWNGRADE_START_CHECK: {}s",
        DOWNGRADE_START_CHECK.load(Ordering::SeqCst) / 1000
    );
    let tmp = std::env::var("LIMIT_SPEED")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        LIMIT_SPEED.store((tmp * 1024. * 1024.) as usize, Ordering::SeqCst);
    }
    log::info!(
        "LIMIT_SPEED: {}Mb/s",
        LIMIT_SPEED.load(Ordering::SeqCst) as f64 / 1024. / 1024.
    );
    let tmp = std::env::var("TOTAL_BANDWIDTH")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        TOTAL_BANDWIDTH.store((tmp * 1024. * 1024.) as usize, Ordering::SeqCst);
    }

    log::info!(
        "TOTAL_BANDWIDTH: {}Mb/s",
        TOTAL_BANDWIDTH.load(Ordering::SeqCst) as f64 / 1024. / 1024.
    );
    let tmp = std::env::var("SINGLE_BANDWIDTH")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        SINGLE_BANDWIDTH.store((tmp * 1024. * 1024.) as usize, Ordering::SeqCst);
    }
    log::info!(
        "SINGLE_BANDWIDTH: {}Mb/s",
        SINGLE_BANDWIDTH.load(Ordering::SeqCst) as f64 / 1024. / 1024.
    )
}

async fn check_cmd(cmd: &str, limiter: Limiter) -> String {
    use std::fmt::Write;

    let mut res = "".to_owned();
    let mut fds = cmd.trim().split(' ');
    match fds.next() {
        Some("h") => {
            res = format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
                "blacklist-add(ba) <ip>",
                "blacklist-remove(br) <ip>",
                "blacklist(b) <ip>",
                "blocklist-add(Ba) <ip>",
                "blocklist-remove(Br) <ip>",
                "blocklist(B) <ip>",
                "downgrade-threshold(dt) [value]",
                "downgrade-start-check(t) [value(second)]",
                "limit-speed(ls) [value(Mb/s)]",
                "total-bandwidth(tb) [value(Mb/s)]",
                "single-bandwidth(sb) [value(Mb/s)]",
                "usage(u)"
            )
        }
        Some("blacklist-add" | "ba") => {
            if let Some(ip) = fds.next() {
                for ip in ip.split('|') {
                    BLACKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
        Some("blacklist-remove" | "br") => {
            if let Some(ip) = fds.next() {
                if ip == "all" {
                    BLACKLIST.write().await.clear();
                } else {
                    for ip in ip.split('|') {
                        BLACKLIST.write().await.remove(ip);
                    }
                }
            }
        }
        Some("blacklist" | "b") => {
            if let Some(ip) = fds.next() {
                res = format!("{}\n", BLACKLIST.read().await.get(ip).is_some());
            } else {
                for ip in BLACKLIST.read().await.clone().into_iter() {
                    let _ = writeln!(res, "{ip}");
                }
            }
        }
        Some("blocklist-add" | "Ba") => {
            if let Some(ip) = fds.next() {
                for ip in ip.split('|') {
                    BLOCKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
        Some("blocklist-remove" | "Br") => {
            if let Some(ip) = fds.next() {
                if ip == "all" {
                    BLOCKLIST.write().await.clear();
                } else {
                    for ip in ip.split('|') {
                        BLOCKLIST.write().await.remove(ip);
                    }
                }
            }
        }
        Some("blocklist" | "B") => {
            if let Some(ip) = fds.next() {
                res = format!("{}\n", BLOCKLIST.read().await.get(ip).is_some());
            } else {
                for ip in BLOCKLIST.read().await.clone().into_iter() {
                    let _ = writeln!(res, "{ip}");
                }
            }
        }
        Some("downgrade-threshold" | "dt") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    // 0 表示关闭自动降级（与 api-server 的 0 = 不限制语义一致）
                    if v >= 0. {
                        DOWNGRADE_THRESHOLD_100.store((v.min(100.) * 100.) as _, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!(
                    "{}\n",
                    DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst) as f64 / 100.
                );
            }
        }
        Some("downgrade-start-check" | "t") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<usize>() {
                    // 0 = 关闭自动降级的时间门槛
                    DOWNGRADE_START_CHECK.store(v * 1000, Ordering::SeqCst);
                }
            } else {
                res = format!("{}s\n", DOWNGRADE_START_CHECK.load(Ordering::SeqCst) / 1000);
            }
        }
        Some("limit-speed" | "ls") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v >= 0. {
                        LIMIT_SPEED.store((v * 1024. * 1024.) as _, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!("{}\n", fmt_speed(LIMIT_SPEED.load(Ordering::SeqCst)));
            }
        }
        Some("total-bandwidth" | "tb") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v >= 0. {
                        let bit = (v * 1024. * 1024.) as usize;
                        TOTAL_BANDWIDTH.store(bit, Ordering::SeqCst);
                        // 立即改到真实生效的 Limiter 上（0 = 不限制）
                        limiter.set_speed_limit(speed_limit(bit));
                    }
                }
            } else {
                res = format!("{}\n", fmt_speed(TOTAL_BANDWIDTH.load(Ordering::SeqCst)));
            }
        }
        Some("single-bandwidth" | "sb") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v >= 0. {
                        SINGLE_BANDWIDTH.store((v * 1024. * 1024.) as _, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!("{}\n", fmt_speed(SINGLE_BANDWIDTH.load(Ordering::SeqCst)));
            }
        }
        Some("usage" | "u") => {
            let mut tmp: Vec<(String, Usage)> = USAGE
                .read()
                .await
                .iter()
                .map(|x| (x.0.clone(), *x.1))
                .collect();
            tmp.sort_by(|a, b| ((b.1).1).partial_cmp(&(a.1).1).unwrap());
            for (ip, (elapsed, total, highest, speed)) in tmp {
                if elapsed == 0 {
                    continue;
                }
                let _ = writeln!(
                    res,
                    "{}: {}s {:.2}MB {}kb/s {}kb/s {}kb/s",
                    ip,
                    elapsed / 1000,
                    total as f64 / 1024. / 1024. / 8.,
                    highest,
                    total / elapsed,
                    speed
                );
            }
        }
        _ => {}
    }
    res
}

/// hbbr 当前活跃连接数（含等待配对/状态查询的短连接）。
///
/// 由 io_loop 在 accept 时 +1、连接结束（含异常路径）时 -1。
/// 消费者是 node_agent 心跳：通过 [`active_conns`] 注册 getter，**不再往
/// node_agent::ACTIVE_CONNS 里镜像**（镜像只在 accept 时递增、连接关闭时漏减，
/// 导致 sessions 只增不减）。
static ACTIVE_CONNS: AtomicUsize = AtomicUsize::new(0);

/// 供 `node_agent::set_conns_getter` 注册的取值函数（必须是无捕获的 fn）。
pub fn active_conns() -> usize {
    ACTIVE_CONNS.load(Ordering::Relaxed)
}

/// 中继累计转发的字节数，供 node_agent 心跳计算带宽。
///
/// 不用主机级 `/proc/net/dev`：① Windows 没有 /proc；② 中继要的是"本进程中转了多少"，
/// 主机网卡总量会被同机其它进程干扰。单位：字节。
static RELAY_BYTES_IN: AtomicU64 = AtomicU64::new(0); // client → peer
static RELAY_BYTES_OUT: AtomicU64 = AtomicU64::new(0); // peer → client

/// 供 `node_agent::set_bw_getter` 注册的取值函数（必须是无捕获的 fn）。
///
/// 返回累计 `(入向字节, 出向字节)`：入向 = 客户端发往对端，出向 = 对端发回客户端。
/// 心跳侧用两次采样的差值算 bit/s，所以这里保持单调递增的累计值即可。
pub fn relay_bytes() -> (u64, u64) {
    (
        RELAY_BYTES_IN.load(Ordering::Relaxed),
        RELAY_BYTES_OUT.load(Ordering::Relaxed),
    )
}

async fn io_loop(listener: TcpListener, listener2: TcpListener, key: &str) {
    check_params();
    // 总带宽限速器只创建一次并登记到 LIMITER；io_loop 因监听异常重启时复用同一实例，
    // 这样 api-server 热更新的 set_speed_limit 不会因为重建 Limiter 而丢失。
    let limiter = match LIMITER.get() {
        Some(l) => l.clone(),
        None => {
            let l = <Limiter>::new(speed_limit(TOTAL_BANDWIDTH.load(Ordering::Relaxed)));
            let _ = LIMITER.set(l.clone());
            l
        }
    };
    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((stream, addr))  => {
                        stream.set_nodelay(true).ok();
                        ACTIVE_CONNS.fetch_add(1, Ordering::Relaxed);
                        handle_connection(stream, addr, &limiter, key, false).await;
                    }
                    Err(err) => {
                       log::error!("listener.accept failed: {}", err);
                       break;
                    }
                }
            }
            res = listener2.accept() => {
                match res {
                    Ok((stream, addr))  => {
                        stream.set_nodelay(true).ok();
                        ACTIVE_CONNS.fetch_add(1, Ordering::Relaxed);
                        handle_connection(stream, addr, &limiter, key, true).await;
                    }
                    Err(err) => {
                       log::error!("listener2.accept failed: {}", err);
                       break;
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    limiter: &Limiter,
    key: &str,
    ws: bool,
) {
    let ip = hbb_common::try_into_v4(addr).ip();
    // Peek first byte to detect hbbs status query (sends 0x00).
    // This reuses the main relay port instead of requiring a separate port.
    if !ws {
        let mut buf = [0u8; 1];
        if let Ok(Ok(_)) = hbb_common::timeout(100, stream.peek(&mut buf)).await {
            if buf[0] == 0x00 {
                let count = ACTIVE_CONNS.load(Ordering::Relaxed);
                let resp = format!("{{\"connections\":{}}}", count);
                tokio::spawn(async move {
                    let _ = stream.try_write(resp.as_bytes());
                });
                ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
                return;
            }
        }
    }
    if !ws && ip.is_loopback() {
        let limiter = limiter.clone();
        tokio::spawn(async move {
            let mut stream = stream;
            let mut buffer = [0; 1024];
            if let Ok(Ok(n)) = timeout(1000, stream.read(&mut buffer[..])).await {
                if let Ok(data) = std::str::from_utf8(&buffer[..n]) {
                    let res = check_cmd(data, limiter).await;
                    stream.write(res.as_bytes()).await.ok();
                }
            }
            ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
        });
        return;
    }
    let ip = ip.to_string();
    if BLOCKLIST.read().await.get(&ip).is_some() {
        log::info!("{} blocked", ip);
        ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
        return;
    }
    let key = key.to_owned();
    let limiter = limiter.clone();
    tokio::spawn(async move {
        allow_err!(make_pair(stream, addr, &key, limiter, ws).await);
        ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
    });
}

async fn make_pair(
    stream: TcpStream,
    mut addr: SocketAddr,
    key: &str,
    limiter: Limiter,
    ws: bool,
) -> ResultType<()> {
    if ws {
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
        let callback = |req: &Request, response: Response| {
            let headers = req.headers();
            let real_ip = headers
                .get("X-Real-IP")
                .or_else(|| headers.get("X-Forwarded-For"))
                .and_then(|header_value| header_value.to_str().ok());
            if let Some(ip) = real_ip {
                if ip.contains('.') {
                    addr = format!("{ip}:0").parse().unwrap_or(addr);
                } else {
                    addr = format!("[{ip}]:0").parse().unwrap_or(addr);
                }
            }
            Ok(response)
        };
        let ws_stream = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
        make_pair_(ws_stream, addr, key, limiter).await;
    } else {
        make_pair_(FramedStream::from(stream, addr), addr, key, limiter).await;
    }
    Ok(())
}

async fn make_pair_(stream: impl StreamTrait, addr: SocketAddr, key: &str, limiter: Limiter) {
    let mut stream = stream;
    if let Ok(Some(Ok(bytes))) = timeout(30_000, stream.recv()).await {
        if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
            if let Some(rendezvous_message::Union::RequestRelay(rf)) = msg_in.union {
                if !key.is_empty() && rf.licence_key != key {
                    return;
                }
                if !rf.uuid.is_empty() {
                    let mut peer = PEERS.lock().await.remove(&rf.uuid);
                    if let Some(peer) = peer.as_mut() {
                        log::info!("Relayrequest {} from {} got paired", rf.uuid, addr);
                        let id = format!("{}:{}", addr.ip(), addr.port());
                        USAGE.write().await.insert(id.clone(), Default::default());
                        if !stream.is_ws() && !peer.is_ws() {
                            peer.set_raw();
                            stream.set_raw();
                            log::info!("Both are raw");
                        }
                        if let Err(err) = relay(addr, &mut stream, peer, limiter, id.clone()).await
                        {
                            log::info!("Relay of {} closed: {}", addr, err);
                        } else {
                            log::info!("Relay of {} closed", addr);
                        }
                        USAGE.write().await.remove(&id);
                    } else {
                        log::info!("New relay request {} from {}", rf.uuid, addr);
                        PEERS.lock().await.insert(rf.uuid.clone(), Box::new(stream));
                        sleep(30.).await;
                        PEERS.lock().await.remove(&rf.uuid);
                    }
                }
            }
        }
    }
}

/// 带超时的单向转发。
///
/// 接收方不读时 `send_raw` 会永久挂起（见 [`RELAY_WRITE_TIMEOUT_MS`]），
/// 这里用超时把它变成一次普通错误，让 [`relay`] 尽快结束、两端 socket 一起关闭。
async fn send_with_timeout(
    stream: &mut (dyn StreamTrait + '_),
    bytes: Bytes,
    to: &str,
) -> ResultType<()> {
    match timeout(RELAY_WRITE_TIMEOUT_MS, stream.send_raw(bytes)).await {
        Ok(res) => res,
        Err(_) => bail!(
            "send to {} timeout ({}s), peer is not reading",
            to,
            RELAY_WRITE_TIMEOUT_MS / 1000
        ),
    }
}

async fn relay(
    addr: SocketAddr,
    stream: &mut impl StreamTrait,
    peer: &mut Box<dyn StreamTrait>,
    total_limiter: Limiter,
    id: String,
) -> ResultType<()> {
    let ip = addr.ip().to_string();
    let mut tm = std::time::Instant::now();
    let mut elapsed = 0;
    let mut total = 0;
    let mut total_s = 0;
    let mut highest_s = 0;
    let mut downgrade: bool = false;
    let mut blacked: bool = false;
    // 单连接 / 黑名单 / 降级限速都在连接建立时读取原子量：
    // api-server 的热更新**只对新连接生效**，已建立的中继连接不会中途改速。
    let sb = SINGLE_BANDWIDTH.load(Ordering::Relaxed);
    let limiter = <Limiter>::new(speed_limit(sb));
    let blacklist_limiter = <Limiter>::new(speed_limit(LIMIT_SPEED.load(Ordering::Relaxed)));
    // 自动降级用固定惩罚带宽，不复用 LIMIT_SPEED（见 DOWNGRADE_LIMIT_SPEED 的说明）
    let downgrade_limiter = <Limiter>::new(DOWNGRADE_LIMIT_SPEED as f64);
    // 阈值单位 bit/ms：单连接不限速（sb=0）时没有比较基准，直接关闭自动降级，
    // 避免把"不限制"当成 0 阈值导致所有连接一上来就被降级。
    let downgrade_threshold = if sb == 0 {
        0
    } else {
        (sb as f64 * DOWNGRADE_THRESHOLD_100.load(Ordering::Relaxed) as f64 / 100. / 1000.) as usize
    };
    let mut timer = interval(Duration::from_secs(3));
    let mut last_recv_time = std::time::Instant::now();
    loop {
        tokio::select! {
            res = peer.recv() => {
                if let Some(Ok(bytes)) = res {
                    last_recv_time = std::time::Instant::now();
                    let len = bytes.len();
                    let nb = len * 8;
                    // 方向：peer → client（对端发回客户端），记入向/出向累计字节
                    RELAY_BYTES_OUT.fetch_add(len as u64, Ordering::Relaxed);
                    if blacked {
                        blacklist_limiter.consume(nb).await;
                    } else if downgrade {
                        downgrade_limiter.consume(nb).await;
                    } else {
                        limiter.consume(nb).await;
                    }
                    total_limiter.consume(nb).await;
                    total += nb;
                    total_s += nb;
                    if !bytes.is_empty() {
                        send_with_timeout(&mut *stream, bytes.into(), "client").await?;
                    }
                } else {
                    break;
                }
            },
            res = stream.recv() => {
                if let Some(Ok(bytes)) = res {
                    last_recv_time = std::time::Instant::now();
                    let len = bytes.len();
                    let nb = len * 8;
                    // 方向：client → peer（客户端发往对端）
                    RELAY_BYTES_IN.fetch_add(len as u64, Ordering::Relaxed);
                    if blacked {
                        blacklist_limiter.consume(nb).await;
                    } else if downgrade {
                        downgrade_limiter.consume(nb).await;
                    } else {
                        limiter.consume(nb).await;
                    }
                    total_limiter.consume(nb).await;
                    total += nb;
                    total_s += nb;
                    if !bytes.is_empty() {
                        send_with_timeout(&mut **peer, bytes.into(), "peer").await?;
                    }
                } else {
                    break;
                }
            },
            _ = timer.tick() => {
                if last_recv_time.elapsed().as_secs() > 30 {
                    bail!("Timeout");
                }
            }
        }

        let n = tm.elapsed().as_millis() as usize;
        if n >= 1_000 {
            if BLOCKLIST.read().await.get(&ip).is_some() {
                log::info!("{} blocked", ip);
                break;
            }
            blacked = BLACKLIST.read().await.get(&ip).is_some();
            tm = std::time::Instant::now();
            let speed = total_s / n;
            if speed > highest_s {
                highest_s = speed;
            }
            elapsed += n;
            USAGE.write().await.insert(
                id.clone(),
                (elapsed as _, total as _, highest_s as _, speed as _),
            );
            total_s = 0;
            // 降级判定：阈值 0（关闭）或检查窗口 0（关闭）时不做降级；
            // saturating_mul 防止阈值/耗时很大时 usize 溢出回绕成假触发。
            let check_ms = DOWNGRADE_START_CHECK.load(Ordering::Relaxed);
            if check_ms > 0
                && downgrade_threshold > 0
                && !downgrade
                && elapsed > check_ms
                && total > elapsed.saturating_mul(downgrade_threshold)
            {
                downgrade = true;
                log::info!(
                    "Downgrade {}, exceed downgrade threshold {}bit/ms in {}ms (penalty {}Mb/s)",
                    id,
                    downgrade_threshold,
                    elapsed,
                    DOWNGRADE_LIMIT_SPEED / 1024 / 1024
                );
            }
        }
    }
    Ok(())
}

fn get_server_sk(key: &str) -> String {
    let mut key = key.to_owned();
    if let Ok(sk) = base64::decode(&key) {
        if sk.len() == sign::SECRETKEYBYTES {
            log::info!("The key is a crypto private key");
            key = base64::encode(&sk[(sign::SECRETKEYBYTES / 2)..]);
        }
    }

    if key == "-" || key == "_" {
        let (pk, _) = crate::common::gen_sk(300);
        key = pk;
    }

    if !key.is_empty() {
        log::info!("Key: {}", key);
    }

    key
}

#[async_trait]
trait StreamTrait: Send + Sync + 'static {
    async fn recv(&mut self) -> Option<Result<BytesMut, Error>>;
    async fn send_raw(&mut self, bytes: Bytes) -> ResultType<()>;
    fn is_ws(&self) -> bool;
    fn set_raw(&mut self);
}

#[async_trait]
impl StreamTrait for FramedStream {
    async fn recv(&mut self) -> Option<Result<BytesMut, Error>> {
        self.next().await
    }

    async fn send_raw(&mut self, bytes: Bytes) -> ResultType<()> {
        self.send_bytes(bytes).await
    }

    fn is_ws(&self) -> bool {
        false
    }

    fn set_raw(&mut self) {
        self.set_raw();
    }
}

#[async_trait]
impl StreamTrait for tokio_tungstenite::WebSocketStream<TcpStream> {
    async fn recv(&mut self) -> Option<Result<BytesMut, Error>> {
        if let Some(msg) = self.next().await {
            match msg {
                Ok(msg) => {
                    match msg {
                        tungstenite::Message::Binary(bytes) => {
                            Some(Ok(bytes[..].into())) // to-do: poor performance
                        }
                        _ => Some(Ok(BytesMut::new())),
                    }
                }
                Err(err) => Some(Err(Error::new(std::io::ErrorKind::Other, err.to_string()))),
            }
        } else {
            None
        }
    }

    async fn send_raw(&mut self, bytes: Bytes) -> ResultType<()> {
        Ok(self
            .send(tungstenite::Message::Binary(bytes.to_vec()))
            .await?) // to-do: poor performance
    }

    fn is_ws(&self) -> bool {
        true
    }

    fn set_raw(&mut self) {}
}
