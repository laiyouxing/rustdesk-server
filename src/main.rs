// https://tools.ietf.org/rfc/rfc5128.txt
// https://blog.csdn.net/bytxl/article/details/44344855

use flexi_logger::*;
use hbb_common::{bail, config::RENDEZVOUS_PORT, ResultType};
use hbbs::{common::*, *};

const RMEM: usize = 0;

fn main() -> ResultType<()> {
    let _logger = Logger::try_with_env_or_str("info")?
        .log_to_stdout()
        .format(opt_format)
        .write_mode(WriteMode::Async)
        .start()?;
    let args = format!(
        "-c --config=[FILE] +takes_value 'Sets a custom config file'
        -p, --port=[NUMBER(default={RENDEZVOUS_PORT})] 'Sets the listening port'
        -s, --serial=[NUMBER(default=0)] 'Sets configure update serial number'
        -R, --rendezvous-servers=[HOSTS] 'Sets rendezvous servers, separated by comma'
        -u, --software-url=[URL] 'Sets download url of RustDesk software of newest version'
        -r, --relay-servers=[HOST] 'Sets the default relay servers, separated by comma'
        -M, --rmem=[NUMBER(default={RMEM})] 'Sets UDP recv buffer size, set system rmem_max first, e.g., sudo sysctl -w net.core.rmem_max=52428800. vi /etc/sysctl.conf, net.core.rmem_max=52428800, sudo sysctl –p'
        , --mask=[MASK] 'Determine if the connection comes from LAN, comma-separated for multiple subnets, e.g. 192.168.0.0/16,10.0.0.0/8'
        -k, --key=[KEY] 'Only allow the client with the same key'
        , --must-login=[Y|N] 'Only allow the client with login'",
    );
    init_args(&args, "hbbs", "RustDesk ID/Rendezvous Server");
    // 集群 agent：注册到 api-server 并定期上报；同时注册中继列表热更新回调
    node_agent::set_relay_apply(rendezvous_server::apply_relay_list);
    // 在线 peer 数由 RendezvousServer 维护（io_loop 每 3s 刷新），agent 心跳实时取值。
    // 原实现的 ONLINE_PEERS 原子量从未被写入，peers 恒为 0。
    node_agent::set_peers_getter(rendezvous_server::online_peers);
    node_agent::start("hbbs");
    let port = get_arg_or("port", RENDEZVOUS_PORT.to_string()).parse::<i32>()?;
    if port < 3 {
        bail!("Invalid port");
    }
    let rmem = get_arg("rmem").parse::<usize>().unwrap_or(RMEM);
    let serial: i32 = get_arg("serial").parse().unwrap_or(0);
    crate::common::check_software_update();
    RendezvousServer::start(port, serial, &get_arg_or("key", "-".to_owned()), rmem)?;
    Ok(())
}
