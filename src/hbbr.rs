use clap::App;
mod common;
mod relay_server;
use flexi_logger::*;
use hbb_common::{config::RELAY_PORT, ResultType};
use relay_server::*;
mod node_agent;
mod version;

fn main() -> ResultType<()> {
    let _logger = Logger::try_with_env_or_str("info")?
        .log_to_stdout()
        .format(opt_format)
        .write_mode(WriteMode::Async)
        .start()?;
    let args = format!(
        "-p, --port=[NUMBER(default={RELAY_PORT})] 'Sets the listening port'
        -k, --key=[KEY] 'Only allow the client with the same key'
        ",
    );
    let matches = App::new("hbbr")
        .version(version::VERSION)
        .author("Purslane Ltd. <info@rustdesk.com>")
        .about("RustDesk Relay Server")
        .args_from_usage(&args)
        .get_matches();
    // 集群 agent：注册到 api-server 并定期上报活跃连接数
    // 注册限速热更新回调，使 api-server 下发的限速配置能立即生效
    node_agent::set_limits_apply(|limit, single, total, threshold, check_ms| {
        relay_server::apply_limits(limit, single, total, threshold, check_ms);
    });
    node_agent::start("hbbr", "127.0.0.1:21117");
    if let Ok(v) = ini::Ini::load_from_file(".env") {
        if let Some(section) = v.section(None::<String>) {
            section.iter().for_each(|(k, v)| std::env::set_var(k, v));
        }
    }
    let mut port = RELAY_PORT;
    if let Ok(v) = std::env::var("PORT") {
        let v: i32 = v.parse().unwrap_or_default();
        if v > 0 {
            port = v + 1;
        }
    }
    start(
        matches.value_of("port").unwrap_or(&port.to_string()),
        matches
            .value_of("key")
            .unwrap_or(&std::env::var("KEY").unwrap_or_default()),
    )?;
    Ok(())
}
