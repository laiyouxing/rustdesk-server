//! 生成 `src/version.rs`（`VERSION` / `BUILD_DATE`），供 `--version`、
//! 节点上报与日志使用。
//!
//! 版本号的**唯一来源是 git tag**。Cargo.toml 的 `package.version` 长期没跟上
//! tag（停在 1.1.16，而 tag 已经到 1.1.25），历史上只有 docker-publish 工作流
//! 用 `sed` 临时改写 Cargo.toml，于是同一份代码在本地构建、在其它工作流里编出来
//! 的二进制 `--version` 与节点上报的 `version` 都还是 1.1.16，和 Release 错位。
//!
//! 取法：`git describe --tags --long` 找最近的版本 tag
//!   - 正好落在 tag 上    → `1.1.25`
//!   - tag 之后 N 个提交  → `1.1.25-N`
//!     （`N` 会被 `hbb_common::get_version_number` 当作补丁号，排序仍大于 1.1.25）
//!
//! 没有 `.git` 时（源码包、docker 构建上下文）回落到 Cargo.toml 的 `version`；
//! 显式设置了 `BUILD_VERSION` 环境变量则以其为准（CI 可强制指定）。

use std::process::Command;

fn main() {
    // tag / 分支切换后重新生成。CI 是全新 checkout，Cargo.toml 的 mtime 一定会变，
    // 通常已能触发重跑；这里再盯一下 HEAD，覆盖「本地打 tag 后直接构建」的情况。
    println!("cargo:rerun-if-changed=.git/HEAD");

    if std::env::var_os("BUILD_VERSION").is_none() {
        if let Some(v) = version_from_git() {
            std::env::set_var("BUILD_VERSION", v);
        }
    }
    hbb_common::gen_version();
}

/// 从 git tag 推导版本号。取不到（无 .git / 无匹配 tag / git 不可用）时返回 None，
/// 由 `gen_version` 回落到 Cargo.toml。
fn version_from_git() -> Option<String> {
    let out = Command::new("git")
        .args([
            "describe", "--tags", "--long", "--match", "[0-9]*", "--match", "v[0-9]*",
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let desc = String::from_utf8(out.stdout).ok()?;
    let desc = desc.trim();
    if desc.is_empty() {
        return None;
    }

    // "1.1.25-0-g4b18d4d" -> ["1.1.25", "0", "g4b18d4d"]
    let mut parts: Vec<&str> = desc.split('-').collect();
    // 丢掉结尾的 -g<hash>：版本号里不需要提交号
    if parts.len() >= 2 && parts[parts.len() - 1].starts_with('g') {
        parts.pop();
    }
    // 丢掉 "-0"：正好在 tag 上时没有补丁号
    if parts.len() >= 2 && parts[parts.len() - 1] == "0" {
        parts.pop();
    }

    let mut v = parts.join("-");
    if let Some(stripped) = v.strip_prefix('v') {
        v = stripped.to_owned();
    }
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}
