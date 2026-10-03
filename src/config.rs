//! 配置模型：单文件 TOML，支持在线热加载（改动文件即生效，无需重启）。
//!
//! * `[[listeners]]` —— 对外统一入口（TCP / UDP 可以各配多个）。
//! * `[[nodes]]`     —— 后端候选节点，`priority` 越大越优先，形成主/备关系。

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

/// 内置示例配置（与仓库根目录 `config.example.toml` 始终同步）
pub const EXAMPLE_CONFIG: &str = include_str!("../config.example.toml");

/// 对外/对内的传输协议
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        }
    }

    pub fn parse_loose(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "tcp" => Ok(Protocol::Tcp),
            "udp" => Ok(Protocol::Udp),
            other => bail!("未知协议: {other}（应为 tcp 或 udp）"),
        }
    }
}

/// 多节点调度策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    /// 主备：priority 最大的健康节点为主，其余为备
    Priority,
    /// 多活：健康节点轮询
    RoundRobin,
    /// 多活：按 weight 加权轮询
    Weighted,
}

impl Strategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Strategy::Priority => "priority",
            Strategy::RoundRobin => "round_robin",
            Strategy::Weighted => "weighted",
        }
    }
}

/// 嗅探（健康探测）方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProbeKind {
    /// 有 tcp 就探 tcp，否则探 udp
    Auto,
    Tcp,
    Udp,
    /// 不主动探测，只靠被动失败摘除
    None,
}

// ---------------------------------------------------------------------------
// 全局
// ---------------------------------------------------------------------------

fn d_true() -> bool {
    true
}
fn d_log_level() -> String {
    "info".to_string()
}
fn d_strategy() -> Strategy {
    Strategy::Priority
}
fn d_admin() -> String {
    "127.0.0.1:9100".to_string()
}
fn d_poll() -> u64 {
    1000
}
fn d_probe_interval() -> u64 {
    1000
}
fn d_probe_timeout() -> u64 {
    800
}
fn d_connect_timeout() -> u64 {
    3000
}
fn d_failure_threshold() -> u32 {
    3
}
fn d_success_threshold() -> u32 {
    2
}
fn d_connect_retry() -> u32 {
    3
}
fn d_passive_threshold() -> u32 {
    3
}
fn d_passive_window() -> u64 {
    5000
}
fn d_drain() -> u64 {
    10_000
}
fn d_udp_idle() -> u64 {
    60_000
}
fn d_shutdown_grace() -> u64 {
    5000
}
fn d_priority() -> i32 {
    0
}
fn d_weight() -> u32 {
    1
}
fn d_probe_kind() -> ProbeKind {
    ProbeKind::Auto
}
fn d_empty() -> String {
    String::new()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    #[serde(default = "d_log_level")]
    pub log_level: String,
    #[serde(default = "d_strategy")]
    pub strategy: Strategy,
    #[serde(default = "d_admin")]
    pub admin_listen: String,
    #[serde(default = "d_poll")]
    pub config_poll_interval_ms: u64,
    #[serde(default = "d_probe_interval")]
    pub probe_interval_ms: u64,
    #[serde(default = "d_probe_timeout")]
    pub probe_timeout_ms: u64,
    #[serde(default = "d_connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "d_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "d_success_threshold")]
    pub success_threshold: u32,
    #[serde(default = "d_connect_retry")]
    pub connect_retry: u32,
    #[serde(default = "d_passive_threshold")]
    pub passive_eject_threshold: u32,
    #[serde(default = "d_passive_window")]
    pub passive_eject_window_ms: u64,
    #[serde(default = "d_drain")]
    pub drain_timeout_ms: u64,
    #[serde(default = "d_udp_idle")]
    pub udp_session_timeout_ms: u64,
    #[serde(default = "d_true")]
    pub udp_rebind_on_switch: bool,
    #[serde(default = "d_true")]
    pub fail_open: bool,
    #[serde(default = "d_shutdown_grace")]
    pub shutdown_grace_ms: u64,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            log_level: d_log_level(),
            strategy: d_strategy(),
            admin_listen: d_admin(),
            config_poll_interval_ms: d_poll(),
            probe_interval_ms: d_probe_interval(),
            probe_timeout_ms: d_probe_timeout(),
            connect_timeout_ms: d_connect_timeout(),
            failure_threshold: d_failure_threshold(),
            success_threshold: d_success_threshold(),
            connect_retry: d_connect_retry(),
            passive_eject_threshold: d_passive_threshold(),
            passive_eject_window_ms: d_passive_window(),
            drain_timeout_ms: d_drain(),
            udp_session_timeout_ms: d_udp_idle(),
            udp_rebind_on_switch: true,
            fail_open: true,
            shutdown_grace_ms: d_shutdown_grace(),
        }
    }
}

/// 全局配置的运行时视图（毫秒 → `Duration`）
#[derive(Debug, Clone)]
pub struct Tuning {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub connect_timeout: Duration,
    pub connect_retry: u32,
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub passive_eject_threshold: u32,
    pub passive_eject_window: Duration,
    pub udp_session_timeout: Duration,
    pub udp_rebind_on_switch: bool,
    pub shutdown_grace: Duration,
    pub config_poll_interval: Duration,
}

impl From<&GlobalConfig> for Tuning {
    fn from(g: &GlobalConfig) -> Self {
        Self {
            probe_interval: Duration::from_millis(g.probe_interval_ms.max(1)),
            probe_timeout: Duration::from_millis(g.probe_timeout_ms.max(1)),
            connect_timeout: Duration::from_millis(g.connect_timeout_ms.max(1)),
            connect_retry: g.connect_retry.max(1),
            failure_threshold: g.failure_threshold.max(1),
            success_threshold: g.success_threshold.max(1),
            passive_eject_threshold: g.passive_eject_threshold.max(1),
            passive_eject_window: Duration::from_millis(g.passive_eject_window_ms.max(1)),
            udp_session_timeout: Duration::from_millis(g.udp_session_timeout_ms.max(1)),
            udp_rebind_on_switch: g.udp_rebind_on_switch,
            shutdown_grace: Duration::from_millis(g.shutdown_grace_ms),
            config_poll_interval: Duration::from_millis(g.config_poll_interval_ms.max(50)),
        }
    }
}

// ---------------------------------------------------------------------------
// 入口 / 节点
// ---------------------------------------------------------------------------

/// 对外统一入口
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerConfig {
    /// 入口名称（唯一，用于日志与控制口）
    pub name: String,
    pub protocol: Protocol,
    /// 监听地址，如 `0.0.0.0:9000`、`[::]:9000`
    pub listen: String,
}

/// 后端节点
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// 节点名（唯一）
    pub name: String,
    #[serde(default = "d_true")]
    pub enabled: bool,
    /// 越大越优先
    #[serde(default = "d_priority")]
    pub priority: i32,
    /// weighted 策略下的权重
    #[serde(default = "d_weight")]
    pub weight: u32,
    #[serde(default)]
    pub tcp: Option<String>,
    #[serde(default)]
    pub udp: Option<String>,
    #[serde(default = "d_probe_kind")]
    pub probe: ProbeKind,
    /// 探测载荷：普通文本；`hex:` 前缀表示十六进制字节串
    #[serde(default = "d_empty")]
    pub probe_payload: String,
    /// 期望响应中包含的字符串（空 = 任何响应/仅连接成功即算通）
    #[serde(default = "d_empty")]
    pub probe_expect: String,
    /// 覆盖全局嗅探间隔（毫秒）
    #[serde(default)]
    pub probe_interval_ms: Option<u64>,
    #[serde(default = "d_empty")]
    pub remark: String,
}

impl NodeConfig {
    pub fn decode_payload(&self) -> Result<Vec<u8>> {
        let raw = &self.probe_payload;
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        match raw.trim().strip_prefix("hex:") {
            // 普通文本：原样使用（保留 \r\n 等控制字符，便于探测行协议）
            None => Ok(raw.as_bytes().to_vec()),
            Some(hex) => {
                let hex: String = hex
                    .chars()
                    .filter(|c| !matches!(c, ' ' | '_' | '-' | ':'))
                    .collect();
                if hex.len() % 2 != 0 {
                    bail!("probe_payload 的 hex 部分长度必须为偶数: {hex}");
                }
                let bytes = hex.as_bytes();
                let mut out = Vec::with_capacity(bytes.len() / 2);
                for i in (0..bytes.len()).step_by(2) {
                    let hi = (bytes[i] as char)
                        .to_digit(16)
                        .context("probe_payload hex 非法字符")?;
                    let lo = (bytes[i + 1] as char)
                        .to_digit(16)
                        .context("probe_payload hex 非法字符")?;
                    out.push((hi * 16 + lo) as u8);
                }
                Ok(out)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 顶层
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub listeners: Vec<ListenerConfig>,
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
}

impl Config {
    /// 读取 + 解析 + 校验
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("解析 TOML 失败: {}", path.display()))?;
        cfg.validate()
            .with_context(|| format!("配置校验失败: {}", path.display()))?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.global.log_level.as_str(),
            "trace" | "debug" | "info" | "warn" | "error"
        ) {
            bail!(
                "global.log_level 非法: {}（可选 trace/debug/info/warn/error）",
                self.global.log_level
            );
        }
        if self.global.probe_interval_ms == 0
            || self.global.probe_timeout_ms == 0
            || self.global.connect_timeout_ms == 0
            || self.global.udp_session_timeout_ms == 0
        {
            bail!("global 中的探测间隔 / 超时不能为 0");
        }
        if self.global.failure_threshold == 0 || self.global.success_threshold == 0 {
            bail!("global.failure_threshold / success_threshold 必须 >= 1");
        }

        if self.listeners.is_empty() {
            bail!("至少需要一个 [[listeners]] 对外入口");
        }
        let mut lnames = HashSet::new();
        let mut laddrs = HashSet::new();
        for l in &self.listeners {
            if l.name.trim().is_empty() {
                bail!("listener.name 不能为空");
            }
            if !lnames.insert(l.name.clone()) {
                bail!("listener 名称重复: {}", l.name);
            }
            if l.listen.trim().is_empty() {
                bail!("listener {} 缺少 listen 地址", l.name);
            }
            if !laddrs.insert((l.protocol, l.listen.clone())) {
                bail!("listener 地址重复: {} {}", l.protocol.as_str(), l.listen);
            }
        }

        if self.nodes.is_empty() {
            bail!("至少需要一个 [[nodes]] 后端节点");
        }
        let mut nnames = HashSet::new();
        for n in &self.nodes {
            if n.name.trim().is_empty() {
                bail!("node.name 不能为空");
            }
            if !nnames.insert(n.name.clone()) {
                bail!("node 名称重复: {}", n.name);
            }
            if n.tcp.is_none() && n.udp.is_none() {
                bail!("node {} 至少需要配置 tcp 或 udp 之一", n.name);
            }
            if n.weight == 0 {
                bail!("node {} 的 weight 必须 >= 1", n.name);
            }
            if n.probe == ProbeKind::Tcp && n.tcp.is_none() {
                bail!("node {} probe = \"tcp\" 但未配置 tcp 地址", n.name);
            }
            if n.probe == ProbeKind::Udp && n.udp.is_none() {
                bail!("node {} probe = \"udp\" 但未配置 udp 地址", n.name);
            }
            n.decode_payload()
                .with_context(|| format!("node {} 的 probe_payload 非法", n.name))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_is_valid() {
        let cfg: Config = toml::from_str(EXAMPLE_CONFIG).expect("示例配置应能解析");
        cfg.validate().expect("示例配置应通过校验");
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.listeners.len(), 2);
        assert_eq!(cfg.global.strategy, Strategy::Priority);
    }

    #[test]
    fn payload_decoding() {
        let mut n: NodeConfig = toml::from_str(
            r#"
name = "x"
tcp = "127.0.0.1:1"
probe_payload = "hex:0d0a 41"
"#,
        )
        .unwrap();
        assert_eq!(n.decode_payload().unwrap(), vec![0x0d, 0x0a, 0x41]);
        n.probe_payload = "PING\r\n".to_string();
        assert_eq!(n.decode_payload().unwrap(), b"PING\r\n");
        n.probe_payload = "hex:ABC".to_string();
        assert!(n.decode_payload().is_err());
    }

    #[test]
    fn validation_rules() {
        let mut cfg: Config = toml::from_str(EXAMPLE_CONFIG).unwrap();
        cfg.nodes[1].name = cfg.nodes[0].name.clone();
        assert!(cfg.validate().is_err(), "重名节点应被拒绝");

        let mut cfg: Config = toml::from_str(EXAMPLE_CONFIG).unwrap();
        cfg.listeners.push(cfg.listeners[0].clone());
        assert!(cfg.validate().is_err(), "重复入口应被拒绝");

        let mut cfg: Config = toml::from_str(EXAMPLE_CONFIG).unwrap();
        cfg.nodes.clear();
        assert!(cfg.validate().is_err(), "无节点应被拒绝");
    }

    #[test]
    fn rejects_unknown_field() {
        let bad = r#"
[[listeners]]
name = "l"
protocol = "tcp"
listen = "127.0.0.1:1"

[[nodes]]
name = "n"
tcp = "127.0.0.1:2"
priorty = 10
"#;
        assert!(toml::from_str::<Config>(bad).is_err(), "拼错字段名应报错");
    }
}
