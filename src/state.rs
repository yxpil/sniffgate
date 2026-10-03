//! 运行时状态：节点视图、健康状态机、主备路由表与热切换事件。
//!
//! 设计要点
//! * 健康状态用原子量 + 细粒度锁维护；探测任务只做「写状态 + 触发重算」。
//! * 路由表是一份不可变快照（[`Runtime`]），通过 `ArcSwap` 无锁发布；
//!   代理任务每次接入连接时读取最新快照，因此切换对在途连接零阻塞。
//! * 热切换 = 重新计算每协议的「主」节点；被降级的节点进入排空（drain）状态，
//!   旧连接在 `drain_timeout_ms` 后关闭，新连接立刻走新的主节点。

use crate::config::{Config, GlobalConfig, ListenerConfig, NodeConfig, Protocol, Strategy, Tuning};
use arc_swap::ArcSwap;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// 当前 Unix 毫秒时间戳
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct NodeStats {
    pub tcp_conns_total: AtomicU64,
    pub tcp_conns_failed: AtomicU64,
    pub tcp_conns_active: AtomicU64,
    pub udp_sessions_total: AtomicU64,
    pub udp_sessions_active: AtomicU64,
    /// 客户端 → 后端 字节数
    pub bytes_tx: AtomicU64,
    /// 后端 → 客户端 字节数
    pub bytes_rx: AtomicU64,
    pub probe_ok: AtomicU64,
    pub probe_fail: AtomicU64,
    pub passive_ejects: AtomicU64,
    /// 被提升为主节点的次数
    pub promotions: AtomicU64,
    /// 排空到期被强制关闭的连接 / 会话数
    pub drain_kills: AtomicU64,
}

impl NodeStats {
    #[inline]
    pub fn inc(v: &AtomicU64) {
        v.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(v: &AtomicU64, n: u64) {
        v.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn dec(v: &AtomicU64) {
        v.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 健康状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthState {
    /// 启动后尚未得到探测结果（按可用处理，避免冷启动抖动）
    Unknown,
    Healthy,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthInfo {
    pub state: HealthState,
    pub consecutive_ok: u32,
    pub consecutive_fail: u32,
    pub last_probe_ms: u64,
    pub last_change_ms: u64,
    pub last_error: Option<String>,
    pub probe_latency_us: Option<u64>,
    /// 被动摘除窗口内的失败次数
    pub passive_fails: usize,
    #[serde(skip_serializing)]
    pub passive: VecDeque<u64>,
}

impl Default for HealthInfo {
    fn default() -> Self {
        Self {
            state: HealthState::Unknown,
            consecutive_ok: 0,
            consecutive_fail: 0,
            last_probe_ms: 0,
            last_change_ms: now_ms(),
            last_error: None,
            probe_latency_us: None,
            passive_fails: 0,
            passive: VecDeque::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// 节点句柄
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct NodeHandle {
    pub name: String,
    /// 节点配置（热加载时就地替换，不重建句柄 → 健康统计不丢）
    pub cfg: ArcSwap<NodeConfig>,
    pub health: Mutex<HealthInfo>,
    /// 健康标记（快路径原子读取）
    pub healthy: AtomicBool,
    /// 当前在途 TCP 连接数（priority 打平局时用）
    pub inflight: AtomicUsize,
    pub stats: NodeStats,
    /// 排空信号：0 = 正常；否则为「旧连接必须关闭」的 Unix 毫秒时刻
    drain_tx: watch::Sender<u64>,
    drain_rx: watch::Receiver<u64>,
    /// 节点被移除 / 进程退出时用于取消其嗅探任务
    pub cancel: CancellationToken,
}

impl NodeHandle {
    pub fn new(cfg: NodeConfig) -> Arc<Self> {
        let (drain_tx, drain_rx) = watch::channel(0u64);
        let name = cfg.name.clone();
        Arc::new(Self {
            name,
            cfg: ArcSwap::from_pointee(cfg),
            health: Mutex::new(HealthInfo::default()),
            // 未知状态按可用处理：冷启动立刻可以转发，探测到失败再降级
            healthy: AtomicBool::new(true),
            inflight: AtomicUsize::new(0),
            stats: NodeStats::default(),
            drain_tx,
            drain_rx,
            cancel: CancellationToken::new(),
        })
    }

    pub fn cfg(&self) -> Arc<NodeConfig> {
        self.cfg.load_full()
    }

    pub fn is_enabled(&self) -> bool {
        self.cfg.load().enabled
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    /// 该节点在指定协议上的后端地址
    pub fn endpoint(&self, proto: Protocol) -> Option<String> {
        let cfg = self.cfg.load();
        match proto {
            Protocol::Tcp => cfg.tcp.clone(),
            Protocol::Udp => cfg.udp.clone(),
        }
    }

    pub fn subscribe_drain(&self) -> watch::Receiver<u64> {
        self.drain_rx.clone()
    }

    pub fn drain_deadline_ms(&self) -> u64 {
        *self.drain_rx.borrow()
    }

    pub fn start_drain(&self, deadline_ms: u64) {
        let _ = self.drain_tx.send(deadline_ms);
    }

    pub fn stop_drain(&self) {
        let _ = self.drain_tx.send(0);
    }

    pub fn is_draining(&self) -> bool {
        self.drain_deadline_ms() > 0
    }

    /// 主动嗅探结果；返回健康状态是否发生变化
    pub fn record_probe(
        &self,
        ok: bool,
        latency_us: Option<u64>,
        err: Option<String>,
        t: &Tuning,
    ) -> bool {
        let mut h = self.health.lock().unwrap();
        h.last_probe_ms = now_ms();
        let mut changed = false;
        if ok {
            h.consecutive_ok = h.consecutive_ok.saturating_add(1);
            h.consecutive_fail = 0;
            h.last_error = None;
            h.passive.clear();
            h.passive_fails = 0;
            h.probe_latency_us = latency_us;
            if h.state != HealthState::Healthy && h.consecutive_ok >= t.success_threshold {
                h.state = HealthState::Healthy;
                h.last_change_ms = now_ms();
                self.healthy.store(true, Ordering::SeqCst);
                changed = true;
            }
            NodeStats::inc(&self.stats.probe_ok);
        } else {
            h.consecutive_fail = h.consecutive_fail.saturating_add(1);
            h.consecutive_ok = 0;
            h.last_error = err;
            h.probe_latency_us = None;
            if h.state != HealthState::Unhealthy && h.consecutive_fail >= t.failure_threshold {
                h.state = HealthState::Unhealthy;
                h.last_change_ms = now_ms();
                self.healthy.store(false, Ordering::SeqCst);
                changed = true;
            }
            NodeStats::inc(&self.stats.probe_fail);
        }
        changed
    }

    /// 被动嗅探：真实转发失败。窗口内达到阈值立即摘除；返回健康状态是否变化
    pub fn record_passive_failure(&self, err: String, t: &Tuning) -> bool {
        let now = now_ms();
        let window = t.passive_eject_window.as_millis() as u64;
        let mut h = self.health.lock().unwrap();
        h.passive.push_back(now);
        while let Some(front) = h.passive.front() {
            if now.saturating_sub(*front) > window {
                h.passive.pop_front();
            } else {
                break;
            }
        }
        h.passive_fails = h.passive.len();
        h.last_error = Some(err);
        if self.healthy.load(Ordering::SeqCst)
            && h.passive_fails as u32 >= t.passive_eject_threshold.max(1)
        {
            h.state = HealthState::Unhealthy;
            h.consecutive_ok = 0;
            h.consecutive_fail = h.passive_fails as u32;
            h.last_change_ms = now;
            self.healthy.store(false, Ordering::SeqCst);
            NodeStats::inc(&self.stats.passive_ejects);
            return true;
        }
        false
    }

    /// 运维手动标记健康 / 不健康
    pub fn force_health(&self, healthy: bool) {
        let mut h = self.health.lock().unwrap();
        h.state = if healthy {
            HealthState::Healthy
        } else {
            HealthState::Unhealthy
        };
        h.last_change_ms = now_ms();
        h.consecutive_ok = 0;
        h.consecutive_fail = 0;
        if healthy {
            h.passive.clear();
            h.passive_fails = 0;
            h.last_error = None;
        }
        self.healthy.store(healthy, Ordering::SeqCst);
    }

    pub fn health_snapshot(&self) -> HealthInfo {
        self.health.lock().unwrap().clone()
    }
}

// ---------------------------------------------------------------------------
// 路由表快照
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Runtime {
    /// 每次发布自增
    pub epoch: u64,
    pub nodes: Vec<Arc<NodeHandle>>,
    pub active_tcp: Option<Arc<NodeHandle>>,
    pub active_udp: Option<Arc<NodeHandle>>,
    pub global: GlobalConfig,
    pub listeners: Vec<ListenerConfig>,
    pub loaded_at_ms: u64,
}

impl Runtime {
    pub fn active(&self, proto: Protocol) -> Option<Arc<NodeHandle>> {
        match proto {
            Protocol::Tcp => self.active_tcp.clone(),
            Protocol::Udp => self.active_udp.clone(),
        }
    }

    pub fn node(&self, name: &str) -> Option<Arc<NodeHandle>> {
        self.nodes.iter().find(|n| n.name == name).cloned()
    }
}

/// 主备切换事件
#[derive(Debug, Clone, Serialize)]
pub struct SwitchEvent {
    pub epoch: u64,
    pub protocol: Protocol,
    pub from: Option<String>,
    pub to: Option<String>,
    pub reason: String,
    pub ts_ms: u64,
}

/// 配置热加载应用结果
#[derive(Debug, Default)]
pub struct ApplyReport {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub updated: Vec<String>,
}

// ---------------------------------------------------------------------------
// 目录（路由中枢）
// ---------------------------------------------------------------------------

pub struct Directory {
    rt: ArcSwap<Runtime>,
    events: broadcast::Sender<SwitchEvent>,
    rr: AtomicUsize,
    /// 串行化「发布路由表」这一写操作
    lock: Mutex<()>,
    /// 手动指定的主节点（控制口 switch），节点不可用时自动失效
    pins: Mutex<HashMap<Protocol, String>>,
}

impl Directory {
    pub fn new(cfg: &Config) -> Arc<Self> {
        let nodes: Vec<Arc<NodeHandle>> = cfg
            .nodes
            .iter()
            .map(|n| NodeHandle::new(n.clone()))
            .collect();
        let (events, _rx) = broadcast::channel(512);
        let dir = Arc::new(Self {
            rt: ArcSwap::from_pointee(Runtime {
                epoch: 1,
                nodes,
                active_tcp: None,
                active_udp: None,
                global: cfg.global.clone(),
                listeners: cfg.listeners.clone(),
                loaded_at_ms: now_ms(),
            }),
            events,
            rr: AtomicUsize::new(0),
            lock: Mutex::new(()),
            pins: Mutex::new(HashMap::new()),
        });
        dir.recompute("startup");
        dir
    }

    /// 该协议当前的主节点（来自最近一次发布的快照）
    pub fn active(&self, proto: Protocol) -> Option<Arc<NodeHandle>> {
        self.rt.load().active(proto)
    }

    pub fn snapshot(&self) -> Arc<Runtime> {
        self.rt.load_full()
    }

    pub fn events(&self) -> broadcast::Receiver<SwitchEvent> {
        self.events.subscribe()
    }

    pub fn tuning(&self) -> Tuning {
        Tuning::from(&self.rt.load().global)
    }

    pub fn pin_of(&self, proto: Protocol) -> Option<String> {
        self.pins.lock().unwrap().get(&proto).cloned()
    }

    // -- 选路 ---------------------------------------------------------------

    fn sort_priority(pool: &mut [Arc<NodeHandle>]) {
        pool.sort_by(|a, b| {
            let (ca, cb) = (a.cfg.load(), b.cfg.load());
            cb.priority
                .cmp(&ca.priority)
                .then_with(|| {
                    a.inflight
                        .load(Ordering::Relaxed)
                        .cmp(&b.inflight.load(Ordering::Relaxed))
                })
                .then_with(|| a.name.cmp(&b.name))
        });
    }

    /// 候选池：启用且有该协议地址；优先健康节点，全挂时按 fail_open 兜底
    fn pool(
        nodes: &[Arc<NodeHandle>],
        proto: Protocol,
        global: &GlobalConfig,
    ) -> Option<Vec<Arc<NodeHandle>>> {
        let eligible: Vec<Arc<NodeHandle>> = nodes
            .iter()
            .filter(|n| n.is_enabled() && n.endpoint(proto).is_some())
            .cloned()
            .collect();
        if eligible.is_empty() {
            return None;
        }
        let healthy: Vec<Arc<NodeHandle>> = eligible
            .iter()
            .filter(|n| n.is_healthy())
            .cloned()
            .collect();
        if healthy.is_empty() {
            if global.fail_open {
                Some(eligible)
            } else {
                None
            }
        } else {
            Some(healthy)
        }
    }

    /// 该协议当前的「主」节点（确定性结果：状态展示 / 切换判定 / UDP 会话重绑）
    fn principal(
        &self,
        nodes: &[Arc<NodeHandle>],
        proto: Protocol,
        global: &GlobalConfig,
    ) -> Option<Arc<NodeHandle>> {
        let mut pool = Self::pool(nodes, proto, global)?;
        let pinned = self.pins.lock().unwrap().get(&proto).cloned();
        if let Some(name) = pinned {
            if let Some(n) = pool.iter().find(|n| n.name == name) {
                return Some(n.clone());
            }
            // 手动指定的节点已不可用 → 自动解除并告警
            self.pins.lock().unwrap().remove(&proto);
            warn!(
                protocol = proto.as_str(),
                node = %name,
                "手动指定的主节点不可用，已自动恢复自动选主"
            );
        }
        Self::sort_priority(&mut pool);
        pool.into_iter().next()
    }

    /// 为一条新连接 / 新会话选择后端节点（按策略 + 手动指定 + 健康状态）
    pub fn select(&self, proto: Protocol, exclude: &[String]) -> Option<Arc<NodeHandle>> {
        let rt = self.rt.load();
        let eligible: Vec<Arc<NodeHandle>> = rt
            .nodes
            .iter()
            .filter(|n| {
                n.is_enabled()
                    && n.endpoint(proto).is_some()
                    && !exclude.iter().any(|e| e == &n.name)
            })
            .cloned()
            .collect();
        if eligible.is_empty() {
            return None;
        }
        let healthy: Vec<Arc<NodeHandle>> = eligible
            .iter()
            .filter(|n| n.is_healthy())
            .cloned()
            .collect();
        let mut pool = if healthy.is_empty() {
            if rt.global.fail_open {
                eligible
            } else {
                return None;
            }
        } else {
            healthy
        };

        // 正在排空（刚被降级）的节点不再承接新连接，除非没有别的选择
        let fresh: Vec<Arc<NodeHandle>> =
            pool.iter().filter(|n| !n.is_draining()).cloned().collect();
        if !fresh.is_empty() {
            pool = fresh;
        }

        if let Some(name) = self.pins.lock().unwrap().get(&proto).cloned() {
            if let Some(n) = pool.iter().find(|n| n.name == name) {
                return Some(n.clone());
            }
        }

        match rt.global.strategy {
            Strategy::Priority => {
                Self::sort_priority(&mut pool);
                pool.into_iter().next()
            }
            Strategy::RoundRobin => {
                pool.sort_by(|a, b| a.name.cmp(&b.name));
                let i = self.rr.fetch_add(1, Ordering::Relaxed) % pool.len();
                Some(pool.remove(i))
            }
            Strategy::Weighted => {
                pool.sort_by(|a, b| a.name.cmp(&b.name));
                let total: u64 = pool.iter().map(|n| n.cfg.load().weight.max(1) as u64).sum();
                let mut pos = self.rr.fetch_add(1, Ordering::Relaxed) as u64 % total.max(1);
                for n in &pool {
                    let w = n.cfg.load().weight.max(1) as u64;
                    if pos < w {
                        return Some(n.clone());
                    }
                    pos -= w;
                }
                pool.into_iter().next()
            }
        }
    }

    // -- 路由表发布 / 热切换 ------------------------------------------------

    /// 健康状态变化后重算主节点（无变化则什么都不做）
    pub fn recompute(&self, reason: &str) {
        let _g = self.lock.lock().unwrap();
        let cur = self.rt.load_full();
        self.publish(
            &cur.nodes,
            cur.global.clone(),
            &cur.listeners,
            reason,
            false,
        );
    }

    /// 应用新配置（热加载）：新增 / 删除 / 就地更新节点与入口
    pub fn apply(&self, cfg: &Config) -> ApplyReport {
        let _g = self.lock.lock().unwrap();
        let cur = self.rt.load_full();
        let mut report = ApplyReport::default();
        let mut nodes: Vec<Arc<NodeHandle>> = Vec::with_capacity(cfg.nodes.len());
        for nc in &cfg.nodes {
            match cur.nodes.iter().find(|n| n.name == nc.name) {
                Some(old) => {
                    let changed = old.cfg.load_full().as_ref() != nc;
                    old.cfg.store(Arc::new(nc.clone()));
                    if changed {
                        report.updated.push(nc.name.clone());
                    }
                    nodes.push(old.clone());
                }
                None => {
                    report.added.push(nc.name.clone());
                    nodes.push(NodeHandle::new(nc.clone()));
                }
            }
        }
        for old in cur.nodes.iter() {
            if !cfg.nodes.iter().any(|nc| nc.name == old.name) {
                report.removed.push(old.name.clone());
                old.cancel.cancel();
                old.stop_drain();
            }
        }
        self.publish(
            &nodes,
            cfg.global.clone(),
            &cfg.listeners,
            "config-reload",
            true,
        );
        report
    }

    /// 控制口：在线启用 / 停用节点（后续文件热加载会以文件内容为准）
    pub fn set_enabled(&self, name: &str, enabled: bool) -> anyhow::Result<()> {
        let cur = self.rt.load_full();
        let node = cur
            .node(name)
            .ok_or_else(|| anyhow::anyhow!("节点不存在: {name}"))?;
        let mut c = (*node.cfg.load_full()).clone();
        c.enabled = enabled;
        node.cfg.store(Arc::new(c));
        self.recompute(if enabled {
            "admin-enable"
        } else {
            "admin-disable"
        });
        Ok(())
    }

    /// 控制口：手动指定主节点（`node` 为 None 时恢复自动选主）
    pub fn force_active(&self, proto: Protocol, node: Option<&str>) -> anyhow::Result<()> {
        {
            let mut pins = self.pins.lock().unwrap();
            match node {
                Some(name) => {
                    let cur = self.rt.load_full();
                    let n = cur
                        .node(name)
                        .ok_or_else(|| anyhow::anyhow!("节点不存在: {name}"))?;
                    if n.endpoint(proto).is_none() {
                        anyhow::bail!("节点 {name} 未配置 {} 后端地址", proto.as_str());
                    }
                    if !n.is_enabled() {
                        anyhow::bail!("节点 {name} 当前处于停用状态");
                    }
                    pins.insert(proto, name.to_string());
                }
                None => {
                    pins.remove(&proto);
                }
            }
        }
        self.recompute("manual-switch");
        Ok(())
    }

    /// 控制口：手动标记节点健康状态（探针结果会随后续探测自动修正）
    pub fn set_health(&self, name: &str, healthy: bool) -> anyhow::Result<()> {
        let cur = self.rt.load_full();
        let node = cur
            .node(name)
            .ok_or_else(|| anyhow::anyhow!("节点不存在: {name}"))?;
        node.force_health(healthy);
        self.recompute("admin-health");
        Ok(())
    }

    fn publish(
        &self,
        nodes: &[Arc<NodeHandle>],
        global: GlobalConfig,
        listeners: &[ListenerConfig],
        reason: &str,
        force_epoch: bool,
    ) -> bool {
        let cur = self.rt.load_full();
        let prev_tcp = cur.active_tcp.as_ref().map(|n| n.name.clone());
        let prev_udp = cur.active_udp.as_ref().map(|n| n.name.clone());

        let tcp = self.principal(nodes, Protocol::Tcp, &global);
        let udp = self.principal(nodes, Protocol::Udp, &global);
        let tcp_name = tcp.as_ref().map(|n| n.name.clone());
        let udp_name = udp.as_ref().map(|n| n.name.clone());

        let same_tcp = prev_tcp == tcp_name;
        let same_udp = prev_udp == udp_name;
        if same_tcp && same_udp && !force_epoch {
            return false;
        }

        let epoch = cur.epoch + 1;
        let now = now_ms();

        let mut prev_names: HashSet<String> = HashSet::new();
        prev_names.extend(prev_tcp.iter().cloned());
        prev_names.extend(prev_udp.iter().cloned());
        let mut active_names: HashSet<String> = HashSet::new();
        active_names.extend(tcp_name.iter().cloned());
        active_names.extend(udp_name.iter().cloned());

        // 提升 → 取消排空；降级 → 进入排空
        for n in nodes {
            let was = prev_names.contains(&n.name);
            let is = active_names.contains(&n.name);
            if is && !was {
                if n.is_draining() {
                    n.stop_drain();
                }
                NodeStats::inc(&n.stats.promotions);
                info!(node = %n.name, epoch, reason, "节点提升为主节点");
            } else if was && !is {
                if global.drain_timeout_ms > 0 {
                    n.start_drain(now.saturating_add(global.drain_timeout_ms));
                    info!(
                        node = %n.name,
                        timeout_ms = global.drain_timeout_ms,
                        "节点被降级，进入排空（旧连接将在超时后关闭）"
                    );
                } else {
                    n.stop_drain();
                }
            } else if is && n.is_draining() {
                // 仍为主节点却被标记排空（例如刚被提升）→ 复位
                n.stop_drain();
            }
        }

        self.rt.store(Arc::new(Runtime {
            epoch,
            nodes: nodes.to_vec(),
            active_tcp: tcp.clone(),
            active_udp: udp.clone(),
            global,
            listeners: listeners.to_vec(),
            loaded_at_ms: now,
        }));

        if !same_tcp {
            let _ = self.events.send(SwitchEvent {
                epoch,
                protocol: Protocol::Tcp,
                from: prev_tcp,
                to: tcp_name,
                reason: reason.to_string(),
                ts_ms: now,
            });
        }
        if !same_udp {
            let _ = self.events.send(SwitchEvent {
                epoch,
                protocol: Protocol::Udp,
                from: prev_udp,
                to: udp_name,
                reason: reason.to_string(),
                ts_ms: now,
            });
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProbeKind;

    fn base_cfg(strategy: &str) -> Config {
        let text = format!(
            r#"
[global]
strategy = "{strategy}"
drain_timeout_ms = 5000

[[listeners]]
name = "tcp"
protocol = "tcp"
listen = "127.0.0.1:1"

[[nodes]]
name = "a"
priority = 100
tcp = "127.0.0.1:11"
udp = "127.0.0.1:11"

[[nodes]]
name = "b"
priority = 50
tcp = "127.0.0.1:12"
udp = "127.0.0.1:12"

[[nodes]]
name = "c"
priority = 10
tcp = "127.0.0.1:13"
"#
        );
        let cfg: Config = toml::from_str(&text).unwrap();
        cfg.validate().unwrap();
        cfg
    }

    #[test]
    fn priority_selects_highest() {
        let cfg = base_cfg("priority");
        let dir = Directory::new(&cfg);
        assert_eq!(dir.active(Protocol::Tcp).unwrap().name, "a");
        assert_eq!(dir.select(Protocol::Tcp, &[]).unwrap().name, "a");
        // 排除 a 后应选 b
        assert_eq!(
            dir.select(Protocol::Tcp, &["a".to_string()]).unwrap().name,
            "b"
        );
    }

    #[test]
    fn unhealthy_node_is_switched_away_and_drained() {
        let cfg = base_cfg("priority");
        let dir = Directory::new(&cfg);
        let a = dir.snapshot().node("a").unwrap();
        a.force_health(false);
        dir.recompute("test");
        assert_eq!(dir.active(Protocol::Tcp).unwrap().name, "b");
        assert!(a.is_draining(), "降级节点应进入排空状态");
        // 恢复后重新成为主节点并解除排空
        a.force_health(true);
        dir.recompute("test");
        assert_eq!(dir.active(Protocol::Tcp).unwrap().name, "a");
        assert!(!a.is_draining());
    }

    #[test]
    fn udp_only_node_is_usable_for_udp_only() {
        let text = r#"
[[listeners]]
name = "udp"
protocol = "udp"
listen = "127.0.0.1:1"

[[nodes]]
name = "only-udp"
udp = "127.0.0.1:13"

[[nodes]]
name = "only-tcp"
tcp = "127.0.0.1:14"
"#;
        let cfg: Config = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        let dir = Directory::new(&cfg);
        let u = dir.snapshot().node("only-udp").unwrap();
        assert!(u.endpoint(Protocol::Tcp).is_none());
        assert_eq!(dir.select(Protocol::Udp, &[]).unwrap().name, "only-udp");
        assert_eq!(dir.select(Protocol::Tcp, &[]).unwrap().name, "only-tcp");
        assert!(dir.select(Protocol::Udp, &["only-udp".into()]).is_none());
    }

    #[test]
    fn manual_switch_and_auto() {
        let cfg = base_cfg("priority");
        let dir = Directory::new(&cfg);
        dir.force_active(Protocol::Tcp, Some("b")).unwrap();
        assert_eq!(dir.active(Protocol::Tcp).unwrap().name, "b");
        assert_eq!(dir.select(Protocol::Tcp, &[]).unwrap().name, "b");
        dir.force_active(Protocol::Tcp, None).unwrap();
        assert_eq!(dir.active(Protocol::Tcp).unwrap().name, "a");
    }

    #[test]
    fn round_robin_uses_all_healthy_nodes() {
        let cfg = base_cfg("round_robin");
        let dir = Directory::new(&cfg);
        let mut seen = HashSet::new();
        for _ in 0..9 {
            seen.insert(dir.select(Protocol::Tcp, &[]).unwrap().name.clone());
        }
        assert_eq!(seen.len(), 3, "轮询应覆盖所有健康节点");
    }

    #[test]
    fn fail_open_and_closed() {
        let mut cfg = base_cfg("priority");
        cfg.global.fail_open = false;
        let dir = Directory::new(&cfg);
        for n in dir.snapshot().nodes.clone() {
            n.force_health(false);
        }
        dir.recompute("test");
        assert!(
            dir.select(Protocol::Tcp, &[]).is_none(),
            "fail-closed 应拒绝转发"
        );
        assert!(dir.active(Protocol::Tcp).is_none());

        let cfg2 = base_cfg("priority");
        let dir2 = Directory::new(&cfg2);
        for n in dir2.snapshot().nodes.clone() {
            n.force_health(false);
        }
        dir2.recompute("test");
        assert_eq!(
            dir2.select(Protocol::Tcp, &[]).unwrap().name,
            "a",
            "fail-open 应兜底按优先级转发"
        );
    }

    #[test]
    fn apply_config_adds_and_removes_nodes() {
        let mut cfg = base_cfg("priority");
        let dir = Directory::new(&cfg);
        cfg.nodes.retain(|n| n.name != "a");
        cfg.nodes.push(NodeConfig {
            name: "d".into(),
            enabled: true,
            priority: 1000,
            weight: 1,
            tcp: Some("127.0.0.1:14".into()),
            udp: Some("127.0.0.1:14".into()),
            probe: ProbeKind::Auto,
            probe_payload: String::new(),
            probe_expect: String::new(),
            probe_interval_ms: None,
            remark: String::new(),
        });
        let report = dir.apply(&cfg);
        assert_eq!(report.added, vec!["d".to_string()]);
        assert_eq!(report.removed, vec!["a".to_string()]);
        assert_eq!(dir.active(Protocol::Tcp).unwrap().name, "d");
        assert!(dir.snapshot().node("a").is_none());
    }

    #[test]
    fn healthy_flag_transitions_by_threshold() {
        let cfg = base_cfg("priority");
        let dir = Directory::new(&cfg);
        let t = dir.tuning();
        let a = dir.snapshot().node("a").unwrap();
        let mut changed = false;
        for _ in 0..t.failure_threshold {
            changed = a.record_probe(false, None, Some("boom".into()), &t);
        }
        assert!(changed, "达到阈值应触发降级");
        assert!(!a.is_healthy());
        let mut changed = false;
        for _ in 0..t.success_threshold {
            changed = a.record_probe(true, Some(100), None, &t);
        }
        assert!(changed, "达到成功阈值应恢复");
        assert!(a.is_healthy());
    }

    #[test]
    fn passive_eject_marks_unhealthy() {
        let cfg = base_cfg("priority");
        let dir = Directory::new(&cfg);
        let t = dir.tuning();
        let a = dir.snapshot().node("a").unwrap();
        let mut ejected = false;
        for _ in 0..t.passive_eject_threshold {
            ejected = a.record_passive_failure("conn refused".into(), &t);
        }
        assert!(ejected);
        assert!(!a.is_healthy());
    }
}
