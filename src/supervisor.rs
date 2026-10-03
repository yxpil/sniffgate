//! 任务栈：统一管理「嗅探任务 / TCP 入口 / UDP 入口 / 运维控制口」的生命周期，
//! 并在配置文件热加载后增删改这些任务（改端口 = 重启该入口，改节点 = 增删嗅探任务）。

use crate::config::Protocol;
use crate::ctx::Ctx;
use crate::health;
use crate::state::NodeHandle;
use crate::{admin, tcp_proxy, udp_proxy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

struct TaskHandle {
    /// 用于判断是否需要重启（入口 = 协议 + 监听地址）
    key: String,
    cancel: CancellationToken,
    #[allow(dead_code)]
    join: Option<JoinHandle<()>>,
}

pub struct Supervisor {
    ctx: Arc<Ctx>,
    probes: Mutex<HashMap<String, TaskHandle>>,
    listeners: Mutex<HashMap<String, TaskHandle>>,
    admin: Mutex<Option<TaskHandle>>,
    self_ref: Mutex<Weak<Supervisor>>,
}

impl Supervisor {
    pub fn new(ctx: Arc<Ctx>) -> Arc<Self> {
        let sup = Arc::new(Self {
            ctx,
            probes: Mutex::new(HashMap::new()),
            listeners: Mutex::new(HashMap::new()),
            admin: Mutex::new(None),
            self_ref: Mutex::new(Weak::new()),
        });
        *sup.self_ref.lock().unwrap() = Arc::downgrade(&sup);
        sup
    }

    fn me(&self) -> Weak<Supervisor> {
        self.self_ref.lock().unwrap().clone()
    }

    /// 让运行中的任务集合与当前路由表（= 当前已生效配置）对齐
    pub fn sync(&self) {
        let rt = self.ctx.dir.snapshot();

        // ---- 每个节点一个嗅探任务 ----
        {
            let mut probes = self.probes.lock().unwrap();
            for node in &rt.nodes {
                if !probes.contains_key(&node.name) {
                    probes.insert(node.name.clone(), self.spawn_probe(node.clone()));
                }
            }
            let stale: Vec<String> = probes
                .keys()
                .filter(|name| !rt.nodes.iter().any(|n| &n.name == *name))
                .cloned()
                .collect();
            for name in stale {
                if let Some(t) = probes.remove(&name) {
                    t.cancel.cancel();
                    info!(node = %name, "嗅探任务已停止（节点已移除）");
                }
            }
        }

        // ---- 对外统一入口 ----
        {
            let mut running = self.listeners.lock().unwrap();
            for lc in &rt.listeners {
                let key = format!("{}://{}", lc.protocol.as_str(), lc.listen);
                match running.get(&lc.name) {
                    Some(t) if t.key == key => {}
                    Some(_) => {
                        if let Some(old) = running.remove(&lc.name) {
                            old.cancel.cancel();
                            info!(listener = %lc.name, "入口配置变更，重启该入口");
                        }
                        let h = self.spawn_listener(lc.clone());
                        running.insert(lc.name.clone(), h);
                    }
                    None => {
                        let h = self.spawn_listener(lc.clone());
                        running.insert(lc.name.clone(), h);
                    }
                }
            }
            let stale: Vec<String> = running
                .keys()
                .filter(|name| !rt.listeners.iter().any(|l| &l.name == *name))
                .cloned()
                .collect();
            for name in stale {
                if let Some(t) = running.remove(&name) {
                    t.cancel.cancel();
                    info!(listener = %name, "入口已移除");
                }
            }
        }

        // ---- 运维控制口 ----
        {
            let mut admin_slot = self.admin.lock().unwrap();
            let want = rt.global.admin_listen.trim().to_string();
            let current = admin_slot.as_ref().map(|t| t.key.clone());
            if current.as_deref() == Some(want.as_str()) {
                return;
            }
            if let Some(old) = admin_slot.take() {
                old.cancel.cancel();
                info!("运维控制口配置变更，重启控制口");
            }
            if !want.is_empty() {
                let cancel = CancellationToken::new();
                let ctx = self.ctx.clone();
                let me = self.me();
                let (addr, c) = (want.clone(), cancel.clone());
                let join = tokio::spawn(async move {
                    if let Err(e) = admin::run(ctx, me, addr, c).await {
                        error!(error = %e, "运维控制口异常退出");
                    }
                });
                *admin_slot = Some(TaskHandle {
                    key: want,
                    cancel,
                    join: Some(join),
                });
            }
        }
    }

    fn spawn_probe(&self, node: Arc<NodeHandle>) -> TaskHandle {
        let cancel = node.cancel.clone();
        let dir = self.ctx.dir.clone();
        let name = node.name.clone();
        let token = cancel.clone();
        let join = tokio::spawn(async move {
            health::probe_loop(node, dir, token).await;
        });
        TaskHandle {
            key: name,
            cancel,
            join: Some(join),
        }
    }

    fn spawn_listener(&self, lc: crate::config::ListenerConfig) -> TaskHandle {
        let cancel = CancellationToken::new();
        let ctx = self.ctx.clone();
        let key = format!("{}://{}", lc.protocol.as_str(), lc.listen);
        let label = lc.name.clone();
        let proto = lc.protocol;
        let c = cancel.clone();
        let join = tokio::spawn(async move {
            let r = match proto {
                Protocol::Tcp => tcp_proxy::run(lc, ctx, c).await,
                Protocol::Udp => udp_proxy::run(lc, ctx, c).await,
            };
            if let Err(e) = r {
                error!(listener = %label, error = %e, "入口异常退出");
            }
        });
        TaskHandle {
            key,
            cancel,
            join: Some(join),
        }
    }

    /// 退出时取消所有后台任务
    pub fn stop_all(&self) {
        for (_, t) in self.probes.lock().unwrap().drain() {
            t.cancel.cancel();
        }
        for (_, t) in self.listeners.lock().unwrap().drain() {
            t.cancel.cancel();
        }
        if let Some(t) = self.admin.lock().unwrap().take() {
            t.cancel.cancel();
        }
    }

    /// 运行中任务规模（供状态展示）
    pub fn task_counts(&self) -> (usize, usize, bool) {
        (
            self.probes.lock().unwrap().len(),
            self.listeners.lock().unwrap().len(),
            self.admin.lock().unwrap().is_some(),
        )
    }
}
