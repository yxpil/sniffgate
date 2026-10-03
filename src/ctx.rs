//! 进程级共享上下文：路由中枢、退出信号、任务跟踪、热加载状态、切换历史。

use crate::state::{now_ms, Directory, SwitchEvent};
use arc_swap::ArcSwap;
use serde::Serialize;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// 最近切换事件保留条数
const HISTORY_LEN: usize = 100;

/// 配置文件热加载状态（控制口可查）
#[derive(Debug, Clone, Serialize)]
pub struct ReloadStatus {
    pub at_ms: u64,
    pub ok: bool,
    pub message: String,
    pub path: String,
    pub epoch: u64,
}

impl Default for ReloadStatus {
    fn default() -> Self {
        Self {
            at_ms: 0,
            ok: true,
            message: "尚未热加载".to_string(),
            path: String::new(),
            epoch: 0,
        }
    }
}

pub struct Ctx {
    /// 路由中枢（节点视图 + 主备选择）
    pub dir: Arc<Directory>,
    /// 进程退出信号
    pub shutdown: CancellationToken,
    /// 所有长生命周期任务（连接转发、UDP 会话、后台任务）
    pub tracker: TaskTracker,
    pub started: Instant,
    reload: ArcSwap<ReloadStatus>,
    history: Mutex<VecDeque<SwitchEvent>>,
    pub config_path: PathBuf,
}

impl Ctx {
    pub fn new(dir: Arc<Directory>, config_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            dir,
            shutdown: CancellationToken::new(),
            tracker: TaskTracker::new(),
            started: Instant::now(),
            reload: ArcSwap::from_pointee(ReloadStatus::default()),
            history: Mutex::new(VecDeque::with_capacity(HISTORY_LEN)),
            config_path,
        })
    }

    pub fn set_reload(&self, mut s: ReloadStatus) {
        if s.epoch == 0 {
            s.epoch = self.dir.snapshot().epoch;
        }
        self.reload.store(Arc::new(s));
    }

    pub fn reload_status(&self) -> ReloadStatus {
        let s = self.reload.load();
        let mut out = (**s).clone();
        if out.at_ms == 0 {
            out.at_ms = now_ms();
        }
        out
    }

    pub fn record_event(&self, ev: SwitchEvent) {
        let mut h = self.history.lock().unwrap();
        if h.len() >= HISTORY_LEN {
            h.pop_front();
        }
        h.push_back(ev);
    }

    /// 最近的切换事件（按时间正序）
    pub fn events_recent(&self, n: usize) -> Vec<SwitchEvent> {
        let h = self.history.lock().unwrap();
        h.iter().rev().take(n).rev().cloned().collect()
    }

    pub fn uptime_s(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    pub fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
