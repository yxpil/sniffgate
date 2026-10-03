//! 配置文件热加载：轮询文件变化 → 解析校验 → 应用到路由表与任务栈。
//!
//! * 解析 / 校验失败时**保持旧配置继续运行**，并把错误写入控制口可见的状态；
//! * 成功时：
//!   - 新增/删除节点、就地更新节点属性（健康统计不丢）；
//!   - 新增/删除对外入口、入口地址变化时自动重启该入口；
//!   - 全局参数（阈值、超时、策略、排空时间…）立即生效。

use crate::config::Config;
use crate::ctx::{Ctx, ReloadStatus};
use crate::state::now_ms;
use crate::supervisor::Supervisor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// (修改时间毫秒, 文件长度)
type Stamp = Option<(u64, u64)>;

fn stamp(path: &Path) -> Stamp {
    let md = std::fs::metadata(path).ok()?;
    let mtime = md.modified().ok()?;
    let ms = mtime
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    Some((ms, md.len()))
}

/// 配置监听循环
pub async fn watch(ctx: Arc<Ctx>, sup: Arc<Supervisor>, cancel: CancellationToken) {
    let path = ctx.config_path.clone();
    let mut last = stamp(&path);
    info!(path = %path.display(), "已开启配置文件热加载监听");
    loop {
        let interval = ctx.dir.tuning().config_poll_interval;
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        let cur = stamp(&path);
        if cur != last {
            // 编辑器保存可能触发多次写入，稍等稳定后再读
            tokio::time::sleep(Duration::from_millis(150)).await;
            last = stamp(&path);
            info!(path = %path.display(), "检测到配置文件变化，开始热加载");
            reload_now(&ctx, &sup);
        }
    }
}

/// 立即执行一次热加载（控制口 `reload` 命令也走这里）
pub fn reload_now(ctx: &Arc<Ctx>, sup: &Arc<Supervisor>) {
    let path = &ctx.config_path;
    match Config::load(path) {
        Ok(cfg) => {
            let report = ctx.dir.apply(&cfg);
            sup.sync();
            let rt = ctx.dir.snapshot();
            let msg = format!(
                "热加载成功：节点 {} 个（新增 {} / 移除 {} / 更新 {}），入口 {} 个，epoch {}",
                rt.nodes.len(),
                report.added.len(),
                report.removed.len(),
                report.updated.len(),
                rt.listeners.len(),
                rt.epoch
            );
            info!("{}", msg);
            if !report.added.is_empty() {
                info!(added = ?report.added, "新增节点");
            }
            if !report.removed.is_empty() {
                info!(removed = ?report.removed, "移除节点");
            }
            if !report.updated.is_empty() {
                info!(updated = ?report.updated, "更新节点");
            }
            info!(
                active_tcp = ?ctx.dir.active(crate::config::Protocol::Tcp).map(|n| n.name.clone()),
                active_udp = ?ctx.dir.active(crate::config::Protocol::Udp).map(|n| n.name.clone()),
                "热加载后的主节点"
            );
            ctx.set_reload(ReloadStatus {
                at_ms: now_ms(),
                ok: true,
                message: msg,
                path: path.display().to_string(),
                epoch: rt.epoch,
            });
        }
        Err(e) => {
            let msg = format!("热加载失败，继续使用旧配置: {e:#}");
            warn!("{}", msg);
            ctx.set_reload(ReloadStatus {
                at_ms: now_ms(),
                ok: false,
                message: msg,
                path: path.display().to_string(),
                epoch: ctx.dir.snapshot().epoch,
            });
        }
    }
}
