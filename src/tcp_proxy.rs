//! TCP 对外统一入口：接收客户端连接，按主备路由选一个后端节点并双向转发。
//!
//! * 选路失败 / 连接失败 → 立刻重试下一个节点，并计入被动嗅探；
//! * 节点被降级（热切换）→ 已建立的连接在排空超时后被关闭；
//! * 进程退出 → 所有连接随退出信号关闭。

use crate::config::{ListenerConfig, Protocol};
use crate::ctx::Ctx;
use crate::health::bind_tcp_with_retry;
use crate::state::{now_ms, NodeHandle, NodeStats};
use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::copy_bidirectional;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// 入口运行循环（正常退出返回 Ok；绑定失败会重试直到被取消）
pub async fn run(listener: ListenerConfig, ctx: Arc<Ctx>, cancel: CancellationToken) -> Result<()> {
    let Some(l) = bind_tcp_with_retry(&listener.listen, &cancel, &listener.name).await else {
        return Ok(());
    };
    let local = l.local_addr().ok();
    info!(
        listener = %listener.name,
        addr = ?local,
        "TCP 统一入口已就绪"
    );
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = l.accept() => match accepted {
                Ok((stream, peer)) => {
                    let ctx2 = ctx.clone();
                    let listener_name = listener.name.clone();
                    ctx.tracker.spawn(async move {
                        handle(stream, peer, ctx2, listener_name).await;
                    });
                }
                Err(e) => {
                    warn!(listener = %listener.name, error = %e, "accept 失败");
                    sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }
    info!(listener = %listener.name, "TCP 统一入口已停止");
    Ok(())
}

async fn handle(mut client: TcpStream, peer: SocketAddr, ctx: Arc<Ctx>, listener: String) {
    let _ = client.set_nodelay(true);
    let tuning = ctx.dir.tuning();

    // ---- 选路（含失败重试） ----
    let mut exclude: Vec<String> = Vec::new();
    let mut chosen: Option<(Arc<NodeHandle>, TcpStream)> = None;
    for attempt in 0..=tuning.connect_retry {
        let Some(node) = ctx.dir.select(Protocol::Tcp, &exclude) else {
            break;
        };
        let Some(addr) = node.endpoint(Protocol::Tcp) else {
            exclude.push(node.name.clone());
            continue;
        };
        match timeout(tuning.connect_timeout, TcpStream::connect(addr.as_str())).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                if attempt > 0 {
                    info!(%peer, node = %node.name, attempt, "已切换到备用节点");
                }
                chosen = Some((node, stream));
                break;
            }
            Ok(Err(e)) => {
                let msg = format!("连接后端 {addr} 失败: {e}");
                penalize(&ctx, &node, msg, &tuning);
                exclude.push(node.name.clone());
            }
            Err(_) => {
                let msg = format!("连接后端 {addr} 超时");
                penalize(&ctx, &node, msg, &tuning);
                exclude.push(node.name.clone());
            }
        }
    }

    let Some((node, mut backend)) = chosen else {
        warn!(%peer, listener = %listener, "无可用后端节点，断开连接");
        let _ = client.shutdown().await;
        return;
    };

    NodeStats::inc(&node.stats.tcp_conns_total);
    NodeStats::inc(&node.stats.tcp_conns_active);
    node.inflight
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _guard = InflightGuard(node.clone());
    debug!(%peer, node = %node.name, "TCP 连接已接入后端");

    let mut drain_rx = node.subscribe_drain();
    let shutdown = ctx.shutdown.clone();
    let mut drained = false;
    tokio::select! {
        r = copy_bidirectional(&mut client, &mut backend) => {
            match r {
                Ok((up, down)) => {
                    NodeStats::add(&node.stats.bytes_tx, up);
                    NodeStats::add(&node.stats.bytes_rx, down);
                    debug!(%peer, node = %node.name, up, down, "TCP 转发结束");
                }
                Err(e) => debug!(%peer, node = %node.name, error = %e, "TCP 转发中断"),
            }
        }
        _ = wait_drain(&mut drain_rx) => {
            drained = true;
            NodeStats::inc(&node.stats.drain_kills);
            info!(%peer, node = %node.name, "排空超时：关闭已降级节点上的旧连接");
        }
        _ = shutdown.cancelled() => {
            debug!(%peer, "进程退出：关闭连接");
        }
    }
    let _ = backend.shutdown().await;
    let _ = client.shutdown().await;
    if drained {
        debug!(%peer, node = %node.name, "旧连接已关闭");
    }
}

/// 记录一次被动失败，达到阈值立即摘除节点并触发热切换
fn penalize(ctx: &Arc<Ctx>, node: &Arc<NodeHandle>, msg: String, t: &crate::config::Tuning) {
    NodeStats::inc(&node.stats.tcp_conns_failed);
    warn!(node = %node.name, error = %msg, "转发前连接后端失败");
    if node.record_passive_failure(msg, t) {
        warn!(node = %node.name, "被动嗅探判定节点不可用，触发主备切换");
        ctx.dir.recompute("passive-eject");
    }
}

struct InflightGuard(Arc<NodeHandle>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        NodeStats::dec(&self.0.stats.tcp_conns_active);
        self.0
            .inflight
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// 等待排空信号（只有真正排空才关闭连接）：
/// * 节点被降级 → 等到 deadline 到点后返回（连接随之关闭）；
/// * 期间节点又被提升为主（drain 复位为 0）→ 继续正常转发，重新等待下一次变化；
/// * 发送端被丢弃（节点被移除）→ 立即关闭。
async fn wait_drain(rx: &mut watch::Receiver<u64>) {
    loop {
        let deadline = *rx.borrow();
        if deadline != 0 {
            let now = now_ms();
            if deadline > now {
                sleep(Duration::from_millis(deadline - now)).await;
            }
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}
