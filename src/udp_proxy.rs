//! UDP 对外统一入口：一个前端套接字接收所有客户端报文，按「客户端地址」建立会话，
//! 每个会话独立持有一个后端套接字；主备热切换时可把在途会话重绑到新的主节点。
//!
//! 说明：UDP 是无连接协议，程序用「会话表 + 空闲回收」模拟连接语义：
//! * 新客户端地址 → 新建会话（选主节点，绑定一个临时后端套接字）；
//! * 会话空闲超过 `udp_session_timeout_ms` → 自动回收；
//! * 收到主备切换事件且 `udp_rebind_on_switch = true` → 会话立即切到新主节点。

use crate::config::{ListenerConfig, Protocol};
use crate::ctx::Ctx;
use crate::health::bind_udp_with_retry;
use crate::state::{Directory, NodeHandle, NodeStats};
use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// 单会话待处理报文队列长度（超出即丢弃，UDP 本身允许丢包）
const SESSION_QUEUE: usize = 1024;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

struct SessionEntry {
    id: u64,
    tx: mpsc::Sender<Vec<u8>>,
}

pub async fn run(listener: ListenerConfig, ctx: Arc<Ctx>, cancel: CancellationToken) -> Result<()> {
    let Some(sock) = bind_udp_with_retry(&listener.listen, &cancel, &listener.name).await else {
        return Ok(());
    };
    let local = sock.local_addr().ok();
    let front = Arc::new(sock);
    let sessions: Arc<Mutex<HashMap<SocketAddr, SessionEntry>>> =
        Arc::new(Mutex::new(HashMap::new()));
    info!(
        listener = %listener.name,
        addr = ?local,
        "UDP 统一入口已就绪"
    );

    let mut buf = vec![0u8; 65535];
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            r = front.recv_from(&mut buf) => {
                let (n, peer) = match r {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(listener = %listener.name, error = %e, "UDP 接收失败");
                        continue;
                    }
                };
                let sender = {
                    let mut map = sessions.lock().unwrap();
                    match map.get(&peer) {
                        Some(e) => Some(e.tx.clone()),
                        None => {
                            let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
                            let (tx, rx) = mpsc::channel::<Vec<u8>>(SESSION_QUEUE);
                            map.insert(peer, SessionEntry { id, tx: tx.clone() });
                            let (ctx2, front2, map2) = (ctx.clone(), front.clone(), sessions.clone());
                            ctx.tracker.spawn(async move {
                                session(peer, id, rx, front2, map2, ctx2).await;
                            });
                            Some(tx)
                        }
                    }
                };
                if let Some(tx) = sender {
                    match tx.try_send(buf[..n].to_vec()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            debug!(%peer, "UDP 会话队列已满，本轮报文丢弃");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            debug!(%peer, "UDP 会话已结束，报文将被随后新建的会话接管");
                        }
                    }
                }
            }
        }
    }

    sessions.lock().unwrap().clear();
    info!(listener = %listener.name, "UDP 统一入口已停止");
    Ok(())
}

/// 单个客户端地址对应一个会话
async fn session(
    peer: SocketAddr,
    id: u64,
    mut rx: mpsc::Receiver<Vec<u8>>,
    front: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, SessionEntry>>>,
    ctx: Arc<Ctx>,
) {
    let tuning = ctx.dir.tuning();
    let bind_addr = if peer.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let backend = match UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            warn!(%peer, error = %e, "创建 UDP 后端套接字失败，会话结束");
            remove_session(&sessions, peer, id);
            return;
        }
    };

    let mut current: Option<(Arc<NodeHandle>, SocketAddr)> = None;
    let mut switch_rx = ctx.dir.events();
    let idle = tuning.udp_session_timeout;
    let mut last = tokio::time::Instant::now();
    let mut rbuf = vec![0u8; 65535];

    loop {
        let deadline = last + idle;
        let mut pending: Option<Vec<u8>> = None;
        let mut reply: Option<(usize, SocketAddr)> = None;
        let mut rebind_requested = false;
        let mut stop = false;

        tokio::select! {
            _ = ctx.shutdown.cancelled() => stop = true,
            _ = tokio::time::sleep_until(deadline) => {
                debug!(%peer, "UDP 会话空闲超时回收");
                stop = true;
            }
            ev = switch_rx.recv() => match ev {
                Ok(ev) => {
                    if tuning.udp_rebind_on_switch && ev.protocol == Protocol::Udp {
                        rebind_requested = true;
                    }
                }
                Err(RecvError::Lagged(n)) => debug!(skipped = n, "UDP 会话漏掉了切换事件"),
                Err(RecvError::Closed) => stop = true,
            },
            maybe = rx.recv() => match maybe {
                None => stop = true,
                Some(data) => {
                    last = tokio::time::Instant::now();
                    pending = Some(data);
                }
            },
            r = backend.recv_from(&mut rbuf) => match r {
                Ok((n, from)) => {
                    last = tokio::time::Instant::now();
                    reply = Some((n, from));
                }
                Err(e) => debug!(%peer, error = %e, "读取 UDP 后端响应失败"),
            },
        }

        if stop {
            break;
        }
        if rebind_requested {
            let next = resolve_backend(&ctx.dir, Protocol::Udp).await;
            rebind(&peer, &mut current, next);
        }
        if let Some(data) = pending {
            if current.is_none() {
                let next = resolve_backend(&ctx.dir, Protocol::Udp).await;
                rebind(&peer, &mut current, next);
            }
            match &current {
                Some((node, addr)) => match backend.send_to(&data, *addr).await {
                    Ok(n) => NodeStats::add(&node.stats.bytes_tx, n as u64),
                    Err(e) => {
                        warn!(%peer, node = %node.name, error = %e, "UDP 转发到后端失败，下个报文重新选主");
                        let msg = format!("UDP 转发失败: {e}");
                        if node.record_passive_failure(msg, &tuning) {
                            ctx.dir.recompute("passive-eject-udp");
                        }
                        current = None;
                    }
                },
                None => debug!(%peer, "当前无可用 UDP 后端，丢弃报文"),
            }
        }
        if let Some((n, from)) = reply {
            match &current {
                Some((node, addr)) if *addr == from => {
                    if let Err(e) = front.send_to(&rbuf[..n], peer).await {
                        debug!(%peer, error = %e, "回包给客户端失败");
                    } else {
                        NodeStats::add(&node.stats.bytes_rx, n as u64);
                    }
                }
                Some(_) => debug!(%from, "忽略非当前后端来源的 UDP 报文"),
                None => {}
            }
        }
    }

    if let Some((node, _)) = current {
        NodeStats::dec(&node.stats.udp_sessions_active);
    }
    remove_session(&sessions, peer, id);
}

fn remove_session(
    sessions: &Arc<Mutex<HashMap<SocketAddr, SessionEntry>>>,
    peer: SocketAddr,
    id: u64,
) {
    let mut map = sessions.lock().unwrap();
    if let Some(e) = map.get(&peer) {
        if e.id == id {
            map.remove(&peer);
            debug!(%peer, "UDP 会话已回收");
        }
    }
}

/// 绑定（或重绑）会话后端，并维护会话计数
fn rebind(
    peer: &SocketAddr,
    current: &mut Option<(Arc<NodeHandle>, SocketAddr)>,
    next: Option<(Arc<NodeHandle>, SocketAddr)>,
) {
    let Some((node, addr)) = next else {
        return;
    };
    match current {
        None => {
            NodeStats::inc(&node.stats.udp_sessions_total);
            NodeStats::inc(&node.stats.udp_sessions_active);
            info!(%peer, node = %node.name, backend = %addr, "UDP 会话建立");
            *current = Some((node, addr));
        }
        Some((old, old_addr)) => {
            if old.name != node.name || *old_addr != addr {
                NodeStats::dec(&old.stats.udp_sessions_active);
                NodeStats::inc(&node.stats.udp_sessions_active);
                info!(%peer, from = %old.name, to = %node.name, "UDP 会话已重绑到新的主节点");
                *current = Some((node, addr));
            }
        }
    }
}

/// 为会话选一个 UDP 后端（选主 + 地址解析）
async fn resolve_backend(
    dir: &Directory,
    proto: Protocol,
) -> Option<(Arc<NodeHandle>, SocketAddr)> {
    let node = dir.select(proto, &[])?;
    let addr = node.endpoint(proto)?;
    let resolved = lookup_host(addr.as_str()).await;
    let target = match resolved {
        Ok(mut it) => it.next(),
        Err(e) => {
            warn!(node = %node.name, addr = %addr, error = %e, "解析后端地址失败");
            return None;
        }
    };
    match target {
        Some(a) => Some((node, a)),
        None => {
            warn!(node = %node.name, addr = %addr, "后端地址无解析结果");
            None
        }
    }
}
