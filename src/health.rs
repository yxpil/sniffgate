//! 主动嗅探：周期性地对后端节点做 TCP 连接 / UDP 请求探测，
//! 结果驱动健康状态机，进而触发主备热切换。

use crate::config::{ProbeKind, Tuning};
use crate::state::{Directory, NodeHandle};
use anyhow::{bail, Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream, UdpSocket};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// 单个节点的嗅探循环：先立即探测一次，再按间隔循环
pub async fn probe_loop(node: Arc<NodeHandle>, dir: Arc<Directory>, cancel: CancellationToken) {
    info!(node = %node.name, "嗅探任务启动");
    loop {
        if cancel.is_cancelled() {
            break;
        }
        let tuning = dir.tuning();
        match probe_once(&node, &tuning).await {
            Ok(latency_us) => {
                if node.record_probe(true, Some(latency_us), None, &tuning) {
                    info!(node = %node.name, latency_us, "节点恢复可用");
                    dir.recompute("probe-recover");
                } else {
                    debug!(node = %node.name, latency_us, "嗅探正常");
                }
            }
            Err(e) => {
                let msg = format!("{e:#}");
                if node.record_probe(false, None, Some(msg.clone()), &tuning) {
                    warn!(node = %node.name, error = %msg, "节点不可用，触发主备切换");
                    dir.recompute("probe-fail");
                } else {
                    debug!(node = %node.name, error = %msg, "嗅探失败（未达阈值）");
                }
            }
        }
        let interval = node
            .cfg()
            .probe_interval_ms
            .map(Duration::from_millis)
            .unwrap_or(tuning.probe_interval)
            .max(Duration::from_millis(50));
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
    info!(node = %node.name, "嗅探任务退出");
}

/// 一次探测，成功返回往返耗时（微秒）
pub async fn probe_once(node: &NodeHandle, t: &Tuning) -> Result<u64> {
    let cfg = node.cfg();
    let kind = match cfg.probe {
        ProbeKind::None => return Ok(0),
        ProbeKind::Auto => {
            if cfg.tcp.is_some() {
                ProbeKind::Tcp
            } else if cfg.udp.is_some() {
                ProbeKind::Udp
            } else {
                return Ok(0);
            }
        }
        other => other,
    };
    let payload = cfg.decode_payload().context("解析 probe_payload 失败")?;
    let started = Instant::now();
    match kind {
        ProbeKind::Tcp => tcp_probe(&cfg, &payload, t).await?,
        ProbeKind::Udp => udp_probe(&cfg, &payload, t).await?,
        ProbeKind::Auto | ProbeKind::None => {}
    }
    Ok(started.elapsed().as_micros() as u64)
}

async fn tcp_probe(cfg: &crate::config::NodeConfig, payload: &[u8], t: &Tuning) -> Result<()> {
    let addr = cfg.tcp.as_deref().context("未配置 tcp 地址")?;
    let mut stream = timeout(t.probe_timeout, TcpStream::connect(addr))
        .await
        .with_context(|| format!("TCP 探测连接超时 ({addr})"))?
        .with_context(|| format!("TCP 探测连接失败 ({addr})"))?;
    if !payload.is_empty() || !cfg.probe_expect.is_empty() {
        stream
            .write_all(payload)
            .await
            .context("发送探测载荷失败")?;
        stream.flush().await.ok();
        let mut buf = vec![0u8; 4096];
        let n = timeout(t.probe_timeout, stream.read(&mut buf))
            .await
            .with_context(|| format!("等待探测响应超时 ({addr})"))?
            .context("读取探测响应失败")?;
        if n == 0 {
            bail!("对端未返回任何数据（{addr}）");
        }
        if !cfg.probe_expect.is_empty() {
            let s = String::from_utf8_lossy(&buf[..n]).to_string();
            if !s.contains(&cfg.probe_expect) {
                bail!(
                    "探测响应不匹配（期望包含 {:?}，实际 {:?}）",
                    cfg.probe_expect,
                    s.escape_debug().to_string()
                );
            }
        }
    }
    Ok(())
}

async fn udp_probe(cfg: &crate::config::NodeConfig, payload: &[u8], t: &Tuning) -> Result<()> {
    let addr = cfg.udp.as_deref().context("未配置 udp 地址")?;
    let target: SocketAddr = lookup_host(addr)
        .await
        .with_context(|| format!("解析 UDP 地址失败 ({addr})"))?
        .next()
        .with_context(|| format!("UDP 地址无解析结果 ({addr})"))?;
    let sock = UdpSocket::bind(wildcard_for(target))
        .await
        .context("创建 UDP 探测套接字失败")?;
    sock.connect(target)
        .await
        .context("UDP 探测 connect 失败")?;
    let send_buf: &[u8] = if payload.is_empty() { &[0u8] } else { payload };
    sock.send(send_buf).await.context("发送 UDP 探测失败")?;
    let mut buf = vec![0u8; 4096];
    let n = timeout(t.probe_timeout, sock.recv(&mut buf))
        .await
        .with_context(|| format!("UDP 探测无响应 ({addr})"))?
        .context("读取 UDP 探测响应失败")?;
    if !cfg.probe_expect.is_empty() {
        let s = String::from_utf8_lossy(&buf[..n]).to_string();
        if !s.contains(&cfg.probe_expect) {
            bail!("UDP 探测响应不匹配（期望包含 {:?}）", cfg.probe_expect);
        }
    }
    Ok(())
}

/// 依据目标地址族选择通配绑定地址
pub fn wildcard_for(target: SocketAddr) -> &'static str {
    if target.is_ipv6() {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    }
}

/// 端口占用时自动重试（应对热加载重启监听、地址被前一个进程短暂占用）
pub async fn bind_tcp_with_retry(
    addr: &str,
    cancel: &CancellationToken,
    label: &str,
) -> Option<tokio::net::TcpListener> {
    let mut backoff = Duration::from_millis(100);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => return Some(l),
            Err(e) => {
                warn!(listener = label, addr, error = %e, "TCP 入口绑定失败，{:?} 后重试", backoff);
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return None,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// 同 [`bind_tcp_with_retry`]，用于 UDP
pub async fn bind_udp_with_retry(
    addr: &str,
    cancel: &CancellationToken,
    label: &str,
) -> Option<UdpSocket> {
    let mut backoff = Duration::from_millis(100);
    loop {
        match UdpSocket::bind(addr).await {
            Ok(s) => return Some(s),
            Err(e) => {
                warn!(listener = label, addr, error = %e, "UDP 入口绑定失败，{:?} 后重试", backoff);
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return None,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}
