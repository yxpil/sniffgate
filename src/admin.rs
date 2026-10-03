//! 运维控制口：监听一个 TCP 端口，支持「行式 JSON」和「纯文本命令」两种协议，
//! 每行一条命令，返回一行 JSON。可直接 `nc 127.0.0.1 9100` 或
//! `sniffgate --admin 127.0.0.1:9100 --cmd status` 使用。
//!
//! 命令一览
//! ```text
//! status                    查看运行状态（主节点、节点健康、入口、热加载状态）
//! stats                     查看累计统计
//! reload                    立即从磁盘重新加载配置文件
//! switch <节点> [tcp|udp]   手动指定主节点（粘滞，直到故障或 auto）
//! auto [tcp|udp]            取消手动指定，恢复自动选主
//! enable <节点>             在线启用节点
//! disable <节点>            在线停用节点
//! health <节点> up|down     手动标记节点健康状态
//! help                      帮助
//! ```

use crate::config::Protocol;
use crate::ctx::Ctx;
use crate::reload::reload_now;
use crate::state::NodeHandle;
use crate::supervisor::Supervisor;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub async fn run(
    ctx: Arc<Ctx>,
    sup: Weak<Supervisor>,
    listen: String,
    cancel: CancellationToken,
) -> Result<()> {
    let l = TcpListener::bind(listen.as_str())
        .await
        .with_context(|| format!("绑定运维控制口失败: {listen}"))?;
    info!(addr = ?l.local_addr().ok(), "运维控制口已就绪（行式 JSON / 文本命令）");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            r = l.accept() => match r {
                Ok((stream, peer)) => {
                    let (ctx2, sup2, c2) = (ctx.clone(), sup.clone(), cancel.clone());
                    ctx.tracker.spawn(async move {
                        if let Err(e) = serve(stream, peer, ctx2, sup2, c2).await {
                            debug!(%peer, error = %e, "控制连接结束");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "控制口 accept 失败");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    info!("运维控制口已停止");
    Ok(())
}

async fn serve(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    ctx: Arc<Ctx>,
    sup: Weak<Supervisor>,
    cancel: CancellationToken,
) -> Result<()> {
    let (r, w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut writer = BufWriter::new(w);
    let mut line = String::new();
    loop {
        line.clear();
        let n = tokio::select! {
            _ = cancel.cancelled() => break,
            n = reader.read_line(&mut line) => n?,
        };
        if n == 0 {
            break;
        }
        let raw = line.trim();
        if raw.is_empty() {
            continue;
        }
        if raw == "quit" || raw == "exit" {
            writer.write_all(b"{\"ok\":true,\"bye\":true}\n").await?;
            writer.flush().await?;
            break;
        }
        let resp = dispatch(raw, &ctx, &sup).await;
        writer.write_all(resp.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    debug!(%peer, "控制连接关闭");
    Ok(())
}

struct Req {
    cmd: String,
    node: Option<String>,
    protocol: Option<Protocol>,
    healthy: Option<bool>,
}

fn parse_request(raw: &str) -> Req {
    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        return Req {
            cmd: v
                .get("cmd")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string(),
            node: v
                .get("node")
                .and_then(|c| c.as_str())
                .map(|s| s.to_string()),
            protocol: v
                .get("protocol")
                .and_then(|c| c.as_str())
                .and_then(|s| Protocol::parse_loose(s).ok()),
            healthy: v.get("healthy").and_then(|c| c.as_bool()).or_else(|| {
                v.get("healthy")
                    .and_then(|c| c.as_str())
                    .map(|s| matches!(s, "up" | "true" | "1" | "healthy"))
            }),
        };
    }
    let mut it = raw.split_whitespace();
    let cmd = it.next().unwrap_or("").to_string();
    let node = it.next().map(|s| s.to_string());
    let rest = it.next().unwrap_or("").to_ascii_lowercase();
    let protocol = Protocol::parse_loose(&rest).ok();
    let healthy = match rest.as_str() {
        "up" | "true" | "1" | "healthy" => Some(true),
        "down" | "false" | "0" | "unhealthy" => Some(false),
        _ => None,
    };
    Req {
        cmd,
        node,
        protocol,
        healthy,
    }
}

async fn dispatch(raw: &str, ctx: &Arc<Ctx>, sup: &Weak<Supervisor>) -> String {
    match dispatch_inner(raw, ctx, sup).await {
        Ok(mut v) => {
            if let Some(obj) = v.as_object_mut() {
                obj.entry("ok").or_insert(json!(true));
            }
            v.to_string()
        }
        Err(e) => json!({ "ok": false, "error": format!("{e:#}") }).to_string(),
    }
}

async fn dispatch_inner(raw: &str, ctx: &Arc<Ctx>, sup: &Weak<Supervisor>) -> Result<Value> {
    let req = parse_request(raw);
    let out: Result<Value> = match req.cmd.as_str() {
        "status" => Ok(status_value(ctx)),
        "stats" => Ok(stats_value(ctx)),
        "reload" => match sup.upgrade() {
            Some(sup) => {
                reload_now(ctx, &sup);
                Ok(json!({ "cmd": "reload", "reload": ctx.reload_status() }))
            }
            None => bail!("内部状态异常：任务栈不可用"),
        },
        "switch" => {
            let name = req.node.clone().context("switch 需要 node 参数")?;
            let protos = protocols_for(&req, &name, ctx);
            let mut applied = Vec::new();
            for p in protos {
                ctx.dir.force_active(p, Some(&name))?;
                applied.push(p.as_str());
            }
            if applied.is_empty() {
                bail!("节点 {name} 没有可用的后端地址");
            }
            Ok(with_status(
                ctx,
                json!({ "cmd": "switch", "node": name, "protocols": applied }),
            ))
        }
        "auto" => {
            let protos: Vec<Protocol> = match req.protocol {
                Some(p) => vec![p],
                None => vec![Protocol::Tcp, Protocol::Udp],
            };
            for p in protos {
                ctx.dir.force_active(p, None)?;
            }
            Ok(with_status(ctx, json!({ "cmd": "auto" })))
        }
        "enable" | "disable" => {
            let name = req.node.clone().context("enable/disable 需要 node 参数")?;
            ctx.dir.set_enabled(&name, req.cmd == "enable")?;
            Ok(with_status(ctx, json!({ "cmd": req.cmd, "node": name })))
        }
        "health" => {
            let name = req.node.clone().context("health 需要 node 参数")?;
            let healthy = req
                .healthy
                .context("health 需要 up/down（或 healthy: true/false）")?;
            ctx.dir.set_health(&name, healthy)?;
            Ok(with_status(
                ctx,
                json!({ "cmd": "health", "node": name, "healthy": healthy }),
            ))
        }
        "help" | "" => Ok(json!({
            "cmd": "help",
            "commands": [
                "status",
                "stats",
                "reload",
                "switch <node> [tcp|udp]",
                "auto [tcp|udp]",
                "enable <node>",
                "disable <node>",
                "health <node> up|down",
                "quit"
            ],
            "json_examples": [
                {"cmd": "status"},
                {"cmd": "switch", "node": "node-b", "protocol": "tcp"},
                {"cmd": "health", "node": "node-a", "healthy": false}
            ]
        })),
        other => bail!(
            "未知命令: {other}（可用: status/stats/reload/switch/auto/enable/disable/health/help）"
        ),
    };
    out
}

/// switch 未指定协议时，自动作用于该节点支持的协议
fn protocols_for(req: &Req, name: &str, ctx: &Arc<Ctx>) -> Vec<Protocol> {
    if let Some(p) = req.protocol {
        return vec![p];
    }
    let rt = ctx.dir.snapshot();
    match rt.node(name) {
        Some(n) => {
            let mut v = Vec::new();
            if n.endpoint(Protocol::Tcp).is_some() {
                v.push(Protocol::Tcp);
            }
            if n.endpoint(Protocol::Udp).is_some() {
                v.push(Protocol::Udp);
            }
            v
        }
        None => Vec::new(),
    }
}

fn node_value(n: &Arc<NodeHandle>) -> Value {
    let c = n.cfg();
    let h = n.health_snapshot();
    json!({
        "name": n.name,
        "enabled": c.enabled,
        "priority": c.priority,
        "weight": c.weight,
        "tcp": c.tcp,
        "udp": c.udp,
        "probe": c.probe,
        "remark": c.remark,
        "healthy": n.is_healthy(),
        "state": h.state,
        "last_error": h.last_error,
        "last_probe_ms": h.last_probe_ms,
        "last_change_ms": h.last_change_ms,
        "probe_latency_us": h.probe_latency_us,
        "passive_fails": h.passive_fails,
        "inflight_tcp": n.inflight.load(Ordering::Relaxed),
        "draining": n.is_draining(),
        "draining_until_ms": n.drain_deadline_ms(),
        "stats": {
            "tcp_conns_total": n.stats.tcp_conns_total.load(Ordering::Relaxed),
            "tcp_conns_failed": n.stats.tcp_conns_failed.load(Ordering::Relaxed),
            "tcp_conns_active": n.stats.tcp_conns_active.load(Ordering::Relaxed),
            "udp_sessions_total": n.stats.udp_sessions_total.load(Ordering::Relaxed),
            "udp_sessions_active": n.stats.udp_sessions_active.load(Ordering::Relaxed),
            "bytes_tx": n.stats.bytes_tx.load(Ordering::Relaxed),
            "bytes_rx": n.stats.bytes_rx.load(Ordering::Relaxed),
            "probe_ok": n.stats.probe_ok.load(Ordering::Relaxed),
            "probe_fail": n.stats.probe_fail.load(Ordering::Relaxed),
            "passive_ejects": n.stats.passive_ejects.load(Ordering::Relaxed),
            "promotions": n.stats.promotions.load(Ordering::Relaxed),
        }
    })
}

/// 把补充字段合并进完整状态（保证客户端一次拿到「执行结果 + 最新状态」）
fn with_status(ctx: &Arc<Ctx>, extra: Value) -> Value {
    let mut base = status_value(ctx);
    if let (Some(b), Some(e)) = (base.as_object_mut(), extra.as_object()) {
        for (k, v) in e.iter() {
            b.insert(k.clone(), v.clone());
        }
    }
    base
}

fn status_value(ctx: &Arc<Ctx>) -> Value {
    let rt = ctx.dir.snapshot();
    json!({
        "engine": "sniffgate",
        "version": ctx.version(),
        "pid": std::process::id(),
        "uptime_s": ctx.uptime_s(),
        "now_ms": crate::state::now_ms(),
        "epoch": rt.epoch,
        "loaded_at_ms": rt.loaded_at_ms,
        "strategy": rt.global.strategy,
        "config_path": ctx.config_path.display().to_string(),
        "active": {
            "tcp": rt.active(Protocol::Tcp).map(|n| n.name.clone()),
            "udp": rt.active(Protocol::Udp).map(|n| n.name.clone()),
        },
        "pins": {
            "tcp": ctx.dir.pin_of(Protocol::Tcp),
            "udp": ctx.dir.pin_of(Protocol::Udp),
        },
        "listeners": rt.listeners.iter().map(|l| json!({
            "name": l.name,
            "protocol": l.protocol,
            "listen": l.listen,
        })).collect::<Vec<_>>(),
        "nodes": rt.nodes.iter().map(node_value).collect::<Vec<_>>(),
        "reload": ctx.reload_status(),
        "recent_switches": ctx.events_recent(10),
    })
}

fn stats_value(ctx: &Arc<Ctx>) -> Value {
    let rt = ctx.dir.snapshot();
    let mut agg = json!({
        "tcp_conns_total": 0u64,
        "tcp_conns_failed": 0u64,
        "tcp_conns_active": 0u64,
        "udp_sessions_total": 0u64,
        "udp_sessions_active": 0u64,
        "bytes_tx": 0u64,
        "bytes_rx": 0u64,
        "probe_ok": 0u64,
        "probe_fail": 0u64,
        "passive_ejects": 0u64,
    });
    for n in &rt.nodes {
        for (k, v) in [
            (
                "tcp_conns_total",
                n.stats.tcp_conns_total.load(Ordering::Relaxed),
            ),
            (
                "tcp_conns_failed",
                n.stats.tcp_conns_failed.load(Ordering::Relaxed),
            ),
            (
                "tcp_conns_active",
                n.stats.tcp_conns_active.load(Ordering::Relaxed),
            ),
            (
                "udp_sessions_total",
                n.stats.udp_sessions_total.load(Ordering::Relaxed),
            ),
            (
                "udp_sessions_active",
                n.stats.udp_sessions_active.load(Ordering::Relaxed),
            ),
            ("bytes_tx", n.stats.bytes_tx.load(Ordering::Relaxed)),
            ("bytes_rx", n.stats.bytes_rx.load(Ordering::Relaxed)),
            ("probe_ok", n.stats.probe_ok.load(Ordering::Relaxed)),
            ("probe_fail", n.stats.probe_fail.load(Ordering::Relaxed)),
            (
                "passive_ejects",
                n.stats.passive_ejects.load(Ordering::Relaxed),
            ),
        ] {
            let cur = agg.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
            agg[k] = json!(cur + v);
        }
    }
    json!({
        "cmd": "stats",
        "epoch": rt.epoch,
        "total": agg,
        "nodes": rt.nodes.iter().map(|n| json!({
            "name": n.name,
            "tcp_conns_total": n.stats.tcp_conns_total.load(Ordering::Relaxed),
            "tcp_conns_failed": n.stats.tcp_conns_failed.load(Ordering::Relaxed),
            "tcp_conns_active": n.stats.tcp_conns_active.load(Ordering::Relaxed),
            "udp_sessions_total": n.stats.udp_sessions_total.load(Ordering::Relaxed),
            "udp_sessions_active": n.stats.udp_sessions_active.load(Ordering::Relaxed),
            "bytes_tx": n.stats.bytes_tx.load(Ordering::Relaxed),
            "bytes_rx": n.stats.bytes_rx.load(Ordering::Relaxed),
            "probe_ok": n.stats.probe_ok.load(Ordering::Relaxed),
            "probe_fail": n.stats.probe_fail.load(Ordering::Relaxed),
            "passive_ejects": n.stats.passive_ejects.load(Ordering::Relaxed),
            "promotions": n.stats.promotions.load(Ordering::Relaxed),
        })).collect::<Vec<_>>(),
    })
}

/// 供外部（如自检 / 日志）复用的统计聚合
pub fn aggregate(ctx: &Arc<Ctx>) -> (u64, u64, u64) {
    let rt = ctx.dir.snapshot();
    let mut t = (0u64, 0u64, 0u64);
    for n in &rt.nodes {
        t.0 += n.stats.tcp_conns_total.load(Ordering::Relaxed);
        t.1 += n.stats.tcp_conns_failed.load(Ordering::Relaxed);
        t.2 += n.stats.udp_sessions_total.load(Ordering::Relaxed);
    }
    t
}
