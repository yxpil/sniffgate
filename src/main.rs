//! sniffgate —— 多节点嗅探主备热切换网关
//!
//! 对外只暴露统一入口（TCP / UDP 各可多个），内部把流量分发到多个后端节点；
//! 通过主动嗅探（周期性探测）+ 被动嗅探（真实转发失败）判定节点可用性，
//! 在主节点故障时秒级热切换到备用节点；配置文件改动后自动热加载，无需重启。

mod admin;
mod client;
mod config;
mod ctx;
mod health;
mod reload;
mod state;
mod supervisor;
mod tcp_proxy;
mod udp_proxy;

use anyhow::Result;
use clap::Parser;
use config::{Config, Protocol};
use ctx::{Ctx, ReloadStatus};
use state::{now_ms, Directory, SwitchEvent};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use supervisor::Supervisor;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "sniffgate",
    version,
    about = "多节点嗅探主备热切换网关：对外统一 TCP/UDP 入口 + 配置文件热加载"
)]
struct Cli {
    /// 配置文件路径（TOML）
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// 只校验配置并退出
    #[arg(long)]
    check: bool,

    /// 把示例配置打印到标准输出（可直接重定向为 config.toml）
    #[arg(long = "gen-config")]
    gen_config: bool,

    /// 覆盖配置文件中的日志级别（trace|debug|info|warn|error）
    #[arg(long)]
    log: Option<String>,

    /// 运维控制口地址，配合 --cmd 用作客户端
    #[arg(long)]
    admin: Option<String>,

    /// 通过控制口执行的命令；"-" 表示交互模式（默认 status）
    #[arg(long)]
    cmd: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.gen_config {
        print!("{}", config::EXAMPLE_CONFIG);
        return Ok(());
    }

    if let Some(admin) = cli.admin.clone() {
        let cmd = cli.cmd.clone().unwrap_or_else(|| "status".to_string());
        return client::run(&admin, &cmd, 15_000);
    }

    let cfg = Config::load(&cli.config)?;
    init_tracing(
        cli.log
            .clone()
            .unwrap_or_else(|| cfg.global.log_level.clone()),
    );

    if cli.check {
        println!("配置有效: {}", cli.config.display());
        println!(
            "  对外入口 {} 个 / 后端节点 {} 个 / 策略 {}",
            cfg.listeners.len(),
            cfg.nodes.len(),
            cfg.global.strategy.as_str()
        );
        for l in &cfg.listeners {
            println!("  入口 [{}] {} {}", l.name, l.protocol.as_str(), l.listen);
        }
        for n in &cfg.nodes {
            println!(
                "  节点 [{}] priority={} enabled={} tcp={:?} udp={:?} probe={:?} {}",
                n.name, n.priority, n.enabled, n.tcp, n.udp, n.probe, n.remark
            );
        }
        return Ok(());
    }

    run_app(cli, cfg).await
}

async fn run_app(cli: Cli, cfg: Config) -> Result<()> {
    let config_path = cli.config.clone();
    let dir = Directory::new(&cfg);
    let ctx = Ctx::new(dir.clone(), config_path.clone());
    ctx.set_reload(ReloadStatus {
        at_ms: now_ms(),
        ok: true,
        message: format!(
            "初始加载成功：{} 个节点 / {} 个入口",
            cfg.nodes.len(),
            cfg.listeners.len()
        ),
        path: config_path.display().to_string(),
        epoch: ctx.dir.snapshot().epoch,
    });

    let sup = Supervisor::new(ctx.clone());
    sup.sync();
    banner(&ctx, &sup);

    spawn_event_watcher(&ctx);
    ctx.tracker.spawn(reload::watch(
        ctx.clone(),
        sup.clone(),
        ctx.shutdown.clone(),
    ));

    shutdown_signal().await;
    info!("收到退出信号，开始优雅退出");
    ctx.shutdown.cancel();
    sup.stop_all();
    ctx.tracker.close();
    let grace = ctx.dir.tuning().shutdown_grace;
    if timeout(grace, ctx.tracker.wait()).await.is_err() {
        warn!("优雅退出超时（{:?}），强制结束", grace);
    }
    let (conns, failed, sessions) = admin::aggregate(&ctx);
    info!(
        tcp_conns_total = conns,
        tcp_conns_failed = failed,
        udp_sessions_total = sessions,
        "已退出"
    );
    Ok(())
}

fn init_tracing(level: String) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(format!("sniffgate={level},warn")))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn banner(ctx: &Arc<Ctx>, sup: &Arc<Supervisor>) {
    let rt = ctx.dir.snapshot();
    info!(
        version = ctx.version(),
        pid = std::process::id(),
        config = %ctx.config_path.display(),
        strategy = rt.global.strategy.as_str(),
        "sniffgate 启动完成"
    );
    for l in &rt.listeners {
        info!(
            listener = %l.name,
            protocol = l.protocol.as_str(),
            listen = %l.listen,
            "对外统一入口"
        );
    }
    for n in &rt.nodes {
        info!(
            node = %n.name,
            priority = n.cfg().priority,
            tcp = ?n.endpoint(Protocol::Tcp),
            udp = ?n.endpoint(Protocol::Udp),
            "后端节点"
        );
    }
    info!(
        active_tcp = ?rt.active_tcp.as_ref().map(|n| n.name.clone()),
        active_udp = ?rt.active_udp.as_ref().map(|n| n.name.clone()),
        admin = %rt.global.admin_listen,
        "主节点与运维控制口"
    );
    let (p, l, a) = sup.task_counts();
    info!(probes = p, listeners = l, admin = a, "后台任务已就绪");
}

/// 订阅切换事件：写日志 + 记入控制口可查的历史
fn spawn_event_watcher(ctx: &Arc<Ctx>) {
    let mut rx = ctx.dir.events();
    let ctx2 = ctx.clone();
    ctx.tracker.spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    log_switch(&ev);
                    ctx2.record_event(ev);
                }
                Err(RecvError::Lagged(n)) => warn!(skipped = n, "切换事件积压，部分事件未记录"),
                Err(RecvError::Closed) => break,
            }
        }
    });
}

fn log_switch(ev: &SwitchEvent) {
    info!(
        protocol = ev.protocol.as_str(),
        from = ?ev.from,
        to = ?ev.to,
        reason = %ev.reason,
        epoch = ev.epoch,
        "主备热切换"
    );
}

/// Ctrl-C / SIGTERM
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

/// 防止未使用告警的占位（保留给未来扩展：SIGHUP 立即重载）
#[allow(dead_code)]
fn _reload_hint(d: Duration) -> Duration {
    d
}
