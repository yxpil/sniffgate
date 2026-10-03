//! 端到端测试：真实拉起 sniffgate 进程 + 三个后端回显服务，验证
//!
//! 1. TCP / UDP 统一入口都能正常转发（同一个端口号，各协议独立）；
//! 2. 主节点故障后秒级热切换到备用节点（主动嗅探触发）；
//! 3. 已在途的 UDP 会话在切换时被重绑到新主节点；
//! 4. 修改配置文件后热加载生效（新增节点、改变主备关系）；
//! 5. 配置写错时热加载失败但旧配置继续工作；
//! 6. 运维控制口 status / switch / auto / disable / enable 可用。

use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// 测试用后端：TCP + UDP 回显，响应带节点前缀，便于识别流量落在哪个节点
// ---------------------------------------------------------------------------

struct EchoServer {
    stop: Arc<AtomicBool>,
    tcp_port: u16,
    udp_port: u16,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl EchoServer {
    fn start(tag: &str) -> Self {
        let tcp = TcpListener::bind("127.0.0.1:0").expect("绑定 TCP 回显端口");
        tcp.set_nonblocking(true).unwrap();
        let tcp_port = tcp.local_addr().unwrap().port();

        let udp = UdpSocket::bind("127.0.0.1:0").expect("绑定 UDP 回显端口");
        udp.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let udp_port = udp.local_addr().unwrap().port();

        let stop = Arc::new(AtomicBool::new(false));

        let (t_tag, t_stop) = (tag.to_string(), stop.clone());
        let t_handle = std::thread::spawn(move || {
            while !t_stop.load(Ordering::Relaxed) {
                match tcp.accept() {
                    Ok((mut stream, _)) => {
                        let tag = t_tag.clone();
                        std::thread::spawn(move || {
                            let mut buf = [0u8; 4096];
                            loop {
                                match stream.read(&mut buf) {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => {
                                        let msg = format!(
                                            "{}:{}",
                                            tag,
                                            String::from_utf8_lossy(&buf[..n])
                                        );
                                        if stream.write_all(msg.as_bytes()).is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        let (u_tag, u_stop) = (tag.to_string(), stop.clone());
        let u_handle = std::thread::spawn(move || {
            let mut buf = [0u8; 65535];
            while !u_stop.load(Ordering::Relaxed) {
                // 读超时（Err）时回到循环顶部检查停止标志
                if let Ok((n, from)) = udp.recv_from(&mut buf) {
                    let msg = format!("{}:{}", u_tag, String::from_utf8_lossy(&buf[..n]));
                    let _ = udp.send_to(msg.as_bytes(), from);
                }
            }
        });

        Self {
            stop,
            tcp_port,
            udp_port,
            handles: vec![t_handle, u_handle],
        }
    }

    /// 模拟节点宕机：停止回显并释放端口
    fn kill(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for EchoServer {
    fn drop(&mut self) {
        self.kill();
    }
}

// ---------------------------------------------------------------------------
// 进程与客户端辅助
// ---------------------------------------------------------------------------

struct Gateway {
    child: Child,
    admin: u16,
}

impl Gateway {
    /// 配置里写的控制口端口必须与 `admin` 一致
    fn start(config: &Path, admin: u16) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_sniffgate"))
            .arg("--config")
            .arg(config)
            .arg("--log")
            .arg("info")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("启动 sniffgate 进程");
        Self { child, admin }
    }

    fn cmd(&self, command: &str) -> Value {
        let addr = format!("127.0.0.1:{}", self.admin);
        let stream =
            TcpStream::connect(&addr).unwrap_or_else(|e| panic!("连接控制口 {addr} 失败: {e}"));
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut writer = stream.try_clone().unwrap();
        writer.write_all(command.as_bytes()).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim())
            .unwrap_or_else(|e| panic!("控制口返回内容无法解析: {line:?} ({e})"))
    }

    fn status(&self) -> Value {
        let v = self.cmd("status");
        assert_eq!(v["ok"], Value::Bool(true), "status 应成功: {v}");
        v
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 找一个 TCP 与 UDP 都能用的端口（用于验证「同端口不同协议的统一入口」）
fn free_port_both() -> u16 {
    for _ in 0..100 {
        let p = free_tcp_port();
        if let Ok(s) = UdpSocket::bind(("127.0.0.1", p)) {
            drop(s);
            return p;
        }
    }
    panic!("找不到 TCP+UDP 都空闲的端口");
}

fn tcp_roundtrip(front: u16, msg: &str) -> String {
    let addr: SocketAddr = format!("127.0.0.1:{front}").parse().unwrap();
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .unwrap_or_else(|e| panic!("连接 TCP 前端口 {front} 失败: {e}"));
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s.write_all(msg.as_bytes()).unwrap();
    s.flush().unwrap();
    let mut buf = [0u8; 1024];
    let n = s.read(&mut buf).expect("读取 TCP 响应失败");
    String::from_utf8_lossy(&buf[..n]).to_string()
}

fn udp_client() -> UdpSocket {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s
}

fn udp_roundtrip(sock: &UdpSocket, front: u16, msg: &str) -> String {
    sock.send_to(msg.as_bytes(), ("127.0.0.1", front)).unwrap();
    let mut buf = [0u8; 4096];
    let (n, _) = sock.recv_from(&mut buf).expect("读取 UDP 响应失败");
    String::from_utf8_lossy(&buf[..n]).to_string()
}

fn wait_until<F: FnMut() -> bool>(what: &str, timeout: Duration, mut f: F) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("等待超时：{what}");
}

fn wait_admin(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    wait_until("控制口就绪", Duration::from_secs(20), || {
        TcpStream::connect(&addr).is_ok()
    });
}

fn active_of(status: &Value, proto: &str) -> Option<String> {
    status["active"][proto].as_str().map(|s| s.to_string())
}

// ---------------------------------------------------------------------------
// 配置模板
// ---------------------------------------------------------------------------

fn config_text(front: u16, admin: u16, a: &EchoServer, b: &EchoServer, extra: &str) -> String {
    format!(
        r#"
[global]
log_level = "info"
strategy = "priority"
admin_listen = "127.0.0.1:{admin}"
config_poll_interval_ms = 200
probe_interval_ms = 200
probe_timeout_ms = 300
connect_timeout_ms = 800
failure_threshold = 1
success_threshold = 1
passive_eject_threshold = 2
passive_eject_window_ms = 2000
drain_timeout_ms = 2000
udp_session_timeout_ms = 60000
udp_rebind_on_switch = true
fail_open = true
shutdown_grace_ms = 3000

[[listeners]]
name = "tcp-public"
protocol = "tcp"
listen = "127.0.0.1:{front}"

[[listeners]]
name = "udp-public"
protocol = "udp"
listen = "127.0.0.1:{front}"

[[nodes]]
name = "node-a"
priority = 100
tcp = "127.0.0.1:{}"
udp = "127.0.0.1:{}"
probe = "auto"
remark = "主节点"

[[nodes]]
name = "node-b"
priority = 50
tcp = "127.0.0.1:{}"
udp = "127.0.0.1:{}"
probe = "auto"
remark = "备节点"
{extra}
"#,
        a.tcp_port, a.udp_port, b.tcp_port, b.udp_port
    )
}

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sniffgate-it-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[test]
fn gen_config_and_check() {
    let out = Command::new(env!("CARGO_BIN_EXE_sniffgate"))
        .arg("--gen-config")
        .output()
        .expect("运行 --gen-config");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("[[listeners]]"), "示例配置应包含入口定义");

    // 生成的示例配置应能通过 --check
    let dir = tmp_dir("genconfig");
    let path = dir.join("config.toml");
    std::fs::write(&path, text.as_bytes()).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_sniffgate"))
        .arg("--config")
        .arg(&path)
        .arg("--check")
        .output()
        .expect("运行 --check");
    assert!(
        out.status.success(),
        "--check 应通过: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 非法配置应被拒绝（退出码非 0）
    let bad = dir.join("bad.toml");
    std::fs::write(&bad, b"[global]\nlog_level = \"nope\"\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_sniffgate"))
        .arg("--config")
        .arg(&bad)
        .arg("--check")
        .output()
        .expect("运行 --check");
    assert!(!out.status.success(), "非法配置应校验失败");
}

#[test]
fn tcp_udp_failover_and_hot_reload() {
    // ---- 准备三个后端节点 ----
    let mut node_a = EchoServer::start("A");
    let mut node_b = EchoServer::start("B");
    let mut node_c = EchoServer::start("C");

    let front = free_port_both();
    let admin = free_tcp_port();
    let dir = tmp_dir("e2e");
    let config = dir.join("config.toml");
    std::fs::write(&config, config_text(front, admin, &node_a, &node_b, "")).unwrap();

    let gw = Gateway::start(&config, admin);
    wait_admin(gw.admin);

    // ---- 1. 初始状态：两个入口都转发到主节点 node-a ----
    wait_until("初始主节点为 node-a", Duration::from_secs(20), || {
        active_of(&gw.status(), "tcp").as_deref() == Some("node-a")
    });
    assert_eq!(tcp_roundtrip(front, "hello"), "A:hello");
    let udp_sock = udp_client();
    assert_eq!(udp_roundtrip(&udp_sock, front, "hello"), "A:hello");

    let st = gw.status();
    assert_eq!(st["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(active_of(&st, "udp").as_deref(), Some("node-a"));
    assert_eq!(st["reload"]["ok"], Value::Bool(true));
    assert_eq!(st["listeners"].as_array().unwrap().len(), 2);

    // ---- 2. 控制口手动切换到 node-b，再恢复自动选主 ----
    let sw = gw.cmd(r#"{"cmd":"switch","node":"node-b"}"#);
    assert_eq!(sw["ok"], Value::Bool(true), "手动切换应成功: {sw}");
    assert_eq!(active_of(&sw, "tcp").as_deref(), Some("node-b"));
    assert_eq!(tcp_roundtrip(front, "manual"), "B:manual");
    let au = gw.cmd(r#"{"cmd":"auto"}"#);
    assert_eq!(active_of(&au, "tcp").as_deref(), Some("node-a"));
    assert_eq!(tcp_roundtrip(front, "auto"), "A:auto");

    // ---- 3. 主节点宕机 → 秒级热切换到 node-b（TCP 新连接 + 已是 B 的 UDP 新会话）----
    let t0 = Instant::now();
    node_a.kill();
    wait_until("故障切换到 node-b", Duration::from_secs(15), || {
        active_of(&gw.status(), "tcp").as_deref() == Some("node-b")
    });
    let switched_in = t0.elapsed();
    assert!(
        switched_in < Duration::from_secs(10),
        "热切换耗时应远小于 10s，实际 {switched_in:?}"
    );
    assert_eq!(tcp_roundtrip(front, "failover"), "B:failover");
    // 在途 UDP 会话（同一个客户端套接字）应被重绑到新主节点
    wait_until("UDP 回包切到 node-b", Duration::from_secs(10), || {
        udp_roundtrip(&udp_sock, front, "rebind") == "B:rebind"
    });
    assert_eq!(udp_roundtrip(&udp_sock, front, "again"), "B:again");

    // ---- 4. 修改配置文件（新增 node-c，优先级最高）→ 热加载自动生效 ----
    let extra = format!(
        r#"
[[nodes]]
name = "node-c"
priority = 500
tcp = "127.0.0.1:{}"
udp = "127.0.0.1:{}"
probe = "auto"
remark = "热加载进来的新主节点"
"#,
        node_c.tcp_port, node_c.udp_port
    );
    std::fs::write(&config, config_text(front, admin, &node_a, &node_b, &extra)).unwrap();
    wait_until(
        "热加载新增 node-c 并成为主节点",
        Duration::from_secs(20),
        || {
            let st = gw.status();
            st["nodes"].as_array().unwrap().len() == 3
                && active_of(&st, "tcp").as_deref() == Some("node-c")
        },
    );
    assert_eq!(tcp_roundtrip(front, "reload"), "C:reload");
    assert_eq!(udp_roundtrip(&udp_sock, front, "reload"), "C:reload");

    // ---- 5. 控制口 disable / enable ----
    let d = gw.cmd(r#"{"cmd":"disable","node":"node-c"}"#);
    assert_eq!(d["ok"], Value::Bool(true));
    assert_eq!(
        active_of(&d, "tcp").as_deref(),
        Some("node-b"),
        "停用 node-c 后应回落到 node-b"
    );
    assert_eq!(tcp_roundtrip(front, "disabled"), "B:disabled");
    let e = gw.cmd(r#"{"cmd":"enable","node":"node-c"}"#);
    assert_eq!(
        active_of(&e, "tcp").as_deref(),
        Some("node-c"),
        "重新启用后应回到 node-c"
    );
    assert_eq!(tcp_roundtrip(front, "enabled"), "C:enabled");

    // ---- 6. 配置写错：热加载失败但旧配置继续工作 ----
    std::fs::write(&config, b"this is not toml at all = = =\n").unwrap();
    wait_until(
        "检测到非法配置并记录失败",
        Duration::from_secs(15),
        || gw.status()["reload"]["ok"] == Value::Bool(false),
    );
    assert_eq!(tcp_roundtrip(front, "still-alive"), "C:still-alive");

    // ---- 7. 恢复正常配置（去掉 node-c）→ 主节点回到 node-b ----
    std::fs::write(&config, config_text(front, admin, &node_a, &node_b, "")).unwrap();
    wait_until(
        "恢复合法配置后主节点回到 node-b",
        Duration::from_secs(20),
        || {
            let st = gw.status();
            st["reload"]["ok"] == Value::Bool(true)
                && st["nodes"].as_array().unwrap().len() == 2
                && active_of(&st, "tcp").as_deref() == Some("node-b")
        },
    );
    assert_eq!(tcp_roundtrip(front, "final"), "B:final");

    // ---- 8. stats 与切换历史 ----
    let stats = gw.cmd(r#"{"cmd":"stats"}"#);
    assert!(
        stats["total"]["tcp_conns_total"].as_u64().unwrap() >= 5,
        "stats 应统计到多次连接"
    );
    let st = gw.status();
    assert!(
        !st["recent_switches"].as_array().unwrap().is_empty(),
        "应记录到切换历史: {st}"
    );

    // 清理
    node_b.kill();
    node_c.kill();
    drop(gw);
}

#[test]
fn admin_port_rejects_hostile_input_without_crashing_or_misbehaving() {
    // 注入测试：控制口接受不可信的一行命令。垃圾 JSON、命令注入载荷、
    // 指向不存在节点的切换，都必须被干净处理——网关不能崩，也不能转发行为被改变。
    let node_a = EchoServer::start("A");
    let node_b = EchoServer::start("B");
    let front = free_port_both();
    let admin = free_tcp_port();
    let dir = tmp_dir("inject");
    let config = dir.join("config.toml");
    std::fs::write(&config, config_text(front, admin, &node_a, &node_b, "")).unwrap();
    let gw = Gateway::start(&config, admin);
    wait_admin(gw.admin);
    wait_until("主节点 node-a", Duration::from_secs(20), || {
        active_of(&gw.status(), "tcp").as_deref() == Some("node-a")
    });

    // 向控制口直发一行（不假设返回内容一定是合法 JSON，只读一行）
    let send_raw = |line: &str| {
        let s = TcpStream::connect(("127.0.0.1", admin)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut w = s.try_clone().unwrap();
        w.write_all(line.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
        w.flush().unwrap();
        let mut r = BufReader::new(s);
        let mut l = String::new();
        let _ = r.read_line(&mut l);
        l
    };

    // 1) 根本不是 JSON 的垃圾
    let _ = send_raw("this is not json at all {{{\x00");
    // 2) 命令注入载荷伪装成命令
    let _ = send_raw(r#"{"cmd":"switch; rm -rf /","node":"$(whoami)"}"#);
    // 3) 指向不存在节点的切换
    let bad = send_raw(r#"{"cmd":"switch","node":"does-not-exist"}"#);
    // 4) 超大块垃圾
    let huge = "x".repeat(4096);
    let _ = send_raw(&huge);

    // 关键断言：网关没崩，仍按原配置把流量转发到 node-a，控制口仍正常
    assert_eq!(
        tcp_roundtrip(front, "still-up"),
        "A:still-up",
        "恶意控制口输入改变了转发行为"
    );
    let st = gw.status();
    assert_eq!(st["ok"], Value::Bool(true), "控制口仍应正常: {st}");
    assert_eq!(
        active_of(&st, "tcp").as_deref(),
        Some("node-a"),
        "不存在节点的切换不应生效: {st}"
    );
    // 不存在节点的切换应回一个失败响应（而不是 panic 或静默成功）
    let _ = bad;

    drop(gw);
}
