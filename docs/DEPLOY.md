# 部署指南

## 1. 编译产物

```bash
cargo build --release
# Linux/macOS: target/release/sniffgate
# Windows:     target\release\sniffgate.exe
```

交叉编译 / 静态链接（musl）：

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

## 2. 目录与端口规划建议

```text
/opt/sniffgate/
├── sniffgate               # 二进制
├── config.toml             # 配置（可热改）
└── logs/sniffgate.log      # 由 systemd 收集
```

| 端口 | 用途 | 暴露范围 |
| --- | --- | --- |
| 9000/tcp + 9000/udp | 对外统一入口 | 公网 / 业务网 |
| 9100/tcp | 运维控制口 | **仅本机或跳板机** |

## 3. Linux（systemd）

```bash
sudo useradd -r -s /sbin/nologin sniffgate
sudo mkdir -p /opt/sniffgate
sudo cp target/release/sniffgate /opt/sniffgate/
sudo cp config.toml /opt/sniffgate/
sudo cp deploy/sniffgate.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sniffgate
journalctl -u sniffgate -f
```

`deploy/sniffgate.service` 已包含重启策略、日志与安全加固项。若要监听 1024 以下端口，二选一：

```bash
# a) 授权能力（推荐，无需 root 运行）
sudo setcap cap_net_bind_service=+ep /opt/sniffgate/sniffgate

# b) 或使用 AmbientCapabilities（见 unit 文件注释）
```

防火墙：

```bash
sudo ufw allow 9000/tcp && sudo ufw allow 9000/udp
sudo firewall-cmd --add-port=9000/tcp --add-port=9000/udp --permanent && sudo firewall-cmd --reload
```

放宽文件描述符与 UDP 缓冲（高并发/UDP 大流量时）：

```bash
# /etc/security/limits.d/sniffgate.conf
sniffgate soft nofile 1048576
sniffgate hard nofile 1048576

# /etc/sysctl.d/99-sniffgate.conf
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.udp_mem = 65536 131072 262144
```

## 4. Windows（注册为服务）

### 方式一：WinSW（推荐）

1. 下载 [WinSW](https://github.com/winsw/winsw/releases)，重命名为 `sniffgate-service.exe`，与 `sniffgate.exe` 放同一目录；
2. 复制 `deploy/winsw-sniffgate.xml` 为 `sniffgate-service.xml`，按需修改路径；
3. 以管理员身份执行：

```powershell
.\sniffgate-service.exe install
.\sniffgate-service.exe start
.\sniffgate-service.exe status
# 卸载： .\sniffgate-service.exe stop ; .\sniffgate-service.exe uninstall
```

### 方式二：sc.exe（原生，日志需自行重定向）

```powershell
sc.exe create sniffgate binPath= "C:\sniffgate\sniffgate.exe --config C:\sniffgate\config.toml" start= auto
sc.exe start sniffgate
sc.exe query sniffgate
```

防火墙：

```powershell
New-NetFirewallRule -DisplayName "sniffgate TCP" -Direction Inbound -Protocol TCP -LocalPort 9000 -Action Allow
New-NetFirewallRule -DisplayName "sniffgate UDP" -Direction Inbound -Protocol UDP -LocalPort 9000 -Action Allow
```

> 提示：Windows 下把控制口绑定在 `127.0.0.1:9100`，不要对外监听。

## 5. Docker

```bash
docker build -t sniffgate:0.1.0 .
docker run -d --name sniffgate --restart unless-stopped \
  -p 9000:9000/tcp -p 9000:9000/udp \
  -p 127.0.0.1:9100:9100 \
  -v /etc/sniffgate/config.toml:/etc/sniffgate/config.toml:ro \
  sniffgate:0.1.0
```

容器内入口建议监听 `0.0.0.0:9000`，控制口 `0.0.0.0:9100`（只映射到宿主 `127.0.0.1`）。
配置文件以只读方式挂载也能热加载——程序监听的是宿主机文件的变化，`mtime` 变化会透传进容器。

## 6. 上线检查清单

- [ ] `sniffgate --config config.toml --check` 通过
- [ ] 入口地址与端口、后端节点地址正确（`nmap`/`nc` 从客户端侧验证）
- [ ] 控制口只对本机/跳板机开放
- [ ] 用 `status` 确认当前主节点符合预期
- [ ] 制造一次计划内故障（`disable` 主节点）确认切换与回切都正常
- [ ] 日志接入（journald / filebeat / 事件日志）并配置告警关键字：`主备热切换`、`禁用`、`节点不可用`
- [ ] 修改一次配置并观察 `status.reload` 确认热加载生效；故意写错一次确认「失败保留旧配置」
- [ ] 压测：`nofile`、`somaxconn`、UDP 缓冲、后端节点连接数上限
