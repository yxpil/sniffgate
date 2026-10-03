# 运维手册

## 1. 控制口速查

```bash
G=./sniffgate                      # 二进制
A="127.0.0.1:9100"                 # 控制口

$G --admin $A --cmd status                 # 全景状态
$G --admin $A --cmd stats                  # 累计统计
$G --admin $A --cmd '{"cmd":"reload"}'     # 立刻重载配置（不用等轮询）
$G --admin $A --cmd '{"cmd":"switch","node":"node-b","protocol":"tcp"}'   # 手动切主
$G --admin $A --cmd '{"cmd":"auto"}'       # 恢复自动选主
$G --admin $A --cmd '{"cmd":"disable","node":"node-c"}'   # 摘除节点（维护）
$G --admin $A --cmd '{"cmd":"enable","node":"node-c"}'    # 放回节点
$G --admin $A --cmd '{"cmd":"health","node":"node-a","healthy":false}'  # 手动标记异常
$G --admin $A --cmd -                      # 交互模式
```

只看关键字段（Linux/macOS 有 jq 时）：

```bash
$G --admin $A --cmd status | jq '{active, pins, nodes: [.nodes[] | {name, healthy, state, tcp, udp}]}'
$G --admin $A --cmd stats  | jq .total
```

## 2. 典型场景

### 2.1 计划内维护（优雅摘除主节点）

```bash
$G --admin $A --cmd '{"cmd":"disable","node":"node-a"}'   # 立刻切到 node-b，node-a 进入排空
# ... 维护 ...
$G --admin $A --cmd '{"cmd":"enable","node":"node-a"}'    # 放回：优先级更高会自动回切
```

因为 `priority` 高，`enable` 后 node-a 会自动重新成为主节点；若不希望自动回切，
可以 `--cmd '{"cmd":"switch","node":"node-b"}'` 把它**粘住**（直到 node-b 故障或执行 `auto`）。

### 2.2 灰度 / 多活

* 临时把 `strategy` 改成 `round_robin`（或 `weighted` + `weight`）保存文件 → 自动热加载；
* 想快速回滚：控制口 `reload` 前先备份文件，或直接改回 `priority` 保存。

### 2.3 新增后端节点（不改流量）

1. 先确认新节点本地服务正常；
2. 编辑 `config.toml` 追加 `[[nodes]]`（`priority` 设为比当前主节点低 → 只做备份，不抢流量）；
3. 保存 → 观察日志 `热加载成功 ... 新增 1`；
4. `status` 确认新节点 `healthy: true`；
5. 需要接管流量时再手动 `switch` 或调高 `priority`（再保存一次即可）。

### 2.4 后端服务重发（连接会被拒绝一段时间）

被动嗅探会在窗口内失败达阈值时立即摘除该节点，客户端感知到的失败仅限「最初的几条连接」。
如果后端重启窗口较长，建议提前 `disable`，避免抖动。

## 3. 排查手册

| 现象 | 排查步骤 |
| --- | --- |
| 客户端连不上入口 | `ss -lntup \| grep 9000`（Windows: `netstat -ano \| findstr 9000`）确认监听；看日志是否「绑定失败，重试」→ 端口被占用 |
| 入口在但转发失败 | `status` 看 `nodes[].healthy`；若都 `unhealthy` 且 `fail_open=false` 则拒绝转发（符合预期） |
| 频繁切换（抖动） | 调大 `failure_threshold` / `success_threshold`，调大 `probe_timeout_ms`，检查网络抖动或后端慢启动 |
| 配置改了不生效 | `status.reload`：`ok=false` 时看 `message`（TOML 语法/字段名/校验失败都会在这里）；`--check` 可离线校验 |
| 端口改了没生效 | 确认改的是 `[[listeners]].listen`（会重启入口）而不是只改了注释；看日志「入口配置变更，重启该入口」 |
| UDP 回包异常/丢失 | 检查 `udp_rebind_on_switch`、`udp_session_timeout_ms`；后端是否严格从「请求来源地址」回包（本程序只接受后端地址回包） |
| 老连接一直不断 | 被降级节点上的连接会在 `drain_timeout_ms` 后关闭；`0` 表示立即关闭 |
| 想彻底确认走的哪条链路 | 后端服务打日志，或临时给每个节点返回不同内容（端到端测试就是这么做的） |

日志关键字（建议告警）：

```text
主备热切换        切换发生（含 from/to/reason/epoch）
节点不可用        探测失败达到阈值
节点被降级，进入排空
节点提升为主节点
热加载失败        配置错误，旧配置仍在服务
入口异常退出      端口绑定失败等
```

## 4. FAQ

**Q: 客户端 IP 会丢吗？**
TCP 是透传字节流，后端看到的是 sniffgate 的 IP（就像普通正向代理）。若需要真实客户端 IP，
需应用层配合（如 PROXY protocol，见 Roadmap）。

**Q: 支持 IPv6 吗？**
支持。`listen = "[::]:9000"`，后端也可写 `[2001:db8::1]:9000`。

**Q: 后端地址能写域名吗？**
可以，TCP 连接和 UDP 会话都会做解析（每次建连/建会话时解析，便于配合 DNS 做后端漂移）。

**Q: 多个 sniffgate 实例能做集群吗？**
当前是**无状态单实例**语义：多实例并跑就是多个独立网关（前置四层 LB 分流量即可），
互不感知，各自按同一份配置做主备切换。集群化一致性在 Roadmap。

**Q: 会不会出现「所有节点都不健康」？**
默认 `fail_open = true`，此时仍按优先级兜底转发（比整体黑洞更好）。
想严格失败可用 `fail_open = false`，此时无健康节点就拒绝转发（对应协议返回连接失败/丢包）。

**Q: 我能只跑 TCP 或只跑 UDP 吗？**
可以，删掉对应的 `[[listeners]]`；节点也可以只配 `tcp` 或只配 `udp`。
