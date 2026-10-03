# 测试说明（sniffgate）

sniffgate 是 TCP/UDP 故障切换网关。测试分两层：`src/main.rs` 内 `#[cfg(test)]`
单元测试，以及 `tests/integration.rs` 端到端（真实拉起 sniffgate 进程 + 本地
TCP/UDP 回显后端）。

## 怎么跑

```powershell
cargo test                 # 全部
cargo test --test integration   # 端到端（failover / 热加载 / 注入）
```

端到端测试全部用 127.0.0.1 随机端口，**不需要外网**。

## 预期结果

全部通过，0 失败：

- 单元测试：13。
- 集成测试：3（`gen_config_and_check`、`tcp_udp_failover_and_hot_reload`、
  `admin_port_rejects_hostile_input_without_crashing_or_misbehaving`）。

## 测了什么

- **转发**：TCP/UDP 同端口统一入口、节点前缀识别流量落点。
- **故障切换**：主节点宕机后秒级热切换、在途 UDP 会话重绑到新主节点。
- **热加载**：改配置文件后新增节点/主备关系生效；非法配置加载失败但旧配置继续服务。
- **控制口**：status / switch / auto / disable / enable / stats。
- **注入（`admin_port_...`）**：控制口接受不可信的一行命令。证明：
  - 非 JSON 垃圾、含 `; rm -rf /` / `$(whoami)` 的命令注入载荷、超大块输入，
    都不会让网关崩溃；
  - 指向不存在节点的切换被拒绝（不生效）；
  - 恶意输入之后，正常转发仍按原配置落到 node-a，控制口仍响应。

## 备注

- 本仓库**没有插件/钩子/事件回调机制**（纯四层转发 + 主动/被动健康探测），
  故不涉及钩子生命周期测试。
- 不提供静态文件服务、不解析 HTTP body，路径穿越 / XSS 不适用。
