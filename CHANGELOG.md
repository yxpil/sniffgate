# Changelog

本项目的所有重要变更都会记录在这里。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [0.1.0] - 2026-10-03

首个可用版本。

### 新增

* **对外统一入口**：`[[listeners]]` 支持任意多个 TCP / UDP 入口，TCP 与 UDP 可共用同一端口号。
* **多节点嗅探**
  * 主动嗅探：每节点独立探测任务，`auto/tcp/udp/none` 四种模式，支持探测载荷（文本或 `hex:`）与期望响应校验。
  * 被动嗅探：真实转发失败在时间窗口内达阈值即刻摘除节点。
  * 健康状态机：`Unknown → Healthy / Unhealthy`，失败/恢复阈值可配。
* **主备热切换**
  * `priority` 主备策略；另提供 `round_robin` / `weighted` 多活策略。
  * 路由表以不可变快照 + `ArcSwap` 无锁发布，切换对在途连接零阻塞。
  * TCP 旧连接排空（`drain_timeout_ms`，0 = 立即断开；节点重新提升则取消排空）。
  * UDP 会话级转发、空闲回收、切换时重绑到新主节点。
  * `fail_open` 全挂兜底，避免整体黑洞。
* **配置文件热加载**：节点增删改、入口增删改（端口变化自动重启入口）、全局参数全部在线生效；
  解析/校验失败时保留旧配置并把错误写入控制口。
* **运维控制口**：行式 JSON 与文本命令（`status` / `stats` / `reload` / `switch` / `auto` /
  `enable` / `disable` / `health` / `help`），内置切换历史。
* **CLI**：`--config` / `--check` / `--gen-config` / `--log` / `--admin` / `--cmd`（含交互模式）。
* **工程化**：13 个单元测试 + 2 个真实进程端到端测试；GitHub Actions 在 Linux/Windows 上跑
  `fmt` / `clippy -D warnings` / `test` / `release build`。
* **文档与部署样例**：README、架构说明、部署指南（systemd / WinSW / Docker）、运维手册、
  `config.example.toml`、`Dockerfile`、`deploy/*`。

[0.1.0]: https://github.com/yxpil/sniffgate/releases/tag/v0.1.0
