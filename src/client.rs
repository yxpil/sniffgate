//! 极简控制口客户端：`sniffgate --admin 127.0.0.1:9100 --cmd status`
//! `--cmd -` 进入交互模式（逐行发送命令）。

use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

pub fn run(addr: &str, cmd: &str, timeout_ms: u64) -> Result<()> {
    let stream = TcpStream::connect(addr).with_context(|| format!("连接运维控制口失败: {addr}"))?;
    stream.set_read_timeout(Some(Duration::from_millis(timeout_ms)))?;
    stream.set_write_timeout(Some(Duration::from_millis(timeout_ms)))?;
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    if cmd == "-" {
        let stdin = std::io::stdin();
        let mut line = String::new();
        loop {
            line.clear();
            print!("sniffgate> ");
            std::io::stdout().flush().ok();
            if stdin.read_line(&mut line)? == 0 {
                break;
            }
            let cmd = line.trim();
            if cmd.is_empty() {
                continue;
            }
            writer.write_all(cmd.as_bytes())?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            let mut resp = String::new();
            if reader.read_line(&mut resp)? == 0 {
                break;
            }
            print_reply(resp.trim());
            if cmd == "quit" || cmd == "exit" {
                break;
            }
        }
        return Ok(());
    }

    writer.write_all(cmd.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    let mut resp = String::new();
    if reader.read_line(&mut resp)? == 0 {
        anyhow::bail!("控制口没有返回内容");
    }
    print_reply(resp.trim());
    Ok(())
}

fn print_reply(line: &str) {
    match serde_json::from_str::<serde_json::Value>(line) {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| line.to_string())
        ),
        Err(_) => println!("{line}"),
    }
}
