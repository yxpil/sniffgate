# ---- 构建
FROM rust:1-slim-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# ---- 运行
FROM debian:bookworm-slim
RUN useradd -r -s /usr/sbin/nologin sniffgate \
 && mkdir -p /etc/sniffgate \
 && apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/sniffgate /usr/local/bin/sniffgate
# 示例配置（挂载真实配置覆盖它）
COPY config.example.toml /etc/sniffgate/config.toml
USER sniffgate
EXPOSE 9000/tcp
EXPOSE 9000/udp
EXPOSE 9100/tcp
VOLUME ["/etc/sniffgate"]
ENTRYPOINT ["/usr/local/bin/sniffgate"]
CMD ["--config", "/etc/sniffgate/config.toml"]
