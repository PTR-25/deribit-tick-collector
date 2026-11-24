# syntax=docker/dockerfile:1

FROM rust:1.88-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config ca-certificates cmake build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml ./
COPY src ./src
COPY tests ./tests

RUN cargo build --release --locked 2>/dev/null || cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 collector

USER collector
WORKDIR /home/collector

COPY --from=builder /app/target/release/deribit-tick-collector /usr/local/bin/deribit-tick-collector
COPY config.example.toml /home/collector/config.toml

VOLUME ["/data"]
EXPOSE 9108

ENTRYPOINT ["deribit-tick-collector"]
CMD ["collect", "--config", "/home/collector/config.toml", "--data-dir", "/data"]
