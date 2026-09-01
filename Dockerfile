FROM rust:1.95-slim-trixie AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . .
RUN cargo build --release --locked


FROM debian:trixie-slim

COPY --from=builder /app/target/release/rln-companion /usr/bin/rln-companion

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates openssl curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENTRYPOINT ["/usr/bin/rln-companion"]
