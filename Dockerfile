FROM rust:1.88.0-bookworm AS builder

WORKDIR /app
COPY Cargo.toml rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --bin flight-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/flight-server /usr/local/bin/flight-server

ENV RUST_LOG=info
EXPOSE 50051
CMD ["flight-server"]
