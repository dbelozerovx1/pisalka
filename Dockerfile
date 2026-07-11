FROM rust:1.88.0-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
COPY e2e/tools ./e2e/tools
RUN cargo build --release \
    --bin flight-server \
    --bin e2e-generate \
    --bin e2e-write \
    --bin e2e-read \
    --bin e2e-create-schema

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV RUST_LOG=info

FROM runtime AS e2e-client

COPY --from=builder /app/target/release/e2e-generate /usr/local/bin/e2e-generate
COPY --from=builder /app/target/release/e2e-write /usr/local/bin/e2e-write
COPY --from=builder /app/target/release/e2e-read /usr/local/bin/e2e-read
COPY --from=builder /app/target/release/e2e-create-schema /usr/local/bin/e2e-create-schema

ENV FLIGHT_MAX_MESSAGE_SIZE=268435456 \
    FLIGHT_DATA_CHUNK_SIZE=16777216

FROM runtime AS server

COPY --from=builder /app/target/release/flight-server /usr/local/bin/flight-server

EXPOSE 50051
CMD ["flight-server"]
