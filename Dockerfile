FROM rust:1.88.0-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
COPY benchmarks/tools ./benchmarks/tools
RUN cargo build --release \
    --bin flight-server \
    --bin gen-arrow \
    --bin bench-put \
    --bin bench-put-multi \
    --bin bench-get \
    --bin bench-coordinator \
    --bin coordinator-query \
    --bin coordinator-action

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV RUST_LOG=info

FROM runtime AS bench

COPY --from=builder /app/target/release/gen-arrow /usr/local/bin/gen-arrow
COPY --from=builder /app/target/release/bench-put /usr/local/bin/bench-put
COPY --from=builder /app/target/release/bench-put-multi /usr/local/bin/bench-put-multi
COPY --from=builder /app/target/release/bench-get /usr/local/bin/bench-get
COPY --from=builder /app/target/release/bench-coordinator /usr/local/bin/bench-coordinator
COPY --from=builder /app/target/release/coordinator-query /usr/local/bin/coordinator-query
COPY --from=builder /app/target/release/coordinator-action /usr/local/bin/coordinator-action
COPY benchmarks/docker/bench.sh /usr/local/bin/bench-docker

RUN chmod +x /usr/local/bin/bench-docker

ENV FLIGHT_URI=http://flight-server:50051 \
    FLIGHT_MAX_MESSAGE_SIZE=268435456 \
    FLIGHT_DATA_CHUNK_SIZE=16777216 \
    BENCH_DATA_DIR=/bench-data

ENTRYPOINT ["/usr/local/bin/bench-docker"]

FROM runtime AS server

COPY --from=builder /app/target/release/flight-server /usr/local/bin/flight-server

EXPOSE 50051
CMD ["flight-server"]
