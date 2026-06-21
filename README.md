# Arrow Flight Raw Parquet Worker

Rust Arrow Flight data-plane worker focused on high-throughput raw Parquet reads and writes against S3-compatible storage. The Java coordinator owns planning, auth, Iceberg commits, and `GetFlightInfo`; this worker accepts direct `DoPut` and `DoGet` calls for already-planned physical files.

## Layout

- `src/main.rs`: data-plane worker server and worker-registry heartbeat.
- `src/flight_service.rs`: direct `DoPut` Arrow stream decode -> async Parquet writer -> S3, and direct `DoGet` Parquet -> Arrow Flight stream.
- `src/put_model.rs`: worker `DoPut` result/profile/file contracts returned to the coordinator.
- `src/admission.rs`: read/write slot guards.
- `src/ticket.rs`: worker `DoGet` ticket parser.
- `src/metrics.rs`: low-overhead Prometheus text metrics endpoint.
- `db/migrations/`: SQL DDL owned by the control plane/coordinator in production. Local Compose applies it with `metadata-migrate`.
- `benchmarks/tools/`: benchmark/data-generation Rust binaries.
- `benchmarks/tools/common/`: benchmark-only profiling/output helpers.
- `benchmarks/scripts/`: benchmark shell entrypoints.
- `dev/`: local environment scripts and compatibility wrappers.

## Requirements

The repo pins Rust `1.88.0` in `rust-toolchain.toml` so the current Arrow, tonic, and object_store stack can be used. First build may ask `rustup` to install that toolchain.

Docker is only needed for the MinIO/server compose environment.

## Fast Start

Start everything in Docker:

```bash
./dev/up.sh
```

Or run only MinIO in Docker and the server as a local release binary:

```bash
./dev/up.sh --minio-only
./dev/server-local.sh
```

Generate test Arrow IPC data:

```bash
./benchmarks/scripts/generate_arrow.sh 1gb data/test-1gb.arrow
```

Benchmark write:

```bash
./benchmarks/scripts/bench_put.sh data/test-1gb.arrow bench/test-1gb.parquet
```

Benchmark write split into target-sized Parquet objects:

```bash
PUT_PARALLELISM=4 ./benchmarks/scripts/bench_put.sh data/test-1gb.arrow bench/test-1gb.parquet 256mb
```

Profiling is opt-in. A normal write benchmark prints only the core client-side result and the compact `put_result` summary. Add `--profile` with the Rust binary, pass `true` as the fourth script argument, or set `PUT_PROFILE=true` to request server-side `DoPut` profiling:

```bash
PUT_PROFILE=true ./benchmarks/scripts/bench_put.sh data/test-1gb.arrow bench/profiled.parquet 256mb
cargo run --release --bin bench-put -- --input data/test-1gb.arrow --path bench/profiled.parquet --file-size 256mb --profile
```

When profiling is enabled, the benchmark also prints client Arrow IPC source-read timing and a server-side stage breakdown:

- `client_source.ipc_read_ms`: client time spent reading/decompressing Arrow IPC source batches before Flight encoding.
- `profile.receive_decode_ms`: server time waiting for and decoding Flight batches.
- `profile.enqueue_wait_ms`: time blocked handing batches to writer tasks.
- `profile.collect_writer_wait_ms`: time waiting for active part writers to finish when the parallelism limit is reached or at final drain.
- `profile.writer_task_write_ms_sum` / `writer_task_write_ms_max`: total writer CPU/object-writer time across all part tasks, plus the slowest single writer.
- `profile.writer_task_flush_ms_sum` and `writer_task_close_ms_sum`: Parquet flush/finalization and object upload close time.
- `part_profiles`: per-output-file rows, batches, Parquet bytes, and writer timings.

For very many output files, the client prints the first 16 part profiles by default. Override with:

```bash
PROFILE_PART_LIMIT=64 PUT_PROFILE=true ./benchmarks/scripts/bench_put.sh data/test-1gb.arrow bench/test-1gb.parquet 128mb
```

Benchmark read:

```bash
./benchmarks/scripts/bench_get.sh bench/test-1gb.parquet
```

Run a small end-to-end smoke:

```bash
./dev/smoke.sh
```

Run a write parallelism matrix against the Docker Compose server:

```bash
./benchmarks/scripts/bench_put_matrix.sh data/test-1gb.arrow 1gb matrix 256mb
```

Run the benchmark client and data generator inside Docker as well:

```bash
BENCH_SIZE=2gb BENCH_MODE=single BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
BENCH_SIZE=2gb BENCH_MODE=multi PUT_STREAMS=6 BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
```

The Docker benchmark service stores generated Arrow IPC files in a named Docker volume at `/bench-data`, so repeated runs can reuse the same input without reading it from the host filesystem. Force regeneration with:

```bash
BENCH_REGENERATE=true BENCH_SIZE=2gb ./dev/bench-docker.sh
```

Use `BENCH_FILE_SIZE=none` to keep single-object write mode instead of target-sized dataset parts.

Server-side write knobs such as `PUT_PARALLELISM` and `PARQUET_COMPRESSION` are applied when the Compose server is started. For example:

```bash
PUT_PARALLELISM=4 PARQUET_COMPRESSION=snappy BENCH_MODE=multi PUT_STREAMS=6 ./dev/bench-docker.sh
```

For repeated runs against an already-started server, skip the Compose `up` step:

```bash
BENCH_SKIP_UP=true BENCH_MODE=single ./dev/bench-docker.sh
```

## Performance Knobs

Useful environment variables:

- `PARQUET_COMPRESSION=uncompressed|snappy|lz4_raw`
- `PARQUET_DICTIONARY=false`
- `PARQUET_MAX_ROW_GROUP_ROWS=1048576`
- `PARQUET_WRITE_BATCH_SIZE=65536`
- `PARQUET_FLUSH_THRESHOLD_BYTES=268435456`
- `PUT_PARALLELISM=4`
- `PUT_QUEUE_DEPTH=2`
- `PUT_MAX_ACTIVE_STREAMS=16`
- `READ_MAX_ACTIVE_STREAMS=16`
- `READ_SLOT_WAIT_MS=30000`
- `WORKER_FLIGHT_URI=http://127.0.0.1:50051`
- `WORKER_REQUIRE_STRUCTURED_TICKETS=false`
- `WORKER_HEARTBEAT_INTERVAL_MS=5000`
- `WORKER_REGISTRY_TTL_MS=15000`
- `METRICS_ENABLED=true`
- `METRICS_ADDR=0.0.0.0:9090`
- `S3_MULTIPART_PART_SIZE=67108864`
- `S3_MULTIPART_MAX_CONCURRENCY=16`
- `FLIGHT_MAX_MESSAGE_SIZE=268435456`
- `FLIGHT_DATA_CHUNK_SIZE=16777216`
- `READ_BATCH_SIZE=65536`
- `TARGET_FILE_SIZE=256mb` for client-side benchmark scripts
- `PUT_PROFILE=true` to request optional benchmark/server profiling

Defaults favor the fastest measured local write path: uncompressed Parquet, dictionary disabled, large Flight chunks, large gRPC message limits, 64 MiB S3 multipart uploads, and up to four active part writers when target-sized output is requested.

Without `--file-size` / `TARGET_FILE_SIZE`, `DoPut` writes one Parquet object at the requested path. With a target file size, `DoPut` writes multiple ordered part files under `<name>.parts/` and returns the file list in the `DoPut` result and metadata DB. `PUT_PARALLELISM` controls the maximum number of active part writers. `DoGet` accepts a direct physical Parquet path ticket and streams that file back.

The worker publishes readiness/capacity through the `worker_registry` table and the Arrow Flight `worker.status` action. `GetFlightInfo`, Iceberg snapshot choice, and table-level read planning are intentionally coordinator-owned.

Metrics are exposed as Prometheus text at `http://127.0.0.1:9090/metrics`, with a cheap `/healthz` endpoint on the same listener. Metrics are intentionally low-cardinality and request-level only: no per-batch metrics, no object-path labels, and no upload-id labels. This keeps the hot path to a few atomic increments and one latency histogram update per completed operation.

`DoGet` accepts direct worker tickets. For compatibility with local benchmarks, raw path tickets are allowed by default. For coordinator-driven environments, prefer structured JSON tickets and set `WORKER_REQUIRE_STRUCTURED_TICKETS=true`:

```json
{
  "path": "table/date=2026-06-20/part-00000.parquet",
  "operation_id": "read-123",
  "expires_at_ms": 1781971200000
}
```

This is a routing/expiry contract, not cryptographic authorization yet. Signed tickets or mTLS-bound tickets should be the next production hardening step.

Worker metadata DDL lives in `db/migrations/0001_worker_metadata.sql`. The worker can still run this migration for local experiments with `METADATA_DB_AUTO_MIGRATE=true`, but the default is `false`; production should apply migrations from the coordinator/control-plane release process.

For real S3 over a slower link, try `PARQUET_COMPRESSION=lz4_raw` or `snappy`, and benchmark `TARGET_FILE_SIZE`, `PUT_PARALLELISM`, `S3_MULTIPART_PART_SIZE`, and `S3_MULTIPART_MAX_CONCURRENCY` as a set.

MinIO console: http://127.0.0.1:9001

Credentials:

```text
minioadmin / minioadmin
```
