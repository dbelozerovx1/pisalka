# Arrow Flight S3 MVP

Rust Arrow Flight service focused on high-throughput `DoPut` into Parquet on S3-compatible storage. The local environment uses MinIO.

## Layout

- `src/main.rs`: Flight server.
- `src/flight_service.rs`: `DoPut` Arrow stream decode -> async Parquet writer -> S3, and `DoGet` Parquet -> Arrow Flight stream.
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
- `profile.manifest_put_ms`: manifest commit time for target-sized datasets.
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

## Performance Knobs

Useful environment variables:

- `PARQUET_COMPRESSION=uncompressed|snappy|lz4_raw`
- `PARQUET_DICTIONARY=false`
- `PARQUET_MAX_ROW_GROUP_ROWS=1048576`
- `PARQUET_WRITE_BATCH_SIZE=65536`
- `PARQUET_FLUSH_THRESHOLD_BYTES=268435456`
- `PUT_PARALLELISM=4`
- `PUT_QUEUE_DEPTH=2`
- `S3_MULTIPART_PART_SIZE=67108864`
- `S3_MULTIPART_MAX_CONCURRENCY=16`
- `FLIGHT_MAX_MESSAGE_SIZE=268435456`
- `FLIGHT_DATA_CHUNK_SIZE=16777216`
- `READ_BATCH_SIZE=65536`
- `TARGET_FILE_SIZE=256mb` for client-side benchmark scripts
- `PUT_PROFILE=true` to request optional benchmark/server profiling

Defaults favor the fastest measured local write path: uncompressed Parquet, dictionary disabled, large Flight chunks, large gRPC message limits, 64 MiB S3 multipart uploads, and up to four active part writers when target-sized output is requested.

Without `--file-size` / `TARGET_FILE_SIZE`, `DoPut` writes one Parquet object at the requested path. With a target file size, `DoPut` writes a logical dataset: ordered part files under `<name>.parts/` plus a manifest at `<name>.parquet.manifest.json`. `PUT_PARALLELISM` then controls the maximum number of active part writers. `DoGet` accepts the original logical path and streams the dataset back.

For real S3 over a slower link, try `PARQUET_COMPRESSION=lz4_raw` or `snappy`, and benchmark `TARGET_FILE_SIZE`, `PUT_PARALLELISM`, `S3_MULTIPART_PART_SIZE`, and `S3_MULTIPART_MAX_CONCURRENCY` as a set.

MinIO console: http://127.0.0.1:9001

Credentials:

```text
minioadmin / minioadmin
```
