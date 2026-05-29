# Arrow Flight S3 MVP

Rust Arrow Flight service focused on high-throughput `DoPut` into Parquet on S3-compatible storage. The local environment uses MinIO.

## Layout

- `src/main.rs`: Flight server.
- `src/flight_service.rs`: `DoPut` Arrow stream decode -> async Parquet writer -> S3, and `DoGet` Parquet -> Arrow Flight stream.
- `src/bin/gen_arrow.rs`: local Arrow IPC stream generator for configurable test sizes.
- `src/bin/bench_put.rs`: `DoPut` benchmark client.
- `src/bin/bench_get.rs`: `DoGet` benchmark client.
- `dev/`: one-command local scripts.

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
./dev/generate_arrow.sh 1gb data/test-1gb.arrow
```

Benchmark write:

```bash
./dev/bench_put.sh data/test-1gb.arrow bench/test-1gb.parquet
```

Benchmark write split into target-sized Parquet objects:

```bash
PUT_PARALLELISM=4 ./dev/bench_put.sh data/test-1gb.arrow bench/test-1gb.parquet 256mb
```

Benchmark read:

```bash
./dev/bench_get.sh bench/test-1gb.parquet
```

Run a small end-to-end smoke:

```bash
./dev/smoke.sh
```

Run a write parallelism matrix against the Docker Compose server:

```bash
./dev/bench_put_matrix.sh data/test-1gb.arrow 1gb matrix 256mb
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

Defaults favor the fastest measured local write path: uncompressed Parquet, dictionary disabled, large Flight chunks, large gRPC message limits, 64 MiB S3 multipart uploads, and up to four active part writers when target-sized output is requested.

Without `--file-size` / `TARGET_FILE_SIZE`, `DoPut` writes one Parquet object at the requested path. With a target file size, `DoPut` writes a logical dataset: ordered part files under `<name>.parts/` plus a manifest at `<name>.parquet.manifest.json`. `PUT_PARALLELISM` then controls the maximum number of active part writers. `DoGet` accepts the original logical path and streams the dataset back.

For real S3 over a slower link, try `PARQUET_COMPRESSION=lz4_raw` or `snappy`, and benchmark `TARGET_FILE_SIZE`, `PUT_PARALLELISM`, `S3_MULTIPART_PART_SIZE`, and `S3_MULTIPART_MAX_CONCURRENCY` as a set.

MinIO console: http://127.0.0.1:9001

Credentials:

```text
minioadmin / minioadmin
```
