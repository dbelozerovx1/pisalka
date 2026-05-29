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
./dev/bench_put_matrix.sh data/test-1gb.arrow 1gb matrix
```

## Performance Knobs

Useful environment variables:

- `PARQUET_COMPRESSION=uncompressed|snappy|lz4_raw`
- `PARQUET_DICTIONARY=false`
- `PARQUET_MAX_ROW_GROUP_ROWS=1048576`
- `PARQUET_WRITE_BATCH_SIZE=65536`
- `PARQUET_FLUSH_THRESHOLD_BYTES=268435456`
- `PUT_PARALLELISM=1`
- `PUT_QUEUE_DEPTH=2`
- `S3_MULTIPART_PART_SIZE=67108864`
- `S3_MULTIPART_MAX_CONCURRENCY=16`
- `FLIGHT_MAX_MESSAGE_SIZE=268435456`
- `FLIGHT_DATA_CHUNK_SIZE=16777216`
- `READ_BATCH_SIZE=65536`

Defaults favor the fastest measured local write path: uncompressed Parquet, dictionary disabled, large Flight chunks, large gRPC message limits, one Parquet writer, and 64 MiB S3 multipart uploads.

With `PUT_PARALLELISM=1`, `DoPut` writes one Parquet object at the requested path. With `PUT_PARALLELISM>1`, `DoPut` writes a logical dataset: part files under `<name>.parts/` plus a manifest at `<name>.parquet.manifest.json`. `DoGet` accepts the original logical path and streams the dataset back. Dataset mode is marked unordered because batches are distributed across writers for throughput.

For real S3 over a slower link, try `PARQUET_COMPRESSION=lz4_raw` or `snappy`, and benchmark `PUT_PARALLELISM`, `S3_MULTIPART_PART_SIZE`, and `S3_MULTIPART_MAX_CONCURRENCY` as a set.

MinIO console: http://127.0.0.1:9001

Credentials:

```text
minioadmin / minioadmin
```
