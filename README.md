# Arrow Flight Raw Parquet Worker

Rust Arrow Flight data-plane worker focused on high-throughput raw Parquet reads and writes against S3-compatible storage. The Java coordinator owns planning, auth, Iceberg commits, and `GetFlightInfo`; the worker accepts direct `DoPut` and `DoGet` calls for already-planned physical files.

The MVP keeps worker and coordinator in one repo because their capability contract, Compose environment, and smoke tests are still evolving together. Once the coordinator contract stabilizes and release cadence diverges, splitting the Java coordinator into a separate repository will be straightforward.

## Layout

- `src/main.rs`: data-plane worker server and worker-registry heartbeat.
- `src/flight_service.rs`: direct `DoPut` Arrow stream decode -> async Parquet writer -> S3, and direct `DoGet` Parquet -> Arrow Flight stream.
- `src/capability.rs`: signed worker capability verification for coordinator-delegated physical work.
- `src/put_model.rs`: worker `DoPut` result/profile/file contracts returned to the coordinator.
- `src/admission.rs`: read/write admission guards.
- `src/resource.rs`: weighted memory-budget limiter used by admission and scheduler signals.
- `src/ticket.rs`: worker `DoGet` ticket parser.
- `src/metrics.rs`: low-overhead Prometheus text metrics endpoint.
- `coordinator-java/`: dependency-light Java coordinator MVP for Trino CTAS and signed worker capability minting.
- `db/migrations/`: SQL DDL owned by the control plane/coordinator in production. Local Compose applies it with `metadata-migrate`.
- `benchmarks/tools/`: benchmark/data-generation Rust binaries.
- `benchmarks/tools/common/`: benchmark-only profiling/output helpers.
- `benchmarks/scripts/`: benchmark shell entrypoints.
- `dev/`: local environment scripts and compatibility wrappers.

## Requirements

The repo pins Rust `1.88.0` in `rust-toolchain.toml` so the current Arrow, tonic, and object_store stack can be used. First build may ask `rustup` to install that toolchain.

Docker is only needed for the local MinIO, Trino, Hive Metastore, coordinator, and worker compose environment.

## Fast Start

Start everything in Docker:

```bash
./dev/up.sh
```

The Compose file contains MinIO, a standalone Hive Metastore, Trino with an Iceberg catalog, the Rust worker, and the Java coordinator. Trino is exposed on `http://127.0.0.1:8080`, the coordinator is exposed on `http://127.0.0.1:8088`, and the worker is exposed on `127.0.0.1:50051`.

`trino-init` waits for Trino and best-effort creates the Iceberg schema. Keep `TRINO_INIT_REQUIRE_SCHEMA=false` for raw Flight worker smoke tests; set it to `true` when testing strict Iceberg schema bootstrap.

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

Run the coordinator-first smoke path. This generates Arrow IPC data inside Docker, asks the coordinator to create an upload session, writes with the issued signed `DoPut` stream ticket, finishes the upload so the coordinator validates worker metadata and produces a Trino DDL preview, asks the coordinator for a signed exact-file `DoGet` ticket for one written Parquet file, and reads it back:

```bash
./dev/coordinator-smoke.sh
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
- `PUT_MAX_STREAMS_PER_UPLOAD=8`
- `PUT_SCHEDULER_RESERVED_SLOTS=0`
- `WORKER_MEMORY_BYTES=` detected from cgroup when possible, otherwise 16 GiB fallback
- `WORKER_RESERVED_MEMORY_BYTES=` reserved process/system headroom, otherwise max(512 MiB, 20%)
- `PUT_MEMORY_PERCENT=55`
- `PUT_MEMORY_BUDGET_BYTES=` explicit write memory pool override
- `PUT_MAX_STREAM_MEMORY_BYTES=` per admitted DoPut memory charge
- `PUT_MAX_RECORD_BATCH_BYTES=` max decoded DoPut batch memory estimate
- `READ_MEMORY_PERCENT=30`
- `READ_MEMORY_BUDGET_BYTES=` explicit read memory pool override
- `READ_MAX_STREAM_MEMORY_BYTES=` per admitted DoGet memory charge
- `READ_MAX_RECORD_BATCH_BYTES=` max decoded DoGet batch memory estimate
- `READ_MAX_BATCH_ROWS=1048576`
- `READ_MAX_ACTIVE_STREAMS=16`
- `READ_MAX_STREAMS_PER_OPERATION=8`
- `READ_SCHEDULER_RESERVED_SLOTS=0`
- `READ_SLOT_WAIT_MS=30000`
- `WORKER_ZONE=` optional coordinator locality hint
- `WORKER_FLIGHT_URI=http://127.0.0.1:50051` worker-published URI stored in `worker_registry`; in Docker Compose this defaults to `http://flight-server:50051`
- `WORKER_REQUIRE_STRUCTURED_TICKETS=false`
- `WORKER_REQUIRE_SIGNED_CAPABILITIES=false`
- `WORKER_CAPABILITY_SECRET=`
- `WORKER_REQUIRE_CAPABILITY_WORKER_ID=false`
- `WORKER_CAPABILITY_MAX_TTL_MS=3600000`
- `WORKER_HEARTBEAT_INTERVAL_MS=5000`
- `WORKER_REGISTRY_TTL_MS=15000`
- `METRICS_ENABLED=true`
- `METRICS_ADDR=0.0.0.0:9090`
- `COORDINATOR_ADDR=0.0.0.0:8088`
- `TRINO_VERSION=481`
- `HIVE_VERSION=4.1.0`
- `TRINO_URI=http://trino:8080`
- `TRINO_CATALOG=iceberg`
- `TRINO_SCHEMA=arrow`
- `ICEBERG_SCHEMA_LOCATION=s3://arrow-flight/iceberg/arrow`
- `CTAS_DEFAULT_CATALOG=iceberg`
- `CTAS_DEFAULT_SCHEMA=arrow`
- `COORDINATOR_CAPABILITY_SECRET=local-dev-secret`
- `COORDINATOR_ADMIN_TOKEN=` optional token required by low-level ticket APIs when set
- `COORDINATOR_METADATA_DATABASE_URL=postgres://flight:flight@metadata-db:5432/flight_metadata`
- `COORDINATOR_UPLOAD_SESSION_TTL_MS=3600000`
- `COORDINATOR_OBJECT_STORE_URI_PREFIX=s3://arrow-flight`
- `COORDINATOR_DEFAULT_UPLOAD_STREAMS=1`
- `S3_MULTIPART_PART_SIZE=67108864`
- `S3_MULTIPART_MAX_CONCURRENCY=16`
- `FLIGHT_MAX_MESSAGE_SIZE=268435456`
- `FLIGHT_DATA_CHUNK_SIZE=16777216`
- `READ_BATCH_SIZE=65536`
- `TARGET_FILE_SIZE=256mb` for client-side benchmark scripts
- `PUT_PROFILE=true` to request optional benchmark/server profiling

Defaults favor the fastest measured local write path: uncompressed Parquet, dictionary disabled, large Flight chunks, large gRPC message limits, 64 MiB S3 multipart uploads, and up to four active part writers when target-sized output is requested.

## Java Coordinator MVP

The coordinator intentionally stays small for the first full-system run:

- `POST /v1/ctas`: accepts user SQL, wraps it as `CREATE TABLE ... AS <sql>`, forwards the caller's `Authorization` header and `X-Trino-User` to Trino, and returns Trino query metadata.
- `POST /v1/flight/create-upload`: creates a durable upload session, selects workers from `worker_registry`, persists planned stream attempts, and returns one signed `DoPut` ticket per granted stream.
- `POST /v1/flight/upload-status`: returns the planned session, stream status joined with worker stream metadata, and written file rows.
- `POST /v1/flight/finish-upload`: explicit finalization gate. It succeeds only when all planned streams are `SUCCEEDED`, reads DB-backed worker file/schema metadata, marks the upload `READY_TO_COMMIT`, and returns the file list plus a Trino `CREATE TABLE` DDL preview.
- `POST /v1/flight/abort-upload`: marks an upload `ABORTED`. Staged-file cleanup is intentionally a separate follow-up task.
- `POST /v1/flight/put-ticket`: selects a write-capable worker from `worker_registry` and mints one signed `DoPut` capability for a coordinator-controlled staging prefix. This remains a low-level/internal dev API; session uploads should use `create-upload`.
- `POST /v1/flight/get-ticket`: selects a read-capable worker from `worker_registry` and mints a signed exact-file `DoGet` ticket. Also internal unless protected by `COORDINATOR_ADMIN_TOKEN`.
- `GET /v1/config`: shows non-secret coordinator config.

Example CTAS call:

```bash
curl -sS http://127.0.0.1:8088/v1/ctas \
  -H 'content-type: application/json' \
  -H 'X-Trino-User: alice' \
  -H 'Authorization: Bearer <trino-token>' \
  -d '{"sql":"select * from source_table limit 10","targetTable":"iceberg.arrow.ctas_tmp_demo"}'
```

Example put capability:

```bash
curl -sS http://127.0.0.1:8088/v1/flight/create-upload \
  -H 'content-type: application/json' \
  -d '{"operationId":"op-1","streams":2,"tableName":"iceberg.arrow.demo_upload"}'
```

Each returned ticket contains a `descriptorPath` and `appMetadata` signed JSON envelope to send as `DoPut` app metadata. For production, the coordinator should only create upload sessions after Trino/Ranger has authorized the user's table operation.

Commit/readiness flow:

1. Workers publish `worker_registry` heartbeats with their Flight URI, live read/write capacity, memory-derived recommended streams, utilization, EWMA wait/throughput, and selection score.
2. `create-upload` selects active, non-draining, heartbeat-fresh workers from `worker_registry`; coordinator has no configured worker id/url.
3. `create-upload` persists `coordinator_upload_sessions` and `coordinator_upload_streams` rows before data moves.
4. Each worker `DoPut` persists its stream status, Arrow schema JSON from the first decoded batch, and written Parquet file rows in `worker_put_streams` / `worker_put_files`.
5. `finish-upload` compares planned streams to actual worker outcomes. Missing, running, rejected, or failed streams block finalization.
6. When all streams succeeded, the coordinator returns `READY_TO_COMMIT` with DB-backed file paths and a generated Trino DDL preview. Files are still staged until a later Iceberg commit/add-files step makes them table-visible.

Without `--file-size` / `TARGET_FILE_SIZE`, `DoPut` writes one Parquet object at the requested path. With a target file size, `DoPut` writes multiple ordered part files under `<name>.parts/` and returns the file list in the `DoPut` result and metadata DB. `PUT_PARALLELISM` controls the maximum number of active part writers. `DoGet` accepts a direct physical Parquet path ticket and streams that file back.

The worker publishes readiness/capacity through the `worker_registry` table and the Arrow Flight `worker.status` / `worker.scheduler.v1` actions. `GetFlightInfo`, Iceberg snapshot choice, and table-level read planning are intentionally coordinator-owned.

Worker admission is resource-derived first, with slots kept as a coarse CPU/task safety rail. The worker detects or receives its memory flavor, reserves headroom, splits the remaining budget between write and read pools, and charges every admitted stream a configurable memory budget. The coordinator-facing grant for a worker is effectively:

```text
worker_recommended_streams = min(
  active_slot_limit - active_streams - reserved_slots,
  memory_available_bytes / max_stream_memory_bytes,
  max_streams_per_operation
)
```

`FLIGHT_MAX_MESSAGE_SIZE` caps individual gRPC messages before decode. `PUT_MAX_RECORD_BATCH_BYTES` and `READ_MAX_RECORD_BATCH_BYTES` reject decoded Arrow batches that exceed the worker flavor budget. This is the Rust-side equivalent of allocator discipline in Java Arrow: there is no shared `BufferAllocator` tree here, so admission, frame limits, batch limits, backpressure, and worker-level budgets are the control points.

The coordinator should not ask users for table or upload size. For `DoPut`, grant streams from client capability, tenant quota, and live worker recommendations:

```text
granted_put_streams = min(
  client_parallel_write_cap,
  tenant_put_limit,
  sum(worker.scheduler.put.recommended_streams),
  upload_session_cap
)
```

For `DoGet`, the coordinator should group known parquet files into read fragments first, then cap by client/tenant/worker capacity:

```text
granted_get_tickets = min(
  read_fragments,
  client_parallel_read_cap,
  tenant_read_limit,
  sum(worker.scheduler.read.recommended_streams)
)
```

Each worker computes `recommended_streams` from current available slots, reserved slots, current free memory budget, and per-operation caps. `selection_score`, utilization, EWMA admission wait, EWMA duration, EWMA throughput, and failure/rejection counters are included so the coordinator can use power-of-two choices or weighted selection instead of HAProxy round-robin. The worker remains the final admission authority and can still return `RESOURCE_EXHAUSTED` when its state changes between ticket creation and stream open.

Coordinator-facing worker status shape:

```json
{
  "worker_id": "local-worker",
  "flight_uri": "http://127.0.0.1:50051",
  "locality": { "zone": null },
  "state": "ACTIVE",
  "put": { "limit": 16, "active": 0, "available": 16, "slot_wait_ms": 30000 },
  "read": { "limit": 16, "active": 0, "available": 16, "slot_wait_ms": 30000 },
  "resources": {
    "worker_memory_bytes": 17179869184,
    "reserved_memory_bytes": 3435973836,
    "put": {
      "limit_bytes": 7559184384,
      "active_bytes": 0,
      "available_bytes": 7559184384,
      "max_stream_memory_bytes": 944892805,
      "max_record_batch_bytes": 268435456,
      "max_batch_rows": null
    },
    "read": {
      "limit_bytes": 4123168604,
      "active_bytes": 0,
      "available_bytes": 4123168604,
      "max_stream_memory_bytes": 515396075,
      "max_record_batch_bytes": 134217728,
      "max_batch_rows": 1048576
    }
  },
  "scheduler": {
    "put": {
      "recommended_streams": 8,
      "max_streams_per_operation": 8,
      "soft_available_slots": 8,
      "memory_available_streams": 8,
      "utilization_per_mille": 0,
      "selection_score": 5000
    },
    "read": {
      "recommended_streams": 8,
      "max_streams_per_operation": 8,
      "soft_available_slots": 8,
      "memory_available_streams": 8,
      "utilization_per_mille": 0,
      "selection_score": 5000
    }
  },
  "capabilities": {
    "capability_version": 1,
    "signed_capabilities_required": true,
    "capability_worker_binding_required": true
  }
}
```

The same scheduler fields are persisted into `worker_registry` as columns for simple SQL filtering and ordering, while the full payload is kept in `status_json`.

Metrics are exposed as Prometheus text at `http://127.0.0.1:9090/metrics`, with a cheap `/healthz` endpoint on the same listener. Metrics are intentionally low-cardinality and request-level only: no per-batch metrics, no object-path labels, and no upload-id labels. This keeps the hot path to a few atomic increments and one latency histogram update per completed operation.

## Worker Security Contract

Trino/Ranger remains the user authorization authority. The Java coordinator checks the user's Trino token, plans Iceberg/control-plane work, then delegates only narrow physical work to Rust workers through signed capabilities. Workers do not call Ranger and do not make table-level auth decisions; they only verify that the coordinator signed this exact operation before touching S3.

For local benchmarks, unsigned raw `DoGet` paths and unsigned `DoPut` metadata remain allowed by default. For production data-plane workers, set:

```bash
WORKER_REQUIRE_SIGNED_CAPABILITIES=true
WORKER_CAPABILITY_SECRET=<shared-secret-from-secret-store>
WORKER_REQUIRE_CAPABILITY_WORKER_ID=true
WORKER_REQUIRE_STRUCTURED_TICKETS=true
PUT_REQUIRE_STAGING_PREFIX=true
```

Signed capabilities use an HMAC-SHA256 envelope. The coordinator base64url-encodes the UTF-8 JSON payload without padding, signs that encoded payload string, and sends:

```json
{
  "version": 1,
  "alg": "HS256",
  "payload": "<base64url-json-payload>",
  "signature": "<base64url-hmac-sha256(payload)>"
}
```

`DoPut` can send the envelope either as the whole Flight `app_metadata` body or inside existing metadata as `{"capability": { ... }}`. A production put payload should look like:

```json
{
  "op": "put",
  "operation_id": "op-123",
  "attempt_id": "attempt-1",
  "upload_id": "upload-123",
  "stream_id": "stream-0",
  "worker_id": "local-worker",
  "issued_at_ms": 1781970000000,
  "expires_at_ms": 1781970900000,
  "allowed_output_prefix": "tables/db/table/.staging/op-123/",
  "staging_prefix": "tables/db/table/.staging/op-123/",
  "target_file_size": 536870912,
  "max_stream_bytes": 10737418240,
  "max_upload_streams": 4,
  "max_record_batch_bytes": 268435456
}
```

The worker rejects a `DoPut` unless the object key is inside `allowed_output_prefix`, optional client ids match the signed ids, requested limits do not exceed signed limits, the ticket is not expired, and the ticket is bound to this worker when worker binding is required. This prevents a user who learned a Flight URL from writing arbitrary S3 keys.

`DoGet` receives the signed envelope as the Flight ticket. A production read payload is exact-file scoped:

```json
{
  "op": "get",
  "operation_id": "read-123",
  "worker_id": "local-worker",
  "issued_at_ms": 1781970000000,
  "expires_at_ms": 1781970300000,
  "path": "tables/db/table/data/part-00000.parquet",
  "max_batch_rows": 65536,
  "max_record_batch_bytes": 134217728
}
```

For compatibility with local benchmarks, raw path tickets are still allowed when signed capabilities are not required:

```json
{
  "path": "table/date=2026-06-20/part-00000.parquet",
  "operation_id": "read-123",
  "expires_at_ms": 1781971200000
}
```

That compatibility mode should not be exposed outside a trusted local/dev network.

Worker metadata DDL lives in `db/migrations/0001_worker_metadata.sql`. The worker can still run this migration for local experiments with `METADATA_DB_AUTO_MIGRATE=true`, but the default is `false`; production should apply migrations from the coordinator/control-plane release process.

For real S3 over a slower link, try `PARQUET_COMPRESSION=lz4_raw` or `snappy`, and benchmark `TARGET_FILE_SIZE`, `PUT_PARALLELISM`, `S3_MULTIPART_PART_SIZE`, and `S3_MULTIPART_MAX_CONCURRENCY` as a set.

MinIO console: http://127.0.0.1:9001

Credentials:

```text
minioadmin / minioadmin
```
