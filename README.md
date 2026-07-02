# Arrow Flight Raw Parquet Worker

Rust Arrow Flight data-plane worker focused on high-throughput raw Parquet reads and writes against S3-compatible storage. The Java coordinator owns planning, auth, Iceberg commits, and `GetFlightInfo`; the worker accepts direct `DoPut` and `DoGet` calls for already-planned physical files.

The MVP keeps worker and coordinator in one repo because their capability contract, Compose environment, and smoke tests are still evolving together. Once the coordinator contract stabilizes and release cadence diverges, splitting the Java coordinator into a separate repository will be straightforward.

Current MVP readiness, intentional Flight boundaries, and first Kubernetes test notes live in [docs/current-state.md](docs/current-state.md).

## Layout

- `src/main.rs`: data-plane worker server and worker-registry heartbeat.
- `src/flight_service.rs`: direct `DoPut` Arrow stream decode -> async Parquet writer -> S3, and direct `DoGet` Parquet -> Arrow Flight stream.
- `src/capability.rs`: signed worker capability verification for coordinator-delegated physical work.
- `src/put_model.rs`: worker `DoPut` result/profile/file contracts returned to the coordinator.
- `src/admission.rs`: read/write admission guards.
- `src/resource.rs`: weighted memory-budget limiter used by admission and scheduler signals.
- `src/ticket.rs`: worker `DoGet` ticket parser.
- `src/metrics.rs`: low-overhead Prometheus text metrics endpoint.
- `coordinator-java/`: dependency-light Java coordinator MVP for Trino CTAS, signed worker capability minting, and Flyway metadata migrations.
- `db/migrations/`: plain SQL DDL reference for the shared metadata schema.
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

The Compose file contains MinIO, a standalone Hive Metastore, Trino with an Iceberg catalog, two Rust workers, and the Java coordinator. Trino is exposed on `http://127.0.0.1:8080`, the coordinator Arrow Flight service is exposed at `http://127.0.0.1:8088`, coordinator metrics are exposed at `http://127.0.0.1:9091`, worker `local-worker` is exposed at `http://127.0.0.1:50051` with metrics on `http://127.0.0.1:9090`, and worker `local-worker-2` is exposed at `http://127.0.0.1:50052` with metrics on `http://127.0.0.1:9092`.

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

Run the simpler user-facing local E2E path. The write script accepts `schema.table` or `catalog.schema.table`, generates example Arrow data in Docker, uploads it through coordinator-issued worker `DoPut` tickets, commits the result into Iceberg, and verifies the table through Trino:

```bash
./dev/e2e-write-table.sh arrow.example_events
E2E_SIZE=1gb E2E_STREAMS=4 E2E_TARGET_FILE_SIZE=512mb ./dev/e2e-write-table.sh arrow.example_events
E2E_COMMIT_MODE=append ./dev/e2e-write-table.sh arrow.example_events
```

Then read it back through the coordinator CTAS planning path. The coordinator creates a generated CTAS temp table from the returned query id, polls Trino, discovers Iceberg files, returns worker `DoGet` tickets, the client prints a small Arrow table preview, and then calls `coordinator.drop-temp` by query id to drop the temp table:

```bash
./dev/e2e-read-table.sh arrow.example_events
E2E_READ_LIMIT=100 E2E_PREVIEW_ROWS=20 ./dev/e2e-read-table.sh arrow.example_events
E2E_READ_SQL='SELECT id, bucket, value FROM iceberg.arrow.example_events WHERE bucket < 3 LIMIT 20' ./dev/e2e-read-table.sh arrow.example_events
```

Run the coordinator-first smoke path. This generates Arrow IPC data inside Docker and then runs the Rust `bench-coordinator` example client. That client asks the coordinator to create an upload session, writes with every issued signed `DoPut` ticket, commits the worker-written Parquet files into Iceberg through the Java API, asks the coordinator for signed exact-file `DoGet` ticket(s), reads the data back, and verifies `SELECT count(*)` plus `SELECT * LIMIT 1` through Trino:

```bash
./dev/coordinator-smoke.sh
```

Useful knobs:

```bash
SMOKE_STREAMS=4 SMOKE_SIZE=256mb SMOKE_TARGET_FILE_SIZE=64mb ./dev/coordinator-smoke.sh
SMOKE_READ_BACK=all SMOKE_READ_MAX_FILES=4 ./dev/coordinator-smoke.sh
SMOKE_COMMIT_MODE=overwrite ./dev/coordinator-smoke.sh
SMOKE_COMMIT_MODE=none ./dev/coordinator-smoke.sh
```

Run a write parallelism matrix against the Docker Compose server:

```bash
./benchmarks/scripts/bench_put_matrix.sh data/test-1gb.arrow 1gb matrix 256mb
```

Run the benchmark client and data generator inside Docker as well:

```bash
BENCH_SIZE=2gb BENCH_MODE=single BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
BENCH_SIZE=2gb BENCH_MODE=multi PUT_STREAMS=6 BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
BENCH_SIZE=2gb BENCH_MODE=coordinator UPLOAD_STREAMS=4 BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
BENCH_MODE=coordinator COORDINATOR_COMMIT_MODE=append COORDINATOR_TABLE_NAME=iceberg.arrow.bench_upload ./dev/bench-docker.sh
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
- `WORKER_CPU_MILLICORES=` detected from cgroup when possible, otherwise available CPU fallback
- `WORKER_MEMORY_BYTES=` detected from cgroup when possible, otherwise 16 GiB fallback
- `PUT_MAX_ACTIVE_STREAMS=auto` defaults to roughly `ceil(cpu_cores * 2)`, capped at 16
- `PUT_MAX_STREAMS_PER_UPLOAD=auto` defaults to `min(put_slots, 4)`
- `PUT_SCHEDULER_RESERVED_SLOTS=auto` defaults to 1 only when the auto slot count is at least 4
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
- `READ_MAX_ACTIVE_STREAMS=auto` defaults to roughly `ceil(cpu_cores * 4)`, capped at 32
- `READ_MAX_STREAMS_PER_OPERATION=auto` defaults to `min(read_slots, 8)`
- `READ_SCHEDULER_RESERVED_SLOTS=auto` defaults to 1 only when the auto slot count is at least 4
- `READ_SLOT_WAIT_MS=30000`
- `WORKER_ZONE=` optional coordinator locality hint
- `WORKER_FLIGHT_URI=http://127.0.0.1:50051` worker-published URI stored in `worker_registry`; in Docker Compose this defaults to `http://flight-server:50051`; in Kubernetes endpoint mode it may be internal because `worker_client_endpoints` supplies the final client URI
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
- `COORDINATOR_METRICS_ENABLED=true`
- `COORDINATOR_METRICS_ADDR=0.0.0.0:9091`
- `COORDINATOR_K8S_WORKER_DISCOVERY_ENABLED=false` watches labeled Kubernetes Services and writes client-routable worker endpoints to Postgres when enabled
- `COORDINATOR_WORKER_CLIENT_ENDPOINTS_REQUIRED=false` requires a fresh `worker_client_endpoints` row during worker selection; defaults to `true` when Kubernetes discovery is enabled
- `COORDINATOR_K8S_WORKER_SERVICE_SELECTOR=role=flight-worker-client-endpoint`
- `COORDINATOR_K8S_WORKER_ID_LABEL=worker-id`
- `COORDINATOR_K8S_WORKER_FLIGHT_PORT_NAME=flight`
- `COORDINATOR_WORKER_CLIENT_URI_SCHEME=http`
- `TRINO_VERSION=476`
- `HIVE_VERSION=3.1.3`
- `HIVE_PLATFORM=linux/amd64`
- `TRINO_URI=http://trino:8080`
- `TRINO_CATALOG=iceberg`
- `TRINO_SCHEMA=arrow`
- `ICEBERG_SCHEMA_LOCATION=s3a://arrow-flight/iceberg/arrow`
- `ICEBERG_CATALOG_NAME=iceberg`
- `ICEBERG_CATALOG_URI=thrift://hive-metastore:9083`
- `ICEBERG_WAREHOUSE=s3a://arrow-flight/iceberg`
- `ICEBERG_HIVE_LOCK_ENABLED=false` disables Iceberg Hive metastore locks from the coordinator; this sets Hadoop config `iceberg.engine.hive.lock-enabled` and writes table property `engine.hive.lock-enabled` on coordinator-created tables
- `CTAS_DEFAULT_CATALOG=iceberg`
- `CTAS_DEFAULT_SCHEMA=arrow`
- `COORDINATOR_CAPABILITY_SECRET=local-dev-secret`
- `COORDINATOR_ADMIN_TOKEN=` optional token required by low-level ticket APIs when set
- `COORDINATOR_METADATA_DATABASE_URL=postgres://flight:flight@metadata-db:5432/flight_metadata`
- `COORDINATOR_UPLOAD_SESSION_TTL_MS=3600000`
- `COORDINATOR_QUERY_REGISTRY_TTL_MS=3600000`
- `COORDINATOR_QUERY_REGISTRY_CLEANUP_INTERVAL_MS=300000`
- `COORDINATOR_OBJECT_STORE_URI_PREFIX=s3a://arrow-flight`
- `COORDINATOR_DEFAULT_UPLOAD_STREAMS=1`
- `S3_MULTIPART_PART_SIZE=67108864`
- `S3_MULTIPART_MAX_CONCURRENCY=16`
- `FLIGHT_MAX_MESSAGE_SIZE=268435456`
- `FLIGHT_DATA_CHUNK_SIZE=16777216`
- `READ_BATCH_SIZE=65536`
- `TARGET_FILE_SIZE=256mb` for client-side benchmark scripts
- `PUT_PROFILE=true` to request optional benchmark/server profiling
- `COMPOSE_NETWORK_NAME=arrow-flight-local` keeps the local Docker network name URI-safe for the Hive 3 metastore client.

Defaults favor the fastest measured local write path: uncompressed Parquet, dictionary disabled, large Flight chunks, large gRPC message limits, 64 MiB S3 multipart uploads, and up to four active part writers when target-sized output is requested.

## Java Coordinator MVP

The coordinator is itself an Arrow Flight service. It does not expose REST endpoints. Standard Flight calls own query planning and polling; non-standard coordinator operations are Flight actions.

Standard Flight methods:

- `GetFlightInfo` with command `{"type":"ctas","sql":"select ...","user":"alice","authorization":"Bearer ..."}` creates a durable query row, generates a CTAS temp table from the returned query id unless `targetTable` is explicitly provided, submits one Trino CTAS request, and returns a `FlightInfo` with `app_metadata.queryId`.
- `PollFlightInfo` with command `{"type":"poll","queryId":"ctas_..."}` loads that row and advances one Trino cursor step. When CTAS finishes, polling switches to Iceberg `$files` discovery, persists discovered file rows, and finally returns `FlightInfo.endpoints` with worker `DoGet` tickets.
- `GetFlightInfo` with command `{"type":"read","path":"bench/file.parquet"}` is a low-level exact-file read planner. When `COORDINATOR_ADMIN_TOKEN` is configured this command must include `adminToken`.

Flight actions:

- `coordinator.create-upload`: creates a durable upload session, selects workers from `worker_registry`, resolves client endpoints when required, persists planned stream attempts, and returns one signed `DoPut` ticket per granted stream. Repeating the action with the same `uploadId` and same parameters can land on any coordinator replica and returns the persisted plan. When the planned mode is `overwrite` / `replace`, the coordinator first drops the target table through Trino and best-effort cleans the target table location, then issues tickets under the clean table `data/` directory.
- `coordinator.commit-upload`: explicit commit gate. The client calls it after it considers all `DoPut` streams complete. The coordinator reads currently recorded worker file rows, loads or creates the Iceberg table metadata through the Hive catalog, collects Parquet footer metrics, and commits that DB-backed file list. `append` requires an existing table and checks the uploaded Arrow-derived Iceberg schema against the existing table schema before committing. `overwrite` is recreate-style when planned at `create-upload`: old table/location are removed before data moves, then the new table metadata is created from the recorded Arrow schema and committed with the uploaded files.
- `coordinator.do-commit`: compatibility alias for `coordinator.commit-upload`.
- `coordinator.abort-upload`: marks an upload `ABORTED` and best-effort deletes the upload staging prefix.
- `coordinator.drop-temp`: drops a coordinator-created CTAS temp table by `queryId`, best-effort deletes its CTAS staging prefix, and marks the query row `DROPPED`. `coordinator.drop_temp` is accepted as an alias.
- `coordinator.put-ticket` and `coordinator.get-ticket`: low-level internal/dev ticket minting actions.
- `coordinator.config`: returns non-secret coordinator config.

Each upload ticket contains a `descriptorPath` and `appMetadata` signed JSON envelope to send as `DoPut` app metadata. For production, the coordinator should only create upload sessions after Trino/Ranger has authorized the user's table operation.

Query polling is client-driven. `GetFlightInfo` always creates a new simple `queryId`; the client stores that id and polls at its own cadence. The query id is also the stable handle for the generated CTAS table and object prefix. The coordinator persists status, progress, failure message, Trino cursor URI, final tickets, final file list, timestamps, and expiry in `coordinator_query_registry`. Expired rows are cleaned opportunistically by coordinator requests according to `COORDINATOR_QUERY_REGISTRY_CLEANUP_INTERVAL_MS`.

Read planning intentionally uses Iceberg `$files` metadata instead of direct S3 listing under the table location. `$files` follows the committed Iceberg snapshot and avoids returning old, orphaned, or partially cleaned objects that may still exist in object storage.

The coordinator does not store bearer tokens in the registry. If Trino requires per-user auth on cursor polling, pass the user's `authorization` value on every `PollFlightInfo` call or use a future service-token model.

For multi-coordinator and multi-worker Kubernetes deployment shape, see [docs/k8s-launch.md](/Users/dbelozerov/Repos/arrow-flight-s3-mvp/docs/k8s-launch.md).

Commit/readiness flow:

1. Workers publish `worker_registry` heartbeats with their Flight URI, live read/write capacity, memory-derived recommended streams, utilization, EWMA wait/throughput, and selection score.
2. In Kubernetes endpoint mode, the coordinator watches labeled worker Services and writes client-routable `ip:port` URIs into `worker_client_endpoints`.
3. `create-upload` selects active, non-draining, heartbeat-fresh workers from `worker_registry`; when client endpoints are required, it also requires a fresh `worker_client_endpoints` row and returns that URI to the client.
4. `create-upload` persists `coordinator_upload_sessions` and `coordinator_upload_streams` rows before data moves.
5. Each worker `DoPut` persists its stream status, Arrow schema JSON from the first decoded batch, and written Parquet file rows in `worker_put_streams` / `worker_put_files`.
6. The client decides when upload streams are complete and calls `commit-upload`.
7. `commit-upload` appends or overwrites Iceberg with the exact files currently stored in `worker_put_files`. If the client commits too early, only files recorded at that moment are part of the table. For `overwrite`, the destructive table drop/location cleanup happens during `create-upload`, before new data files are written.
8. A successful commit stores mode, table name, snapshot id, and commit summary on the upload session. Repeating `commit-upload` for an already committed upload returns the stored result instead of appending the same files again.
9. If a coordinator dies or fails around the Iceberg commit boundary, a later `commit-upload` on any coordinator first checks Iceberg snapshots for `coordinator.upload-id` and repairs the DB row when the snapshot already landed.
10. If commit fails before a committed Iceberg snapshot is visible, the coordinator marks the upload `FAILED`, best-effort deletes the upload's exact planned/recorded objects, and drops the table only for overwrite when the table did not exist before this commit attempt. Append failures never drop or clean the existing table location; only files recorded for this upload are cleaned. After a committed snapshot is visible, raw object deletion is skipped because the files are table data.

Coordinator-created uploads place Parquet files under the Iceberg table data directory: `<warehouse>/<schema>/<table>/data/`. Each planned stream receives a `flight-<uuid>.parquet` descriptor path. Without `--file-size` / `TARGET_FILE_SIZE`, `DoPut` writes that single Parquet object. With a target file size, `DoPut` writes ordered sibling part files such as `flight-<uuid>-part-00000.parquet` under the same `data/` directory and returns the file list in the `DoPut` result and metadata DB. `PUT_PARALLELISM` controls the maximum number of active part writers. `DoGet` accepts a direct physical Parquet path ticket and streams that file back.

For Iceberg v2 compatibility, coordinator table planning accepts Arrow timestamps only when the unit is `microsecond`. `Timestamp(Second)`, `Timestamp(Millisecond)`, `Timestamp(Nanosecond)`, missing timestamp units, and Arrow `Date64` are rejected instead of being silently converted or truncated.

Coordinator Flight errors are returned with a compact support handle:

```text
errorId=coord-err-... code=INVALID_REQUEST method=GetFlightInfo uploadId=... queryId=...: human readable message hint=optional next step
```

The same `errorId` is written to coordinator logs with internal details. User-fixable 4xx/409/503 errors keep their concrete message and hint; internal failures are shortened for clients and should be investigated by `errorId`. Worker `DoPut` failures that have parsed upload metadata similarly include `errorId=worker-err-...` plus `operationId`, `uploadId`, `streamId`, and `attemptId`.

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

Only the worker fields needed for coordinator scheduling are persisted into `worker_registry`: worker-published Flight URI, state/draining, read/write recommended streams, scores, utilization, admission wait EWMA, throughput EWMA, TTL, and heartbeat time. When Kubernetes endpoint discovery is enabled, client-routable addresses live in `worker_client_endpoints`. The full worker payload remains available through worker status actions and Prometheus metrics instead of being duplicated in Postgres.

Worker metrics are exposed as Prometheus text at `http://127.0.0.1:9090/metrics`; coordinator metrics are exposed at `http://127.0.0.1:9091/metrics`. Both listeners also expose cheap `/healthz` endpoints. Metrics are intentionally low-cardinality and request-level only: no per-batch metrics, no object-path labels, no upload-id labels, no query-id labels, and no `errorId` labels. This keeps the hot path to a few atomic increments while logs retain the exact request ids for investigation.

Coordinator errors are counted from the same formatter that creates user-facing `errorId` values:

```promql
sum(rate(coordinator_flight_errors_total{status=~"5.."}[5m]))
sum by (code) (rate(coordinator_flight_errors_total[5m]))
sum(rate(coordinator_flight_errors_total[5m])) / sum(rate(coordinator_flight_requests_total[5m]))
```

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
  "allowed_output_prefix": "iceberg/db/table/data/",
  "staging_prefix": "iceberg/db/table/data/",
  "path": "iceberg/db/table/data/flight-6f2f7a01-4b3a-4ad7-a0ef-a59ce6bc2a3d.parquet",
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

The Java coordinator runs Flyway metadata migrations on startup by default when `COORDINATOR_METADATA_DATABASE_URL` is configured. Set `COORDINATOR_METADATA_MIGRATIONS_ENABLED=false` only when your deployment pipeline applies the same migrations before the coordinator starts. Workers should keep `METADATA_DB_AUTO_MIGRATE=false`; their old auto-migrate path is only for isolated local experiments.

For real S3 over a slower link, try `PARQUET_COMPRESSION=lz4_raw` or `snappy`, and benchmark `TARGET_FILE_SIZE`, `PUT_PARALLELISM`, `S3_MULTIPART_PART_SIZE`, and `S3_MULTIPART_MAX_CONCURRENCY` as a set.

MinIO console: http://127.0.0.1:9001

Credentials:

```text
minioadmin / minioadmin
```
