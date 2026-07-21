# Arrow Flight Lakehouse Service

This repository contains a split Arrow Flight service for high-throughput lakehouse reads and writes.

The Java coordinator owns the control plane: Trino authorization and CTAS, upload/read planning, Postgres state, worker discovery, signed capabilities, and Iceberg commits. Rust workers own the data plane: Arrow Flight `DoPut`/`DoGet`, Parquet encoding/decoding, S3 access, resource admission, and capacity heartbeats.

## Architecture

```text
Client
  |
  | Arrow Flight metadata calls and actions
  v
Java coordinator
  |-- Trino: authorization, schemas, CTAS
  |-- Hive/Iceberg: table resolution and commits
  |-- Postgres: uploads, queries, workers, Flyway
  |-- signed direct worker tickets
  v
Rust workers
  |-- DoPut: Arrow stream -> Parquet -> S3
  |-- DoGet: Parquet -> Arrow stream
  |-- metrics and capacity heartbeats
```

The coordinator never proxies data. Clients receive worker locations and signed, operation-specific tickets. Workers do not perform Trino operations, table planning, or Iceberg commits.

## Layout

- `src/`: Rust data-plane worker.
- `coordinator-java/`: Java coordinator and Flyway migrations.
- `e2e/tools/`: helper binaries used only by the two coordinator E2E workflows.
- `dev/e2e-write-table.sh`: complete write and Iceberg commit check.
- `dev/e2e-read-table.sh`: complete CTAS and `DoGet` check.
- `docker-compose.yml`: local two-worker stack.
- `.env.example`: local configuration template.
- `docs/k8s-env.md`: Kubernetes deployment configuration.

## Local Requirements

- Docker Desktop, OrbStack, or another Docker Compose runtime.
- Rust from `rust-toolchain.toml` only for host-side development.
- Java 21 and Maven only for host-side coordinator development.

The E2E scripts build and start MinIO, Postgres, Hive Metastore, Trino, two workers, and the coordinator automatically. Stop the stack with:

```bash
docker compose down
```

Local endpoints:

- Coordinator Flight: `grpc+tcp://127.0.0.1:8088`
- Coordinator metrics and worker map: `http://127.0.0.1:9091`
- Trino: `http://127.0.0.1:8080`
- Worker Flight: `grpc+tcp://127.0.0.1:50051` and `grpc+tcp://127.0.0.1:50052`

## Write E2E

Create or recreate an Iceberg table through coordinator-issued `DoPut` tickets, commit it, and verify it through Trino:

```bash
./dev/e2e-write-table.sh arrow.example_events
```

Useful parameters:

```bash
E2E_SIZE=1gb \
E2E_UPLOAD_FLAVOR=medium \
E2E_TARGET_FILE_SIZE=512mb \
./dev/e2e-write-table.sh arrow.example_events

E2E_COMMIT_MODE=append ./dev/e2e-write-table.sh arrow.example_events
```

Upload flavors are `small`, `medium`, and `large`. They are adaptive concurrency ceilings, not guaranteed stream counts. The coordinator may grant fewer tickets when worker capacity is busy or reserved.

The write path is:

```text
create-schema -> create-upload -> worker DoPut streams
              -> commit-upload -> Iceberg table -> Trino verification
```

## Read E2E

Read the table through coordinator CTAS planning and worker `DoGet` tickets:

```bash
./dev/e2e-read-table.sh arrow.example_events
```

Useful parameters:

```bash
E2E_READ_LIMIT=100 \
E2E_PREVIEW_ROWS=20 \
./dev/e2e-read-table.sh arrow.example_events
```

The read path is:

```text
GetFlightInfo(CTAS) -> PollFlightInfo until ready
                    -> worker DoGet tickets -> Arrow preview -> drop-temp
```

Both scripts accept `schema.table`. A bare table uses `E2E_SCHEMA`, defaulting to `arrow`. Catalog is coordinator configuration and is never provided by clients.

Set `E2E_START_STACK=false` when the Compose stack is already running. Set `E2E_FORCE_RECREATE=true` when testing image or environment changes.

## Coordinator Contract

Important coordinator actions:

- `coordinator.create-schema`: create an Iceberg schema through Trino.
- `coordinator.create-upload`: plan a flavored upload and return signed worker tickets.
- `coordinator.commit-upload`: append or overwrite the recorded Parquet files in Iceberg.
- `coordinator.abort-upload`: abort an upload and clean only its staged files.
- `coordinator.drop-temp`: remove a coordinator-created CTAS table.
- `coordinator.config`: return non-secret effective configuration.

Reads use `GetFlightInfo` to create a new CTAS query and `PollFlightInfo` with its query id to advance Trino polling and obtain final `DoGet` endpoints.

Upload plans and capacity reservations are persisted transactionally. This keeps multiple coordinator replicas idempotent and prevents concurrent planners from granting the same worker capacity. Workers still enforce local stream, memory, writer, and signed-capability limits.

## Configuration

Configuration sources of truth:

- `.env.example` and `docker-compose.yml` for local development.
- `src/config.rs` for worker defaults and validation.
- `coordinator-java/src/main/java/com/arrowflight/coordinator/Config.java` for coordinator defaults and validation.
- `coordinator-java/src/main/resources/db/migration/` for metadata schema migrations.
- `docs/k8s-env.md` for Kubernetes deployment guidance.

The coordinator owns Flyway migrations. Workers only read and update runtime metadata.

## Observability

Workers expose Prometheus metrics and publish live resource capacity into `worker_registry`. The coordinator exposes metrics plus `GET /workers` and `GET /worker-endpoints` on port `9091`.

Worker registry lifecycle is `ACTIVE` -> `DRAINING` during graceful shutdown -> `STALE` after heartbeat TTL. The coordinator removes stale rows after the configured retention period; stale and draining workers are never selected for new reads or writes.

Every log is structured JSON and includes `env`, `group`, `system`, and `namespace`. Client request lifecycle events use stable `*_request_started`, `*_request_completed`, and `*_request_failed` names with a `phase`, `outcome`, and `elapsedMs`.

Correlation identifiers have distinct scopes:

- `requestId`: one coordinator Flight RPC; accepted from `x-request-id` or generated and returned to the client.
- `operationId`: the complete upload or read operation; signed into worker capabilities and shared by coordinator and worker logs.
- `uploadId`, `attemptId`, and `streamId`: progressively narrower upload scopes.
- `queryId`: the durable read/CTAS query and polling scope.
- `errorId`: one failure, returned to the client and emitted in the matching failure log.

Start an investigation with `errorId` for a failure, then use `operationId` or `queryId` to reconstruct the full cross-service lifecycle.

## Developer Verification

```bash
cargo fmt --all -- --check
cargo test --all-targets
(cd coordinator-java && mvn -q test)
bash -n dev/e2e-write-table.sh dev/e2e-read-table.sh
docker compose config --quiet
```

Then run both supported functional checks:

```bash
./dev/e2e-write-table.sh arrow.example_events
./dev/e2e-read-table.sh arrow.example_events
```
