# Arrow Flight Lakehouse Worker

This repository contains an MVP split Arrow Flight service for high-throughput lakehouse reads and writes.

The important architectural idea is simple: the Java coordinator owns the control plane, and Rust workers own the hot data plane. The coordinator plans uploads/reads, talks to Trino, checks permissions through the existing Trino/Ranger path, tracks state in Postgres, signs worker capabilities, and commits Parquet files to Iceberg. Workers receive already-authorized direct `DoPut` and `DoGet` calls, write/read Parquet in S3-compatible storage, publish capacity, and stay out of table governance.

The repo is still a monorepo because the coordinator-worker contract, local Compose setup, and test scripts are evolving together. Splitting the coordinator later should be straightforward once the contract stabilizes.

## Architecture

```text
Client
  |
  |  Arrow Flight: GetFlightInfo / PollFlightInfo / Actions
  v
Java coordinator
  |-- Trino: auth, CTAS, schema creation
  |-- Hive/Iceberg API: table resolution, append/overwrite commits
  |-- Postgres: upload/query registry, worker registry, Flyway migrations
  |-- signs worker capabilities
  v
Rust workers
  |-- DoPut: Arrow Flight stream -> Parquet -> S3/object storage
  |-- DoGet: Parquet -> Arrow Flight stream
  |-- metrics + resource-aware capacity heartbeats
```

Intentional boundaries:

- Coordinator does not accept data-plane `DoPut`/`DoGet`; it returns worker endpoints and tickets.
- Workers do not own `GetFlightInfo`, `PollFlightInfo`, Trino, Iceberg commits, or table planning.
- Catalog is application configuration. Clients use `schema.table`, or `table` plus a `schema` parameter. Clients do not pass catalog-qualified table names.
- Schema creation is explicit through the coordinator action `coordinator.create-schema`.
- Reads are currently planned through Trino CTAS into a configured temp schema/location, then worker `DoGet` reads the discovered Parquet files.

## Repository Layout

- `src/`: Rust data-plane worker.
- `coordinator-java/`: Java Arrow Flight coordinator, Iceberg commit logic, Trino client, and Flyway migrations.
- `benchmarks/tools/`: Rust data generator, benchmark clients, coordinator example clients.
- `benchmarks/scripts/`: host-side raw worker benchmark wrappers.
- `benchmarks/docker/`: Docker benchmark entrypoint used by Compose.
- `dev/`: local start/stop, smoke, and end-to-end scripts.
- `docs/`: deeper architecture, Kubernetes, and resource-control notes.
- `docker-compose.yml`: local full stack.
- `.env.example`: local configuration template.

## Local Stack

Requirements:

- Docker/OrbStack or compatible Docker runtime.
- Rust toolchain from `rust-toolchain.toml` for local builds.
- Java/Maven only if you build the coordinator outside Docker.

Start the full local stack:

```bash
./dev/up.sh
```

Compose starts MinIO, Postgres, Hive Metastore, Trino, two Rust workers, and the Java coordinator.

Useful local endpoints:

- Coordinator Arrow Flight: `http://127.0.0.1:8088`
- Coordinator metrics and worker maps: `http://127.0.0.1:9091`
- Trino: `http://127.0.0.1:8080`
- Worker 1 Arrow Flight: `grpc+tcp://127.0.0.1:50051`
- Worker 2 Arrow Flight: `grpc+tcp://127.0.0.1:50052`

Stop the stack:

```bash
./dev/down.sh
```

Run only MinIO plus a local worker binary:

```bash
./dev/up.sh --minio-only
./dev/server-local.sh
```

## End-To-End Checks

The best sanity checks are the user-facing coordinator scripts.

Create or overwrite an Iceberg table through coordinator-issued `DoPut` tickets:

```bash
./dev/e2e-write-table.sh arrow.example_events
```

The script generates Arrow IPC data inside Docker, calls `coordinator.create-schema` when needed, creates an upload session, streams to workers, commits to Iceberg, and verifies the table through Trino.

Useful variations:

```bash
E2E_SIZE=1gb E2E_STREAMS=4 E2E_TARGET_FILE_SIZE=512mb ./dev/e2e-write-table.sh arrow.example_events
E2E_COMMIT_MODE=append ./dev/e2e-write-table.sh arrow.example_events
```

Read back through coordinator CTAS planning and worker `DoGet`:

```bash
./dev/e2e-read-table.sh arrow.example_events
E2E_READ_LIMIT=100 E2E_PREVIEW_ROWS=20 ./dev/e2e-read-table.sh arrow.example_events
```

For table names, use either:

```text
schema.table
table + schema parameter
```

The scripts default a bare table to `E2E_SCHEMA`, but clients using the raw coordinator action must provide schema explicitly when `tableName` is not already `schema.table`.

Run the older coordinator smoke path:

```bash
./dev/coordinator-smoke.sh
```

Run raw worker benchmarks:

```bash
./benchmarks/scripts/generate_arrow.sh 1gb data/test-1gb.arrow
./benchmarks/scripts/bench_put.sh data/test-1gb.arrow bench/test-1gb.parquet
./benchmarks/scripts/bench_get.sh bench/test-1gb.parquet
```

Run benchmark client and data generation inside Docker:

```bash
BENCH_SIZE=2gb BENCH_MODE=single BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
BENCH_SIZE=2gb BENCH_MODE=multi PUT_STREAMS=6 BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
BENCH_SIZE=2gb BENCH_MODE=coordinator UPLOAD_STREAMS=4 BENCH_FILE_SIZE=512mb ./dev/bench-docker.sh
```

Run a generic coordinator action from the bench container:

```bash
docker compose run --rm \
  --entrypoint coordinator-action \
  bench \
  --coordinator-uri http://coordinator:8088 \
  --action coordinator.create-schema \
  --body '{"schemaName":"analytics","location":"s3a://arrow-flight/iceberg/analytics"}' \
  --user local
```

## Coordinator Contract

The coordinator is an Arrow Flight service.

Standard calls:

- `GetFlightInfo` with `{"type":"ctas","sql":"select ...","schema":"analytics"}` starts a CTAS read request and returns a query id in app metadata.
- `PollFlightInfo` with `{"type":"poll","queryId":"ctas_..."}` advances Trino polling on demand. When ready, the response contains worker `DoGet` endpoints.
- `GetFlightInfo` with `{"type":"read","path":"...parquet"}` plans a low-level exact-file read for internal/dev use.

Coordinator actions:

- `coordinator.create-schema`: creates an Iceberg schema through Trino. `schemaName` is required, `location` is optional.
- `coordinator.create-upload`: creates an upload session and returns signed worker `DoPut` tickets.
- `coordinator.commit-upload`: commits recorded worker files to Iceberg with `append` or `overwrite`.
- `coordinator.abort-upload`: aborts an upload and best-effort cleans written files.
- `coordinator.drop-temp`: drops a coordinator-created CTAS temp table by query id.
- `coordinator.config`: returns non-secret effective coordinator configuration.
- `coordinator.put-ticket` / `coordinator.get-ticket`: low-level internal/dev ticket minting.

Upload consistency model:

- `create-upload` persists the plan before data moves.
- Workers persist stream status, Arrow schema JSON, and exact Parquet files.
- The client decides when streams are finished and calls `commit-upload`.
- `commit-upload` commits exactly the files currently recorded for that upload.
- Repeated `commit-upload` for an already committed upload returns the stored commit result.
- Append never drops existing table data on failure; failed append cleanup touches only this upload's files.
- Overwrite is a recreate-style operation: old table/location cleanup happens at upload planning time, before new data files are written.

Read consistency model:

- CTAS requests get a generated query id.
- Polling is client-driven.
- Final worker read tickets are stored in Postgres.
- The client should call `coordinator.drop-temp` after reading CTAS results.

## Configuration

Do not use the README as the configuration source of truth.

Use these instead:

- `.env.example` for local defaults and knobs.
- `docker-compose.yml` for the local service wiring.
- `src/config.rs` for worker configuration parsing and defaults.
- `coordinator-java/src/main/java/com/arrowflight/coordinator/Config.java` for coordinator configuration parsing and defaults.
- `coordinator-java/src/main/resources/db/migration/` for coordinator-owned Flyway migrations.

Workers no longer own database migrations. The coordinator runs Flyway by default; a deployment pipeline may run the same migrations as an explicit release gate, but workers should not create or mutate schema.

Important production defaults to review before deployment:

- signed worker capabilities and shared coordinator/worker secret;
- worker TLS / `grpc+tls` client URIs;
- object-store prefix, Iceberg warehouse, and schema locations;
- Postgres URL and migration policy;
- Trino URI/catalog/schema and auth forwarding;
- worker memory/slot limits and resource-derived recommendations;
- Kubernetes worker discovery and client-routable endpoint strategy.

## Metrics And Operations

Workers expose Prometheus text metrics and publish live capacity into `worker_registry`. The coordinator selects workers from fresh registry rows and can optionally use Kubernetes service discovery to resolve client-routable worker endpoints.

Coordinator metrics and helper HTTP endpoints are exposed on the coordinator metrics listener:

- `GET /workers`
- `GET /worker-endpoints`

Those endpoints are intended for SRE/debugging and HAProxy sidecar map generation. Flight responses can also use the request header `x-base-hostname` to rewrite worker locations as `<worker-id>.<basehostname>:<port>` for sidecar-based routing.

Errors returned to clients include a compact support handle such as:

```text
errorId=coord-err-... code=INVALID_REQUEST method=GetFlightInfo queryId=...: human readable message
```

Use that `errorId` to correlate with coordinator or worker logs. User-fixable errors should stay readable; internal failures are shortened for clients.

## Production Notes

This repo is suitable for local development and first integration/performance testing. It is not a finished production package yet.

Before production, expect to add or harden:

- Helm/Kubernetes manifests and rollout strategy;
- NetworkPolicies, PodDisruptionBudgets, TLS/mTLS, and secret management;
- CI-grade integration tests around upload idempotency and commit recovery;
- abandoned CTAS temp-table cleanup policy;
- autoscaling based on worker capacity metrics and rejection/error rates;
- stricter coordinator HA testing under concurrent commits and process restarts.

See also:

- [docs/k8s-env.md](docs/k8s-env.md)

## Developer Checks

Fast compile checks:

```bash
cargo check --bins
cd coordinator-java && mvn -q -DskipTests package
```

Script syntax checks:

```bash
bash -n dev/e2e-write-table.sh dev/e2e-read-table.sh benchmarks/docker/bench.sh
```

Recommended functional check after coordinator/worker contract changes:

```bash
./dev/e2e-write-table.sh arrow.example_events
./dev/e2e-read-table.sh arrow.example_events
```
