#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

size="${SMOKE_SIZE:-16mb}"
target_file_size="${SMOKE_TARGET_FILE_SIZE:-8mb}"
streams="${SMOKE_STREAMS:-1}"
read_back="${SMOKE_READ_BACK:-first}"
input="${SMOKE_INPUT:-/bench-data/coordinator-smoke-${size}.arrow}"
operation_id="${SMOKE_OPERATION_ID:-smoke-$(date +%Y%m%d-%H%M%S)}"
commit_mode="${SMOKE_COMMIT_MODE:-append}"
table_suffix="$(printf '%s' "$operation_id" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_' '_')"
table_name="${SMOKE_TABLE_NAME:-iceberg.arrow.smoke_${table_suffix}}"

export WORKER_REQUIRE_SIGNED_CAPABILITIES="${WORKER_REQUIRE_SIGNED_CAPABILITIES:-true}"
export WORKER_CAPABILITY_SECRET="${WORKER_CAPABILITY_SECRET:-local-dev-secret}"
export WORKER_REQUIRE_CAPABILITY_WORKER_ID="${WORKER_REQUIRE_CAPABILITY_WORKER_ID:-true}"
export WORKER_REQUIRE_STRUCTURED_TICKETS="${WORKER_REQUIRE_STRUCTURED_TICKETS:-true}"
export PUT_REQUIRE_STAGING_PREFIX="${PUT_REQUIRE_STAGING_PREFIX:-true}"
export COORDINATOR_CAPABILITY_SECRET="${COORDINATOR_CAPABILITY_SECRET:-$WORKER_CAPABILITY_SECRET}"
export COORDINATOR_ADMIN_TOKEN="${COORDINATOR_ADMIN_TOKEN:-}"
export TRINO_URI="${TRINO_URI:-http://trino:8080}"

echo "starting_compose_stack=true"
docker compose --profile bench build bench flight-server flight-server-2 coordinator
docker compose --profile bench up -d --build --force-recreate \
  minio \
  minio-create-bucket \
  metadata-db \
  metadata-migrate \
  hive-metastore \
  trino \
  trino-init \
  flight-server \
  flight-server-2 \
  coordinator
docker compose run --rm metadata-migrate

echo "generating_input=$input size=$size"
docker compose run --rm --entrypoint gen-arrow bench \
  --target-size "$size" \
  --output "$input" \
  --rows-per-batch "${ROWS_PER_BATCH:-65536}" \
  --payload-bytes "${PAYLOAD_BYTES:-64}"

args=(
  --input "$input"
  --coordinator-uri "http://coordinator:8088"
  --operation-id "$operation_id"
  --streams "$streams"
  --file-size "$target_file_size"
  --read-back "$read_back"
)

if [[ -n "${SMOKE_TABLE_NAME:-}" ]]; then
  args+=(--table-name "$SMOKE_TABLE_NAME")
elif [[ "$commit_mode" != "none" ]]; then
  args+=(--table-name "$table_name")
fi

if [[ "$commit_mode" != "none" ]]; then
  args+=(--commit-mode "$commit_mode")
fi

if [[ -n "${SMOKE_READ_MAX_FILES:-}" ]]; then
  args+=(--read-max-files "$SMOKE_READ_MAX_FILES")
fi

echo "running_coordinator_client=true operation_id=$operation_id streams=$streams read_back=$read_back"
docker compose run --rm \
  -e "COORDINATOR_ADMIN_TOKEN=$COORDINATOR_ADMIN_TOKEN" \
  --entrypoint env bench \
  -u COORDINATOR_OPERATION_ID \
  -u COORDINATOR_UPLOAD_ID \
  -u COORDINATOR_TABLE_NAME \
  -u COORDINATOR_COMMIT_MODE \
  -u PUT_MAX_STREAM_BYTES \
  -u PUT_MAX_RECORD_BATCH_BYTES \
  -u COORDINATOR_UPLOAD_TTL_MS \
  -u READ_MAX_FILES \
  -u GET_MAX_BATCH_ROWS \
  -u GET_MAX_RECORD_BATCH_BYTES \
  bench-coordinator \
  "${args[@]}"

if [[ "$commit_mode" != "none" ]]; then
  echo "verifying_trino_select=true table=$table_name"
  docker compose run --rm --entrypoint python trino-init \
    /run_sql.py "SELECT count(*) AS row_count FROM $table_name"
  docker compose run --rm --entrypoint python trino-init \
    /run_sql.py "SELECT * FROM $table_name LIMIT 1"
fi
