#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

bool_enabled() {
  case "$(printf '%s' "${1:-false}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

usage() {
  cat >&2 <<'USAGE'
usage: dev/e2e-write-table.sh <schema.table|catalog.schema.table>

Creates an example Arrow dataset, writes it through coordinator-issued DoPut
tickets, commits it into Iceberg, and verifies that Trino can read it.

Useful env:
  E2E_SIZE=64mb
  E2E_STREAMS=1
  E2E_TARGET_FILE_SIZE=64mb
  E2E_COMMIT_MODE=overwrite|append
  E2E_INTEGER_KIND=signed|unsigned
  E2E_INPUT=/bench-data/e2e-64mb.arrow
  E2E_REGENERATE=false
  E2E_START_STACK=true
  E2E_VERIFY_LIMIT=1
USAGE
}

qualify_table() {
  local raw="$1"
  local catalog="${E2E_CATALOG:-iceberg}"
  local schema="${E2E_SCHEMA:-arrow}"
  local dot_count="${raw//[^.]}"
  if [[ -z "$raw" || "$raw" == .* || "$raw" == *. || "$raw" == *..* ]]; then
    echo "invalid table name: $raw" >&2
    return 2
  fi
  case "${#dot_count}" in
    0) printf '%s.%s.%s\n' "$catalog" "$schema" "$raw" ;;
    1) printf '%s.%s\n' "$catalog" "$raw" ;;
    2) printf '%s\n' "$raw" ;;
    *)
      echo "table name must be table, schema.table, or catalog.schema.table: $raw" >&2
      return 2
      ;;
  esac
}

table_part() {
  local table="$1"
  local index="$2"
  IFS='.' read -r catalog schema name <<<"$table"
  case "$index" in
    catalog) printf '%s\n' "$catalog" ;;
    schema) printf '%s\n' "$schema" ;;
    name) printf '%s\n' "$name" ;;
  esac
}

start_stack() {
  export WORKER_REQUIRE_SIGNED_CAPABILITIES="${WORKER_REQUIRE_SIGNED_CAPABILITIES:-true}"
  export WORKER_CAPABILITY_SECRET="${WORKER_CAPABILITY_SECRET:-local-dev-secret}"
  export WORKER_REQUIRE_CAPABILITY_WORKER_ID="${WORKER_REQUIRE_CAPABILITY_WORKER_ID:-true}"
  export WORKER_REQUIRE_STRUCTURED_TICKETS="${WORKER_REQUIRE_STRUCTURED_TICKETS:-true}"
  export PUT_REQUIRE_STAGING_PREFIX="${PUT_REQUIRE_STAGING_PREFIX:-true}"
  export COORDINATOR_CAPABILITY_SECRET="${COORDINATOR_CAPABILITY_SECRET:-$WORKER_CAPABILITY_SECRET}"
  export COORDINATOR_ADMIN_TOKEN="${COORDINATOR_ADMIN_TOKEN:-}"
  export TRINO_URI="${TRINO_URI:-http://trino:8080}"

  if ! bool_enabled "${E2E_START_STACK:-true}"; then
    return 0
  fi

  local up_args=(--profile bench up -d --build)
  if bool_enabled "${E2E_FORCE_RECREATE:-false}"; then
    up_args+=(--force-recreate)
  fi

  docker compose --profile bench build bench flight-server flight-server-2 coordinator
  docker compose "${up_args[@]}" \
    minio \
    minio-create-bucket \
    metadata-db \
    hive-metastore \
    trino \
    trino-init \
    flight-server \
    flight-server-2 \
    coordinator
}

ensure_input() {
  local input="$1"
  local size="$2"
  local rows_per_batch="${ROWS_PER_BATCH:-65536}"
  local payload_bytes="${PAYLOAD_BYTES:-64}"

  if bool_enabled "${E2E_REGENERATE:-false}" \
    || ! docker compose run --rm --entrypoint sh bench -c "test -f '$input'" >/dev/null 2>&1; then
    echo "generating_input=$input size=$size"
    docker compose run --rm --entrypoint gen-arrow bench \
      --target-size "$size" \
      --output "$input" \
      --rows-per-batch "$rows_per_batch" \
      --payload-bytes "$payload_bytes" \
      --integer-kind "${E2E_INTEGER_KIND:-signed}"
  else
    echo "using_existing_input=$input"
  fi
}

raw_table="${1:-${E2E_TABLE_NAME:-${TABLE_NAME:-}}}"
if [[ -z "$raw_table" || "${raw_table:-}" == "-h" || "${raw_table:-}" == "--help" ]]; then
  usage
  exit 2
fi

table_name="$(qualify_table "$raw_table")"
catalog="$(table_part "$table_name" catalog)"
schema="$(table_part "$table_name" schema)"
safe_table="$(printf '%s' "$table_name" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_' '_')"
timestamp="$(date +%Y%m%d-%H%M%S)"

size="${E2E_SIZE:-64mb}"
input="${E2E_INPUT:-/bench-data/e2e-${size}.arrow}"
streams="${E2E_STREAMS:-1}"
target_file_size="${E2E_TARGET_FILE_SIZE:-64mb}"
commit_mode="${E2E_COMMIT_MODE:-overwrite}"
operation_id="${E2E_OPERATION_ID:-e2e-write-${safe_table}-${timestamp}}"
verify_limit="${E2E_VERIFY_LIMIT:-1}"

start_stack
ensure_input "$input" "$size"

echo "target_table=$table_name"
echo "ensuring_schema=$catalog.$schema"
docker compose run --rm --entrypoint python trino-init \
  /run_sql.py "CREATE SCHEMA IF NOT EXISTS $catalog.$schema"

args=(
  --input "$input"
  --coordinator-uri "${E2E_COORDINATOR_URI:-http://coordinator:8088}"
  --operation-id "$operation_id"
  --streams "$streams"
  --table-name "$table_name"
  --commit-mode "$commit_mode"
  --read-back none
)

target_file_size_lc="$(printf '%s' "$target_file_size" | tr '[:upper:]' '[:lower:]')"
if [[ -n "$target_file_size" && "$target_file_size_lc" != "none" && "$target_file_size_lc" != "single" && "$target_file_size_lc" != "off" ]]; then
  args+=(--file-size "$target_file_size")
fi
if [[ -n "${E2E_MAX_STREAM_BYTES:-}" ]]; then
  args+=(--max-stream-bytes "$E2E_MAX_STREAM_BYTES")
fi
if [[ -n "${E2E_PUT_MAX_RECORD_BATCH_BYTES:-}" ]]; then
  args+=(--max-record-batch-bytes "$E2E_PUT_MAX_RECORD_BATCH_BYTES")
fi
if [[ -n "${E2E_UPLOAD_TTL_MS:-}" ]]; then
  args+=(--upload-ttl-ms "$E2E_UPLOAD_TTL_MS")
fi
if [[ -n "${TRINO_USER:-}" ]]; then
  args+=(--trino-user "$TRINO_USER")
fi
if [[ -n "${TRINO_AUTHORIZATION:-}" ]]; then
  args+=(--trino-authorization "$TRINO_AUTHORIZATION")
fi

echo "writing_table=true table=$table_name operation_id=$operation_id streams=$streams"
docker compose run --rm \
  -e "COORDINATOR_ADMIN_TOKEN=${COORDINATOR_ADMIN_TOKEN:-}" \
  --entrypoint env bench \
  -u COORDINATOR_OPERATION_ID \
  -u COORDINATOR_UPLOAD_ID \
  -u COORDINATOR_TABLE_NAME \
  -u COORDINATOR_COMMIT_MODE \
  -u PUT_MAX_STREAM_BYTES \
  -u PUT_MAX_RECORD_BATCH_BYTES \
  -u COORDINATOR_UPLOAD_TTL_MS \
  -u READ_BACK \
  -u READ_MAX_FILES \
  -u GET_MAX_BATCH_ROWS \
  -u GET_MAX_RECORD_BATCH_BYTES \
  -u TRINO_USER \
  -u TRINO_AUTHORIZATION \
  bench-coordinator \
  "${args[@]}"

echo "verifying_trino_table=true table=$table_name"
docker compose run --rm --entrypoint python trino-init \
  /run_sql.py "SELECT count(*) AS row_count FROM $table_name"
docker compose run --rm --entrypoint python trino-init \
  /run_sql.py "SELECT * FROM $table_name LIMIT $verify_limit"

echo "table_ready=$table_name"
echo "read_example=dev/e2e-read-table.sh $table_name"
