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
usage: dev/e2e-read-table.sh <schema.table|catalog.schema.table>

Runs the user-facing read path:
  coordinator GetFlightInfo CTAS -> PollFlightInfo -> worker DoGet tickets
and prints a small Arrow table preview to the console.

Useful env:
  E2E_READ_LIMIT=20
  E2E_READ_SQL='SELECT ...'
  E2E_TARGET_TABLE=iceberg.arrow.my_read_tmp  # optional debug override
  E2E_PREVIEW_ROWS=20
  E2E_READ_MAX_ENDPOINTS=
  E2E_DROP_TEMP_AFTER=true
  E2E_START_STACK=true
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

raw_table="${1:-${E2E_TABLE_NAME:-${TABLE_NAME:-}}}"
if [[ -z "$raw_table" || "${raw_table:-}" == "-h" || "${raw_table:-}" == "--help" ]]; then
  usage
  exit 2
fi

source_table="$(qualify_table "$raw_table")"
source_catalog="$(table_part "$source_table" catalog)"
source_schema="$(table_part "$source_table" schema)"
target_table=""
if [[ -n "${E2E_TARGET_TABLE:-}" ]]; then
  target_table="$(qualify_table "$E2E_TARGET_TABLE")"
fi
read_limit="${E2E_READ_LIMIT:-20}"

if [[ -n "${E2E_READ_SQL:-}" ]]; then
  sql="$E2E_READ_SQL"
else
  read_limit_lc="$(printf '%s' "$read_limit" | tr '[:upper:]' '[:lower:]')"
  if [[ "$read_limit_lc" == "none" || "$read_limit_lc" == "all" || "$read_limit_lc" == "off" ]]; then
    sql="SELECT * FROM $source_table"
  else
    sql="SELECT * FROM $source_table LIMIT $read_limit"
  fi
fi

start_stack

echo "source_table=$source_table"
if [[ -n "$target_table" ]]; then
  target_catalog="$(table_part "$target_table" catalog)"
  target_schema="$(table_part "$target_table" schema)"
  echo "ctas_table=$target_table"
  echo "ensuring_schema=$target_catalog.$target_schema"
  docker compose run --rm --entrypoint python trino-init \
    /run_sql.py "CREATE SCHEMA IF NOT EXISTS $target_catalog.$target_schema"
else
  echo "ctas_table=<coordinator-generated-from-query-id>"
fi
echo "ctas_sql=$sql"

args=(
  --coordinator-uri "${E2E_COORDINATOR_URI:-http://coordinator:8088}"
  --sql "$sql"
  --read-results
  --preview-rows "${E2E_PREVIEW_ROWS:-20}"
)

if [[ -n "$target_table" ]]; then
  args+=(--target-table "$target_table")
fi
if bool_enabled "${E2E_DROP_TEMP_AFTER:-${E2E_DROP_CTAS_AFTER:-true}}"; then
  args+=(--drop-temp)
fi
if [[ -n "${E2E_READ_MAX_ENDPOINTS:-}" ]]; then
  args+=(--read-max-endpoints "$E2E_READ_MAX_ENDPOINTS")
fi
if [[ -n "${TRINO_USER:-}" ]]; then
  args+=(--user "$TRINO_USER")
fi
if [[ -n "${TRINO_AUTHORIZATION:-}" ]]; then
  args+=(--authorization "$TRINO_AUTHORIZATION")
fi
if [[ -n "${E2E_POLL_INTERVAL_MS:-}" ]]; then
  args+=(--poll-interval-ms "$E2E_POLL_INTERVAL_MS")
fi
if [[ -n "${E2E_MAX_POLLS:-}" ]]; then
  args+=(--max-polls "$E2E_MAX_POLLS")
fi

docker compose run --rm \
  --entrypoint env bench \
  -u COORDINATOR_QUERY_SQL \
  -u COORDINATOR_TARGET_TABLE \
  -u COORDINATOR_POLL_INTERVAL_MS \
  -u COORDINATOR_MAX_POLLS \
  -u COORDINATOR_READ_RESULTS \
  -u COORDINATOR_READ_MAX_ENDPOINTS \
  -u COORDINATOR_PREVIEW_ROWS \
  -u COORDINATOR_DROP_TEMP \
  -u TRINO_USER \
  -u TRINO_AUTHORIZATION \
  coordinator-query \
  "${args[@]}"
