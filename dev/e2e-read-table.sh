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
usage: dev/e2e-read-table.sh <table|schema.table>

Runs the user-facing read path:
  coordinator GetFlightInfo CTAS -> PollFlightInfo -> worker DoGet tickets
and prints a small Arrow table preview to the console.

Useful env:
  E2E_READ_LIMIT=20
  E2E_READ_SQL='SELECT ...'
  E2E_TARGET_TABLE=arrow.my_read_tmp  # optional debug override
  E2E_PREVIEW_ROWS=20
  E2E_READ_MAX_ENDPOINTS=
  E2E_DROP_TEMP_AFTER=true
  E2E_START_STACK=true
USAGE
}

qualify_table() {
  local raw="$1"
  local schema="${E2E_SCHEMA:-arrow}"
  local dot_count="${raw//[^.]}"
  if [[ -z "$raw" || "$raw" == .* || "$raw" == *. || "$raw" == *..* ]]; then
    echo "invalid table name: $raw" >&2
    return 2
  fi
  case "${#dot_count}" in
    0) printf '%s.%s\n' "$schema" "$raw" ;;
    1) printf '%s\n' "$raw" ;;
    *)
      echo "table name must be table or schema.table: $raw" >&2
      return 2
      ;;
  esac
}

table_part() {
  local table="$1"
  local index="$2"
  IFS='.' read -r schema name <<<"$table"
  case "$index" in
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

  local up_args=(--profile e2e up -d --build)
  if bool_enabled "${E2E_FORCE_RECREATE:-false}"; then
    up_args+=(--force-recreate)
  fi

  docker compose --profile e2e build e2e-client flight-server flight-server-2 coordinator
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
source_catalog="${E2E_CATALOG:-iceberg}"
source_schema="$(table_part "$source_table" schema)"
source_name="$(table_part "$source_table" name)"
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
    sql="SELECT * FROM $source_name"
  else
    sql="SELECT * FROM $source_name LIMIT $read_limit"
  fi
fi

start_stack

echo "source_table=$source_table"
if [[ -n "$target_table" ]]; then
  target_catalog="$source_catalog"
  target_schema="$(table_part "$target_table" schema)"
  echo "ctas_table=$target_table"
  echo "ensuring_schema=$target_catalog.$target_schema"
  schema_action_args=(
    --coordinator-uri "${E2E_COORDINATOR_URI:-http://coordinator:8088}"
    --schema-name "$target_schema"
    --user "${TRINO_USER:-local}"
  )
  if [[ -n "${TRINO_AUTHORIZATION:-}" ]]; then
    schema_action_args+=(--authorization "$TRINO_AUTHORIZATION")
  fi
  if [[ -n "${COORDINATOR_ADMIN_TOKEN:-}" ]]; then
    schema_action_args+=(--admin-token "$COORDINATOR_ADMIN_TOKEN")
  fi
  docker compose run --rm \
    --entrypoint e2e-create-schema \
    e2e-client \
    "${schema_action_args[@]}"
else
  echo "ctas_table=<coordinator-generated-from-query-id>"
fi
echo "ctas_sql=$sql"

args=(
  --coordinator-uri "${E2E_COORDINATOR_URI:-http://coordinator:8088}"
  --sql "$sql"
  --schema "$source_schema"
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
  --entrypoint e2e-read \
  e2e-client \
  "${args[@]}"
