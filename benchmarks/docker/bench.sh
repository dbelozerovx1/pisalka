#!/usr/bin/env bash
set -euo pipefail

bool_enabled() {
  case "$(printf '%s' "${1:-false}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on|profile) return 0 ;;
    *) return 1 ;;
  esac
}

mode="${BENCH_MODE:-single}"
size="${BENCH_SIZE:-1gb}"
data_dir="${BENCH_DATA_DIR:-/bench-data}"
input="${BENCH_INPUT:-${data_dir}/test-${size}.arrow}"
rows_per_batch="${ROWS_PER_BATCH:-65536}"
payload_bytes="${PAYLOAD_BYTES:-64}"
regenerate="${BENCH_REGENERATE:-false}"
startup_wait="${BENCH_STARTUP_WAIT:-2}"

mkdir -p "$(dirname "$input")"

if [[ "$startup_wait" != "0" ]]; then
  sleep "$startup_wait"
fi

if bool_enabled "$regenerate" || [[ ! -f "$input" ]]; then
  gen-arrow \
    --target-size "$size" \
    --output "$input" \
    --rows-per-batch "$rows_per_batch" \
    --payload-bytes "$payload_bytes"
else
  echo "using_existing_input=$input"
fi

timestamp="$(date +%Y%m%d-%H%M%S)"
path="${BENCH_PATH:-bench/docker-${mode}-${timestamp}.parquet}"
file_size="${BENCH_FILE_SIZE:-${TARGET_FILE_SIZE:-512mb}}"
profile="${PUT_PROFILE:-false}"

put_args=()
file_size_lc="$(printf '%s' "$file_size" | tr '[:upper:]' '[:lower:]')"
if [[ -n "$file_size" && "$file_size_lc" != "none" && "$file_size_lc" != "single" && "$file_size_lc" != "off" ]]; then
  put_args+=(--file-size "$file_size")
fi
if [[ -n "${BENCH_MAX_UPLOAD_STREAMS:-}" ]]; then
  put_args+=(--max-upload-streams "$BENCH_MAX_UPLOAD_STREAMS")
fi
if [[ -n "${BENCH_MAX_STREAM_BYTES:-}" ]]; then
  put_args+=(--max-stream-bytes "$BENCH_MAX_STREAM_BYTES")
fi
if bool_enabled "$profile"; then
  put_args+=(--profile)
fi

case "$mode" in
  single|put)
    bench-put \
      --input "$input" \
      --path "$path" \
      "${put_args[@]}"
    ;;
  multi|put-multi)
    streams="${PUT_STREAMS:-6}"
    staging_prefix="${BENCH_STAGING_PREFIX:-}"
    client_queue_depth="${PUT_CLIENT_QUEUE_DEPTH:-2}"

    args=(
      --input "$input"
      --streams "$streams"
      --client-queue-depth "$client_queue_depth"
    )

    if [[ -n "$staging_prefix" ]]; then
      args+=(--staging-prefix "$staging_prefix")
    fi
    args+=("${put_args[@]}")

    bench-put-multi "${args[@]}"
    ;;
  coordinator|coord)
    streams="${UPLOAD_STREAMS:-${PUT_STREAMS:-1}}"
    staging_prefix="${BENCH_STAGING_PREFIX:-coordinator/bench/${timestamp}}"
    read_back="${READ_BACK:-first}"
    operation_id="${COORDINATOR_OPERATION_ID:-bench-docker-${timestamp}}"

    args=(
      --input "$input"
      --operation-id "$operation_id"
      --staging-prefix "$staging_prefix"
      --streams "$streams"
      --read-back "$read_back"
    )

    if [[ -n "$file_size" && "$file_size_lc" != "none" && "$file_size_lc" != "single" && "$file_size_lc" != "off" ]]; then
      args+=(--file-size "$file_size")
    fi
    if [[ -n "${BENCH_MAX_STREAM_BYTES:-}" ]]; then
      args+=(--max-stream-bytes "$BENCH_MAX_STREAM_BYTES")
    fi
    if [[ -n "${PUT_MAX_RECORD_BATCH_BYTES:-}" ]]; then
      args+=(--max-record-batch-bytes "$PUT_MAX_RECORD_BATCH_BYTES")
    fi
    if [[ -n "${GET_MAX_RECORD_BATCH_BYTES:-}" ]]; then
      args+=(--get-max-record-batch-bytes "$GET_MAX_RECORD_BATCH_BYTES")
    fi
    if [[ -n "${GET_MAX_BATCH_ROWS:-}" ]]; then
      args+=(--get-max-batch-rows "$GET_MAX_BATCH_ROWS")
    fi
    if [[ -n "${READ_MAX_FILES:-}" ]]; then
      args+=(--read-max-files "$READ_MAX_FILES")
    fi
    if [[ -n "${COORDINATOR_TABLE_NAME:-}" ]]; then
      args+=(--table-name "$COORDINATOR_TABLE_NAME")
    fi

    for optional_env in \
      COORDINATOR_TABLE_NAME \
      GET_MAX_BATCH_ROWS \
      GET_MAX_RECORD_BATCH_BYTES \
      PUT_MAX_RECORD_BATCH_BYTES \
      READ_MAX_FILES
    do
      if [[ -z "${!optional_env:-}" ]]; then
        unset "$optional_env"
      fi
    done

    bench-coordinator "${args[@]}"
    ;;
  coordinator-query|coord-query|ctas)
    target_table="${COORDINATOR_TARGET_TABLE:-iceberg.arrow.ctas_tmp_${timestamp//-/_}}"
    sql="${COORDINATOR_QUERY_SQL:-SELECT * FROM (VALUES (1, 'hello')) AS t(id, name)}"

    args=(
      --coordinator-uri "${COORDINATOR_URI:-http://coordinator:8088}"
      --sql "$sql"
      --target-table "$target_table"
    )

    if [[ -n "${TRINO_USER:-}" ]]; then
      args+=(--user "$TRINO_USER")
    fi
    if [[ -n "${TRINO_AUTHORIZATION:-}" ]]; then
      args+=(--authorization "$TRINO_AUTHORIZATION")
    fi
    if [[ -n "${COORDINATOR_POLL_INTERVAL_MS:-}" ]]; then
      args+=(--poll-interval-ms "$COORDINATOR_POLL_INTERVAL_MS")
    fi
    if [[ -n "${COORDINATOR_MAX_POLLS:-}" ]]; then
      args+=(--max-polls "$COORDINATOR_MAX_POLLS")
    fi

    for optional_env in \
      COORDINATOR_POLL_INTERVAL_MS \
      COORDINATOR_MAX_POLLS \
      TRINO_USER \
      TRINO_AUTHORIZATION
    do
      if [[ -z "${!optional_env:-}" ]]; then
        unset "$optional_env"
      fi
    done

    coordinator-query "${args[@]}"
    ;;
  get)
    bench-get --path "$path"
    ;;
  generate)
    echo "generated_input=$input"
    ;;
  *)
    echo "unsupported BENCH_MODE=$mode; use single, multi, coordinator, coordinator-query, get, or generate" >&2
    exit 2
    ;;
esac
