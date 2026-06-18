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
  get)
    bench-get --path "$path"
    ;;
  generate)
    echo "generated_input=$input"
    ;;
  *)
    echo "unsupported BENCH_MODE=$mode; use single, multi, get, or generate" >&2
    exit 2
    ;;
esac
