#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

input="${1:-data/test-3gb.arrow}"
streams="${2:-${PUT_STREAMS:-6}}"
staging_prefix="${3:-${PUT_STAGING_PREFIX:-}}"
file_size="${4:-${TARGET_FILE_SIZE:-512mb}}"
profile="${5:-${PUT_PROFILE:-false}}"

export FLIGHT_URI="${FLIGHT_URI:-http://127.0.0.1:50051}"
export FLIGHT_MAX_MESSAGE_SIZE="${FLIGHT_MAX_MESSAGE_SIZE:-268435456}"
export FLIGHT_DATA_CHUNK_SIZE="${FLIGHT_DATA_CHUNK_SIZE:-16777216}"
export PUT_PARALLELISM=4
export PARQUET_COMPRESSION=snappy

args=(
  --input "$input"
  --streams "$streams"
)

if [[ -n "$staging_prefix" ]]; then
  args+=(--staging-prefix "$staging_prefix")
fi

if [[ -n "$file_size" ]]; then
  args+=(--file-size "$file_size")
fi

profile_lc="$(printf '%s' "$profile" | tr '[:upper:]' '[:lower:]')"
case "$profile_lc" in
  1|true|yes|on|profile)
    args+=(--profile)
    ;;
esac

cargo run --release --bin bench-put-multi -- "${args[@]}"
