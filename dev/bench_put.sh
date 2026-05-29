#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

input="${1:-data/test-1gb.arrow}"
key="${2:-bench/test.parquet}"

export FLIGHT_URI="${FLIGHT_URI:-http://127.0.0.1:50051}"
export FLIGHT_MAX_MESSAGE_SIZE="${FLIGHT_MAX_MESSAGE_SIZE:-268435456}"
export FLIGHT_DATA_CHUNK_SIZE="${FLIGHT_DATA_CHUNK_SIZE:-16777216}"

cargo run --release --bin bench-put -- \
  --input "$input" \
  --path "$key"
