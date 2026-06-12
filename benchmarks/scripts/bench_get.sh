#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

key="${1:-bench/test.parquet}"

export FLIGHT_URI="${FLIGHT_URI:-http://127.0.0.1:50051}"
export FLIGHT_MAX_MESSAGE_SIZE="${FLIGHT_MAX_MESSAGE_SIZE:-268435456}"

cargo run --release --bin bench-get -- \
  --path "$key"
