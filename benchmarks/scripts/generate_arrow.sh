#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

size="${3:-3gb}"
output="${2:-data/test-${size}.arrow}"
rows_per_batch="${ROWS_PER_BATCH:-65536}"
payload_bytes="${PAYLOAD_BYTES:-64}"

cargo run --release --bin gen-arrow -- \
  --target-size "$size" \
  --output "$output" \
  --rows-per-batch "$rows_per_batch" \
  --payload-bytes "$payload_bytes"
