#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

INPUT="${1:-data/perf-1gb.arrow}"
SIZE="${2:-1gb}"
PREFIX="${3:-matrix}"
PARALLELISM_MATRIX="${PUT_PARALLELISM_MATRIX:-1 2 4 8}"

if [[ ! -f "$INPUT" ]]; then
  ./dev/generate_arrow.sh "$SIZE" "$INPUT"
fi

for parallelism in $PARALLELISM_MATRIX; do
  echo
  echo "== PUT_PARALLELISM=$parallelism =="
  PUT_PARALLELISM="$parallelism" docker compose up -d --force-recreate --no-deps flight-server
  sleep 1
  ./dev/bench_put.sh "$INPUT" "$PREFIX/p${parallelism}-$(date +%s).parquet"
done
