#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

./benchmarks/scripts/generate_arrow.sh 128mb data/smoke.arrow
./benchmarks/scripts/bench_put.sh data/smoke.arrow smoke/smoke.parquet
./benchmarks/scripts/bench_get.sh smoke/smoke.parquet
