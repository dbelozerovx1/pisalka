#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

./dev/generate_arrow.sh 128mb data/smoke.arrow
./dev/bench_put.sh data/smoke.arrow smoke/smoke.parquet
./dev/bench_get.sh smoke/smoke.parquet
