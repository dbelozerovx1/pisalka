#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${BENCH_SKIP_UP:-false}" != "true" ]]; then
  docker compose up -d --build flight-server
fi

docker compose run --rm --no-deps bench "$@"
