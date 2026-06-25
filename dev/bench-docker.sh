#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${BENCH_SKIP_UP:-false}" != "true" ]]; then
  docker compose --profile bench build bench

  case "${BENCH_MODE:-single}" in
    coordinator|coord|coordinator-query|coord-query|ctas)
      export WORKER_REQUIRE_SIGNED_CAPABILITIES="${WORKER_REQUIRE_SIGNED_CAPABILITIES:-true}"
      export WORKER_CAPABILITY_SECRET="${WORKER_CAPABILITY_SECRET:-local-dev-secret}"
      export WORKER_REQUIRE_CAPABILITY_WORKER_ID="${WORKER_REQUIRE_CAPABILITY_WORKER_ID:-true}"
      export WORKER_REQUIRE_STRUCTURED_TICKETS="${WORKER_REQUIRE_STRUCTURED_TICKETS:-true}"
      export PUT_REQUIRE_STAGING_PREFIX="${PUT_REQUIRE_STAGING_PREFIX:-true}"
      export COORDINATOR_CAPABILITY_SECRET="${COORDINATOR_CAPABILITY_SECRET:-$WORKER_CAPABILITY_SECRET}"
      docker compose --profile bench up -d --build flight-server coordinator trino-init
      ;;
    *)
      docker compose up -d --build flight-server
      ;;
  esac
fi

docker compose run --rm --no-deps bench "$@"
