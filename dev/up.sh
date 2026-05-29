#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--minio-only" ]]; then
  docker compose up -d minio minio-create-bucket
else
  docker compose up -d --build
fi
