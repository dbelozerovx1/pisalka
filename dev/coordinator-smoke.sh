#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

size="${SMOKE_SIZE:-16mb}"
target_file_size="${SMOKE_TARGET_FILE_SIZE:-8mb}"
input="${SMOKE_INPUT:-/bench-data/coordinator-smoke-${size}.arrow}"
operation_id="${SMOKE_OPERATION_ID:-smoke-$(date +%Y%m%d-%H%M%S)}"
staging_prefix="${SMOKE_STAGING_PREFIX:-coordinator/smoke/${operation_id}}"
path="${SMOKE_PATH:-${staging_prefix}/dataset.parquet}"
coordinator_uri="${COORDINATOR_URI:-http://127.0.0.1:8088}"

export WORKER_REQUIRE_SIGNED_CAPABILITIES="${WORKER_REQUIRE_SIGNED_CAPABILITIES:-true}"
export WORKER_CAPABILITY_SECRET="${WORKER_CAPABILITY_SECRET:-local-dev-secret}"
export WORKER_REQUIRE_CAPABILITY_WORKER_ID="${WORKER_REQUIRE_CAPABILITY_WORKER_ID:-true}"
export WORKER_REQUIRE_STRUCTURED_TICKETS="${WORKER_REQUIRE_STRUCTURED_TICKETS:-true}"
export PUT_REQUIRE_STAGING_PREFIX="${PUT_REQUIRE_STAGING_PREFIX:-true}"
export COORDINATOR_CAPABILITY_SECRET="${COORDINATOR_CAPABILITY_SECRET:-$WORKER_CAPABILITY_SECRET}"
export TRINO_URI="${TRINO_URI:-http://trino:8080}"

json_field() {
  python3 -c 'import json,sys; value=json.load(sys.stdin); 
for key in sys.argv[1].split("."):
    value=value[key]
print(value)' "$1"
}

size_to_bytes() {
  VALUE="$1" python3 -c 'import os
raw = os.environ["VALUE"].strip().lower()
units = [("gb", 1024**3), ("mb", 1024**2), ("kb", 1024), ("b", 1)]
for suffix, mult in units:
    if raw.endswith(suffix):
        print(int(float(raw[:-len(suffix)]) * mult))
        raise SystemExit
print(int(raw))'
}

first_put_file() {
  python3 -c 'import json,sys
for line in sys.stdin:
    if line.startswith("put_result="):
        result = json.loads(line[len("put_result="):])
        print(result["files"][0]["key"])
        raise SystemExit(0)
raise SystemExit("put_result line was not found")'
}

echo "starting_compose_stack=true"
docker compose --profile bench build bench flight-server coordinator
docker compose --profile bench up -d --build flight-server coordinator trino-init

echo "generating_input=$input size=$size"
docker compose run --rm --entrypoint gen-arrow bench \
  --target-size "$size" \
  --output "$input" \
  --rows-per-batch "${ROWS_PER_BATCH:-65536}" \
  --payload-bytes "${PAYLOAD_BYTES:-64}"

put_request="$(
  operation_id="$operation_id" \
    staging_prefix="$staging_prefix" \
    path="$path" \
    target_file_size_bytes="$(size_to_bytes "$target_file_size")" \
    python3 -c 'import json,os
print(json.dumps({
    "operationId": os.environ["operation_id"],
    "stagingPrefix": os.environ["staging_prefix"],
    "path": os.environ["path"],
    "targetFileSizeBytes": int(os.environ["target_file_size_bytes"]),
}))'
)"

echo "requesting_put_ticket=$coordinator_uri/v1/flight/put-ticket"
put_ticket="$(curl -fsS "$coordinator_uri/v1/flight/put-ticket" \
  -H "content-type: application/json" \
  -d "$put_request")"
descriptor_path="$(printf '%s' "$put_ticket" | json_field descriptorPath)"
flight_uri="$(printf '%s' "$put_ticket" | json_field flightUri)"
app_metadata="$(printf '%s' "$put_ticket" | json_field appMetadata)"

echo "running_doput=true descriptor_path=$descriptor_path"
put_output="$(
  docker compose run --rm \
    -e "FLIGHT_URI=$flight_uri" \
    -e "PUT_APP_METADATA_JSON=$app_metadata" \
    --entrypoint bench-put bench \
    --input "$input" \
    --path "$descriptor_path"
)"
printf '%s\n' "$put_output"

first_file="$(printf '%s\n' "$put_output" | first_put_file)"
echo "first_written_file=$first_file"

get_request="$(
  operation_id="$operation_id" \
    first_file="$first_file" \
    python3 -c 'import json,os
print(json.dumps({
    "operationId": os.environ["operation_id"] + "-read",
    "path": os.environ["first_file"],
}))'
)"

echo "requesting_get_ticket=$coordinator_uri/v1/flight/get-ticket"
get_ticket_response="$(curl -fsS "$coordinator_uri/v1/flight/get-ticket" \
  -H "content-type: application/json" \
  -d "$get_request")"
get_flight_uri="$(printf '%s' "$get_ticket_response" | json_field flightUri)"
get_ticket="$(printf '%s' "$get_ticket_response" | json_field ticket)"

echo "running_doget=true path=$first_file"
docker compose run --rm \
  -e "FLIGHT_URI=$get_flight_uri" \
  -e "GET_TICKET_JSON=$get_ticket" \
  --entrypoint bench-get bench \
  --path "$first_file"
