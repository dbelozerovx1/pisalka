import json
import os
import sys
import time
import urllib.error
import urllib.request


TRINO_URI = os.environ.get("TRINO_URI", "http://trino:8080").rstrip("/")
TRINO_USER = os.environ.get("TRINO_USER", "local")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "default")
SCHEMA_LOCATION = os.environ.get(
    "ICEBERG_SCHEMA_LOCATION", "s3://arrow-flight/iceberg/default"
)
REQUIRE_SCHEMA = os.environ.get("TRINO_INIT_REQUIRE_SCHEMA", "false").lower() in (
    "1",
    "true",
    "yes",
)


def request_json(url, data=None):
    headers = {
        "X-Trino-User": TRINO_USER,
        "X-Trino-Catalog": TRINO_CATALOG,
        "X-Trino-Schema": TRINO_SCHEMA,
    }
    body = None if data is None else data.encode("utf-8")
    if body is not None:
        headers["content-type"] = "text/plain; charset=utf-8"
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_sql(sql):
    body = request_json(f"{TRINO_URI}/v1/statement", sql)
    while body.get("nextUri"):
        body = request_json(body["nextUri"])
    if body.get("error"):
        error = body["error"]
        raise RuntimeError(f"{error.get('errorName')}: {error.get('message')}")
    return body


def wait_for_trino():
    deadline = time.monotonic() + 180
    last_error = None
    while time.monotonic() < deadline:
        try:
            run_sql("SELECT 1")
            return
        except (urllib.error.URLError, TimeoutError, RuntimeError) as error:
            last_error = error
            time.sleep(2)
    raise RuntimeError(f"Trino did not become ready: {last_error}")


def main():
    wait_for_trino()
    try:
        run_sql(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{TRINO_CATALOG}.{TRINO_SCHEMA} "
            f"WITH (location = '{SCHEMA_LOCATION}')"
        )
    except Exception as error:
        if REQUIRE_SCHEMA:
            raise
        print(
            "trino_schema_warning="
            f"{TRINO_CATALOG}.{TRINO_SCHEMA} location={SCHEMA_LOCATION} error={error}",
            file=sys.stderr,
            flush=True,
        )
        print("trino_ready=true", flush=True)
        return
    print(
        "trino_schema_ready="
        f"{TRINO_CATALOG}.{TRINO_SCHEMA} location={SCHEMA_LOCATION}",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"trino_init_error={error}", file=sys.stderr, flush=True)
        raise
