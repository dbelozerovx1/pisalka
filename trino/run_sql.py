import json
import os
import sys
import urllib.request


TRINO_URI = os.environ.get("TRINO_URI", "http://trino:8080").rstrip("/")
TRINO_USER = os.environ.get("TRINO_USER", "local")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "arrow")


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
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_sql(sql):
    body = request_json(f"{TRINO_URI}/v1/statement", sql)
    columns = body.get("columns")
    data = []
    if body.get("data"):
        data.extend(body["data"])
    while body.get("nextUri"):
        body = request_json(body["nextUri"])
        if body.get("columns"):
            columns = body["columns"]
        if body.get("data"):
            data.extend(body["data"])
    if body.get("error"):
        error = body["error"]
        raise RuntimeError(f"{error.get('errorName')}: {error.get('message')}")
    if columns is not None:
        body["columns"] = columns
    if data:
        body["data"] = data
    return body


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: run_sql.py '<sql>'")
    print(json.dumps(run_sql(sys.argv[1]), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
