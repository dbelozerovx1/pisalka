"""Python 3.12 production-path write example.

Dependencies: pyarrow, numpy. Configure the constants below directly in a
notebook or set their matching environment variables before running the file.
"""

import json
import math
import os
import queue
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.flight as flight


# Coordinator, identity, and table.
COORDINATOR_URI = os.getenv("COORDINATOR_URI", "grpc+tls://flight-coordinator:8088")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "")
TRINO_USER = os.getenv("TRINO_USER", "")
BASE_HOSTNAME = os.getenv("BASE_HOSTNAME", "")
ADMIN_TOKEN = os.getenv("COORDINATOR_ADMIN_TOKEN", "")
TABLE_NAME = os.getenv("TABLE_NAME", "example.events")
SCHEMA_NAME = os.getenv("SCHEMA_NAME", "")  # Needed only when TABLE_NAME has no schema.
COMMIT_MODE = os.getenv("COMMIT_MODE", "overwrite")
UPLOAD_FLAVOR = os.getenv("UPLOAD_FLAVOR", "small")

# Generated Arrow data. Generation is lazy, so a 5 GiB run does not allocate 5 GiB.
DATA_SIZE = os.getenv("DATA_SIZE", "1gb")
BATCH_ROWS = int(os.getenv("BATCH_ROWS", "65536"))
PAYLOAD_BYTES = int(os.getenv("PAYLOAD_BYTES", "128"))
CLIENT_QUEUE_DEPTH = int(os.getenv("CLIENT_QUEUE_DEPTH", "2"))

# Optional upload constraints. Empty values let the coordinator use its defaults.
TARGET_FILE_SIZE = os.getenv("TARGET_FILE_SIZE", "512mb")
MAX_STREAM_BYTES = os.getenv("MAX_STREAM_BYTES", "")
MAX_RECORD_BATCH_BYTES = os.getenv("MAX_RECORD_BATCH_BYTES", "")
UPLOAD_TTL_MS = os.getenv("UPLOAD_TTL_MS", "")

# TLS is verified by default. Set cert/key as well when the deployment requires mTLS.
TLS_CA_CERT = os.getenv("FLIGHT_TLS_CA_CERT", "")
TLS_CLIENT_CERT = os.getenv("FLIGHT_TLS_CLIENT_CERT", "")
TLS_CLIENT_KEY = os.getenv("FLIGHT_TLS_CLIENT_KEY", "")
TLS_OVERRIDE_HOSTNAME = os.getenv("FLIGHT_TLS_OVERRIDE_HOSTNAME", "")
MAX_MESSAGE_SIZE = int(os.getenv("FLIGHT_MAX_MESSAGE_SIZE", str(256 * 1024 * 1024)))
COORDINATOR_TIMEOUT_SECONDS = float(os.getenv("COORDINATOR_TIMEOUT_SECONDS", "60"))
UPLOAD_TIMEOUT_SECONDS = float(os.getenv("UPLOAD_TIMEOUT_SECONDS", "3600"))


ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_time", pa.timestamp("us"), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("group_id", pa.int32(), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
)


def parse_size(value):
    value = value.strip().lower().replace(" ", "")
    suffixes = {
        "": 1,
        "b": 1,
        "kb": 1024,
        "kib": 1024,
        "mb": 1024**2,
        "mib": 1024**2,
        "gb": 1024**3,
        "gib": 1024**3,
        "tb": 1024**4,
        "tib": 1024**4,
    }
    for suffix in sorted(suffixes, key=len, reverse=True):
        if suffix and value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * suffixes[suffix])
    return int(float(value))


def optional_size(value):
    return parse_size(value) if value.strip() else None


def read_pem(path):
    return Path(path).read_bytes() if path else None


def normalize_flight_uri(uri):
    uri = uri.strip()
    if uri.startswith("grpc+tcp://"):
        return "grpc://" + uri.removeprefix("grpc+tcp://")
    if uri.startswith("http://"):
        return "grpc://" + uri.removeprefix("http://")
    if uri.startswith("https://"):
        return "grpc+tls://" + uri.removeprefix("https://")
    return uri


def new_flight_client(uri):
    uri = normalize_flight_uri(uri)
    options = [
        ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
        ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
    ]
    tls = uri.startswith("grpc+tls://")
    return flight.FlightClient(
        uri,
        tls_root_certs=read_pem(TLS_CA_CERT) if tls else None,
        cert_chain=read_pem(TLS_CLIENT_CERT) if tls else None,
        private_key=read_pem(TLS_CLIENT_KEY) if tls else None,
        override_hostname=TLS_OVERRIDE_HOSTNAME or None,
        disable_server_verification=False,
        generic_options=options,
    )


def bearer(value):
    value = value.strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    return f"Bearer {value}"


RUN_ID = os.getenv("REQUEST_ID", f"python-write-{uuid.uuid4().hex}")
OPERATION_ID = os.getenv("OPERATION_ID", RUN_ID)


def coordinator_options(timeout=COORDINATOR_TIMEOUT_SECONDS):
    headers = [(b"x-request-id", RUN_ID.encode())]
    if AUTH_TOKEN:
        headers.append((b"authorization", bearer(AUTH_TOKEN).encode()))
    if TRINO_USER:
        headers.append((b"x-trino-user", TRINO_USER.encode()))
    if BASE_HOSTNAME:
        headers.append((b"x-base-hostname", BASE_HOSTNAME.encode()))
    return flight.FlightCallOptions(timeout=timeout, headers=headers)


def action_json(client, action_type, body):
    body = dict(body)
    if ADMIN_TOKEN:
        body["adminToken"] = ADMIN_TOKEN
    action = flight.Action(action_type, json.dumps(body, separators=(",", ":")).encode())
    results = list(client.do_action(action, options=coordinator_options()))
    return json.loads(bytes(results[0].body))


def create_upload(client):
    body = {
        "operationId": OPERATION_ID,
        "tableName": TABLE_NAME,
        "mode": COMMIT_MODE,
        "flavor": UPLOAD_FLAVOR,
    }
    if SCHEMA_NAME:
        body["schema"] = SCHEMA_NAME
    if TARGET_FILE_SIZE:
        body["targetFileSizeBytes"] = optional_size(TARGET_FILE_SIZE)
    if MAX_STREAM_BYTES:
        body["maxStreamBytes"] = optional_size(MAX_STREAM_BYTES)
    if MAX_RECORD_BATCH_BYTES:
        body["maxRecordBatchBytes"] = optional_size(MAX_RECORD_BATCH_BYTES)
    if UPLOAD_TTL_MS:
        body["ttlMs"] = int(UPLOAD_TTL_MS)
    return action_json(client, "coordinator.create-upload", body)


def generated_batches(target_bytes, batch_rows, payload_bytes, minimum_batches):
    row_bytes = 8 + 8 + 8 + 4 + 4 + payload_bytes
    total_rows = max(minimum_batches, math.ceil(target_bytes / row_bytes))
    batch_rows = min(batch_rows, max(1, total_rows // minimum_batches))
    payload = (b"0123456789abcdef" * math.ceil(payload_bytes / 16))[:payload_bytes]
    base_time_us = 1_735_689_600_000_000

    for start in range(0, total_rows, batch_rows):
        rows = min(batch_rows, total_rows - start)
        ids = np.arange(start, start + rows, dtype=np.int64)
        event_times = ids + base_time_us
        values = (ids % 100_000).astype(np.float64) / 100.0
        groups = (ids % 1024).astype(np.int32)
        offsets = np.arange(rows + 1, dtype=np.int32) * payload_bytes
        payload_data = payload * rows

        yield pa.RecordBatch.from_arrays(
            [
                pa.Array.from_buffers(pa.int64(), rows, [None, pa.py_buffer(ids)]),
                pa.Array.from_buffers(pa.timestamp("us"), rows, [None, pa.py_buffer(event_times)]),
                pa.Array.from_buffers(pa.float64(), rows, [None, pa.py_buffer(values)]),
                pa.Array.from_buffers(pa.int32(), rows, [None, pa.py_buffer(groups)]),
                pa.Array.from_buffers(
                    pa.string(),
                    rows,
                    [None, pa.py_buffer(offsets), pa.py_buffer(payload_data)],
                ),
            ],
            schema=ARROW_SCHEMA,
        )


def run_put_stream(ticket, batches, stop_event):
    client = new_flight_client(ticket["flightUri"])
    descriptor = flight.FlightDescriptor.for_path(ticket["descriptorPath"])
    options = flight.FlightCallOptions(
        timeout=UPLOAD_TIMEOUT_SECONDS,
        headers=[
            (b"x-flight-capability", ticket["appMetadata"].encode()),
            (b"x-request-id", RUN_ID.encode()),
        ],
    )
    writer = None
    started = time.monotonic()
    rows = 0
    batch_count = 0
    put_results = []
    try:
        writer, metadata_reader = client.do_put(descriptor, ARROW_SCHEMA, options=options)
        while not stop_event.is_set():
            try:
                batch = batches.get(timeout=0.25)
            except queue.Empty:
                continue
            if batch is None:
                break
            writer.write_batch(batch)
            rows += batch.num_rows
            batch_count += 1

        if stop_event.is_set():
            raise RuntimeError("upload cancelled because another DoPut stream failed")

        writer.done_writing()
        while True:
            result = metadata_reader.read()
            if result is None:
                break
            put_results.append(bytes(result).decode())
        writer.close()
        writer = None
        return {
            "streamId": ticket["streamId"],
            "workerId": ticket["workerId"],
            "flightUri": ticket["flightUri"],
            "rows": rows,
            "batches": batch_count,
            "elapsedSeconds": time.monotonic() - started,
            "putResults": put_results,
        }
    except Exception:
        stop_event.set()
        raise
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        client.close()


def run_upload(tickets):
    queues = [queue.Queue(maxsize=max(1, CLIENT_QUEUE_DEPTH)) for _ in tickets]
    stop_event = threading.Event()
    generated_bytes = 0
    generated_rows = 0
    generated_batch_count = 0
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=len(tickets)) as executor:
        futures = [
            executor.submit(run_put_stream, ticket, batches, stop_event)
            for ticket, batches in zip(tickets, queues)
        ]
        try:
            for index, batch in enumerate(
                generated_batches(
                    parse_size(DATA_SIZE),
                    BATCH_ROWS,
                    PAYLOAD_BYTES,
                    len(tickets),
                )
            ):
                target = queues[index % len(queues)]
                while not stop_event.is_set():
                    try:
                        target.put(batch, timeout=0.25)
                        break
                    except queue.Full:
                        pass
                if stop_event.is_set():
                    for future in futures:
                        if future.done() and future.exception() is not None:
                            future.result()
                    raise RuntimeError("DoPut stream stopped during upload")
                generated_bytes += batch.nbytes
                generated_rows += batch.num_rows
                generated_batch_count += 1

            for batches in queues:
                batches.put(None)
            results = [future.result() for future in futures]
        except Exception:
            stop_event.set()
            for batches in queues:
                try:
                    batches.put_nowait(None)
                except queue.Full:
                    pass
            raise

    elapsed = time.monotonic() - started
    return {
        "generatedBytes": generated_bytes,
        "generatedRows": generated_rows,
        "generatedBatches": generated_batch_count,
        "elapsedSeconds": elapsed,
        "throughputMiBPerSecond": generated_bytes / 1024**2 / elapsed,
        "streams": sorted(results, key=lambda item: item["streamId"]),
    }


def upload_plan_summary(upload):
    return {
        key: upload.get(key)
        for key in [
            "uploadId",
            "operationId",
            "tableName",
            "mode",
            "status",
            "requestedFlavor",
            "grantedStreams",
            "targetFileSizeBytes",
            "stagingPrefix",
        ]
    } | {
        "tickets": [
            {
                "streamId": ticket["streamId"],
                "workerId": ticket["workerId"],
                "flightUri": ticket["flightUri"],
                "descriptorPath": ticket["descriptorPath"],
            }
            for ticket in upload["tickets"]
        ]
    }


def printable_upload_summary(summary):
    value = dict(summary)
    value["streams"] = [
        {
            key: stream[key]
            for key in [
                "streamId",
                "workerId",
                "flightUri",
                "rows",
                "batches",
                "elapsedSeconds",
            ]
        }
        | {"putResultCount": len(stream["putResults"])}
        for stream in summary["streams"]
    ]
    return value


def commit_summary(commit):
    return {
        key: commit.get(key)
        for key in [
            "uploadId",
            "operationId",
            "tableName",
            "tableLocation",
            "status",
            "mode",
            "snapshotId",
            "recordCount",
            "parquetObjectBytes",
            "alreadyCommitted",
        ]
    } | {"files": len(commit.get("files", []))}


coordinator = new_flight_client(COORDINATOR_URI)
upload = None
try:
    upload = create_upload(coordinator)
    print("create_upload=", json.dumps(upload_plan_summary(upload), indent=2))
    upload_summary = run_upload(upload["tickets"])
    print("upload_summary=", json.dumps(printable_upload_summary(upload_summary), indent=2))
    commit_result = action_json(
        coordinator,
        "coordinator.commit-upload",
        {
            "uploadId": upload["uploadId"],
            "tableName": upload.get("tableName", TABLE_NAME),
            "mode": COMMIT_MODE,
        },
    )
    print("commit_result=", json.dumps(commit_summary(commit_result), indent=2))
except Exception as error:
    if upload is not None and upload.get("uploadId"):
        try:
            abort_result = action_json(
                coordinator,
                "coordinator.abort-upload",
                {"uploadId": upload["uploadId"], "reason": f"Python client failed: {error}"},
            )
            print("abort_result=", json.dumps(abort_result, indent=2))
        except Exception as abort_error:
            print(f"abort_upload_failed={abort_error}")
    raise
finally:
    coordinator.close()
