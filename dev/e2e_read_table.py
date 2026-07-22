"""Python 3.12 production-path read example.

Dependencies: pyarrow, grpcio, protobuf. Configure the constants below directly
in a notebook or set their matching environment variables before running.
"""

import json
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import grpc
import pyarrow as pa
import pyarrow.flight as flight
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


# Coordinator, identity, and source query.
COORDINATOR_URI = os.getenv("COORDINATOR_URI", "grpc+tls://flight-coordinator:8088")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "")
TRINO_USER = os.getenv("TRINO_USER", "")
BASE_HOSTNAME = os.getenv("BASE_HOSTNAME", "")
ADMIN_TOKEN = os.getenv("COORDINATOR_ADMIN_TOKEN", "")
TABLE_NAME = os.getenv("TABLE_NAME", "example.events")
SCHEMA_NAME = os.getenv("SCHEMA_NAME", "")  # Needed only when TABLE_NAME has no schema.
SQL = os.getenv("SQL", f"SELECT * FROM {TABLE_NAME}")

# Polling and result handling.
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.25"))
MAX_POLLS = int(os.getenv("MAX_POLLS", "1200"))
READ_PARALLELISM = int(os.getenv("READ_PARALLELISM", "8"))
MAX_ENDPOINTS = int(os.getenv("MAX_ENDPOINTS", "0"))  # Zero reads every endpoint.
PREVIEW_ROWS = int(os.getenv("PREVIEW_ROWS", "20"))
# Set this to false for large performance runs to keep memory bounded.
COLLECT_TABLE = os.getenv("COLLECT_TABLE", "true").lower() in {"1", "true", "yes"}
DROP_TEMP = os.getenv("DROP_TEMP", "true").lower() in {"1", "true", "yes"}

# TLS is verified by default. Set cert/key as well when the deployment requires mTLS.
TLS_CA_CERT = os.getenv("FLIGHT_TLS_CA_CERT", "")
TLS_CLIENT_CERT = os.getenv("FLIGHT_TLS_CLIENT_CERT", "")
TLS_CLIENT_KEY = os.getenv("FLIGHT_TLS_CLIENT_KEY", "")
TLS_OVERRIDE_HOSTNAME = os.getenv("FLIGHT_TLS_OVERRIDE_HOSTNAME", "")
MAX_MESSAGE_SIZE = int(os.getenv("FLIGHT_MAX_MESSAGE_SIZE", str(256 * 1024 * 1024)))
COORDINATOR_TIMEOUT_SECONDS = float(os.getenv("COORDINATOR_TIMEOUT_SECONDS", "60"))
READ_TIMEOUT_SECONDS = float(os.getenv("READ_TIMEOUT_SECONDS", "3600"))


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


RUN_ID = os.getenv("REQUEST_ID", f"python-read-{uuid.uuid4().hex}")


def coordinator_headers():
    headers = [("x-request-id", RUN_ID)]
    if AUTH_TOKEN:
        headers.append(("authorization", bearer(AUTH_TOKEN)))
    if TRINO_USER:
        headers.append(("x-trino-user", TRINO_USER))
    if BASE_HOSTNAME:
        headers.append(("x-base-hostname", BASE_HOSTNAME))
    return headers


def coordinator_options(timeout=COORDINATOR_TIMEOUT_SECONDS):
    headers = [(key.encode(), value.encode()) for key, value in coordinator_headers()]
    return flight.FlightCallOptions(timeout=timeout, headers=headers)


def action_json(client, action_type, body):
    body = dict(body)
    if ADMIN_TOKEN:
        body["adminToken"] = ADMIN_TOKEN
    action = flight.Action(action_type, json.dumps(body, separators=(",", ":")).encode())
    results = list(client.do_action(action, options=coordinator_options()))
    return json.loads(bytes(results[0].body))


def metadata_json(info):
    raw = bytes(info.app_metadata)
    return json.loads(raw) if raw else {}


def poll_info_message_class():
    # PyArrow does not currently expose PollFlightInfo. Defining only the fields
    # used here lets protobuf preserve the nested FlightInfo wire bytes verbatim.
    proto = descriptor_pb2.FileDescriptorProto()
    proto.name = "python_flight_poll.proto"
    proto.package = "arrow.flight.protocol"
    proto.syntax = "proto3"

    descriptor_message = proto.message_type.add()
    descriptor_message.name = "FlightDescriptor"
    for name, number, field_type, label in [
        ("type", 1, descriptor_pb2.FieldDescriptorProto.TYPE_INT32, 1),
        ("cmd", 2, descriptor_pb2.FieldDescriptorProto.TYPE_BYTES, 1),
        ("path", 3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING, 3),
    ]:
        field = descriptor_message.field.add()
        field.name = name
        field.number = number
        field.type = field_type
        field.label = label

    info_message = proto.message_type.add()
    info_message.name = "FlightInfo"

    poll_message = proto.message_type.add()
    poll_message.name = "PollInfo"
    for name, number, field_type, type_name in [
        ("info", 1, descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, ".arrow.flight.protocol.FlightInfo"),
        (
            "flight_descriptor",
            2,
            descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
            ".arrow.flight.protocol.FlightDescriptor",
        ),
        ("progress", 3, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, ""),
    ]:
        field = poll_message.field.add()
        field.name = name
        field.number = number
        field.type = field_type
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        if type_name:
            field.type_name = type_name

    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(proto.SerializeToString())
    descriptor = pool.FindMessageTypeByName("arrow.flight.protocol.PollInfo")
    if hasattr(message_factory, "GetMessageClass"):
        return message_factory.GetMessageClass(descriptor)
    return message_factory.MessageFactory(pool).GetPrototype(descriptor)


POLL_INFO = poll_info_message_class()


def new_poll_channel(uri):
    uri = normalize_flight_uri(uri)
    parsed = urlsplit(uri)
    target = parsed.netloc
    options = [
        ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
        ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
    ]
    if TLS_OVERRIDE_HOSTNAME:
        options.extend(
            [
                ("grpc.ssl_target_name_override", TLS_OVERRIDE_HOSTNAME),
                ("grpc.default_authority", TLS_OVERRIDE_HOSTNAME),
            ]
        )
    if parsed.scheme == "grpc+tls":
        credentials = grpc.ssl_channel_credentials(
            root_certificates=read_pem(TLS_CA_CERT),
            private_key=read_pem(TLS_CLIENT_KEY),
            certificate_chain=read_pem(TLS_CLIENT_CERT),
        )
        return grpc.secure_channel(target, credentials, options=options)
    return grpc.insecure_channel(target, options=options)


def flight_descriptor_bytes(descriptor):
    return bytes(descriptor.serialize())


def poll_flight_info(channel, descriptor_bytes):
    rpc = channel.unary_unary(
        "/arrow.flight.protocol.FlightService/PollFlightInfo",
        request_serializer=lambda value: value,
        response_deserializer=lambda value: value,
    )
    raw = rpc(
        descriptor_bytes,
        timeout=COORDINATOR_TIMEOUT_SECONDS,
        metadata=coordinator_headers(),
    )
    poll = POLL_INFO()
    poll.ParseFromString(raw)
    if not poll.HasField("info"):
        raise RuntimeError("PollFlightInfo response did not include FlightInfo")
    info = flight.FlightInfo.deserialize(poll.info.SerializeToString())
    next_descriptor = (
        poll.flight_descriptor.SerializeToString()
        if poll.HasField("flight_descriptor")
        else None
    )
    return info, next_descriptor, poll.progress


def endpoint_uri(endpoint):
    value = endpoint.locations[0].uri
    return value.decode() if isinstance(value, bytes) else value


def read_endpoint(index, endpoint):
    uri = endpoint_uri(endpoint)
    endpoint_metadata = json.loads(bytes(endpoint.app_metadata) or b"{}")
    path = endpoint_metadata.get("path", "unknown")
    client = new_flight_client(uri)
    started = time.monotonic()
    rows = 0
    batch_count = 0
    arrow_bytes = 0
    batches = []
    preview_batches = []
    preview_rows = 0
    try:
        options = flight.FlightCallOptions(
            timeout=READ_TIMEOUT_SECONDS,
            headers=[(b"x-request-id", RUN_ID.encode())],
        )
        reader = client.do_get(endpoint.ticket, options=options)
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            batch = chunk.data
            if batch is None:
                continue
            rows += batch.num_rows
            batch_count += 1
            arrow_bytes += batch.nbytes
            if COLLECT_TABLE:
                batches.append(batch)
            if preview_rows < PREVIEW_ROWS:
                take = min(PREVIEW_ROWS - preview_rows, batch.num_rows)
                preview_batches.append(batch.slice(0, take))
                preview_rows += take
        return {
            "index": index,
            "uri": uri,
            "path": path,
            "rows": rows,
            "batches": batch_count,
            "arrowBytes": arrow_bytes,
            "elapsedSeconds": time.monotonic() - started,
            "data": batches,
            "preview": preview_batches,
        }
    finally:
        client.close()


def read_all_endpoints(info):
    endpoints = list(info.endpoints)
    if MAX_ENDPOINTS > 0:
        endpoints = endpoints[:MAX_ENDPOINTS]
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, min(READ_PARALLELISM, len(endpoints)))) as executor:
        results = list(
            executor.map(
                lambda item: read_endpoint(*item),
                enumerate(endpoints),
            )
        )
    results.sort(key=lambda item: item["index"])
    elapsed = time.monotonic() - started
    batches = [batch for result in results for batch in result.pop("data")]
    preview_batches = [batch for result in results for batch in result.pop("preview")]
    total_bytes = sum(result["arrowBytes"] for result in results)
    summary = {
        "queryId": query_id,
        "endpoints": len(results),
        "rows": sum(result["rows"] for result in results),
        "batches": sum(result["batches"] for result in results),
        "arrowBytes": total_bytes,
        "elapsedSeconds": elapsed,
        "throughputMiBPerSecond": total_bytes / 1024**2 / elapsed if elapsed else 0,
        "endpointResults": results,
    }
    table = pa.Table.from_batches(batches) if batches else None
    preview = pa.Table.from_batches(preview_batches).slice(0, PREVIEW_ROWS) if preview_batches else None
    return summary, table, preview


coordinator = new_flight_client(COORDINATOR_URI)
poll_channel = None
query_id = None
result_table = None
preview_table = None
read_summary = None
try:
    request = {"type": "ctas", "sql": SQL}
    if SCHEMA_NAME:
        request["schema"] = SCHEMA_NAME
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(request, separators=(",", ":")).encode()
    )
    initial_info = coordinator.get_flight_info(descriptor, options=coordinator_options())
    initial_metadata = metadata_json(initial_info)
    query_id = initial_metadata["queryId"]
    print(f"query_id={query_id}")

    next_descriptor = flight_descriptor_bytes(
        flight.FlightDescriptor.for_command(
            json.dumps({"type": "poll", "queryId": query_id}, separators=(",", ":")).encode()
        )
    )
    poll_channel = new_poll_channel(COORDINATOR_URI)
    final_info = None
    for poll_index in range(MAX_POLLS):
        info, next_descriptor, progress = poll_flight_info(poll_channel, next_descriptor)
        metadata = metadata_json(info)
        status = metadata.get("status", "UNKNOWN")
        print(f"poll={poll_index} status={status} progress={progress:.3f}")
        if status == "FAILED":
            raise RuntimeError(metadata.get("errorMessage", f"query {query_id} failed"))
        if next_descriptor is None:
            if status != "SUCCEEDED":
                raise RuntimeError(f"query {query_id} completed with status {status}")
            final_info = info
            break
        time.sleep(POLL_INTERVAL_SECONDS)
    if final_info is None:
        raise TimeoutError(f"query {query_id} did not complete after {MAX_POLLS} polls")

    read_summary, result_table, preview_table = read_all_endpoints(final_info)
    printable_summary = {
        key: value
        for key, value in read_summary.items()
        if key != "endpointResults"
    }
    print("read_summary=", json.dumps(printable_summary, indent=2))
    if preview_table is not None:
        print("preview_table=")
        print(preview_table)
finally:
    active_error = sys.exc_info()[0] is not None
    if poll_channel is not None:
        poll_channel.close()
    if query_id is not None and DROP_TEMP:
        try:
            drop_result = action_json(
                coordinator,
                "coordinator.drop-temp",
                {"queryId": query_id},
            )
            print("drop_temp_result=", json.dumps(drop_result, indent=2))
        except Exception as drop_error:
            if not active_error:
                raise
            print(f"drop_temp_failed={drop_error}")
    coordinator.close()
