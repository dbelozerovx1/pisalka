use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::Deserialize;
use tonic::Status;

use crate::{
    capability::{parse_capability_envelope, validate_optional_id, verify_get_capability},
    config::{SecurityConfig, WorkerConfig},
    util::normalize_object_key,
};

#[derive(Debug, Clone)]
pub(crate) struct ReadTicket {
    pub(crate) key: String,
    pub(crate) operation_id: Option<String>,
    pub(crate) max_batch_rows: Option<usize>,
    pub(crate) max_record_batch_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct StructuredReadTicket {
    #[serde(alias = "key", alias = "file_path")]
    path: String,
    operation_id: Option<String>,
    expires_at_ms: Option<u64>,
}

pub(crate) fn parse_read_ticket(
    bytes: &Bytes,
    worker: &WorkerConfig,
    security: &SecurityConfig,
) -> Result<ReadTicket, Status> {
    if bytes.is_empty() {
        return Err(Status::invalid_argument("DoGet ticket must not be empty"));
    }

    if let Some(value) = parse_capability_envelope(bytes) {
        let capability = verify_get_capability(value, worker, security)?;
        return Ok(ReadTicket {
            key: capability.path,
            operation_id: capability.operation_id,
            max_batch_rows: capability.max_batch_rows,
            max_record_batch_bytes: capability.max_record_batch_bytes,
        });
    }

    if security.require_signed_capabilities {
        return Err(Status::permission_denied(
            "worker requires signed DoGet capability tickets",
        ));
    }

    if bytes.first() == Some(&b'{') {
        let ticket: StructuredReadTicket = serde_json::from_slice(bytes)
            .map_err(|err| Status::invalid_argument(format!("invalid DoGet ticket JSON: {err}")))?;
        validate_expiry(ticket.expires_at_ms)?;
        return Ok(ReadTicket {
            key: normalize_object_key(&ticket.path),
            operation_id: validate_optional_id("operation_id", ticket.operation_id)?,
            max_batch_rows: None,
            max_record_batch_bytes: None,
        });
    }

    if worker.require_structured_tickets {
        return Err(Status::permission_denied(
            "worker requires structured DoGet tickets",
        ));
    }

    let raw = std::str::from_utf8(bytes)
        .map_err(|err| Status::invalid_argument(format!("DoGet ticket is not utf8: {err}")))?;
    Ok(ReadTicket {
        key: normalize_object_key(raw),
        operation_id: None,
        max_batch_rows: None,
        max_record_batch_bytes: None,
    })
}

fn validate_expiry(expires_at_ms: Option<u64>) -> Result<(), Status> {
    let Some(expires_at_ms) = expires_at_ms else {
        return Ok(());
    };

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Status::internal(err.to_string()))?
        .as_millis() as u64;

    if now_ms > expires_at_ms {
        return Err(Status::permission_denied("DoGet ticket has expired"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{capability::signed_envelope_for_test, config::SecurityConfig};

    fn worker(require_structured_tickets: bool) -> WorkerConfig {
        WorkerConfig {
            worker_id: "worker-1".to_owned(),
            flight_uri: "grpc+tcp://worker-1:50051".to_owned(),
            zone: None,
            draining: false,
            max_active_put_streams: 16,
            max_put_streams_per_upload: 8,
            put_scheduler_reserved_slots: 0,
            put_slot_wait_ms: 30_000,
            put_first_batch_timeout_ms: 10_000,
            max_put_stream_bytes: None,
            require_staging_prefix: false,
            max_active_read_streams: 16,
            max_read_streams_per_operation: 8,
            read_scheduler_reserved_slots: 0,
            read_slot_wait_ms: 30_000,
            require_structured_tickets,
            registry_heartbeat_interval_ms: 5_000,
            registry_ttl_ms: 15_000,
        }
    }

    fn security(require_signed_capabilities: bool) -> SecurityConfig {
        SecurityConfig {
            require_signed_capabilities,
            capability_secret: Some("secret".to_owned()),
            require_capability_worker_binding: require_signed_capabilities,
            max_capability_ttl_ms: 3_600_000,
        }
    }

    #[test]
    fn parses_legacy_raw_path_when_allowed() {
        let ticket = parse_read_ticket(
            &Bytes::from_static(b"bench/file.parquet"),
            &worker(false),
            &security(false),
        )
        .unwrap();

        assert_eq!(ticket.key, "bench/file.parquet");
        assert_eq!(ticket.operation_id, None);
    }

    #[test]
    fn rejects_legacy_raw_path_when_structured_required() {
        let error = parse_read_ticket(
            &Bytes::from_static(b"bench/file.parquet"),
            &worker(true),
            &security(false),
        )
        .expect_err("raw ticket should be rejected");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn parses_structured_ticket() {
        let ticket = parse_read_ticket(
            &Bytes::from_static(
                br#"{"path":"bench/file.parquet","operation_id":"read-1","expires_at_ms":4102444800000}"#,
            ),
            &worker(true),
            &security(false),
        )
        .unwrap();

        assert_eq!(ticket.key, "bench/file.parquet");
        assert_eq!(ticket.operation_id.as_deref(), Some("read-1"));
    }

    #[test]
    fn rejects_expired_structured_ticket() {
        let error = parse_read_ticket(
            &Bytes::from_static(br#"{"path":"bench/file.parquet","expires_at_ms":1}"#),
            &worker(true),
            &security(false),
        )
        .expect_err("expired ticket should be rejected");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn parses_signed_read_capability() {
        let ticket = signed_envelope_for_test(
            serde_json::json!({
                "op": "get",
                "worker_id": "worker-1",
                "expires_at_ms": 4102444800000u64,
                "path": "bench/file.parquet",
                "operation_id": "read-1",
                "max_batch_rows": 1024,
                "max_record_batch_bytes": 4096
            }),
            "secret",
        );
        let ticket = parse_read_ticket(&ticket, &worker(true), &security(true)).unwrap();

        assert_eq!(ticket.key, "bench/file.parquet");
        assert_eq!(ticket.operation_id.as_deref(), Some("read-1"));
        assert_eq!(ticket.max_batch_rows, Some(1024));
        assert_eq!(ticket.max_record_batch_bytes, Some(4096));
    }

    #[test]
    fn rejects_unsigned_ticket_when_signed_capabilities_required() {
        let error = parse_read_ticket(
            &Bytes::from_static(
                br#"{"path":"bench/file.parquet","operation_id":"read-1","expires_at_ms":4102444800000}"#,
            ),
            &worker(true),
            &security(true),
        )
        .expect_err("unsigned structured ticket should be rejected");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
}
