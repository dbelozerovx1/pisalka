use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::Deserialize;
use tonic::Status;

use crate::util::normalize_object_key;

#[derive(Debug, Clone)]
pub(crate) struct ReadTicket {
    pub(crate) key: String,
    pub(crate) operation_id: Option<String>,
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
    require_structured: bool,
) -> Result<ReadTicket, Status> {
    if bytes.is_empty() {
        return Err(Status::invalid_argument("DoGet ticket must not be empty"));
    }

    if bytes.first() == Some(&b'{') {
        let ticket: StructuredReadTicket = serde_json::from_slice(bytes)
            .map_err(|err| Status::invalid_argument(format!("invalid DoGet ticket JSON: {err}")))?;
        validate_expiry(ticket.expires_at_ms)?;
        return Ok(ReadTicket {
            key: normalize_object_key(&ticket.path),
            operation_id: validate_optional_id("operation_id", ticket.operation_id)?,
        });
    }

    if require_structured {
        return Err(Status::permission_denied(
            "worker requires structured DoGet tickets",
        ));
    }

    let raw = std::str::from_utf8(bytes)
        .map_err(|err| Status::invalid_argument(format!("DoGet ticket is not utf8: {err}")))?;
    Ok(ReadTicket {
        key: normalize_object_key(raw),
        operation_id: None,
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

fn validate_optional_id(name: &str, value: Option<String>) -> Result<Option<String>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{name} must not be empty"
        )));
    }
    if value.len() > 128 {
        return Err(Status::invalid_argument(format!(
            "{name} must be at most 128 bytes"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Status::invalid_argument(format!(
            "{name} may only contain ASCII letters, digits, '-', '_', '.', and ':'"
        )));
    }

    Ok(Some(value.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_legacy_raw_path_when_allowed() {
        let ticket = parse_read_ticket(&Bytes::from_static(b"bench/file.parquet"), false).unwrap();

        assert_eq!(ticket.key, "bench/file.parquet");
        assert_eq!(ticket.operation_id, None);
    }

    #[test]
    fn rejects_legacy_raw_path_when_structured_required() {
        let error = parse_read_ticket(&Bytes::from_static(b"bench/file.parquet"), true)
            .expect_err("raw ticket should be rejected");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn parses_structured_ticket() {
        let ticket = parse_read_ticket(
            &Bytes::from_static(
                br#"{"path":"bench/file.parquet","operation_id":"read-1","expires_at_ms":4102444800000}"#,
            ),
            true,
        )
        .unwrap();

        assert_eq!(ticket.key, "bench/file.parquet");
        assert_eq!(ticket.operation_id.as_deref(), Some("read-1"));
    }

    #[test]
    fn rejects_expired_structured_ticket() {
        let error = parse_read_ticket(
            &Bytes::from_static(br#"{"path":"bench/file.parquet","expires_at_ms":1}"#),
            true,
        )
        .expect_err("expired ticket should be rejected");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
}
