use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use tonic::Status;

use crate::{
    config::{SecurityConfig, WorkerConfig},
    util::normalize_object_key,
};

type HmacSha256 = Hmac<Sha256>;

pub(crate) const CAPABILITY_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub(crate) struct VerifiedPutCapability {
    pub(crate) bucket: Option<String>,
    pub(crate) operation_id: Option<String>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) upload_id: Option<String>,
    pub(crate) stream_id: Option<String>,
    pub(crate) allowed_output_prefix: String,
    pub(crate) staging_prefix: Option<String>,
    pub(crate) max_upload_streams: Option<usize>,
    pub(crate) max_stream_bytes: Option<u64>,
    pub(crate) target_file_size: Option<usize>,
    pub(crate) max_record_batch_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedGetCapability {
    pub(crate) bucket: Option<String>,
    pub(crate) operation_id: Option<String>,
    pub(crate) path: String,
    pub(crate) max_batch_rows: Option<usize>,
    pub(crate) max_record_batch_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CapabilityEnvelope {
    version: u16,
    #[serde(default)]
    alg: Option<String>,
    payload: String,
    signature: String,
}

#[derive(Debug, Deserialize)]
struct CapabilityPayload {
    op: String,
    operation_id: Option<String>,
    attempt_id: Option<String>,
    upload_id: Option<String>,
    stream_id: Option<String>,
    bucket: Option<String>,
    path: Option<String>,
    allowed_output_prefix: Option<String>,
    staging_prefix: Option<String>,
    worker_id: Option<String>,
    worker_group: Option<String>,
    expires_at_ms: Option<u64>,
    start_before_ms: Option<u64>,
    not_before_ms: Option<u64>,
    issued_at_ms: Option<u64>,
    max_upload_streams: Option<usize>,
    max_stream_bytes: Option<u64>,
    target_file_size: Option<usize>,
    max_batch_rows: Option<usize>,
    max_record_batch_bytes: Option<u64>,
}

pub(crate) fn parse_capability_envelope(bytes: &Bytes) -> Option<Value> {
    serde_json::from_slice(bytes)
        .ok()
        .filter(|value: &Value| value.get("payload").is_some() && value.get("signature").is_some())
}

pub(crate) fn verify_put_capability(
    value: Value,
    worker: &WorkerConfig,
    security: &SecurityConfig,
) -> Result<VerifiedPutCapability, Status> {
    let payload = verify_payload(value, worker, security)?;
    if payload.op != "put" {
        return Err(Status::permission_denied(format!(
            "worker capability op {:?} cannot authorize DoPut",
            payload.op
        )));
    }
    let current_time_ms = now_ms()?;
    if payload
        .start_before_ms
        .is_some_and(|start_before_ms| current_time_ms > start_before_ms)
    {
        return Err(Status::permission_denied(
            "DoPut capability start reservation has expired; create a new upload",
        ));
    }

    let allowed_output_prefix = payload
        .allowed_output_prefix
        .as_deref()
        .ok_or_else(|| Status::permission_denied("DoPut capability requires allowed_output_prefix"))
        .and_then(normalize_prefix)?;
    let staging_prefix = payload
        .staging_prefix
        .as_deref()
        .map(normalize_prefix)
        .transpose()?;

    Ok(VerifiedPutCapability {
        bucket: validate_optional_bucket(payload.bucket)?,
        operation_id: validate_optional_id("operation_id", payload.operation_id)?,
        attempt_id: validate_optional_id("attempt_id", payload.attempt_id)?,
        upload_id: validate_optional_id("upload_id", payload.upload_id)?,
        stream_id: validate_optional_id("stream_id", payload.stream_id)?,
        allowed_output_prefix,
        staging_prefix,
        max_upload_streams: payload.max_upload_streams,
        max_stream_bytes: payload.max_stream_bytes,
        target_file_size: payload.target_file_size,
        max_record_batch_bytes: payload.max_record_batch_bytes,
    })
}

pub(crate) fn verify_get_capability(
    value: Value,
    worker: &WorkerConfig,
    security: &SecurityConfig,
) -> Result<VerifiedGetCapability, Status> {
    let payload = verify_payload(value, worker, security)?;
    if payload.op != "get" {
        return Err(Status::permission_denied(format!(
            "worker capability op {:?} cannot authorize DoGet",
            payload.op
        )));
    }

    let path = payload
        .path
        .as_deref()
        .ok_or_else(|| Status::permission_denied("DoGet capability requires exact parquet path"))?;

    Ok(VerifiedGetCapability {
        bucket: validate_optional_bucket(payload.bucket)?,
        operation_id: validate_optional_id("operation_id", payload.operation_id)?,
        path: normalize_object_key(path),
        max_batch_rows: payload.max_batch_rows,
        max_record_batch_bytes: payload.max_record_batch_bytes,
    })
}

fn verify_payload(
    value: Value,
    worker: &WorkerConfig,
    security: &SecurityConfig,
) -> Result<CapabilityPayload, Status> {
    let envelope: CapabilityEnvelope = serde_json::from_value(value)
        .map_err(|err| Status::invalid_argument(format!("invalid worker capability: {err}")))?;
    if envelope.version != CAPABILITY_VERSION {
        return Err(Status::permission_denied(format!(
            "unsupported worker capability version {}",
            envelope.version
        )));
    }
    if !matches!(envelope.alg.as_deref().unwrap_or("HS256"), "HS256") {
        return Err(Status::permission_denied(
            "unsupported worker capability signature algorithm",
        ));
    }

    let secret = security.capability_secret.as_deref().ok_or_else(|| {
        Status::permission_denied("worker is missing capability verification secret")
    })?;
    verify_signature(secret, &envelope.payload, &envelope.signature)?;

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(envelope.payload.as_bytes())
        .map_err(|err| {
            Status::invalid_argument(format!("invalid worker capability payload encoding: {err}"))
        })?;
    let payload: CapabilityPayload = serde_json::from_slice(&payload_bytes).map_err(|err| {
        Status::invalid_argument(format!("invalid worker capability payload: {err}"))
    })?;

    validate_time_bounds(&payload, security)?;
    validate_worker_binding(&payload, worker, security)?;

    Ok(payload)
}

fn verify_signature(secret: &str, payload: &str, signature: &str) -> Result<(), Status> {
    let expected = sign_payload(secret, payload)?;
    let expected_bytes = URL_SAFE_NO_PAD
        .decode(expected.as_bytes())
        .map_err(|err| Status::internal(err.to_string()))?;
    let actual_bytes = URL_SAFE_NO_PAD
        .decode(signature.as_bytes())
        .map_err(|err| {
            Status::invalid_argument(format!(
                "invalid worker capability signature encoding: {err}"
            ))
        })?;

    if expected_bytes.len() != actual_bytes.len()
        || !constant_time_eq(&expected_bytes, &actual_bytes)
    {
        return Err(Status::permission_denied(
            "worker capability signature is invalid",
        ));
    }

    Ok(())
}

fn sign_payload(secret: &str, payload: &str) -> Result<String, Status> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|err| Status::internal(format!("invalid capability secret: {err}")))?;
    mac.update(payload.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn validate_time_bounds(
    payload: &CapabilityPayload,
    security: &SecurityConfig,
) -> Result<(), Status> {
    let now_ms = now_ms()?;
    if let Some(not_before_ms) = payload.not_before_ms {
        if now_ms < not_before_ms {
            return Err(Status::permission_denied(
                "worker capability is not active yet",
            ));
        }
    }

    let expires_at_ms = payload
        .expires_at_ms
        .ok_or_else(|| Status::permission_denied("worker capability requires expires_at_ms"))?;
    if now_ms > expires_at_ms {
        return Err(Status::permission_denied("worker capability has expired"));
    }
    if let Some(issued_at_ms) = payload.issued_at_ms {
        let max_expires_at_ms = issued_at_ms.saturating_add(security.max_capability_ttl_ms.max(1));
        if expires_at_ms > max_expires_at_ms {
            return Err(Status::permission_denied(format!(
                "worker capability ttl exceeds {}ms",
                security.max_capability_ttl_ms
            )));
        }
    }

    Ok(())
}

fn validate_worker_binding(
    payload: &CapabilityPayload,
    worker: &WorkerConfig,
    security: &SecurityConfig,
) -> Result<(), Status> {
    if let Some(worker_id) = payload.worker_id.as_deref() {
        if worker_id != worker.worker_id {
            return Err(Status::permission_denied(format!(
                "worker capability is bound to worker {worker_id:?}, not {:?}",
                worker.worker_id
            )));
        }
        return Ok(());
    }

    if let (Some(worker_group), Some(zone)) =
        (payload.worker_group.as_deref(), worker.zone.as_deref())
    {
        if worker_group == zone {
            return Ok(());
        }
    }

    if security.require_capability_worker_binding {
        return Err(Status::permission_denied(
            "worker capability requires worker_id binding",
        ));
    }

    Ok(())
}

pub(crate) fn normalize_prefix(raw: &str) -> Result<String, Status> {
    let prefix = raw
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .collect::<Vec<_>>()
        .join("/");

    if prefix.is_empty() {
        return Err(Status::invalid_argument(
            "capability prefix must not be empty",
        ));
    }

    Ok(format!("{prefix}/"))
}

pub(crate) fn validate_optional_id(
    name: &str,
    value: Option<String>,
) -> Result<Option<String>, Status> {
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

pub(crate) fn validate_optional_bucket(bucket: Option<String>) -> Result<Option<String>, Status> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };
    if bucket.is_empty()
        || bucket.starts_with("s3://")
        || bucket.starts_with("s3a://")
        || bucket.contains('/')
        || bucket.contains('\\')
    {
        return Err(Status::permission_denied(
            "capability bucket must be a plain bucket name without s3://, s3a://, or subdirectories",
        ));
    }
    Ok(Some(bucket))
}

fn now_ms() -> Result<u64, Status> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| Status::internal(err.to_string()))?
        .as_millis() as u64)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0u8, |diff, (left, right)| diff | (left ^ right))
        == 0
}

#[cfg(test)]
pub(crate) fn signed_envelope_for_test(payload: serde_json::Value, secret: &str) -> Bytes {
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
    let signature = sign_payload(secret, &payload).unwrap();
    Bytes::from(
        serde_json::json!({
            "version": CAPABILITY_VERSION,
            "alg": "HS256",
            "payload": payload,
            "signature": signature
        })
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SecurityConfig, WorkerConfig};

    fn worker() -> WorkerConfig {
        WorkerConfig {
            worker_id: "worker-1".to_owned(),
            flight_uri: "grpc+tcp://worker-1:50051".to_owned(),
            zone: Some("zone-a".to_owned()),
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
            require_structured_tickets: true,
            registry_heartbeat_interval_ms: 5_000,
            registry_ttl_ms: 15_000,
        }
    }

    fn security() -> SecurityConfig {
        SecurityConfig {
            require_signed_capabilities: true,
            capability_secret: Some("secret".to_owned()),
            require_capability_worker_binding: true,
            max_capability_ttl_ms: 3_600_000,
        }
    }

    #[test]
    fn verifies_signed_put_capability() {
        let ticket = signed_envelope_for_test(
            serde_json::json!({
                "op": "put",
                "worker_id": "worker-1",
                "expires_at_ms": 4102444800000u64,
                "allowed_output_prefix": "table/.staging/op-1",
                "staging_prefix": "table/.staging/op-1",
                "attempt_id": "attempt-1"
            }),
            "secret",
        );
        let value = parse_capability_envelope(&ticket).unwrap();
        let verified = verify_put_capability(value, &worker(), &security()).unwrap();

        assert_eq!(verified.attempt_id.as_deref(), Some("attempt-1"));
        assert_eq!(verified.allowed_output_prefix, "table/.staging/op-1/");
    }

    #[test]
    fn rejects_tampered_signature() {
        let ticket = signed_envelope_for_test(
            serde_json::json!({
                "op": "get",
                "worker_id": "worker-1",
                "expires_at_ms": 4102444800000u64,
                "path": "table/data/file.parquet"
            }),
            "secret",
        );
        let mut value: Value = serde_json::from_slice(&ticket).unwrap();
        value["signature"] = Value::String("bad".to_owned());

        let error = verify_get_capability(value, &worker(), &security())
            .expect_err("bad signature must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn rejects_wrong_worker_binding() {
        let ticket = signed_envelope_for_test(
            serde_json::json!({
                "op": "get",
                "worker_id": "worker-2",
                "expires_at_ms": 4102444800000u64,
                "path": "table/data/file.parquet"
            }),
            "secret",
        );
        let value = parse_capability_envelope(&ticket).unwrap();

        let error = verify_get_capability(value, &worker(), &security())
            .expect_err("wrong worker binding must be rejected");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn rejects_put_started_after_planning_reservation() {
        let ticket = signed_envelope_for_test(
            serde_json::json!({
                "op": "put",
                "worker_id": "worker-1",
                "expires_at_ms": 4102444800000u64,
                "start_before_ms": 1,
                "allowed_output_prefix": "table/data",
                "staging_prefix": "table/data",
                "attempt_id": "attempt-expired"
            }),
            "secret",
        );
        let value = parse_capability_envelope(&ticket).unwrap();

        let error = verify_put_capability(value, &worker(), &security())
            .expect_err("expired planning reservation must be rejected");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(error.message().contains("start reservation has expired"));
    }
}
