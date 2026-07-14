use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt::{self, Debug},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Number, Value};
use tonic::{Code, Status, metadata::MetadataValue};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};

pub const ERROR_ID_METADATA: &str = "x-error-id";
pub const OPERATION_ID_METADATA: &str = "x-operation-id";
pub const UPLOAD_ID_METADATA: &str = "x-upload-id";
pub const STREAM_ID_METADATA: &str = "x-stream-id";
pub const ATTEMPT_ID_METADATA: &str = "x-attempt-id";

#[derive(Clone, Debug, Default)]
pub struct WorkerRequestContext {
    pub worker_id: String,
    pub bucket: String,
    pub key: String,
    pub operation_id: Option<String>,
    pub upload_id: Option<String>,
    pub stream_id: Option<String>,
    pub attempt_id: Option<String>,
}

pub fn enrich_worker_status(status: Status, context: &WorkerRequestContext) -> Status {
    if status.metadata().get(ERROR_ID_METADATA).is_some() {
        return status;
    }

    let error_id = format!(
        "worker-err-{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    let mut metadata = status.metadata().clone();
    insert_status_metadata(&mut metadata, ERROR_ID_METADATA, &error_id);
    insert_optional_status_metadata(
        &mut metadata,
        OPERATION_ID_METADATA,
        context.operation_id.as_deref(),
    );
    insert_optional_status_metadata(
        &mut metadata,
        UPLOAD_ID_METADATA,
        context.upload_id.as_deref(),
    );
    insert_optional_status_metadata(
        &mut metadata,
        STREAM_ID_METADATA,
        context.stream_id.as_deref(),
    );
    insert_optional_status_metadata(
        &mut metadata,
        ATTEMPT_ID_METADATA,
        context.attempt_id.as_deref(),
    );

    let mut message = format!("errorId={error_id}");
    append_status_id(&mut message, "operationId", context.operation_id.as_deref());
    append_status_id(&mut message, "uploadId", context.upload_id.as_deref());
    append_status_id(&mut message, "streamId", context.stream_id.as_deref());
    append_status_id(&mut message, "attemptId", context.attempt_id.as_deref());
    message.push_str(": ");
    message.push_str(status.message());

    Status::with_details_and_metadata(
        status.code(),
        message,
        bytes::Bytes::copy_from_slice(status.details()),
        metadata,
    )
}

pub fn status_metadata<'a>(status: &'a Status, name: &str) -> &'a str {
    status
        .metadata()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

pub fn worker_error_class(status: &Status) -> &'static str {
    match status.code() {
        Code::Unknown | Code::Internal | Code::Unavailable | Code::DataLoss => "internal",
        _ => "request",
    }
}

fn insert_optional_status_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    name: &'static str,
    value: Option<&str>,
) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        insert_status_metadata(metadata, name, value);
    }
}

fn insert_status_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    name: &'static str,
    value: &str,
) {
    if let Ok(value) = MetadataValue::try_from(value) {
        metadata.insert(name, value);
    }
}

fn append_status_id(message: &mut String, name: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        message.push(' ');
        message.push_str(name);
        message.push('=');
        message.push_str(value);
    }
}
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        FmtContext,
        format::{FormatEvent, FormatFields, Writer},
    },
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
};

#[derive(Clone)]
pub struct LogIdentity {
    env: String,
    group: String,
    system: String,
    namespace: String,
}

impl LogIdentity {
    pub fn from_env(default_system: &str) -> Self {
        Self {
            env: env_value(&["LOG_ENV", "APP_ENV", "ENV"], "local"),
            group: env_value(&["LOG_GROUP", "GROUP"], "arrow-flight"),
            system: env_value(&["LOG_SYSTEM", "SYSTEM"], default_system),
            namespace: env_value(&["LOG_NAMESPACE", "POD_NAMESPACE", "NAMESPACE"], "local"),
        }
    }

    fn insert(&self, fields: &mut Map<String, Value>) {
        fields.insert("env".to_owned(), Value::String(self.env.clone()));
        fields.insert("group".to_owned(), Value::String(self.group.clone()));
        fields.insert("system".to_owned(), Value::String(self.system.clone()));
        fields.insert(
            "namespace".to_owned(),
            Value::String(self.namespace.clone()),
        );
    }
}

pub fn init_tracing(default_system: &str) {
    let identity = LogIdentity::from_env(default_system);
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(JsonLogFormatter { identity })
                .with_ansi(false),
        )
        .init();
}

struct JsonLogFormatter {
    identity: LogIdentity,
}

impl<S, N> FormatEvent<S, N> for JsonLogFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let mut fields = Map::new();
        self.identity.insert(&mut fields);
        fields.insert(
            "timestamp".to_owned(),
            Value::String(unix_timestamp_millis()),
        );
        fields.insert(
            "level".to_owned(),
            Value::String(metadata.level().to_string()),
        );
        fields.insert(
            "target".to_owned(),
            Value::String(metadata.target().to_owned()),
        );

        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);
        for (key, value) in visitor.fields {
            fields.insert(key, value);
        }

        writeln!(writer, "{}", Value::Object(fields))
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    fields: BTreeMap<String, Value>,
}

impl JsonFieldVisitor {
    fn insert(&mut self, field: &Field, value: Value) {
        self.fields.insert(field.name().to_owned(), value);
    }
}

impl Visit for JsonFieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, Value::String(value.to_owned()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, Value::Number(Number::from(value)));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, Value::Number(Number::from(value)));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let value = Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string()));
        self.insert(field, value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn Error + 'static)) {
        self.insert(field, Value::String(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.insert(field, Value::String(format!("{value:?}")));
    }
}

fn env_value(keys: &[&str], default: &str) -> String {
    keys.iter()
        .find_map(|key| env::var(key).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn unix_timestamp_millis() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    millis.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_error_status_carries_structured_correlation_metadata() {
        let status = enrich_worker_status(
            Status::invalid_argument("bad ticket"),
            &WorkerRequestContext {
                operation_id: Some("read-123".to_owned()),
                upload_id: Some("upload-123".to_owned()),
                stream_id: Some("stream-2".to_owned()),
                attempt_id: Some("attempt-2".to_owned()),
                ..WorkerRequestContext::default()
            },
        );

        assert!(status_metadata(&status, ERROR_ID_METADATA).starts_with("worker-err-"));
        assert_eq!(status_metadata(&status, OPERATION_ID_METADATA), "read-123");
        assert_eq!(status_metadata(&status, UPLOAD_ID_METADATA), "upload-123");
        assert_eq!(status_metadata(&status, STREAM_ID_METADATA), "stream-2");
        assert_eq!(status_metadata(&status, ATTEMPT_ID_METADATA), "attempt-2");
        assert!(status.message().contains("operationId=read-123"));
        assert_eq!(worker_error_class(&status), "request");
    }

    #[test]
    fn worker_error_status_is_not_wrapped_twice() {
        let first = enrich_worker_status(
            Status::internal("storage failed"),
            &WorkerRequestContext::default(),
        );
        let error_id = status_metadata(&first, ERROR_ID_METADATA).to_owned();
        let message = first.message().to_owned();

        let second = enrich_worker_status(first, &WorkerRequestContext::default());

        assert_eq!(status_metadata(&second, ERROR_ID_METADATA), error_id);
        assert_eq!(second.message(), message);
        assert_eq!(worker_error_class(&second), "internal");
    }
}
