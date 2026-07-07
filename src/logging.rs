use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt::{self, Debug},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Number, Value};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
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
