use anyhow::{Result, bail};

pub fn tonic_uri(uri: &str) -> Result<String> {
    let value = uri.trim();
    if value.is_empty() {
        bail!("Flight URI must not be empty");
    }

    if let Some(rest) = value.strip_prefix("grpc+tcp://") {
        return Ok(format!("http://{rest}"));
    }
    if let Some(rest) = value.strip_prefix("grpc+tls://") {
        return Ok(format!("https://{rest}"));
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return Ok(value.to_owned());
    }

    bail!(
        "unsupported Flight URI scheme in {value}; use grpc+tcp://, grpc+tls://, http://, or https://"
    );
}
