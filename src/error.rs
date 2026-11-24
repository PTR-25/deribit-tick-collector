use thiserror::Error;

pub type Result<T> = std::result::Result<T, CollectorError>;

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Deribit protocol error (code={code}): {message}")]
    DeribitRpc { code: i64, message: String },

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[cfg(feature = "s3")]
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("invalid instrument name: {0}")]
    InvalidInstrument(String),
}
