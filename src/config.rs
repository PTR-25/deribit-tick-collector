use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{CollectorError, Result};

/// Maximum channels allowed per Deribit WebSocket connection.
pub const DERIBIT_MAX_CHANNELS_PER_CONN: usize = 500;
/// Maximum simultaneous WebSocket connections recommended per IP on Deribit.
pub const DERIBIT_MAX_CONNS_PER_IP: usize = 32;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub deribit: DeribitConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub s3: S3Config,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DeribitConfig {
    pub ws_url: String,
    pub rest_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    /// Market families to subscribe to, e.g. ["BTC", "ETH", "SOL_USDC"] or ["ALL"].
    pub markets: Vec<String>,
    pub collect_options: bool,
    pub collect_perpetuals: bool,
    pub collect_dated_futures: bool,
    /// Optional upper bound on days-to-expiry for options and dated futures (None or 0 = unlimited).
    pub max_dte_days: Option<u64>,
    pub collect_orderbook: bool,
    pub options_book_interval: BookInterval,
    pub futures_book_interval: BookInterval,
    pub collect_ticker: bool,
    pub ticker_mode: TickerMode,
    pub ticker_emit_policy: TickerEmitPolicy,
    pub collect_trades: bool,
    pub collect_combo_trades: bool,
    pub trades_interval: BookInterval,
    /// Subscribe to Deribit spot price indices (`deribit_price_index.*`) and 30d DVOL
    /// volatility indices (`deribit_volatility_index.*`) for configured markets.
    pub collect_indices: bool,
}

impl Default for DeribitConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://www.deribit.com/ws/api/v2".to_string(),
            rest_url: "https://www.deribit.com/api/v2".to_string(),
            client_id: None,
            client_secret: None,
            markets: vec!["BTC".to_string(), "ETH".to_string()],
            collect_options: true,
            collect_perpetuals: true,
            collect_dated_futures: true,
            max_dte_days: None,
            collect_orderbook: true,
            options_book_interval: BookInterval::Raw,
            futures_book_interval: BookInterval::Raw,
            collect_ticker: true,
            ticker_mode: TickerMode::Incremental,
            ticker_emit_policy: TickerEmitPolicy::All,
            collect_trades: true,
            collect_combo_trades: true,
            trades_interval: BookInterval::Raw,
            collect_indices: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum BookInterval {
    #[value(name = "raw")]
    Raw,
    #[serde(rename = "100ms")]
    #[value(name = "100ms")]
    Ms100,
    #[value(name = "agg2")]
    Agg2,
}

impl BookInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Ms100 => "100ms",
            Self::Agg2 => "agg2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TickerMode {
    #[value(name = "incremental")]
    Incremental,
    #[value(name = "raw")]
    Raw,
    #[serde(rename = "100ms")]
    #[value(name = "100ms")]
    Ms100,
    #[value(name = "agg2")]
    Agg2,
}

impl TickerMode {
    pub fn channel_for(self, instrument_name: &str) -> String {
        match self {
            Self::Incremental => format!("incremental_ticker.{instrument_name}"),
            Self::Raw => format!("ticker.{instrument_name}.raw"),
            Self::Ms100 => format!("ticker.{instrument_name}.100ms"),
            Self::Agg2 => format!("ticker.{instrument_name}.agg2"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TickerEmitPolicy {
    /// Emit every ticker message where at least one tracked field changed (plus initial snapshots).
    All,
    /// Emit snapshots and updates where non-BBO state changed (OI, IV, Greeks, funding, volume,
    /// mark/index/underlying/last/settlement price, or instrument state).
    InterestingOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    pub max_channels_per_conn: usize,
    pub subscribe_batch_size: usize,
    pub subscribe_pace_ms: u64,
    pub heartbeat_interval_secs: u64,
    pub read_stall_timeout_secs: u64,
    pub tcp_recv_buffer_kib: usize,
    pub catalog_reconcile_secs: u64,
    pub ingest_channel_capacity: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            max_channels_per_conn: 420,
            subscribe_batch_size: 150,
            subscribe_pace_ms: 350,
            heartbeat_interval_secs: 30,
            read_stall_timeout_secs: 90,
            tcp_recv_buffer_kib: 4096,
            catalog_reconcile_secs: 900,
            ingest_channel_capacity: 262_144,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum CompressionCodec {
    Zstd,
    Snappy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub compression: CompressionCodec,
    pub zstd_level: i32,
    pub max_rows_per_row_group: usize,
    pub flush_interval_secs: u64,
    pub rotation_interval_secs: u64,
    pub max_segment_size_mib: u64,
    pub fsync_on_rotate: bool,
    pub local_retention_hours: u64,
    /// Emit an anchor L2 snapshot (and quiet ticker OI anchor) at every UTC hour boundary so each
    /// `date=YYYY-MM-DD/hour=HH` partition is independently replayable without reading prior hours.
    pub hourly_book_checkpoints: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            compression: CompressionCodec::Zstd,
            zstd_level: 3,
            max_rows_per_row_group: 250_000,
            flush_interval_secs: 30,
            rotation_interval_secs: 900,
            max_segment_size_mib: 256,
            fsync_on_rotate: false,
            local_retention_hours: 0,
            hourly_book_checkpoints: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub enabled: bool,
    pub bucket: Option<String>,
    pub prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub delete_local_after_upload: bool,
    pub max_concurrent_uploads: usize,
    pub max_retries: usize,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            enabled: false,
            bucket: None,
            prefix: "deribit".to_string(),
            region: "eu-west-2".to_string(),
            endpoint: None,
            delete_local_after_upload: false,
            max_concurrent_uploads: 4,
            max_retries: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    pub metrics_bind: String,
    pub stats_log_interval_secs: u64,
    pub json_logs: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            metrics_bind: "127.0.0.1:9108".to_string(),
            stats_log_interval_secs: 30,
            json_logs: false,
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut cfg = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p).map_err(|e| {
                    CollectorError::Config(format!("failed to read config {}: {e}", p.display()))
                })?;
                toml::from_str::<AppConfig>(&text).map_err(|e| {
                    CollectorError::Config(format!("failed to parse TOML {}: {e}", p.display()))
                })?
            }
            None => AppConfig::default(),
        };

        cfg.apply_env_overrides();
        cfg.normalize();
        Ok(cfg)
    }

    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("DERIBIT_WS_URL")
            && !v.trim().is_empty()
        {
            self.deribit.ws_url = v;
        }
        if let Ok(v) = std::env::var("DERIBIT_REST_URL")
            && !v.trim().is_empty()
        {
            self.deribit.rest_url = v;
        }
        if let Ok(v) = std::env::var("DERIBIT_CLIENT_ID")
            && !v.trim().is_empty()
        {
            self.deribit.client_id = Some(v);
        }
        if let Ok(v) = std::env::var("DERIBIT_CLIENT_SECRET")
            && !v.trim().is_empty()
        {
            self.deribit.client_secret = Some(v);
        }
        if let Ok(v) = std::env::var("S3_BUCKET")
            && !v.trim().is_empty()
        {
            self.s3.bucket = Some(v);
        }
        if let Ok(v) = std::env::var("S3_PREFIX")
            && !v.trim().is_empty()
        {
            self.s3.prefix = v;
        }
        if let Ok(v) = std::env::var("AWS_REGION")
            && !v.trim().is_empty()
        {
            self.s3.region = v;
        }
        if let Ok(v) = std::env::var("S3_ENDPOINT")
            && !v.trim().is_empty()
        {
            self.s3.endpoint = Some(v);
        }
    }

    pub fn normalize(&mut self) {
        for m in &mut self.deribit.markets {
            *m = m.trim().to_ascii_uppercase();
        }
        self.deribit.markets.retain(|m| !m.is_empty());
        if self.deribit.markets.is_empty() {
            self.deribit.markets = vec!["BTC".to_string(), "ETH".to_string()];
        }
    }

    pub fn validate(&self) -> Result<()> {
        if !self.deribit.collect_options
            && !self.deribit.collect_perpetuals
            && !self.deribit.collect_dated_futures
        {
            return Err(CollectorError::Config(
                "at least one of collect_options, collect_perpetuals, or collect_dated_futures must be enabled".into(),
            ));
        }

        if !self.deribit.collect_orderbook
            && !self.deribit.collect_ticker
            && !self.deribit.collect_trades
            && !self.deribit.collect_indices
        {
            return Err(CollectorError::Config(
                "at least one of collect_orderbook, collect_ticker, collect_trades, or collect_indices must be enabled".into(),
            ));
        }

        if self.network.max_channels_per_conn == 0
            || self.network.max_channels_per_conn > DERIBIT_MAX_CHANNELS_PER_CONN
        {
            return Err(CollectorError::Config(format!(
                "network.max_channels_per_conn must be in 1..={DERIBIT_MAX_CHANNELS_PER_CONN}"
            )));
        }

        if self.network.subscribe_batch_size == 0
            || self.network.subscribe_batch_size > self.network.max_channels_per_conn
        {
            return Err(CollectorError::Config(
                "network.subscribe_batch_size must be in 1..=max_channels_per_conn".into(),
            ));
        }

        if self.network.heartbeat_interval_secs < 10 {
            return Err(CollectorError::Config(
                "network.heartbeat_interval_secs must be >= 10 (Deribit minimum)".into(),
            ));
        }

        if self.network.read_stall_timeout_secs <= self.network.heartbeat_interval_secs {
            return Err(CollectorError::Config(
                "network.read_stall_timeout_secs must exceed heartbeat_interval_secs".into(),
            ));
        }

        if !(1..=22).contains(&self.storage.zstd_level) {
            return Err(CollectorError::Config(
                "storage.zstd_level must be in 1..=22".into(),
            ));
        }

        if self.storage.max_rows_per_row_group == 0 {
            return Err(CollectorError::Config(
                "storage.max_rows_per_row_group must be > 0".into(),
            ));
        }

        if self.s3.enabled {
            let bucket_ok = self
                .s3
                .bucket
                .as_deref()
                .is_some_and(|b| !b.trim().is_empty());
            if !bucket_ok {
                return Err(CollectorError::Config(
                    "s3.enabled = true requires s3.bucket or S3_BUCKET".into(),
                ));
            }
        }

        match (&self.deribit.client_id, &self.deribit.client_secret) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(CollectorError::Config(
                    "both client_id and client_secret must be set together".into(),
                ));
            }
            _ => {}
        }

        Ok(())
    }

    pub fn has_credentials(&self) -> bool {
        self.deribit.client_id.is_some() && self.deribit.client_secret.is_some()
    }
}
