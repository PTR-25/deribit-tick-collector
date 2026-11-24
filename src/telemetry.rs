use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

/// Lock-free runtime telemetry counters shared across all shards, storage writers, and uploaders.
#[derive(Debug)]
pub struct Telemetry {
    started_at: Instant,

    // Network / WebSocket counters
    pub active_shards: AtomicUsize,
    pub total_shards: AtomicUsize,
    pub subscribed_channels: AtomicUsize,
    pub ws_frames_rx: AtomicU64,
    pub ws_bytes_rx: AtomicU64,
    pub ws_reconnects: AtomicU64,
    pub ws_parse_errors: AtomicU64,
    pub heartbeats_rx: AtomicU64,

    // Market event counters
    pub book_messages: AtomicU64,
    pub book_level_rows: AtomicU64,
    pub book_snapshots: AtomicU64,
    pub book_checkpoints: AtomicU64,
    pub book_gaps: AtomicU64,
    pub book_resyncs: AtomicU64,
    pub ticker_rows: AtomicU64,
    pub ticker_oi_changes: AtomicU64,
    pub ticker_suppressed: AtomicU64,
    pub trade_rows: AtomicU64,
    pub combo_trade_rows: AtomicU64,
    pub index_rows: AtomicU64,
    pub instrument_updates: AtomicU64,

    // Backpressure & Storage counters
    pub ingest_drops: AtomicU64,
    pub parquet_row_groups_written: AtomicU64,
    pub parquet_rows_written: AtomicU64,
    pub parquet_files_rotated: AtomicU64,
    pub parquet_bytes_written: AtomicU64,

    // S3 uploader counters
    pub s3_uploads_ok: AtomicU64,
    pub s3_uploads_err: AtomicU64,
    pub s3_bytes_uploaded: AtomicU64,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl Telemetry {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            active_shards: AtomicUsize::new(0),
            total_shards: AtomicUsize::new(0),
            subscribed_channels: AtomicUsize::new(0),
            ws_frames_rx: AtomicU64::new(0),
            ws_bytes_rx: AtomicU64::new(0),
            ws_reconnects: AtomicU64::new(0),
            ws_parse_errors: AtomicU64::new(0),
            heartbeats_rx: AtomicU64::new(0),
            book_messages: AtomicU64::new(0),
            book_level_rows: AtomicU64::new(0),
            book_snapshots: AtomicU64::new(0),
            book_checkpoints: AtomicU64::new(0),
            book_gaps: AtomicU64::new(0),
            book_resyncs: AtomicU64::new(0),
            ticker_rows: AtomicU64::new(0),
            ticker_oi_changes: AtomicU64::new(0),
            ticker_suppressed: AtomicU64::new(0),
            trade_rows: AtomicU64::new(0),
            combo_trade_rows: AtomicU64::new(0),
            index_rows: AtomicU64::new(0),
            instrument_updates: AtomicU64::new(0),
            ingest_drops: AtomicU64::new(0),
            parquet_row_groups_written: AtomicU64::new(0),
            parquet_rows_written: AtomicU64::new(0),
            parquet_files_rotated: AtomicU64::new(0),
            parquet_bytes_written: AtomicU64::new(0),
            s3_uploads_ok: AtomicU64::new(0),
            s3_uploads_err: AtomicU64::new(0),
            s3_bytes_uploaded: AtomicU64::new(0),
        }
    }

    pub fn uptime_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn is_healthy(&self) -> bool {
        let total = self.total_shards.load(Ordering::Relaxed);
        let active = self.active_shards.load(Ordering::Relaxed);
        total == 0 || active > 0
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        let _ = writeln!(out, "# TYPE collector_uptime_seconds gauge");
        let _ = writeln!(out, "collector_uptime_seconds {:.3}", self.uptime_secs());

        macro_rules! gauge {
            ($name:expr, $val:expr) => {
                let _ = writeln!(out, "# TYPE {} gauge", $name);
                let _ = writeln!(out, "{} {}", $name, $val.load(Ordering::Relaxed));
            };
        }
        macro_rules! counter {
            ($name:expr, $val:expr) => {
                let _ = writeln!(out, "# TYPE {} counter", $name);
                let _ = writeln!(out, "{} {}", $name, $val.load(Ordering::Relaxed));
            };
        }

        gauge!("collector_shards_active", self.active_shards);
        gauge!("collector_shards_total", self.total_shards);
        gauge!("collector_channels_subscribed", self.subscribed_channels);

        counter!("collector_ws_frames_rx_total", self.ws_frames_rx);
        counter!("collector_ws_bytes_rx_total", self.ws_bytes_rx);
        counter!("collector_ws_reconnects_total", self.ws_reconnects);
        counter!("collector_ws_parse_errors_total", self.ws_parse_errors);
        counter!("collector_heartbeats_rx_total", self.heartbeats_rx);

        counter!("collector_book_messages_total", self.book_messages);
        counter!("collector_book_level_rows_total", self.book_level_rows);
        counter!("collector_book_snapshots_total", self.book_snapshots);
        counter!("collector_book_checkpoints_total", self.book_checkpoints);
        counter!("collector_book_gaps_total", self.book_gaps);
        counter!("collector_book_resyncs_total", self.book_resyncs);
        counter!("collector_ticker_rows_total", self.ticker_rows);
        counter!("collector_ticker_oi_changes_total", self.ticker_oi_changes);
        counter!("collector_ticker_suppressed_total", self.ticker_suppressed);
        counter!("collector_trade_rows_total", self.trade_rows);
        counter!("collector_combo_trade_rows_total", self.combo_trade_rows);
        counter!("collector_index_rows_total", self.index_rows);
        counter!(
            "collector_instrument_updates_total",
            self.instrument_updates
        );

        counter!("collector_ingest_drops_total", self.ingest_drops);
        counter!(
            "collector_parquet_row_groups_written_total",
            self.parquet_row_groups_written
        );
        counter!(
            "collector_parquet_rows_written_total",
            self.parquet_rows_written
        );
        counter!(
            "collector_parquet_files_rotated_total",
            self.parquet_files_rotated
        );
        counter!(
            "collector_parquet_bytes_written_total",
            self.parquet_bytes_written
        );

        counter!("collector_s3_uploads_ok_total", self.s3_uploads_ok);
        counter!("collector_s3_uploads_err_total", self.s3_uploads_err);
        counter!("collector_s3_bytes_uploaded_total", self.s3_bytes_uploaded);

        out
    }

    pub fn log_summary(&self) {
        let up = self.uptime_secs().max(0.001);
        let frames = self.ws_frames_rx.load(Ordering::Relaxed);
        let bytes = self.ws_bytes_rx.load(Ordering::Relaxed);
        let book_msgs = self.book_messages.load(Ordering::Relaxed);
        let book_rows = self.book_level_rows.load(Ordering::Relaxed);
        let tickers = self.ticker_rows.load(Ordering::Relaxed);
        let oi_changes = self.ticker_oi_changes.load(Ordering::Relaxed);
        let trades = self.trade_rows.load(Ordering::Relaxed);
        let indices = self.index_rows.load(Ordering::Relaxed);
        let checkpoints = self.book_checkpoints.load(Ordering::Relaxed);
        let gaps = self.book_gaps.load(Ordering::Relaxed);
        let drops = self.ingest_drops.load(Ordering::Relaxed);
        let pq_rows = self.parquet_rows_written.load(Ordering::Relaxed);
        let pq_bytes = self.parquet_bytes_written.load(Ordering::Relaxed);
        let pq_files = self.parquet_files_rotated.load(Ordering::Relaxed);

        info!(
            shards = format!(
                "{}/{}",
                self.active_shards.load(Ordering::Relaxed),
                self.total_shards.load(Ordering::Relaxed)
            ),
            channels = self.subscribed_channels.load(Ordering::Relaxed),
            frames_per_sec = format!("{:.1}", frames as f64 / up),
            rx_kib_per_sec = format!("{:.1}", (bytes as f64 / 1024.0) / up),
            book_msgs,
            book_rows,
            tickers,
            oi_changes,
            trades,
            indices,
            checkpoints,
            gaps,
            drops,
            pq_rows,
            pq_files,
            pq_mib = format!("{:.2}", pq_bytes as f64 / (1024.0 * 1024.0)),
            "collector runtime stats"
        );
    }
}

/// Spawn a lightweight zero-dependency HTTP/1.1 Prometheus `/metrics` & `/healthz` server.
pub fn spawn_metrics_server(
    bind_addr: String,
    telemetry: Arc<Telemetry>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => {
                info!(addr = %bind_addr, "telemetry HTTP server listening (/metrics, /healthz)");
                l
            }
            Err(e) => {
                warn!(addr = %bind_addr, error = %e, "failed to bind telemetry HTTP server");
                return;
            }
        };

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                accept = listener.accept() => {
                    let Ok((mut stream, _)) = accept else {
                        continue;
                    };
                    let tel = Arc::clone(&telemetry);
                    tokio::spawn(async move {
                        let mut req_buf = [0u8; 1024];
                        let n = match tokio::time::timeout(
                            Duration::from_secs(2),
                            stream.read(&mut req_buf),
                        ).await {
                            Ok(Ok(n)) if n > 0 => n,
                            _ => return,
                        };
                        let req = std::str::from_utf8(&req_buf[..n]).unwrap_or("");
                        let first_line = req.lines().next().unwrap_or("");
                        let path = first_line.split_whitespace().nth(1).unwrap_or("/");

                        let (status, content_type, body) = match path {
                            "/metrics" => (
                                "200 OK",
                                "text/plain; version=0.0.4; charset=utf-8",
                                tel.render_prometheus(),
                            ),
                            "/healthz" | "/readyz" => {
                                if tel.is_healthy() {
                                    ("200 OK", "text/plain; charset=utf-8", "ok\n".to_string())
                                } else {
                                    (
                                        "503 Service Unavailable",
                                        "text/plain; charset=utf-8",
                                        "no active shards\n".to_string(),
                                    )
                                }
                            }
                            _ => (
                                "404 Not Found",
                                "text/plain; charset=utf-8",
                                "not found\n".to_string(),
                            ),
                        };

                        let resp = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    });
                }
            }
        }
    })
}

/// Periodic structured stats logger.
pub fn spawn_stats_logger(
    interval_secs: u64,
    telemetry: Arc<Telemetry>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if interval_secs == 0 {
            return;
        }
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    telemetry.log_summary();
                }
            }
        }
    })
}
