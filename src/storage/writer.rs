use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use chrono::{DateTime, Datelike, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::StorageConfig;
use crate::error::Result;
use crate::model::{InstrumentKind, MarketEvent};
use crate::registry::InstrumentRegistry;
use crate::storage::buffers::{
    BookBuffer, CaptureEventBuffer, IndexBuffer, InstrumentBuffer, TickerBuffer, TradeBuffer,
};
use crate::storage::schema::{Dataset, writer_properties_for};
use crate::telemetry::Telemetry;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    MarketStream {
        dataset: Dataset,
        market: Arc<str>,
        kind: InstrumentKind,
        date: String,
        hour: String,
    },
    HourlyGlobalStream {
        dataset: Dataset,
        date: String,
        hour: String,
    },
    GlobalStream {
        dataset: Dataset,
        date: String,
    },
}

impl PartitionKey {
    pub fn dataset(&self) -> Dataset {
        match self {
            Self::MarketStream { dataset, .. }
            | Self::HourlyGlobalStream { dataset, .. }
            | Self::GlobalStream { dataset, .. } => *dataset,
        }
    }

    pub fn dir_path(&self, base_dir: &Path) -> PathBuf {
        match self {
            Self::MarketStream {
                dataset,
                market,
                kind,
                date,
                hour,
            } => base_dir
                .join(format!("dataset={}", dataset.as_str()))
                .join(format!("currency={market}"))
                .join(format!("kind={}", kind.as_str()))
                .join(format!("date={date}"))
                .join(format!("hour={hour}")),
            Self::HourlyGlobalStream {
                dataset,
                date,
                hour,
            } => base_dir
                .join(format!("dataset={}", dataset.as_str()))
                .join(format!("date={date}"))
                .join(format!("hour={hour}")),
            Self::GlobalStream { dataset, date } => base_dir
                .join(format!("dataset={}", dataset.as_str()))
                .join(format!("date={date}")),
        }
    }

    fn is_stale_hour(&self, cur_date: &str, cur_hour: &str) -> bool {
        match self {
            Self::MarketStream { date, hour, .. } | Self::HourlyGlobalStream { date, hour, .. } => {
                date != cur_date || hour != cur_hour
            }
            Self::GlobalStream { date, .. } => date != cur_date,
        }
    }
}

enum PartitionBuffer {
    Book(Box<BookBuffer>),
    Ticker(TickerBuffer),
    Trade(TradeBuffer),
    Index(IndexBuffer),
    Instrument(InstrumentBuffer),
    Capture(CaptureEventBuffer),
}

impl PartitionBuffer {
    fn new(dataset: Dataset) -> Self {
        match dataset {
            Dataset::BookUpdates => Self::Book(Box::default()),
            Dataset::Tickers => Self::Ticker(TickerBuffer::default()),
            Dataset::Trades => Self::Trade(TradeBuffer::default()),
            Dataset::Indices => Self::Index(IndexBuffer::default()),
            Dataset::Instruments => Self::Instrument(InstrumentBuffer::default()),
            Dataset::CaptureEvents => Self::Capture(CaptureEventBuffer::default()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Book(b) => b.len(),
            Self::Ticker(b) => b.len(),
            Self::Trade(b) => b.len(),
            Self::Index(b) => b.len(),
            Self::Instrument(b) => b.len(),
            Self::Capture(b) => b.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn drain_batch(&mut self) -> Result<RecordBatch> {
        match self {
            Self::Book(b) => b.drain_sorted_batch(),
            Self::Ticker(b) => b.drain_sorted_batch(),
            Self::Trade(b) => b.drain_sorted_batch(),
            Self::Index(b) => b.drain_sorted_batch(),
            Self::Instrument(b) => b.drain_batch(),
            Self::Capture(b) => b.drain_batch(),
        }
    }
}

enum IoCommand {
    WriteBatch {
        key: PartitionKey,
        batch: RecordBatch,
    },
    RotateKey(PartitionKey),
    RotateDue {
        cur_date: String,
        cur_hour: String,
    },
    Shutdown,
}

struct OpenSegment {
    tmp_path: PathBuf,
    final_path: PathBuf,
    writer: ArrowWriter<BufWriter<File>>,
    opened_at: Instant,
    rows_written: u64,
}

/// Remove orphaned `.parquet.tmp` files left behind by a previous hard crash (SIGKILL / power loss).
pub fn cleanup_stale_tmp_files(base_dir: &Path) -> usize {
    if !base_dir.exists() {
        return 0;
    }
    let mut removed = 0usize;
    let mut stack = vec![base_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".parquet.tmp"))
                && fs::remove_file(&path).is_ok()
            {
                removed += 1;
            }
        }
    }
    if removed > 0 {
        warn!(removed, dir = %base_dir.display(), "cleaned up stale .parquet.tmp files from previous run");
    }
    removed
}

/// Run the two-stage decoupled storage engine.
///
/// Stage A (async accumulator) drains bursts of `MarketEvent`s into SoA buffers, builds sorted
/// Arrow `RecordBatch`es on row-group/timer thresholds, and never stalls on disk I/O.
/// Stage B (dedicated OS thread) compresses row groups with Parquet v2, ZSTD, BYTE_STREAM_SPLIT,
/// and DELTA_BINARY_PACKED, atomically rotates `.parquet.tmp` to `.parquet`, and hands finalized
/// segments to the optional S3 uploader.
pub async fn run_storage_engine(
    cfg: StorageConfig,
    registry: Arc<InstrumentRegistry>,
    telemetry: Arc<Telemetry>,
    mut ingest_rx: mpsc::Receiver<MarketEvent>,
    upload_tx: Option<mpsc::Sender<PathBuf>>,
) -> Result<()> {
    fs::create_dir_all(&cfg.data_dir)?;
    cleanup_stale_tmp_files(&cfg.data_dir);

    let (io_tx, io_rx) = std::sync::mpsc::sync_channel::<IoCommand>(128);

    let io_cfg = cfg.clone();
    let io_telemetry = Arc::clone(&telemetry);
    let io_handle = std::thread::Builder::new()
        .name("parquet-io".to_string())
        .spawn(move || run_parquet_io_thread(io_cfg, io_telemetry, io_rx, upload_tx))
        .map_err(crate::error::CollectorError::Io)?;

    let mut buffers: HashMap<PartitionKey, PartitionBuffer> = HashMap::new();
    let max_rg = cfg.max_rows_per_row_group;
    let flush_secs = cfg.flush_interval_secs.max(1);
    let rotation_dur = Duration::from_secs(cfg.rotation_interval_secs.max(10));
    let mut partition_opened: HashMap<PartitionKey, Instant> = HashMap::new();

    let mut flush_tick = tokio::time::interval(Duration::from_secs(flush_secs));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    flush_tick.tick().await;

    let mut rotate_check = tokio::time::interval(Duration::from_secs(5));
    rotate_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    rotate_check.tick().await;

    loop {
        tokio::select! {
            biased;

            maybe_ev = ingest_rx.recv() => {
                let Some(first) = maybe_ev else {
                    // All senders dropped -> graceful shutdown drain!
                    break;
                };
                accumulate_event(&registry, &mut buffers, &mut partition_opened, first, max_rg, &io_tx);

                // Burst-drain up to 4095 queued events to amortize tokio wakeups.
                for _ in 0..4095 {
                    match ingest_rx.try_recv() {
                        Ok(ev) => accumulate_event(&registry, &mut buffers, &mut partition_opened, ev, max_rg, &io_tx),
                        Err(_) => break,
                    }
                }

                // Enforce a strict global in-memory row cap across all partitions.
                let total_buffered: usize = buffers.values().map(PartitionBuffer::len).sum();
                if total_buffered >= max_rg.saturating_mul(2)
                    && let Some(largest_key) = buffers
                        .iter()
                        .max_by_key(|(_, b)| b.len())
                        .map(|(k, _)| k.clone())
                    && let Some(buf) = buffers.get_mut(&largest_key)
                {
                    send_buffer_batch(largest_key, buf, &io_tx);
                }
            }

            _ = flush_tick.tick() => {
                // Only spill large buffers mid-segment so quiet partitions avoid tiny row groups.
                let min_mid_flush = (max_rg / 4).max(16_384);
                for (key, buf) in buffers.iter_mut() {
                    if buf.len() >= min_mid_flush {
                        send_buffer_batch(key.clone(), buf, &io_tx);
                    }
                }
            }

            _ = rotate_check.tick() => {
                let (cur_date, cur_hour) = utc_date_hour_from_ms(Utc::now().timestamp_millis());
                let due_keys: Vec<PartitionKey> = partition_opened
                    .iter()
                    .filter(|(k, opened)| {
                        k.is_stale_hour(&cur_date, &cur_hour) || opened.elapsed() >= rotation_dur
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in due_keys {
                    partition_opened.remove(&key);
                    if let Some(mut buf) = buffers.remove(&key)
                        && !buf.is_empty()
                    {
                        send_buffer_batch(key.clone(), &mut buf, &io_tx);
                    }
                    let _ = io_tx.send(IoCommand::RotateKey(key));
                }
                let _ = io_tx.send(IoCommand::RotateDue { cur_date, cur_hour });
            }
        }
    }

    // Graceful shutdown: flush all remaining buffered rows and finalize every open Parquet file!
    info!("storage engine draining remaining buffers before shutdown");
    flush_all_buffers(&mut buffers, &io_tx);
    let _ = io_tx.send(IoCommand::Shutdown);
    drop(io_tx);

    let io_res = tokio::task::spawn_blocking(move || io_handle.join()).await;
    match io_res {
        Ok(Ok(Ok(()))) => info!("parquet-io thread finalized all segments cleanly"),
        Ok(Ok(Err(e))) => error!(error = %e, "parquet-io thread exited with error"),
        Ok(Err(_)) => error!("parquet-io thread panicked"),
        Err(e) => error!(error = %e, "failed joining parquet-io thread"),
    }

    Ok(())
}

fn accumulate_event(
    registry: &InstrumentRegistry,
    buffers: &mut HashMap<PartitionKey, PartitionBuffer>,
    partition_opened: &mut HashMap<PartitionKey, Instant>,
    ev: MarketEvent,
    max_rg: usize,
    io_tx: &std::sync::mpsc::SyncSender<IoCommand>,
) {
    match ev {
        MarketEvent::Book(book) => {
            let Some(meta) = registry.get_meta(book.instrument_id) else {
                return;
            };
            let (date, hour) = utc_date_hour_from_ms(book.exchange_ts_ms);
            let key = PartitionKey::MarketStream {
                dataset: Dataset::BookUpdates,
                market: Arc::clone(&meta.market),
                kind: meta.kind,
                date,
                hour,
            };
            partition_opened
                .entry(key.clone())
                .or_insert_with(Instant::now);
            let entry = buffers
                .entry(key.clone())
                .or_insert_with(|| PartitionBuffer::new(Dataset::BookUpdates));
            if let PartitionBuffer::Book(b) = entry {
                b.push(&meta.instrument_name, &book);
            }
            if entry.len() >= max_rg {
                send_buffer_batch(key, entry, io_tx);
            }
        }
        MarketEvent::Ticker(ticker) => {
            let Some(meta) = registry.get_meta(ticker.instrument_id) else {
                return;
            };
            let (date, hour) = utc_date_hour_from_ms(ticker.exchange_ts_ms);
            let key = PartitionKey::MarketStream {
                dataset: Dataset::Tickers,
                market: Arc::clone(&meta.market),
                kind: meta.kind,
                date,
                hour,
            };
            partition_opened
                .entry(key.clone())
                .or_insert_with(Instant::now);
            let entry = buffers
                .entry(key.clone())
                .or_insert_with(|| PartitionBuffer::new(Dataset::Tickers));
            if let PartitionBuffer::Ticker(b) = entry {
                b.push(&meta.instrument_name, *ticker);
            }
            if entry.len() >= max_rg {
                send_buffer_batch(key, entry, io_tx);
            }
        }
        MarketEvent::Trade(trade) => {
            let Some(meta) = registry.get_meta(trade.instrument_id) else {
                return;
            };
            let (date, hour) = utc_date_hour_from_ms(trade.exchange_ts_ms);
            let key = PartitionKey::MarketStream {
                dataset: Dataset::Trades,
                market: Arc::clone(&meta.market),
                kind: meta.kind,
                date,
                hour,
            };
            partition_opened
                .entry(key.clone())
                .or_insert_with(Instant::now);
            let entry = buffers
                .entry(key.clone())
                .or_insert_with(|| PartitionBuffer::new(Dataset::Trades));
            if let PartitionBuffer::Trade(b) = entry {
                b.push(&meta.instrument_name, trade);
            }
            if entry.len() >= max_rg {
                send_buffer_batch(key, entry, io_tx);
            }
        }
        MarketEvent::Index(idx_row) => {
            let (date, hour) = utc_date_hour_from_ms(idx_row.exchange_ts_ms);
            let key = PartitionKey::HourlyGlobalStream {
                dataset: Dataset::Indices,
                date,
                hour,
            };
            partition_opened
                .entry(key.clone())
                .or_insert_with(Instant::now);
            let entry = buffers
                .entry(key.clone())
                .or_insert_with(|| PartitionBuffer::new(Dataset::Indices));
            if let PartitionBuffer::Index(b) = entry {
                b.push(idx_row);
            }
            if entry.len() >= max_rg {
                send_buffer_batch(key, entry, io_tx);
            }
        }
        MarketEvent::Instrument(meta) => {
            let (date, _) = utc_date_hour_from_ns(meta.recorded_ts_ns);
            let key = PartitionKey::GlobalStream {
                dataset: Dataset::Instruments,
                date,
            };
            partition_opened
                .entry(key.clone())
                .or_insert_with(Instant::now);
            let entry = buffers
                .entry(key.clone())
                .or_insert_with(|| PartitionBuffer::new(Dataset::Instruments));
            if let PartitionBuffer::Instrument(b) = entry {
                b.push(meta);
            }
            if entry.len() >= 10_000 {
                send_buffer_batch(key, entry, io_tx);
            }
        }
        MarketEvent::Capture(cap) => {
            let (date, _) = utc_date_hour_from_ns(cap.recorded_ts_ns);
            let key = PartitionKey::GlobalStream {
                dataset: Dataset::CaptureEvents,
                date,
            };
            partition_opened
                .entry(key.clone())
                .or_insert_with(Instant::now);
            let entry = buffers
                .entry(key.clone())
                .or_insert_with(|| PartitionBuffer::new(Dataset::CaptureEvents));
            if let PartitionBuffer::Capture(b) = entry {
                b.push(cap);
            }
            if entry.len() >= 1_000 {
                send_buffer_batch(key, entry, io_tx);
            }
        }
    }
}

fn flush_all_buffers(
    buffers: &mut HashMap<PartitionKey, PartitionBuffer>,
    io_tx: &std::sync::mpsc::SyncSender<IoCommand>,
) {
    for (key, buf) in buffers.iter_mut() {
        if !buf.is_empty() {
            send_buffer_batch(key.clone(), buf, io_tx);
        }
    }
}

fn send_buffer_batch(
    key: PartitionKey,
    buf: &mut PartitionBuffer,
    io_tx: &std::sync::mpsc::SyncSender<IoCommand>,
) {
    match buf.drain_batch() {
        Ok(batch) if batch.num_rows() > 0 => {
            if let Err(e) = io_tx.send(IoCommand::WriteBatch { key, batch }) {
                error!(error = %e, "failed to hand RecordBatch to parquet-io thread");
            }
        }
        Ok(_) => {}
        Err(e) => error!(error = %e, "failed building Arrow RecordBatch from SoA buffer"),
    }
}

fn run_parquet_io_thread(
    cfg: StorageConfig,
    telemetry: Arc<Telemetry>,
    rx: std::sync::mpsc::Receiver<IoCommand>,
    upload_tx: Option<mpsc::Sender<PathBuf>>,
) -> Result<()> {
    let mut open: HashMap<PartitionKey, OpenSegment> = HashMap::new();
    let mut part_seq: u64 = 0;
    let rotation_dur = Duration::from_secs(cfg.rotation_interval_secs.max(10));
    let max_bytes = cfg.max_segment_size_mib.saturating_mul(1024 * 1024);

    while let Ok(cmd) = rx.recv() {
        match cmd {
            IoCommand::WriteBatch { key, batch } => {
                if let Err(e) = write_batch_to_segment(
                    &cfg,
                    &telemetry,
                    &mut open,
                    &mut part_seq,
                    &key,
                    &batch,
                    max_bytes,
                    upload_tx.as_ref(),
                ) {
                    error!(error = %e, partition = ?key, "failed writing Parquet row group");
                }
            }
            IoCommand::RotateKey(key) => {
                if let Some(seg) = open.remove(&key)
                    && let Err(e) = finalize_segment(&cfg, &telemetry, seg, upload_tx.as_ref())
                {
                    error!(error = %e, partition = ?key, "failed finalizing rotated Parquet segment");
                }
            }
            IoCommand::RotateDue { cur_date, cur_hour } => {
                let due_keys: Vec<PartitionKey> = open
                    .iter()
                    .filter(|(k, seg)| {
                        k.is_stale_hour(&cur_date, &cur_hour)
                            || seg.opened_at.elapsed() >= rotation_dur
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in due_keys {
                    if let Some(seg) = open.remove(&key)
                        && let Err(e) = finalize_segment(&cfg, &telemetry, seg, upload_tx.as_ref())
                    {
                        error!(error = %e, partition = ?key, "failed finalizing due Parquet segment");
                    }
                }
            }
            IoCommand::Shutdown => {
                for (key, seg) in open.drain() {
                    if let Err(e) = finalize_segment(&cfg, &telemetry, seg, upload_tx.as_ref()) {
                        error!(error = %e, partition = ?key, "failed finalizing Parquet segment on shutdown");
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_batch_to_segment(
    cfg: &StorageConfig,
    telemetry: &Telemetry,
    open: &mut HashMap<PartitionKey, OpenSegment>,
    part_seq: &mut u64,
    key: &PartitionKey,
    batch: &RecordBatch,
    max_bytes: u64,
    upload_tx: Option<&mpsc::Sender<PathBuf>>,
) -> Result<()> {
    if !open.contains_key(key) {
        let dir = key.dir_path(&cfg.data_dir);
        fs::create_dir_all(&dir)?;

        let seq = *part_seq;
        *part_seq += 1;
        let ts = Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
        let base_name = format!("part-{seq:05}-{ts}");
        let tmp_path = dir.join(format!("{base_name}.parquet.tmp"));
        let final_path = dir.join(format!("{base_name}.parquet"));

        let file = File::create(&tmp_path)?;
        let buf_writer = BufWriter::with_capacity(256 * 1024, file);
        let props = writer_properties_for(key.dataset(), cfg)?;
        let writer = ArrowWriter::try_new(buf_writer, key.dataset().schema(), Some(props))?;

        open.insert(
            key.clone(),
            OpenSegment {
                tmp_path,
                final_path,
                writer,
                opened_at: Instant::now(),
                rows_written: 0,
            },
        );
    }

    let should_rotate = {
        let seg = open.get_mut(key).expect("segment inserted above");
        seg.writer.write(batch)?;
        let rows = batch.num_rows() as u64;
        seg.rows_written += rows;

        telemetry
            .parquet_rows_written
            .fetch_add(rows, Ordering::Relaxed);

        let cur_bytes = fs::metadata(&seg.tmp_path).map(|m| m.len()).unwrap_or(0);
        max_bytes > 0 && cur_bytes >= max_bytes
    };

    if should_rotate && let Some(seg) = open.remove(key) {
        finalize_segment(cfg, telemetry, seg, upload_tx)?;
    }

    Ok(())
}

fn finalize_segment(
    cfg: &StorageConfig,
    telemetry: &Telemetry,
    seg: OpenSegment,
    upload_tx: Option<&mpsc::Sender<PathBuf>>,
) -> Result<()> {
    let row_groups = seg.writer.flushed_row_groups().len() as u64 + u64::from(seg.rows_written > 0);
    let buf_writer = seg.writer.into_inner()?;
    let file = buf_writer
        .into_inner()
        .map_err(|e| crate::error::CollectorError::Io(e.into_error()))?;
    if cfg.fsync_on_rotate {
        file.sync_all()?;
    }
    drop(file);

    fs::rename(&seg.tmp_path, &seg.final_path)?;

    let size = fs::metadata(&seg.final_path).map(|m| m.len()).unwrap_or(0);
    telemetry
        .parquet_row_groups_written
        .fetch_add(row_groups, Ordering::Relaxed);
    telemetry
        .parquet_files_rotated
        .fetch_add(1, Ordering::Relaxed);
    telemetry
        .parquet_bytes_written
        .fetch_add(size, Ordering::Relaxed);

    debug!(
        path = %seg.final_path.display(),
        rows = seg.rows_written,
        row_groups,
        bytes = size,
        bytes_per_row = format!("{:.2}", size as f64 / seg.rows_written.max(1) as f64),
        "finalized Parquet segment"
    );

    if let Some(tx) = upload_tx
        && let Err(e) = tx.blocking_send(seg.final_path)
    {
        warn!(error = %e, "failed enqueueing finalized Parquet segment for S3 upload");
    }

    Ok(())
}

pub fn utc_date_hour_from_ms(ms: i64) -> (String, String) {
    let dt = DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now);
    (
        format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        format!("{:02}", dt.hour()),
    )
}

pub fn utc_date_hour_from_ns(ns: i64) -> (String, String) {
    let dt = DateTime::from_timestamp_nanos(ns);
    (
        format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        format!("{:02}", dt.hour()),
    )
}
