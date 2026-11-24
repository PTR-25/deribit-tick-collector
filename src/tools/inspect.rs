use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, TimestampMillisecondType, TimestampNanosecondType};
use arrow_array::{Array, ArrayAccessor, BooleanArray, Float64Array, UInt32Array, UInt64Array};
use chrono::DateTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::Result;
use crate::model::ticker_mask;

#[derive(Debug, Default)]
pub struct PartitionSummary {
    pub files: usize,
    pub row_groups: usize,
    pub rows: u64,
    pub bytes: u64,
    pub min_ts_ms: Option<i64>,
    pub max_ts_ms: Option<i64>,
    pub instruments: BTreeSet<String>,
    // Book-specific integrity stats
    pub book_snapshots: u64,
    pub book_checkpoints: u64,
    pub book_messages: u64,
    pub book_gaps: u64,
    // Ticker-specific OI stats
    pub ticker_oi_changes: u64,
    pub ticker_non_null_oi: u64,
    pub ticker_non_null_iv: u64,
    pub ticker_non_null_funding: u64,
}

#[derive(Debug, Default)]
pub struct InspectReport {
    pub partitions: BTreeMap<String, PartitionSummary>,
    pub total_files: usize,
    pub total_rows: u64,
    pub total_bytes: u64,
}

type BookChainKey = (i64, u64, bool);
type BookChainMap = BTreeMap<(String, String), BTreeMap<BookChainKey, (bool, Option<u64>)>>;

pub fn inspect_path(target: &Path) -> Result<InspectReport> {
    let mut files = Vec::new();
    if target.is_file() {
        files.push(target.to_path_buf());
    } else {
        collect_parquet_files(target, &mut files);
    }
    files.sort();

    let mut report = InspectReport::default();
    // Track per-instrument book packets (change_id -> (is_snapshot, prev_change_id)) across files
    let mut book_chains: BookChainMap = BTreeMap::new();

    for path in files {
        let size = fs::metadata(&path)?.len();
        let label = partition_label(&path);
        let is_book = label.starts_with("book_updates");
        let is_ticker = label.starts_with("tickers");

        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let meta = builder.metadata().clone();
        let rg_count = meta.num_row_groups();
        let file_rows = meta.file_metadata().num_rows() as u64;

        let entry = report.partitions.entry(label.clone()).or_default();
        entry.files += 1;
        entry.row_groups += rg_count;
        entry.rows += file_rows;
        entry.bytes += size;

        report.total_files += 1;
        report.total_rows += file_rows;
        report.total_bytes += size;

        let reader = builder.with_batch_size(65_536).build()?;
        for batch in reader {
            let batch = batch?;
            let schema = batch.schema();

            // Timestamp range (prefer exchange_ts, fallback to recorded_ts)
            if let Ok(idx) = schema.index_of("exchange_ts") {
                let ts_col = batch.column(idx).as_primitive::<TimestampMillisecondType>();
                for v in ts_col.values() {
                    entry.min_ts_ms = Some(entry.min_ts_ms.map_or(*v, |m| m.min(*v)));
                    entry.max_ts_ms = Some(entry.max_ts_ms.map_or(*v, |m| m.max(*v)));
                }
            } else if let Ok(idx) = schema.index_of("recorded_ts") {
                let ts_col = batch.column(idx).as_primitive::<TimestampNanosecondType>();
                for v in ts_col.values() {
                    let ms = *v / 1_000_000;
                    entry.min_ts_ms = Some(entry.min_ts_ms.map_or(ms, |m| m.min(ms)));
                    entry.max_ts_ms = Some(entry.max_ts_ms.map_or(ms, |m| m.max(ms)));
                }
            }

            // Collect instrument names (or index_name for dataset=indices)
            let inst_strings = extract_string_column(&batch, "instrument_name")
                .or_else(|| extract_string_column(&batch, "index_name"));
            if let Some(names) = &inst_strings {
                for n in names.iter().flatten() {
                    entry.instruments.insert(n.clone());
                }
            }

            if is_book
                && let Some(names) = &inst_strings
                && let (Ok(ts_idx), Ok(snap_idx), Ok(cid_idx), Ok(prev_idx)) = (
                    schema.index_of("exchange_ts"),
                    schema.index_of("is_snapshot"),
                    schema.index_of("change_id"),
                    schema.index_of("prev_change_id"),
                )
            {
                let ts_col = batch
                    .column(ts_idx)
                    .as_primitive::<TimestampMillisecondType>();
                let snaps = batch
                    .column(snap_idx)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("is_snapshot is Boolean");
                let ckpts = schema
                    .index_of("is_checkpoint")
                    .ok()
                    .and_then(|idx| batch.column(idx).as_any().downcast_ref::<BooleanArray>());
                let cids = batch
                    .column(cid_idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("change_id is UInt64");
                let prevs = batch
                    .column(prev_idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("prev_change_id is UInt64");

                for (i, inst_opt) in names.iter().enumerate().take(batch.num_rows()) {
                    if let Some(inst) = inst_opt {
                        let ts = ts_col.value(i);
                        let cid = cids.value(i);
                        let snap = snaps.value(i);
                        let ckpt = ckpts.is_some_and(|c| c.value(i));
                        let prev = if prevs.is_null(i) {
                            None
                        } else {
                            Some(prevs.value(i))
                        };
                        book_chains
                            .entry((label.clone(), inst.clone()))
                            .or_default()
                            .entry((ts, cid, ckpt))
                            .or_insert((snap, prev));
                    }
                }
            }

            if is_ticker {
                if let Ok(mask_idx) = schema.index_of("changed_mask") {
                    let masks = batch
                        .column(mask_idx)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .expect("changed_mask is UInt32");
                    for i in 0..batch.num_rows() {
                        if (masks.value(i) & ticker_mask::OPEN_INTEREST) != 0 {
                            entry.ticker_oi_changes += 1;
                        }
                    }
                }
                if let Ok(oi_idx) = schema.index_of("open_interest") {
                    let col = batch
                        .column(oi_idx)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("open_interest is Float64");
                    entry.ticker_non_null_oi += (col.len() - col.null_count()) as u64;
                }
                if let Ok(iv_idx) = schema.index_of("mark_iv") {
                    let col = batch
                        .column(iv_idx)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("mark_iv is Float64");
                    entry.ticker_non_null_iv += (col.len() - col.null_count()) as u64;
                }
                if let Ok(f_idx) = schema.index_of("funding_8h") {
                    let col = batch
                        .column(f_idx)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("funding_8h is Float64");
                    entry.ticker_non_null_funding += (col.len() - col.null_count()) as u64;
                }
            }
        }
    }

    // Audit change_id continuity across all book instruments (handles both single-hour and multi-hour spans).
    for ((label, _inst), chain) in book_chains {
        if let Some(entry) = report.partitions.get_mut(&label) {
            let mut last_cid: Option<u64> = None;
            for ((_ts, cid, is_ckpt), (is_snap, prev)) in chain {
                entry.book_messages += 1;
                if is_ckpt {
                    entry.book_checkpoints += 1;
                    // Trigger B quiet-instrument hourly anchor repeats last_cid -> no gap.
                    if last_cid == Some(cid) {
                        continue;
                    }
                    // Trigger A promoted checkpoint carries prev_change_id -> verify continuity across hours.
                    if let (Some(expected), Some(got_prev)) = (last_cid, prev)
                        && got_prev != expected
                    {
                        entry.book_gaps += 1;
                    }
                    last_cid = Some(cid);
                } else if is_snap {
                    entry.book_snapshots += 1;
                    last_cid = Some(cid);
                } else {
                    if let Some(expected) = last_cid
                        && prev != Some(expected)
                    {
                        entry.book_gaps += 1;
                    }
                    last_cid = Some(cid);
                }
            }
        }
    }

    Ok(report)
}

pub fn print_report(report: &InspectReport) {
    println!("=== Parquet Dataset Inspection ===");
    println!(
        "Total files: {} | Total rows: {} | Total size: {:.2} MiB ({:.2} B/row)",
        report.total_files,
        report.total_rows,
        report.total_bytes as f64 / (1024.0 * 1024.0),
        if report.total_rows > 0 {
            report.total_bytes as f64 / report.total_rows as f64
        } else {
            0.0
        }
    );
    println!();

    for (label, s) in &report.partitions {
        let bpr = if s.rows > 0 {
            s.bytes as f64 / s.rows as f64
        } else {
            0.0
        };
        let span = match (s.min_ts_ms, s.max_ts_ms) {
            (Some(a), Some(b)) => format!(
                "{} -> {}",
                DateTime::from_timestamp_millis(a)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| a.to_string()),
                DateTime::from_timestamp_millis(b)
                    .map(|d| d.format("%H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| b.to_string()),
            ),
            _ => "n/a".to_string(),
        };

        println!("[{label}]");
        println!(
            "  files={} row_groups={} rows={} size={:.2} KiB ({:.2} B/row) instruments={}",
            s.files,
            s.row_groups,
            s.rows,
            s.bytes as f64 / 1024.0,
            bpr,
            s.instruments.len()
        );
        println!("  span: {span}");
        if s.book_messages > 0 {
            println!(
                "  book: messages={} snapshots={} checkpoints={} sequence_gaps={}",
                s.book_messages, s.book_snapshots, s.book_checkpoints, s.book_gaps
            );
        }
        if s.ticker_non_null_oi > 0 || s.ticker_non_null_iv > 0 || s.ticker_non_null_funding > 0 {
            println!(
                "  ticker: non_null_oi={} oi_change_ticks={} non_null_iv={} non_null_funding={}",
                s.ticker_non_null_oi,
                s.ticker_oi_changes,
                s.ticker_non_null_iv,
                s.ticker_non_null_funding
            );
        }
    }
}

pub(crate) fn extract_string_column(
    batch: &arrow_array::RecordBatch,
    col_name: &str,
) -> Option<Vec<Option<String>>> {
    let idx = batch.schema().index_of(col_name).ok()?;
    let col = batch.column(idx);
    if let Some(dict) = col.as_dictionary_opt::<Int32Type>() {
        let vals = dict.downcast_dict::<arrow_array::StringArray>()?;
        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if dict.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(vals.value(i).to_owned()));
            }
        }
        Some(out)
    } else if let Some(arr) = col.as_string_opt::<i32>() {
        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(arr.value(i).to_owned()));
            }
        }
        Some(out)
    } else {
        None
    }
}

pub(crate) fn collect_parquet_files(root: &Path, out: &mut Vec<PathBuf>) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
}

fn partition_label(path: &Path) -> String {
    let mut ds = None;
    let mut curr = None;
    let mut kind = None;
    for comp in path.components() {
        let s = comp.as_os_str().to_string_lossy();
        if let Some(v) = s.strip_prefix("dataset=") {
            ds = Some(v.to_string());
        } else if let Some(v) = s.strip_prefix("currency=") {
            curr = Some(v.to_string());
        } else if let Some(v) = s.strip_prefix("kind=") {
            kind = Some(v.to_string());
        }
    }
    match (ds, curr, kind) {
        (Some(d), Some(c), Some(k)) => format!("{d}/{c}/{k}"),
        (Some(d), _, _) => d,
        _ => path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}
