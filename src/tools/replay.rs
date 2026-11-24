use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::{Array, BooleanArray, Float64Array, UInt8Array, UInt32Array, UInt64Array};
use chrono::DateTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::Result;
use crate::model::{BookAction, BookSide, OrderedPrice, ticker_mask};
use crate::tools::inspect::{collect_parquet_files, extract_string_column};

#[derive(Debug)]
struct Packet {
    exchange_ts_ms: i64,
    is_snapshot: bool,
    is_checkpoint: bool,
    prev_change_id: Option<u64>,
    levels: Vec<(BookSide, BookAction, Option<f64>, f64)>,
}

#[derive(Debug)]
pub struct ReplayResult {
    pub instrument: String,
    pub total_packets: usize,
    pub snapshots: usize,
    pub checkpoints: usize,
    pub sequence_gaps: usize,
    pub crossed_states: usize,
    pub final_bid_levels: usize,
    pub final_ask_levels: usize,
    pub final_best_bid: Option<(f64, f64)>,
    pub final_best_ask: Option<(f64, f64)>,
    pub ticker_rows: usize,
    pub oi_changes: usize,
    pub latest_open_interest: Option<f64>,
    pub latest_mark_iv: Option<f64>,
    pub latest_funding_8h: Option<f64>,
}

/// Reconstruct an instrument's exact L2 order book tick-by-tick from recorded Parquet files
/// (single hour partition or multi-hour span) and audit sequence continuity, book invariants,
/// and ticker/OI state.
pub fn replay_instrument(target: &Path, instrument: &str, samples: usize) -> Result<ReplayResult> {
    let mut files = Vec::new();
    if target.is_file() {
        files.push(target.to_path_buf());
    } else {
        collect_parquet_files(target, &mut files);
    }
    files.sort();

    // Key by (exchange_ts_ms, change_id, is_checkpoint) so hourly quiet-instrument checkpoints
    // never collide with earlier deltas sharing the same change_id in a multi-hour replay.
    let mut packets: BTreeMap<(i64, u64, bool), Packet> = BTreeMap::new();
    let mut ticker_rows = 0usize;
    let mut oi_changes = 0usize;
    let mut latest_oi = None;
    let mut latest_mark_iv = None;
    let mut latest_funding_8h = None;
    let mut latest_ticker_ts = i64::MIN;

    for path in files {
        let f = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(f)?;
        let schema = builder.schema().clone();

        let is_book = schema.index_of("change_id").is_ok() && schema.index_of("side").is_ok();
        let is_ticker =
            schema.index_of("changed_mask").is_ok() && schema.index_of("open_interest").is_ok();

        if !is_book && !is_ticker {
            continue;
        }

        let reader = builder.with_batch_size(65_536).build()?;
        for batch in reader {
            let batch = batch?;
            let Some(names) = extract_string_column(&batch, "instrument_name") else {
                continue;
            };

            if is_book {
                let ts_col = batch
                    .column(batch.schema().index_of("exchange_ts")?)
                    .as_primitive::<TimestampMillisecondType>();
                let snap_col = batch
                    .column(batch.schema().index_of("is_snapshot")?)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("is_snapshot");
                let ckpt_col = batch
                    .schema()
                    .index_of("is_checkpoint")
                    .ok()
                    .and_then(|idx| batch.column(idx).as_any().downcast_ref::<BooleanArray>());
                let cid_col = batch
                    .column(batch.schema().index_of("change_id")?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("change_id");
                let prev_col = batch
                    .column(batch.schema().index_of("prev_change_id")?)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("prev_change_id");
                let side_col = batch
                    .column(batch.schema().index_of("side")?)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("side");
                let act_col = batch
                    .column(batch.schema().index_of("action")?)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("action");
                let px_col = batch
                    .column(batch.schema().index_of("price")?)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("price");
                let amt_col = batch
                    .column(batch.schema().index_of("amount")?)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("amount");

                for (i, inst_opt) in names.iter().enumerate().take(batch.num_rows()) {
                    if inst_opt.as_deref() != Some(instrument) {
                        continue;
                    }
                    let ts = ts_col.value(i);
                    let cid = cid_col.value(i);
                    let is_checkpoint = ckpt_col.is_some_and(|c| c.value(i));
                    let pkt = packets
                        .entry((ts, cid, is_checkpoint))
                        .or_insert_with(|| Packet {
                            exchange_ts_ms: ts,
                            is_snapshot: snap_col.value(i),
                            is_checkpoint,
                            prev_change_id: if prev_col.is_null(i) {
                                None
                            } else {
                                Some(prev_col.value(i))
                            },
                            levels: Vec::new(),
                        });
                    let side = BookSide::from_u8(side_col.value(i)).unwrap_or(BookSide::Empty);
                    let act = BookAction::from_u8(act_col.value(i)).unwrap_or(BookAction::Empty);
                    let px = if px_col.is_null(i) {
                        None
                    } else {
                        Some(px_col.value(i))
                    };
                    pkt.levels.push((side, act, px, amt_col.value(i)));
                }
            } else if is_ticker {
                let ts_col = batch
                    .column(batch.schema().index_of("exchange_ts")?)
                    .as_primitive::<TimestampMillisecondType>();
                let mask_col = batch
                    .column(batch.schema().index_of("changed_mask")?)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("changed_mask");
                let oi_col = batch
                    .column(batch.schema().index_of("open_interest")?)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("open_interest");
                let iv_col = batch
                    .column(batch.schema().index_of("mark_iv")?)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("mark_iv");
                let fund_col = batch
                    .column(batch.schema().index_of("funding_8h")?)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("funding_8h");

                for (i, inst_opt) in names.iter().enumerate().take(batch.num_rows()) {
                    if inst_opt.as_deref() != Some(instrument) {
                        continue;
                    }
                    ticker_rows += 1;
                    if (mask_col.value(i) & ticker_mask::OPEN_INTEREST) != 0 {
                        oi_changes += 1;
                    }
                    let ts = ts_col.value(i);
                    if ts >= latest_ticker_ts {
                        latest_ticker_ts = ts;
                        if !oi_col.is_null(i) {
                            latest_oi = Some(oi_col.value(i));
                        }
                        if !iv_col.is_null(i) {
                            latest_mark_iv = Some(iv_col.value(i));
                        }
                        if !fund_col.is_null(i) {
                            latest_funding_8h = Some(fund_col.value(i));
                        }
                    }
                }
            }
        }
    }

    let mut bids: BTreeMap<OrderedPrice, f64> = BTreeMap::new();
    let mut asks: BTreeMap<OrderedPrice, f64> = BTreeMap::new();
    let mut last_cid: Option<u64> = None;
    let mut snapshots = 0usize;
    let mut checkpoints = 0usize;
    let mut sequence_gaps = 0usize;
    let mut crossed_states = 0usize;

    let total_packets = packets.len();
    let stride = if samples > 0 && total_packets > samples {
        total_packets / samples
    } else {
        1
    };

    for (idx, ((_ts, cid, _ckpt), pkt)) in packets.iter().enumerate() {
        if pkt.is_checkpoint {
            checkpoints += 1;
            if last_cid == Some(*cid) {
                // Redundant quiet-instrument hourly anchor already satisfied by prior hour state.
                continue;
            }
            if let (Some(prev), Some(got_prev)) = (last_cid, pkt.prev_change_id)
                && got_prev != prev
            {
                sequence_gaps += 1;
            }
            bids.clear();
            asks.clear();
        } else if pkt.is_snapshot {
            snapshots += 1;
            bids.clear();
            asks.clear();
        } else if let Some(prev) = last_cid
            && pkt.prev_change_id != Some(prev)
        {
            sequence_gaps += 1;
        }
        last_cid = Some(*cid);

        for &(side, action, price_opt, amount) in &pkt.levels {
            let Some(price) = price_opt else {
                continue;
            };
            let key = OrderedPrice(price);
            let book = match side {
                BookSide::Bid => &mut bids,
                BookSide::Ask => &mut asks,
                BookSide::Empty => continue,
            };
            match action {
                BookAction::New | BookAction::Change => {
                    if amount > 0.0 {
                        book.insert(key, amount);
                    } else {
                        book.remove(&key);
                    }
                }
                BookAction::Delete => {
                    book.remove(&key);
                }
                BookAction::Empty => {}
            }
        }

        let best_bid = bids.iter().next_back().map(|(k, &v)| (k.0, v));
        let best_ask = asks.iter().next().map(|(k, &v)| (k.0, v));
        if let (Some((b, _)), Some((a, _))) = (best_bid, best_ask)
            && b >= a
        {
            crossed_states += 1;
        }

        if samples > 0 && (idx % stride == 0 || idx + 1 == total_packets) {
            let ts_str = DateTime::from_timestamp_millis(pkt.exchange_ts_ms)
                .map(|d| d.format("%H:%M:%S%.3f").to_string())
                .unwrap_or_else(|| pkt.exchange_ts_ms.to_string());
            println!(
                "  [{ts_str}] cid={cid} snap={} ckpt={} bid={:?} ask={:?} depth=({}/{})",
                pkt.is_snapshot,
                pkt.is_checkpoint,
                best_bid,
                best_ask,
                bids.len(),
                asks.len()
            );
        }
    }

    Ok(ReplayResult {
        instrument: instrument.to_string(),
        total_packets,
        snapshots,
        checkpoints,
        sequence_gaps,
        crossed_states,
        final_bid_levels: bids.len(),
        final_ask_levels: asks.len(),
        final_best_bid: bids.iter().next_back().map(|(k, &v)| (k.0, v)),
        final_best_ask: asks.iter().next().map(|(k, &v)| (k.0, v)),
        ticker_rows,
        oi_changes,
        latest_open_interest: latest_oi,
        latest_mark_iv,
        latest_funding_8h,
    })
}
