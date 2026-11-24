use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int8Array, RecordBatch, StringArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};

use crate::error::Result;
use crate::model::{
    BookSide, BookUpdateEvent, CaptureEvent, IndexRow, InstrumentMeta, TickerRow, TradeRow,
};
use crate::storage::schema::{
    book_updates_schema, capture_events_schema, indices_schema, instruments_schema, tickers_schema,
    trades_schema,
};

fn build_dict_array<'a>(iter: impl Iterator<Item = &'a str>) -> ArrayRef {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for s in iter {
        builder.append_value(s);
    }
    Arc::new(builder.finish())
}

fn build_opt_dict_array<'a>(iter: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for s in iter {
        match s {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

// ---------------------------------------------------------------------------
// Book updates SoA buffer (sorted by instrument_name, exchange_ts, change_id)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct BookBuffer {
    received_ts: Vec<i64>,
    exchange_ts: Vec<i64>,
    instrument_name: Vec<Arc<str>>,
    is_snapshot: Vec<bool>,
    is_checkpoint: Vec<bool>,
    change_id: Vec<u64>,
    prev_change_id: Vec<Option<u64>>,
    side: Vec<u8>,
    action: Vec<u8>,
    price: Vec<Option<f64>>,
    amount: Vec<f64>,
}

impl BookBuffer {
    pub fn len(&self) -> usize {
        self.received_ts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.received_ts.is_empty()
    }

    pub fn push(&mut self, instrument_name: &Arc<str>, ev: &BookUpdateEvent) {
        let n = ev.levels.len();
        self.received_ts.reserve(n);
        self.exchange_ts.reserve(n);
        self.instrument_name.reserve(n);
        self.is_snapshot.reserve(n);
        self.is_checkpoint.reserve(n);
        self.change_id.reserve(n);
        self.prev_change_id.reserve(n);
        self.side.reserve(n);
        self.action.reserve(n);
        self.price.reserve(n);
        self.amount.reserve(n);

        for lvl in &ev.levels {
            self.received_ts.push(ev.received_ts_ns);
            self.exchange_ts.push(ev.exchange_ts_ms);
            self.instrument_name.push(Arc::clone(instrument_name));
            self.is_snapshot.push(ev.is_snapshot);
            self.is_checkpoint.push(ev.is_checkpoint);
            self.change_id.push(ev.change_id);
            self.prev_change_id.push(ev.prev_change_id);
            self.side.push(lvl.side as u8);
            self.action.push(lvl.action as u8);
            self.price
                .push(if lvl.side == BookSide::Empty || lvl.price.is_nan() {
                    None
                } else {
                    Some(lvl.price)
                });
            self.amount.push(lvl.amount);
        }
    }

    pub fn drain_sorted_batch(&mut self) -> Result<RecordBatch> {
        let n = self.len();
        let mut order: Vec<usize> = (0..n).collect();
        // Sort by (instrument_name, exchange_ts, change_id, original_idx) for maximum
        // RLE/delta/ZSTD compression while keeping intra-message level order deterministic.
        order.sort_unstable_by(|&a, &b| {
            self.instrument_name[a]
                .cmp(&self.instrument_name[b])
                .then_with(|| self.exchange_ts[a].cmp(&self.exchange_ts[b]))
                .then_with(|| self.change_id[a].cmp(&self.change_id[b]))
                .then_with(|| a.cmp(&b))
        });

        let received_ts = Arc::new(
            TimestampNanosecondArray::from_iter_values(order.iter().map(|&i| self.received_ts[i]))
                .with_timezone("UTC"),
        ) as ArrayRef;
        let exchange_ts = Arc::new(
            TimestampMillisecondArray::from_iter_values(order.iter().map(|&i| self.exchange_ts[i]))
                .with_timezone("UTC"),
        ) as ArrayRef;
        let instrument_name =
            build_dict_array(order.iter().map(|&i| self.instrument_name[i].as_ref()));
        let is_snapshot = Arc::new(BooleanArray::from_iter(
            order.iter().map(|&i| Some(self.is_snapshot[i])),
        )) as ArrayRef;
        let is_checkpoint = Arc::new(BooleanArray::from_iter(
            order.iter().map(|&i| Some(self.is_checkpoint[i])),
        )) as ArrayRef;
        let change_id = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.change_id[i]),
        )) as ArrayRef;
        let prev_change_id = Arc::new(UInt64Array::from_iter(
            order.iter().map(|&i| self.prev_change_id[i]),
        )) as ArrayRef;
        let side = Arc::new(UInt8Array::from_iter_values(
            order.iter().map(|&i| self.side[i]),
        )) as ArrayRef;
        let action = Arc::new(UInt8Array::from_iter_values(
            order.iter().map(|&i| self.action[i]),
        )) as ArrayRef;
        let price = Arc::new(Float64Array::from_iter(
            order.iter().map(|&i| self.price[i]),
        )) as ArrayRef;
        let amount = Arc::new(Float64Array::from_iter_values(
            order.iter().map(|&i| self.amount[i]),
        )) as ArrayRef;

        self.clear();

        Ok(RecordBatch::try_new(
            book_updates_schema(),
            vec![
                received_ts,
                exchange_ts,
                instrument_name,
                is_snapshot,
                is_checkpoint,
                change_id,
                prev_change_id,
                side,
                action,
                price,
                amount,
            ],
        )?)
    }

    fn clear(&mut self) {
        self.received_ts.clear();
        self.exchange_ts.clear();
        self.instrument_name.clear();
        self.is_snapshot.clear();
        self.is_checkpoint.clear();
        self.change_id.clear();
        self.prev_change_id.clear();
        self.side.clear();
        self.action.clear();
        self.price.clear();
        self.amount.clear();
    }
}

// ---------------------------------------------------------------------------
// Tickers SoA buffer (sorted by instrument_name, exchange_ts, received_ts)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct TickerBuffer {
    rows: Vec<(Arc<str>, TickerRow)>,
}

impl TickerBuffer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn push(&mut self, instrument_name: &Arc<str>, row: TickerRow) {
        self.rows.push((Arc::clone(instrument_name), row));
    }

    pub fn drain_sorted_batch(&mut self) -> Result<RecordBatch> {
        let n = self.rows.len();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_unstable_by(|&a, &b| {
            self.rows[a]
                .0
                .cmp(&self.rows[b].0)
                .then_with(|| {
                    self.rows[a]
                        .1
                        .exchange_ts_ms
                        .cmp(&self.rows[b].1.exchange_ts_ms)
                })
                .then_with(|| {
                    self.rows[a]
                        .1
                        .received_ts_ns
                        .cmp(&self.rows[b].1.received_ts_ns)
                })
                .then_with(|| a.cmp(&b))
        });

        macro_rules! opt_f64_col {
            ($field:ident) => {
                Arc::new(Float64Array::from_iter(
                    order.iter().map(|&i| self.rows[i].1.$field),
                )) as ArrayRef
            };
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from_iter_values(
                    order.iter().map(|&i| self.rows[i].1.received_ts_ns),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    order.iter().map(|&i| self.rows[i].1.exchange_ts_ms),
                )
                .with_timezone("UTC"),
            ),
            build_dict_array(order.iter().map(|&i| self.rows[i].0.as_ref())),
            Arc::new(BooleanArray::from_iter(
                order.iter().map(|&i| Some(self.rows[i].1.is_snapshot)),
            )),
            Arc::new(UInt32Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.changed_mask),
            )),
            build_dict_array(order.iter().map(|&i| self.rows[i].1.state.as_ref())),
            opt_f64_col!(open_interest),
            opt_f64_col!(mark_price),
            opt_f64_col!(index_price),
            opt_f64_col!(last_price),
            opt_f64_col!(settlement_price),
            opt_f64_col!(estimated_delivery_price),
            opt_f64_col!(min_price),
            opt_f64_col!(max_price),
            opt_f64_col!(best_bid_price),
            opt_f64_col!(best_bid_amount),
            opt_f64_col!(best_ask_price),
            opt_f64_col!(best_ask_amount),
            opt_f64_col!(volume),
            opt_f64_col!(volume_usd),
            opt_f64_col!(volume_notional),
            opt_f64_col!(high),
            opt_f64_col!(low),
            opt_f64_col!(price_change),
            build_opt_dict_array(
                order
                    .iter()
                    .map(|&i| self.rows[i].1.underlying_index.as_deref()),
            ),
            opt_f64_col!(underlying_price),
            opt_f64_col!(mark_iv),
            opt_f64_col!(bid_iv),
            opt_f64_col!(ask_iv),
            opt_f64_col!(interest_rate),
            opt_f64_col!(delta),
            opt_f64_col!(gamma),
            opt_f64_col!(vega),
            opt_f64_col!(theta),
            opt_f64_col!(rho),
            opt_f64_col!(current_funding),
            opt_f64_col!(funding_8h),
            opt_f64_col!(interest_value),
        ];

        self.rows.clear();

        Ok(RecordBatch::try_new(tickers_schema(), cols)?)
    }
}

// ---------------------------------------------------------------------------
// Trades buffer
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct TradeBuffer {
    rows: Vec<(Arc<str>, TradeRow)>,
}

impl TradeBuffer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn push(&mut self, instrument_name: &Arc<str>, row: TradeRow) {
        self.rows.push((Arc::clone(instrument_name), row));
    }

    pub fn drain_sorted_batch(&mut self) -> Result<RecordBatch> {
        let n = self.rows.len();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_unstable_by(|&a, &b| {
            self.rows[a]
                .0
                .cmp(&self.rows[b].0)
                .then_with(|| {
                    self.rows[a]
                        .1
                        .exchange_ts_ms
                        .cmp(&self.rows[b].1.exchange_ts_ms)
                })
                .then_with(|| self.rows[a].1.trade_seq.cmp(&self.rows[b].1.trade_seq))
                .then_with(|| a.cmp(&b))
        });

        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from_iter_values(
                    order.iter().map(|&i| self.rows[i].1.received_ts_ns),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    order.iter().map(|&i| self.rows[i].1.exchange_ts_ms),
                )
                .with_timezone("UTC"),
            ),
            build_dict_array(order.iter().map(|&i| self.rows[i].0.as_ref())),
            Arc::new(StringArray::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.trade_id.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.trade_seq),
            )),
            Arc::new(Float64Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.price),
            )),
            Arc::new(Float64Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.amount),
            )),
            Arc::new(Float64Array::from_iter(
                order.iter().map(|&i| self.rows[i].1.contracts),
            )),
            Arc::new(UInt8Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.direction as u8),
            )),
            Arc::new(Int8Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.tick_direction),
            )),
            Arc::new(Float64Array::from_iter(
                order.iter().map(|&i| self.rows[i].1.iv),
            )),
            Arc::new(Float64Array::from_iter(
                order.iter().map(|&i| self.rows[i].1.index_price),
            )),
            Arc::new(Float64Array::from_iter(
                order.iter().map(|&i| self.rows[i].1.mark_price),
            )),
            Arc::new(UInt8Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].1.liquidation as u8),
            )),
            Arc::new(StringArray::from_iter(
                order
                    .iter()
                    .map(|&i| self.rows[i].1.block_trade_id.as_deref()),
            )),
            Arc::new(UInt16Array::from_iter(
                order.iter().map(|&i| self.rows[i].1.block_trade_leg_count),
            )),
            Arc::new(StringArray::from_iter(
                order.iter().map(|&i| self.rows[i].1.combo_id.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                order
                    .iter()
                    .map(|&i| self.rows[i].1.combo_trade_id.as_deref()),
            )),
        ];

        self.rows.clear();

        Ok(RecordBatch::try_new(trades_schema(), cols)?)
    }
}

// ---------------------------------------------------------------------------
// Spot Price Index & Volatility Index (DVOL) buffer
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct IndexBuffer {
    rows: Vec<IndexRow>,
}

impl IndexBuffer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn push(&mut self, row: IndexRow) {
        self.rows.push(row);
    }

    pub fn drain_sorted_batch(&mut self) -> Result<RecordBatch> {
        let n = self.rows.len();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_unstable_by(|&a, &b| {
            self.rows[a]
                .index_name
                .cmp(&self.rows[b].index_name)
                .then_with(|| (self.rows[a].kind as u8).cmp(&(self.rows[b].kind as u8)))
                .then_with(|| {
                    self.rows[a]
                        .exchange_ts_ms
                        .cmp(&self.rows[b].exchange_ts_ms)
                })
                .then_with(|| a.cmp(&b))
        });

        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from_iter_values(
                    order.iter().map(|&i| self.rows[i].received_ts_ns),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    order.iter().map(|&i| self.rows[i].exchange_ts_ms),
                )
                .with_timezone("UTC"),
            ),
            build_dict_array(order.iter().map(|&i| self.rows[i].index_name.as_ref())),
            Arc::new(UInt8Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].kind as u8),
            )),
            Arc::new(Float64Array::from_iter_values(
                order.iter().map(|&i| self.rows[i].value),
            )),
        ];

        self.rows.clear();

        Ok(RecordBatch::try_new(indices_schema(), cols)?)
    }
}

// ---------------------------------------------------------------------------
// Instruments catalog buffer
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InstrumentBuffer {
    rows: Vec<InstrumentMeta>,
}

impl InstrumentBuffer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn push(&mut self, meta: InstrumentMeta) {
        self.rows.push(meta);
    }

    pub fn drain_batch(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from_iter_values(
                    self.rows.iter().map(|r| r.recorded_ts_ns),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from_iter_values(
                self.rows.iter().map(|r| r.instrument_name.as_ref()),
            )),
            Arc::new(UInt64Array::from_iter(
                self.rows.iter().map(|r| r.deribit_instrument_id),
            )),
            build_dict_array(self.rows.iter().map(|r| r.market.as_ref())),
            build_dict_array(self.rows.iter().map(|r| r.base_currency.as_ref())),
            build_dict_array(self.rows.iter().map(|r| r.quote_currency.as_ref())),
            build_dict_array(self.rows.iter().map(|r| r.settlement_currency.as_ref())),
            build_dict_array(self.rows.iter().map(|r| r.settlement_period.as_ref())),
            Arc::new(UInt8Array::from_iter_values(
                self.rows.iter().map(|r| r.kind as u8),
            )),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    self.rows.iter().map(|r| r.creation_ts_ms),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    self.rows.iter().map(|r| r.expiration_ts_ms),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from_iter(self.rows.iter().map(|r| r.strike))),
            build_opt_dict_array(self.rows.iter().map(|r| r.option_type.map(|o| o.as_str()))),
            Arc::new(Float64Array::from_iter_values(
                self.rows.iter().map(|r| r.tick_size),
            )),
            Arc::new(Float64Array::from_iter_values(
                self.rows.iter().map(|r| r.min_trade_amount),
            )),
            Arc::new(Float64Array::from_iter_values(
                self.rows.iter().map(|r| r.contract_size),
            )),
            Arc::new(Float64Array::from_iter_values(
                self.rows.iter().map(|r| r.maker_commission),
            )),
            Arc::new(Float64Array::from_iter_values(
                self.rows.iter().map(|r| r.taker_commission),
            )),
            Arc::new(BooleanArray::from_iter(
                self.rows.iter().map(|r| Some(r.is_active)),
            )),
            build_dict_array(self.rows.iter().map(|r| r.state.as_ref())),
        ];

        self.rows.clear();
        Ok(RecordBatch::try_new(instruments_schema(), cols)?)
    }
}

// ---------------------------------------------------------------------------
// Capture events audit journal buffer
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct CaptureEventBuffer {
    rows: Vec<CaptureEvent>,
}

impl CaptureEventBuffer {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn push(&mut self, ev: CaptureEvent) {
        self.rows.push(ev);
    }

    pub fn drain_batch(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from_iter_values(
                    self.rows.iter().map(|r| r.recorded_ts_ns),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt16Array::from_iter_values(
                self.rows.iter().map(|r| r.shard_id),
            )),
            build_dict_array(self.rows.iter().map(|r| r.event_type.as_ref())),
            Arc::new(StringArray::from_iter(
                self.rows.iter().map(|r| r.instrument_name.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(
                self.rows.iter().map(|r| r.detail.as_str()),
            )),
        ];

        self.rows.clear();
        Ok(RecordBatch::try_new(capture_events_schema(), cols)?)
    }
}
