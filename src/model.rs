use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use smallvec::SmallVec;

/// Compact interned identifier for an instrument in `InstrumentRegistry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstrumentId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum InstrumentKind {
    Option = 0,
    Perpetual = 1,
    Future = 2,
    OptionCombo = 3,
    FutureCombo = 4,
}

impl InstrumentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Option => "option",
            Self::Perpetual => "perpetual",
            Self::Future => "future",
            Self::OptionCombo => "option_combo",
            Self::FutureCombo => "future_combo",
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Option),
            1 => Some(Self::Perpetual),
            2 => Some(Self::Future),
            3 => Some(Self::OptionCombo),
            4 => Some(Self::FutureCombo),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum IndexKind {
    Price = 0,
    Volatility = 1,
}

impl IndexKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Price => "price",
            Self::Volatility => "volatility",
        }
    }
}

/// Total-ordered floating-point wrapper safe for `BTreeMap` price keys (including negative spread prices).
#[derive(Debug, Clone, Copy)]
pub struct OrderedPrice(pub f64);

impl PartialEq for OrderedPrice {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}
impl Eq for OrderedPrice {}
impl PartialOrd for OrderedPrice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedPrice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// In-memory L2 order book tracked per instrument inside each shard to validate `change_id`
/// continuity and emit self-contained UTC hourly anchor checkpoints.
#[derive(Debug, Clone)]
pub struct LiveBook {
    pub last_change_id: u64,
    pub last_exchange_ts_ms: i64,
    /// UTC hour index (`exchange_ts_ms.div_euclid(3_600_000)`) for which this book has already
    /// written an anchor snapshot (`is_snapshot = true`).
    pub last_anchor_hour: i64,
    pub bids: BTreeMap<OrderedPrice, f64>,
    pub asks: BTreeMap<OrderedPrice, f64>,
}

impl LiveBook {
    pub fn from_snapshot(change_id: u64, exchange_ts_ms: i64, levels: &[BookLevelDelta]) -> Self {
        let mut book = Self {
            last_change_id: change_id,
            last_exchange_ts_ms: exchange_ts_ms,
            last_anchor_hour: utc_hour_index(exchange_ts_ms),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        book.apply_levels(levels);
        book
    }

    pub fn reset_snapshot(
        &mut self,
        change_id: u64,
        exchange_ts_ms: i64,
        levels: &[BookLevelDelta],
    ) {
        self.bids.clear();
        self.asks.clear();
        self.last_change_id = change_id;
        self.last_exchange_ts_ms = exchange_ts_ms;
        self.last_anchor_hour = utc_hour_index(exchange_ts_ms);
        self.apply_levels(levels);
    }

    pub fn apply_delta(&mut self, change_id: u64, exchange_ts_ms: i64, levels: &[BookLevelDelta]) {
        self.last_change_id = change_id;
        self.last_exchange_ts_ms = exchange_ts_ms;
        self.apply_levels(levels);
    }

    fn apply_levels(&mut self, levels: &[BookLevelDelta]) {
        for lvl in levels {
            if lvl.side == BookSide::Empty || lvl.price.is_nan() {
                continue;
            }
            let map = match lvl.side {
                BookSide::Bid => &mut self.bids,
                BookSide::Ask => &mut self.asks,
                BookSide::Empty => continue,
            };
            let key = OrderedPrice(lvl.price);
            match lvl.action {
                BookAction::New | BookAction::Change => {
                    if lvl.amount > 0.0 {
                        map.insert(key, lvl.amount);
                    } else {
                        map.remove(&key);
                    }
                }
                BookAction::Delete => {
                    map.remove(&key);
                }
                BookAction::Empty => {}
            }
        }
    }

    /// Materialize the full current book as snapshot level rows (bids descending, asks ascending).
    pub fn snapshot_levels(&self) -> SmallVec<[BookLevelDelta; 8]> {
        let total = self.bids.len() + self.asks.len();
        let mut out = SmallVec::with_capacity(total.max(1));
        for (px, &amt) in self.bids.iter().rev() {
            out.push(BookLevelDelta {
                side: BookSide::Bid,
                action: BookAction::New,
                price: px.0,
                amount: amt,
            });
        }
        for (px, &amt) in &self.asks {
            out.push(BookLevelDelta {
                side: BookSide::Ask,
                action: BookAction::New,
                price: px.0,
                amount: amt,
            });
        }
        if out.is_empty() {
            out.push(BookLevelDelta {
                side: BookSide::Empty,
                action: BookAction::Empty,
                price: f64::NAN,
                amount: 0.0,
            });
        }
        out
    }
}

#[inline]
pub fn utc_hour_index(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(3_600_000)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OptionType {
    Call = 0,
    Put = 1,
}

impl OptionType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Call => "call",
            Self::Put => "put",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum BookSide {
    Bid = 0,
    Ask = 1,
    /// Sentinel row emitted when a Deribit book message carries zero bid/ask levels,
    /// preserving lossless `change_id -> prev_change_id` continuity on disk.
    Empty = 2,
}

impl BookSide {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Bid),
            1 => Some(Self::Ask),
            2 => Some(Self::Empty),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum BookAction {
    New = 0,
    Change = 1,
    Delete = 2,
    Empty = 3,
}

impl BookAction {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::New),
            1 => Some(Self::Change),
            2 => Some(Self::Delete),
            3 => Some(Self::Empty),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TradeDirection {
    Buy = 0,
    Sell = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LiquidationSide {
    None = 0,
    Maker = 1,
    Taker = 2,
    Both = 3,
}

/// Bitmask constants describing which ticker fields changed on a given tick.
pub mod ticker_mask {
    pub const OPEN_INTEREST: u32 = 1 << 0;
    pub const MARK_PRICE: u32 = 1 << 1;
    pub const INDEX_PRICE: u32 = 1 << 2;
    pub const UNDERLYING_PRICE: u32 = 1 << 3;
    pub const IV: u32 = 1 << 4;
    pub const GREEKS: u32 = 1 << 5;
    pub const BBO: u32 = 1 << 6;
    pub const LAST_PRICE: u32 = 1 << 7;
    pub const FUNDING: u32 = 1 << 8;
    pub const VOLUME: u32 = 1 << 9;
    pub const SETTLEMENT: u32 = 1 << 10;
    pub const STATE: u32 = 1 << 11;

    /// Everything except pure BBO-only ticks (already recorded at full L2 depth in `book_updates`).
    pub const INTERESTING: u32 = OPEN_INTEREST
        | MARK_PRICE
        | INDEX_PRICE
        | UNDERLYING_PRICE
        | IV
        | GREEKS
        | LAST_PRICE
        | FUNDING
        | VOLUME
        | SETTLEMENT
        | STATE;
}

/// Canonical instrument metadata stored in the registry and persisted to the `instruments` dataset.
#[derive(Debug, Clone)]
pub struct InstrumentMeta {
    pub id: InstrumentId,
    pub deribit_instrument_id: Option<u64>,
    pub instrument_name: Arc<str>,
    /// Canonical market family used for Hive partitioning (`currency=<market>`):
    /// inverse markets use base currency (`BTC`, `ETH`); linear markets use `<BASE>_<SETTLEMENT>`
    /// (`BTC_USDC`, `SOL_USDC`, `XRP_USDC`, etc.) so inverse and linear never collide.
    pub market: Arc<str>,
    pub base_currency: Arc<str>,
    pub quote_currency: Arc<str>,
    pub settlement_currency: Arc<str>,
    pub settlement_period: Arc<str>,
    pub kind: InstrumentKind,
    pub creation_ts_ms: i64,
    pub expiration_ts_ms: i64,
    pub strike: Option<f64>,
    pub option_type: Option<OptionType>,
    pub tick_size: f64,
    pub min_trade_amount: f64,
    pub contract_size: f64,
    pub maker_commission: f64,
    pub taker_commission: f64,
    pub is_active: bool,
    pub state: Arc<str>,
    pub recorded_ts_ns: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct BookLevelDelta {
    pub side: BookSide,
    pub action: BookAction,
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct BookUpdateEvent {
    pub received_ts_ns: i64,
    pub exchange_ts_ms: i64,
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    /// True when this full L2 snapshot was synthesized by the collector at a UTC hour boundary
    /// so the target hour partition is self-contained and independently replayable.
    pub is_checkpoint: bool,
    pub change_id: u64,
    pub prev_change_id: Option<u64>,
    pub levels: SmallVec<[BookLevelDelta; 8]>,
}

/// Fully materialized ticker row (even when sourced from bandwidth-saving `incremental_ticker`).
#[derive(Debug, Clone)]
pub struct TickerRow {
    pub received_ts_ns: i64,
    pub exchange_ts_ms: i64,
    pub instrument_id: InstrumentId,
    pub is_snapshot: bool,
    pub changed_mask: u32,
    pub state: Arc<str>,

    // Core valuation & open interest (all instruments)
    pub open_interest: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub last_price: Option<f64>,
    pub settlement_price: Option<f64>,
    pub estimated_delivery_price: Option<f64>,
    pub min_price: Option<f64>,
    pub max_price: Option<f64>,

    // Top of book
    pub best_bid_price: Option<f64>,
    pub best_bid_amount: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub best_ask_amount: Option<f64>,

    // 24h rolling statistics
    pub volume: Option<f64>,
    pub volume_usd: Option<f64>,
    pub volume_notional: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub price_change: Option<f64>,

    // Options-specific fields
    pub underlying_index: Option<Arc<str>>,
    pub underlying_price: Option<f64>,
    pub mark_iv: Option<f64>,
    pub bid_iv: Option<f64>,
    pub ask_iv: Option<f64>,
    pub interest_rate: Option<f64>,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub vega: Option<f64>,
    pub theta: Option<f64>,
    pub rho: Option<f64>,

    // Perpetuals-specific fields
    pub current_funding: Option<f64>,
    pub funding_8h: Option<f64>,
    pub interest_value: Option<f64>,
}

/// Three-state patch field used to accurately apply Deribit `incremental_ticker` deltas:
/// - `Absent`: JSON key omitted -> leave existing materialized value untouched
/// - `Null`: JSON key explicitly `null` -> clear existing value to `None`
/// - `Value(v)`: JSON key present -> replace existing value with `Some(v)`
#[derive(Debug, Clone, Default)]
pub enum Patch<T> {
    #[default]
    Absent,
    Null,
    Value(T),
}

impl<T> Patch<T> {
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }
}

/// Normalized ticker patch decoded from either `ticker.*` or `incremental_ticker.*`.
#[derive(Debug, Clone, Default)]
pub struct TickerPatch {
    pub exchange_ts_ms: i64,
    pub is_snapshot: bool,
    pub state: Patch<Arc<str>>,

    pub open_interest: Patch<f64>,
    pub mark_price: Patch<f64>,
    pub index_price: Patch<f64>,
    pub last_price: Patch<f64>,
    pub settlement_price: Patch<f64>,
    pub estimated_delivery_price: Patch<f64>,
    pub min_price: Patch<f64>,
    pub max_price: Patch<f64>,

    pub best_bid_price: Patch<f64>,
    pub best_bid_amount: Patch<f64>,
    pub best_ask_price: Patch<f64>,
    pub best_ask_amount: Patch<f64>,

    pub volume: Patch<f64>,
    pub volume_usd: Patch<f64>,
    pub volume_notional: Patch<f64>,
    pub high: Patch<f64>,
    pub low: Patch<f64>,
    pub price_change: Patch<f64>,

    pub underlying_index: Patch<Arc<str>>,
    pub underlying_price: Patch<f64>,
    pub mark_iv: Patch<f64>,
    pub bid_iv: Patch<f64>,
    pub ask_iv: Patch<f64>,
    pub interest_rate: Patch<f64>,
    pub delta: Patch<f64>,
    pub gamma: Patch<f64>,
    pub vega: Patch<f64>,
    pub theta: Patch<f64>,
    pub rho: Patch<f64>,

    pub current_funding: Patch<f64>,
    pub funding_8h: Patch<f64>,
    pub interest_value: Patch<f64>,
}

/// Per-instrument in-memory ticker state materializer.
#[derive(Debug, Clone)]
pub struct TickerState {
    instrument_id: InstrumentId,
    initialized: bool,
    pub last_emitted_hour: i64,
    state: Arc<str>,
    open_interest: Option<f64>,
    mark_price: Option<f64>,
    index_price: Option<f64>,
    last_price: Option<f64>,
    settlement_price: Option<f64>,
    estimated_delivery_price: Option<f64>,
    min_price: Option<f64>,
    max_price: Option<f64>,
    best_bid_price: Option<f64>,
    best_bid_amount: Option<f64>,
    best_ask_price: Option<f64>,
    best_ask_amount: Option<f64>,
    volume: Option<f64>,
    volume_usd: Option<f64>,
    volume_notional: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    price_change: Option<f64>,
    underlying_index: Option<Arc<str>>,
    underlying_price: Option<f64>,
    mark_iv: Option<f64>,
    bid_iv: Option<f64>,
    ask_iv: Option<f64>,
    interest_rate: Option<f64>,
    delta: Option<f64>,
    gamma: Option<f64>,
    vega: Option<f64>,
    theta: Option<f64>,
    rho: Option<f64>,
    current_funding: Option<f64>,
    funding_8h: Option<f64>,
    interest_value: Option<f64>,
}

fn opt_f64_eq(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
        _ => false,
    }
}

fn apply_opt_f64(slot: &mut Option<f64>, patch: &Patch<f64>, is_full: bool) -> bool {
    let next = match patch {
        Patch::Absent => {
            if is_full {
                None
            } else {
                return false;
            }
        }
        Patch::Null => None,
        Patch::Value(v) => Some(*v),
    };
    if opt_f64_eq(*slot, next) {
        false
    } else {
        *slot = next;
        true
    }
}

fn apply_opt_str(slot: &mut Option<Arc<str>>, patch: &Patch<Arc<str>>, is_full: bool) -> bool {
    let next = match patch {
        Patch::Absent => {
            if is_full {
                None
            } else {
                return false;
            }
        }
        Patch::Null => None,
        Patch::Value(v) => Some(Arc::clone(v)),
    };
    if *slot == next {
        false
    } else {
        *slot = next;
        true
    }
}

impl TickerState {
    pub fn new(instrument_id: InstrumentId) -> Self {
        Self {
            instrument_id,
            initialized: false,
            last_emitted_hour: i64::MIN,
            state: Arc::from("open"),
            open_interest: None,
            mark_price: None,
            index_price: None,
            last_price: None,
            settlement_price: None,
            estimated_delivery_price: None,
            min_price: None,
            max_price: None,
            best_bid_price: None,
            best_bid_amount: None,
            best_ask_price: None,
            best_ask_amount: None,
            volume: None,
            volume_usd: None,
            volume_notional: None,
            high: None,
            low: None,
            price_change: None,
            underlying_index: None,
            underlying_price: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            interest_rate: None,
            delta: None,
            gamma: None,
            vega: None,
            theta: None,
            rho: None,
            current_funding: None,
            funding_8h: None,
            interest_value: None,
        }
    }

    /// Apply a ticker snapshot or delta.
    /// Returns `(materialized_row, changed_mask, is_initial_snapshot)`.
    pub fn apply(
        &mut self,
        patch: &TickerPatch,
        received_ts_ns: i64,
        full_replace_on_snapshot: bool,
    ) -> (TickerRow, u32, bool) {
        let is_full = patch.is_snapshot && full_replace_on_snapshot;
        let first_seen = !self.initialized;
        self.initialized = true;

        let mut mask = 0u32;

        if let Patch::Value(s) = &patch.state
            && self.state != *s
        {
            self.state = Arc::clone(s);
            mask |= ticker_mask::STATE;
        }

        if apply_opt_f64(&mut self.open_interest, &patch.open_interest, is_full) {
            mask |= ticker_mask::OPEN_INTEREST;
        }
        if apply_opt_f64(&mut self.mark_price, &patch.mark_price, is_full) {
            mask |= ticker_mask::MARK_PRICE;
        }
        if apply_opt_f64(&mut self.index_price, &patch.index_price, is_full) {
            mask |= ticker_mask::INDEX_PRICE;
        }
        if apply_opt_f64(&mut self.last_price, &patch.last_price, is_full) {
            mask |= ticker_mask::LAST_PRICE;
        }
        if apply_opt_f64(&mut self.settlement_price, &patch.settlement_price, is_full)
            | apply_opt_f64(
                &mut self.estimated_delivery_price,
                &patch.estimated_delivery_price,
                is_full,
            )
            | apply_opt_f64(&mut self.min_price, &patch.min_price, is_full)
            | apply_opt_f64(&mut self.max_price, &patch.max_price, is_full)
        {
            mask |= ticker_mask::SETTLEMENT;
        }

        if apply_opt_f64(&mut self.best_bid_price, &patch.best_bid_price, is_full)
            | apply_opt_f64(&mut self.best_bid_amount, &patch.best_bid_amount, is_full)
            | apply_opt_f64(&mut self.best_ask_price, &patch.best_ask_price, is_full)
            | apply_opt_f64(&mut self.best_ask_amount, &patch.best_ask_amount, is_full)
        {
            mask |= ticker_mask::BBO;
        }

        if apply_opt_f64(&mut self.volume, &patch.volume, is_full)
            | apply_opt_f64(&mut self.volume_usd, &patch.volume_usd, is_full)
            | apply_opt_f64(&mut self.volume_notional, &patch.volume_notional, is_full)
            | apply_opt_f64(&mut self.high, &patch.high, is_full)
            | apply_opt_f64(&mut self.low, &patch.low, is_full)
            | apply_opt_f64(&mut self.price_change, &patch.price_change, is_full)
        {
            mask |= ticker_mask::VOLUME;
        }

        if apply_opt_str(&mut self.underlying_index, &patch.underlying_index, is_full)
            | apply_opt_f64(&mut self.underlying_price, &patch.underlying_price, is_full)
        {
            mask |= ticker_mask::UNDERLYING_PRICE;
        }

        if apply_opt_f64(&mut self.mark_iv, &patch.mark_iv, is_full)
            | apply_opt_f64(&mut self.bid_iv, &patch.bid_iv, is_full)
            | apply_opt_f64(&mut self.ask_iv, &patch.ask_iv, is_full)
            | apply_opt_f64(&mut self.interest_rate, &patch.interest_rate, is_full)
        {
            mask |= ticker_mask::IV;
        }

        if apply_opt_f64(&mut self.delta, &patch.delta, is_full)
            | apply_opt_f64(&mut self.gamma, &patch.gamma, is_full)
            | apply_opt_f64(&mut self.vega, &patch.vega, is_full)
            | apply_opt_f64(&mut self.theta, &patch.theta, is_full)
            | apply_opt_f64(&mut self.rho, &patch.rho, is_full)
        {
            mask |= ticker_mask::GREEKS;
        }

        if apply_opt_f64(&mut self.current_funding, &patch.current_funding, is_full)
            | apply_opt_f64(&mut self.funding_8h, &patch.funding_8h, is_full)
            | apply_opt_f64(&mut self.interest_value, &patch.interest_value, is_full)
        {
            mask |= ticker_mask::FUNDING;
        }

        let row = TickerRow {
            received_ts_ns,
            exchange_ts_ms: patch.exchange_ts_ms,
            instrument_id: self.instrument_id,
            is_snapshot: patch.is_snapshot || first_seen,
            changed_mask: mask,
            state: Arc::clone(&self.state),
            open_interest: self.open_interest,
            mark_price: self.mark_price,
            index_price: self.index_price,
            last_price: self.last_price,
            settlement_price: self.settlement_price,
            estimated_delivery_price: self.estimated_delivery_price,
            min_price: self.min_price,
            max_price: self.max_price,
            best_bid_price: self.best_bid_price,
            best_bid_amount: self.best_bid_amount,
            best_ask_price: self.best_ask_price,
            best_ask_amount: self.best_ask_amount,
            volume: self.volume,
            volume_usd: self.volume_usd,
            volume_notional: self.volume_notional,
            high: self.high,
            low: self.low,
            price_change: self.price_change,
            underlying_index: self.underlying_index.clone(),
            underlying_price: self.underlying_price,
            mark_iv: self.mark_iv,
            bid_iv: self.bid_iv,
            ask_iv: self.ask_iv,
            interest_rate: self.interest_rate,
            delta: self.delta,
            gamma: self.gamma,
            vega: self.vega,
            theta: self.theta,
            rho: self.rho,
            current_funding: self.current_funding,
            funding_8h: self.funding_8h,
            interest_value: self.interest_value,
        };

        (row, mask, first_seen || patch.is_snapshot)
    }

    /// Emit a complete anchor row (e.g. at a UTC hour boundary for quiet strikes whose open interest
    /// would otherwise be absent from an hour partition).
    pub fn anchor_row(&mut self, exchange_ts_ms: i64, received_ts_ns: i64) -> Option<TickerRow> {
        if !self.initialized {
            return None;
        }
        self.last_emitted_hour = utc_hour_index(exchange_ts_ms);
        Some(TickerRow {
            received_ts_ns,
            exchange_ts_ms,
            instrument_id: self.instrument_id,
            is_snapshot: true,
            changed_mask: 0,
            state: Arc::clone(&self.state),
            open_interest: self.open_interest,
            mark_price: self.mark_price,
            index_price: self.index_price,
            last_price: self.last_price,
            settlement_price: self.settlement_price,
            estimated_delivery_price: self.estimated_delivery_price,
            min_price: self.min_price,
            max_price: self.max_price,
            best_bid_price: self.best_bid_price,
            best_bid_amount: self.best_bid_amount,
            best_ask_price: self.best_ask_price,
            best_ask_amount: self.best_ask_amount,
            volume: self.volume,
            volume_usd: self.volume_usd,
            volume_notional: self.volume_notional,
            high: self.high,
            low: self.low,
            price_change: self.price_change,
            underlying_index: self.underlying_index.clone(),
            underlying_price: self.underlying_price,
            mark_iv: self.mark_iv,
            bid_iv: self.bid_iv,
            ask_iv: self.ask_iv,
            interest_rate: self.interest_rate,
            delta: self.delta,
            gamma: self.gamma,
            vega: self.vega,
            theta: self.theta,
            rho: self.rho,
            current_funding: self.current_funding,
            funding_8h: self.funding_8h,
            interest_value: self.interest_value,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TradeRow {
    pub received_ts_ns: i64,
    pub exchange_ts_ms: i64,
    pub instrument_id: InstrumentId,
    pub trade_id: String,
    pub trade_seq: u64,
    pub price: f64,
    pub amount: f64,
    pub contracts: Option<f64>,
    pub direction: TradeDirection,
    pub tick_direction: i8,
    pub iv: Option<f64>,
    pub index_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub liquidation: LiquidationSide,
    pub block_trade_id: Option<String>,
    pub block_trade_leg_count: Option<u16>,
    pub combo_id: Option<String>,
    pub combo_trade_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexRow {
    pub received_ts_ns: i64,
    pub exchange_ts_ms: i64,
    pub index_name: Arc<str>,
    pub kind: IndexKind,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct CaptureEvent {
    pub recorded_ts_ns: i64,
    pub shard_id: u16,
    pub event_type: Arc<str>,
    pub instrument_name: Option<Arc<str>>,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Book(BookUpdateEvent),
    Ticker(Box<TickerRow>),
    Trade(TradeRow),
    Index(IndexRow),
    Instrument(InstrumentMeta),
    Capture(CaptureEvent),
}

pub fn now_utc_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000)
}
