use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde_json::value::RawValue;
use smallvec::SmallVec;

use crate::error::{CollectorError, Result};
use crate::model::{
    BookAction, BookLevelDelta, BookSide, LiquidationSide, Patch, TickerPatch, TradeDirection,
};

/// Borrowed top-level Deribit JSON-RPC 2.0 frame.
/// `params` and `result` stay as zero-copy `&RawValue` slices into the incoming text buffer
/// so no intermediate `serde_json::Value` tree is ever allocated on the hot path.
#[derive(Debug, Deserialize)]
pub struct WireFrame<'a> {
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(borrow, default)]
    pub method: Option<&'a str>,
    #[serde(borrow, default)]
    pub params: Option<&'a RawValue>,
    #[serde(borrow, default)]
    pub result: Option<&'a RawValue>,
    #[serde(default)]
    pub error: Option<WireRpcError>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WireRpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct WireHeartbeatParams<'a> {
    #[serde(borrow, rename = "type")]
    pub kind: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct WireSubscriptionParams<'a> {
    #[serde(borrow)]
    pub channel: &'a str,
    #[serde(borrow)]
    pub data: &'a RawValue,
}

#[derive(Debug, Deserialize)]
pub struct WireAuthResult {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
}

/// Parsed Deribit WebSocket message ready for shard routing.
#[derive(Debug)]
pub enum ParsedWsMessage<'a> {
    Heartbeat {
        kind: &'a str,
    },
    RpcSuccess {
        id: u64,
        result: Option<&'a RawValue>,
    },
    RpcError {
        id: Option<u64>,
        error: WireRpcError,
    },
    Book {
        channel: &'a str,
        data: WireBookData<'a>,
    },
    Ticker {
        channel: &'a str,
        is_incremental_channel: bool,
        instrument_name: String,
        patch: TickerPatch,
    },
    Trades {
        channel: &'a str,
        trades: Vec<WireTrade<'a>>,
    },
    PriceIndex {
        channel: &'a str,
        data: WirePriceIndex<'a>,
    },
    VolatilityIndex {
        channel: &'a str,
        data: WireVolatilityIndex<'a>,
    },
    InstrumentState {
        channel: &'a str,
        data: WireInstrumentState<'a>,
    },
    Ignored,
}

/// Zero-Value-allocation entry point for every incoming Deribit WebSocket text frame.
pub fn parse_ws_frame(raw: &str) -> Result<ParsedWsMessage<'_>> {
    let frame: WireFrame<'_> = serde_json::from_str(raw)?;

    if let Some(error) = frame.error {
        return Ok(ParsedWsMessage::RpcError {
            id: frame.id,
            error,
        });
    }

    if let Some(method) = frame.method {
        match method {
            "subscription" => {
                let params_raw = frame.params.ok_or_else(|| {
                    CollectorError::Protocol("subscription missing params".into())
                })?;
                let sub: WireSubscriptionParams<'_> = serde_json::from_str(params_raw.get())?;
                return parse_subscription(sub.channel, sub.data.get());
            }
            "heartbeat" => {
                let params_raw = frame
                    .params
                    .ok_or_else(|| CollectorError::Protocol("heartbeat missing params".into()))?;
                let hb: WireHeartbeatParams<'_> = serde_json::from_str(params_raw.get())?;
                return Ok(ParsedWsMessage::Heartbeat { kind: hb.kind });
            }
            _ => return Ok(ParsedWsMessage::Ignored),
        }
    }

    if let Some(id) = frame.id {
        return Ok(ParsedWsMessage::RpcSuccess {
            id,
            result: frame.result,
        });
    }

    Ok(ParsedWsMessage::Ignored)
}

fn parse_subscription<'a>(channel: &'a str, data_json: &'a str) -> Result<ParsedWsMessage<'a>> {
    if channel.starts_with("book.") {
        let data: WireBookData<'a> = serde_json::from_str(data_json)?;
        Ok(ParsedWsMessage::Book { channel, data })
    } else if channel.starts_with("incremental_ticker.") {
        let wire: WireTicker<'a> = serde_json::from_str(data_json)?;
        let instrument_name = wire.instrument_name.to_owned();
        let patch = wire.into_patch(true);
        Ok(ParsedWsMessage::Ticker {
            channel,
            is_incremental_channel: true,
            instrument_name,
            patch,
        })
    } else if channel.starts_with("ticker.") {
        let wire: WireTicker<'a> = serde_json::from_str(data_json)?;
        let instrument_name = wire.instrument_name.to_owned();
        let patch = wire.into_patch(false);
        Ok(ParsedWsMessage::Ticker {
            channel,
            is_incremental_channel: false,
            instrument_name,
            patch,
        })
    } else if channel.starts_with("trades.") {
        let trades: Vec<WireTrade<'a>> = serde_json::from_str(data_json)?;
        Ok(ParsedWsMessage::Trades { channel, trades })
    } else if channel.starts_with("deribit_price_index.") {
        let data: WirePriceIndex<'a> = serde_json::from_str(data_json)?;
        Ok(ParsedWsMessage::PriceIndex { channel, data })
    } else if channel.starts_with("deribit_volatility_index.") {
        let data: WireVolatilityIndex<'a> = serde_json::from_str(data_json)?;
        Ok(ParsedWsMessage::VolatilityIndex { channel, data })
    } else if channel.starts_with("instrument.state.") {
        let data: WireInstrumentState<'a> = serde_json::from_str(data_json)?;
        Ok(ParsedWsMessage::InstrumentState { channel, data })
    } else {
        Ok(ParsedWsMessage::Ignored)
    }
}

// ---------------------------------------------------------------------------
// Order book wire types (custom zero-String-allocation tuple visitor)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct WireBookEntry {
    pub action: BookAction,
    pub price: f64,
    pub amount: f64,
}

impl<'de> Deserialize<'de> for WireBookEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntryVisitor;

        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = WireBookEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 3-element array [action, price, amount]")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let action_str: &str = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let action = match action_str {
                    "new" => BookAction::New,
                    "change" => BookAction::Change,
                    "delete" => BookAction::Delete,
                    other => {
                        return Err(de::Error::unknown_variant(
                            other,
                            &["new", "change", "delete"],
                        ));
                    }
                };
                let price: f64 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let amount: f64 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                // Drain any trailing elements if Deribit ever adds extra tuple items.
                while seq.next_element::<de::IgnoredAny>()?.is_some() {}
                Ok(WireBookEntry {
                    action,
                    price,
                    amount,
                })
            }
        }

        deserializer.deserialize_seq(EntryVisitor)
    }
}

#[derive(Debug, Deserialize)]
pub struct WireBookData<'a> {
    pub timestamp: i64,
    #[serde(borrow, rename = "type")]
    pub msg_type: &'a str,
    pub change_id: u64,
    #[serde(default)]
    pub prev_change_id: Option<u64>,
    #[serde(borrow)]
    pub instrument_name: &'a str,
    #[serde(default)]
    pub bids: SmallVec<[WireBookEntry; 8]>,
    #[serde(default)]
    pub asks: SmallVec<[WireBookEntry; 8]>,
}

impl WireBookData<'_> {
    pub fn is_snapshot(&self) -> bool {
        self.msg_type == "snapshot"
    }

    pub fn into_levels(&self) -> SmallVec<[BookLevelDelta; 8]> {
        let total = self.bids.len() + self.asks.len();
        let mut out = SmallVec::with_capacity(total.max(1));
        for b in &self.bids {
            out.push(BookLevelDelta {
                side: BookSide::Bid,
                action: b.action,
                price: b.price,
                amount: b.amount,
            });
        }
        for a in &self.asks {
            out.push(BookLevelDelta {
                side: BookSide::Ask,
                action: a.action,
                price: a.price,
                amount: a.amount,
            });
        }
        if out.is_empty() {
            // Preserve change_id continuity even on empty book snapshots/deltas.
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

// ---------------------------------------------------------------------------
// Ticker & incremental_ticker wire types with 3-state Patch<T>
// ---------------------------------------------------------------------------

fn deserialize_patch_f64<'de, D>(deserializer: D) -> std::result::Result<Patch<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<f64>::deserialize(deserializer)?;
    Ok(match opt {
        Some(v) => Patch::Value(v),
        None => Patch::Null,
    })
}

fn deserialize_patch_arc_str<'de, D>(
    deserializer: D,
) -> std::result::Result<Patch<Arc<str>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(match opt {
        Some(v) => Patch::Value(Arc::from(v)),
        None => Patch::Null,
    })
}

#[derive(Debug, Default, Deserialize)]
struct WireStats {
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    volume: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    volume_usd: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    volume_notional: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    high: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    low: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    price_change: Patch<f64>,
}

#[derive(Debug, Default, Deserialize)]
struct WireGreeks {
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    delta: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    gamma: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    vega: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    theta: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    rho: Patch<f64>,
}

#[derive(Debug, Deserialize)]
struct WireTicker<'a> {
    timestamp: i64,
    #[serde(borrow)]
    instrument_name: &'a str,
    #[serde(borrow, rename = "type", default)]
    msg_type: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_patch_arc_str")]
    state: Patch<Arc<str>>,

    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    open_interest: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    mark_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    index_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    last_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    settlement_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    estimated_delivery_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    min_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    max_price: Patch<f64>,

    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    best_bid_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    best_bid_amount: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    best_ask_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    best_ask_amount: Patch<f64>,

    #[serde(default)]
    stats: Option<WireStats>,
    #[serde(default)]
    greeks: Option<WireGreeks>,

    #[serde(default, deserialize_with = "deserialize_patch_arc_str")]
    underlying_index: Patch<Arc<str>>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    underlying_price: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    mark_iv: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    bid_iv: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    ask_iv: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    interest_rate: Patch<f64>,

    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    current_funding: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    funding_8h: Patch<f64>,
    #[serde(default, deserialize_with = "deserialize_patch_f64")]
    interest_value: Patch<f64>,
}

impl WireTicker<'_> {
    fn into_patch(self, is_incremental_channel: bool) -> TickerPatch {
        let is_snapshot = if is_incremental_channel {
            self.msg_type == Some("snapshot")
        } else {
            true
        };
        let stats = self.stats.unwrap_or_default();
        let greeks = self.greeks.unwrap_or_default();

        TickerPatch {
            exchange_ts_ms: self.timestamp,
            is_snapshot,
            state: self.state,
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
            volume: stats.volume,
            volume_usd: stats.volume_usd,
            volume_notional: stats.volume_notional,
            high: stats.high,
            low: stats.low,
            price_change: stats.price_change,
            underlying_index: self.underlying_index,
            underlying_price: self.underlying_price,
            mark_iv: self.mark_iv,
            bid_iv: self.bid_iv,
            ask_iv: self.ask_iv,
            interest_rate: self.interest_rate,
            delta: greeks.delta,
            gamma: greeks.gamma,
            vega: greeks.vega,
            theta: greeks.theta,
            rho: greeks.rho,
            current_funding: self.current_funding,
            funding_8h: self.funding_8h,
            interest_value: self.interest_value,
        }
    }
}

// ---------------------------------------------------------------------------
// Trades & Instrument State wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct WireTrade<'a> {
    pub timestamp: i64,
    #[serde(borrow)]
    pub instrument_name: &'a str,
    #[serde(borrow)]
    pub trade_id: &'a str,
    pub trade_seq: u64,
    pub price: f64,
    pub amount: f64,
    #[serde(default)]
    pub contracts: Option<f64>,
    #[serde(borrow)]
    pub direction: &'a str,
    #[serde(default)]
    pub tick_direction: i8,
    #[serde(default)]
    pub iv: Option<f64>,
    #[serde(default)]
    pub index_price: Option<f64>,
    #[serde(default)]
    pub mark_price: Option<f64>,
    #[serde(borrow, default)]
    pub liquidation: Option<&'a str>,
    #[serde(borrow, default)]
    pub block_trade_id: Option<&'a str>,
    #[serde(default)]
    pub block_trade_leg_count: Option<u16>,
    #[serde(borrow, default)]
    pub combo_id: Option<&'a str>,
    #[serde(borrow, default)]
    pub combo_trade_id: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
pub struct WirePriceIndex<'a> {
    pub timestamp: i64,
    #[serde(borrow)]
    pub index_name: &'a str,
    pub price: f64,
}

#[derive(Debug, Deserialize)]
pub struct WireVolatilityIndex<'a> {
    pub timestamp: i64,
    #[serde(borrow)]
    pub index_name: &'a str,
    pub volatility: f64,
}

impl WireTrade<'_> {
    pub fn parsed_direction(&self) -> TradeDirection {
        if self.direction.eq_ignore_ascii_case("buy") {
            TradeDirection::Buy
        } else {
            TradeDirection::Sell
        }
    }

    pub fn parsed_liquidation(&self) -> LiquidationSide {
        match self.liquidation {
            Some("M") => LiquidationSide::Maker,
            Some("T") => LiquidationSide::Taker,
            Some("MT") => LiquidationSide::Both,
            _ => LiquidationSide::None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WireInstrumentState<'a> {
    pub timestamp: i64,
    #[serde(borrow)]
    pub instrument_name: &'a str,
    #[serde(borrow)]
    pub state: &'a str,
}

// ---------------------------------------------------------------------------
// Outbound JSON-RPC helpers
// ---------------------------------------------------------------------------

pub fn build_set_heartbeat(id: u64, interval_secs: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"public/set_heartbeat","params":{{"interval":{interval_secs}}}}}"#
    )
}

pub fn build_test_request(id: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"public/test","params":{{}}}}"#)
}

pub fn build_subscribe(id: u64, channels: &[String]) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/subscribe",
        "params": { "channels": channels }
    })
    .to_string()
}

pub fn build_unsubscribe(id: u64, channels: &[String]) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/unsubscribe",
        "params": { "channels": channels }
    })
    .to_string()
}

pub fn build_auth_client_credentials(id: u64, client_id: &str, client_secret: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    })
    .to_string()
}

pub fn build_auth_refresh(id: u64, refresh_token: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/auth",
        "params": {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    })
    .to_string()
}
