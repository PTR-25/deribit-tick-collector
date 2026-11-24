use std::sync::Arc;

use chrono::{NaiveDate, TimeZone, Utc};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::model::{InstrumentId, InstrumentKind, InstrumentMeta, OptionType, now_utc_ns};

/// Parsed fields derived purely from a Deribit instrument name (used as fallback when an
/// instrument appears on a stream before a REST catalog refresh).
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedInstrumentName {
    pub market: String,
    pub base_currency: String,
    pub settlement_currency: String,
    pub kind: InstrumentKind,
    pub expiration_ts_ms: Option<i64>,
    pub strike: Option<f64>,
    pub option_type: Option<OptionType>,
}

/// Derive canonical market family from Deribit base + settlement currencies.
/// - Inverse (`base == settlement`, e.g., BTC/BTC, ETH/ETH): `"BTC"`, `"ETH"`
/// - Linear (`base != settlement`, e.g., BTC/USDC, SOL/USDC): `"BTC_USDC"`, `"SOL_USDC"`
pub fn canonical_market(base_currency: &str, settlement_currency: &str) -> String {
    if base_currency.eq_ignore_ascii_case(settlement_currency) {
        base_currency.to_ascii_uppercase()
    } else {
        format!(
            "{}_{}",
            base_currency.to_ascii_uppercase(),
            settlement_currency.to_ascii_uppercase()
        )
    }
}

/// Parse a Deribit instrument name such as:
/// - `BTC-25DEC26-100000-C`
/// - `XRP_USDC-26SEP25-0d625-P`
/// - `BTC_USDC-25NOV26-60000-C`
/// - `BTC-PERPETUAL`
/// - `SOL_USDC-PERPETUAL`
/// - `1000PEPE_USDC-PERPETUAL`
/// - `BTC-25DEC26`
/// - `BTC_USDC-25DEC26`
pub fn parse_instrument_name(name: &str) -> Option<ParsedInstrumentName> {
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    let prefix = parts[0];
    let (base_currency, settlement_currency) = match prefix.split_once('_') {
        Some((base, settle)) if !base.is_empty() && !settle.is_empty() => {
            (base.to_ascii_uppercase(), settle.to_ascii_uppercase())
        }
        _ => (prefix.to_ascii_uppercase(), prefix.to_ascii_uppercase()),
    };
    let market = canonical_market(&base_currency, &settlement_currency);

    match parts.as_slice() {
        [_, "PERPETUAL"] => Some(ParsedInstrumentName {
            market,
            base_currency,
            settlement_currency,
            kind: InstrumentKind::Perpetual,
            expiration_ts_ms: None,
            strike: None,
            option_type: None,
        }),
        [_, expiry_str] => {
            let exp_ms = parse_deribit_expiry_ms(expiry_str)?;
            Some(ParsedInstrumentName {
                market,
                base_currency,
                settlement_currency,
                kind: InstrumentKind::Future,
                expiration_ts_ms: Some(exp_ms),
                strike: None,
                option_type: None,
            })
        }
        [_, expiry_str, strike_str, opt_str] => {
            let exp_ms = parse_deribit_expiry_ms(expiry_str)?;
            let strike = parse_deribit_strike(strike_str)?;
            let option_type = match *opt_str {
                "C" => OptionType::Call,
                "P" => OptionType::Put,
                _ => return None,
            };
            Some(ParsedInstrumentName {
                market,
                base_currency,
                settlement_currency,
                kind: InstrumentKind::Option,
                expiration_ts_ms: Some(exp_ms),
                strike: Some(strike),
                option_type: Some(option_type),
            })
        }
        _ => None,
    }
}

/// Parse Deribit expiry code like `25NOV26` or `4DEC26` into 08:00:00 UTC epoch milliseconds.
pub fn parse_deribit_expiry_ms(code: &str) -> Option<i64> {
    let date = NaiveDate::parse_from_str(code, "%d%b%y").ok()?;
    let dt = date.and_hms_opt(8, 0, 0)?;
    Some(Utc.from_utc_datetime(&dt).timestamp_millis())
}

/// Parse Deribit strike token (supports decimal notation like `0d625` -> `0.625`).
pub fn parse_deribit_strike(raw: &str) -> Option<f64> {
    if raw.contains('d') {
        let normalized = raw.replacen('d', ".", 1);
        normalized.parse::<f64>().ok()
    } else {
        raw.parse::<f64>().ok()
    }
}

/// Derive canonical Deribit index name from a market family:
/// - Inverse `"BTC"` -> `"btc_usd"`, `"ETH"` -> `"eth_usd"`
/// - Linear `"BTC_USDC"` -> `"btc_usdc"`, `"SOL_USDC"` -> `"sol_usdc"`
pub fn deribit_index_name(market: &str) -> String {
    if market.contains('_') {
        market.to_ascii_lowercase()
    } else {
        format!("{}_usd", market.to_ascii_lowercase())
    }
}

/// Extract canonical market family from any Deribit instrument name prefix (including combos like
/// `BTC-CCAL-26NOV26_25NOV26-57500`, `BTC-FS-25NOV26_PERP`, `SOL_USDC-STRG-...`).
pub fn market_from_instrument_prefix(name: &str) -> Option<(String, String, String)> {
    let prefix = name.split('-').next()?;
    if prefix.is_empty() {
        return None;
    }
    let (base, settle) = match prefix.split_once('_') {
        Some((b, s)) if !b.is_empty() && !s.is_empty() => {
            (b.to_ascii_uppercase(), s.to_ascii_uppercase())
        }
        _ => (prefix.to_ascii_uppercase(), prefix.to_ascii_uppercase()),
    };
    let market = canonical_market(&base, &settle);
    Some((market, base, settle))
}

/// Check whether a canonical market name matches the configured market filter list.
pub fn market_matches_filter(market: &str, filter: &[String]) -> bool {
    filter
        .iter()
        .any(|f| f == "ALL" || f.eq_ignore_ascii_case(market))
}

#[derive(Debug, Default)]
struct RegistryInner {
    by_name: FxHashMap<Arc<str>, InstrumentId>,
    metas: Vec<Arc<InstrumentMeta>>,
}

/// Thread-safe, interned instrument catalog shared across all WebSocket shards and storage writers.
#[derive(Debug, Default)]
pub struct InstrumentRegistry {
    inner: RwLock<RegistryInner>,
}

impl InstrumentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update an instrument's metadata, returning its stable interned `InstrumentId`.
    pub fn upsert(&self, mut meta: InstrumentMeta) -> InstrumentId {
        let mut guard = self.inner.write();
        if let Some(&existing_id) = guard.by_name.get(&meta.instrument_name) {
            meta.id = existing_id;
            guard.metas[existing_id.0 as usize] = Arc::new(meta);
            existing_id
        } else {
            let id = InstrumentId(guard.metas.len() as u32);
            meta.id = id;
            let name = Arc::clone(&meta.instrument_name);
            guard.metas.push(Arc::new(meta));
            guard.by_name.insert(name, id);
            id
        }
    }

    /// Fast lookup of `InstrumentId` by Deribit instrument name.
    pub fn get_id(&self, name: &str) -> Option<InstrumentId> {
        let guard = self.inner.read();
        guard.by_name.get(name).copied()
    }

    /// Look up or synthesize metadata from an instrument name if it appeared before catalog refresh.
    pub fn get_or_register_from_name(&self, name: &str) -> Option<InstrumentId> {
        if let Some(id) = self.get_id(name) {
            return Some(id);
        }
        let parsed = parse_instrument_name(name)?;
        let meta = InstrumentMeta {
            id: InstrumentId(0),
            deribit_instrument_id: None,
            instrument_name: Arc::from(name),
            market: Arc::from(parsed.market),
            base_currency: Arc::from(parsed.base_currency),
            quote_currency: Arc::from(if parsed.settlement_currency == "USDC" {
                "USDC"
            } else {
                "USD"
            }),
            settlement_currency: Arc::from(parsed.settlement_currency),
            settlement_period: Arc::from(match parsed.kind {
                InstrumentKind::Perpetual => "perpetual",
                _ => "unknown",
            }),
            kind: parsed.kind,
            creation_ts_ms: 0,
            expiration_ts_ms: parsed.expiration_ts_ms.unwrap_or(0),
            strike: parsed.strike,
            option_type: parsed.option_type,
            tick_size: 0.0,
            min_trade_amount: 0.0,
            contract_size: 0.0,
            maker_commission: 0.0,
            taker_commission: 0.0,
            is_active: true,
            state: Arc::from("open"),
            recorded_ts_ns: now_utc_ns(),
        };
        Some(self.upsert(meta))
    }

    /// Register an intraday combo instrument (e.g. `BTC-CCAL-...` or `BTC-FS-...`) seen on a
    /// `trades.option_combo.*` or `trades.future_combo.*` channel if its market matches `filter`.
    pub fn get_or_register_combo(
        &self,
        name: &str,
        kind: InstrumentKind,
        market_filter: &[String],
    ) -> Option<InstrumentId> {
        if let Some(id) = self.get_id(name) {
            return Some(id);
        }
        let (market, base, settle) = market_from_instrument_prefix(name)?;
        if !market_matches_filter(&market, market_filter) {
            return None;
        }
        let meta = InstrumentMeta {
            id: InstrumentId(0),
            deribit_instrument_id: None,
            instrument_name: Arc::from(name),
            market: Arc::from(market),
            base_currency: Arc::from(base),
            quote_currency: Arc::from(if settle == "USDC" { "USDC" } else { "USD" }),
            settlement_currency: Arc::from(settle),
            settlement_period: Arc::from("combo"),
            kind,
            creation_ts_ms: 0,
            expiration_ts_ms: 0,
            strike: None,
            option_type: None,
            tick_size: 0.0,
            min_trade_amount: 0.0,
            contract_size: 0.0,
            maker_commission: 0.0,
            taker_commission: 0.0,
            is_active: true,
            state: Arc::from("open"),
            recorded_ts_ns: now_utc_ns(),
        };
        Some(self.upsert(meta))
    }

    pub fn get_meta(&self, id: InstrumentId) -> Option<Arc<InstrumentMeta>> {
        let guard = self.inner.read();
        guard.metas.get(id.0 as usize).cloned()
    }

    pub fn update_state(&self, name: &str, state: &str) -> Option<InstrumentMeta> {
        let mut guard = self.inner.write();
        let id = *guard.by_name.get(name)?;
        let mut updated = (*guard.metas[id.0 as usize]).clone();
        updated.state = Arc::from(state);
        updated.is_active = matches!(state, "open" | "created" | "started");
        updated.recorded_ts_ns = now_utc_ns();
        guard.metas[id.0 as usize] = Arc::new(updated.clone());
        Some(updated)
    }

    pub fn len(&self) -> usize {
        self.inner.read().metas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all_metas(&self) -> Vec<Arc<InstrumentMeta>> {
        self.inner.read().metas.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inverse_and_linear_options_and_futures() {
        let opt = parse_instrument_name("BTC-25DEC26-100000-C").unwrap();
        assert_eq!(opt.market, "BTC");
        assert_eq!(opt.kind, InstrumentKind::Option);
        assert_eq!(opt.strike, Some(100_000.0));
        assert_eq!(opt.option_type, Some(OptionType::Call));

        let dec_opt = parse_instrument_name("XRP_USDC-26SEP25-0d625-P").unwrap();
        assert_eq!(dec_opt.market, "XRP_USDC");
        assert_eq!(dec_opt.base_currency, "XRP");
        assert_eq!(dec_opt.settlement_currency, "USDC");
        assert_eq!(dec_opt.strike, Some(0.625));
        assert_eq!(dec_opt.option_type, Some(OptionType::Put));

        let lin_btc = parse_instrument_name("BTC_USDC-4DEC26-71000-C").unwrap();
        assert_eq!(lin_btc.market, "BTC_USDC");
        assert_eq!(lin_btc.kind, InstrumentKind::Option);

        let perp = parse_instrument_name("BTC-PERPETUAL").unwrap();
        assert_eq!(perp.market, "BTC");
        assert_eq!(perp.kind, InstrumentKind::Perpetual);

        let lin_perp = parse_instrument_name("1000PEPE_USDC-PERPETUAL").unwrap();
        assert_eq!(lin_perp.market, "1000PEPE_USDC");
        assert_eq!(lin_perp.kind, InstrumentKind::Perpetual);

        let fut = parse_instrument_name("BTC-25NOV26").unwrap();
        assert_eq!(fut.market, "BTC");
        assert_eq!(fut.kind, InstrumentKind::Future);
        assert!(fut.expiration_ts_ms.is_some());
    }
}
