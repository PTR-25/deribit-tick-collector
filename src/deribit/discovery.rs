use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::Deserialize;
use tracing::{debug, info};

use crate::config::DeribitConfig;
use crate::error::{CollectorError, Result};
use crate::model::{InstrumentId, InstrumentKind, InstrumentMeta, OptionType, now_utc_ns};
use crate::registry::{
    InstrumentRegistry, canonical_market, deribit_index_name, market_matches_filter,
};

#[derive(Debug, Deserialize)]
struct RestEnvelope<T> {
    result: Option<T>,
    error: Option<RestError>,
}

#[derive(Debug, Deserialize)]
struct RestError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RestInstrument {
    instrument_name: String,
    #[serde(default)]
    instrument_id: Option<u64>,
    kind: String,
    base_currency: String,
    #[serde(default)]
    quote_currency: String,
    #[serde(default)]
    settlement_currency: String,
    #[serde(default)]
    settlement_period: String,
    #[serde(default)]
    creation_timestamp: i64,
    #[serde(default)]
    expiration_timestamp: i64,
    #[serde(default)]
    strike: Option<f64>,
    #[serde(default)]
    option_type: Option<String>,
    #[serde(default)]
    tick_size: f64,
    #[serde(default)]
    min_trade_amount: f64,
    #[serde(default)]
    contract_size: f64,
    #[serde(default)]
    maker_commission: f64,
    #[serde(default)]
    taker_commission: f64,
    #[serde(default = "default_true")]
    is_active: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone)]
pub struct DiscoveredCatalog {
    pub options: Vec<InstrumentMeta>,
    pub perpetuals: Vec<InstrumentMeta>,
    pub dated_futures: Vec<InstrumentMeta>,
    /// Active multi-leg option & future combos (for combo/block trade metadata resolution).
    pub combos: Vec<InstrumentMeta>,
    /// Unique settlement currencies represented (e.g. `BTC`, `ETH`, `USDC`) for `trades.*` channels.
    pub settlement_currencies: BTreeSet<String>,
    /// Unique Deribit spot price index names represented (e.g. `btc_usd`, `eth_usd`, `sol_usdc`).
    pub price_index_names: BTreeSet<String>,
    /// Unique Deribit index names for option-bearing markets (for 30d DVOL subscriptions).
    pub volatility_index_names: BTreeSet<String>,
}

impl DiscoveredCatalog {
    pub fn total_instruments(&self) -> usize {
        self.options.len() + self.perpetuals.len() + self.dated_futures.len()
    }

    pub fn all_iter(&self) -> impl Iterator<Item = &InstrumentMeta> {
        self.perpetuals
            .iter()
            .chain(self.dated_futures.iter())
            .chain(self.options.iter())
            .chain(self.combos.iter())
    }

    pub fn summary_by_market(&self) -> BTreeMap<String, (usize, usize, usize)> {
        let mut map: BTreeMap<String, (usize, usize, usize)> = BTreeMap::new();
        for inst in &self.options {
            map.entry(inst.market.to_string()).or_default().0 += 1;
        }
        for inst in &self.perpetuals {
            map.entry(inst.market.to_string()).or_default().1 += 1;
        }
        for inst in &self.dated_futures {
            map.entry(inst.market.to_string()).or_default().2 += 1;
        }
        map
    }
}

pub struct InstrumentDiscovery {
    client: reqwest::Client,
    rest_url: String,
}

impl InstrumentDiscovery {
    pub fn new(rest_url: impl Into<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .gzip(true)
            .user_agent("deribit-tick-collector/0.1")
            .build()?;
        Ok(Self {
            client,
            rest_url: rest_url.into().trim_end_matches('/').to_string(),
        })
    }

    /// Fetch active Deribit options and futures matching `cfg` and register them in `registry`.
    pub async fn fetch_catalog(
        &self,
        cfg: &DeribitConfig,
        registry: &InstrumentRegistry,
    ) -> Result<DiscoveredCatalog> {
        let mut options = Vec::new();
        let mut perpetuals = Vec::new();
        let mut dated_futures = Vec::new();
        let mut combos = Vec::new();
        let mut settlement_currencies = BTreeSet::new();
        let mut price_index_names = BTreeSet::new();
        let mut volatility_index_names = BTreeSet::new();

        let now_ms = Utc::now().timestamp_millis();
        let max_exp_ms = cfg
            .max_dte_days
            .filter(|&d| d > 0)
            .map(|days| now_ms + (days as i64) * 86_400_000);

        if cfg.collect_options {
            let raw = self.fetch_kind("option").await?;
            debug!(count = raw.len(), "fetched raw active Deribit options");
            for item in raw {
                if !item.is_active {
                    continue;
                }
                if let Some(limit) = max_exp_ms
                    && item.expiration_timestamp > limit
                {
                    continue;
                }
                let meta = convert_rest_instrument(item);
                if !market_matches_filter(&meta.market, &cfg.markets) {
                    continue;
                }
                settlement_currencies.insert(meta.settlement_currency.to_string());
                let idx_name = deribit_index_name(&meta.market);
                price_index_names.insert(idx_name.clone());
                volatility_index_names.insert(idx_name);
                let id = registry.upsert(meta.clone());
                let mut with_id = meta;
                with_id.id = id;
                options.push(with_id);
            }
        }

        if cfg.collect_perpetuals || cfg.collect_dated_futures {
            let raw = self.fetch_kind("future").await?;
            debug!(count = raw.len(), "fetched raw active Deribit futures");
            for item in raw {
                if !item.is_active {
                    continue;
                }
                let meta = convert_rest_instrument(item);
                if !market_matches_filter(&meta.market, &cfg.markets) {
                    continue;
                }
                match meta.kind {
                    InstrumentKind::Perpetual if cfg.collect_perpetuals => {
                        settlement_currencies.insert(meta.settlement_currency.to_string());
                        price_index_names.insert(deribit_index_name(&meta.market));
                        let id = registry.upsert(meta.clone());
                        let mut with_id = meta;
                        with_id.id = id;
                        perpetuals.push(with_id);
                    }
                    InstrumentKind::Future if cfg.collect_dated_futures => {
                        if let Some(limit) = max_exp_ms
                            && meta.expiration_ts_ms > limit
                        {
                            continue;
                        }
                        settlement_currencies.insert(meta.settlement_currency.to_string());
                        price_index_names.insert(deribit_index_name(&meta.market));
                        let id = registry.upsert(meta.clone());
                        let mut with_id = meta;
                        with_id.id = id;
                        dated_futures.push(with_id);
                    }
                    _ => {}
                }
            }
        }

        if cfg.collect_trades && cfg.collect_combo_trades {
            for combo_kind in ["option_combo", "future_combo"] {
                if let Ok(raw) = self.fetch_kind(combo_kind).await {
                    for item in raw {
                        if !item.is_active {
                            continue;
                        }
                        let meta = convert_rest_instrument(item);
                        if !market_matches_filter(&meta.market, &cfg.markets) {
                            continue;
                        }
                        let id = registry.upsert(meta.clone());
                        let mut with_id = meta;
                        with_id.id = id;
                        combos.push(with_id);
                    }
                }
            }
        }

        // Deterministic ordering: options sorted by (expiration_ts_ms, market, strike, option_type)
        // so round-robin shard assignment spreads near-term active ATM options evenly across shards!
        options.sort_by(|a, b| {
            a.expiration_ts_ms
                .cmp(&b.expiration_ts_ms)
                .then_with(|| a.market.cmp(&b.market))
                .then_with(|| a.strike.unwrap_or(0.0).total_cmp(&b.strike.unwrap_or(0.0)))
                .then_with(|| a.instrument_name.cmp(&b.instrument_name))
        });
        perpetuals.sort_by(|a, b| a.instrument_name.cmp(&b.instrument_name));
        dated_futures.sort_by(|a, b| {
            a.expiration_ts_ms
                .cmp(&b.expiration_ts_ms)
                .then_with(|| a.instrument_name.cmp(&b.instrument_name))
        });

        info!(
            options = options.len(),
            perpetuals = perpetuals.len(),
            dated_futures = dated_futures.len(),
            combos = combos.len(),
            price_indices = price_index_names.len(),
            dvol_indices = volatility_index_names.len(),
            markets = ?cfg.markets,
            "discovered active Deribit instruments"
        );

        Ok(DiscoveredCatalog {
            options,
            perpetuals,
            dated_futures,
            combos,
            settlement_currencies,
            price_index_names,
            volatility_index_names,
        })
    }

    /// Fetch a single instrument definition on demand (e.g. when `instrument.state` announces a new listing).
    pub async fn fetch_single(&self, instrument_name: &str) -> Result<Option<InstrumentMeta>> {
        let url = format!(
            "{}/public/get_instrument?instrument_name={}",
            self.rest_url, instrument_name
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let env: RestEnvelope<RestInstrument> = resp.json().await?;
        if let Some(err) = env.error {
            return Err(CollectorError::DeribitRpc {
                code: err.code,
                message: err.message,
            });
        }
        Ok(env.result.map(convert_rest_instrument))
    }

    async fn fetch_kind(&self, kind: &str) -> Result<Vec<RestInstrument>> {
        let url = format!(
            "{}/public/get_instruments?currency=any&kind={kind}&expired=false",
            self.rest_url
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let env: RestEnvelope<Vec<RestInstrument>> = resp.json().await?;
        if let Some(err) = env.error {
            return Err(CollectorError::DeribitRpc {
                code: err.code,
                message: err.message,
            });
        }
        Ok(env.result.unwrap_or_default())
    }
}

fn convert_rest_instrument(item: RestInstrument) -> InstrumentMeta {
    let settlement = if item.settlement_currency.is_empty() {
        item.base_currency.clone()
    } else {
        item.settlement_currency.clone()
    };
    let market = canonical_market(&item.base_currency, &settlement);
    let kind = match item.kind.as_str() {
        "option" => InstrumentKind::Option,
        "option_combo" => InstrumentKind::OptionCombo,
        "future_combo" => InstrumentKind::FutureCombo,
        _ if item.settlement_period.eq_ignore_ascii_case("perpetual")
            || item.instrument_name.ends_with("-PERPETUAL") =>
        {
            InstrumentKind::Perpetual
        }
        _ => InstrumentKind::Future,
    };
    let option_type = match item.option_type.as_deref() {
        Some("call") => Some(OptionType::Call),
        Some("put") => Some(OptionType::Put),
        _ => None,
    };

    InstrumentMeta {
        id: InstrumentId(0),
        deribit_instrument_id: item.instrument_id,
        instrument_name: Arc::from(item.instrument_name),
        market: Arc::from(market),
        base_currency: Arc::from(item.base_currency.to_ascii_uppercase()),
        quote_currency: Arc::from(item.quote_currency.to_ascii_uppercase()),
        settlement_currency: Arc::from(settlement.to_ascii_uppercase()),
        settlement_period: Arc::from(item.settlement_period),
        kind,
        creation_ts_ms: item.creation_timestamp,
        expiration_ts_ms: item.expiration_timestamp,
        strike: item.strike,
        option_type,
        tick_size: item.tick_size,
        min_trade_amount: item.min_trade_amount,
        contract_size: item.contract_size,
        maker_commission: item.maker_commission,
        taker_commission: item.taker_commission,
        is_active: item.is_active,
        state: Arc::from(if item.is_active { "open" } else { "closed" }),
        recorded_ts_ns: now_utc_ns(),
    }
}

/// Generate the exact set of Deribit WebSocket channel names for an instrument.
pub fn channels_for_instrument(meta: &InstrumentMeta, cfg: &DeribitConfig) -> Vec<String> {
    let mut channels = Vec::with_capacity(2);
    if cfg.collect_orderbook {
        let interval = match meta.kind {
            InstrumentKind::Option | InstrumentKind::OptionCombo => cfg.options_book_interval,
            InstrumentKind::Perpetual | InstrumentKind::Future | InstrumentKind::FutureCombo => {
                cfg.futures_book_interval
            }
        };
        channels.push(format!(
            "book.{}.{}",
            meta.instrument_name,
            interval.as_str()
        ));
    }
    if cfg.collect_ticker {
        channels.push(cfg.ticker_mode.channel_for(&meta.instrument_name));
    }
    channels
}
