use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::{AppConfig, DERIBIT_MAX_CONNS_PER_IP};
use crate::deribit::discovery::{DiscoveredCatalog, InstrumentDiscovery, channels_for_instrument};
use crate::deribit::shard::{
    InstrumentLifecycleNotice, ShardCommand, ShardConfig, ShardRole, run_shard,
};
use crate::error::Result;
use crate::model::{CaptureEvent, InstrumentKind, MarketEvent, now_utc_ns};
use crate::registry::{InstrumentRegistry, market_matches_filter};
use crate::telemetry::Telemetry;

struct ManagedShard {
    shard_id: u16,
    role: ShardRole,
    cmd_tx: mpsc::Sender<ShardCommand>,
    channels: BTreeSet<String>,
}

/// Orchestrates workload-isolated WebSocket shards and zero-downtime instrument lifecycle tracking.
pub struct WsManager {
    app: Arc<AppConfig>,
    registry: Arc<InstrumentRegistry>,
    telemetry: Arc<Telemetry>,
    discovery: Arc<InstrumentDiscovery>,
    ingest_tx: mpsc::Sender<MarketEvent>,
    shutdown_rx: watch::Receiver<bool>,
    shards: Vec<ManagedShard>,
    channel_owner: BTreeMap<String, usize>,
    instrument_channels: BTreeMap<String, Vec<String>>,
    handles: Vec<JoinHandle<()>>,
    lifecycle_rx: Option<mpsc::Receiver<InstrumentLifecycleNotice>>,
}

impl WsManager {
    pub fn new(
        app: Arc<AppConfig>,
        registry: Arc<InstrumentRegistry>,
        telemetry: Arc<Telemetry>,
        discovery: Arc<InstrumentDiscovery>,
        ingest_tx: mpsc::Sender<MarketEvent>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            app,
            registry,
            telemetry,
            discovery,
            ingest_tx,
            shutdown_rx,
            shards: Vec::new(),
            channel_owner: BTreeMap::new(),
            instrument_channels: BTreeMap::new(),
            handles: Vec::new(),
            lifecycle_rx: None,
        }
    }

    /// Perform initial catalog discovery, persist instrument metadata, and spawn all shards.
    pub async fn start(&mut self) -> Result<()> {
        let catalog = self
            .discovery
            .fetch_catalog(&self.app.deribit, &self.registry)
            .await?;

        // Persist initial instrument catalog snapshot to the `instruments` dataset.
        for meta in catalog.all_iter() {
            self.telemetry
                .instrument_updates
                .fetch_add(1, Ordering::Relaxed);
            let _ = self
                .ingest_tx
                .send(MarketEvent::Instrument(meta.clone()))
                .await;
        }

        self.spawn_initial_shards(&catalog);

        let _ = self
            .ingest_tx
            .send(MarketEvent::Capture(CaptureEvent {
                recorded_ts_ns: now_utc_ns(),
                shard_id: 0,
                event_type: Arc::from("startup"),
                instrument_name: None,
                detail: format!(
                    "options={} perpetuals={} futures={} shards={} channels={}",
                    catalog.options.len(),
                    catalog.perpetuals.len(),
                    catalog.dated_futures.len(),
                    self.shards.len(),
                    self.channel_owner.len()
                ),
            }))
            .await;

        Ok(())
    }

    fn spawn_initial_shards(&mut self, catalog: &DiscoveredCatalog) {
        let max_per_conn = self.app.network.max_channels_per_conn;
        // Leave 10% headroom per option shard for intraday strike/expiry listings without re-sharding.
        let target_opt_cap = (max_per_conn * 90 / 100).max(2);

        // 1. Dedicated lifecycle + trades + indices shard
        let mut lifecycle_channels = vec!["instrument.state.any.any".to_string()];
        if self.app.deribit.collect_trades {
            let interval = self.app.deribit.trades_interval.as_str();
            for curr in &catalog.settlement_currencies {
                if self.app.deribit.collect_options {
                    lifecycle_channels.push(format!("trades.option.{curr}.{interval}"));
                    if self.app.deribit.collect_combo_trades {
                        lifecycle_channels.push(format!("trades.option_combo.{curr}.{interval}"));
                    }
                }
                if self.app.deribit.collect_perpetuals || self.app.deribit.collect_dated_futures {
                    lifecycle_channels.push(format!("trades.future.{curr}.{interval}"));
                    if self.app.deribit.collect_combo_trades {
                        lifecycle_channels.push(format!("trades.future_combo.{curr}.{interval}"));
                    }
                }
            }
        }
        if self.app.deribit.collect_indices {
            for idx in &catalog.index_names {
                lifecycle_channels.push(format!("deribit_price_index.{idx}"));
                lifecycle_channels.push(format!("deribit_volatility_index.{idx}"));
            }
        }
        let (lifecycle_tx, lifecycle_rx) = mpsc::channel(4096);
        self.lifecycle_rx = Some(lifecycle_rx);
        self.spawn_shard(
            ShardRole::LifecycleAndTrades,
            lifecycle_channels,
            Some(lifecycle_tx),
        );

        // 2. Dedicated perpetuals & dated futures shard(s) (isolated from option chains!)
        let mut fut_inst_channels: Vec<(String, Vec<String>)> = Vec::new();
        for meta in catalog
            .perpetuals
            .iter()
            .chain(catalog.dated_futures.iter())
        {
            let chs = channels_for_instrument(meta, &self.app.deribit);
            if !chs.is_empty() {
                fut_inst_channels.push((meta.instrument_name.to_string(), chs));
            }
        }
        if !fut_inst_channels.is_empty() {
            let total_fut_ch: usize = fut_inst_channels.iter().map(|(_, c)| c.len()).sum();
            // Keep perpetuals/futures lightly packed (at most half max_per_conn) to minimize HOL blocking.
            let fut_cap = (max_per_conn / 2).max(20);
            let num_fut_shards = total_fut_ch.div_ceil(fut_cap).max(1);
            let mut buckets: Vec<Vec<String>> = vec![Vec::new(); num_fut_shards];
            for (idx, (name, chs)) in fut_inst_channels.into_iter().enumerate() {
                let bucket = idx % num_fut_shards;
                buckets[bucket].extend(chs.iter().cloned());
                self.instrument_channels.insert(name, chs);
            }
            for bucket in buckets {
                if !bucket.is_empty() {
                    self.spawn_shard(ShardRole::FuturesAndPerps, bucket, None);
                }
            }
        }

        // 3. Round-robin load-balanced option shards
        // Because `catalog.options` is sorted by (expiration, market, strike), round-robin spreads
        // active near-expiry ATM options evenly across all option shards & CPU cores.
        let mut opt_inst_channels: Vec<(String, Vec<String>)> = Vec::new();
        for meta in &catalog.options {
            let chs = channels_for_instrument(meta, &self.app.deribit);
            if !chs.is_empty() {
                opt_inst_channels.push((meta.instrument_name.to_string(), chs));
            }
        }
        if !opt_inst_channels.is_empty() {
            let total_opt_ch: usize = opt_inst_channels.iter().map(|(_, c)| c.len()).sum();
            let num_opt_shards = total_opt_ch.div_ceil(target_opt_cap).max(1);
            let mut buckets: Vec<Vec<String>> = vec![Vec::new(); num_opt_shards];
            for (idx, (name, chs)) in opt_inst_channels.into_iter().enumerate() {
                let bucket = idx % num_opt_shards;
                buckets[bucket].extend(chs.iter().cloned());
                self.instrument_channels.insert(name, chs);
            }
            for bucket in buckets {
                if !bucket.is_empty() {
                    self.spawn_shard(ShardRole::Options, bucket, None);
                }
            }
        }

        self.telemetry
            .total_shards
            .store(self.shards.len(), Ordering::Relaxed);

        if self.shards.len() > DERIBIT_MAX_CONNS_PER_IP {
            warn!(
                shards = self.shards.len(),
                limit = DERIBIT_MAX_CONNS_PER_IP,
                "shard count exceeds Deribit's 32 connections/IP guideline; narrow `markets` or increase `max_channels_per_conn`"
            );
        }

        info!(
            shards = self.shards.len(),
            total_channels = self.channel_owner.len(),
            "spawned WebSocket shard pool"
        );
    }

    fn spawn_shard(
        &mut self,
        role: ShardRole,
        initial_channels: Vec<String>,
        lifecycle_tx: Option<mpsc::Sender<InstrumentLifecycleNotice>>,
    ) -> usize {
        let shard_idx = self.shards.len();
        let shard_id = shard_idx as u16;
        let (cmd_tx, cmd_rx) = mpsc::channel(256);

        let mut ch_set = BTreeSet::new();
        for ch in &initial_channels {
            ch_set.insert(ch.clone());
            self.channel_owner.insert(ch.clone(), shard_idx);
        }

        let handle = tokio::spawn(run_shard(ShardConfig {
            shard_id,
            role,
            initial_channels,
            app: Arc::clone(&self.app),
            registry: Arc::clone(&self.registry),
            telemetry: Arc::clone(&self.telemetry),
            ingest_tx: self.ingest_tx.clone(),
            lifecycle_tx,
            cmd_rx,
            shutdown: self.shutdown_rx.clone(),
        }));

        self.shards.push(ManagedShard {
            shard_id,
            role,
            cmd_tx,
            channels: ch_set,
        });
        self.handles.push(handle);
        shard_idx
    }

    /// Run real-time `instrument.state` lifecycle handling and periodic catalog reconciliation
    /// until the shutdown signal fires, then cleanly join all shard tasks.
    pub async fn run_lifecycle_loop(mut self) {
        let mut lifecycle_rx = self.lifecycle_rx.take();
        let reconcile_secs = self.app.network.catalog_reconcile_secs.max(60);
        let mut reconcile_tick = tokio::time::interval(Duration::from_secs(reconcile_secs));
        reconcile_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        reconcile_tick.tick().await;

        let mut shutdown = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                notice = async {
                    match lifecycle_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(n) = notice {
                        self.handle_lifecycle_notice(n).await;
                    }
                }
                _ = reconcile_tick.tick() => {
                    self.reconcile_catalog().await;
                }
            }
        }

        info!(
            shards = self.handles.len(),
            "waiting for WebSocket shards to shut down"
        );
        for h in self.handles {
            let _ = h.await;
        }
    }

    async fn handle_lifecycle_notice(&mut self, notice: InstrumentLifecycleNotice) {
        let name = notice.instrument_name;
        let state = notice.state;
        debug!(instrument = %name, state = %state, "received instrument.state update");

        match state.as_str() {
            "created" | "started" | "open" => {
                if self.instrument_channels.contains_key(&name) {
                    if let Some(updated) = self.registry.update_state(&name, &state) {
                        let _ = self.ingest_tx.try_send(MarketEvent::Instrument(updated));
                    }
                    return;
                }
                match self.discovery.fetch_single(&name).await {
                    Ok(Some(mut meta)) => {
                        if !self.should_track(&meta) {
                            return;
                        }
                        meta.state = Arc::from(state.as_str());
                        let id = self.registry.upsert(meta.clone());
                        meta.id = id;
                        self.telemetry
                            .instrument_updates
                            .fetch_add(1, Ordering::Relaxed);
                        let _ = self
                            .ingest_tx
                            .try_send(MarketEvent::Instrument(meta.clone()));
                        let _ = self.ingest_tx.try_send(MarketEvent::Capture(CaptureEvent {
                            recorded_ts_ns: now_utc_ns(),
                            shard_id: 0,
                            event_type: Arc::from("instrument_listed"),
                            instrument_name: Some(Arc::clone(&meta.instrument_name)),
                            detail: format!("state={state} kind={}", meta.kind.as_str()),
                        }));
                        self.subscribe_instrument(&meta).await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(instrument = %name, error = %e, "could not fetch newly announced instrument");
                    }
                }
            }
            "settled" | "closed" | "terminated" => {
                if let Some(updated) = self.registry.update_state(&name, &state) {
                    self.telemetry
                        .instrument_updates
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = self.ingest_tx.try_send(MarketEvent::Instrument(updated));
                }
                self.unsubscribe_instrument(&name).await;
                let _ = self.ingest_tx.try_send(MarketEvent::Capture(CaptureEvent {
                    recorded_ts_ns: now_utc_ns(),
                    shard_id: 0,
                    event_type: Arc::from("instrument_expired"),
                    instrument_name: Some(Arc::from(name.as_str())),
                    detail: format!("state={state}"),
                }));
            }
            _ => {
                if let Some(updated) = self.registry.update_state(&name, &state) {
                    let _ = self.ingest_tx.try_send(MarketEvent::Instrument(updated));
                }
            }
        }
    }

    async fn reconcile_catalog(&mut self) {
        let catalog = match self
            .discovery
            .fetch_catalog(&self.app.deribit, &self.registry)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "periodic catalog reconciliation failed");
                return;
            }
        };

        let mut live_names: BTreeSet<String> = BTreeSet::new();
        let mut added = 0usize;

        for meta in catalog.all_iter() {
            live_names.insert(meta.instrument_name.to_string());
            if !self
                .instrument_channels
                .contains_key(meta.instrument_name.as_ref())
            {
                self.telemetry
                    .instrument_updates
                    .fetch_add(1, Ordering::Relaxed);
                let _ = self
                    .ingest_tx
                    .try_send(MarketEvent::Instrument(meta.clone()));
                self.subscribe_instrument(meta).await;
                added += 1;
            }
        }

        let stale: Vec<String> = self
            .instrument_channels
            .keys()
            .filter(|name| !live_names.contains(*name))
            .cloned()
            .collect();
        let removed = stale.len();
        for name in stale {
            self.unsubscribe_instrument(&name).await;
        }

        if added > 0 || removed > 0 {
            info!(added, removed, "reconciled active Deribit catalog in-place");
            let _ = self.ingest_tx.try_send(MarketEvent::Capture(CaptureEvent {
                recorded_ts_ns: now_utc_ns(),
                shard_id: 0,
                event_type: Arc::from("catalog_reconcile"),
                instrument_name: None,
                detail: format!("added={added} removed={removed}"),
            }));
        }
    }

    fn should_track(&self, meta: &crate::model::InstrumentMeta) -> bool {
        if !market_matches_filter(&meta.market, &self.app.deribit.markets) {
            return false;
        }
        match meta.kind {
            InstrumentKind::Option => self.app.deribit.collect_options,
            InstrumentKind::Perpetual => self.app.deribit.collect_perpetuals,
            InstrumentKind::Future => self.app.deribit.collect_dated_futures,
            InstrumentKind::OptionCombo | InstrumentKind::FutureCombo => false,
        }
    }

    async fn subscribe_instrument(&mut self, meta: &crate::model::InstrumentMeta) {
        let chs = channels_for_instrument(meta, &self.app.deribit);
        if chs.is_empty() {
            return;
        }
        let target_role = match meta.kind {
            InstrumentKind::Option | InstrumentKind::OptionCombo => ShardRole::Options,
            InstrumentKind::Perpetual | InstrumentKind::Future | InstrumentKind::FutureCombo => {
                ShardRole::FuturesAndPerps
            }
        };
        let max_per_conn = self.app.network.max_channels_per_conn;

        // Pick least-loaded shard with the right role that has room for all channels of this instrument.
        let candidate = self
            .shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.role == target_role && s.channels.len() + chs.len() <= max_per_conn)
            .min_by_key(|(_, s)| s.channels.len())
            .map(|(idx, _)| idx);

        let shard_idx = match candidate {
            Some(idx) => {
                for ch in &chs {
                    self.shards[idx].channels.insert(ch.clone());
                    self.channel_owner.insert(ch.clone(), idx);
                }
                let _ = self.shards[idx]
                    .cmd_tx
                    .send(ShardCommand::Subscribe(chs.clone()))
                    .await;
                idx
            }
            None => {
                // Spawn a new shard dynamically if existing shards hit max_channels_per_conn.
                let idx = self.spawn_shard(target_role, chs.clone(), None);
                self.telemetry
                    .total_shards
                    .store(self.shards.len(), Ordering::Relaxed);
                idx
            }
        };

        debug!(
            shard_id = self.shards[shard_idx].shard_id,
            instrument = %meta.instrument_name,
            channels = chs.len(),
            "routed new instrument to shard"
        );
        self.instrument_channels
            .insert(meta.instrument_name.to_string(), chs);
    }

    async fn unsubscribe_instrument(&mut self, instrument_name: &str) {
        let Some(chs) = self.instrument_channels.remove(instrument_name) else {
            return;
        };
        let mut by_shard: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for ch in chs {
            if let Some(shard_idx) = self.channel_owner.remove(&ch) {
                self.shards[shard_idx].channels.remove(&ch);
                by_shard.entry(shard_idx).or_default().push(ch);
            }
        }
        for (shard_idx, list) in by_shard {
            let _ = self.shards[shard_idx]
                .cmd_tx
                .send(ShardCommand::Unsubscribe(list))
                .await;
        }
    }
}
