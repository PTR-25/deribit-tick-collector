use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio::sync::{mpsc, watch};
use tracing::info;
use tracing_subscriber::EnvFilter;

use deribit_tick_collector::config::{
    AppConfig, BookInterval, CompressionCodec, StorageConfig, TickerEmitPolicy, TickerMode,
};
use deribit_tick_collector::deribit::discovery::InstrumentDiscovery;
use deribit_tick_collector::deribit::manager::WsManager;
use deribit_tick_collector::registry::InstrumentRegistry;
use deribit_tick_collector::storage::compact::compact_directory;
use deribit_tick_collector::storage::uploader::{spawn_retention_pruner, spawn_uploader};
use deribit_tick_collector::storage::writer::run_storage_engine;
use deribit_tick_collector::telemetry::{Telemetry, spawn_metrics_server, spawn_stats_logger};
use deribit_tick_collector::tools::inspect::{inspect_path, print_report};
use deribit_tick_collector::tools::replay::replay_instrument;

#[derive(Debug, Parser)]
#[command(
    name = "deribit-tick-collector",
    version,
    about = "High-performance Deribit options, perpetuals, dated futures, and ticker/OI market data collector"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the live tick-by-tick market data collector.
    Collect {
        /// Path to TOML configuration file (optional; defaults apply if omitted).
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Comma-separated markets to collect (e.g. BTC,ETH,SOL_USDC or ALL).
        #[arg(long, value_delimiter = ',')]
        markets: Option<Vec<String>>,

        /// Output directory for Hive-partitioned Parquet datasets.
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Order book interval for options (raw, 100ms, agg2).
        #[arg(long, value_enum)]
        options_book_interval: Option<BookInterval>,

        /// Order book interval for perpetuals & dated futures (raw, 100ms, agg2).
        #[arg(long, value_enum)]
        futures_book_interval: Option<BookInterval>,

        /// Ticker feed mode (incremental, raw, 100ms, agg2).
        #[arg(long, value_enum)]
        ticker_mode: Option<TickerMode>,

        /// Ticker emission policy (all, interesting-only).
        #[arg(long, value_enum)]
        ticker_emit_policy: Option<TickerEmitPolicy>,

        /// Skip options collection.
        #[arg(long)]
        no_options: bool,

        /// Skip perpetuals collection.
        #[arg(long)]
        no_perpetuals: bool,

        /// Skip dated futures collection.
        #[arg(long)]
        no_dated_futures: bool,

        /// Skip public trade prints.
        #[arg(long)]
        no_trades: bool,

        /// Skip option/future combo trade prints.
        #[arg(long)]
        no_combo_trades: bool,

        /// Skip Deribit spot price & DVOL volatility index streams.
        #[arg(long)]
        no_indices: bool,

        /// Disable hourly self-contained L2 book checkpoints and quiet ticker OI anchors.
        #[arg(long)]
        no_hourly_checkpoints: bool,

        /// Optional upper bound on days-to-expiry for options & dated futures.
        #[arg(long)]
        max_dte_days: Option<u64>,

        /// Zstd compression level (1..=22).
        #[arg(long)]
        zstd_level: Option<i32>,

        /// Flush SoA buffers to Parquet row groups every N seconds.
        #[arg(long)]
        flush_secs: Option<u64>,

        /// Rotate finalized .parquet segments every N seconds.
        #[arg(long)]
        rotation_secs: Option<u64>,

        /// Prometheus /metrics & /healthz bind address (empty string disables server).
        #[arg(long)]
        metrics_bind: Option<String>,

        /// Emit structured JSON logs.
        #[arg(long)]
        json_logs: bool,

        /// Optional finite capture duration in seconds (for smoke tests or scheduled captures).
        #[arg(long)]
        duration_secs: Option<u64>,
    },

    /// Query live Deribit active instruments and show channel/shard estimates.
    Instruments {
        #[arg(short, long)]
        config: Option<PathBuf>,

        #[arg(long, value_delimiter = ',')]
        markets: Option<Vec<String>>,
    },

    /// Inspect recorded Parquet datasets (sizes, bytes/row, OI coverage, book sequence gaps).
    Inspect {
        /// Path to a Parquet file or data directory.
        path: PathBuf,
    },

    /// Reconstruct an instrument's L2 orderbook tick-by-tick and verify integrity & OI.
    Replay {
        /// Path to a Parquet file or data directory.
        path: PathBuf,

        /// Exact Deribit instrument name (e.g. BTC-PERPETUAL or BTC-25DEC26-100000-C).
        #[arg(short, long)]
        instrument: String,

        /// Number of sample BBO snapshots to print during replay (0 = summary only).
        #[arg(long, default_value_t = 10)]
        samples: usize,
    },

    /// Compact multi-part Hive partition directories into single optimal Parquet files.
    Compact {
        /// Root data directory containing Hive partitions.
        path: PathBuf,

        /// Keep original part-*.parquet files after creating compacted files.
        #[arg(long)]
        keep_originals: bool,

        /// Zstd compression level for compacted files (default: 6).
        #[arg(long, default_value_t = 6)]
        zstd_level: i32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cli = Cli::parse();

    match cli.command {
        Command::Collect {
            config,
            markets,
            data_dir,
            options_book_interval,
            futures_book_interval,
            ticker_mode,
            ticker_emit_policy,
            no_options,
            no_perpetuals,
            no_dated_futures,
            no_trades,
            no_combo_trades,
            no_indices,
            no_hourly_checkpoints,
            max_dte_days,
            zstd_level,
            flush_secs,
            rotation_secs,
            metrics_bind,
            json_logs,
            duration_secs,
        } => {
            let mut cfg = AppConfig::load(config.as_deref())?;
            if let Some(m) = markets {
                cfg.deribit.markets = m;
            }
            if let Some(d) = data_dir {
                cfg.storage.data_dir = d;
            }
            if let Some(i) = options_book_interval {
                cfg.deribit.options_book_interval = i;
            }
            if let Some(i) = futures_book_interval {
                cfg.deribit.futures_book_interval = i;
            }
            if let Some(m) = ticker_mode {
                cfg.deribit.ticker_mode = m;
            }
            if let Some(p) = ticker_emit_policy {
                cfg.deribit.ticker_emit_policy = p;
            }
            if no_options {
                cfg.deribit.collect_options = false;
            }
            if no_perpetuals {
                cfg.deribit.collect_perpetuals = false;
            }
            if no_dated_futures {
                cfg.deribit.collect_dated_futures = false;
            }
            if no_trades {
                cfg.deribit.collect_trades = false;
            }
            if no_combo_trades {
                cfg.deribit.collect_combo_trades = false;
            }
            if no_indices {
                cfg.deribit.collect_indices = false;
            }
            if no_hourly_checkpoints {
                cfg.storage.hourly_book_checkpoints = false;
            }
            if let Some(dte) = max_dte_days {
                cfg.deribit.max_dte_days = Some(dte);
            }
            if let Some(lvl) = zstd_level {
                cfg.storage.zstd_level = lvl;
            }
            if let Some(f) = flush_secs {
                cfg.storage.flush_interval_secs = f;
            }
            if let Some(r) = rotation_secs {
                cfg.storage.rotation_interval_secs = r;
            }
            if let Some(bind) = metrics_bind {
                cfg.telemetry.metrics_bind = bind;
            }
            if json_logs {
                cfg.telemetry.json_logs = true;
            }

            cfg.normalize();
            cfg.validate()?;
            init_logging(cfg.telemetry.json_logs);
            run_collect(cfg, duration_secs).await
        }

        Command::Instruments { config, markets } => {
            init_logging(false);
            let mut cfg = AppConfig::load(config.as_deref())?;
            if let Some(m) = markets {
                cfg.deribit.markets = m;
            }
            cfg.normalize();
            run_instruments(cfg).await
        }

        Command::Inspect { path } => {
            let report = inspect_path(&path)?;
            print_report(&report);
            Ok(())
        }

        Command::Replay {
            path,
            instrument,
            samples,
        } => {
            println!("Replaying {instrument} from {}...", path.display());
            let res = replay_instrument(&path, &instrument, samples)?;
            println!("\n=== Replay Integrity Summary ===");
            println!("Instrument:       {}", res.instrument);
            println!(
                "Book packets:     {} (snapshots={}, checkpoints={}, sequence_gaps={}, crossed_states={})",
                res.total_packets,
                res.snapshots,
                res.checkpoints,
                res.sequence_gaps,
                res.crossed_states
            );
            println!(
                "Final depth:      {} bids / {} asks",
                res.final_bid_levels, res.final_ask_levels
            );
            println!("Final best bid:   {:?}", res.final_best_bid);
            println!("Final best ask:   {:?}", res.final_best_ask);
            println!(
                "Tickers:          {} rows ({} OI changes)",
                res.ticker_rows, res.oi_changes
            );
            println!("Latest OI:        {:?}", res.latest_open_interest);
            println!("Latest mark_iv:   {:?}", res.latest_mark_iv);
            println!("Latest funding8h: {:?}", res.latest_funding_8h);
            Ok(())
        }

        Command::Compact {
            path,
            keep_originals,
            zstd_level,
        } => {
            init_logging(false);
            let cfg = StorageConfig {
                data_dir: path.clone(),
                compression: CompressionCodec::Zstd,
                zstd_level,
                ..StorageConfig::default()
            };
            let report = compact_directory(&path, &cfg, keep_originals)?;
            let ratio = if report.bytes_after > 0 {
                report.bytes_before as f64 / report.bytes_after as f64
            } else {
                1.0
            };
            println!(
                "Compacted {} partition(s): {} -> {} file(s), {} rows, {:.2} MiB -> {:.2} MiB ({:.2}x)",
                report.partitions_compacted,
                report.files_before,
                report.files_after,
                report.total_rows,
                report.bytes_before as f64 / (1024.0 * 1024.0),
                report.bytes_after as f64 / (1024.0 * 1024.0),
                ratio
            );
            Ok(())
        }
    }
}

fn init_logging(json: bool) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,deribit_tick_collector=info"));
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
    }
}

async fn run_instruments(cfg: AppConfig) -> anyhow::Result<()> {
    let discovery = InstrumentDiscovery::new(&cfg.deribit.rest_url)?;
    let registry = InstrumentRegistry::new();
    let catalog = discovery.fetch_catalog(&cfg.deribit, &registry).await?;

    println!(
        "{:<16} {:>10} {:>12} {:>14} {:>10}",
        "MARKET", "OPTIONS", "PERPETUALS", "DATED_FUTURES", "TOTAL"
    );
    println!("{}", "-".repeat(66));
    for (mkt, (o, p, f)) in catalog.summary_by_market() {
        println!("{mkt:<16} {o:>10} {p:>12} {f:>14} {:>10}", o + p + f);
    }
    println!("{}", "-".repeat(66));
    println!(
        "{:<16} {:>10} {:>12} {:>14} {:>10}",
        "TOTAL",
        catalog.options.len(),
        catalog.perpetuals.len(),
        catalog.dated_futures.len(),
        catalog.total_instruments()
    );

    let per_inst =
        usize::from(cfg.deribit.collect_orderbook) + usize::from(cfg.deribit.collect_ticker);
    let trade_channels = if cfg.deribit.collect_trades {
        catalog.settlement_currencies.len()
            * if cfg.deribit.collect_combo_trades {
                4
            } else {
                2
            }
    } else {
        0
    };
    let index_channels = if cfg.deribit.collect_indices {
        catalog.index_names.len() * 2
    } else {
        0
    };
    let est_channels = catalog.total_instruments() * per_inst + 1 + trade_channels + index_channels;
    let est_shards = 1
        + (catalog.perpetuals.len() + catalog.dated_futures.len())
            .saturating_mul(per_inst)
            .div_ceil((cfg.network.max_channels_per_conn / 2).max(20))
        + catalog
            .options
            .len()
            .saturating_mul(per_inst)
            .div_ceil((cfg.network.max_channels_per_conn * 90 / 100).max(2));
    println!(
        "\nActive combos tracked for trade metadata: {} | Index/DVOL streams: {} ({})",
        catalog.combos.len(),
        index_channels,
        catalog
            .index_names
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "Estimated subscription footprint: ~{est_channels} channels across ~{est_shards} WebSocket shards (Deribit IP limit: 32)"
    );
    Ok(())
}

async fn run_collect(cfg: AppConfig, duration_secs: Option<u64>) -> anyhow::Result<()> {
    let cfg = Arc::new(cfg);
    let registry = Arc::new(InstrumentRegistry::new());
    let telemetry = Arc::new(Telemetry::new());
    let discovery = Arc::new(InstrumentDiscovery::new(&cfg.deribit.rest_url)?);

    info!(
        markets = ?cfg.deribit.markets,
        options = cfg.deribit.collect_options,
        perpetuals = cfg.deribit.collect_perpetuals,
        dated_futures = cfg.deribit.collect_dated_futures,
        book_opt = cfg.deribit.options_book_interval.as_str(),
        book_fut = cfg.deribit.futures_book_interval.as_str(),
        ticker_mode = ?cfg.deribit.ticker_mode,
        trades = cfg.deribit.collect_trades,
        combo_trades = cfg.deribit.collect_combo_trades,
        indices = cfg.deribit.collect_indices,
        hourly_checkpoints = cfg.storage.hourly_book_checkpoints,
        auth = cfg.has_credentials(),
        data_dir = %cfg.storage.data_dir.display(),
        "starting deribit-tick-collector"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ingest_tx, ingest_rx) = mpsc::channel(cfg.network.ingest_channel_capacity);

    // Optional S3 uploader
    let (upload_tx, uploader_handle) = spawn_uploader(
        cfg.s3.clone(),
        cfg.storage.data_dir.clone(),
        Arc::clone(&telemetry),
    )?;

    // Optional local retention pruner
    let pruner_handle = spawn_retention_pruner(cfg.storage.clone(), shutdown_rx.clone());

    // Telemetry HTTP server + periodic summary logger
    let metrics_handle = if !cfg.telemetry.metrics_bind.trim().is_empty() {
        Some(spawn_metrics_server(
            cfg.telemetry.metrics_bind.clone(),
            Arc::clone(&telemetry),
            shutdown_rx.clone(),
        ))
    } else {
        None
    };
    let stats_handle = spawn_stats_logger(
        cfg.telemetry.stats_log_interval_secs,
        Arc::clone(&telemetry),
        shutdown_rx.clone(),
    );

    // Storage engine (owns SoA buffers + dedicated parquet-io OS thread)
    let storage_cfg = cfg.storage.clone();
    let storage_registry = Arc::clone(&registry);
    let storage_telemetry = Arc::clone(&telemetry);
    let storage_handle = tokio::spawn(async move {
        run_storage_engine(
            storage_cfg,
            storage_registry,
            storage_telemetry,
            ingest_rx,
            upload_tx,
        )
        .await
    });

    // WebSocket shard pool & lifecycle manager
    let mut manager = WsManager::new(
        Arc::clone(&cfg),
        Arc::clone(&registry),
        Arc::clone(&telemetry),
        Arc::clone(&discovery),
        ingest_tx.clone(),
        shutdown_rx.clone(),
    );
    manager
        .start()
        .await
        .context("failed to initialize Deribit WebSocket manager")?;

    let manager_handle = tokio::spawn(manager.run_lifecycle_loop());

    // Wait for SIGINT, SIGTERM, or optional --duration-secs timer
    wait_for_shutdown(duration_secs).await;

    info!("shutdown triggered; stopping WebSocket shards and finalizing Parquet segments...");
    let _ = shutdown_tx.send(true);

    let _ = manager_handle.await;
    // Drop our last ingest sender so the storage engine drains & finalizes every open file!
    drop(ingest_tx);

    match storage_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = %e, "storage engine exited with error"),
        Err(e) => tracing::error!(error = %e, "storage engine task panicked"),
    }

    if let Some(h) = uploader_handle {
        let _ = h.await;
    }
    if let Some(h) = pruner_handle {
        let _ = h.await;
    }
    if let Some(h) = metrics_handle {
        let _ = h.await;
    }
    let _ = stats_handle.await;

    telemetry.log_summary();
    info!("deribit-tick-collector shut down cleanly");
    Ok(())
}

async fn wait_for_shutdown(duration_secs: Option<u64>) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let sigterm = async {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sig) = signal(SignalKind::terminate()) {
            sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    let timer = async {
        match duration_secs {
            Some(secs) => tokio::time::sleep(Duration::from_secs(secs)).await,
            None => std::future::pending::<()>().await,
        }
    };

    tokio::select! {
        _ = ctrl_c => info!("received SIGINT"),
        _ = sigterm => info!("received SIGTERM"),
        _ = timer => info!("capture duration reached"),
    }
}
