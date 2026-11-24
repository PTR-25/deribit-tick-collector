use std::sync::Arc;

use smallvec::smallvec;
use tempfile::tempdir;
use tokio::sync::mpsc;

use deribit_tick_collector::config::StorageConfig;
use deribit_tick_collector::model::{
    BookAction, BookLevelDelta, BookSide, BookUpdateEvent, CaptureEvent, IndexKind, IndexRow,
    InstrumentId, InstrumentKind, InstrumentMeta, LiquidationSide, LiveBook, MarketEvent,
    OptionType, TickerRow, TradeDirection, TradeRow, ticker_mask,
};
use deribit_tick_collector::registry::InstrumentRegistry;
use deribit_tick_collector::storage::compact::compact_directory;
use deribit_tick_collector::storage::writer::run_storage_engine;
use deribit_tick_collector::telemetry::Telemetry;
use deribit_tick_collector::tools::inspect::inspect_path;
use deribit_tick_collector::tools::replay::replay_instrument;

fn sample_meta(name: &str, market: &str, kind: InstrumentKind) -> InstrumentMeta {
    InstrumentMeta {
        id: InstrumentId(0),
        deribit_instrument_id: Some(12345),
        instrument_name: Arc::from(name),
        market: Arc::from(market),
        base_currency: Arc::from("BTC"),
        quote_currency: Arc::from("USD"),
        settlement_currency: Arc::from(if market.ends_with("USDC") {
            "USDC"
        } else {
            "BTC"
        }),
        settlement_period: Arc::from(match kind {
            InstrumentKind::Perpetual => "perpetual",
            _ => "month",
        }),
        kind,
        creation_ts_ms: 1_700_000_000_000,
        expiration_ts_ms: 1_795_593_600_000,
        strike: if kind == InstrumentKind::Option {
            Some(70_000.0)
        } else {
            None
        },
        option_type: if kind == InstrumentKind::Option {
            Some(OptionType::Call)
        } else {
            None
        },
        tick_size: 0.5,
        min_trade_amount: 10.0,
        contract_size: 10.0,
        maker_commission: 0.0,
        taker_commission: 0.0005,
        is_active: true,
        state: Arc::from("open"),
        recorded_ts_ns: 1_795_541_685_000_000_000,
    }
}

#[tokio::test]
async fn end_to_end_write_inspect_replay_and_compact() {
    let dir = tempdir().expect("tempdir");
    let registry = Arc::new(InstrumentRegistry::new());
    let telemetry = Arc::new(Telemetry::new());

    let perp_meta = sample_meta("BTC-PERPETUAL", "BTC", InstrumentKind::Perpetual);
    let fut_meta = sample_meta("BTC-25DEC26", "BTC", InstrumentKind::Future);
    let opt_meta = sample_meta("BTC-25DEC26-70000-C", "BTC", InstrumentKind::Option);
    let lin_opt_meta = sample_meta(
        "BTC_USDC-25DEC26-70000-C",
        "BTC_USDC",
        InstrumentKind::Option,
    );

    let perp_id = registry.upsert(perp_meta.clone());
    let fut_id = registry.upsert(fut_meta.clone());
    let opt_id = registry.upsert(opt_meta.clone());
    let lin_id = registry.upsert(lin_opt_meta.clone());

    let cfg = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_row_group: 100,
        flush_interval_secs: 1,
        rotation_interval_secs: 60,
        ..StorageConfig::default()
    };

    let (tx, rx) = mpsc::channel(1024);
    let writer_task = tokio::spawn(run_storage_engine(
        cfg.clone(),
        Arc::clone(&registry),
        Arc::clone(&telemetry),
        rx,
        None,
    ));

    // Persist catalog + capture event
    tx.send(MarketEvent::Instrument(perp_meta)).await.unwrap();
    tx.send(MarketEvent::Instrument(fut_meta)).await.unwrap();
    tx.send(MarketEvent::Instrument(opt_meta)).await.unwrap();
    tx.send(MarketEvent::Instrument(lin_opt_meta))
        .await
        .unwrap();
    tx.send(MarketEvent::Capture(CaptureEvent {
        recorded_ts_ns: 1_795_541_685_000_000_000,
        shard_id: 0,
        event_type: Arc::from("startup"),
        instrument_name: None,
        detail: "test".into(),
    }))
    .await
    .unwrap();

    // Send continuous snapshot + 2 deltas for BTC-PERPETUAL
    let base_ms = 1_795_541_685_000i64;
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: base_ms * 1_000_000 + 100,
        exchange_ts_ms: base_ms,
        instrument_id: perp_id,
        is_snapshot: true,
        is_checkpoint: false,
        change_id: 1000,
        prev_change_id: None,
        levels: smallvec![
            BookLevelDelta {
                side: BookSide::Bid,
                action: BookAction::New,
                price: 60000.0,
                amount: 1000.0
            },
            BookLevelDelta {
                side: BookSide::Bid,
                action: BookAction::New,
                price: 59999.5,
                amount: 2000.0
            },
            BookLevelDelta {
                side: BookSide::Ask,
                action: BookAction::New,
                price: 60000.5,
                amount: 1500.0
            },
            BookLevelDelta {
                side: BookSide::Ask,
                action: BookAction::New,
                price: 60001.0,
                amount: 3000.0
            },
        ],
    }))
    .await
    .unwrap();

    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: (base_ms + 50) * 1_000_000,
        exchange_ts_ms: base_ms + 50,
        instrument_id: perp_id,
        is_snapshot: false,
        is_checkpoint: false,
        change_id: 1001,
        prev_change_id: Some(1000),
        levels: smallvec![
            BookLevelDelta {
                side: BookSide::Bid,
                action: BookAction::Change,
                price: 60000.0,
                amount: 2500.0
            },
            BookLevelDelta {
                side: BookSide::Ask,
                action: BookAction::Delete,
                price: 60000.5,
                amount: 0.0
            },
        ],
    }))
    .await
    .unwrap();

    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: (base_ms + 90) * 1_000_000,
        exchange_ts_ms: base_ms + 90,
        instrument_id: perp_id,
        is_snapshot: false,
        is_checkpoint: false,
        change_id: 1002,
        prev_change_id: Some(1001),
        levels: smallvec![BookLevelDelta {
            side: BookSide::Ask,
            action: BookAction::New,
            price: 60000.5,
            amount: 800.0
        },],
    }))
    .await
    .unwrap();

    // Also send book snapshots for dated future, inverse option, and linear USDC option
    for (id, px) in [(fut_id, 61000.0), (opt_id, 0.045), (lin_id, 2700.0)] {
        tx.send(MarketEvent::Book(BookUpdateEvent {
            received_ts_ns: base_ms * 1_000_000,
            exchange_ts_ms: base_ms,
            instrument_id: id,
            is_snapshot: true,
            is_checkpoint: false,
            change_id: 500,
            prev_change_id: None,
            levels: smallvec![
                BookLevelDelta {
                    side: BookSide::Bid,
                    action: BookAction::New,
                    price: px,
                    amount: 10.0
                },
                BookLevelDelta {
                    side: BookSide::Ask,
                    action: BookAction::New,
                    price: px + 1.0,
                    amount: 12.0
                },
            ],
        }))
        .await
        .unwrap();
    }

    // Send ticker rows with OI change
    tx.send(MarketEvent::Ticker(Box::new(TickerRow {
        received_ts_ns: base_ms * 1_000_000,
        exchange_ts_ms: base_ms,
        instrument_id: perp_id,
        is_snapshot: true,
        changed_mask: ticker_mask::OPEN_INTEREST | ticker_mask::FUNDING,
        state: Arc::from("open"),
        open_interest: Some(850_000_000.0),
        mark_price: Some(60000.25),
        index_price: Some(59995.0),
        last_price: Some(60000.0),
        settlement_price: Some(60100.0),
        estimated_delivery_price: Some(59995.0),
        min_price: Some(59000.0),
        max_price: Some(61000.0),
        best_bid_price: Some(60000.0),
        best_bid_amount: Some(2500.0),
        best_ask_price: Some(60000.5),
        best_ask_amount: Some(800.0),
        volume: Some(35000.0),
        volume_usd: Some(2_100_000_000.0),
        volume_notional: Some(2_100_000_000.0),
        high: Some(61500.0),
        low: Some(59100.0),
        price_change: Some(-1.5),
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
        current_funding: Some(0.00002),
        funding_8h: Some(0.0001),
        interest_value: Some(0.0003),
    })))
    .await
    .unwrap();

    // Send trade row
    tx.send(MarketEvent::Trade(TradeRow {
        received_ts_ns: base_ms * 1_000_000,
        exchange_ts_ms: base_ms,
        instrument_id: opt_id,
        trade_id: "999001".to_string(),
        trade_seq: 42,
        price: 0.046,
        amount: 5.0,
        contracts: Some(5.0),
        direction: TradeDirection::Buy,
        tick_direction: 0,
        iv: Some(58.2),
        index_price: Some(60000.0),
        mark_price: Some(0.0455),
        liquidation: LiquidationSide::None,
        block_trade_id: Some("BLK-7".to_string()),
        block_trade_leg_count: Some(2),
        combo_id: Some("BTC-STRG-25DEC26".to_string()),
        combo_trade_id: Some("CT-7".to_string()),
    }))
    .await
    .unwrap();

    // Send spot price index + DVOL volatility index rows
    tx.send(MarketEvent::Index(IndexRow {
        received_ts_ns: base_ms * 1_000_000,
        exchange_ts_ms: base_ms,
        index_name: Arc::from("btc_usd"),
        kind: IndexKind::Price,
        value: 59995.0,
    }))
    .await
    .unwrap();
    tx.send(MarketEvent::Index(IndexRow {
        received_ts_ns: base_ms * 1_000_000 + 1000,
        exchange_ts_ms: base_ms + 1,
        index_name: Arc::from("btc_usd"),
        kind: IndexKind::Volatility,
        value: 53.64,
    }))
    .await
    .unwrap();

    // Close sender to trigger graceful flush & atomic finalization
    drop(tx);
    writer_task.await.unwrap().unwrap();

    // Verify inverse BTC and linear BTC_USDC are stored in separate Hive partitions!
    let report = inspect_path(dir.path()).expect("inspect");
    assert!(report.partitions.contains_key("book_updates/BTC/perpetual"));
    assert!(report.partitions.contains_key("book_updates/BTC/future"));
    assert!(report.partitions.contains_key("book_updates/BTC/option"));
    assert!(
        report
            .partitions
            .contains_key("book_updates/BTC_USDC/option")
    );
    assert!(report.partitions.contains_key("tickers/BTC/perpetual"));
    assert!(report.partitions.contains_key("trades/BTC/option"));
    assert!(report.partitions.contains_key("indices"));
    assert_eq!(report.partitions["indices"].rows, 2);
    assert!(report.partitions.contains_key("instruments"));
    assert!(report.partitions.contains_key("capture_events"));

    // Verify zero gaps & exact L2 reconstruction via replay
    let replay = replay_instrument(dir.path(), "BTC-PERPETUAL", 0).expect("replay");
    assert_eq!(replay.total_packets, 3);
    assert_eq!(replay.snapshots, 1);
    assert_eq!(replay.checkpoints, 0);
    assert_eq!(replay.sequence_gaps, 0);
    assert_eq!(replay.crossed_states, 0);
    assert_eq!(replay.final_bid_levels, 2);
    assert_eq!(replay.final_ask_levels, 2);
    assert_eq!(replay.final_best_bid, Some((60000.0, 2500.0)));
    assert_eq!(replay.final_best_ask, Some((60000.5, 800.0)));
    assert_eq!(replay.ticker_rows, 1);
    assert_eq!(replay.oi_changes, 1);
    assert_eq!(replay.latest_open_interest, Some(850_000_000.0));
    assert_eq!(replay.latest_funding_8h, Some(0.0001));

    // Verify compactor runs cleanly
    let compact_rep = compact_directory(dir.path(), &cfg, false).expect("compact");
    assert_eq!(compact_rep.partitions_compacted, 0); // single segment per partition
}

#[tokio::test]
async fn hourly_checkpoints_make_each_hour_partition_independently_replayable() {
    let dir = tempdir().expect("tempdir");
    let registry = Arc::new(InstrumentRegistry::new());
    let telemetry = Arc::new(Telemetry::new());

    let active_meta = sample_meta("BTC-PERPETUAL", "BTC", InstrumentKind::Perpetual);
    let quiet_meta = sample_meta("BTC-25DEC26-70000-C", "BTC", InstrumentKind::Option);
    let active_id = registry.upsert(active_meta);
    let quiet_id = registry.upsert(quiet_meta);

    let cfg = StorageConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_row_group: 100,
        flush_interval_secs: 1,
        rotation_interval_secs: 60,
        hourly_book_checkpoints: true,
        ..StorageConfig::default()
    };

    let (tx, rx) = mpsc::channel(1024);
    let writer_task = tokio::spawn(run_storage_engine(
        cfg,
        Arc::clone(&registry),
        Arc::clone(&telemetry),
        rx,
        None,
    ));

    // Exact hour boundary: 2026-11-24 17:59:59.000 UTC -> 2026-11-24 18:00:00.000 UTC
    let h18_start_ms: i64 = 1_795_543_200_000;
    let h17_ts_ms = h18_start_ms - 1_000;

    // 1) Active instrument (Trigger A): snapshot in hour 17, then first delta in hour 18 promoted
    //    to a full self-contained checkpoint, followed by a regular delta in hour 18.
    let init_levels = smallvec![
        BookLevelDelta {
            side: BookSide::Bid,
            action: BookAction::New,
            price: 60000.0,
            amount: 100.0
        },
        BookLevelDelta {
            side: BookSide::Bid,
            action: BookAction::New,
            price: 59999.0,
            amount: 200.0
        },
        BookLevelDelta {
            side: BookSide::Ask,
            action: BookAction::New,
            price: 60001.0,
            amount: 150.0
        },
    ];
    let mut live_active = LiveBook::from_snapshot(100, h17_ts_ms, &init_levels);
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: h17_ts_ms * 1_000_000,
        exchange_ts_ms: h17_ts_ms,
        instrument_id: active_id,
        is_snapshot: true,
        is_checkpoint: false,
        change_id: 100,
        prev_change_id: None,
        levels: init_levels,
    }))
    .await
    .unwrap();

    // First message in hour 18 (cid=101, prev=100): Trigger A promotes to full checkpoint snapshot!
    let delta_101 = [BookLevelDelta {
        side: BookSide::Bid,
        action: BookAction::Change,
        price: 60000.0,
        amount: 450.0,
    }];
    live_active.apply_delta(101, h18_start_ms + 200, &delta_101);
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: (h18_start_ms + 200) * 1_000_000,
        exchange_ts_ms: h18_start_ms + 200,
        instrument_id: active_id,
        is_snapshot: true,
        is_checkpoint: true,
        change_id: 101,
        prev_change_id: Some(100),
        levels: live_active.snapshot_levels(),
    }))
    .await
    .unwrap();

    // Second message in hour 18 (cid=102, prev=101): regular delta
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: (h18_start_ms + 500) * 1_000_000,
        exchange_ts_ms: h18_start_ms + 500,
        instrument_id: active_id,
        is_snapshot: false,
        is_checkpoint: false,
        change_id: 102,
        prev_change_id: Some(101),
        levels: smallvec![BookLevelDelta {
            side: BookSide::Ask,
            action: BookAction::New,
            price: 60000.5,
            amount: 75.0
        },],
    }))
    .await
    .unwrap();

    // 2) Quiet option (Trigger B): snapshot in hour 17 (cid=500), quiet anchor at 18:00:00 (cid=500),
    //    then later delta at 18:20:00 (cid=501, prev=500).
    let quiet_init = smallvec![
        BookLevelDelta {
            side: BookSide::Bid,
            action: BookAction::New,
            price: 0.04,
            amount: 5.0
        },
        BookLevelDelta {
            side: BookSide::Ask,
            action: BookAction::New,
            price: 0.05,
            amount: 8.0
        },
    ];
    let live_quiet = LiveBook::from_snapshot(500, h17_ts_ms, &quiet_init);
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: h17_ts_ms * 1_000_000,
        exchange_ts_ms: h17_ts_ms,
        instrument_id: quiet_id,
        is_snapshot: true,
        is_checkpoint: false,
        change_id: 500,
        prev_change_id: None,
        levels: quiet_init,
    }))
    .await
    .unwrap();
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: (h18_start_ms + 2_000) * 1_000_000,
        exchange_ts_ms: h18_start_ms,
        instrument_id: quiet_id,
        is_snapshot: true,
        is_checkpoint: true,
        change_id: 500,
        prev_change_id: None,
        levels: live_quiet.snapshot_levels(),
    }))
    .await
    .unwrap();
    tx.send(MarketEvent::Book(BookUpdateEvent {
        received_ts_ns: (h18_start_ms + 1_200_000) * 1_000_000,
        exchange_ts_ms: h18_start_ms + 1_200_000,
        instrument_id: quiet_id,
        is_snapshot: false,
        is_checkpoint: false,
        change_id: 501,
        prev_change_id: Some(500),
        levels: smallvec![BookLevelDelta {
            side: BookSide::Bid,
            action: BookAction::Change,
            price: 0.04,
            amount: 12.5
        },],
    }))
    .await
    .unwrap();

    drop(tx);
    writer_task.await.unwrap().unwrap();

    // Replay ONLY hour=18 for BOTH instruments -> must reconstruct exact full books with 0 gaps!
    let h18_perp_dir = dir
        .path()
        .join("dataset=book_updates/currency=BTC/kind=perpetual/date=2026-11-24/hour=18");
    let h18_opt_dir = dir
        .path()
        .join("dataset=book_updates/currency=BTC/kind=option/date=2026-11-24/hour=18");

    let active_h18 = replay_instrument(&h18_perp_dir, "BTC-PERPETUAL", 0).unwrap();
    let active_all = replay_instrument(dir.path(), "BTC-PERPETUAL", 0).unwrap();
    assert_eq!(active_h18.sequence_gaps, 0);
    assert_eq!(active_all.sequence_gaps, 0);
    assert_eq!(active_h18.checkpoints, 1);
    assert_eq!(active_h18.final_bid_levels, 2);
    assert_eq!(active_h18.final_ask_levels, 2);
    assert_eq!(active_h18.final_best_bid, Some((60000.0, 450.0)));
    assert_eq!(active_h18.final_best_ask, Some((60000.5, 75.0)));
    assert_eq!(active_h18.final_best_bid, active_all.final_best_bid);
    assert_eq!(active_h18.final_best_ask, active_all.final_best_ask);

    let quiet_h18 = replay_instrument(&h18_opt_dir, "BTC-25DEC26-70000-C", 0).unwrap();
    let quiet_all = replay_instrument(dir.path(), "BTC-25DEC26-70000-C", 0).unwrap();
    assert_eq!(quiet_h18.sequence_gaps, 0);
    assert_eq!(quiet_all.sequence_gaps, 0);
    assert_eq!(quiet_h18.checkpoints, 1);
    assert_eq!(quiet_all.checkpoints, 1);
    assert_eq!(quiet_h18.final_best_bid, Some((0.04, 12.5)));
    assert_eq!(quiet_h18.final_best_ask, Some((0.05, 8.0)));
    assert_eq!(quiet_h18.final_best_bid, quiet_all.final_best_bid);
    assert_eq!(quiet_h18.final_best_ask, quiet_all.final_best_ask);

    let full_inspect = inspect_path(dir.path()).unwrap();
    assert_eq!(
        full_inspect.partitions["book_updates/BTC/perpetual"].book_gaps,
        0
    );
    assert_eq!(
        full_inspect.partitions["book_updates/BTC/perpetual"].book_checkpoints,
        1
    );
    assert_eq!(
        full_inspect.partitions["book_updates/BTC/option"].book_gaps,
        0
    );
    assert_eq!(
        full_inspect.partitions["book_updates/BTC/option"].book_checkpoints,
        1
    );
}
