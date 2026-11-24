use deribit_tick_collector::deribit::protocol::{ParsedWsMessage, parse_ws_frame};
use deribit_tick_collector::model::{
    BookAction, BookSide, InstrumentId, LiquidationSide, TickerState, TradeDirection, ticker_mask,
};

#[test]
fn parses_live_deribit_book_snapshot_and_change() {
    let snap_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "book.BTC-PERPETUAL.raw",
            "data": {
                "timestamp": 1795541685655,
                "type": "snapshot",
                "change_id": 184368776562,
                "instrument_name": "BTC-PERPETUAL",
                "bids": [["new", 59977.5, 15050.0], ["new", 59977.0, 8870.0]],
                "asks": [["new", 59978.0, 23800.0]]
            }
        }
    }"#;

    let msg = parse_ws_frame(snap_json).expect("parse snapshot");
    match msg {
        ParsedWsMessage::Book { channel, data } => {
            assert_eq!(channel, "book.BTC-PERPETUAL.raw");
            assert!(data.is_snapshot());
            assert_eq!(data.change_id, 184368776562);
            assert_eq!(data.prev_change_id, None);
            let lvls = data.into_levels();
            assert_eq!(lvls.len(), 3);
            assert_eq!(lvls[0].side, BookSide::Bid);
            assert_eq!(lvls[0].action, BookAction::New);
            assert_eq!(lvls[0].price, 59977.5);
            assert_eq!(lvls[0].amount, 15050.0);
            assert_eq!(lvls[2].side, BookSide::Ask);
            assert_eq!(lvls[2].price, 59978.0);
        }
        other => panic!("unexpected variant: {other:?}"),
    }

    let change_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "book.BTC-25NOV26.100ms",
            "data": {
                "timestamp": 1795541685855,
                "type": "change",
                "change_id": 184368776820,
                "prev_change_id": 184368776212,
                "instrument_name": "BTC-25NOV26",
                "bids": [["change", 59962.5, 25500.0]],
                "asks": [["delete", 59970.0, 0.0]]
            }
        }
    }"#;

    match parse_ws_frame(change_json).expect("parse change") {
        ParsedWsMessage::Book { data, .. } => {
            assert!(!data.is_snapshot());
            assert_eq!(data.change_id, 184368776820);
            assert_eq!(data.prev_change_id, Some(184368776212));
            let lvls = data.into_levels();
            assert_eq!(lvls[0].action, BookAction::Change);
            assert_eq!(lvls[1].action, BookAction::Delete);
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[test]
fn materializes_incremental_ticker_and_tracks_open_interest_and_null_clears() {
    let snap_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "incremental_ticker.BTC-4DEC26-71000-C",
            "data": {
                "timestamp": 1795541684248,
                "type": "snapshot",
                "state": "open",
                "instrument_name": "BTC-4DEC26-71000-C",
                "open_interest": 564.5,
                "mark_price": 0.001,
                "index_price": 59962.37,
                "underlying_index": "BTC-4DEC26",
                "underlying_price": 60020.67,
                "mark_iv": 54.4,
                "bid_iv": 53.04,
                "ask_iv": 54.84,
                "best_bid_price": 0.0009,
                "best_bid_amount": 2.9,
                "best_ask_price": 0.0011,
                "best_ask_amount": 30.0,
                "greeks": {"delta": 0.03145, "gamma": 1e-05, "vega": 6.88819, "theta": -19.51629, "rho": 0.48002},
                "stats": {"high": 0.0016, "low": 0.001, "volume": 3.9, "volume_usd": 356.45}
            }
        }
    }"#;

    let mut state = TickerState::new(InstrumentId(7));

    let ParsedWsMessage::Ticker {
        is_incremental_channel,
        patch,
        ..
    } = parse_ws_frame(snap_json).unwrap()
    else {
        panic!("expected ticker");
    };
    assert!(is_incremental_channel);
    let (row1, mask1, is_snap1) = state.apply(&patch, 1_000, true);
    assert!(is_snap1);
    assert_eq!(row1.open_interest, Some(564.5));
    assert_eq!(row1.mark_iv, Some(54.4));
    assert_eq!(row1.delta, Some(0.03145));
    assert_eq!(row1.best_bid_price, Some(0.0009));
    assert_ne!(mask1 & ticker_mask::OPEN_INTEREST, 0);

    // Delta 1: Greeks & underlying update, OI omitted -> OI MUST carry forward unchanged, OI bit NOT set!
    let delta1_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "incremental_ticker.BTC-4DEC26-71000-C",
            "data": {
                "timestamp": 1795541685255,
                "type": "change",
                "instrument_name": "BTC-4DEC26-71000-C",
                "greeks": {"vega": 6.88744},
                "underlying_price": 60020.54
            }
        }
    }"#;
    let ParsedWsMessage::Ticker { patch: p2, .. } = parse_ws_frame(delta1_json).unwrap() else {
        panic!("expected ticker");
    };
    let (row2, mask2, is_snap2) = state.apply(&p2, 2_000, true);
    assert!(!is_snap2);
    assert_eq!(row2.open_interest, Some(564.5));
    assert_eq!(row2.delta, Some(0.03145));
    assert_eq!(row2.vega, Some(6.88744));
    assert_eq!(row2.underlying_price, Some(60020.54));
    assert_eq!(mask2 & ticker_mask::OPEN_INTEREST, 0);
    assert_ne!(mask2 & ticker_mask::GREEKS, 0);
    assert_ne!(mask2 & ticker_mask::UNDERLYING_PRICE, 0);

    // Delta 2: OI increases AND best_bid_price explicitly cleared to null!
    let delta2_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "incremental_ticker.BTC-4DEC26-71000-C",
            "data": {
                "timestamp": 1795541686255,
                "type": "change",
                "instrument_name": "BTC-4DEC26-71000-C",
                "open_interest": 574.5,
                "best_bid_price": null,
                "best_bid_amount": 0.0
            }
        }
    }"#;
    let ParsedWsMessage::Ticker { patch: p3, .. } = parse_ws_frame(delta2_json).unwrap() else {
        panic!("expected ticker");
    };
    let (row3, mask3, _) = state.apply(&p3, 3_000, true);
    assert_eq!(row3.open_interest, Some(574.5));
    assert_eq!(row3.best_bid_price, None);
    assert_eq!(row3.best_bid_amount, Some(0.0));
    assert_ne!(mask3 & ticker_mask::OPEN_INTEREST, 0);
    assert_ne!(mask3 & ticker_mask::BBO, 0);
}

#[test]
fn parses_trades_heartbeat_and_instrument_state() {
    let trade_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "trades.future.BTC.raw",
            "data": [{
                "timestamp": 1795541685825,
                "price": 59978.5,
                "direction": "buy",
                "index_price": 59962.3,
                "instrument_name": "BTC-PERPETUAL",
                "trade_seq": 327398657,
                "amount": 5070.0,
                "mark_price": 59978.64,
                "tick_direction": 0,
                "trade_id": "475000582",
                "contracts": 507.0,
                "liquidation": "T"
            }]
        }
    }"#;
    match parse_ws_frame(trade_json).unwrap() {
        ParsedWsMessage::Trades { trades, .. } => {
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].parsed_direction(), TradeDirection::Buy);
            assert_eq!(trades[0].parsed_liquidation(), LiquidationSide::Taker);
            assert_eq!(trades[0].trade_seq, 327398657);
        }
        other => panic!("unexpected: {other:?}"),
    }

    let hb_json = r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#;
    match parse_ws_frame(hb_json).unwrap() {
        ParsedWsMessage::Heartbeat { kind } => assert_eq!(kind, "test_request"),
        other => panic!("unexpected: {other:?}"),
    }

    let state_json = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "instrument.state.any.any",
            "data": {
                "timestamp": 1795541690000,
                "instrument_name": "BTC-28NOV26-65000-C",
                "state": "created"
            }
        }
    }"#;
    match parse_ws_frame(state_json).unwrap() {
        ParsedWsMessage::InstrumentState { data, .. } => {
            assert_eq!(data.instrument_name, "BTC-28NOV26-65000-C");
            assert_eq!(data.state, "created");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn parses_spot_price_index_dvol_and_combo_block_trades() {
    let px_idx = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "deribit_price_index.btc_usd",
            "data": {"timestamp": 1795551733929, "price": 59650.47, "index_name": "btc_usd"}
        }
    }"#;
    match parse_ws_frame(px_idx).unwrap() {
        ParsedWsMessage::PriceIndex { data, .. } => {
            assert_eq!(data.index_name, "btc_usd");
            assert_eq!(data.price, 59650.47);
        }
        other => panic!("unexpected: {other:?}"),
    }

    let dvol = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "deribit_volatility_index.eth_usd",
            "data": {"timestamp": 1795551734043, "index_name": "eth_usd", "volatility": 66.99}
        }
    }"#;
    match parse_ws_frame(dvol).unwrap() {
        ParsedWsMessage::VolatilityIndex { data, .. } => {
            assert_eq!(data.index_name, "eth_usd");
            assert_eq!(data.volatility, 66.99);
        }
        other => panic!("unexpected: {other:?}"),
    }

    let combo_trade = r#"{
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": "trades.option_combo.BTC.raw",
            "data": [{
                "timestamp": 1795551735000,
                "price": 0.0125,
                "direction": "sell",
                "index_price": 59650.47,
                "instrument_name": "BTC-CCAL-26NOV26_25NOV26-57500",
                "trade_seq": 102,
                "amount": 25.0,
                "tick_direction": 2,
                "trade_id": "C-88123",
                "block_trade_id": "BLK-42",
                "block_trade_leg_count": 2,
                "combo_id": "BTC-CCAL-26NOV26_25NOV26-57500",
                "combo_trade_id": "CT-99"
            }]
        }
    }"#;
    match parse_ws_frame(combo_trade).unwrap() {
        ParsedWsMessage::Trades { channel, trades } => {
            assert_eq!(channel, "trades.option_combo.BTC.raw");
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].block_trade_id, Some("BLK-42"));
            assert_eq!(trades[0].block_trade_leg_count, Some(2));
            assert_eq!(trades[0].combo_id, Some("BTC-CCAL-26NOV26_25NOV26-57500"));
            assert_eq!(trades[0].combo_trade_id, Some("CT-99"));
        }
        other => panic!("unexpected: {other:?}"),
    }
}
