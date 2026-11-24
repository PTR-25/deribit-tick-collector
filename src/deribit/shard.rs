use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use rustc_hash::{FxHashMap, FxHashSet};
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::{AppConfig, TickerEmitPolicy};
use crate::deribit::protocol::{
    ParsedWsMessage, WireAuthResult, build_auth_client_credentials, build_auth_refresh,
    build_set_heartbeat, build_subscribe, build_test_request, build_unsubscribe, parse_ws_frame,
};
use crate::error::{CollectorError, Result};
use crate::model::{
    BookUpdateEvent, CaptureEvent, IndexKind, IndexRow, InstrumentId, InstrumentKind, LiveBook,
    MarketEvent, TickerState, TradeRow, now_utc_ns, ticker_mask, utc_hour_index,
};
use crate::registry::InstrumentRegistry;
use crate::telemetry::Telemetry;

/// Control commands dispatched from `WsManager` to a running shard for zero-downtime updates.
#[derive(Debug)]
pub enum ShardCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

/// Real-time instrument lifecycle update forwarded from the lifecycle shard to `WsManager`.
#[derive(Debug, Clone)]
pub struct InstrumentLifecycleNotice {
    pub instrument_name: String,
    pub state: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRole {
    LifecycleAndTrades,
    FuturesAndPerps,
    Options,
}

impl ShardRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleAndTrades => "lifecycle",
            Self::FuturesAndPerps => "futures",
            Self::Options => "options",
        }
    }
}

pub struct ShardConfig {
    pub shard_id: u16,
    pub role: ShardRole,
    pub initial_channels: Vec<String>,
    pub app: Arc<AppConfig>,
    pub registry: Arc<InstrumentRegistry>,
    pub telemetry: Arc<Telemetry>,
    pub ingest_tx: mpsc::Sender<MarketEvent>,
    pub lifecycle_tx: Option<mpsc::Sender<InstrumentLifecycleNotice>>,
    pub cmd_rx: mpsc::Receiver<ShardCommand>,
    pub shutdown: watch::Receiver<bool>,
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Run a single resilient WebSocket shard until shutdown.
pub async fn run_shard(mut cfg: ShardConfig) {
    let shard_id = cfg.shard_id;
    let role = cfg.role.as_str();
    let mut channels: BTreeSet<String> = cfg.initial_channels.drain(..).collect();

    cfg.telemetry
        .subscribed_channels
        .fetch_add(channels.len(), Ordering::Relaxed);

    let mut backoff_ms: u64 = 500;
    let max_backoff_ms: u64 = 30_000;

    loop {
        if *cfg.shutdown.borrow() {
            break;
        }

        // Drain any queued channel mutations before connecting so reconnects use the freshest set.
        drain_pending_commands(&mut cfg.cmd_rx, &mut channels, &cfg.telemetry);

        match connect_tuned_ws(&cfg.app.deribit.ws_url, cfg.app.network.tcp_recv_buffer_kib).await {
            Ok(ws) => {
                backoff_ms = 500;
                cfg.telemetry.active_shards.fetch_add(1, Ordering::Relaxed);
                info!(
                    shard_id,
                    role,
                    channels = channels.len(),
                    "WebSocket shard connected"
                );
                emit_capture_event(
                    &cfg.ingest_tx,
                    &cfg.telemetry,
                    shard_id,
                    "shard_connected",
                    None,
                    format!("role={role} channels={}", channels.len()),
                );

                let session_res = run_connected_session(ws, &mut cfg, &mut channels).await;

                cfg.telemetry.active_shards.fetch_sub(1, Ordering::Relaxed);

                if *cfg.shutdown.borrow() {
                    info!(shard_id, role, "WebSocket shard stopped cleanly");
                    break;
                }

                cfg.telemetry.ws_reconnects.fetch_add(1, Ordering::Relaxed);
                let reason = match session_res {
                    Ok(()) => "stream closed by peer".to_string(),
                    Err(e) => e.to_string(),
                };
                warn!(shard_id, role, reason = %reason, "WebSocket shard disconnected; reconnecting");
                emit_capture_event(
                    &cfg.ingest_tx,
                    &cfg.telemetry,
                    shard_id,
                    "shard_disconnected",
                    None,
                    reason,
                );
            }
            Err(e) => {
                cfg.telemetry.ws_reconnects.fetch_add(1, Ordering::Relaxed);
                error!(
                    shard_id,
                    role,
                    error = %e,
                    backoff_ms,
                    "failed to connect WebSocket shard"
                );
            }
        }

        if *cfg.shutdown.borrow() {
            break;
        }

        // Exponential backoff with full jitter to avoid thundering-herd reconnect storms.
        let jitter = rand::rng().random_range(0..=(backoff_ms / 2).max(1));
        let wait = Duration::from_millis(backoff_ms + jitter);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = cfg.shutdown.changed() => {
                if *cfg.shutdown.borrow() {
                    break;
                }
            }
        }
        backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
    }
}

fn drain_pending_commands(
    cmd_rx: &mut mpsc::Receiver<ShardCommand>,
    channels: &mut BTreeSet<String>,
    telemetry: &Telemetry,
) {
    while let Ok(cmd) = cmd_rx.try_recv() {
        match cmd {
            ShardCommand::Subscribe(list) => {
                let mut added = 0usize;
                for ch in list {
                    if channels.insert(ch) {
                        added += 1;
                    }
                }
                telemetry
                    .subscribed_channels
                    .fetch_add(added, Ordering::Relaxed);
            }
            ShardCommand::Unsubscribe(list) => {
                let mut removed = 0usize;
                for ch in list {
                    if channels.remove(&ch) {
                        removed += 1;
                    }
                }
                telemetry
                    .subscribed_channels
                    .fetch_sub(removed, Ordering::Relaxed);
            }
        }
    }
}

/// Open a low-latency, large-buffer TCP socket (TCP_NODELAY + tuned SO_RCVBUF + keepalive)
/// and perform the TLS + WebSocket handshake.
async fn connect_tuned_ws(ws_url: &str, recv_buf_kib: usize) -> Result<WsStream> {
    let url = Url::parse(ws_url)
        .map_err(|e| CollectorError::Config(format!("invalid ws_url '{ws_url}': {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| CollectorError::Config(format!("ws_url missing host: {ws_url}")))?;
    let port = url.port_or_known_default().unwrap_or(443);

    let addrs: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .map_err(CollectorError::Io)?
        .collect();
    if addrs.is_empty() {
        return Err(CollectorError::Protocol(format!(
            "DNS returned no addresses for {host}:{port}"
        )));
    }

    let mut last_err = None;
    let mut connected_stream = None;

    for addr in addrs {
        match connect_socket2(addr, recv_buf_kib).await {
            Ok(s) => {
                connected_stream = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }

    let tcp = match connected_stream {
        Some(s) => s,
        None => {
            return Err(CollectorError::Io(last_err.unwrap_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connect failed")
            })));
        }
    };

    let mut ws_cfg = WebSocketConfig::default();
    ws_cfg.max_message_size = Some(64 * 1024 * 1024);
    ws_cfg.max_frame_size = Some(16 * 1024 * 1024);
    ws_cfg.read_buffer_size = 256 * 1024;
    ws_cfg.write_buffer_size = 64 * 1024;

    let (ws, _resp) = tokio::time::timeout(
        Duration::from_secs(20),
        client_async_tls_with_config(ws_url, tcp, Some(ws_cfg), None),
    )
    .await
    .map_err(|_| CollectorError::Protocol("WebSocket TLS handshake timed out".into()))??;

    Ok(ws)
}

async fn connect_socket2(addr: SocketAddr, recv_buf_kib: usize) -> std::io::Result<TcpStream> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    socket.set_tcp_nodelay(true)?;
    let _ = socket.set_recv_buffer_size(recv_buf_kib * 1024);
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10));
    let _ = socket.set_tcp_keepalive(&keepalive);

    match socket.connect(&addr.into()) {
        Ok(()) => {}
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.raw_os_error() == Some(libc_einprogress()) => {}
        Err(e) => return Err(e),
    }

    let std_stream: std::net::TcpStream = socket.into();
    let stream = TcpStream::from_std(std_stream)?;
    tokio::time::timeout(Duration::from_secs(15), stream.writable())
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "TCP connect timed out")
        })??;
    if let Some(err) = stream.take_error()? {
        return Err(err);
    }
    Ok(stream)
}

#[cfg(target_os = "macos")]
const fn libc_einprogress() -> i32 {
    36
}

#[cfg(target_os = "linux")]
const fn libc_einprogress() -> i32 {
    115
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
const fn libc_einprogress() -> i32 {
    115
}

async fn run_connected_session(
    ws: WsStream,
    cfg: &mut ShardConfig,
    channels: &mut BTreeSet<String>,
) -> Result<()> {
    let (mut sink, mut stream) = ws.split();
    let mut rpc_id: u64 = 1;
    let mut next_id = || {
        let id = rpc_id;
        rpc_id += 1;
        id
    };

    // 1. Optional in-band authentication
    let mut refresh_token: Option<String> = None;
    let mut auth_refresh_deadline: Option<Instant> = None;
    let mut auth_req_id: Option<u64> = None;

    if let (Some(cid), Some(csec)) = (
        cfg.app.deribit.client_id.as_deref(),
        cfg.app.deribit.client_secret.as_deref(),
    ) {
        let id = next_id();
        let req = build_auth_client_credentials(id, cid, csec);
        sink.send(Message::Text(req.into())).await?;

        // Wait synchronously for auth confirmation before subscribing so raw channels inherit auth.
        let auth_res = tokio::time::timeout(Duration::from_secs(15), async {
            while let Some(msg) = stream.next().await {
                let msg = msg?;
                if let Message::Text(txt) = msg {
                    match parse_ws_frame(&txt)? {
                        ParsedWsMessage::RpcSuccess {
                            id: resp_id,
                            result: Some(raw),
                        } if resp_id == id => {
                            let parsed: WireAuthResult = serde_json::from_str(raw.get())?;
                            return Ok::<WireAuthResult, CollectorError>(parsed);
                        }
                        ParsedWsMessage::RpcError {
                            id: Some(resp_id),
                            error,
                        } if resp_id == id => {
                            return Err(CollectorError::DeribitRpc {
                                code: error.code,
                                message: error.message,
                            });
                        }
                        _ => {}
                    }
                }
            }
            Err(CollectorError::Protocol(
                "WebSocket closed before auth response".into(),
            ))
        })
        .await
        .map_err(|_| CollectorError::Protocol("Deribit auth timed out".into()))??;

        let refresh_in = auth_res.expires_in.saturating_sub(60).max(30);
        refresh_token = Some(auth_res.refresh_token);
        auth_refresh_deadline = Some(Instant::now() + Duration::from_secs(refresh_in));
        debug!(
            shard_id = cfg.shard_id,
            expires_in = auth_res.expires_in,
            "authenticated WebSocket shard in-band"
        );
    }

    // 2. Enable Deribit server heartbeat
    let hb_id = next_id();
    let hb_msg = build_set_heartbeat(hb_id, cfg.app.network.heartbeat_interval_secs);
    sink.send(Message::Text(hb_msg.into())).await?;

    // 3. Paced initial subscription batches
    let initial_list: Vec<String> = channels.iter().cloned().collect();
    let batch_size = cfg.app.network.subscribe_batch_size;
    let pace = Duration::from_millis(cfg.app.network.subscribe_pace_ms);

    for (idx, chunk) in initial_list.chunks(batch_size).enumerate() {
        if *cfg.shutdown.borrow() {
            let _ = sink.send(Message::Close(None)).await;
            return Ok(());
        }
        if idx > 0 && !pace.is_zero() {
            tokio::time::sleep(pace).await;
        }
        let sub_id = next_id();
        let req = build_subscribe(sub_id, chunk);
        sink.send(Message::Text(req.into())).await?;
    }

    // Per-connection integrity, live L2 book, and ticker state trackers (fresh on every reconnect!)
    let mut live_books: FxHashMap<InstrumentId, LiveBook> = FxHashMap::default();
    let mut pending_resync: FxHashSet<String> = FxHashSet::default();
    let mut ticker_states: FxHashMap<InstrumentId, TickerState> = FxHashMap::default();

    let stall_timeout = Duration::from_secs(cfg.app.network.read_stall_timeout_secs);
    let mut last_rx = Instant::now();
    let mut watchdog = tokio::time::interval(Duration::from_secs(5));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // 4. Main select loop: inbound frames + dynamic control commands + stall watchdog + token refresh
    loop {
        tokio::select! {
            biased;

            _ = cfg.shutdown.changed() => {
                if *cfg.shutdown.borrow() {
                    let _ = sink.send(Message::Close(None)).await;
                    return Ok(());
                }
            }

            frame = stream.next() => {
                let Some(frame) = frame else {
                    return Ok(());
                };
                let msg = frame?;
                last_rx = Instant::now();

                match msg {
                    Message::Text(txt) => {
                        let received_ts_ns = now_utc_ns();
                        cfg.telemetry.ws_frames_rx.fetch_add(1, Ordering::Relaxed);
                        cfg.telemetry.ws_bytes_rx.fetch_add(txt.len() as u64, Ordering::Relaxed);

                        match parse_ws_frame(&txt) {
                            Ok(ParsedWsMessage::Book { channel, data }) => {
                                handle_book_message(
                                    cfg,
                                    channel,
                                    data,
                                    received_ts_ns,
                                    &mut live_books,
                                    &mut pending_resync,
                                    &mut sink,
                                    &mut next_id,
                                ).await?;
                            }
                            Ok(ParsedWsMessage::Ticker {
                                is_incremental_channel,
                                instrument_name,
                                patch,
                                ..
                            }) => {
                                handle_ticker_message(
                                    cfg,
                                    &instrument_name,
                                    is_incremental_channel,
                                    &patch,
                                    received_ts_ns,
                                    &mut ticker_states,
                                );
                            }
                            Ok(ParsedWsMessage::Trades { channel, trades }) => {
                                handle_trades_message(cfg, channel, &trades, received_ts_ns);
                            }
                            Ok(ParsedWsMessage::PriceIndex { data, .. }) => {
                                cfg.telemetry.index_rows.fetch_add(1, Ordering::Relaxed);
                                try_send_event(
                                    &cfg.ingest_tx,
                                    &cfg.telemetry,
                                    MarketEvent::Index(IndexRow {
                                        received_ts_ns,
                                        exchange_ts_ms: data.timestamp,
                                        index_name: Arc::from(data.index_name),
                                        kind: IndexKind::Price,
                                        value: data.price,
                                    }),
                                );
                            }
                            Ok(ParsedWsMessage::VolatilityIndex { data, .. }) => {
                                cfg.telemetry.index_rows.fetch_add(1, Ordering::Relaxed);
                                try_send_event(
                                    &cfg.ingest_tx,
                                    &cfg.telemetry,
                                    MarketEvent::Index(IndexRow {
                                        received_ts_ns,
                                        exchange_ts_ms: data.timestamp,
                                        index_name: Arc::from(data.index_name),
                                        kind: IndexKind::Volatility,
                                        value: data.volatility,
                                    }),
                                );
                            }
                            Ok(ParsedWsMessage::InstrumentState { data, .. }) => {
                                if let Some(tx) = &cfg.lifecycle_tx {
                                    let _ = tx.try_send(InstrumentLifecycleNotice {
                                        instrument_name: data.instrument_name.to_owned(),
                                        state: data.state.to_owned(),
                                        timestamp_ms: data.timestamp,
                                    });
                                }
                            }
                            Ok(ParsedWsMessage::Heartbeat { kind }) => {
                                cfg.telemetry.heartbeats_rx.fetch_add(1, Ordering::Relaxed);
                                if kind == "test_request" {
                                    let tid = next_id();
                                    let test_msg = build_test_request(tid);
                                    sink.send(Message::Text(test_msg.into())).await?;
                                }
                            }
                            Ok(ParsedWsMessage::RpcSuccess { id, result }) => {
                                if auth_req_id == Some(id) {
                                    auth_req_id = None;
                                    if let Some(raw) = result
                                        && let Ok(parsed) = serde_json::from_str::<WireAuthResult>(raw.get())
                                    {
                                        let refresh_in = parsed.expires_in.saturating_sub(60).max(30);
                                        refresh_token = Some(parsed.refresh_token);
                                        auth_refresh_deadline = Some(Instant::now() + Duration::from_secs(refresh_in));
                                        debug!(shard_id = cfg.shard_id, "refreshed Deribit auth token in-band");
                                    }
                                }
                            }
                            Ok(ParsedWsMessage::RpcError { id, error }) => {
                                warn!(
                                    shard_id = cfg.shard_id,
                                    rpc_id = ?id,
                                    code = error.code,
                                    message = %error.message,
                                    "Deribit JSON-RPC error"
                                );
                            }
                            Ok(ParsedWsMessage::Ignored) => {}
                            Err(e) => {
                                cfg.telemetry.ws_parse_errors.fetch_add(1, Ordering::Relaxed);
                                debug!(
                                    shard_id = cfg.shard_id,
                                    error = %e,
                                    sample = %txt.chars().take(240).collect::<String>(),
                                    "failed to parse Deribit WS frame"
                                );
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload)).await?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        debug!(shard_id = cfg.shard_id, close = ?frame, "received WS Close frame");
                        return Ok(());
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }

            Some(cmd) = cfg.cmd_rx.recv() => {
                match cmd {
                    ShardCommand::Subscribe(list) => {
                        let mut to_add = Vec::new();
                        for ch in list {
                            if channels.insert(ch.clone()) {
                                to_add.push(ch);
                            }
                        }
                        if !to_add.is_empty() {
                            cfg.telemetry.subscribed_channels.fetch_add(to_add.len(), Ordering::Relaxed);
                            for chunk in to_add.chunks(batch_size) {
                                let req = build_subscribe(next_id(), chunk);
                                sink.send(Message::Text(req.into())).await?;
                                if !pace.is_zero() {
                                    tokio::time::sleep(pace).await;
                                }
                            }
                            info!(shard_id = cfg.shard_id, added = to_add.len(), total = channels.len(), "dynamically subscribed channels");
                        }
                    }
                    ShardCommand::Unsubscribe(list) => {
                        let mut to_remove = Vec::new();
                        for ch in list {
                            if channels.remove(&ch) {
                                to_remove.push(ch);
                            }
                        }
                        if !to_remove.is_empty() {
                            cfg.telemetry.subscribed_channels.fetch_sub(to_remove.len(), Ordering::Relaxed);
                            for chunk in to_remove.chunks(batch_size) {
                                let req = build_unsubscribe(next_id(), chunk);
                                sink.send(Message::Text(req.into())).await?;
                                if !pace.is_zero() {
                                    tokio::time::sleep(pace).await;
                                }
                            }
                            info!(shard_id = cfg.shard_id, removed = to_remove.len(), total = channels.len(), "dynamically unsubscribed channels");
                        }
                    }
                }
            }

            _ = watchdog.tick() => {
                if last_rx.elapsed() > stall_timeout {
                    return Err(CollectorError::Protocol(format!(
                        "read stall timeout ({}s with no inbound frames)",
                        stall_timeout.as_secs()
                    )));
                }
                if cfg.app.storage.hourly_book_checkpoints {
                    emit_hourly_quiet_anchors(cfg, &mut live_books, &mut ticker_states);
                }
                if let (Some(deadline), Some(tok)) = (auth_refresh_deadline, refresh_token.as_deref())
                    && Instant::now() >= deadline
                    && auth_req_id.is_none()
                {
                    let id = next_id();
                    auth_req_id = Some(id);
                    let req = build_auth_refresh(id, tok);
                    sink.send(Message::Text(req.into())).await?;
                }
            }
        }
    }
}

/// Trigger B: shortly after a UTC hour rollover, emit self-contained L2 snapshots and ticker OI
/// anchors for quiet instruments that have not yet ticked in the new UTC hour.
fn emit_hourly_quiet_anchors(
    cfg: &ShardConfig,
    live_books: &mut FxHashMap<InstrumentId, LiveBook>,
    ticker_states: &mut FxHashMap<InstrumentId, TickerState>,
) {
    let now_ms = Utc::now().timestamp_millis();
    let offset_ms = now_ms.rem_euclid(3_600_000);
    // Wait 2 seconds past the top of the hour so active instruments fire Trigger A naturally.
    if offset_ms < 2_000 {
        return;
    }
    let cur_hour = utc_hour_index(now_ms);
    let hour_start_ms = cur_hour * 3_600_000;
    let received_ts_ns = now_utc_ns();

    let mut books_emitted = 0u64;
    for (&inst_id, book) in live_books.iter_mut() {
        if book.last_anchor_hour < cur_hour {
            book.last_anchor_hour = cur_hour;
            let levels = book.snapshot_levels();
            let lvl_count = levels.len() as u64;
            let ev = BookUpdateEvent {
                received_ts_ns,
                exchange_ts_ms: hour_start_ms,
                instrument_id: inst_id,
                is_snapshot: true,
                is_checkpoint: true,
                change_id: book.last_change_id,
                prev_change_id: None,
                levels,
            };
            cfg.telemetry
                .book_checkpoints
                .fetch_add(1, Ordering::Relaxed);
            cfg.telemetry
                .book_level_rows
                .fetch_add(lvl_count, Ordering::Relaxed);
            try_send_event(&cfg.ingest_tx, &cfg.telemetry, MarketEvent::Book(ev));
            books_emitted += 1;
        }
    }

    let mut tickers_emitted = 0u64;
    if offset_ms >= 5_000 {
        for tstate in ticker_states.values_mut() {
            if tstate.last_emitted_hour < cur_hour
                && let Some(row) = tstate.anchor_row(hour_start_ms, received_ts_ns)
            {
                cfg.telemetry.ticker_rows.fetch_add(1, Ordering::Relaxed);
                try_send_event(
                    &cfg.ingest_tx,
                    &cfg.telemetry,
                    MarketEvent::Ticker(Box::new(row)),
                );
                tickers_emitted += 1;
            }
        }
    }

    if books_emitted > 0 || tickers_emitted > 0 {
        debug!(
            shard_id = cfg.shard_id,
            hour = cur_hour,
            books_emitted,
            tickers_emitted,
            "emitted hourly self-contained anchors for quiet instruments"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_book_message<S>(
    cfg: &ShardConfig,
    channel: &str,
    data: crate::deribit::protocol::WireBookData<'_>,
    received_ts_ns: i64,
    live_books: &mut FxHashMap<InstrumentId, LiveBook>,
    pending_resync: &mut FxHashSet<String>,
    sink: &mut S,
    next_id: &mut impl FnMut() -> u64,
) -> Result<()>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let Some(inst_id) = cfg.registry.get_or_register_from_name(data.instrument_name) else {
        return Ok(());
    };

    let wire_levels = data.into_levels();
    let msg_hour = utc_hour_index(data.timestamp);
    let mut is_snapshot = data.is_snapshot();
    let mut is_checkpoint = false;
    let out_levels;

    if is_snapshot {
        live_books
            .entry(inst_id)
            .and_modify(|b| b.reset_snapshot(data.change_id, data.timestamp, &wire_levels))
            .or_insert_with(|| {
                LiveBook::from_snapshot(data.change_id, data.timestamp, &wire_levels)
            });
        cfg.telemetry.book_snapshots.fetch_add(1, Ordering::Relaxed);
        if pending_resync.remove(channel) {
            cfg.telemetry.book_resyncs.fetch_add(1, Ordering::Relaxed);
            emit_capture_event(
                &cfg.ingest_tx,
                &cfg.telemetry,
                cfg.shard_id,
                "book_resynced",
                Some(Arc::from(data.instrument_name)),
                format!("change_id={}", data.change_id),
            );
        }
        out_levels = wire_levels;
    } else {
        if pending_resync.contains(channel) {
            // Suppress post-gap deltas until the authoritative resync snapshot arrives.
            return Ok(());
        }
        match live_books.get_mut(&inst_id) {
            Some(book) => {
                let expected_prev = book.last_change_id;
                if data.prev_change_id != Some(expected_prev) {
                    // Sequence gap detected -> record gap and force single-channel resync!
                    cfg.telemetry.book_gaps.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        shard_id = cfg.shard_id,
                        instrument = data.instrument_name,
                        expected_prev,
                        got_prev = ?data.prev_change_id,
                        change_id = data.change_id,
                        "orderbook sequence gap detected; triggering single-channel resync"
                    );
                    emit_capture_event(
                        &cfg.ingest_tx,
                        &cfg.telemetry,
                        cfg.shard_id,
                        "book_gap",
                        Some(Arc::from(data.instrument_name)),
                        format!(
                            "expected_prev={expected_prev} got_prev={:?} change_id={}",
                            data.prev_change_id, data.change_id
                        ),
                    );
                    live_books.remove(&inst_id);
                    pending_resync.insert(channel.to_owned());
                    let ch_vec = vec![channel.to_owned()];
                    let unsub = build_unsubscribe(next_id(), &ch_vec);
                    let sub = build_subscribe(next_id(), &ch_vec);
                    sink.send(Message::Text(unsub.into())).await?;
                    sink.send(Message::Text(sub.into())).await?;
                    return Ok(());
                }

                book.apply_delta(data.change_id, data.timestamp, &wire_levels);

                // Trigger A: if this is the first update in a new UTC hour, promote it to a full
                // self-contained anchor snapshot (preserving exact change_id & prev_change_id).
                if cfg.app.storage.hourly_book_checkpoints && msg_hour > book.last_anchor_hour {
                    book.last_anchor_hour = msg_hour;
                    is_snapshot = true;
                    is_checkpoint = true;
                    out_levels = book.snapshot_levels();
                    cfg.telemetry
                        .book_checkpoints
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    out_levels = wire_levels;
                }
            }
            None => {
                // Received delta before initial snapshot; trigger resync to obtain snapshot.
                pending_resync.insert(channel.to_owned());
                let ch_vec = vec![channel.to_owned()];
                let unsub = build_unsubscribe(next_id(), &ch_vec);
                let sub = build_subscribe(next_id(), &ch_vec);
                sink.send(Message::Text(unsub.into())).await?;
                sink.send(Message::Text(sub.into())).await?;
                return Ok(());
            }
        }
    }

    let level_count = out_levels.len() as u64;
    let event = BookUpdateEvent {
        received_ts_ns,
        exchange_ts_ms: data.timestamp,
        instrument_id: inst_id,
        is_snapshot,
        is_checkpoint,
        change_id: data.change_id,
        prev_change_id: data.prev_change_id,
        levels: out_levels,
    };

    cfg.telemetry.book_messages.fetch_add(1, Ordering::Relaxed);
    cfg.telemetry
        .book_level_rows
        .fetch_add(level_count, Ordering::Relaxed);

    try_send_event(&cfg.ingest_tx, &cfg.telemetry, MarketEvent::Book(event));
    Ok(())
}

fn handle_ticker_message(
    cfg: &ShardConfig,
    instrument_name: &str,
    is_incremental_channel: bool,
    patch: &crate::model::TickerPatch,
    received_ts_ns: i64,
    ticker_states: &mut FxHashMap<InstrumentId, TickerState>,
) {
    let Some(inst_id) = cfg.registry.get_or_register_from_name(instrument_name) else {
        return;
    };
    let state = ticker_states
        .entry(inst_id)
        .or_insert_with(|| TickerState::new(inst_id));

    let msg_hour = utc_hour_index(patch.exchange_ts_ms);
    let crossed_hour =
        cfg.app.storage.hourly_book_checkpoints && msg_hour > state.last_emitted_hour;

    let (mut row, mask, is_snap) = state.apply(patch, received_ts_ns, is_incremental_channel);

    if (mask & ticker_mask::OPEN_INTEREST) != 0 {
        cfg.telemetry
            .ticker_oi_changes
            .fetch_add(1, Ordering::Relaxed);
    }

    let should_emit = crossed_hour
        || match cfg.app.deribit.ticker_emit_policy {
            TickerEmitPolicy::All => is_snap || mask != 0,
            TickerEmitPolicy::InterestingOnly => is_snap || (mask & ticker_mask::INTERESTING) != 0,
        };

    if !should_emit {
        cfg.telemetry
            .ticker_suppressed
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    if crossed_hour {
        row.is_snapshot = true;
    }
    state.last_emitted_hour = msg_hour;

    cfg.telemetry.ticker_rows.fetch_add(1, Ordering::Relaxed);
    try_send_event(
        &cfg.ingest_tx,
        &cfg.telemetry,
        MarketEvent::Ticker(Box::new(row)),
    );
}

fn handle_trades_message(
    cfg: &ShardConfig,
    channel: &str,
    trades: &[crate::deribit::protocol::WireTrade<'_>],
    received_ts_ns: i64,
) {
    let combo_kind = if channel.starts_with("trades.option_combo.") {
        Some(InstrumentKind::OptionCombo)
    } else if channel.starts_with("trades.future_combo.") {
        Some(InstrumentKind::FutureCombo)
    } else {
        None
    };

    for t in trades {
        let inst_id = match combo_kind {
            Some(kind) => {
                let Some(id) = cfg.registry.get_or_register_combo(
                    t.instrument_name,
                    kind,
                    &cfg.app.deribit.markets,
                ) else {
                    continue;
                };
                cfg.telemetry
                    .combo_trade_rows
                    .fetch_add(1, Ordering::Relaxed);
                id
            }
            None => {
                let Some(id) = cfg.registry.get_id(t.instrument_name) else {
                    continue;
                };
                id
            }
        };

        let row = TradeRow {
            received_ts_ns,
            exchange_ts_ms: t.timestamp,
            instrument_id: inst_id,
            trade_id: t.trade_id.to_owned(),
            trade_seq: t.trade_seq,
            price: t.price,
            amount: t.amount,
            contracts: t.contracts,
            direction: t.parsed_direction(),
            tick_direction: t.tick_direction,
            iv: t.iv,
            index_price: t.index_price,
            mark_price: t.mark_price,
            liquidation: t.parsed_liquidation(),
            block_trade_id: t.block_trade_id.map(str::to_owned),
            block_trade_leg_count: t.block_trade_leg_count,
            combo_id: t.combo_id.map(str::to_owned),
            combo_trade_id: t.combo_trade_id.map(str::to_owned),
        };
        cfg.telemetry.trade_rows.fetch_add(1, Ordering::Relaxed);
        try_send_event(&cfg.ingest_tx, &cfg.telemetry, MarketEvent::Trade(row));
    }
}

fn try_send_event(tx: &mpsc::Sender<MarketEvent>, telemetry: &Telemetry, event: MarketEvent) {
    if tx.try_send(event).is_err() {
        telemetry.ingest_drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn emit_capture_event(
    tx: &mpsc::Sender<MarketEvent>,
    telemetry: &Telemetry,
    shard_id: u16,
    event_type: &str,
    instrument_name: Option<Arc<str>>,
    detail: String,
) {
    let ev = CaptureEvent {
        recorded_ts_ns: now_utc_ns(),
        shard_id,
        event_type: Arc::from(event_type),
        instrument_name,
        detail,
    };
    try_send_event(tx, telemetry, MarketEvent::Capture(ev));
}
