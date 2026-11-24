# deribit-tick-collector

High-performance, low-footprint Rust market data collector for **Deribit** options, perpetuals, and dated futures.

Captures lossless **tick-by-tick L2 order book updates** (with self-contained hourly checkpoints), **ticker / open interest / IV / Greeks / funding streams**, **public & combo/block trade prints**, **Deribit spot & DVOL volatility indices**, and **instrument lifecycle metadata** into query-ready, Hive-partitioned **Apache Parquet v2** datasets — with optional asynchronous S3 / R2 / MinIO archival and built-in inspection, compaction, and order-book replay verification tools.

---

## Key Capabilities

### Market Data Coverage (Public API — No Credentials Required)
- **Tick-by-tick Options L2 Order Books** (`book.<instrument>.raw` / `100ms` / `agg2`): full-depth initial snapshots plus incremental `new`/`change`/`delete` price-level deltas with per-instrument `change_id` / `prev_change_id` continuity tracking.
- **Tick-by-tick Perpetual & Dated Futures L2 Order Books**: isolated onto dedicated WebSocket shards so high-frequency `BTC-PERPETUAL` / `ETH-PERPETUAL` and dated futures bursts never experience head-of-line blocking behind large option chains.
- **Tickers & Open Interest Tracking** (`incremental_ticker.<instrument>` or `ticker.<instrument>.<interval>`): tracks `open_interest`, `mark_iv`, `bid_iv`, `ask_iv`, Black-Scholes Greeks (`delta`, `gamma`, `vega`, `theta`, `rho`), `mark_price`, `index_price`, `underlying_index`, `underlying_price`, perpetual funding (`current_funding`, `funding_8h`, `interest_value`), settlement/delivery prices, BBO, and 24h volume stats.
  - When using `incremental_ticker` (default), the collector receives minimal wire deltas, **materializes full ticker state in memory** (accurately handling explicit `null` clears), and attaches a `changed_mask` bitmask to every stored row so downstream queries can filter exact open-interest or Greek changes in a single bitwise check while keeping every row self-contained.
- **Self-Contained Hourly L2 Book Checkpoints & Quiet Ticker Anchors**: each shard maintains lightweight live L2 books in memory and emits anchor snapshots (`is_snapshot = true, is_checkpoint = true`) at every UTC hour boundary (promoting the first new-hour tick for active contracts and emitting an `HH:00:00` anchor for quiet contracts, plus quiet ticker OI anchors). Every `date=YYYY-MM-DD/hour=HH/` partition is therefore **independently replayable** without reading prior hours or days, while multi-hour spans still verify unbroken `change_id` continuity.
- **Public, Combo & Block Trade Prints** (`trades.option.*`, `trades.future.*`, `trades.option_combo.*`, `trades.future_combo.*`): captures executions, option IV, liquidations (`M`/`T`/`MT`), multi-leg combos (`combo_id`, `combo_trade_id`), and block trades (`block_trade_id`, `block_trade_leg_count`) on the single `lifecycle` shard — linking institutional flow directly to open-interest changes.
- **Spot Price & DVOL Volatility Indices** (`deribit_price_index.*`, `deribit_volatility_index.*`): streams real-time Deribit spot indices (`btc_usd`, `eth_usd`, `sol_usdc`, ...) and 30-day DVOL implied volatility indices into `dataset=indices`.
- **Zero-Downtime Instrument Lifecycle** (`instrument.state.any.any` + periodic catalog reconciliation): dynamically subscribes newly listed strikes/expiries and unsubscribes settled/expired contracts in-place without dropping active connections during Deribit's 08:00 UTC rollover.
- **Inverse & Linear Market Isolation**: cleanly separates inverse markets (`BTC`, `ETH`) from linear USDC-settled markets (`BTC_USDC`, `ETH_USDC`, `SOL_USDC`, `XRP_USDC`, `PAXG_USDC`, etc.) so inverse and linear contracts never collide.

### Networking Optimizations
- **Workload-Isolated WebSocket Sharding**:
  - Dedicated `lifecycle` shard (`instrument.state.any.any` + `trades.*`)
  - Dedicated `futures` shard(s) (perpetuals + dated futures, lightly packed)
  - Round-robin load-balanced `options` shards (sorted by expiry/strike before round-robin assignment so active near-term ATM contracts spread evenly across connections and CPU cores, leaving 10% headroom under Deribit's 500-channel/connection limit)
- **Tuned Sockets & Pure-Rust TLS**: `socket2` TCP sockets (`TCP_NODELAY`, configurable 4 MiB `SO_RCVBUF`, TCP keepalive) + `rustls` (`aws-lc-rs`, zero OpenSSL dependency).
- **Zero-`Value` Wire Parsing**: single-pass borrowed JSON-RPC deserialization using `serde_json::value::RawValue` and custom tuple visitors (`["new", price, amount]` -> `Copy` structs with `SmallVec`, zero intermediate `serde_json::Value` trees or action `String` allocations).
- **Automatic Sequence-Gap Recovery**: per-instrument `change_id` -> `prev_change_id` verification; on any gap, logs a structured `book_gap` audit record to `capture_events`, suppresses out-of-sequence deltas, and triggers an in-place single-channel `unsubscribe`/`subscribe` resync for a fresh snapshot.
- **Resilient Operations**: Deribit server heartbeat + automatic `public/test` response, read-stall watchdog, paced batch subscriptions respecting Deribit's ~3.3 req/s rate limit, exponential backoff with full jitter, optional in-band `public/auth` + `refresh_token`, and Prometheus `/metrics` + `/healthz`.

### Storage Optimizations
- **Two-Stage Decoupled Pipeline**: async Tokio Struct-of-Arrays (SoA) accumulator -> bounded channel -> dedicated `parquet-io` OS thread, ensuring ZSTD compression and disk I/O never stall WebSocket reads.
- **Locality-Sorted Row Groups**: every row group is permutation-sorted by `(instrument_name, exchange_ts, change_id)` before writing, maximizing run-length, delta, and dictionary compression while enabling Parquet row-group & page pruning.
- **Parquet v2 Column-Specific Encodings**:
  - `ZSTD` compression (configurable level, default `3`; `6` for offline compaction)
  - `DELTA_BINARY_PACKED` on monotonic UTC nanosecond/millisecond timestamps, `change_id`, `prev_change_id`, and `trade_seq`
  - Hybrid `RLE_DICTIONARY` + `BYTE_STREAM_SPLIT` fallback on `Float64` columns (constant/repeated fields like `open_interest`, `settlement_price`, and tick-quantized prices collapse via RLE dictionary, while high-entropy continuous Greeks/IVs spill to `BYTE_STREAM_SPLIT`)
  - Dictionary encoding + **Bloom filters** + **Page Index** statistics on `instrument_name`
- **Crash-Safe Atomic Rotation**: writes to `.parquet.tmp`, optionally calls `fsync`, and atomically renames to `.parquet` on UTC hour boundaries, rotation intervals, size thresholds, or graceful `SIGINT`/`SIGTERM` shutdown. Orphaned `.parquet.tmp` files from hard crashes are automatically cleaned on startup.

---

## Architecture

```text
Deribit API v2 (wss://www.deribit.com/ws/api/v2)
   │
   ├── Shard 0 (lifecycle)     ── instrument.state.any.any, trades.* (incl. combos), price_index.*, dvol.*
   ├── Shard 1 (futures)       ── BTC/ETH perpetuals & dated futures (book.raw + incremental_ticker)
   └── Shards 2..N (options)   ── round-robin balanced option chains (<= 420 ch/conn, headroom < 500)
          │
          │  TCP_NODELAY + 4MiB SO_RCVBUF + rustls + borrowed RawValue JSON parser
          │  LiveBook change_id gap detector + hourly checkpoints + TickerState materializer
          ▼
   Bounded MPSC Channel (zero network stalls)
          │
          ▼
   Stage A: SoA Columnar Accumulator (Tokio task, permutation-sorted batches, memory-capped)
          │
          ▼
   Stage B: Dedicated parquet-io OS Thread (Parquet v2 + ZSTD + DELTA + BYTE_STREAM_SPLIT + Bloom)
          │  Atomic rename: part-NNNNN-<ts>.parquet.tmp -> part-NNNNN-<ts>.parquet
          ▼
   Hive-Partitioned Local Lake  ──►  Optional Async S3 / R2 / MinIO Uploader (object_store)
```

---

## Measured Live Mainnet Performance

Measured against live Deribit mainnet capturing **all active inverse BTC + ETH options (1,740), perpetuals (2), and dated futures (22)** — **1,764 instruments / 3,533 live WebSocket channels across 12 shards** (`book.*.raw` + `incremental_ticker.*` + `trades.*.raw` + `instrument.state.any.any`), 25-second capture on Apple Silicon:

| Metric | Measured Result |
|---|---|
| Inbound throughput | **2,559 frames/sec** (~914 KiB/s raw JSON) |
| Total CPU usage (`user + sys`) | **4.5% of 1 CPU core** (`0.69s user + 0.46s sys` over 25.6s) |
| Peak memory footprint | **~115 MiB** (all 12 TLS shards + 1,764 state trackers + Parquet writers) |
| Parse errors / Ingest drops / Sequence gaps | **0 / 0 / 0** |
| Graceful shutdown & Parquet finalization | **70 ms** (17 finalized `.parquet` files) |
| End-to-end compression ratio (JSON -> Parquet) | **~11x** (`22.9 MiB` wire JSON -> `2.09 MiB` on disk) |
| `book_updates` (`BTC-PERPETUAL`, full L2 depth) | **4.84 bytes / level update** (573 bid / 761 ask levels, 0 gaps) |
| `book_updates` (all 958 `BTC` options) | **9.00 bytes / level update** (958/958 snapshots, 0 gaps) |
| `book_updates` (all 11 `BTC` dated futures) | **7.66 bytes / level update** (0 gaps) |
| `tickers` (all 958 `BTC` options, 38 columns) | **34.40 bytes / row** (drops further over full 15–60m segments) |

---

## Storage Layout & Schemas

### Hive Partition Layout
```text
data/
├── dataset=book_updates/currency=BTC/kind=option/date=2026-11-24/hour=18/part-00000-...parquet
├── dataset=book_updates/currency=BTC/kind=perpetual/date=2026-11-24/hour=18/part-00001-...parquet
├── dataset=book_updates/currency=BTC/kind=future/date=2026-11-24/hour=18/part-00002-...parquet
├── dataset=tickers/currency=BTC/kind=option/date=2026-11-24/hour=18/part-00003-...parquet
├── dataset=tickers/currency=BTC/kind=perpetual/date=2026-11-24/hour=18/part-00004-...parquet
├── dataset=tickers/currency=BTC/kind=future/date=2026-11-24/hour=18/part-00005-...parquet
├── dataset=trades/currency=BTC/kind=option/date=2026-11-24/hour=18/part-00006-...parquet
├── dataset=trades/currency=BTC/kind=option_combo/date=2026-11-24/hour=18/part-00007-...parquet
├── dataset=indices/date=2026-11-24/hour=18/part-00008-...parquet
├── dataset=instruments/date=2026-11-24/part-00009-...parquet
└── dataset=capture_events/date=2026-11-24/part-00010-...parquet
```

### Datasets
1. **`book_updates`**: `received_ts` (UTC ns), `exchange_ts` (UTC ms), `instrument_name` (dict), `is_snapshot`, `is_checkpoint` (true for collector-synthesized UTC hour boundary anchors), `change_id`, `prev_change_id`, `side` (`0=bid`, `1=ask`, `2=empty`), `action` (`0=new`, `1=change`, `2=delete`, `3=empty`), `price`, `amount`.
2. **`tickers`**: `received_ts`, `exchange_ts`, `instrument_name`, `is_snapshot`, `changed_mask` (`u32` bitflags), `state`, `open_interest`, `mark_price`, `index_price`, `last_price`, `settlement_price`, `estimated_delivery_price`, `min_price`, `max_price`, `best_bid_price`, `best_bid_amount`, `best_ask_price`, `best_ask_amount`, `volume`, `volume_usd`, `volume_notional`, `high`, `low`, `price_change`, `underlying_index`, `underlying_price`, `mark_iv`, `bid_iv`, `ask_iv`, `interest_rate`, `delta`, `gamma`, `vega`, `theta`, `rho`, `current_funding`, `funding_8h`, `interest_value`.
3. **`trades`**: `received_ts`, `exchange_ts`, `instrument_name`, `trade_id`, `trade_seq`, `price`, `amount`, `contracts`, `direction` (`0=buy`, `1=sell`), `tick_direction`, `iv`, `index_price`, `mark_price`, `liquidation` (`0=none`, `1=maker`, `2=taker`, `3=both`), `block_trade_id`, `block_trade_leg_count`, `combo_id`, `combo_trade_id`.
4. **`indices`**: `received_ts` (UTC ns), `exchange_ts` (UTC ms), `index_name` (dict, e.g. `btc_usd`, `eth_usd`, `sol_usdc`), `index_kind` (`0=spot price`, `1=30d DVOL volatility`), `value` (`Float64`).
5. **`instruments`**: full Deribit contract specs (`deribit_instrument_id`, `market`, `base/quote/settlement_currency`, `settlement_period`, `kind`, `creation_ts`, `expiration_ts`, `strike`, `option_type`, `tick_size`, `min_trade_amount`, `contract_size`, `maker/taker_commission`, `is_active`, `state`).
6. **`capture_events`**: integrity journal (`startup`, `shard_connected`, `shard_disconnected`, `book_gap`, `book_resynced`, `instrument_listed`, `instrument_expired`, `catalog_reconcile`).

### `tickers.changed_mask` Bitflags
| Bit | Value | Meaning |
|---|---|---|
| `0` | `1` (`0x001`) | `open_interest` changed |
| `1` | `2` (`0x002`) | `mark_price` changed |
| `2` | `4` (`0x004`) | `index_price` changed |
| `3` | `8` (`0x008`) | `underlying_price` / `underlying_index` changed |
| `4` | `16` (`0x010`) | `mark_iv` / `bid_iv` / `ask_iv` / `interest_rate` changed |
| `5` | `32` (`0x020`) | Greeks (`delta`, `gamma`, `vega`, `theta`, `rho`) changed |
| `6` | `64` (`0x040`) | Top-of-book (`best_bid_*` / `best_ask_*`) changed |
| `7` | `128` (`0x080`) | `last_price` changed |
| `8` | `256` (`0x100`) | Perpetual funding (`current_funding`, `funding_8h`, `interest_value`) changed |
| `9` | `512` (`0x200`) | 24h `stats` (`volume`, `high`, `low`, `price_change`) changed |
| `10` | `1024` (`0x400`) | Settlement / delivery / min / max price bounds changed |
| `11` | `2048` (`0x800`) | Instrument `state` changed |

---

## Quick Start

### Prerequisites
- Rust toolchain `>= 1.88` (`rustup update stable`)

### Build
```bash
cargo build --release
# Or without S3/object_store support for an even smaller local-only binary:
cargo build --release --no-default-features
```

### 1. Preview Active Deribit Instruments & Shard Footprint
```bash
./target/release/deribit-tick-collector instruments --markets BTC,ETH,SOL_USDC
```

### 2. Collect Live Market Data
```bash
# Default: BTC & ETH options + perpetuals + dated futures (book.raw + incremental_ticker + trades.raw)
./target/release/deribit-tick-collector collect --data-dir ./data

# Using a TOML configuration file
cp config.example.toml config.toml
./target/release/deribit-tick-collector collect --config config.toml

# Bounded 60-second capture including linear USDC markets
./target/release/deribit-tick-collector collect \
  --markets BTC,ETH,SOL_USDC \
  --data-dir ./data \
  --duration-secs 60
```

> **Storage Tip:** Because `book_updates` already captures full tick-by-tick BBO & depth, passing `--ticker-emit-policy interesting-only` skips pure BBO-only ticker updates while retaining every open-interest, IV, Greek, funding, volume, mark/index, and state change.

### 3. Inspect & Audit Recorded Data
```bash
./target/release/deribit-tick-collector inspect ./data
```

### 4. Reconstruct & Verify L2 Order Book Tick-by-Tick
```bash
./target/release/deribit-tick-collector replay ./data --instrument BTC-PERPETUAL --samples 10
./target/release/deribit-tick-collector replay ./data --instrument BTC-25DEC26-100000-C --samples 10
```

### 5. Compact Multi-Segment Hourly Partitions
```bash
./target/release/deribit-tick-collector compact ./data --zstd-level 6
```

---

## Querying with DuckDB / Polars

### DuckDB SQL Examples
```sql
-- Track every open-interest change across BTC options with strike & expiry metadata
SELECT
    t.exchange_ts,
    t.instrument_name,
    i.expiration_ts,
    i.strike,
    i.option_type,
    t.open_interest,
    t.mark_iv,
    t.delta,
    t.underlying_price
FROM read_parquet('data/dataset=tickers/currency=BTC/kind=option/**/*.parquet', hive_partitioning = true) t
LEFT JOIN (
    SELECT DISTINCT ON (instrument_name) instrument_name, expiration_ts, strike, option_type
    FROM read_parquet('data/dataset=instruments/**/*.parquet')
    ORDER BY instrument_name, recorded_ts DESC
) i USING (instrument_name)
WHERE (t.changed_mask & 1) != 0
ORDER BY t.exchange_ts;

-- Aggregate total BTC options open interest by expiry over time
SELECT
    time_bucket(INTERVAL '1 minute', t.exchange_ts) AS bucket,
    i.expiration_ts,
    i.option_type,
    SUM(arg_max(t.open_interest, t.exchange_ts)) AS total_oi
FROM read_parquet('data/dataset=tickers/currency=BTC/kind=option/**/*.parquet', hive_partitioning = true) t
JOIN read_parquet('data/dataset=instruments/**/*.parquet') i USING (instrument_name)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Inspect tick-by-tick L2 book updates for BTC-PERPETUAL
SELECT exchange_ts, change_id, prev_change_id, is_snapshot, is_checkpoint, side, action, price, amount
FROM read_parquet('data/dataset=book_updates/currency=BTC/kind=perpetual/**/*.parquet')
WHERE instrument_name = 'BTC-PERPETUAL'
ORDER BY exchange_ts, change_id
LIMIT 50;

-- Track BTC spot price index alongside 30-day DVOL implied volatility
SELECT exchange_ts, index_name, CASE index_kind WHEN 0 THEN 'price' ELSE 'dvol' END AS kind, value
FROM read_parquet('data/dataset=indices/**/*.parquet')
WHERE index_name = 'btc_usd'
ORDER BY exchange_ts;

-- Inspect institutional block & multi-leg option combo prints
SELECT exchange_ts, instrument_name, direction, price, amount, iv, block_trade_id, block_trade_leg_count, combo_id
FROM read_parquet('data/dataset=trades/currency=BTC/**/*.parquet')
WHERE block_trade_id IS NOT NULL OR combo_id IS NOT NULL
ORDER BY exchange_ts;
```

### Python (Polars)
```python
import polars as pl

# Lazy scan with automatic Hive partition + Parquet Page/Bloom predicate pushdown
oi_changes = (
    pl.scan_parquet("data/dataset=tickers/currency=BTC/kind=option/**/*.parquet")
    .filter((pl.col("changed_mask") & 1) != 0)
    .select(["exchange_ts", "instrument_name", "open_interest", "mark_iv", "delta"])
    .collect()
)
```

---

## Production Deployment (EC2 / Docker / S3)

### Optional S3 / Cloudflare R2 / MinIO Upload
Enable `[s3]` in `config.toml` and supply credentials via IAM instance role or environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, and optional `S3_ENDPOINT` for R2/MinIO). Finalized `.parquet` segments are uploaded asynchronously with bounded concurrency and exponential-backoff retries, optionally deleting local copies after verified upload.

### Prometheus & Health Checks
When `telemetry.metrics_bind` is configured (default `127.0.0.1:9108`):
- `GET /healthz` — returns `200 OK` while shards are active (`503` if all shards are disconnected)
- `GET /metrics` — Prometheus text format exporting frame rates, bytes received, reconnects, book gaps/resyncs, OI changes, ingest drops, Parquet files/bytes written, and S3 upload status

### Docker
```bash
docker build -t deribit-tick-collector .
docker run -d --name deribit-collector \
  -v $(pwd)/data:/data \
  -p 127.0.0.1:9108:9108 \
  deribit-tick-collector
```

### systemd (EC2 / Linux)
A hardened systemd unit is provided at [`deploy/deribit-tick-collector.service`](deploy/deribit-tick-collector.service).

---

## Development & Testing

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --no-default-features --all-targets -- -D warnings
cargo test
```

## License

MIT — see [LICENSE](LICENSE).
