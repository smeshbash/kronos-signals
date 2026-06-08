"""
Kronos Trading System — Database layer
All 10 tables per Section 18.4 of the requirements spec.
"""

import json
import os
import sqlite3
import time
from contextlib import contextmanager

DB_PATH = os.environ.get('KRONOS_DB_PATH', os.path.join(os.path.dirname(__file__), 'data', 'kronos.db'))

# ── Pipeline regime versioning ─────────────────────────────────────────────────
# Bump this when pipeline config changes materially (filters, risk rules, exit
# logic). All signal generators write this value so benchmark_analysis.py can
# compare apples-to-apples across a consistent rule set.
#
# v1 — original (trailing stop active, 1.17% return floor, no per-model conf gate)
# v2 — 2026-05-31: trailing stop disabled in paper mode, 2.0% return floor,
#       kronos-base confidence gate ≥ 0.4
# v3 — 2026-06-01: structural fixes (TDS corrected, TP multiplier 3→2, cost floor
#       0.17%, consecutive loss limit 3→5, stacking guard, M15/M16 re-enabled mid-run)
#       Active models: custom (M4), kronos-mini (M13), kronos-base (M14),
#                      kronos-mini-4h (M15), kronos-base-4h (M16)
#       v3 data: 187 trades, benchmark analysis completed 2026-06-05.
#
# v4 — 2026-06-05: regime-aware pipeline — data-driven overhaul post-benchmark:
#       - M4 (custom) HALTED: -431 Rs/trade expectancy, 91% long-bias, 7% WR (n=29).
#         Benchmark data showed no edge in any regime, any symbol, any direction.
#       - kronos-base 0.4 confidence floor REMOVED: filter was inverted at boundary —
#         blocked signals had 38.1% dir_acc vs 24.4% for passing signals (n=373).
#         Root cause: low-conf = shorts (winning in bear), high-conf = longs (losing).
#       - Regime direction filter ADDED (M5, REGIME_FILTER_ENABLED=True):
#         Composite EMA50/EMA200 on 4H candles, per-symbol.
#           BEAR (close < EMA50 < EMA200): longs blocked, shorts pass
#           BULL (close > EMA50 > EMA200): shorts blocked, longs pass
#           NEUTRAL (transition): both directions pass
#         Applied in paper and live mode. Rejected signals persist to DB with
#         reason='regime_bear_long_blocked' | 'regime_bull_short_blocked' for
#         retrospective validation as actual_return_pct populates.
#       - BENCHMARK_MODEL_SOURCE = 'kronos-base-4h': only model with positive
#         expectancy in v3 (+51 Rs/trade, n=41). Correct directional bias (63% short).
#         All other models' metrics expressed as deltas vs this benchmark.
#       - Active models: kronos-mini (M13), kronos-base (M14),
#                        kronos-mini-4h (M15), kronos-base-4h (M16)
#       v5 changes (2026-06-08):
#         - XRP contract size corrected (1.0 XRP/contract, was 10.0 — fees were ~10x inflated pre-fix)
#         - Per-asset TP/SL tuned for all 4 models via MFE/MAE grid search (0.25x–5.0x, 400 combos)
#         - Per-symbol halts: ETHUSD(mini), BTCUSD+XRPUSD(base), BNBUSD(mini-4h), ETHUSD+XRPUSD(base-4h)
#         - M14 (kronos-base 1H) + M15 (kronos-mini-4h) re-enabled with tuned per-asset configs
#         - kronos-mini XRPUSD halt lifted: post-fix re-analysis (+Rs 757 sim, TP=2.0x SL=0.25x optimal)
#       v1/v2/v3/v4 are historical reference. v5 is the current benchmark dataset.
SIGNAL_REGIME_VERSION = 5

# ── Benchmark model ────────────────────────────────────────────────────────────
# Set 2026-06-05. Purely an evaluation marker — zero impact on signal generation,
# risk checks, execution, or position management. Used by analysis scripts to
# express other models' metrics as deltas relative to this reference model.
#
# Rationale: kronos-base-4h is the only model with positive expectancy (+51 Rs/trade)
# in regime v3 data (n=41 trades). It is also the only model with correct directional
# bias (63% short) in the current bear regime. All other models are evaluated against
# its performance as the bar to beat.
BENCHMARK_MODEL_SOURCE = 'kronos-base-4h'


@contextmanager
def get_connection():
    """Yield a WAL-mode SQLite connection; commit on success, rollback on error."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=5000')
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create all 10 tables (idempotent — safe to call on every startup)."""
    _dir = os.path.dirname(DB_PATH)
    if _dir:
        os.makedirs(_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')

    conn.executescript("""
        -- ── Table 1: OHLCV ─────────────────────────────────────────────────
        -- Historical candle data per asset per timeframe.
        -- Exactly six OHLCV columns plus symbol/timeframe/timestamp.
        -- Open interest is NOT stored here; it belongs in orderbook_snapshots
        -- as liquidity context for the slippage model (Section 12.3).
        CREATE TABLE IF NOT EXISTS ohlcv (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT    NOT NULL,
            timeframe   TEXT    NOT NULL,
            timestamp   INTEGER NOT NULL,   -- Unix epoch seconds (candle open time)
            open        REAL    NOT NULL,
            high        REAL    NOT NULL,
            low         REAL    NOT NULL,
            close       REAL    NOT NULL,
            volume      REAL    NOT NULL,
            created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            UNIQUE(symbol, timeframe, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_tf_ts
            ON ohlcv(symbol, timeframe, timestamp DESC);

        -- ── Table 2: Order book snapshots ──────────────────────────────────
        -- L1 bid/ask snapshots every 15 min from Delta WebSocket (Section 12.1).
        -- open_interest and funding_rate are included here because Section 12.3
        -- explicitly names them as slippage model inputs alongside spread/depth:
        -- "Funding rate and open interest context → Liquidity regime classifier".
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol        TEXT    NOT NULL,
            timestamp     INTEGER NOT NULL,   -- Unix epoch seconds of snapshot
            best_bid      REAL,
            best_ask      REAL,
            bid_size      REAL,
            ask_size      REAL,
            spread        REAL,               -- best_ask - best_bid
            mark_price    REAL,
            open_interest REAL,
            funding_rate  REAL,
            created_at    INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_ob_sym_ts
            ON orderbook_snapshots(symbol, timestamp DESC);

        -- ── Table 3: Funding rates ──────────────────────────────────────────
        -- Historical funding rates per asset, fetched every 8H via REST.
        -- Retained indefinitely per Section 18.4.
        CREATE TABLE IF NOT EXISTS funding_rates (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT    NOT NULL,
            timestamp   INTEGER NOT NULL,   -- Unix epoch seconds of fetch
            rate        REAL    NOT NULL,   -- raw 8H funding rate (e.g. 0.0001 = 0.01%)
            created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            UNIQUE(symbol, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_fr_sym_ts
            ON funding_rates(symbol, timestamp DESC);

        -- ── Table 4: Kronos signals ─────────────────────────────────────────
        -- All signals generated — approved, rejected, executed, expired.
        -- Retained indefinitely for model drift detection (Section 20.3).
        CREATE TABLE IF NOT EXISTS signals (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol               TEXT    NOT NULL,
            direction            TEXT    NOT NULL,      -- 'long' | 'short'
            confidence           REAL    NOT NULL,
            horizon              TEXT    NOT NULL,      -- e.g. '4h', '8h', '24h'
            status               TEXT    NOT NULL DEFAULT 'pending',
            -- 'pending' | 'approved' | 'rejected' | 'executed' | 'expired'
            rejection_reason     TEXT,
            predicted_return_pct REAL    NOT NULL DEFAULT 0.0,
            -- signed % predicted price change from current close over horizon.
            -- Used by M5 cost-adjusted funding filter: only enter if
            -- |predicted_return| covers funding + fees for the full horizon.
            signal_timestamp     INTEGER NOT NULL,      -- Unix epoch seconds
            created_at           INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_signals_sym_ts
            ON signals(symbol, signal_timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_signals_status
            ON signals(status);

        -- ── Table 5: Trades ─────────────────────────────────────────────────
        -- Every executed trade. Retained indefinitely — primary tax record.
        -- TDS, funding, and fee tracking per Section 16.1.
        CREATE TABLE IF NOT EXISTS trades (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id        INTEGER REFERENCES signals(id),
            symbol           TEXT    NOT NULL,
            direction        TEXT    NOT NULL,      -- 'long' | 'short'
            entry_price      REAL,
            exit_price       REAL,
            size_contracts   REAL    NOT NULL,      -- number of contracts
            notional_value   REAL,                  -- entry_price * size * contract_value
            entry_timestamp  INTEGER,               -- Unix epoch seconds
            exit_timestamp   INTEGER,
            exit_reason      TEXT,
            -- 'take_profit' | 'stop_loss' | 'time_limit' | 'funding_cost'
            -- | 'drawdown_alert' | 'forced_override' | 'manual'
            pnl_gross        REAL,                  -- P&L before TDS and tax
            pnl_net          REAL,                  -- P&L after TDS deduction
            tds_deducted     REAL,                  -- always 0 for futures/options (TDS not applicable; Delta Exchange India does not deduct TDS on derivatives)
            funding_paid     REAL    NOT NULL DEFAULT 0.0,
            funding_received REAL    NOT NULL DEFAULT 0.0,
            fees             REAL,                  -- maker fees + 18% GST
            status           TEXT    NOT NULL DEFAULT 'open',
            -- 'open' | 'closed' | 'cancelled' (order placed but never filled — Module 6)
            created_at       INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_trades_sym_status
            ON trades(symbol, status);
        CREATE INDEX IF NOT EXISTS idx_trades_entry_ts
            ON trades(entry_timestamp DESC);

        -- ── Table 6: Open positions ─────────────────────────────────────────
        -- Live state of all currently open positions.
        -- One row per open trade; row deleted or marked closed on exit.
        CREATE TABLE IF NOT EXISTS positions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id         INTEGER NOT NULL UNIQUE REFERENCES trades(id),
            symbol           TEXT    NOT NULL,
            direction        TEXT    NOT NULL,      -- 'long' | 'short'
            entry_price      REAL    NOT NULL,
            current_price    REAL,
            size_contracts   REAL    NOT NULL,
            notional_value   REAL,
            margin_used      REAL,                  -- INR margin allocated
            leverage         REAL,
            stop_loss_price  REAL,
            take_profit_price REAL,
            entry_timestamp  INTEGER NOT NULL,
            max_hold_until   INTEGER NOT NULL,      -- entry + 5 days (hard exit)
            unrealised_pnl   REAL,
            status           TEXT    NOT NULL DEFAULT 'open',
            -- 'open' | 'closing'
            created_at       INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_positions_sym
            ON positions(symbol, status);

        -- ── Table 7: Portfolio snapshots ────────────────────────────────────
        -- Portfolio state snapshot every 4 hours. Retained indefinitely.
        -- Feeds drawdown alert system (Section 8.2) and performance reporting.
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       INTEGER NOT NULL,       -- Unix epoch seconds
            total_value     REAL    NOT NULL,        -- INR total portfolio value
            active_margin   REAL,                    -- margin in open positions
            available_margin REAL,
            unrealised_pnl  REAL    NOT NULL DEFAULT 0.0,
            drawdown_pct    REAL    NOT NULL DEFAULT 0.0, -- % from peak
            peak_value      REAL,                    -- highest portfolio value to date
            open_positions  INTEGER NOT NULL DEFAULT 0,
            phase           TEXT    NOT NULL DEFAULT 'pre_live',
            created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_pf_ts
            ON portfolio_snapshots(timestamp DESC);

        -- ── Table 8: TDS deduction log ──────────────────────────────────────
        -- Every TDS deduction with UTR reference. Retained indefinitely.
        -- Required for ITR TDS reconciliation (Section 16.2).
        CREATE TABLE IF NOT EXISTS tds_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id       INTEGER NOT NULL REFERENCES trades(id),
            symbol         TEXT    NOT NULL,
            sell_notional  REAL    NOT NULL,         -- INR value of sell
            tds_amount     REAL    NOT NULL,         -- 1% of sell_notional
            utr_reference  TEXT,                     -- exchange UTR for reconciliation
            timestamp      INTEGER NOT NULL,
            created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );

        -- ── Table 9: Tax reserve ─────────────────────────────────────────────
        -- Running balance of the 30% gross profit tax reserve (Section 11.4).
        -- Every credit and debit recorded; balance_after is the running total.
        CREATE TABLE IF NOT EXISTS tax_reserve (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_type    TEXT    NOT NULL,
            -- 'reserve' (credit) | 'release' (on withdrawal calc) | 'payment' (ITR)
            amount              REAL    NOT NULL,    -- positive = credit, negative = debit
            balance_after       REAL    NOT NULL,    -- running balance after this transaction
            reference_trade_id  INTEGER REFERENCES trades(id),
            notes               TEXT,
            timestamp           INTEGER NOT NULL,
            created_at          INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );

        -- ── Table 10: System events ──────────────────────────────────────────
        -- All system events: heartbeats, alerts, errors, overrides, module output.
        -- Also stores 15-min aggregated fill windows (event_type='fill_window')
        -- used by Module 2 (Slippage Model) for slippage distribution analysis.
        -- Minimum 90-day retention per Section 14.3.
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            module      TEXT    NOT NULL,   -- originating module name
            event_type  TEXT    NOT NULL,
            -- 'heartbeat' | 'fill_window' | 'alert_yellow' | 'alert_orange'
            -- | 'alert_red' | 'forced_override' | 'ws_connected'
            -- | 'ws_disconnected' | 'ohlcv_fetch' | 'funding_rate_fetch'
            -- | 'orderbook_snapshot' | 'error' | 'info' | 'warning'
            severity    TEXT    NOT NULL DEFAULT 'info',
            -- 'debug' | 'info' | 'warning' | 'error' | 'critical'
            message     TEXT    NOT NULL,
            data        TEXT,               -- JSON blob for structured payload
            timestamp   INTEGER NOT NULL,
            created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_events_module_ts
            ON events(module, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_events_type
            ON events(event_type, timestamp DESC);

        -- ── Table 11: Shadow signals (legacy — no longer written) ────────────
        -- Originally used to log foundation model predictions separately.
        -- Superseded: M13/M14 now write directly to the shared signals table
        -- with model_source='kronos-mini'/'kronos-base', enabling unified M5/M6
        -- processing and per-model accuracy tracking on the dashboard.
        -- Table retained to avoid migration errors on existing databases.
        CREATE TABLE IF NOT EXISTS shadow_signals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol           TEXT    NOT NULL,
            model_name       TEXT    NOT NULL,   -- 'kronos-mini' | 'kronos-base'
            direction        TEXT    NOT NULL,   -- 'long' | 'short'
            confidence       REAL    NOT NULL,
            predicted_return REAL    NOT NULL,   -- signed %, horizon=24H
            context_candles  INTEGER NOT NULL,   -- candles fed to this model
            signal_timestamp INTEGER NOT NULL,
            created_at       INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_shadow_sym_ts
            ON shadow_signals (symbol, signal_timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_shadow_model
            ON shadow_signals (model_name, signal_timestamp DESC);
    """)

    # ── Column migrations (idempotent — safe on every startup) ──────────────────
    # SQLite does not support ADD COLUMN IF NOT EXISTS; we catch the error instead.
    _migrations = [
        "ALTER TABLE signals ADD COLUMN predicted_return_pct REAL NOT NULL DEFAULT 0.0",
        # Data-quality flag — NULL means clean; non-NULL records are excluded from
        # win-rate, PnL, model accuracy, and benchmark_analysis.
        # Values: 'test_artifact' | 'corrupted_bug:<reason>' | 'incomplete_data:<reason>'
        "ALTER TABLE signals ADD COLUMN quality_flag TEXT DEFAULT NULL",
        "ALTER TABLE trades  ADD COLUMN quality_flag TEXT DEFAULT NULL",
        # model_source — which generator wrote this signal.
        # 'custom' = KronosForecaster (M4); 'kronos-mini' = M13; 'kronos-base' = M14.
        # DEFAULT 'custom' keeps all existing M4 signals correct without any M4 change.
        "ALTER TABLE signals ADD COLUMN model_source TEXT NOT NULL DEFAULT 'custom'",
        # ATR at entry time — stored by M6 so M7 trailing stop uses the same
        # volatility reference throughout the position's life.
        "ALTER TABLE positions ADD COLUMN entry_atr REAL",
        # Highest mark price seen since entry (long) or lowest (short).
        # M7 advances this each 15-min cycle; trailing SL trails it by 1 × entry_atr.
        # Initialised to entry_price by M6.
        "ALTER TABLE positions ADD COLUMN running_extreme REAL",
        # Per-model capital tracking (M8 / M6).
        # NULL  = aggregate snapshot (sum of all models; historical rows keep NULL).
        # 'custom' | 'kronos-mini' | 'kronos-base' = model-specific snapshot.
        # Each model has its own KRONOS_STARTING_CAPITAL_INR pool (₹1L by default),
        # enabling independent capital-growth comparison across the three generators.
        "ALTER TABLE portfolio_snapshots ADD COLUMN model_source TEXT DEFAULT NULL",
        # Exchange-side bracket order IDs (live mode only).
        # NULL in paper mode — M7 WebSocket handles SL/TP for paper positions.
        # In live mode M6 places a stop-market SL + limit TP as separate reduce-only
        # orders immediately after entry fills and stores their IDs here.
        # M7 uses these to: (a) skip redundant WebSocket SL/TP checks, (b) cancel the
        # surviving leg when the other fills, (c) cancel+replace sl_order_id when the
        # trailing stop advances.
        "ALTER TABLE positions ADD COLUMN sl_order_id TEXT DEFAULT NULL",
        "ALTER TABLE positions ADD COLUMN tp_order_id TEXT DEFAULT NULL",
        # Pipeline regime version — see SIGNAL_REGIME_VERSION constant.
        # DEFAULT 1 tags all existing signals as v1 (pre-2026-05-31 rule set).
        # Generators write the current SIGNAL_REGIME_VERSION on every new signal
        # so benchmark_analysis.py can compare within a consistent rule set only.
        "ALTER TABLE signals ADD COLUMN regime_version INTEGER NOT NULL DEFAULT 1",
        # Horizon exit timestamp — Unix epoch when the model's prediction window ends.
        # = signal_timestamp + horizon_seconds (e.g. T+24H for 24H-horizon signals).
        # In paper mode M7 exits the position at this time regardless of P&L,
        # directly testing what the model actually predicted rather than ATR-based targets.
        # NULL on positions opened before this column existed (ignored by M7).
        "ALTER TABLE positions ADD COLUMN horizon_exit_at INTEGER DEFAULT NULL",
        # Absolute highest mark price seen during the position's life (all directions).
        # Tracked by M7 _update_position_price on every tick.
        # NULL until first M7 update — falls back to entry_price in display code.
        "ALTER TABLE positions ADD COLUMN running_high REAL DEFAULT NULL",
        # Absolute lowest mark price seen during the position's life (all directions).
        "ALTER TABLE positions ADD COLUMN running_low  REAL DEFAULT NULL",
        # Peak (highest) and trough (lowest) mark prices reached during the trade.
        # Copied from positions.running_high / running_low at exit time so they
        # survive the position row deletion. NULL for trades closed before this existed.
        "ALTER TABLE trades ADD COLUMN peak_price   REAL DEFAULT NULL",
        "ALTER TABLE trades ADD COLUMN trough_price REAL DEFAULT NULL",
        # Rejection reason — written by M5 when a signal is rejected so the
        # benchmark can distinguish over-filtering from legitimate blocks.
        # Values: 'confidence_gate' | 'return_floor' | 'funding_rate' |
        #         'blackout' | 'entry_cost' | 'position_cap' | 'stacking' |
        #         'correlation' | other M5 reason strings.
        # NULL on executed signals and on signals from before this column existed.
        "ALTER TABLE signals ADD COLUMN rejection_reason TEXT DEFAULT NULL",
        # Actual return at the signal's horizon — written back by benchmark_analysis
        # after the OHLCV candle at signal_timestamp + HORIZON_SECONDS is available.
        # = (close_at_horizon - close_at_signal) / close_at_signal × 100
        # Positive = price went up. Compare sign against direction for hit/miss.
        # NULL until resolved.
        "ALTER TABLE signals ADD COLUMN actual_return_pct REAL DEFAULT NULL",
        # Regime version stamp on portfolio snapshots — mirrors SIGNAL_REGIME_VERSION.
        # NULL on snapshots written before v5 (these are the v1–v4 archive).
        # v5+ snapshots have an explicit integer so dashboard/PM queries can
        # filter to the current regime for a clean capital baseline.
        "ALTER TABLE portfolio_snapshots ADD COLUMN regime_version INTEGER DEFAULT NULL",
    ]
    for _sql in _migrations:
        try:
            conn.execute(_sql)
            conn.commit()
        except Exception:
            pass  # column already exists — harmless

    # ── Regime change event log ────────────────────────────────────────────────
    # Writes a single regime_change event the first time this version's init_db
    # runs. Idempotent — subsequent calls are no-ops.
    try:
        already = conn.execute(
            """SELECT COUNT(*) FROM events
               WHERE event_type='regime_change'
                 AND data LIKE ?""",
            (f'%"version": {SIGNAL_REGIME_VERSION}%',),
        ).fetchone()[0]
        if not already:
            conn.execute(
                """INSERT INTO events
                       (module, event_type, severity, message, data, timestamp)
                   VALUES ('system', 'regime_change', 'info', ?, ?, strftime('%s','now'))""",
                (
                    f'Pipeline regime v{SIGNAL_REGIME_VERSION} activated',
                    json.dumps({
                        'version': SIGNAL_REGIME_VERSION,
                        'changes': [
                            'xrp_contract_size_fix: 1.0 XRP/contract (was 10.0 — fees ~10x inflated pre 2026-06-07)',
                            'per_asset_tpsl: TP/SL tuned per model×symbol via MFE/MAE grid search (0.25x–5.0x, 400 combos)',
                            'per_asset_halts: ETHUSD(mini), BTCUSD+XRPUSD(base), BNBUSD(mini-4h), ETHUSD+XRPUSD(base-4h)',
                            'M14_M15_reenabled: kronos-base-1h and kronos-mini-4h active with tuned configs',
                            'kronos_mini_xrp_halt_lifted: post-fix re-analysis confirmed TP=2.0x SL=0.25x optimal',
                        ],
                        'note': 'v1/v2/v3/v4 are historical reference — exclude from benchmark comparisons. '
                                'v5 is the primary benchmark dataset.',
                    }),
                ),
            )
            conn.commit()
    except Exception:
        pass  # events table may not exist yet on first-ever init — harmless

    conn.close()


def log_event(
    module: str,
    severity: str,
    event_type: str,
    message: str,
    data: dict = None,
) -> None:
    """Write a single event row to the events table."""
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO events (module, event_type, severity, message, data, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                module,
                event_type,
                severity,
                message,
                json.dumps(data) if data is not None else None,
                int(time.time()),
            ),
        )


def get_table_names() -> list[str]:
    """Return list of all table names in the database."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        return [r['name'] for r in rows]
