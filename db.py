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
            tds_deducted     REAL,                  -- 1% of sell notional (auto by exchange)
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

        -- ── Table 11: Shadow signals ─────────────────────────────────────────
        -- Predictions from foundation models (kronos-mini, kronos-base) running
        -- in shadow mode alongside the custom model. Used for week-6 benchmarking.
        -- Writes here ONLY — never touches signals, trades, or positions tables.
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
    ]
    for _sql in _migrations:
        try:
            conn.execute(_sql)
            conn.commit()
        except Exception:
            pass  # column already exists — harmless

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
