# Kronos Trading System — Complete Technical Documentation

**Generated:** 2026-05-27  
**Basis:** Full code review of every source file. No inference or assumptions — every fact stated here is directly traceable to a line of code.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Repository Layout](#2-repository-layout)
3. [Environment Variables](#3-environment-variables)
4. [Database Schema (db.py)](#4-database-schema-dbpy)
5. [Module 1 — Data Collection](#5-module-1--data-collection-01_data_collectionpy)
6. [Module 2 — Slippage Model](#6-module-2--slippage-model-02_slippage_modelpy)
7. [Module 3 — Macro Calendar](#7-module-3--macro-calendar-03_macro_calendarpy)
8. [Module 4 — Signal Generator](#8-module-4--signal-generator-04_signal_generatorpy)
9. [Module 5 — Risk Check](#9-module-5--risk-check-05_risk_checkpy)
10. [Module 6 — Execution](#10-module-6--execution-06_executionpy)
11. [Module 7 — Position Monitor](#11-module-7--position-monitor-07_position_monitorpy)
12. [Module 8 — Portfolio Manager](#12-module-8--portfolio-manager-08_portfolio_managerpy)
13. [Module 9 — Tax & TDS Tracker](#13-module-9--tax--tds-tracker-09_tax_trackerpy)
14. [Module 10 — Notification](#14-module-10--notification-10_notificationpy)
15. [Module 11 — Health Monitor](#15-module-11--health-monitor-11_health_monitorpy)
16. [Module 12 — Dashboard](#16-module-12--dashboard-dashboardpy)
17. [ML Model — KronosForecaster (kronos_model.py)](#17-ml-model--kronosforecaster-kronos_modelpy)
18. [Training Script (train_kronos.py)](#18-training-script-train_kronospy)
19. [Shadow Inference (shadow_inference.py)](#19-shadow-inference-shadow_inferencepy)
20. [Benchmark Analysis (benchmark_analysis.py)](#20-benchmark-analysis-benchmark_analysispy)
21. [Startup Script (start_dev_windows.ps1)](#21-startup-script-start_dev_windowsps1)
22. [Dependencies (requirements.txt)](#22-dependencies-requirementstxt)
23. [Macro Calendar (macro_calendar.json)](#23-macro-calendar-macro_calendarjson)
24. [Data Quality Management](#24-data-quality-management)
25. [Cron Schedule Summary](#25-cron-schedule-summary)
26. [Inter-Module Data Flow](#26-inter-module-data-flow)
27. [Security Architecture](#27-security-architecture)

---

## 1. System Overview

Kronos is a fully automated crypto futures trading system for Delta Exchange India. It operates in paper trading mode during a 6-week pre-live phase, then transitions to live trading. All components run as independent Python processes coordinated by a shared SQLite database.

**Exchange:** Delta Exchange India (`https://api.india.delta.exchange`, WebSocket `wss://socket.india.delta.exchange`)  
**Assets traded:** BTCUSD, ETHUSD, and a rotating Slot 3 (SOLUSD, BNBUSD, or XRPUSD selected weekly)  
**Signal horizon:** 24H (6 × 4H candles)  
**Starting capital:** ₹1,00,000 INR  
**Leverage:** 2× standard (3× hard ceiling)  
**Position sizing:** 10% margin per position  
**Stop loss:** 3% of portfolio value  
**Take profit:** 6% of portfolio value (R:R = 2.0)  
**Max hold:** 5 days  
**Max positions:** 3 simultaneous  
**Phase at documentation time:** Pre-live (KRONOS_PHASE=pre_live), PAPER_MODE=true

---

## 2. Repository Layout

```
kronos/
├── db.py                      # Database layer — 11 tables, migrations, helpers
├── 01_data_collection.py      # Module 1  — OHLCV, orderbook, funding rates
├── 02_slippage_model.py       # Module 2  — Liquidity regime + slippage estimates
├── 03_macro_calendar.py       # Module 3  — Macro event blackout checker
├── 04_signal_generator.py     # Module 4  — ML inference, signal creation
├── 05_risk_check.py           # Module 5  — 13-check risk gate, signal approval
├── 06_execution.py            # Module 6  — Order placement on Delta Exchange
├── 07_position_monitor.py     # Module 7  — 15-min exit condition checks
├── 08_portfolio_manager.py    # Module 8  — Portfolio value, drawdown alerts
├── 09_tax_tracker.py          # Module 9  — TDS, fees, funding, pnl_net
├── 10_notification.py         # Module 10 — Telegram + email alerts + commands
├── 11_health_monitor.py       # Module 11 — System health + asset win rate checks
├── dashboard.py               # Module 12 — Flask web dashboard (port 8050)
├── kronos_model.py            # KronosForecaster model definition (~1.4M params)
├── train_kronos.py            # Training script (Binance data, local GPU)
├── shadow_inference.py        # Shadow inference — foundation model comparison
├── benchmark_analysis.py      # Week-6 directional accuracy report
├── macro_calendar.json        # Macro event calendar (human-maintained, weekly)
├── start_dev_windows.ps1      # PowerShell launcher for all 12 modules
├── requirements.txt           # Python dependencies
├── .gitignore                 # Excludes .env, data/, models/, vendor/, Kronos/
├── mark_corrupted_data.py     # One-time script: flag corrupted historical data
├── fix_stuck_positions.py     # One-time script: fix stuck position statuses
└── data/
    ├── kronos.db              # SQLite database (WAL mode)
    ├── notifier_state.json    # Module 10 event cursor state
    └── reports/               # Generated tax and benchmark reports
```

**Excluded from git (`.gitignore`):**
- `.env`, `*.env`, `.env.*` — API keys, secrets
- `data/`, `*.db` — live database
- `models/`, `*.pt`, `*.pth`, `*.ckpt`, `*.bin`, `*.safetensors` — trained weights
- `__pycache__/`, `venv/`, `.venv/`, `env/`, `ENV/`
- `*.log`, `logs/`
- `vendor/`, `Kronos/` — third-party cloned repos
- `.claude/` — Claude Code local settings
- `debug_*.py`, `fix_*.py`, `check_*.py`, `verify_*.py`, `backfill_*.py` — one-off scripts

---

## 3. Environment Variables

All loaded via `os.environ.get()` with defaults. Store in `.env` file — **never commit to git**.

| Variable | Used by | Default | Description |
|---|---|---|---|
| `KRONOS_DB_PATH` | db.py | `./data/kronos.db` | SQLite database path |
| `KRONOS_API_KEY` | M6, M7, M8 | `''` | Delta Exchange API key (trade-only) |
| `KRONOS_API_SECRET` | M6, M7, M8 | `''` | Delta Exchange API secret |
| `KRONOS_PAPER_MODE` | M6, M7, M8, M11 | `'false'` | `'true'` = no real orders placed |
| `KRONOS_PHASE` | M8, M12 | `'pre_live'` | `'pre_live'` \| `'income'` \| `'compound'` |
| `KRONOS_LEVERAGE` | M6 | `'2.0'` | Default leverage (hard ceiling 3.0) |
| `KRONOS_USD_INR_RATE` | M6, M7, M8, M9 | `'84.0'` | USD/INR conversion rate (update manually) |
| `KRONOS_STARTING_CAPITAL_INR` | M8, M9, M10, M12 | `'100000.0'` | Starting capital in INR |
| `KRONOS_PORTFOLIO_VALUE_INR` | M6 | `'100000.0'` | Fallback when no portfolio snapshot exists |
| `KRONOS_MONTHLY_FIXED_COSTS_INR` | M8 | `'915.0'` | Monthly costs for survival benchmark |
| `KRONOS_MODEL_PATH` | M4 | `./models/kronos_model.pt` | Path to trained KronosForecaster weights |
| `KRONOS_SEQ_LEN` | M4, training | `'96'` | Input sequence length (96 × 4H = 16 days) |
| `KRONOS_PRED_LEN` | M4, training | `'6'` | Prediction horizon (6 × 4H = 24H) |
| `KRONOS_MAX_PRED_RETURN` | M4 | `'20.0'` | Cap on predicted return % (clip extremes) |
| `KRONOS_SLOT3_SYMBOL` | M4 | `''` | Override for Slot 3 symbol |
| `KRONOS_CB_SPREAD_PCT` | M5 | `'1.0'` | Circuit breaker spread threshold % |
| `KRONOS_FUNDING_EXIT_ENABLED` | M7 | `'false'` | Enable funding rate exit (disabled) |
| `KRONOS_TELEGRAM_BOT_TOKEN` | M10 | `''` | Telegram bot token |
| `KRONOS_TELEGRAM_CHAT_ID` | M10 | `''` | Telegram chat ID (integer) |
| `KRONOS_EMAIL_FROM` | M10 | `''` | Gmail sender address |
| `KRONOS_EMAIL_TO` | M10 | `''` | Email recipient |
| `KRONOS_EMAIL_APP_PASSWORD` | M10 | `''` | Gmail app password |
| `KRONOS_NOTIFIER_STATE_PATH` | M10, M11 | `./data/notifier_state.json` | Notification cursor state file |
| `KRONOS_MACRO_CALENDAR` | M3 | `./macro_calendar.json` | Path to macro calendar JSON |
| `KRONOS_DASHBOARD_PORT` | M12 | `8050` | Flask dashboard port |
| `KRONOS_SHADOW_MINI_CONTEXT` | Shadow | `1024` | Candles fed to Kronos-mini |
| `KRONOS_SHADOW_BASE_CONTEXT` | Shadow | `512` | Candles fed to Kronos-base |
| `KRONOS_SHADOW_SAMPLE_COUNT` | Shadow | `50` | Probabilistic samples per model cycle |
| `KRONOS_SHADOW_DEVICE` | Shadow | `'cpu'` | `'cpu'` (Windows TDR) \| `'cuda'` (Linux) |
| `KRONOS_SUPERVISORD_PREFIX` | M11 | `'kronos-'` | Supervisord process name prefix |
| Various `KRONOS_HM_*` | M11 | See M11 section | Health monitor thresholds |

---

## 4. Database Schema (db.py)

**Location:** `DB_PATH = os.environ.get('KRONOS_DB_PATH', './data/kronos.db')`  
**Mode:** WAL journal, foreign keys ON, busy_timeout=5000ms  
**Connection:** `get_connection()` context manager — commits on success, rolls back on exception, closes on exit.

### Table 1: ohlcv
Historical OHLCV candles per symbol per timeframe.
```sql
id INTEGER PK, symbol TEXT, timeframe TEXT, timestamp INTEGER,
open REAL, high REAL, low REAL, close REAL, volume REAL,
created_at INTEGER DEFAULT (strftime('%s','now')),
UNIQUE(symbol, timeframe, timestamp)
```
Index: `idx_ohlcv_sym_tf_ts` on (symbol, timeframe, timestamp DESC)

### Table 2: orderbook_snapshots
L1 bid/ask snapshots every 15 minutes from WebSocket.
```sql
id INTEGER PK, symbol TEXT, timestamp INTEGER,
best_bid REAL, best_ask REAL, bid_size REAL, ask_size REAL,
spread REAL, mark_price REAL, open_interest REAL, funding_rate REAL,
created_at INTEGER
```
Index: `idx_ob_sym_ts` on (symbol, timestamp DESC)

### Table 3: funding_rates
Historical 8H funding rates, fetched every 8H via REST.
```sql
id INTEGER PK, symbol TEXT, timestamp INTEGER, rate REAL,
created_at INTEGER, UNIQUE(symbol, timestamp)
```
Index: `idx_fr_sym_ts` on (symbol, timestamp DESC)

### Table 4: signals
All signals generated — every status.
```sql
id INTEGER PK, symbol TEXT, direction TEXT, confidence REAL,
horizon TEXT, status TEXT DEFAULT 'pending',
rejection_reason TEXT, predicted_return_pct REAL NOT NULL DEFAULT 0.0,
signal_timestamp INTEGER, created_at INTEGER,
quality_flag TEXT DEFAULT NULL   -- added via migration
```
Status values: `'pending'` | `'approved'` | `'rejected'` | `'executed'` | `'expired'`  
Indexes: `idx_signals_sym_ts`, `idx_signals_status`

### Table 5: trades
Every executed trade — primary tax record, retained indefinitely.
```sql
id INTEGER PK, signal_id INTEGER REFERENCES signals(id),
symbol TEXT, direction TEXT, entry_price REAL, exit_price REAL,
size_contracts REAL, notional_value REAL,
entry_timestamp INTEGER, exit_timestamp INTEGER, exit_reason TEXT,
pnl_gross REAL, pnl_net REAL, tds_deducted REAL,
funding_paid REAL DEFAULT 0.0, funding_received REAL DEFAULT 0.0,
fees REAL, status TEXT DEFAULT 'open', created_at INTEGER,
quality_flag TEXT DEFAULT NULL   -- added via migration
```
Status: `'open'` | `'closed'` | `'cancelled'`  
Exit reasons: `'take_profit'` | `'stop_loss'` | `'time_limit'` | `'funding_cost'` | `'drawdown_alert'` | `'forced_override'` | `'manual'`

### Table 6: positions
Live state of open positions. One row per open trade.
```sql
id INTEGER PK, trade_id INTEGER UNIQUE REFERENCES trades(id),
symbol TEXT, direction TEXT, entry_price REAL, current_price REAL,
size_contracts REAL, notional_value REAL, margin_used REAL, leverage REAL,
stop_loss_price REAL, take_profit_price REAL,
entry_timestamp INTEGER, max_hold_until INTEGER,
unrealised_pnl REAL, status TEXT DEFAULT 'open', created_at INTEGER
```
Status: `'open'` | `'closing'`

### Table 7: portfolio_snapshots
Portfolio state every 4H. Used by Module 6 for position sizing.
```sql
id INTEGER PK, timestamp INTEGER, total_value REAL,
active_margin REAL, available_margin REAL,
unrealised_pnl REAL DEFAULT 0.0, drawdown_pct REAL DEFAULT 0.0,
peak_value REAL, open_positions INTEGER DEFAULT 0,
phase TEXT DEFAULT 'pre_live', created_at INTEGER
```

### Table 8: tds_log
Every TDS deduction with UTR reference for ITR reconciliation.
```sql
id INTEGER PK, trade_id INTEGER REFERENCES trades(id),
symbol TEXT, sell_notional REAL, tds_amount REAL,
utr_reference TEXT, timestamp INTEGER, created_at INTEGER
```

### Table 9: tax_reserve
Running balance of the 30% gross profit tax reserve. Every credit and debit logged.
```sql
id INTEGER PK, transaction_type TEXT, amount REAL,
balance_after REAL, reference_trade_id INTEGER REFERENCES trades(id),
notes TEXT, timestamp INTEGER, created_at INTEGER
```
Transaction types: `'reserve'` (credit) | `'release'` | `'payment'`

### Table 10: events
All system events — heartbeats, alerts, errors, fill windows, everything.
```sql
id INTEGER PK, module TEXT, event_type TEXT, severity TEXT DEFAULT 'info',
message TEXT, data TEXT (JSON blob), timestamp INTEGER, created_at INTEGER
```
Severity: `'debug'` | `'info'` | `'warning'` | `'error'` | `'critical'`  
Indexes: `idx_events_module_ts`, `idx_events_type`

### Table 11: shadow_signals
Foundation model predictions (read-only; never touches live pipeline).
```sql
id INTEGER PK, symbol TEXT, model_name TEXT, direction TEXT,
confidence REAL, predicted_return REAL, context_candles INTEGER,
signal_timestamp INTEGER, created_at INTEGER
```
Model names: `'kronos-mini'` | `'kronos-base'`

### Column Migrations (idempotent)
Run on every `init_db()` call via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (caught via try/except):
1. `signals.predicted_return_pct REAL NOT NULL DEFAULT 0.0`
2. `signals.quality_flag TEXT DEFAULT NULL`
3. `trades.quality_flag TEXT DEFAULT NULL`

### Helper Functions
- `log_event(module, severity, event_type, message, data=None)` — inserts one events row
- `get_table_names()` — returns list of all table names

---

## 5. Module 1 — Data Collection (`01_data_collection.py`)

**Purpose:** Continuously collects market data from Delta Exchange and stores it in the database.

### Constants
```python
DELTA_WS_URL        = 'wss://socket.india.delta.exchange'
DELTA_REST_BASE     = 'https://api.india.delta.exchange'
OHLCV_BACKFILL_LIMIT = 500          # candles on startup (~83 days of 4H)
```

### Active symbols
`['BTCUSD', 'ETHUSD', 'SOLUSD', 'BNBUSD', 'XRPUSD']`

### WebSocket (live data)
- Subscribes to `v2/ticker` and `all_trades` channels for all 5 symbols
- `_ticker_cache` dict per symbol — stores `best_bid`, `best_ask`, `spread`, `mark_price`, `open_interest`, `funding_rate`
- `_fills_buffer` dict per symbol — list of fill events `{price, size, side}` accumulated for 15-min flush
- `_ws_ready` asyncio.Event — set when all 5 symbols have received at least one ticker event

### Reconnection
Exponential backoff: 1s → 2s → 4s → … → 60s maximum. Resets to 1s on successful connection.

### OHLCV Collection (`_job_ohlcv`)
**Cron:** `00:02, 04:02, 08:02, 12:02, 16:02, 20:02 UTC`

- REST call to `/v2/history/candles?symbol={sym}&resolution=60&limit={LIMIT}` for each symbol
- 60-minute resolution candles (Delta Exchange uses minutes, not '4h')
- Inserts with `INSERT OR IGNORE` — idempotent, duplicate timestamps silently discarded
- Startup: calls immediately on first run with `OHLCV_BACKFILL_LIMIT=500` candles

### Orderbook Snapshots (`_job_orderbook`)
**Cron:** Every 15 minutes (`:02, :17, :32, :47` past each hour)

- Reads from `_ticker_cache` (populated by WebSocket)
- Computes `spread = best_ask - best_bid`
- Writes one row per symbol per snapshot to `orderbook_snapshots`
- Only writes when WebSocket is ready (waits for `_ws_ready` event)

### Funding Rate Collection (`_job_funding`)
**Cron:** `00:02, 08:02, 16:02 UTC` (every 8H at exchange settlement times)

- REST call to `/v2/products/{symbol}/funding/history`
- Stores `rate` (raw 8H rate, e.g. 0.0001 = 0.01%) in `funding_rates`
- `INSERT OR IGNORE` — idempotent

### 15-min Fill Window Flush (`_flush_fills`)
- Called every 15 minutes alongside orderbook snapshot job
- Aggregates `_fills_buffer` per symbol:
  - `vwap` = Σ(price × size) / Σ(size)
  - `min_price`, `max_price`, `avg_size`
  - `fill_count` = number of fills
  - `taker_buy_ratio` = buy fills / total fills
- Writes aggregated data as `event_type='fill_window'` JSON blob to events table
- Module 2 reads these events for slippage model calibration

---

## 6. Module 2 — Slippage Model (`02_slippage_model.py`)

**Purpose:** Estimates per-symbol liquidity regime and expected order slippage from self-collected data.

### Key constants
```python
MIN_HISTORY_WINDOWS  = 100      # fill_window rows needed (~25 hours)
REGIME_OI_HIGH_MULT  = 1.5      # OI spike threshold: current > rolling_mean × 1.5
REGIME_FUNDING_LOW_LIQ  = 0.003  # |rate| >= 0.30%/8H → low liquidity
REGIME_FUNDING_HIGH_LIQ = 0.0005 # |rate| <= 0.05%/8H → high liquidity candidate
ROLLING_WINDOW_ROWS  = 672      # 7 days of 4 snapshots/hour
SPREAD_DEGRADED_BPS  = 10.0     # above → fill probability reduced
SPREAD_EXTREME_BPS   = 30.0     # above → fill probability severely reduced
```

### SlippageEstimate dataclass
Fields: `symbol`, `timestamp`, `regime`, `current_spread_bps`, `estimated_slippage_bps`, `fill_probability_4h`, `regime_slip_p50_bps`, `regime_slip_p95_bps`, `data_points`, `regime_data_points`, `is_calibrated`

### Regime Classification
```
Low liquidity:  OI > rolling_mean × 1.5, OR |funding_rate| >= 0.003
High liquidity: OI not elevated AND |funding_rate| <= 0.0005
Normal:         everything else
```

### Slippage Estimation
1. Fetch latest orderbook snapshot for `current_spread_bps`
2. Fetch rolling 672 orderbook snapshots for `rolling_oi_mean`
3. Classify current regime
4. Fetch rolling 672 `fill_window` events — compute `data_points`, determine `is_calibrated`
5. For each fill_window, binary-search nearest orderbook snapshot by timestamp
6. If matched snapshot is in target regime: compute `|VWAP - mark_price| / mark_price × 10000` (deviation in bps)
7. p50 and p95 of regime-matched deviations → `estimated_slippage_bps`
8. Fallback when no regime data: `p50 = current_spread_bps × 0.5`, `p95 = current_spread_bps`

### Fill Probability
Base by regime: `high_liquidity=0.85`, `normal=0.75`, `low_liquidity=0.55`  
Penalties: spread ≥ 30bps → ×0.60; spread ≥ 10bps → ×0.85; avg_fills < 5 → ×0.80  
Boost: avg_fills > 50 → ×1.05 (capped at 0.95)  
Final clamp: [0.10, 0.95]

### Schedule
Updates every 15 minutes (same cadence as Module 1 snapshot). Writes `event_type='slippage_estimate'` to events table after each update (severity='debug').

### API for downstream modules
- `get_estimate(symbol)` — returns calibrated estimate or `None` (before `MIN_HISTORY_WINDOWS`)
- `get_raw_estimate(symbol)` — uncalibrated, for diagnostics only
- `get_all_estimates()` — dict of all calibrated symbols

---

## 7. Module 3 — Macro Calendar (`03_macro_calendar.py`)

**Purpose:** Blocks new signal entries during macro event windows.

### Blackout rule
Symmetric ±4H window around each event timestamp. Blocked if `|event_timestamp - now| <= 14400` seconds (4H). This protects both pre-event uncertainty (4H before) and post-event volatility (4H after). System resumes automatically once `now > event_timestamp + 4H`.

### MacroStatus dataclass
`is_blocked`, `hours_until_next`, `blocking_event_name`, `next_event_name`, `next_event_time`, `checked_at`

### Calendar file
Path: `KRONOS_MACRO_CALENDAR` env var, default `./macro_calendar.json`  
Format: JSON array of objects with `name` (str), `timestamp` (int UTC epoch), `type` (str), `notes` (optional)  
Returns empty list (no block) if file not found — warning logged but system continues.

### Event types in calendar
`"fomc"` | `"cpi"` | `"nfp"` | `"rbi_mpc"` | `"jackson_hole"` | `"india_budget"` | `"meta"`

Module 3 is a library — it has no scheduler. `MacroCalendar().check()` is called by Module 5 on every risk check cycle.

---

## 8. Module 4 — Signal Generator (`04_signal_generator.py`)

**Purpose:** Runs the custom ML model every 4H to generate directional signals.

### Key constants
```python
SEQ_LEN    = int(os.environ.get('KRONOS_SEQ_LEN',  '96'))   # 96 × 4H = 16 days
PRED_LEN   = int(os.environ.get('KRONOS_PRED_LEN', '6'))    # 6 × 4H = 24H
HORIZON    = '24h'
ATR_PERIOD = 14
MAX_PREDICTED_RETURN_PCT = 20.0   # clip extremes
EXTREME_FUNDING_THRESHOLD = 0.003  # 0.3%/8H pre-signal block
SLOT1_SYMBOL = 'BTCUSD'
SLOT2_SYMBOL = 'ETHUSD'
SLOT3_CANDIDATES = ['SOLUSD', 'BNBUSD', 'XRPUSD']
```

### KronosInference class
Loads the trained model from `KRONOS_MODEL_PATH` (default `./models/kronos_model.pt`).  
Load order: `torch.jit.load()` first, then `torch.load()` as fallback.  
Device: auto-detects CUDA, falls back to CPU.

**predict(ohlcv_rows) → (direction, confidence, predicted_return_pct)**

Preprocessing (RevIN / instance normalisation):
1. Extract OHLCV as float32 array `[SEQ_LEN, 5]`
2. Compute per-channel mean and std over input window
3. Normalise: `x_norm = (x - mean) / (std + 1e-8)`
4. Run forward pass: `model(x_norm)` → `[1, PRED_LEN, 5]`
5. Denormalise output: `pred = pred_norm * std + mean`
6. Extract predicted close prices: `pred_closes = pred[0, :, 3]` (index 3 = close)

Confidence formula:
```python
atr_pct     = _compute_atr_pct(ohlcv_rows)  # mean true range over ATR_PERIOD / final_close
pred_return = (pred_closes[-1] - current_close) / current_close
base_conf   = min(1.0, abs(pred_return) / (2.0 * atr_pct))
n_agree     = number of pred_closes steps agreeing with final direction
consistency = n_agree / PRED_LEN
confidence  = round(base_conf * consistency, 4)
```

ATR calculation:
```python
for i in 1..ATR_PERIOD:
    TR = max(high[i] - low[i],
             abs(high[i] - prev_close[i]),
             abs(low[i]  - prev_close[i]))
atr = mean(TRs)
atr_pct = atr / final_close
```

### Slot 3 selection
Runs weekly: **Sunday 00:03 UTC** (`CronTrigger(day_of_week=6, hour=0, minute=3)`)

For each SLOT3_CANDIDATES symbol:
1. Check asset exclusion from events table (M11 `asset_exclusion` events)
2. Fetch 120 candles from `ohlcv`
3. Run `KronosInference.predict()` → confidence
4. Select symbol with highest confidence
5. Write `event_type='slot3_selection'` event with payload `{slot3_symbol, confidence, ranking}`

### Signal generation (`_job_generate`)
**Cron:** `00:05, 04:05, 08:05, 12:05, 16:05, 20:05 UTC`

For each of 3 slots:
1. Check asset exclusion (reads M11 `asset_exclusion` events)
2. Check extreme funding rate: if `|current_rate| >= 0.003` → log and skip
3. Fetch `SEQ_LEN + ATR_PERIOD + 1` = 111 candles from `ohlcv`
4. Run `KronosInference.predict()`
5. Clip `predicted_return_pct` to `MAX_PREDICTED_RETURN_PCT = 20.0`
6. Insert signal row with status='pending', direction, confidence, predicted_return_pct
7. After all slots: fire `asyncio.create_task(self._run_shadow())` (background, non-blocking)

**Shadow fire is non-blocking.** M4 returns immediately after `generate()`. Shadow runs in background executor — does not delay M5 at :07.

### Asset exclusion check
Reads events table for `event_type='asset_exclusion'` where `json_extract(data, '$.symbol') = symbol`. Takes the most recent such event. If `excluded=True` → symbol is excluded. If `excluded=False` → reinstated.

---

## 9. Module 5 — Risk Check (`05_risk_check.py`)

**Purpose:** 13-check gate that approves or rejects pending signals.

### Key constants
```python
CONSECUTIVE_LOSS_LIMIT = 3
ROUND_TRIP_FEES_PCT    = 0.0017   # 0.04%×1.18 + 0.10%×1.18
MAX_POSITIONS          = 3
FULL_SIZE_PCT          = 10.0     # % of portfolio per position
REDUCED_SIZE_PCT       = 5.0      # reduced at Yellow Alert or correlation
CIRCUIT_BREAKER_SPREAD_PCT = float(os.environ.get('KRONOS_CB_SPREAD_PCT', '1.0'))
CORR_HIGH_THRESHOLD    = 0.85
CORR_MED_THRESHOLD     = 0.70
CORR_PERIOD_CANDLES    = 42       # ~7 days of 4H candles
```

### 13 Checks (in priority order)

**1. Expiry check** — signal must be `< 20 minutes` old (`signal_timestamp > now - 1200`). Rejects stale pending signals. Runs once, not per signal.

**2. Forced override check** — if any `forced_override` event exists with no subsequent `forced_override_cleared` event → reject all signals as `forced_override_active`.

**3. Consecutive losses check** — queries the 3 most recent closed clean trades for the symbol (`quality_flag IS NULL`). If all 3 are losses (`pnl_gross <= 0`) and no `forced_override_cleared` event exists AFTER the most recent losing exit → reject as `consecutive_loss_limit`.  
Dedup logic: if `forced_override_cleared.timestamp > most_recent_exit.timestamp` → block is lifted.

**4. Macro calendar check** — calls `MacroCalendar().check()`. If `is_blocked=True` → reject as `macro_blackout:{event_name}`.

**5. Funding settlement blackout** — blocks entry in the window `[-2H, +2H]` around each 8H funding settlement (00:00, 08:00, 16:00 UTC). Computed as `now mod 28800 < 7200 OR now mod 28800 > 21600`.

**6. Entry funding cost block** — reads most recent `funding_rate` for the symbol. Computes expected funding cost over 24H horizon: `|rate| × (24/8) = rate × 3`. Also computes total round-trip cost: `funding_cost + ROUND_TRIP_FEES_PCT`. If `predicted_return_pct != 0.0` (non-legacy signal): blocks if `|predicted_return_pct / 100| < round_trip_cost`. Entry funding threshold: `float(os.environ.get('KRONOS_ENTRY_FUNDING_THRESHOLD', '0.005'))` (0.5%/8H hard block).

**7. Stop loss blackout (§19.2)** — checks events table for `stop_loss_exit` on the same symbol within the last 4H (`timestamp > now - 14400`). If found → reject as `stop_loss_blackout:{symbol}`.

**8. Asset exclusion** — same logic as M4's `_load_exclusions()`. Reads most recent `asset_exclusion` event per symbol. If `excluded=True` → reject.

**9. Circuit breaker** — reads latest `orderbook_snapshot` for spread. If `spread_pct = spread / mark_price × 100 > CIRCUIT_BREAKER_SPREAD_PCT (1.0%)` → reject as `circuit_breaker:spread_too_wide`.

**10. Alert level check** — reads most recent alert event from events table:
- `alert_red` → reject all new entries
- `alert_orange` → reject all new entries  
- `alert_yellow` → reject Slot 3 signals only; allow Slot 1 and 2 at `REDUCED_SIZE_PCT=5%`
- Green → proceed normally

**11. Position cap** — counts open positions. If `open >= MAX_POSITIONS (3)` → reject as `position_cap`.

**12. Confidence threshold** — base threshold `CONF_THRESHOLD = 0.25`. Win rate check: queries `win_rate_alert_7d` events in last 4H cycle period. If found: reads threshold adjustment from payload (`threshold_adjustment`), adds to base. Rejects if `confidence < adjusted_threshold`.

**13. Correlation check** — for each existing open position symbol, fetches `CORR_PERIOD_CANDLES=42` candles from `ohlcv` and computes Pearson correlation with the new signal's symbol.
- If both signal and existing position are in the same direction AND correlation > 0.85 → reject as `high_correlation:{symbol}`
- If same direction AND 0.70 ≤ correlation ≤ 0.85 → approve but set `size_cap_pct=REDUCED_SIZE_PCT=5%`
- `combined_margin_cap_pct` is set on all-same-direction slots (10% + 5% + 5% = 20% max combined margin)

### Approval
If all 13 checks pass: `UPDATE signals SET status='approved'`. Writes `event_type='risk_check'` event with full payload including `size_cap_pct`, `combined_margin_cap_pct`.

### Win rate tracking
After approving signals: queries last 28 4H cycles (7 days) of closed clean signals (non-rejected, non-expired, `quality_flag IS NULL`). Computes win rate. If < 0.55 → writes `win_rate_alert_7d` event with `threshold_adjustment = 0.05 × floor((0.55 - win_rate) / 0.05)`.

### Cron
`00:07, 04:07, 08:07, 12:07, 16:07, 20:07 UTC` (2 minutes after M4)

---

## 10. Module 6 — Execution (`06_execution.py`)

**Purpose:** Places and manages limit orders on Delta Exchange.

### Key constants
```python
MARGIN_PCT       = 0.10        # 10% margin per position
LEVERAGE_DEFAULT = float(os.environ.get('KRONOS_LEVERAGE', '2.0'))
MAX_LEVERAGE     = 3.0         # hard ceiling
SL_PCT           = 0.03        # 3% of portfolio
TP_PCT           = 0.06        # 6% of portfolio
MIN_RR_RATIO     = 2.0
FILL_TIMEOUT_SEC = 4 * 3600   # 4H fill timeout
MAX_HOLD_DAYS    = 5
RETRY_DELAY_SEC  = 30
```

### Default contract sizes (fallback if `load_markets()` fails)
```python
'BTCUSD': 0.001,  'ETHUSD': 0.01,  'SOLUSD': 1.0,
'BNBUSD': 0.1,    'XRPUSD': 10.0
```

### Position sizing formula
```python
margin_inr     = portfolio_value × MARGIN_PCT           (or size_cap_pct from M5)
notional_inr   = margin_inr × leverage
size_contracts = floor(notional_inr / (contract_size × mark_price × USD_INR_RATE) × 1_000_000) / 1_000_000
```
`portfolio_value` from latest `portfolio_snapshots` row; fallback: `KRONOS_PORTFOLIO_VALUE_INR`  
`mark_price` from latest `orderbook_snapshots` row.

### SL/TP prices
```python
sl_dist = (portfolio_value × 0.03) / (size_contracts × contract_size × USD_INR_RATE)
tp_dist = (portfolio_value × 0.06) / (size_contracts × contract_size × USD_INR_RATE)
# Long:  sl_price = entry - sl_dist,  tp_price = entry + tp_dist
# Short: sl_price = entry + sl_dist,  tp_price = entry - tp_dist
```
R:R validation: `tp_dist / sl_dist >= 2.0 - 1e-6` (always ~2.0 by design).

### Combined margin cap
If M5 passed `combined_margin_cap_pct`: checks `SUM(margin_used)` of all open positions. Available = `cap_inr - existing`. `margin_inr = min(margin_inr, available)`. If available ≤ 0 → `ExecutionSkipped`.

### Paper mode
Simulates immediate fill: writes `trades` and `positions` rows directly, no API call. Sets `status='open'` on trade and position.

### Live mode
1. `exchange.create_order(symbol=ccxt_sym, type='limit', side=side, amount=size, price=entry_price)`
2. On `ccxt.NetworkError`: retry once after 30s
3. On second failure: reject signal (`api_timeout_both_retries`), halt further processing this cycle (returns `True`)
4. On `ccxt.ExchangeError`: reject signal, continue

### Fill timeout
`DateTrigger` job at `now + 4H` per order. Calls `exchange.fetch_order()`:
- Fully filled → update `entry_price` to average fill price in both `trades` and `positions`
- Partial or unfilled → cancel remainder, mark trade `'cancelled'`, delete position row, mark signal `'expired'`

### Order processing
Processes approved signals in `signal_timestamp ASC` order. `_get_risk_data()` reads the `risk_check` event for `size_cap_pct` and `combined_margin_cap_pct`.

### Cron
`00:09, 04:09, 08:09, 12:09, 16:09, 20:09 UTC`

---

## 11. Module 7 — Position Monitor (`07_position_monitor.py`)

**Purpose:** Checks all open positions every 15 minutes. Triggers exits.

### Key constants
```python
FUNDING_EXIT_ENABLED   = False    # disabled by default (env: KRONOS_FUNDING_EXIT_ENABLED)
FUNDING_COST_THRESHOLD = 0.001    # 0.1%/8H — only if FUNDING_EXIT_ENABLED=true
STOP_LOSS_BLACKOUT_SEC = 4 * 3600
RETRY_DELAY_SEC        = 30
```

### Exit condition checks (priority order per position)

**1. Stop loss** — market close order
- Long: `mark_price <= stop_loss_price`
- Short: `mark_price >= stop_loss_price`
- After exit: writes `stop_loss_exit` event with `blackout_until = exit_ts + 14400`

**2. Take profit** — limit close order at `take_profit_price`
- Long: `mark_price >= take_profit_price`
- Short: `mark_price <= take_profit_price`
- Writes `take_profit_exit` event

**3. Time limit** — market close, 5-day hard exit
- `time.time() >= max_hold_until`
- Writes `time_limit_exit` event

**4. Funding cost exit** — disabled by default
- When enabled: checks funding rate from `funding_rates` where `timestamp > entry_timestamp` (post-entry settlement)
- Long: `funding_rate > 0.001`; Short: `funding_rate < -0.001`
- Writes `funding_cost_exit` event

**Why funding exit is disabled:** Moving the gate to M5 entry-side is correct. Exiting an open 24H-signal position early due to funding guarantees a loss without giving the signal time to play out.

### Position update each cycle
Regardless of exit, updates `current_price` and `unrealised_pnl` from latest `mark_price` in `orderbook_snapshots`.

### PnL computation (gross INR)
```python
price_diff = exit_price - entry_price
direction_mul = +1 (long) or -1 (short)
pnl_gross = direction_mul × price_diff × size_contracts × contract_size × USD_INR_RATE
```

### Database update on exit
```sql
UPDATE trades SET exit_price=?, exit_timestamp=?, exit_reason=?, pnl_gross=?, status='closed' WHERE id=?
UPDATE positions SET status='closing' WHERE id=?
```
Position status becomes `'closing'` (not deleted). Module 9 processes it from `trades` not `positions`.

### Close order (live mode)
Uses `reduceOnly: True` to prevent accidental position flip.  
On partial fill: cancels remainder, updates `positions.size_contracts` to remaining, returns `None` (deferred to next 15-min cycle).

### Cron
Every 15 minutes: `minute='0,15,30,45'`

---

## 12. Module 8 — Portfolio Manager (`08_portfolio_manager.py`)

**Purpose:** Tracks portfolio value and peak, fires drawdown alerts, writes 4H snapshots, computes monthly withdrawals.

### Key constants
```python
STARTING_CAPITAL_INR    = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', '100000.0'))
MONTHLY_FIXED_COSTS_INR = float(os.environ.get('KRONOS_MONTHLY_FIXED_COSTS_INR', '915.0'))
YELLOW_ALERT_PCT = 5.0
ORANGE_ALERT_PCT = 10.0
RED_ALERT_PCT    = 15.0
```

### Portfolio value formula
```python
total_value = STARTING_CAPITAL_INR
            + SUM(pnl_gross FROM trades WHERE status='closed')
            + SUM(unrealised_pnl FROM positions WHERE status='open')
```
Note: `unrealised_pnl` is written by Module 7 each 15-min cycle.

### Peak and drawdown
```python
peak_value   = MAX(peak_value) from portfolio_snapshots; fallback = STARTING_CAPITAL_INR
new_peak     = max(peak_value, total_value)
drawdown_pct = max(0.0, (new_peak - total_value) / new_peak × 100)
```

### Alert actions
| Level | Threshold | Action |
|---|---|---|
| Yellow | ≥ 5% | Reduce all open positions by 50% (market order); block new Slot 3 entries (M5 reads event) |
| Orange | ≥ 10% | Close Slot 3 position at market; block all new entries (M5 reads event) |
| Red | ≥ 15% | Close ALL positions at market; write `forced_override` event; system halted pending human review |

De-escalation: when drawdown recovers below current threshold → writes `alert_cleared` (to green) or `alert_{lower_level}` (partial). De-escalation writes event only — does NOT re-open positions.

### 4H portfolio snapshot
Written at: hour `∈ {0,4,8,12,16,20}` AND minute=3 UTC (i.e., `00:03, 04:03, 08:03, 12:03, 16:03, 20:03 UTC`)  
Also written when `|total_value - last_snapshot_value| / last_snapshot_value >= 0.10` (≥10% change)

### Monthly withdrawal calculation (last calendar day)
Allocation formula (when `net_before_tax > 0`):
```
tax_reserve_credit = gross_pnl × 0.30   (30% of gross — earmarked for ITR)
net_profit         = gross_pnl × 0.70   (gross minus tax reserve)
system_retention   = net_profit × 0.20  (20% of net)
human_withdrawal   = net_profit × 0.80  (80% of net — human-initiated only, never automated)
```
Survival benchmark: `net_before_tax = pnl_net_sum - MONTHLY_FIXED_COSTS_INR`. If ≤ 0 → no withdrawal eligible.  
Writes credit to `tax_reserve` table: `INSERT INTO tax_reserve (transaction_type='reserve', amount, balance_after, ...)`  
Writes `withdrawal_calculation` event. **Human must initiate withdrawal via Telegram `/withdraw` command.**

Recuperation milestones:
- 50%: `cumulative_withdrawn >= STARTING_CAPITAL_INR × 0.5` → `recuperation_milestone` event
- 100%: `cumulative_withdrawn >= STARTING_CAPITAL_INR` → `recuperation_milestone` event

### Weekly R:R audit (every Monday 00:03 UTC)
Queries last 7 days of closed trades joined to positions for `stop_loss_price`.  
`R:R per trade = |exit_price - entry_price| / |entry_price - stop_loss_price|`  
Writes `rr_alert_7d` event if average R:R < 1.5 (minimum for positive expectancy at 40% win rate).

### Cron
`minute='3,18,33,48'` (3 minutes after Module 7 at :00,:15,:30,:45)

---

## 13. Module 9 — Tax & TDS Tracker (`09_tax_tracker.py`)

**Purpose:** Computes TDS, fees, funding, and pnl_net for every closed trade. Generates tax reports.

### Tax rates (Indian law)
```python
TDS_RATE       = 0.01     # 1% TDS on sell notional > INR 10,000
TDS_THRESHOLD  = 10000.0  # INR minimum
TAX_RATE       = 0.30     # 30% flat on VDA gains (no loss offset)
GST_RATE       = 0.18     # 18% GST on exchange fees
MAKER_FEE_RATE = 0.0004   # 0.04% limit orders
TAKER_FEE_RATE = 0.0010   # 0.10% market orders
```

### Taker exit reasons (market orders)
`'stop_loss'`, `'time_limit'`, `'funding_cost'`, `'drawdown_alert'`, `'forced_override'`, `'manual'`  
All others (take_profit) use maker fee.

### Per-trade computation
```python
sell_notional  = size × contract_size × exit_price × USD_INR_RATE
entry_notional = size × contract_size × entry_price × USD_INR_RATE
tds_deducted   = sell_notional × 0.01  if sell_notional > 10000 else 0.0
fees           = (entry_notional × MAKER_FEE_RATE + sell_notional × exit_fee_rate) × 1.18
funding_paid, funding_received = computed from funding_rates table
pnl_net        = pnl_gross - tds_deducted - fees - funding_paid
```

### Funding computation per trade
Queries `funding_rates WHERE symbol=? AND timestamp > entry_timestamp AND timestamp <= exit_timestamp + 130`.  
Each row = one 8H settlement. Notional approximated as entry notional for full hold period.  
`+130s grace` accounts for Module 1 writing rates at :02 past settlement hour.  
Long pays positive rate; short pays negative rate. Non-paying direction receives.

### Trigger
Processes all `trades WHERE status='closed' AND pnl_net IS NULL` every 15 minutes.

### Periodic reports

**Monthly tax summary (last calendar day):**
```
net_taxable_income = pnl_gross + funding_received - funding_paid
tax_liability      = max(0, net_taxable × 0.30)
```
Writes `monthly_tax_summary` event with full breakdown.  
Writes `recuperation_ledger` event with cumulative withdrawal tracking.

**Advance tax alert (March 15 UTC):**  
Sums `tax_liability_30pct` and `tds_advance_credit` from all monthly summaries in current Indian FY (April 1 previous year → now).  
Writes `advance_tax_alert` event with `advance_tax_due`.

**Annual Schedule VDA report (July 31 UTC — ITR filing deadline):**  
Covers Indian FY: April 1 (previous year) → March 31 (current year).  
Generates per-trade Schedule VDA table with cost of acquisition, sale value, P&L.  
`taxable_base = total_gains + total_funding_received` (losses not deducted — Indian law).  
Saves JSON to `data/reports/schedule_vda_{fy_label}.json`.  
Writes `annual_vda_report` and `tds_reconciliation` events.

### Cron
`minute='4,19,34,49'` (1 minute after Module 8 at :03,:18,:33,:48)

---

## 14. Module 10 — Notification (`10_notification.py`)

**Purpose:** Sends all alerts to human via Telegram (primary) and Gmail (forced override backup).

### Channels
- **Telegram:** `python-telegram-bot[job-queue]` library, `Application.run_polling()` mode
- **Email backup:** Gmail SMTP SSL (port 465), only for `forced_override` events

### Events that trigger Telegram notifications
```python
NOTIFY_EVENTS = {
    'paper_fill', 'order_filled', 'order_timeout', 'execution_error',
    'stop_loss_exit', 'take_profit_exit', 'time_limit_exit', 'funding_cost_exit',
    'alert_yellow', 'alert_orange', 'alert_red', 'alert_cleared',
    'forced_override',
    'rr_alert_7d', 'win_rate_alert_7d',
    'recuperation_milestone',
    'advance_tax_alert', 'annual_vda_report',
    'withdrawal_calculation',
    'slot3_selection',
    'module_stale', 'orphaned_order', 'position_close_required',
}
```

### Events that ALSO send email
`EMAIL_EVENTS = {'forced_override'}` — Telegram AND email simultaneously.

### Deduplication
- Alert events (`alert_yellow/orange/red/cleared`): dedup within 5 minutes (`_DEDUP_WINDOW_S=300`)
- Strategy alerts (`win_rate_alert_7d`, `rr_alert_7d`): dedup within 23 hours (`_STRATEGY_DEDUP_WINDOW_S=82800`)
- `slot3_selection`: only notified when event timestamp falls on a Sunday (UTC)

### Event cursor
State persisted in `KRONOS_NOTIFIER_STATE_PATH` (default `data/notifier_state.json`).  
Stores `{"last_event_id": N}`. On each poll, queries `events WHERE id > last_event_id`. Cursor advances after processing, regardless of send outcome (prevents replay on restart).

### Telegram commands
| Command | Action |
|---|---|
| `/withdraw <amount>` | Records human withdrawal; writes `withdrawal_made` event with `amount` in payload |
| `/resume [reason]` | Option A (§19.3): clears forced override; writes `forced_override_cleared` event |
| `/halt [reason]` | Option C (§19.3): permanent halt; writes new `forced_override` event |
| `/status` | Returns current 8H portfolio snapshot (from DB state) |

### 8H cyclic summary
Sent at `00:02, 08:02, 16:02 UTC` via `job_queue.run_daily()`.  
Content: portfolio value, P&L vs start, drawdown, alert level, open positions with unrealised PnL, latest funding rates per symbol.

### Event polling
`job_queue.run_repeating(interval=900, first=30)` — every 15 minutes, starting 30 seconds after launch.

---

## 15. Module 11 — Health Monitor (`11_health_monitor.py`)

**Purpose:** 5-minute loop checking all other modules for staleness, anomalies, and performance issues.

### Key thresholds
```python
HEARTBEAT_STALE_S       = 1800    # 30 min (M1 heartbeat)
SLIPPAGE_STALE_S        = 1800    # 30 min (M2 slippage_estimate event)
PORTFOLIO_SNAP_STALE_S  = 18000   # 5H (M8 portfolio_snapshots table)
NOTIFIER_CURSOR_STALE_S = 1800    # 30 min (M10 state file cursor)
ORPHAN_ORDER_STALE_S    = 18000   # 5H (M6 open trade with no position row)
FO_WINDOW_S             = 86400   # 24H window for forced_override frequency
FO_MAX_COUNT            = 3       # max overrides in window before health_error
```

### 8 Health checks (every cycle)

**1. Heartbeat freshness** — `events.event_type='heartbeat'` from `data_collection` module. Stale > 30min → `module_stale` event + supervisorctl restart attempt.

**2. Slippage estimate freshness** — `events.event_type='slippage_estimate'`. Stale > 30min → `module_stale` + restart attempt.

**3. Portfolio snapshot freshness** — `portfolio_snapshots` table last row. Stale > 5H → `module_stale` (no auto-restart for M8).

**4. Notifier cursor freshness** — reads `notifier_state.json`, checks if oldest unprocessed event is older than 30min → `module_stale`.

**5. Orphaned orders (live only)** — `trades WHERE status='open'` with no matching `positions` row AND `entry_timestamp < now - 5H`. Writes `orphaned_order` event per orphan.

**6. Forced override frequency** — counts `forced_override` events in last 24H. If ≥ 3 → writes `health_error` critical event.

**7. System win rate (§19.1 item 7)** — checks 30-day win rate on all closed clean trades (5+ trades, span ≥ 23 days). If < 50% → writes `forced_override` with `reason='win_rate_below_50pct_30d'`. Deduped: no repeat if win-rate FO is still active.

**8. Unresponded override (§19.1 preamble)** — if most recent `forced_override` is > 24H old with no subsequent `forced_override_cleared` → writes `position_close_required` critical event (deduped).

### Asset win rate exclusion/reinstatement (§20.6, per cycle)

**Exclusion:** Per-asset, 30-day window, 5+ trades, span ≥ 23 days. Win rate < 50% → writes `asset_exclusion{excluded:True}`. Deduped: same symbol not re-excluded within 24H.

**Reinstatement:** 14-day window, 3+ trades, span ≥ 10 days. Win rate > 55% AND most recent `asset_exclusion` event has `excluded=True` → writes `asset_exclusion{excluded:False}`.

Both events are consumed by Module 4 `_load_exclusions()`.

### Auto-restart
Only `data_collection` and `slippage_model` are safe for auto-restart (no in-memory scheduler state).  
Uses `supervisorctl restart kronos-{process_name}` with 30s timeout.

### Loop
Runs every 300 seconds (`time.sleep(300)`). No APScheduler — Supervisord manages process-level restarts.

---

## 16. Module 12 — Dashboard (`dashboard.py`)

**Purpose:** Flask web dashboard providing real-time system visibility.

### Configuration
```python
PORT  = int(os.environ.get('KRONOS_DASHBOARD_PORT', 8050))
PAPER = os.environ.get('KRONOS_PAPER_MODE', 'true').lower() == 'true'
PHASE = os.environ.get('KRONOS_PHASE', 'pre_live')
START = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', 100000.0))
```

### Routes
- `GET /` — full dashboard HTML
- `GET /health` — `{'status': 'ok', 'ts': int(time.time())}`

### Auto-refresh
`<meta http-equiv="refresh" content="30">` — page reloads every 30 seconds.

### Tab memory
JavaScript writes `localStorage.setItem('kronos-tab', name)` on tab click. `window.onload` reads and restores active tab.

### Tab 1: Summary
- **Data quality notice banner** — yellow if `flagged_trades > 0 AND n_clean == 0`; grey if flagged but clean trades also exist
- **4 headline metric cards:**
  1. Portfolio Value — `total_value` from latest `portfolio_snapshots`; delta from `STARTING_CAPITAL_INR`
  2. Gross P&L — `SUM(pnl_gross)` from `trades WHERE status='closed' AND quality_flag IS NULL`
  3. Win Rate — wins/total clean closed trades; progress bar coloured green (≥50%) or red (<50%)
  4. Max Drawdown — `MAX(drawdown_pct)` from `portfolio_snapshots`; current DD; open position count
- **Open Positions table** — from `positions WHERE status IN ('open','closing')`: direction badge, symbol, entry, current price, unrealised P&L + %, held duration, max hold deadline (UTC)
- **Trade History table** — last 20 clean closed trades: closed time, symbol, direction, entry→exit prices, gross P&L, exit reason

### Tab 2: Operational
- **Model Accuracy** — 24H horizon evaluation: queries `ohlcv` to find close at signal time and close at signal_ts + 86400s. Accuracy = fraction where predicted direction matches actual direction. Shows "maturing" count for signals < 24H old. Custom model filtered by `quality_flag IS NULL`.
- **Signal Pipeline** — last 20 signals (all, including flagged); flagged rows dimmed (opacity 0.55) with "excluded" badge
- **Shadow Signals** — last 30 from `shadow_signals` table
- **Funding Rates** — latest per symbol from `funding_rates`; rates > 0.1%/8H highlighted

### Data quality filtering
All trade metrics use `WHERE quality_flag IS NULL`. Signal accuracy uses `quality_flag IS NULL`. Shadow signals have no quality_flag (separate table).

### Database access
Direct SQLite connection per request (no connection pool). WAL mode. Returns `[]` on any exception (dashboard never crashes).

---

## 17. ML Model — KronosForecaster (`kronos_model.py`)

**Architecture:** Channel-independent PatchTST (similar to iTransformer/PatchTST family).

### Hyperparameters
```python
SEQ_LEN    = 96     # input sequence (matches KRONOS_SEQ_LEN)
PRED_LEN   = 6      # output horizon (matches KRONOS_PRED_LEN)
N_CHANNELS = 5      # OHLCV
PATCH_LEN  = 16     # patch size
STRIDE     = 8      # patch stride
D_MODEL    = 128    # embedding dimension
N_HEADS    = 8      # attention heads
N_LAYERS   = 3      # transformer encoder layers
D_FF       = 256    # feed-forward hidden size
DROPOUT    = 0.1
```

### Architecture details
- `n_patches = (SEQ_LEN - PATCH_LEN) // STRIDE + 1 = (96 - 16) // 8 + 1 = 11`
- **Channel-independent:** each of 5 channels processed independently (batch and channel dims merged: `[B*C, L]`)
- `_PatchEmbedding`: `unfold(-1, patch_len, stride)` → `nn.Linear(patch_len, d_model)` → `[B*C, n_patches, d_model]`
- Learnable positional embedding: `nn.Parameter(torch.zeros(1, n_patches, d_model))`, initialised with truncated normal (std=0.02)
- `_EncoderLayer`: Pre-LayerNorm self-attention + Pre-LayerNorm FFN (GELU activation)
- Projection head: `nn.Linear(n_patches × d_model, pred_len)` per channel
- Final reshape: `[B, pred_len, n_channels]`

### Parameter count
~1.4M parameters. Fits in ~150MB RAM. No GPU required for inference.

### Interface
```python
# Input:  Tensor[batch, SEQ_LEN, 5]  — instance-normalised OHLCV (caller applies RevIN)
# Output: Tensor[batch, PRED_LEN, 5] — predicted normalised OHLCV (caller denormalises)
model = KronosForecaster()
y = model(x)   # x: [1, 96, 5] → y: [1, 6, 5]
```
Caller (Module 4) is responsible for instance normalisation before calling `forward()` and for denormalising output.

### Saving/loading
Training: `torch.jit.script(model).save(path)` — preferred  
Fallback save: `torch.save(model, path)` if scripting fails  
Loading (M4): `torch.jit.load(path)` first, `torch.load(path)` fallback  
Weight init: Xavier uniform for all Linear layers; zeros for biases

---

## 18. Training Script (`train_kronos.py`)

**Data source:** Binance (not Delta Exchange) — deepest available 4H history.  
**Symbols:** `BTC/USDT, ETH/USDT, SOL/USDT, BNB/USDT, XRP/USDT`  
**Target:** ~2.5 years / 5,500 candles per asset

### Key training constants
```python
WINDOW_SIZE          = SEQ_LEN + PRED_LEN = 102   # candles per sample
VAL_SPLIT            = 0.20                        # last 20% per asset
BATCH_SIZE           = 64
EPOCHS               = 100
LR                   = 1e-4
WEIGHT_DECAY         = 1e-4
EARLY_STOP_PATIENCE  = 10
```

### Dataset (OHLCVDataset)
Sliding window with per-channel instance normalisation (RevIN) — **identical to Module 4 inference**:
```python
x_raw = window[:SEQ_LEN]        # [96, 5]
y_raw = window[SEQ_LEN:]        # [6, 5]
mean  = x_raw.mean(axis=0)      # [1, 5]
std   = x_raw.std(axis=0, ddof=1) + 1e-8
x_norm = (x_raw - mean) / std   # normalised input
y_norm = (y_raw - mean) / std   # normalised target (same stats as input)
```

### Training setup
- Per-asset 80/20 time-ordered split (no shuffling across boundary)
- Val overlap: `val_arr = arr[split - SEQ_LEN:]` (SEQ_LEN candles overlap to avoid gap)
- `ConcatDataset` of all assets — no window ever crosses asset boundary
- `AdamW` optimizer + `CosineAnnealingLR(T_max=epochs, eta_min=lr×0.01)`
- Loss: `nn.MSELoss()`
- Gradient clipping: `max_norm=1.0`
- Early stopping: saves `MODEL_PATH.best.pt` at each new best val loss; stops after `patience=10` no-improve epochs

### Output
1. Loads best weights from `kronos_model.pt.best.pt`
2. Attempts `torch.jit.script()` → saves as `models/kronos_model.pt`
3. Fallback: `torch.save(model, path)`
4. Creates timestamped backup: `models/kronos_model_YYYYMMDD_HHMMSS.pt`
5. Deletes `*.best.pt` temp file

### Usage
```bash
python train_kronos.py
python train_kronos.py --epochs 200 --batch 128 --lr 5e-4
```
Training must be done locally (GPU). VPS has no GPU.

---

## 19. Shadow Inference (`shadow_inference.py`)

**Purpose:** Runs Kronos foundation models (from HuggingFace) in shadow mode alongside the custom model for week-6 benchmarking.

### Key decision: CPU-only on Windows
GPU inference with 50 probabilistic samples exceeds Windows 2-second TDR (Timeout Detection and Recovery) limit, corrupting the CUDA context.  
**Default: CPU.** Overrideable via `KRONOS_SHADOW_DEVICE=cuda` (Linux only with TDR disabled).  
CPU inference: ~30s per full cycle — well within 4H window.

### Foundation models
| Model | HuggingFace ID | Tokenizer | Context | Env override |
|---|---|---|---|---|
| kronos-mini | `NeoQuasar/Kronos-mini` | `NeoQuasar/Kronos-Tokenizer-2k` | 1024 candles | `KRONOS_SHADOW_MINI_CONTEXT` |
| kronos-base | `NeoQuasar/Kronos-base` | `NeoQuasar/Kronos-Tokenizer-base` | 512 candles | `KRONOS_SHADOW_BASE_CONTEXT` |

### Vendor path
```python
_VENDOR_PATH = os.path.join(os.path.dirname(__file__), 'vendor', 'kronos')
sys.path.insert(0, _VENDOR_PATH)  # enables 'from model import Kronos, ...'
```
Kronos repo must be cloned to `vendor/kronos/` (excluded from git).

### Lazy model loading
Models load on first `run_all_symbols()` call, not at import time. Each model has independent `_mini_ready` / `_base_ready` flags — failure of one does not prevent the other from running.

### Prediction flow
1. Fetch `context_len` candles from `ohlcv` table for the symbol (reverse-sorted, then reversed back to chronological)
2. Build `pandas.DataFrame` with columns: `open, high, low, close, volume, amount`
3. Build `x_timestamp` (historical) and `y_timestamp` (next 6 × 4H steps) as UTC pandas datetime Series
4. Call `predictor.predict(df, x_timestamp, y_timestamp, pred_len=6, T=1.0, top_p=0.9, sample_count=50, verbose=False)`
5. Extract `pred_df['close'].values` → 6 predicted close prices

### Confidence formula (byte-for-byte identical to Module 4)
```python
direction        = 'long' if pred_closes[-1] > current_close else 'short'
predicted_return = (pred_closes[-1] - current_close) / current_close
atr_pct          = _compute_atr_pct(rows)   # same function as M4
base_conf        = min(1.0, abs(predicted_return) / (2.0 × atr_pct))
n_agree          = steps agreeing with final direction
consistency      = n_agree / PRED_LEN
confidence       = round(base_conf × consistency, 4)
```
ATR fallback: if `atr_pct <= 0`: use `abs(predicted_return)` or `1e-6`.

### Output
Writes to `shadow_signals` table only. Never writes to `signals`, `trades`, or `positions`.

### Active symbols
BTCUSD + ETHUSD + Slot 3 (from most recent `slot3_selection` event in `events` table where `module='signal_generator'`).

---

## 20. Benchmark Analysis (`benchmark_analysis.py`)

**Purpose:** Run manually at week 6 to compare three models on directional accuracy.

### Evaluation method
For each signal:
1. Find close price at signal time: latest 4H candle close `<= signal_timestamp`
2. Find close price at 24H horizon: earliest 4H candle close `>= signal_timestamp + 86400`
3. Actual direction = `'long'` if horizon_close > signal_close else `'short'`
4. Hit = signal direction matches actual direction

Signal not evaluated if either close price is unavailable (not yet in DB).

### Custom model signals
From `signals WHERE status IN ('approved', 'executed')` — note: this does NOT filter by `quality_flag`. For clean analysis, run after marking corrupted data.

### Shadow signals
From `shadow_signals` grouped by `model_name`.

### Confidence bands
`[0.0–0.2, 0.2–0.4, 0.4–0.6, 0.6–0.8, 0.8–1.0]`

### Output
- Ranked table to stdout: overall accuracy + per-confidence-band breakdown + per-symbol breakdown
- CSV saved to `data/reports/benchmark_YYYYMMDD.csv`

### Usage
```bash
python benchmark_analysis.py
```

---

## 21. Startup Script (`start_dev_windows.ps1`)

Launches all 12 modules as separate PowerShell windows.

```powershell
.\start_dev_windows.ps1         # starts all 12 modules
.\start_dev_windows.ps1 -Module 4  # starts only module 4
```

Each module gets its own `powershell.exe` window with a title like `Kronos M4 - Signal Generator`.  
400ms delay between module launches.  
Module numbers: 1–12 (including Dashboard as 12).

---

## 22. Dependencies (`requirements.txt`)

| Package | Version | Used by |
|---|---|---|
| `ccxt` | ≥4.5.54 | M1, M6, M7, M8, training |
| `APScheduler` | ≥3.11.2 | M1, M2, M4, M5, M6, M7, M8, M9 |
| `websockets` | ≥12.0 | M1 (WebSocket feed) |
| `requests` | ≥2.31.0 | M1 (REST fallback) |
| `numpy` | ≥1.26.0 | M2, training |
| `torch` | ≥2.1.0 | M4, training, shadow inference |
| `python-telegram-bot[job-queue]` | ≥21.0 | M10 |
| `pandas` | ≥2.0.0 | Shadow inference |
| `einops` | ≥0.8.1 | Shadow inference (Kronos repo dependency) |
| `huggingface_hub` | ≥0.21.0 | Shadow inference (model download) |
| `safetensors` | ≥0.3.0 | Shadow inference (model weights format) |
| `flask` | ≥3.0.0 | M12 (dashboard) |

**PyTorch install (separate step before pip install -r requirements.txt):**
- Windows/CUDA 12.4: `pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124`
- Linux VPS (CPU only): `pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu`

---

## 23. Macro Calendar (`macro_calendar.json`)

**Format:** JSON array. Each event has `name` (str), `timestamp` (int UTC epoch), `type` (str), `notes` (str, optional).

**Maintenance:** Manually updated weekly by human operator. Module 3 uses a symmetric ±4H blackout around each event.

**Event types in current calendar:** `fomc`, `cpi`, `nfp`, `rbi_mpc`, `jackson_hole`, `india_budget`

**Coverage:** June 2026 through December 2027 (pre-populated).

**Metadata entry:** First entry has `"name": "_METADATA"`, `"timestamp": 0` — ignored by Module 3 (timestamp=0 is never within 4H of real time).

**DST handling:** Notes include both EDT (UTC-4) and EST (UTC-5) conversions; timestamps in the file are already in UTC.

---

## 24. Data Quality Management

### Problem discovered (2026-05-27)
Multiple Python processes for M4, M5, M7 were running simultaneously (old instances never killed when new code was deployed). The old M7 instance had pre-fix code that ran `funding_cost` exits when `KRONOS_FUNDING_EXIT_ENABLED=false` should have blocked them.

### quality_flag column
Added to `signals` and `trades` tables via migration:
```sql
ALTER TABLE signals ADD COLUMN quality_flag TEXT DEFAULT NULL;
ALTER TABLE trades  ADD COLUMN quality_flag TEXT DEFAULT NULL;
```
**NULL = clean data** (included in all analysis). **Non-NULL = excluded** (test artifacts or bug-corrupted).

### quality_flag values
| Value | Meaning |
|---|---|
| `'test_artifact'` | Dummy data from setup/testing phase |
| `'corrupted_bug:duplicate_m7_exit'` | Trade closed incorrectly by duplicate M7 process via `funding_cost` exit |
| `'incomplete_data:no_predicted_return'` | Signal had `predicted_return_pct=0.0` (M4 not restarted, blind cost filter) |
| `'duplicate:two_m4_instances'` | Duplicate signal from two M4 processes running simultaneously |
| `'corrupted_bug:trade_was_bad_exit'` | Signal whose executed trade was flagged as corrupted |

### mark_corrupted_data.py
One-time script run after fixing the duplicate process issue (`FIX_TS = 1779872400` = 2026-05-27 10:00 UTC).

Marking logic:
1. `trades` with `exit_reason='test_artifact'` → `test_artifact`
2. `trades` with `exit_reason='funding_cost' AND entry_timestamp < FIX_TS` → `corrupted_bug:duplicate_m7_exit`
3. `signals` with `rejection_reason LIKE '%test_artifact%'` → `test_artifact`
4. `signals` with `rejection_reason LIKE 'duplicate_pending_signal%'` → `duplicate:two_m4_instances`
5. `signals` with `predicted_return_pct=0.0 AND status NOT IN (rejected/expired/cancelled) AND signal_timestamp < FIX_TS` → `incomplete_data:no_predicted_return`
6. `signals` whose executed trade is `corrupted_bug:duplicate_m7_exit` → `corrupted_bug:trade_was_bad_exit`

Safe to re-run: uses `WHERE quality_flag IS NULL` to avoid overwriting manual flags. Data is NOT deleted.

### fix_stuck_positions.py
One-time script that fixed positions stuck in `'open'` or `'closing'` state where the associated trade was already `'closed'`. Fixed 5 positions (IDs 142-146) left by interference between two M7 instances.

---

## 25. Cron Schedule Summary

All times UTC. Sequence within each 4H cycle designed so each module has data from the previous before running.

| Time (UTC) | Module | Job |
|---|---|---|
| :02 past each 4H (00,04,08,12,16,20) | M1 | OHLCV fetch + funding rate (8H: 00,08,16 only) |
| :02 past each 15 min | M1 | Orderbook snapshot + fill window flush |
| :03 past each 4H | M8 | Portfolio snapshot (4H boundary cycles only) |
| :03, :18, :33, :48 | M8 | 15-min portfolio cycle (drawdown checks, monthly calc) |
| :04, :19, :34, :49 | M9 | 15-min tax processing cycle |
| :05 past each 4H | M4 | Signal generation |
| Sun 00:03 UTC | M4 | Weekly Slot 3 selection |
| :07 past each 4H | M5 | Risk check + signal approval |
| :09 past each 4H | M6 | Execute approved signals |
| :00, :15, :30, :45 | M7 | Position exit condition checks |
| Every 5 min | M11 | System health checks |
| Every 15 min | M2 | Slippage model cache update |
| Every 15 min (first=30s) | M10 | Event poller (Telegram dispatch) |
| 00:02, 08:02, 16:02 | M10 | 8H cyclic portfolio summary (Telegram) |

---

## 26. Inter-Module Data Flow

```
M1 → ohlcv                 → M4 (signal generation), M2 (calibration)
M1 → orderbook_snapshots   → M2 (regime), M5 (circuit breaker), M6 (mark price), M7 (mark price)
M1 → funding_rates         → M5 (entry funding block), M7 (funding exit check), M9 (funding per trade)
M1 → events (fill_window)  → M2 (fill deviation calculation)

M2 → events (slippage_estimate) → M5 (slippage check), M6 (reference), M11 (staleness check)

M4 → signals (pending)     → M5 (risk gate input)
M4 → events (slot3_selection) → M8 (slot3 close on orange), Shadow (active symbol list)
M4 → shadow_signals        → (via shadow_inference, background task)

M5 → signals (approved)    → M6 (execution trigger)
M5 → events (risk_check)   → M6 (size_cap_pct, combined_margin_cap_pct)
M5 → events (win_rate_alert_7d) → M10 (Telegram), M5 (own threshold adjustment)

M6 → trades (open)         → M7 (exit monitoring), M8 (portfolio value), M9 (tax processing)
M6 → positions             → M7 (exit monitoring), M8 (portfolio value)

M7 → trades (closed)       → M8 (portfolio value), M9 (tax trigger), M10 (Telegram exit notification)
M7 → events (stop_loss_exit) → M5 (4H blackout check)
M7 → events (take_profit_exit, time_limit_exit, funding_cost_exit) → M10 (Telegram)
M7 → positions (unrealised_pnl) → M8 (portfolio value computation)

M8 → portfolio_snapshots   → M6 (portfolio_value for sizing), M11 (staleness check), M12 (dashboard)
M8 → events (alert_*) → M5 (entry blocks), M10 (Telegram), M12 (dashboard)
M8 → events (forced_override) → M5 (block all), M10 (Telegram + email)
M8 → tax_reserve           → M9 (reads reserve balance for reports)
M8 → events (withdrawal_calculation) → M10 (monthly summary Telegram)
M8 → events (rr_alert_7d)  → M10 (Telegram)

M9 → trades (pnl_net, fees, tds, funding) → M8 (monthly calc uses COALESCE(pnl_net, pnl_gross))
M9 → tds_log               → M9 annual report, M12 (dashboard)
M9 → events (monthly_tax_summary) → M10 (monthly summary Telegram composition)
M9 → events (advance_tax_alert, annual_vda_report) → M10 (Telegram)

M10 → events (withdrawal_made) → M8 (cumulative withdrawal), M9 (recuperation ledger)
M10 → events (forced_override_cleared) → M5 (unblock signals), M8 (consecutive loss dedup)
M10 → events (forced_override) → M5 (block all — from /halt command)

M11 → events (asset_exclusion) → M4 (per-symbol exclusion check)
M11 → events (module_stale, orphaned_order, position_close_required) → M10 (Telegram)
M11 → events (forced_override) → M5 (block all — from system win rate check)
```

---

## 27. Security Architecture

### API key permissions
Trade-only permission on Delta Exchange. **Withdrawal permission is DISABLED.** The system can open/close positions but cannot transfer funds.

### Secret storage
All secrets stored as environment variables in `.env` file only. Never hardcoded. `.env` is in `.gitignore` — must never be committed.

Secrets:
- `KRONOS_API_KEY` — Delta Exchange API key
- `KRONOS_API_SECRET` — Delta Exchange API secret
- `KRONOS_TELEGRAM_BOT_TOKEN` — Telegram bot token
- `KRONOS_EMAIL_APP_PASSWORD` — Gmail app password

### Tax reserve
`tax_reserve` table balance is earmarked for ITR payment only. Module 8 only adds credits (`transaction_type='reserve'`). The system has no code path to use the reserve for trading. This is a non-negotiable design constraint.

### SSH (VPS)
Key-based authentication only. Password login disabled.

### Firewall
Only ports 22 (SSH), 443 (HTTPS), and required API ports open.

### Human control
- `/resume` command (Telegram) required to unblock after any `forced_override`
- `/withdraw <amount>` required to record any withdrawal (never automated)
- `/halt` permanently stops signal processing
- Red Alert (15% drawdown): writes `forced_override` — human must explicitly `/resume` to restart

---

*End of documentation. Every fact in this document was verified against source code. Last code review: 2026-05-27.*
