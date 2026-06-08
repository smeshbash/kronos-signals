"""
Kronos Trading System — Module 1: Data Collection
Continuously collects all market data from Delta Exchange India and stores in SQLite.

Schedule (all times UTC, aligned to market boundaries):
  • WebSocket v2/ticker   — persistent; in-memory ticker cache updated on every message
  • WebSocket all_trades  — persistent; fills buffered in memory, flushed every 15 min
  • orderbook_snapshots   — snapshot from ticker cache to DB every 15 min
  • fill_window events    — aggregated fill stats flushed to events table every 15 min
  • ohlcv                 — REST fetch at 00:02, 04:02, 08:02, 12:02, 16:02, 20:02 UTC
                            (2 min after each 4H candle close per Section 7.1)
  • funding_rates         — REST fetch at 00:02, 08:02, 16:02 UTC
                            (aligned to Delta 8H funding settlement windows)

Per Section 14.2: APScheduler, CCXT + Delta native API, WebSocket via native Delta India.
Per Section 18.3: Module active from pre-live day one.
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, List, Optional

import ccxt
import requests
import websockets
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import get_connection, init_db, log_event

# ── Constants ──────────────────────────────────────────────────────────────────

DELTA_WS_URL    = 'wss://socket.india.delta.exchange'
DELTA_REST_BASE = 'https://api.india.delta.exchange'
MODULE          = 'data_collection'

# 4 active assets — slot 5 reserved for LINKUSD (add when ready).
ASSETS: Dict[str, Dict[str, str]] = {
    'BTC': {'delta': 'BTCUSD', 'ccxt': 'BTC/USD:USD'},
    'ETH': {'delta': 'ETHUSD', 'ccxt': 'ETH/USD:USD'},
    'BNB': {'delta': 'BNBUSD', 'ccxt': 'BNB/USD:USD'},
    'XRP': {'delta': 'XRPUSD', 'ccxt': 'XRP/USD:USD'},
}

DELTA_SYMBOLS = [v['delta'] for v in ASSETS.values()]
CCXT_SYMBOLS  = {k: v['ccxt'] for k, v in ASSETS.items()}

# 500 candles × 4H = ~83 days of history backfilled on startup.
OHLCV_BACKFILL_LIMIT = 500
OHLCV_TIMEFRAME      = '4h'

# 2100 candles × 1H = ~87 days backfilled on startup.
# Kronos-mini needs 2048; +52 buffer covers any gaps.
OHLCV_1H_BACKFILL_LIMIT = 2100
OHLCV_1H_TIMEFRAME      = '1h'

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(MODULE)

# ── Shared WebSocket state ─────────────────────────────────────────────────────

# Latest v2/ticker message per Delta symbol. Updated by WS receiver.
_ticker_cache: Dict[str, dict] = {}
_ticker_lock = asyncio.Lock()

# In-memory buffer of recent public fills per symbol. Flushed every 15 min.
# Each entry: {'price': str, 'size': int, 'buyer_role': str, 'seller_role': str,
#              'timestamp': int}  (timestamp in microseconds from Delta)
_fills_buffer: Dict[str, List[dict]] = {sym: [] for sym in DELTA_SYMBOLS}
_fills_lock = asyncio.Lock()

# Set when all 5 symbols have at least one ticker message in cache.
_ws_ready = asyncio.Event()


# ── REST helper ────────────────────────────────────────────────────────────────

def _rest_get(path: str, params: dict = None) -> dict:
    """Synchronous REST GET against Delta India API. Raises on HTTP error."""
    resp = requests.get(
        DELTA_REST_BASE + path,
        params=params,
        timeout=15,
        headers={'Accept': 'application/json'},
    )
    resp.raise_for_status()
    return resp.json()


# ── DataCollector ──────────────────────────────────────────────────────────────

class DataCollector:
    """
    Module 1 — collects all market data from Delta Exchange India.
    Two persistent WebSocket subscriptions (ticker + trades) and three
    REST-based scheduled jobs (OHLCV, funding rates, orderbook snapshots).
    """

    def __init__(self) -> None:
        self.exchange = ccxt.delta({'options': {'defaultType': 'swap'}})
        self.exchange.urls['api']['public'] = DELTA_REST_BASE
        self._running   = False
        self._ws_task: Optional[asyncio.Task] = None
        self._scheduler: Optional[AsyncIOScheduler] = None

    # ── Job: orderbook snapshot + fill window flush (every 15 min) ───────────

    async def job_snapshot_orderbook(self) -> None:
        """
        Two actions in one job (aligned to the same 15-min cycle):
          1. Snapshot L1 ticker data from WS cache → orderbook_snapshots table.
          2. Flush accumulated public fills → events table as 'fill_window' rows.

        Heartbeat logged after both succeed.
        """
        await self._write_orderbook_snapshot()
        await self._flush_fills_buffer()

    async def _write_orderbook_snapshot(self) -> None:
        async with _ticker_lock:
            snapshot = dict(_ticker_cache)

        if not snapshot:
            log.warning('Orderbook snapshot skipped — WebSocket ticker cache empty')
            log_event(MODULE, 'warning', 'orderbook_snapshot',
                      'Snapshot skipped: WebSocket cache empty')
            return

        ts   = int(time.time())
        rows = []
        for sym, ticker in snapshot.items():
            quotes       = ticker.get('quotes') or {}
            best_bid     = _to_float(quotes.get('best_bid'))
            best_ask     = _to_float(quotes.get('best_ask'))
            bid_size     = _to_float(quotes.get('bid_size'))
            ask_size     = _to_float(quotes.get('ask_size'))
            spread       = round(best_ask - best_bid, 8) if (best_bid is not None and best_ask is not None) else None
            mark_price   = _to_float(ticker.get('mark_price'))
            open_interest = _to_float(ticker.get('oi'))
            funding_rate = _to_float(ticker.get('funding_rate'))
            rows.append((sym, ts, best_bid, best_ask, bid_size, ask_size,
                         spread, mark_price, open_interest, funding_rate))

        with get_connection() as conn:
            conn.executemany(
                """INSERT INTO orderbook_snapshots
                   (symbol, timestamp, best_bid, best_ask, bid_size, ask_size,
                    spread, mark_price, open_interest, funding_rate)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )

        log.info('Orderbook snapshot: %d symbols saved', len(rows))
        log_event(MODULE, 'info', 'heartbeat',
                  f'Orderbook snapshot complete: {len(rows)} symbols',
                  {'symbols': list(snapshot.keys()), 'timestamp': ts})

    async def _flush_fills_buffer(self) -> None:
        """
        Drain the in-memory fills buffer and write one aggregated 'fill_window'
        event row per symbol to the events table. This is the form in which
        Module 2 (Slippage Model) consumes historical fill data to build
        slippage distributions (Section 12.3: 'Historical fill data vs mid-price
        → Slippage distribution per asset per market condition').

        Storing aggregations rather than individual fills keeps storage bounded:
        5 assets × 96 windows/day × ~200 bytes ≈ 96 KB/day vs potentially
        hundreds of MB/day for raw ticks on BTC/USD alone.
        """
        async with _fills_lock:
            buffer_copy: Dict[str, List[dict]] = {}
            for sym in DELTA_SYMBOLS:
                if _fills_buffer[sym]:
                    buffer_copy[sym] = _fills_buffer[sym]
                    _fills_buffer[sym] = []

        if not buffer_copy:
            return

        window_end   = int(time.time())
        window_start = window_end - 900   # 15-minute window

        event_rows = []
        for sym, fills in buffer_copy.items():
            prices    = [float(f['price']) for f in fills]
            raw_sizes = [f.get('size') for f in fills]
            int_sizes = [int(s) for s in raw_sizes if s is not None]
            total_vol = sum(int_sizes)
            vwap      = (sum(float(f['price']) * int(f['size']) for f in fills
                             if f.get('size') is not None)
                         / total_vol) if total_vol else 0.0
            taker_buys = sum(1 for f in fills if f.get('buyer_role') == 'taker')

            event_rows.append((
                MODULE,
                'fill_window',
                'debug',
                f'{sym}: {len(fills)} fills, VWAP={vwap:.4f}',
                json.dumps({
                    'symbol':           sym,
                    'window_start':     window_start,
                    'window_end':       window_end,
                    'fill_count':       len(fills),
                    'vwap':             round(vwap, 4),
                    'min_price':        min(prices) if prices else None,
                    'max_price':        max(prices) if prices else None,
                    'avg_size':         round(total_vol / len(int_sizes), 2) if int_sizes else 0.0,
                    'taker_buy_ratio':  round(taker_buys / len(fills), 4),
                }),
                window_end,
            ))

        with get_connection() as conn:
            conn.executemany(
                """INSERT INTO events
                   (module, event_type, severity, message, data, timestamp)
                   VALUES (?,?,?,?,?,?)""",
                event_rows,
            )

        log.debug('Flushed %d fill windows to events table', len(event_rows))

    # ── Job: OHLCV collection (cron: candle-close-aligned) ────────────────────

    async def job_collect_ohlcv_1h(self) -> None:
        """
        Fetch latest 1H OHLCV candles for all 5 assets via CCXT REST.
        Scheduled at :02 past every hour — 2 minutes after each 1H candle close.
        Kronos-mini (M13) and Kronos-base (M14) read from this data at full context.
        """
        loop = asyncio.get_running_loop()
        total_stored = 0

        for asset, ccxt_sym in CCXT_SYMBOLS.items():
            delta_sym = ASSETS[asset]['delta']
            try:
                candles = await loop.run_in_executor(
                    None,
                    lambda s=ccxt_sym: self.exchange.fetch_ohlcv(
                        s, OHLCV_1H_TIMEFRAME, limit=OHLCV_1H_BACKFILL_LIMIT
                    ),
                )

                rows = [
                    (delta_sym, OHLCV_1H_TIMEFRAME, c[0] // 1000,
                     c[1], c[2], c[3], c[4], c[5])
                    for c in candles
                ]

                with get_connection() as conn:
                    conn.executemany(
                        """INSERT OR IGNORE INTO ohlcv
                           (symbol, timeframe, timestamp, open, high, low, close, volume)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        rows,
                    )

                total_stored += len(rows)
                log.info('OHLCV 1H %s: %d candles', asset, len(rows))

            except Exception as exc:
                log.error('OHLCV 1H fetch failed for %s: %s', asset, exc)
                log_event(MODULE, 'error', 'ohlcv_error',
                          f'OHLCV 1H fetch failed for {asset}',
                          {'asset': asset, 'timeframe': '1h', 'error': str(exc)})

        log_event(MODULE, 'info', 'heartbeat',
                  f'OHLCV 1H cycle complete: {total_stored} rows across 5 assets',
                  {'total_rows': total_stored, 'timeframe': OHLCV_1H_TIMEFRAME})

    async def job_collect_ohlcv(self) -> None:
        """
        Fetch latest 4H OHLCV candles for all 5 assets via CCXT REST.
        Scheduled at 00:02, 04:02, 08:02, 12:02, 16:02, 20:02 UTC — 2 minutes
        after each 4H candle close so the completed candle is finalised on the
        exchange before we fetch (Section 7.1: 'Kronos re-evaluated on each new
        4H candle close').

        INSERT OR REPLACE makes repeated runs idempotent.
        Heartbeat logged on success.
        """
        loop = asyncio.get_running_loop()
        total_stored = 0

        for asset, ccxt_sym in CCXT_SYMBOLS.items():
            delta_sym = ASSETS[asset]['delta']
            try:
                candles = await loop.run_in_executor(
                    None,
                    lambda s=ccxt_sym: self.exchange.fetch_ohlcv(
                        s, OHLCV_TIMEFRAME, limit=OHLCV_BACKFILL_LIMIT
                    ),
                )

                # CCXT returns [ts_ms, open, high, low, close, volume].
                rows = [
                    (delta_sym, OHLCV_TIMEFRAME, c[0] // 1000,
                     c[1], c[2], c[3], c[4], c[5])
                    for c in candles
                ]

                with get_connection() as conn:
                    conn.executemany(
                        """INSERT OR IGNORE INTO ohlcv
                           (symbol, timeframe, timestamp, open, high, low, close, volume)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        rows,
                    )

                total_stored += len(rows)
                log.info('OHLCV %s: %d candles (latest close=%.4f)',
                         asset, len(rows), candles[-1][4] if candles else 0)

            except Exception as exc:
                log.error('OHLCV fetch failed for %s: %s', asset, exc)
                log_event(MODULE, 'error', 'ohlcv_error',
                          f'OHLCV fetch failed for {asset}',
                          {'asset': asset, 'error': str(exc)})

        log_event(MODULE, 'info', 'heartbeat',
                  f'OHLCV cycle complete: {total_stored} rows across 5 assets',
                  {'total_rows': total_stored, 'timeframe': OHLCV_TIMEFRAME})

    # ── Job: Funding rate collection (cron: settlement-aligned) ───────────────

    async def job_collect_funding_rates(self) -> None:
        """
        Fetch current 8H funding rate for all 5 assets via REST ticker endpoint.
        Scheduled at 00:02, 08:02, 16:02 UTC — 2 minutes after each Delta 8H
        funding settlement window so the settled rate is captured.

        INSERT OR REPLACE prevents duplicates if the job fires twice in a window.
        Heartbeat logged on success.
        """
        loop = asyncio.get_running_loop()
        ts   = int(time.time())

        for asset, info in ASSETS.items():
            delta_sym = info['delta']
            try:
                data = await loop.run_in_executor(
                    None,
                    lambda sym=delta_sym: _rest_get(f'/v2/tickers/{sym}')['result'],
                )
                # Delta India API returns funding_rate as a percentage value
                # (e.g. 0.0100 means 0.0100%/8H).  Convert to decimal fraction
                # (0.0001 = 0.01%) so M5's thresholds (EXTREME_FUNDING_THRESHOLD=0.003
                # = 0.3%/8H, ROUND_TRIP_FEES_PCT=0.0017 = 0.17%) are on the same scale.
                rate = float(data['funding_rate']) / 100.0

                with get_connection() as conn:
                    conn.execute(
                        """INSERT OR REPLACE INTO funding_rates
                           (symbol, timestamp, rate)
                           VALUES (?, ?, ?)""",
                        (delta_sym, ts, rate),
                    )

                log.info('Funding rate %s: %.8f', asset, rate)

            except Exception as exc:
                log.error('Funding rate fetch failed for %s: %s', asset, exc)
                log_event(MODULE, 'error', 'funding_rate_error',
                          f'Funding rate fetch failed for {asset}',
                          {'asset': asset, 'error': str(exc)})

        log_event(MODULE, 'info', 'heartbeat',
                  'Funding rate cycle complete',
                  {'assets': list(ASSETS.keys()), 'timestamp': ts})

    # ── WebSocket: persistent connection ──────────────────────────────────────

    async def _ws_run(self) -> None:
        """
        Maintain one persistent WebSocket connection subscribing to both:
          • v2/ticker   — L1 order book + funding rate + OI per symbol
          • all_trades  — public trade execution fills per symbol

        Ticker messages update _ticker_cache (read by orderbook snapshot job).
        Fill messages accumulate in _fills_buffer (flushed by snapshot job).
        Reconnects with exponential backoff (1 → 60 s) on any failure.
        """
        backoff = 1

        while self._running:
            try:
                async with websockets.connect(
                    DELTA_WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    open_timeout=15,
                ) as ws:

                    backoff = 1

                    sub_msg = {
                        'type': 'subscribe',
                        'payload': {
                            'channels': [
                                {'name': 'v2/ticker',  'symbols': DELTA_SYMBOLS},
                                {'name': 'all_trades', 'symbols': DELTA_SYMBOLS},
                            ]
                        },
                    }
                    await ws.send(json.dumps(sub_msg))

                    log.info('WebSocket connected — subscribed to v2/ticker + '
                             'all_trades for %s', DELTA_SYMBOLS)
                    log_event(MODULE, 'info', 'ws_connected',
                              f'WebSocket subscribed: v2/ticker + all_trades '
                              f'for {len(DELTA_SYMBOLS)} symbols',
                              {'symbols': DELTA_SYMBOLS,
                               'channels': ['v2/ticker', 'all_trades']})

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        msg_type = msg.get('type')

                        # ── Ticker update ──────────────────────────────────
                        if msg_type == 'v2/ticker':
                            sym = msg.get('symbol')
                            if sym in DELTA_SYMBOLS:
                                async with _ticker_lock:
                                    _ticker_cache[sym] = msg
                                    cache_size = len(_ticker_cache)
                                if not _ws_ready.is_set() and cache_size == len(DELTA_SYMBOLS):
                                    _ws_ready.set()

                        # ── Fill snapshot (initial burst on subscribe) ─────
                        elif msg_type == 'all_trades_snapshot':
                            sym = msg.get('symbol')
                            if sym in DELTA_SYMBOLS:
                                trades = msg.get('trades') or []
                                async with _fills_lock:
                                    _fills_buffer[sym].extend(trades)

                        # ── Individual real-time fill ──────────────────────
                        elif msg_type == 'all_trades':
                            sym = msg.get('symbol')
                            if sym in DELTA_SYMBOLS:
                                fill = {
                                    'price':       msg.get('price'),
                                    'size':        msg.get('size'),
                                    'buyer_role':  msg.get('buyer_role'),
                                    'seller_role': msg.get('seller_role'),
                                    'timestamp':   msg.get('timestamp'),
                                }
                                async with _fills_lock:
                                    _fills_buffer[sym].append(fill)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning('WebSocket error: %s — reconnecting in %ds', exc, backoff)
                log_event(MODULE, 'warning', 'ws_disconnected',
                          f'WebSocket disconnected: {exc}. Reconnect in {backoff}s',
                          {'backoff_seconds': backoff, 'error': str(exc)})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        log.info('WebSocket loop exited')

    # ── Main run loop ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Start Module 1. Sequence:
          1. Init database (idempotent).
          2. Immediate OHLCV backfill + funding rate fetch.
          3. Start WebSocket task.
          4. Wait up to 20s for all 5 symbols in ticker cache.
          5. First orderbook snapshot + fill window flush.
          6. Start APScheduler with cron-aligned jobs.
          7. Run indefinitely.
        """
        self._running = True

        # 1. DB init
        log.info('Initialising database')
        init_db()
        log_event(MODULE, 'info', 'info', 'Module 1 startup — database initialised')

        # 2. Initial fetches
        log.info('Running initial 4H OHLCV backfill...')
        await self.job_collect_ohlcv()

        log.info('Running initial 1H OHLCV backfill (2100 candles for foundation models)...')
        await self.job_collect_ohlcv_1h()

        log.info('Running initial funding rate fetch...')
        await self.job_collect_funding_rates()

        # 3. WebSocket
        self._ws_task = asyncio.create_task(self._ws_run(), name='ws_maintain')

        # 4. Wait for full ticker cache
        log.info('Waiting for WebSocket ticker cache (all %d symbols)...', len(DELTA_SYMBOLS))
        try:
            await asyncio.wait_for(_ws_ready.wait(), timeout=20.0)
            log.info('WebSocket ready — all %d symbols in cache', len(DELTA_SYMBOLS))
        except asyncio.TimeoutError:
            log.warning('Ticker cache not fully populated within 20s — '
                        'first snapshot may be partial')
            log_event(MODULE, 'warning', 'ws_timeout',
                      'WebSocket cache not fully populated within 20s of startup')

        # 5. First snapshot
        log.info('Taking first orderbook snapshot and fill flush...')
        await self.job_snapshot_orderbook()

        # 6. Scheduler — cron triggers aligned to market boundaries
        self._scheduler = AsyncIOScheduler(timezone='UTC')

        # Every 15 min (not cron because 15-min intervals don't align to hours neatly).
        self._scheduler.add_job(
            self.job_snapshot_orderbook,
            'interval', minutes=15,
            id='orderbook_snapshot',
            max_instances=1,
            misfire_grace_time=60,
        )

        # OHLCV 4H: 2 min after each 4H candle close (Section 7.1).
        self._scheduler.add_job(
            self.job_collect_ohlcv,
            'cron', hour='0,4,8,12,16,20', minute=2,
            id='ohlcv_collect',
            max_instances=1,
            misfire_grace_time=300,
        )

        # OHLCV 1H: 2 min after every 1H candle close — feeds Kronos-mini (M13) and
        # Kronos-base (M14) which run at :05 past each hour.
        self._scheduler.add_job(
            self.job_collect_ohlcv_1h,
            'cron', minute=2,
            id='ohlcv_collect_1h',
            max_instances=1,
            misfire_grace_time=120,
        )

        # Funding rates: 2 min after each 8H settlement window.
        self._scheduler.add_job(
            self.job_collect_funding_rates,
            'cron', hour='0,8,16', minute=2,
            id='funding_rates',
            max_instances=1,
            misfire_grace_time=300,
        )

        self._scheduler.start()
        log.info('Scheduler started — '
                 'orderbook every 15m | '
                 'OHLCV at 00:02/04:02/08:02/12:02/16:02/20:02 UTC | '
                 'funding rates at 00:02/08:02/16:02 UTC')
        log_event(MODULE, 'info', 'info',
                  'Module 1 fully initialised and running',
                  {'scheduler_jobs': ['orderbook_snapshot', 'ohlcv_collect',
                                      'funding_rates']})

        # 7. Run until cancelled
        try:
            await self._ws_task
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        self._running = False
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        log.info('Module 1 shut down cleanly')
        log_event(MODULE, 'info', 'info', 'Module 1 shut down')


# ── Utilities ──────────────────────────────────────────────────────────────────

def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    collector = DataCollector()
    try:
        await collector.run()
    except KeyboardInterrupt:
        log.info('Keyboard interrupt received')
        await collector.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
