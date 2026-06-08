"""
Kronos Trading System — Module 7: Position Monitor
Sections 7.1, 8.1, 9.1, 9.4, 11.3, 11.5, 19.2 of the spec (v2.9).

Production-grade dual-component design:

  1. WebSocket real-time monitor (always running)
     Subscribes to v2/ticker for all 5 symbols.
     On every mark-price tick, checks ALL open positions for SL/TP.
     Executes close within one tick (<1 s typical on Delta India).

  2. 15-min maintenance cron (APScheduler)
     • Refreshes position cache from DB (picks up newly opened positions)
     • Advances trailing stop (DB write only when price moves)
     • Updates positions.current_price + unrealised_pnl
     • Checks 5-day time-limit exits
     • Checks funding-cost exits (if enabled)
     • Logs heartbeat

Exit conditions (checked in priority order):
  SL — market close; triggers §19.2 4H blackout
  TP — limit close at take_profit_price (paper: exact price; live: limit order)
  Time limit — 5-day hard exit; market close (§8.1)
  Funding cost — rate against direction > 0.1%/8H; market close (§9.1, disabled by default)

Double-exit guard: _exiting_pos_ids set prevents the cron from re-triggering
an exit already in flight from the WebSocket path (and vice versa).
"""

import asyncio
import json
import logging
import math
import os
import time
from typing import Optional

import ccxt
import websockets
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import get_connection, init_db, log_event

log = logging.getLogger(__name__)
MODULE = 'position_monitor'

# ── Constants ──────────────────────────────────────────────────────────────────

DELTA_REST_BASE        = 'https://api.india.delta.exchange'
DELTA_WS_URL           = 'wss://socket.india.delta.exchange'

# Funding cost exit is disabled by default (KRONOS_FUNDING_EXIT_ENABLED=false).
# The funding gate has been moved to M5 entry-side (hard block at 0.5%/8H).
# Exiting an open position due to funding is counter-productive: it guarantees
# a loss on the trade without giving the 24H signal time to play out.
FUNDING_EXIT_ENABLED   = os.environ.get('KRONOS_FUNDING_EXIT_ENABLED', 'false').lower() == 'true'
FUNDING_COST_THRESHOLD = 0.001       # 0.1% per 8H — only used if FUNDING_EXIT_ENABLED=true

# Trailing stop — distance from running extreme to trailing SL, in multiples of entry_atr.
TRAILING_SL_MULTIPLIER   = 1.0
TRAILING_ACTIVATION_MULT = 1.0
USD_INR_RATE           = float(os.environ.get('KRONOS_USD_INR_RATE', '84.0'))
PAPER_MODE             = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'
STOP_LOSS_BLACKOUT_SEC = 4 * 3600
RETRY_DELAY_SEC        = 30

# All monitored symbols — WebSocket subscribes to all regardless of open positions
# so new positions are detected immediately without re-subscribing.
ASSETS: dict[str, str] = {
    'BTCUSD': 'BTC/USD:USD',
    'ETHUSD': 'ETH/USD:USD',
    'BNBUSD': 'BNB/USD:USD',
    'XRPUSD': 'XRP/USD:USD',
}
DELTA_SYMBOLS = list(ASSETS.keys())

_DEFAULT_CONTRACT_SIZES: dict[str, float] = {
    'BTCUSD': 0.001,
    'ETHUSD': 0.01,
    'BNBUSD': 0.1,
    'XRPUSD': 10.0,
}

_CCXT_TO_DELTA: dict[str, str] = {v: k for k, v in ASSETS.items()}


# ── Main class ─────────────────────────────────────────────────────────────────

class PositionMonitor:
    """
    Module 7 — Position Monitor.

    Two concurrent components:
      • WebSocket task  — real-time SL/TP detection on every price tick
      • 15-min cron     — cache refresh, trailing stop, PnL, time limits
    """

    def __init__(self) -> None:
        self._contract_sizes: dict[str, float]           = dict(_DEFAULT_CONTRACT_SIZES)
        self._scheduler:      Optional[AsyncIOScheduler] = None
        self._exchange:       Optional[ccxt.Exchange]    = None
        self._ws_task:        Optional[asyncio.Task]     = None
        self._running:        bool                       = False

        # Position cache: pos_id → position dict (loaded from DB, kept in sync)
        # The WebSocket path reads this to check SL/TP on every tick.
        self._position_cache: dict[int, dict] = {}

        # Prevents double-exit: pos_ids currently being closed by WS or cron.
        self._exiting_pos_ids: set[int] = set()

        # Protects _position_cache and _exiting_pos_ids from concurrent access.
        self._cache_lock: asyncio.Lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _init_exchange(self) -> None:
        kwargs: dict = {'options': {'defaultType': 'swap'}}
        if not PAPER_MODE:
            kwargs['apiKey'] = os.environ.get('KRONOS_API_KEY', '')
            kwargs['secret'] = os.environ.get('KRONOS_API_SECRET', '')
        self._exchange = ccxt.delta(kwargs)
        self._exchange.urls['api']['public'] = DELTA_REST_BASE
        if not PAPER_MODE:
            self._exchange.urls['api']['private'] = DELTA_REST_BASE

        try:
            markets = self._exchange.load_markets()
            for delta_sym, ccxt_sym in ASSETS.items():
                if ccxt_sym in markets:
                    cs = markets[ccxt_sym].get('contractSize')
                    if cs:
                        self._contract_sizes[delta_sym] = float(cs)
            log.info('Contract sizes loaded: %s', self._contract_sizes)
        except Exception as exc:
            log.warning('load_markets() failed — using default contract sizes: %s', exc)

    async def start(self) -> None:
        """Start the position monitoring service. Runs indefinitely."""
        init_db()
        self._init_exchange()
        self._running = True

        # Populate cache before WebSocket starts so first tick can check positions.
        await self._refresh_position_cache()

        # Launch WebSocket real-time monitor.
        self._ws_task = asyncio.create_task(self._ws_run(), name='m7_ws_ticker')

        # Launch 15-min maintenance cron.
        self._scheduler = AsyncIOScheduler(timezone='UTC')
        self._scheduler.add_job(
            self._job_maintenance,
            CronTrigger(minute='0,15,30,45', timezone='UTC'),
            id='position_monitor_cron',
            name='Position Monitor — 15-min maintenance',
            max_instances=1,
        )
        self._scheduler.start()

        log.info('Position Monitor started — WebSocket real-time SL/TP + 15-min cron (paper=%s)',
                 PAPER_MODE)
        log_event(MODULE, 'info', 'info',
                  'Module 7 started — real-time WebSocket SL/TP monitor active',
                  {'paper': PAPER_MODE})

        await asyncio.Event().wait()  # run forever

    # ── WebSocket: real-time SL/TP detection ──────────────────────────────────

    async def _ws_run(self) -> None:
        """
        Maintain a persistent WebSocket connection to Delta India.
        Subscribes to v2/ticker for all 5 symbols.
        On every mark-price tick, checks open positions for SL/TP and fires
        exits immediately in a background executor (non-blocking).

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

                    backoff = 1  # reset on successful connect

                    sub_msg = {
                        'type': 'subscribe',
                        'payload': {
                            'channels': [
                                {'name': 'v2/ticker', 'symbols': DELTA_SYMBOLS},
                            ]
                        },
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info('M7 WebSocket connected — subscribed to v2/ticker for %s',
                             DELTA_SYMBOLS)
                    log_event(MODULE, 'info', 'ws_connected',
                              'M7 real-time ticker WebSocket connected',
                              {'symbols': DELTA_SYMBOLS})

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if msg.get('type') != 'v2/ticker':
                            continue

                        symbol = msg.get('symbol')
                        if symbol not in DELTA_SYMBOLS:
                            continue

                        raw_price = msg.get('mark_price')
                        if raw_price is None:
                            continue
                        try:
                            mark_price = float(raw_price)
                        except (TypeError, ValueError):
                            continue

                        # Check positions for this symbol against new mark price.
                        await self._check_positions_from_tick(symbol, mark_price)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning('M7 WebSocket error: %s — reconnecting in %ds', exc, backoff)
                log_event(MODULE, 'warning', 'ws_disconnected',
                          f'M7 WebSocket disconnected: {exc}. Reconnect in {backoff}s',
                          {'backoff_seconds': backoff, 'error': str(exc)})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        log.info('M7 WebSocket loop exited')

    async def _check_positions_from_tick(self, symbol: str, mark_price: float) -> None:
        """
        Called on every WebSocket ticker update for a symbol.
        Checks all open positions for that symbol against the new mark price.
        Fires exits asynchronously in a thread executor (non-blocking).
        """
        async with self._cache_lock:
            # Build snapshot of positions for this symbol that are not already exiting.
            to_check = [
                pos for pos in self._position_cache.values()
                if pos['symbol'] == symbol and pos['id'] not in self._exiting_pos_ids
            ]

        loop = asyncio.get_running_loop()

        for pos in to_check:
            # Live mode: exchange-side bracket orders handle SL/TP.
            # WebSocket path skips the check — no double-exit risk.
            # M7 maintenance cron reconciles fills from the exchange.
            if pos.get('sl_order_id') and pos.get('tp_order_id'):
                continue

            # Track running high/low on every tick (in-memory only — no DB write).
            # The maintenance cron persists these to the positions table every 15 min.
            # _exit_position reads them from pos at close, so this must happen BEFORE
            # the exit check — otherwise trades that close before the first cron cycle
            # (tight 1H ATR targets) write peak_price=trough_price=entry_price.
            entry_price = pos['entry_price']
            pos['running_high'] = max(pos.get('running_high') or entry_price, mark_price)
            pos['running_low']  = min(pos.get('running_low')  or entry_price, mark_price)

            exit_reason = None

            if self._is_stop_loss(pos, mark_price):
                exit_reason = 'stop_loss'
            elif self._is_take_profit(pos, mark_price):
                exit_reason = 'take_profit'

            if exit_reason:
                # Guard: claim this position before spawning the exit task.
                async with self._cache_lock:
                    if pos['id'] in self._exiting_pos_ids:
                        continue  # another task already claimed it
                    self._exiting_pos_ids.add(pos['id'])

                log.info(
                    'Real-time %s triggered: pos_id=%d trade_id=%d %s %s '
                    'mark=%.4f sl=%.4f tp=%.4f',
                    exit_reason, pos['id'], pos['trade_id'],
                    symbol, pos['direction'], mark_price,
                    pos['stop_loss_price'], pos['take_profit_price'],
                )

                # Run blocking exit (CCXT order / DB write) in thread executor.
                # Use fire-and-forget create_task so we don't block the WS loop.
                asyncio.create_task(
                    self._async_exit(pos, exit_reason, mark_price),
                    name=f'exit_{pos["id"]}_{exit_reason}',
                )

    async def _async_exit(self, pos: dict, exit_reason: str, mark_price: float) -> None:
        """
        Runs _exit_position() in a thread executor, then removes the position
        from the cache so the 15-min cron doesn't attempt a second exit.
        """
        loop = asyncio.get_running_loop()
        try:
            order_type = 'limit' if exit_reason == 'take_profit' else 'market'
            await loop.run_in_executor(
                None, self._exit_position, pos, exit_reason, order_type, mark_price
            )
        except Exception as exc:
            log.exception('_async_exit failed for pos_id=%d: %s', pos['id'], exc)
        finally:
            # Always clean up — whether exit succeeded or failed.
            async with self._cache_lock:
                self._position_cache.pop(pos['id'], None)
                self._exiting_pos_ids.discard(pos['id'])

    # ── 15-min maintenance cron ───────────────────────────────────────────────

    async def _job_maintenance(self) -> None:
        """Async scheduler wrapper — runs maintenance in thread executor."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_maintenance)

    def _run_maintenance(self) -> None:
        """
        Called every 15 min. Responsibilities:
          1. Refresh position cache from DB (picks up new positions).
          2. Advance trailing stop for all open positions.
          3. Update current_price + unrealised_pnl in positions table.
          4. Check time-limit and funding-cost exit conditions.
          5. Log heartbeat.

        SL/TP are NOT checked here — those are handled by the WebSocket path
        in real time. The cron only handles slow/time-based conditions.
        """
        log.debug('M7 maintenance cycle starting')

        # Reconcile exchange-side bracket fills before refreshing cache.
        # Positions closed by the exchange (SL/TP hit) will be removed from DB
        # here so they are absent from the cache after the refresh below.
        if not PAPER_MODE and self._exchange is not None:
            self._reconcile_bracket_fills()

        try:
            # Sync-refresh the position cache (blocking call, safe in executor).
            asyncio.get_event_loop().run_until_complete(self._refresh_position_cache())
        except RuntimeError:
            # If no event loop in this thread (executor), use a fresh loop.
            asyncio.run(self._refresh_position_cache())

        try:
            self._maintenance_checks()
        except Exception as exc:
            log.exception('Unhandled error in _run_maintenance: %s', exc)
            log_event(MODULE, 'critical', 'execution_error',
                      f'Unhandled exception in maintenance cycle: {exc}')

    def _maintenance_checks(self) -> None:
        """
        Iterate open positions from DB; update PnL, trailing stop, time/funding exits.
        Skips positions already being exited by the WebSocket path.
        """
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT p.id, p.trade_id, p.symbol, p.direction,
                          p.entry_price, p.size_contracts,
                          p.stop_loss_price, p.take_profit_price,
                          p.margin_used, p.leverage,
                          p.entry_timestamp, p.max_hold_until,
                          p.unrealised_pnl,
                          p.entry_atr, p.running_extreme,
                          p.running_high, p.running_low,
                          p.horizon_exit_at
                   FROM positions p WHERE p.status='open'"""
            ).fetchall()

        if not rows:
            log_event(MODULE, 'info', 'heartbeat', 'Maintenance cycle: no open positions')
            return

        for row in rows:
            pos = dict(row)
            pos_id = pos['id']

            # Skip positions already claimed by the WebSocket exit path.
            # Use a simple non-async check — we're in a thread executor here.
            if pos_id in self._exiting_pos_ids:
                continue

            try:
                self._maintain_position(pos)
            except Exception as exc:
                log.exception('Maintenance error pos_id=%d: %s', pos_id, exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Maintenance error pos_id={pos_id}: {exc}',
                          {'pos_id': pos_id, 'trade_id': pos.get('trade_id')})

        log_event(MODULE, 'info', 'heartbeat',
                  f'Maintenance cycle complete — {len(rows)} positions checked')

    def _maintain_position(self, pos: dict) -> None:
        """
        Non-SL/TP maintenance for one position:
          • Get latest mark price.
          • Update unrealised PnL.
          • Advance trailing stop.
          • Check time-limit exit.
          • Check funding-cost exit (if enabled).
        """
        symbol    = pos['symbol']
        trade_id  = pos['trade_id']
        direction = pos['direction']

        mark_price   = self._get_mark_price(symbol)
        funding_rate = self._get_funding_rate(symbol, after_ts=pos['entry_timestamp'])

        if mark_price is not None:
            self._update_position_price(pos, mark_price)
            pos = self._update_trailing_stop(pos, mark_price)

        # Horizon exit — paper mode only.
        # Exits at signal_timestamp + horizon_seconds regardless of P&L.
        # Tests the model's actual prediction directly: was the direction right
        # when the prediction window closed? No ATR-based target distortion.
        if PAPER_MODE and self._is_horizon_exit(pos):
            pos_id = pos['id']
            if pos_id in self._exiting_pos_ids:
                return
            self._exiting_pos_ids.add(pos_id)
            mp = mark_price or pos['entry_price']
            pnl_sign = '+' if (
                (pos['direction'] == 'long'  and mp > pos['entry_price']) or
                (pos['direction'] == 'short' and mp < pos['entry_price'])
            ) else '-'
            log.info('Horizon exit: trade_id=%d %s %s @ %.4f (entry=%.4f %s)',
                     pos['trade_id'], pos['symbol'], pos['direction'],
                     mp, pos['entry_price'], pnl_sign)
            self._exit_position(pos, 'horizon_exit', 'market', mp)
            self._exiting_pos_ids.discard(pos_id)
            return

        # Time limit (5-day hard exit)
        if self._is_time_limit(pos):
            pos_id = pos['id']
            if pos_id in self._exiting_pos_ids:
                return
            self._exiting_pos_ids.add(pos_id)
            log.info('Time limit triggered: trade_id=%d %s held past 5 days', trade_id, symbol)
            # Cancel bracket orders before placing the market close so
            # exchange doesn't double-execute on the same position.
            self._cancel_bracket_orders(pos)
            mp = mark_price or pos['entry_price']
            self._exit_position(pos, 'time_limit', 'market', mp)
            self._exiting_pos_ids.discard(pos_id)
            return

        # Funding cost exit (disabled by default)
        if FUNDING_EXIT_ENABLED and self._is_funding_cost_exit(pos, funding_rate):
            pos_id = pos['id']
            if pos_id in self._exiting_pos_ids:
                return
            self._exiting_pos_ids.add(pos_id)
            log.info('Funding cost exit triggered: trade_id=%d %s %s rate=%.6f',
                     trade_id, symbol, direction, funding_rate)
            self._cancel_bracket_orders(pos)
            mp = mark_price or pos['entry_price']
            self._exit_position(pos, 'funding_cost', 'market', mp)
            self._exiting_pos_ids.discard(pos_id)
            return

    # ── Position cache ────────────────────────────────────────────────────────

    async def _refresh_position_cache(self) -> None:
        """
        Load all open positions from DB into _position_cache.
        Called on startup and every 15 min so newly opened positions
        are picked up without restarting M7.
        """
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT p.id, p.trade_id, p.symbol, p.direction,
                              p.entry_price, p.size_contracts,
                              p.stop_loss_price, p.take_profit_price,
                              p.margin_used, p.leverage,
                              p.entry_timestamp, p.max_hold_until,
                              p.unrealised_pnl,
                              p.entry_atr, p.running_extreme,
                              p.running_high, p.running_low,
                              p.sl_order_id, p.tp_order_id,
                              p.horizon_exit_at
                       FROM positions p WHERE p.status='open'"""
                ).fetchall()

            async with self._cache_lock:
                db_ids = {row['id'] for row in rows}

                # Add new positions.
                for row in rows:
                    pid = row['id']
                    if pid not in self._position_cache:
                        self._position_cache[pid] = dict(row)
                        log.debug('Cache: added pos_id=%d %s %s',
                                  pid, row['symbol'], row['direction'])

                # Remove positions that are no longer open in DB
                # (closed by a previous exit or external action).
                stale = [pid for pid in self._position_cache if pid not in db_ids]
                for pid in stale:
                    sym = self._position_cache[pid].get('symbol', '?')
                    log.debug('Cache: removed stale pos_id=%d %s', pid, sym)
                    del self._position_cache[pid]
                    self._exiting_pos_ids.discard(pid)

            log.debug('Position cache refreshed: %d open positions', len(self._position_cache))

        except Exception as exc:
            log.warning('Failed to refresh position cache: %s', exc)

    # ── Exit condition checks ─────────────────────────────────────────────────

    @staticmethod
    def _is_stop_loss(pos: dict, mark_price: float) -> bool:
        if pos['direction'] == 'long':
            return mark_price <= pos['stop_loss_price']
        else:
            return mark_price >= pos['stop_loss_price']

    @staticmethod
    def _is_take_profit(pos: dict, mark_price: float) -> bool:
        if pos['direction'] == 'long':
            return mark_price >= pos['take_profit_price']
        else:
            return mark_price <= pos['take_profit_price']

    @staticmethod
    def _is_time_limit(pos: dict) -> bool:
        return int(time.time()) >= pos['max_hold_until']

    @staticmethod
    def _is_horizon_exit(pos: dict) -> bool:
        """
        Paper mode only. True when the model's prediction window has elapsed.
        horizon_exit_at = signal_timestamp + horizon_seconds (set by M6).
        Fires at T+6H for 1H models, T+24H for 4H/custom models.
        NULL on positions pre-dating this column — returns False (ignored).
        """
        horizon_exit_at = pos.get('horizon_exit_at')
        if not horizon_exit_at:
            return False
        return int(time.time()) >= horizon_exit_at

    @staticmethod
    def _is_funding_cost_exit(pos: dict, funding_rate: Optional[float]) -> bool:
        if funding_rate is None:
            return False
        if pos['direction'] == 'long' and funding_rate > FUNDING_COST_THRESHOLD:
            return True
        if pos['direction'] == 'short' and funding_rate < -FUNDING_COST_THRESHOLD:
            return True
        return False

    # ── Trailing stop ────────────────────────────────────────────────────────

    def _update_trailing_stop(self, pos: dict, mark_price: float) -> dict:
        """
        Advance the trailing stop loss if price has moved in the favourable direction.
        Called from the 15-min maintenance cron (not from the WS tick path).

        Disabled in paper mode — fixed SL/TP run to completion so benchmark data
        reflects true model R:R without the trailing stop cutting wins short.
        Re-evaluate trailing parameters after paper trading phase completes.

        Logic:
          - Tracks running_extreme: highest mark seen (long) / lowest (short).
          - Activates only after price has moved ≥ TRAILING_ACTIVATION_MULT × ATR.
          - Trailing SL = running_extreme ∓ TRAILING_SL_MULTIPLIER × ATR.
          - SL only advances (never retreats).
        """
        if PAPER_MODE:
            return pos   # disabled in paper mode — let fixed SL/TP run to completion

        entry_atr = pos.get('entry_atr')
        if not entry_atr or entry_atr <= 0:
            return pos

        direction       = pos['direction']
        entry_price     = pos['entry_price']
        old_sl          = pos['stop_loss_price']
        running_extreme = pos.get('running_extreme') or entry_price

        new_extreme = running_extreme
        new_sl      = old_sl

        if direction == 'long':
            if mark_price > running_extreme:
                new_extreme = mark_price
            if new_extreme >= entry_price + TRAILING_ACTIVATION_MULT * entry_atr:
                candidate_sl = new_extreme - TRAILING_SL_MULTIPLIER * entry_atr
                if candidate_sl > old_sl:
                    new_sl = candidate_sl
        else:
            if mark_price < running_extreme:
                new_extreme = mark_price
            if new_extreme <= entry_price - TRAILING_ACTIVATION_MULT * entry_atr:
                candidate_sl = new_extreme + TRAILING_SL_MULTIPLIER * entry_atr
                if candidate_sl < old_sl:
                    new_sl = candidate_sl

        if new_extreme == running_extreme and new_sl == old_sl:
            return pos

        # ── Live mode: cancel old SL bracket, place new one ───────────────────
        # Order: place NEW first → cancel OLD.
        # Brief overlap (both orders live simultaneously) is safe — reduceOnly
        # means only one can fill (the closer stop fires, the other becomes
        # reduce-only with no position to reduce and is auto-cancelled).
        old_sl_order_id = pos.get('sl_order_id')
        new_sl_order_id = old_sl_order_id  # unchanged unless we successfully re-place

        if new_sl != old_sl and old_sl_order_id and self._exchange is not None:
            ccxt_sym    = ASSETS.get(pos['symbol'])
            close_side  = 'sell' if pos['direction'] == 'long' else 'buy'
            size        = pos['size_contracts']

            # 1. Place new stop-market SL at advanced price.
            try:
                new_order = self._exchange.create_order(
                    symbol=ccxt_sym,
                    type='stop_market',
                    side=close_side,
                    amount=size,
                    params={'stopPrice': new_sl, 'reduceOnly': True},
                )
                new_sl_order_id = str(new_order['id'])
                log.info('Trailing SL bracket placed: id=%s %s stop=%.4f',
                         new_sl_order_id, ccxt_sym, new_sl)
            except Exception as exc:
                log.warning('Failed to place new trailing SL for %s (pos=%d): %s — '
                            'keeping old SL bracket %s',
                            pos['symbol'], pos['id'], exc, old_sl_order_id)
                # Keep old order ID, don't advance SL in DB.
                new_sl_order_id = old_sl_order_id
                new_sl = old_sl  # revert — old bracket still protects position

            # 2. Cancel old SL order only if new one was placed successfully.
            if new_sl_order_id != old_sl_order_id:
                self._cancel_order_safe(ccxt_sym, old_sl_order_id)

        # ── Write to DB ───────────────────────────────────────────────────────
        try:
            with get_connection() as conn:
                conn.execute(
                    """UPDATE positions
                       SET running_extreme=?, stop_loss_price=?, sl_order_id=?
                       WHERE id=?""",
                    (new_extreme, new_sl, new_sl_order_id, pos['id']),
                )
        except Exception as exc:
            log.warning('Failed to update trailing stop for pos %d: %s', pos['id'], exc)
            return pos

        if new_sl != old_sl:
            log.info(
                'Trailing SL advanced: pos=%d %s %s sl=%.4f→%.4f extreme=%.4f',
                pos['id'], pos['symbol'], direction, old_sl, new_sl, new_extreme,
            )
            log_event(MODULE, 'info', 'trailing_sl_advanced',
                      f'{pos["symbol"]} {direction.upper()} trailing SL '
                      f'{old_sl:.4f}→{new_sl:.4f} (extreme={new_extreme:.4f})',
                      {'position_id': pos['id'], 'trade_id': pos['trade_id'],
                       'symbol': pos['symbol'], 'direction': direction,
                       'old_sl': old_sl, 'new_sl': new_sl,
                       'running_extreme': new_extreme, 'entry_atr': entry_atr,
                       'old_sl_order_id': old_sl_order_id,
                       'new_sl_order_id': new_sl_order_id})

        # Sync to in-memory cache so the WS path uses updated values.
        pos = dict(pos)
        pos['running_extreme'] = new_extreme
        pos['stop_loss_price'] = new_sl
        pos['sl_order_id']     = new_sl_order_id
        try:
            self._position_cache[pos['id']]['running_extreme'] = new_extreme
            self._position_cache[pos['id']]['stop_loss_price'] = new_sl
            self._position_cache[pos['id']]['sl_order_id']     = new_sl_order_id
        except (KeyError, TypeError):
            pass
        return pos

    # ── Bracket order reconciliation (live mode) ──────────────────────────────

    def _reconcile_bracket_fills(self) -> None:
        """
        Called every 15 min (live mode only). For each open position that has
        exchange-side bracket orders, fetches order status from the exchange.

        If a bracket order is filled:
          • Determines exit_reason (stop_loss or take_profit).
          • Cancels the surviving sibling order.
          • Updates trades + positions tables (closes the position in DB).

        This is how M7 learns that the exchange executed a SL or TP.
        Without this, the position would remain open in the DB indefinitely
        even though the exchange already closed it.
        """
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT p.id, p.trade_id, p.symbol, p.direction,
                          p.entry_price, p.size_contracts,
                          p.stop_loss_price, p.take_profit_price,
                          p.margin_used, p.leverage,
                          p.entry_timestamp, p.max_hold_until,
                          p.unrealised_pnl, p.entry_atr, p.running_extreme,
                          p.running_high, p.running_low,
                          p.sl_order_id, p.tp_order_id,
                          p.horizon_exit_at
                   FROM positions p
                   WHERE p.status='open'
                     AND (p.sl_order_id IS NOT NULL OR p.tp_order_id IS NOT NULL)"""
            ).fetchall()

        for row in rows:
            pos    = dict(row)
            pos_id = pos['id']

            if pos_id in self._exiting_pos_ids:
                continue

            ccxt_sym       = ASSETS.get(pos['symbol'])
            sl_order_id    = pos.get('sl_order_id')
            tp_order_id    = pos.get('tp_order_id')
            filled_reason  = None
            filled_price   = None
            sibling_id     = None

            # Check SL order
            if sl_order_id and ccxt_sym:
                try:
                    order = self._exchange.fetch_order(sl_order_id, symbol=ccxt_sym)
                    if order.get('status') in ('closed', 'filled'):
                        filled_reason = 'stop_loss'
                        filled_price  = float(order.get('average') or order.get('price') or 0)
                        sibling_id    = tp_order_id
                except Exception as exc:
                    log.warning('fetch_order SL %s failed: %s', sl_order_id, exc)

            # Check TP order (only if SL hasn't already filled)
            if filled_reason is None and tp_order_id and ccxt_sym:
                try:
                    order = self._exchange.fetch_order(tp_order_id, symbol=ccxt_sym)
                    if order.get('status') in ('closed', 'filled'):
                        filled_reason = 'take_profit'
                        filled_price  = float(order.get('average') or order.get('price') or 0)
                        sibling_id    = sl_order_id
                except Exception as exc:
                    log.warning('fetch_order TP %s failed: %s', tp_order_id, exc)

            if filled_reason and filled_price:
                # Claim position before processing to prevent double-exit.
                self._exiting_pos_ids.add(pos_id)

                log.info('Reconciled bracket fill: pos=%d %s %s %s @ %.4f',
                         pos_id, pos['symbol'], pos['direction'],
                         filled_reason, filled_price)

                # Cancel the surviving sibling order (exchange may have already
                # cancelled it via reduceOnly, but we explicitly cancel to be sure).
                if sibling_id and ccxt_sym:
                    self._cancel_order_safe(ccxt_sym, sibling_id)

                # Close position in DB using the exchange fill price.
                self._exit_position(pos, filled_reason, 'market', filled_price)
                self._exiting_pos_ids.discard(pos_id)

    def _cancel_bracket_orders(self, pos: dict) -> None:
        """
        Cancel both bracket orders for a position (called before a software-
        initiated exit such as time_limit or funding_cost so the exchange
        doesn't execute a stale bracket after the position is already closed).
        No-op in paper mode or when order IDs are absent.
        """
        if PAPER_MODE or self._exchange is None:
            return
        ccxt_sym = ASSETS.get(pos['symbol'])
        if not ccxt_sym:
            return
        for order_id in (pos.get('sl_order_id'), pos.get('tp_order_id')):
            if order_id:
                self._cancel_order_safe(ccxt_sym, order_id)

    def _cancel_order_safe(self, ccxt_sym: str, order_id: str) -> None:
        """
        Cancel an order by ID. Swallows 'order already closed/cancelled' errors
        (these are expected when the exchange auto-cancels via reduceOnly).
        """
        try:
            self._exchange.cancel_order(order_id, symbol=ccxt_sym)
            log.debug('Cancelled order %s on %s', order_id, ccxt_sym)
        except ccxt.OrderNotFound:
            log.debug('Order %s already gone (reduceOnly auto-cancel?)', order_id)
        except Exception as exc:
            log.warning('cancel_order %s failed: %s', order_id, exc)

    # ── Exit execution ────────────────────────────────────────────────────────

    def _exit_position(
        self,
        pos:         dict,
        exit_reason: str,
        order_type:  str,  # 'market' | 'limit'
        mark_price:  float,
    ) -> None:
        """
        Place a close order (real or paper), update trades + positions tables,
        and fire the appropriate event. For SL, also logs stop_loss_exit so
        Module 5 enforces the §19.2 4H same-asset blackout.
        """
        trade_id  = pos['trade_id']
        pos_id    = pos['id']
        symbol    = pos['symbol']
        direction = pos['direction']
        size      = pos['size_contracts']

        close_side   = 'sell' if direction == 'long' else 'buy'
        target_price = pos['take_profit_price'] if exit_reason == 'take_profit' else None

        if PAPER_MODE:
            exit_price = pos['take_profit_price'] if exit_reason == 'take_profit' else mark_price
        else:
            ccxt_sym = ASSETS.get(symbol)
            if ccxt_sym is None:
                log.error('Unknown symbol %s — cannot close trade %d', symbol, trade_id)
                return
            exit_price = self._place_close_order(
                ccxt_sym, close_side, size, target_price, order_type,
                pos_id=pos_id, mark_price_fallback=mark_price,
            )
            if exit_price is None:
                log.error('Close order failed for trade %d (%s) — exit deferred',
                          trade_id, symbol)
                log_event(MODULE, 'error', 'execution_error',
                          f'Close order failed for trade {trade_id} ({symbol}) — exit deferred',
                          {'trade_id': trade_id, 'symbol': symbol, 'exit_reason': exit_reason})
                return

        exit_ts   = int(time.time())
        pnl_gross = self._compute_pnl_gross(pos, exit_price)

        peak_price   = pos.get('running_high') or pos['entry_price']
        trough_price = pos.get('running_low')  or pos['entry_price']

        with get_connection() as conn:
            conn.execute(
                """UPDATE trades
                   SET exit_price=?, exit_timestamp=?, exit_reason=?,
                       pnl_gross=?, peak_price=?, trough_price=?, status='closed'
                   WHERE id=?""",
                (exit_price, exit_ts, exit_reason, pnl_gross,
                 peak_price, trough_price, trade_id),
            )
            if PAPER_MODE:
                conn.execute("DELETE FROM positions WHERE id=?", (pos_id,))
            else:
                conn.execute(
                    "UPDATE positions SET status='closing' WHERE id=?",
                    (pos_id,),
                )

        exit_data = {
            'trade_id':       trade_id,
            'symbol':         symbol,
            'direction':      direction,
            'exit_reason':    exit_reason,
            'exit_price':     exit_price,
            'entry_price':    pos['entry_price'],
            'pnl_gross':      round(pnl_gross, 2),
            'exit_timestamp': exit_ts,
            'paper':          PAPER_MODE,
            'realtime_exit':  True,   # marks this as WebSocket-triggered (vs old cron path)
        }

        if exit_reason == 'stop_loss':
            blackout_until = exit_ts + STOP_LOSS_BLACKOUT_SEC
            log_event(
                MODULE, 'warning', 'stop_loss_exit',
                f'{symbol} stop loss exit at {exit_price:.4f} — '
                f'new entries blocked until {blackout_until}',
                {**exit_data, 'blackout_until': blackout_until},
            )
        elif exit_reason == 'take_profit':
            log_event(MODULE, 'info', 'take_profit_exit',
                      f'{symbol} {direction.upper()} take profit @ {exit_price:.4f}, '
                      f'pnl={pnl_gross:+.2f} INR',
                      exit_data)
        elif exit_reason == 'time_limit':
            log_event(MODULE, 'info', 'time_limit_exit',
                      f'{symbol} {direction.upper()} time limit @ {exit_price:.4f}, '
                      f'pnl={pnl_gross:+.2f} INR',
                      exit_data)
        elif exit_reason == 'funding_cost':
            log_event(MODULE, 'info', 'funding_cost_exit',
                      f'{symbol} {direction.upper()} funding cost exit @ {exit_price:.4f}, '
                      f'pnl={pnl_gross:+.2f} INR',
                      exit_data)

        log.info('Position exit: trade=%d %s %s %s pnl=%.2f INR',
                 trade_id, symbol, direction, exit_reason, pnl_gross)

    # ── Order placement ───────────────────────────────────────────────────────

    def _place_close_order(
        self,
        ccxt_sym:            str,
        side:                str,
        amount:              float,
        price:               Optional[float],
        order_type:          str,
        pos_id:              Optional[int] = None,
        mark_price_fallback: Optional[float] = None,
    ) -> Optional[float]:
        """
        Place a close order via CCXT. Returns fill price or None on failure.
        Retries once after RETRY_DELAY_SEC on NetworkError (§10.4).
        """
        for attempt in range(2):
            try:
                kwargs: dict = {'params': {'reduceOnly': True}}
                if order_type == 'limit' and price is not None:
                    kwargs['price'] = price

                order = self._exchange.create_order(
                    symbol=ccxt_sym,
                    type=order_type,
                    side=side,
                    amount=amount,
                    **kwargs,
                )

                filled    = float(order.get('filled') or 0)
                remaining = float(order.get('remaining') or 0)

                if filled > 0 and remaining > 0:
                    order_id = order.get('id')
                    try:
                        self._exchange.cancel_order(order_id, ccxt_sym)
                        log.warning('Close order %s partial fill (%.6f/%.6f) — '
                                    'remainder cancelled, will re-check next cycle',
                                    order_id, filled, amount)
                        log_event(MODULE, 'warning', 'execution_error',
                                  f'Partial close {order_id} on {ccxt_sym}: '
                                  f'{filled}/{amount} filled, remainder cancelled',
                                  {'order_id': order_id, 'filled': filled, 'amount': amount})
                    except Exception as cancel_exc:
                        log.error('Failed to cancel partial close %s: %s', order_id, cancel_exc)
                    if pos_id is not None:
                        new_size      = amount - filled
                        delta_sym     = _CCXT_TO_DELTA.get(ccxt_sym, '')
                        contract_size = self._contract_sizes.get(delta_sym, 1.0)
                        if mark_price_fallback is not None:
                            new_notional = new_size * contract_size * mark_price_fallback * USD_INR_RATE
                        else:
                            with get_connection() as conn:
                                nb_row = conn.execute(
                                    'SELECT notional_value FROM positions WHERE id=?',
                                    (pos_id,),
                                ).fetchone()
                            old_notional = float(nb_row['notional_value'] or 0.0) if nb_row else 0.0
                            new_notional = old_notional * (new_size / amount) if amount > 0 else 0.0
                        with get_connection() as conn:
                            conn.execute(
                                "UPDATE positions SET size_contracts=?, notional_value=? WHERE id=?",
                                (new_size, new_notional, pos_id),
                            )
                    return None

                fill_price = (
                    order.get('average')
                    or order.get('price')
                    or price
                    or mark_price_fallback
                )
                return float(fill_price) if fill_price else None

            except ccxt.NetworkError as exc:
                if attempt == 0:
                    log.warning('API timeout closing %s attempt 1, retry in %ds: %s',
                                ccxt_sym, RETRY_DELAY_SEC, exc)
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    log.error('API timeout on second attempt closing %s: %s', ccxt_sym, exc)
                    return None
            except ccxt.ExchangeError as exc:
                log.error('Exchange rejected close order for %s: %s', ccxt_sym, exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Exchange rejected close order for {ccxt_sym}: {exc}',
                          {'ccxt_symbol': ccxt_sym, 'side': side, 'amount': amount})
                return None
        return None

    # ── PnL and price helpers ─────────────────────────────────────────────────

    def _compute_pnl_gross(self, pos: dict, exit_price: float) -> float:
        """
        Gross P&L in INR before TDS, fees, and tax.
        Long:  (exit − entry) × size × contract_size × USD_INR_RATE
        Short: (entry − exit) × size × contract_size × USD_INR_RATE
        """
        symbol        = pos['symbol']
        contract_size = self._contract_sizes.get(symbol, _DEFAULT_CONTRACT_SIZES.get(symbol, 0.001))
        price_diff    = exit_price - pos['entry_price']
        direction_mul = 1.0 if pos['direction'] == 'long' else -1.0
        return direction_mul * price_diff * pos['size_contracts'] * contract_size * USD_INR_RATE

    def _update_position_price(self, pos: dict, mark_price: float) -> None:
        """Update current_price, unrealised_pnl, running_high, and running_low."""
        pnl         = self._compute_pnl_gross(pos, mark_price)
        entry_price = pos['entry_price']
        new_high    = max(pos.get('running_high') or entry_price, mark_price)
        new_low     = min(pos.get('running_low')  or entry_price, mark_price)
        try:
            with get_connection() as conn:
                conn.execute(
                    """UPDATE positions
                       SET current_price=?, unrealised_pnl=?,
                           running_high=?, running_low=?
                       WHERE id=?""",
                    (mark_price, pnl, new_high, new_low, pos['id']),
                )
            if pos['id'] in self._position_cache:
                self._position_cache[pos['id']]['unrealised_pnl'] = pnl
                self._position_cache[pos['id']]['running_high']   = new_high
                self._position_cache[pos['id']]['running_low']    = new_low
        except Exception as exc:
            log.warning('Failed to update position price for pos %d: %s', pos['id'], exc)

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _get_mark_price(self, delta_sym: str) -> Optional[float]:
        """Latest mark_price from orderbook_snapshots (used by maintenance cron only)."""
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT mark_price FROM orderbook_snapshots
                       WHERE symbol=? AND mark_price IS NOT NULL
                       ORDER BY id DESC LIMIT 1""",
                    (delta_sym,),
                ).fetchone()
            if row and row['mark_price']:
                return float(row['mark_price'])
        except Exception as exc:
            log.warning('mark_price unavailable for %s: %s', delta_sym, exc)
        return None

    def _get_funding_rate(
        self,
        delta_sym: str,
        after_ts:  Optional[int] = None,
    ) -> Optional[float]:
        """Most recent settled funding rate for the symbol (Section 9.4)."""
        try:
            with get_connection() as conn:
                if after_ts is not None:
                    row = conn.execute(
                        """SELECT rate FROM funding_rates
                           WHERE symbol=? AND timestamp > ?
                           ORDER BY id DESC LIMIT 1""",
                        (delta_sym, after_ts),
                    ).fetchone()
                else:
                    row = conn.execute(
                        """SELECT rate FROM funding_rates
                           WHERE symbol=? ORDER BY id DESC LIMIT 1""",
                        (delta_sym,),
                    ).fetchone()
            if row:
                return float(row['rate'])
        except Exception as exc:
            log.warning('funding_rate unavailable for %s: %s', delta_sym, exc)
        return None


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    asyncio.run(PositionMonitor().start())


if __name__ == '__main__':
    main()
