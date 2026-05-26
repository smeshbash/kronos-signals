"""
Kronos Trading System — Module 6: Execution
Sections 7.1, 8.1, 10.1-10.5, 11.2 of the spec (v2.6).

Places and manages limit orders on Delta Exchange. Paper mode (KRONOS_PAPER_MODE=true)
simulates an immediate fill without touching the exchange API.

Position sizing (Section 10.3):
  margin_inr     = portfolio_value × MARGIN_PCT  (10% income phase)
  notional_inr   = margin_inr × leverage         (2x standard)
  size_contracts = notional_inr / (contract_size_usd × mark_price_usd × USD_INR_RATE)

SL / TP (Section 8.1):
  sl_amount_inr = portfolio_value × 3%
  tp_amount_inr = portfolio_value × 6%
  sl_price (long) = entry - sl_amount / (size × contract_size × USD_INR_RATE)
  tp_price (long) = entry + tp_amount / (size × contract_size × USD_INR_RATE)
  R:R = tp_distance / sl_distance = 6% / 3% = 2.0 (by design; validated at execution)

Schedule (UTC):
  Cron:        00:09, 04:09, 08:09, 12:09, 16:09, 20:09 — process approved signals
  DateTrigger: order_time + 4H per order — check fill status / cancel unfilled
"""

import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from db import get_connection, init_db, log_event

log = logging.getLogger(__name__)
MODULE = 'execution'

# ── Constants ──────────────────────────────────────────────────────────────────

DELTA_REST_BASE = 'https://api.india.delta.exchange'

MARGIN_PCT       = 0.10               # 10% margin per position, income phase (Section 10.3)
LEVERAGE_DEFAULT = float(os.environ.get('KRONOS_LEVERAGE', '2.0'))
MAX_LEVERAGE     = 3.0                # hard ceiling (Section 8.1)
SL_PCT           = 0.03              # 3% of portfolio stop loss (Section 8.1)
TP_PCT           = 0.06              # 6% of portfolio take profit (Section 8.1)
MIN_RR_RATIO     = 2.0               # minimum 1:2 R:R ratio (Section 8.1)
FILL_TIMEOUT_SEC = 4 * 3600          # 4H fill timeout (Section 10.1)
MAX_HOLD_DAYS    = 5                 # 5-day hard exit (Section 8.1)
RETRY_DELAY_SEC  = 30                # retry once after 30s on API timeout (Section 10.4)

PAPER_MODE             = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'
USD_INR_RATE           = float(os.environ.get('KRONOS_USD_INR_RATE', '84.0'))
PORTFOLIO_FALLBACK_INR = float(os.environ.get('KRONOS_PORTFOLIO_VALUE_INR', '100000.0'))

# Delta symbol → CCXT perpetual symbol
ASSETS: dict[str, str] = {
    'BTCUSD': 'BTC/USD:USD',
    'ETHUSD': 'ETH/USD:USD',
    'SOLUSD': 'SOL/USD:USD',
    'BNBUSD': 'BNB/USD:USD',
    'XRPUSD': 'XRP/USD:USD',
}

# Fallback contract sizes (base units per contract) when exchange.load_markets() fails
_DEFAULT_CONTRACT_SIZES: dict[str, float] = {
    'BTCUSD': 0.001,
    'ETHUSD': 0.01,
    'SOLUSD': 1.0,
    'BNBUSD': 0.1,
    'XRPUSD': 10.0,
}


# ── Exception ──────────────────────────────────────────────────────────────────

class ExecutionSkipped(Exception):
    """
    Signal cannot be executed due to a sizing or validation constraint.
    The signal is rejected (not retried). Does not halt the system.
    """


# ── Main class ─────────────────────────────────────────────────────────────────

class Execution:
    """
    Module 6 — Execution.

    Cron-triggered every 4H at :09 UTC. Reads approved signals from the DB,
    sizes each position, places a limit order on Delta Exchange (or simulates in
    paper mode), writes the trade + position rows, and schedules a 4H fill-timeout
    job per order.
    """

    def __init__(self) -> None:
        leverage = LEVERAGE_DEFAULT
        if leverage > MAX_LEVERAGE:
            log.warning(
                'KRONOS_LEVERAGE=%.1f exceeds MAX_LEVERAGE=%.1f — clamped',
                leverage, MAX_LEVERAGE,
            )
            leverage = MAX_LEVERAGE
        self._leverage:        float                        = leverage
        self._contract_sizes:  dict[str, float]             = dict(_DEFAULT_CONTRACT_SIZES)
        self._scheduler:       Optional[AsyncIOScheduler]   = None
        self._exchange:        Optional[ccxt.Exchange]      = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _init_exchange(self) -> None:
        """Initialise CCXT Delta exchange and attempt to load market contract sizes."""
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
        """Start the execution service. Runs indefinitely."""
        init_db()
        self._init_exchange()
        self._scheduler = AsyncIOScheduler(timezone='UTC')
        self._scheduler.add_job(
            self._job_run,
            CronTrigger(hour='0,4,8,12,16,20', minute=9, timezone='UTC'),
            id='execution_cron',
            name='Execution — process approved signals',
            max_instances=1,
        )
        self._scheduler.start()
        log.info('Execution started (paper=%s, leverage=%.1fx)', PAPER_MODE, self._leverage)
        await asyncio.Event().wait()

    # ── Cron entry point ──────────────────────────────────────────────────────

    async def _job_run(self) -> None:
        """Async scheduler wrapper — runs sync run() in thread executor."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.run)

    def run(self) -> None:
        """Cron-triggered every 4H at :09 UTC. Processes all approved signals."""
        log.info('Execution: processing approved signals')
        try:
            self._process_approved_signals()
        except Exception as exc:
            log.exception('Unhandled error in Execution.run(): %s', exc)
            log_event(MODULE, 'critical', 'execution_error',
                      f'Unhandled exception in run(): {exc}')

    def _process_approved_signals(self) -> None:
        """Query all approved signals and execute each in order of signal_timestamp."""
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT id, symbol, direction, confidence, signal_timestamp
                   FROM signals WHERE status='approved'
                   ORDER BY signal_timestamp ASC""",
            ).fetchall()

        if not rows:
            log.debug('No approved signals to execute')
            log_event(MODULE, 'info', 'heartbeat', 'No approved signals this cycle')
            return

        halt = False
        for row in rows:
            if halt:
                break
            try:
                halt = self._execute_signal(dict(row))
            except Exception as exc:
                log.exception('Unhandled error on signal %d: %s', row['id'], exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Unhandled error on signal {row["id"]} ({row["symbol"]}): {exc}',
                          {'signal_id': row['id'], 'symbol': row['symbol']})

    def _execute_signal(self, signal: dict) -> bool:
        """
        Execute one approved signal.
        Returns True to halt further processing this cycle (on unrecoverable API failure).
        """
        signal_id = signal['id']
        symbol    = signal['symbol']
        direction = signal['direction']

        ccxt_sym = ASSETS.get(symbol)
        if ccxt_sym is None:
            log.error('Unknown symbol %s in signal %d — rejecting', symbol, signal_id)
            Execution._update_signal(signal_id, 'rejected', f'unknown_symbol_{symbol}')
            return False

        risk_data       = self._get_risk_data(signal_id)
        portfolio_value = self._get_portfolio_value()
        mark_price      = self._get_mark_price(symbol)

        if mark_price is None:
            log.error('No mark price for %s, signal %d — rejecting', symbol, signal_id)
            log_event(MODULE, 'error', 'execution_error',
                      f'No mark price available for {symbol}, signal {signal_id}',
                      {'signal_id': signal_id, 'symbol': symbol})
            Execution._update_signal(signal_id, 'rejected', 'no_mark_price_available')
            return False

        try:
            (margin_inr, notional_inr, size_contracts,
             entry_price, sl_price, tp_price) = self._size_position(
                symbol, direction, risk_data, portfolio_value, mark_price,
            )
        except ExecutionSkipped as exc:
            log.warning('Signal %d skipped: %s', signal_id, exc)
            log_event(MODULE, 'warning', 'execution_error',
                      f'Signal {signal_id} ({symbol}) skipped: {exc}',
                      {'signal_id': signal_id, 'symbol': symbol, 'reason': str(exc)})
            Execution._update_signal(signal_id, 'rejected', str(exc))
            return False

        contract_size  = self._contract_sizes.get(symbol, _DEFAULT_CONTRACT_SIZES[symbol])
        notional_value = size_contracts * contract_size * entry_price * USD_INR_RATE
        now            = int(time.time())

        if PAPER_MODE:
            self._execute_paper(
                signal_id, symbol, direction,
                size_contracts, entry_price, sl_price, tp_price,
                margin_inr, notional_value, now,
            )
            return False

        return self._execute_live(
            signal_id, symbol, ccxt_sym, direction,
            size_contracts, entry_price, sl_price, tp_price,
            margin_inr, notional_value, now,
        )

    # ── Position sizing ───────────────────────────────────────────────────────

    def _size_position(
        self,
        symbol:          str,
        direction:       str,
        risk_data:       dict,
        portfolio_value: float,
        mark_price:      float,
    ) -> tuple[float, float, float, float, float, float]:
        """
        Compute (margin_inr, notional_inr, size_contracts, entry_price, sl_price, tp_price).
        Raises ExecutionSkipped if the position cannot be validly constructed.
        """
        size_cap_pct            = risk_data.get('size_cap_pct')
        combined_margin_cap_pct = risk_data.get('combined_margin_cap_pct')

        # Effective margin percentage (Section 10.3 + Section 11.2 size_cap)
        margin_pct = (size_cap_pct / 100.0) if size_cap_pct is not None else MARGIN_PCT
        margin_inr = portfolio_value * margin_pct

        # Combined margin cap enforcement — all-3-same-direction rule (Section 11.2)
        if combined_margin_cap_pct is not None:
            cap_inr       = portfolio_value * combined_margin_cap_pct / 100.0
            existing_inr  = self._get_open_positions_margin()
            available_inr = cap_inr - existing_inr
            if available_inr <= 0:
                raise ExecutionSkipped(
                    f'combined_margin_cap_exhausted: cap={combined_margin_cap_pct}% '
                    f'existing={existing_inr:.2f} available={available_inr:.2f}'
                )
            margin_inr = min(margin_inr, available_inr)

        # Leverage (Section 8.1 — Module 6 enforces 3x hard ceiling)
        leverage     = self._leverage
        notional_inr = margin_inr * leverage
        if notional_inr / margin_inr > MAX_LEVERAGE:
            raise ExecutionSkipped(
                f'leverage_exceeds_3x: computed={notional_inr / margin_inr:.2f}x'
            )

        entry_price   = mark_price
        contract_size = self._contract_sizes.get(symbol, _DEFAULT_CONTRACT_SIZES.get(symbol, 0.001))

        if entry_price <= 0 or contract_size <= 0 or USD_INR_RATE <= 0:
            raise ExecutionSkipped('invalid_price_or_contract_params')

        # size = notional_inr / (contract_size_usd × mark_price_usd × usd_inr_rate)
        raw_size       = notional_inr / (contract_size * entry_price * USD_INR_RATE)
        size_contracts = math.floor(raw_size * 1_000_000) / 1_000_000  # round down, 6 dp

        if size_contracts <= 0:
            raise ExecutionSkipped(
                f'size_contracts_zero_after_floor: notional={notional_inr:.2f} '
                f'raw={raw_size:.8f}'
            )

        # SL / TP prices (Section 8.1)
        sl_amount_inr = portfolio_value * SL_PCT
        tp_amount_inr = portfolio_value * TP_PCT
        denom         = size_contracts * contract_size * USD_INR_RATE  # INR per $1 move

        sl_dist = sl_amount_inr / denom
        tp_dist = tp_amount_inr / denom

        if direction == 'long':
            sl_price = entry_price - sl_dist
            tp_price = entry_price + tp_dist
        else:  # short
            sl_price = entry_price + sl_dist
            tp_price = entry_price - tp_dist

        # R:R sanity check (Section 8.1 — always 2.0 by design; catches any float error)
        rr = tp_dist / sl_dist if sl_dist > 0 else 0.0
        if rr < MIN_RR_RATIO - 1e-6:
            raise ExecutionSkipped(f'rr_below_minimum: rr={rr:.6f} min={MIN_RR_RATIO}')

        if direction == 'long' and sl_price >= entry_price:
            raise ExecutionSkipped('sl_not_below_entry_for_long')
        if direction == 'short' and sl_price <= entry_price:
            raise ExecutionSkipped('sl_not_above_entry_for_short')

        return margin_inr, notional_inr, size_contracts, entry_price, sl_price, tp_price

    # ── Paper execution ───────────────────────────────────────────────────────

    def _execute_paper(
        self,
        signal_id:     int,
        symbol:        str,
        direction:     str,
        size_contracts: float,
        entry_price:   float,
        sl_price:      float,
        tp_price:      float,
        margin_inr:    float,
        notional_value: float,
        now:           int,
    ) -> None:
        """Simulated immediate fill — no real orders placed (Section 18.3 pre-live)."""
        max_hold_until = now + MAX_HOLD_DAYS * 86400

        with get_connection() as conn:
            cur = conn.execute(
                """INSERT INTO trades
                   (signal_id, symbol, direction, entry_price, size_contracts,
                    notional_value, entry_timestamp, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'open')""",
                (signal_id, symbol, direction, entry_price, size_contracts,
                 notional_value, now),
            )
            trade_id = cur.lastrowid

            conn.execute(
                """INSERT INTO positions
                   (trade_id, symbol, direction, entry_price, current_price,
                    size_contracts, notional_value, margin_used, leverage,
                    stop_loss_price, take_profit_price, entry_timestamp, max_hold_until)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_id, symbol, direction, entry_price, entry_price,
                 size_contracts, notional_value, margin_inr, self._leverage,
                 sl_price, tp_price, now, max_hold_until),
            )

            conn.execute(
                "UPDATE signals SET status='executed' WHERE id=?", (signal_id,)
            )

        log_event(MODULE, 'info', 'paper_fill',
                  f'Paper fill: {symbol} {direction.upper()} '
                  f'{size_contracts:.6f} contracts @ {entry_price:.4f}',
                  {
                      'signal_id':      signal_id,
                      'trade_id':       trade_id,
                      'symbol':         symbol,
                      'direction':      direction,
                      'size_contracts': size_contracts,
                      'entry_price':    entry_price,
                      'sl_price':       round(sl_price, 6),
                      'tp_price':       round(tp_price, 6),
                      'margin_inr':     round(margin_inr, 2),
                      'notional_inr':   round(notional_value, 2),
                      'leverage':       self._leverage,
                      'paper':          True,
                  })
        log.info('Paper fill: trade=%d %s %s %.6f contracts @ %.4f',
                 trade_id, symbol, direction, size_contracts, entry_price)

    # ── Live execution ────────────────────────────────────────────────────────

    def _execute_live(
        self,
        signal_id:      int,
        symbol:         str,
        ccxt_sym:       str,
        direction:      str,
        size_contracts: float,
        entry_price:    float,
        sl_price:       float,
        tp_price:       float,
        margin_inr:     float,
        notional_value: float,
        now:            int,
    ) -> bool:
        """
        Place a real limit order on Delta Exchange.
        Returns True to halt further processing this cycle if both retries fail.
        """
        side  = 'buy' if direction == 'long' else 'sell'
        order = self._place_order_with_retry(ccxt_sym, side, size_contracts, entry_price)

        if order is None:
            log_event(MODULE, 'critical', 'execution_error',
                      f'API failed for {symbol} signal {signal_id} — halting orders this cycle',
                      {'signal_id': signal_id, 'symbol': symbol})
            Execution._update_signal(signal_id, 'rejected', 'api_timeout_both_retries')
            return True  # halt further orders this cycle

        order_id       = str(order['id'])
        max_hold_until = now + MAX_HOLD_DAYS * 86400

        with get_connection() as conn:
            cur = conn.execute(
                """INSERT INTO trades
                   (signal_id, symbol, direction, entry_price, size_contracts,
                    notional_value, entry_timestamp, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'open')""",
                (signal_id, symbol, direction, entry_price, size_contracts,
                 notional_value, now),
            )
            trade_id = cur.lastrowid

            conn.execute(
                """INSERT INTO positions
                   (trade_id, symbol, direction, entry_price, current_price,
                    size_contracts, notional_value, margin_used, leverage,
                    stop_loss_price, take_profit_price, entry_timestamp, max_hold_until)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_id, symbol, direction, entry_price, entry_price,
                 size_contracts, notional_value, margin_inr, self._leverage,
                 sl_price, tp_price, now, max_hold_until),
            )

            conn.execute(
                "UPDATE signals SET status='executed' WHERE id=?", (signal_id,)
            )

        log_event(MODULE, 'info', 'order_placed',
                  f'Limit order: {symbol} {direction.upper()} '
                  f'{size_contracts:.6f} contracts @ {entry_price:.4f}',
                  {
                      'signal_id':      signal_id,
                      'trade_id':       trade_id,
                      'order_id':       order_id,
                      'symbol':         symbol,
                      'ccxt_symbol':    ccxt_sym,
                      'direction':      direction,
                      'size_contracts': size_contracts,
                      'entry_price':    entry_price,
                      'sl_price':       round(sl_price, 6),
                      'tp_price':       round(tp_price, 6),
                      'margin_inr':     round(margin_inr, 2),
                      'notional_inr':   round(notional_value, 2),
                      'leverage':       self._leverage,
                  })

        self._schedule_fill_timeout(order_id, trade_id, ccxt_sym, symbol)

        log.info('Order placed: trade=%d order=%s %s %s %.6f contracts @ %.4f',
                 trade_id, order_id, symbol, direction, size_contracts, entry_price)
        return False

    # ── Order placement ───────────────────────────────────────────────────────

    def _place_order_with_retry(
        self,
        ccxt_sym: str,
        side:     str,
        amount:   float,
        price:    float,
    ) -> Optional[dict]:
        """
        Place a limit order via CCXT with one retry after RETRY_DELAY_SEC on
        NetworkError (Section 10.4). Returns order dict on success, None on failure.
        """
        for attempt in range(2):
            try:
                return self._exchange.create_order(
                    symbol=ccxt_sym,
                    type='limit',
                    side=side,
                    amount=amount,
                    price=price,
                )
            except ccxt.NetworkError as exc:
                if attempt == 0:
                    log.warning('API timeout attempt 1, retry in %ds: %s',
                                RETRY_DELAY_SEC, exc)
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    log.error('API timeout on second attempt for %s: %s', ccxt_sym, exc)
                    return None
            except ccxt.ExchangeError as exc:
                log.error('Exchange rejected order for %s: %s', ccxt_sym, exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Exchange rejected order for {ccxt_sym}: {exc}',
                          {'ccxt_symbol': ccxt_sym, 'side': side,
                           'amount': amount, 'price': price})
                return None
        return None

    # ── Fill timeout ──────────────────────────────────────────────────────────

    def _schedule_fill_timeout(
        self,
        order_id:  str,
        trade_id:  int,
        ccxt_sym:  str,
        delta_sym: str,
    ) -> None:
        """Schedule a DateTrigger job at now + FILL_TIMEOUT_SEC (4H)."""
        run_at = datetime.now(tz=timezone.utc) + timedelta(seconds=FILL_TIMEOUT_SEC)

        async def _timeout_wrapper(oid=order_id, tid=trade_id, csym=ccxt_sym, dsym=delta_sym):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._on_fill_timeout, oid, tid, csym, dsym)

        self._scheduler.add_job(
            _timeout_wrapper,
            DateTrigger(run_date=run_at),
            id=f'fill_timeout_{trade_id}',
            name=f'Fill timeout trade {trade_id}',
            max_instances=1,
        )
        log.debug('Fill timeout at %s for trade %d', run_at.isoformat(), trade_id)

    def _on_fill_timeout(
        self,
        order_id:  str,
        trade_id:  int,
        ccxt_sym:  str,
        delta_sym: str,
    ) -> None:
        """
        4H fill timeout callback.
        Fully filled -> confirm and leave position open.
        Partial or unfilled -> cancel remainder, mark trade 'cancelled', delete position.
        """
        if self._exchange is None:
            log.error('Exchange not initialised in _on_fill_timeout for trade %d', trade_id)
            return

        try:
            order = self._exchange.fetch_order(order_id, symbol=ccxt_sym)
        except Exception as exc:
            log.error('fetch_order failed order=%s trade=%d: %s', order_id, trade_id, exc)
            log_event(MODULE, 'error', 'execution_error',
                      f'fetch_order failed at 4H timeout for trade {trade_id}: {exc}',
                      {'order_id': order_id, 'trade_id': trade_id})
            return

        filled = float(order.get('filled') or 0)
        amount = float(order.get('amount') or 0)
        status = order.get('status', '')

        fully_filled = (status == 'closed' and amount > 0 and filled >= amount - 1e-9)

        if fully_filled:
            avg_price = order.get('average') or order.get('price')
            if avg_price:
                with get_connection() as conn:
                    conn.execute(
                        "UPDATE trades    SET entry_price=?              WHERE id=?",
                        (float(avg_price), trade_id),
                    )
                    conn.execute(
                        "UPDATE positions SET entry_price=?, current_price=? WHERE trade_id=?",
                        (float(avg_price), float(avg_price), trade_id),
                    )
            log_event(MODULE, 'info', 'order_filled',
                      f'Order {order_id} fully filled — trade {trade_id} is live',
                      {'order_id': order_id, 'trade_id': trade_id,
                       'delta_symbol': delta_sym, 'filled': filled, 'avg_price': avg_price})
            log.info('Order %s filled — trade %d active', order_id, trade_id)
        else:
            # Partial or unfilled — cancel remainder, clean up (Section 10.1 / 10.4)
            if status not in ('canceled', 'cancelled'):
                try:
                    self._exchange.cancel_order(order_id, symbol=ccxt_sym)
                    log.info('Cancelled order %s (%s fill=%.6f)', order_id, status, filled)
                except Exception as exc:
                    log.warning('cancel_order failed for %s: %s', order_id, exc)

            reason = 'partial_fill_at_timeout' if filled > 0 else 'unfilled_at_timeout'

            with get_connection() as conn:
                sig_row = conn.execute(
                    "SELECT signal_id FROM trades WHERE id=?", (trade_id,)
                ).fetchone()
                conn.execute(
                    "UPDATE trades SET status='cancelled', exit_reason=? WHERE id=?",
                    (reason, trade_id),
                )
                conn.execute(
                    "DELETE FROM positions WHERE trade_id=?", (trade_id,)
                )
                if sig_row and sig_row['signal_id']:
                    conn.execute(
                        "UPDATE signals SET status='expired', rejection_reason=? WHERE id=?",
                        (reason, sig_row['signal_id']),
                    )

            log_event(MODULE, 'warning', 'order_timeout',
                      f'Order {order_id} {reason}: trade {trade_id} cancelled',
                      {'order_id': order_id, 'trade_id': trade_id,
                       'delta_symbol': delta_sym, 'filled': filled,
                       'amount': amount, 'reason': reason})
            log.warning('Trade %d %s — order %s', trade_id, reason, order_id)

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _get_portfolio_value(self) -> float:
        """Latest total_value from portfolio_snapshots; fallback: KRONOS_PORTFOLIO_VALUE_INR."""
        try:
            with get_connection() as conn:
                row = conn.execute(
                    "SELECT total_value FROM portfolio_snapshots ORDER BY id DESC LIMIT 1"
                ).fetchone()
            if row:
                return float(row['total_value'])
        except Exception as exc:
            log.warning('portfolio_snapshots unavailable: %s', exc)
        return PORTFOLIO_FALLBACK_INR

    def _get_mark_price(self, delta_sym: str) -> Optional[float]:
        """Latest mark_price for symbol from orderbook_snapshots."""
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

    def _get_risk_data(self, signal_id: int) -> dict:
        """
        Read the most recent risk_check event for signal_id.
        Returns dict with size_cap_pct, combined_margin_cap_pct, etc.
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='risk_check'
                         AND json_extract(data, '$.signal_id') = ?
                       ORDER BY id DESC LIMIT 1""",
                    (signal_id,),
                ).fetchone()
            if row and row['data']:
                return json.loads(row['data'])
        except Exception as exc:
            log.warning('risk_data unavailable for signal %d: %s', signal_id, exc)
        return {}

    def _get_open_positions_margin(self) -> float:
        """Sum of margin_used across all open positions (for combined_margin_cap check)."""
        try:
            with get_connection() as conn:
                row = conn.execute(
                    "SELECT COALESCE(SUM(margin_used), 0.0) AS total FROM positions WHERE status='open'"
                ).fetchone()
            return float(row['total']) if row else 0.0
        except Exception as exc:
            log.warning('get_open_positions_margin error: %s', exc)
            return 0.0

    @staticmethod
    def _update_signal(signal_id: int, status: str, reason: Optional[str]) -> None:
        try:
            with get_connection() as conn:
                conn.execute(
                    "UPDATE signals SET status=?, rejection_reason=? WHERE id=?",
                    (status, reason, signal_id),
                )
        except Exception as exc:
            log.error('Failed to update signal %d to %s: %s', signal_id, status, exc)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    asyncio.run(Execution().start())


if __name__ == '__main__':
    main()
