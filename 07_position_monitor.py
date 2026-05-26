"""
Kronos Trading System — Module 7: Position Monitor
Sections 7.1, 8.1, 9.1, 9.4, 11.3, 11.5, 19.2 of the spec (v2.9).

Monitors all open positions every 15 minutes. Triggers an exit order when any
exit condition is met, updates the DB, and logs structured exit events.

Exit conditions (checked in priority order per position):
  1. Stop loss hit   — market close order; stop_loss_exit event for §19.2 4H blackout
  2. Take profit hit — limit close order at take_profit_price
  3. Time limit      — 5-day hard exit; market close order (§8.1)
  4. Funding cost    — rate against direction > 0.1% per 8H; market close (§9.1)

Also updates positions.current_price and positions.unrealised_pnl each cycle.

Schedule (UTC):
  Cron: every 15 minutes — 0,15,30,45 past each hour
"""

import asyncio
import json
import logging
import math
import os
import time
from typing import Optional

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import get_connection, init_db, log_event

log = logging.getLogger(__name__)
MODULE = 'position_monitor'

# ── Constants ──────────────────────────────────────────────────────────────────

DELTA_REST_BASE        = 'https://api.india.delta.exchange'
# Funding cost exit is disabled by default (KRONOS_FUNDING_EXIT_ENABLED=false).
# The funding gate has been moved to M5 entry-side (hard block at 0.5%/8H).
# Exiting an open position due to funding is counter-productive: it guarantees
# a loss on the trade without giving the 24H signal time to play out.
# Re-enable only if running on a very long horizon where funding accumulates.
FUNDING_EXIT_ENABLED   = os.environ.get('KRONOS_FUNDING_EXIT_ENABLED', 'false').lower() == 'true'
FUNDING_COST_THRESHOLD = 0.001       # 0.1% per 8H — only used if FUNDING_EXIT_ENABLED=true
USD_INR_RATE           = float(os.environ.get('KRONOS_USD_INR_RATE', '84.0'))
PAPER_MODE             = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'
STOP_LOSS_BLACKOUT_SEC = 4 * 3600   # §19.2 — no new entry on asset for 4H after SL hit
RETRY_DELAY_SEC        = 30          # §10.4 — retry once after 30s

# Delta symbol → CCXT symbol
ASSETS: dict[str, str] = {
    'BTCUSD': 'BTC/USD:USD',
    'ETHUSD': 'ETH/USD:USD',
    'SOLUSD': 'SOL/USD:USD',
    'BNBUSD': 'BNB/USD:USD',
    'XRPUSD': 'XRP/USD:USD',
}

_DEFAULT_CONTRACT_SIZES: dict[str, float] = {
    'BTCUSD': 0.001,
    'ETHUSD': 0.01,
    'SOLUSD': 1.0,
    'BNBUSD': 0.1,
    'XRPUSD': 10.0,
}

# Reverse lookup: CCXT symbol → Delta symbol (for notional recomputation)
_CCXT_TO_DELTA: dict[str, str] = {v: k for k, v in ASSETS.items()}


# ── Main class ─────────────────────────────────────────────────────────────────

class PositionMonitor:
    """
    Module 7 — Position Monitor.

    Polls open positions every 15 minutes. For each position, checks stop loss,
    take profit, 5-day time limit, and funding rate cost exit conditions.
    The first triggered condition closes the position and logs the exit event.
    """

    def __init__(self) -> None:
        self._contract_sizes: dict[str, float]           = dict(_DEFAULT_CONTRACT_SIZES)
        self._scheduler:      Optional[AsyncIOScheduler] = None
        self._exchange:       Optional[ccxt.Exchange]    = None

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
        self._scheduler = AsyncIOScheduler(timezone='UTC')
        self._scheduler.add_job(
            self._job_run,
            CronTrigger(minute='0,15,30,45', timezone='UTC'),
            id='position_monitor_cron',
            name='Position Monitor — 15-min check cycle',
            max_instances=1,
        )
        self._scheduler.start()
        log.info('Position Monitor started (paper=%s)', PAPER_MODE)
        await asyncio.Event().wait()

    # ── Cron entry point ──────────────────────────────────────────────────────

    async def _job_run(self) -> None:
        """Async scheduler wrapper — runs sync run() in thread executor."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.run)

    def run(self) -> None:
        """Called every 15 min. Checks all open positions and updates unrealised PnL."""
        log.debug('Position Monitor: checking open positions')
        try:
            self._check_all_positions()
        except Exception as exc:
            log.exception('Unhandled error in PositionMonitor.run(): %s', exc)
            log_event(MODULE, 'critical', 'execution_error',
                      f'Unhandled exception in run(): {exc}')

    # ── Core loop ─────────────────────────────────────────────────────────────

    def _check_all_positions(self) -> None:
        """
        Fetch all open positions, check each for exit conditions, update PnL.
        Only positions with status='open' are evaluated — 'closing' positions
        are already in the process of being closed.
        """
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT p.id, p.trade_id, p.symbol, p.direction,
                          p.entry_price, p.size_contracts,
                          p.stop_loss_price, p.take_profit_price,
                          p.margin_used, p.leverage,
                          p.entry_timestamp, p.max_hold_until,
                          p.unrealised_pnl
                   FROM positions p WHERE p.status='open'"""
            ).fetchall()

        if not rows:
            log.debug('No open positions to monitor')
            log_event(MODULE, 'info', 'heartbeat', 'No open positions this cycle')
            return

        for row in rows:
            pos = dict(row)
            try:
                self._check_position(pos)
            except Exception as exc:
                log.exception('Error checking position trade_id=%d: %s', pos['trade_id'], exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Error checking position trade_id={pos["trade_id"]}: {exc}',
                          {'trade_id': pos['trade_id'], 'symbol': pos['symbol']})

    def _check_position(self, pos: dict) -> None:
        """
        Evaluate all exit conditions for one open position.
        Exits on the first triggered condition (priority: SL > TP > time > funding).
        Also updates current_price and unrealised_pnl.
        """
        symbol     = pos['symbol']
        direction  = pos['direction']
        trade_id   = pos['trade_id']

        mark_price    = self._get_mark_price(symbol)
        funding_rate  = self._get_funding_rate(symbol, after_ts=pos['entry_timestamp'])

        # Update unrealised PnL even if no exit triggered
        if mark_price is not None:
            self._update_position_price(pos, mark_price)

        # 1. Stop loss (market order — Section 7.1)
        if mark_price is not None and self._is_stop_loss(pos, mark_price):
            log.info('Stop loss triggered: trade_id=%d %s %s mark=%.4f sl=%.4f',
                     trade_id, symbol, direction, mark_price, pos['stop_loss_price'])
            self._exit_position(pos, 'stop_loss', 'market', mark_price)
            return

        # 2. Take profit (limit order — Section 7.1)
        if mark_price is not None and self._is_take_profit(pos, mark_price):
            log.info('Take profit triggered: trade_id=%d %s %s mark=%.4f tp=%.4f',
                     trade_id, symbol, direction, mark_price, pos['take_profit_price'])
            self._exit_position(pos, 'take_profit', 'limit', mark_price)
            return

        # 3. Time limit (market order — Section 8.1)
        if self._is_time_limit(pos):
            log.info('Time limit triggered: trade_id=%d %s held past 5 days',
                     trade_id, symbol)
            if mark_price is None and PAPER_MODE:
                log.warning('mark_price unavailable for %s — using entry_price as exit_price '
                            'fallback; paper pnl_gross will be 0', symbol)
            mp = mark_price or pos['entry_price']
            self._exit_position(pos, 'time_limit', 'market', mp)
            return

        # 4. Funding rate cost (disabled by default — gate moved to M5 entry-side).
        # When KRONOS_FUNDING_EXIT_ENABLED=true, exits if the settled funding rate
        # against this direction exceeds FUNDING_COST_THRESHOLD since position entry.
        if FUNDING_EXIT_ENABLED and self._is_funding_cost_exit(pos, funding_rate):
            log.info('Funding cost exit triggered: trade_id=%d %s %s rate=%.6f',
                     trade_id, symbol, direction, funding_rate)
            if mark_price is None and PAPER_MODE:
                log.warning('mark_price unavailable for %s — using entry_price as exit_price '
                            'fallback; paper pnl_gross will be 0', symbol)
            mp = mark_price or pos['entry_price']
            self._exit_position(pos, 'funding_cost', 'market', mp)
            return

    # ── Exit condition checks ─────────────────────────────────────────────────

    @staticmethod
    def _is_stop_loss(pos: dict, mark_price: float) -> bool:
        if pos['direction'] == 'long':
            return mark_price <= pos['stop_loss_price']
        else:  # short
            return mark_price >= pos['stop_loss_price']

    @staticmethod
    def _is_take_profit(pos: dict, mark_price: float) -> bool:
        if pos['direction'] == 'long':
            return mark_price >= pos['take_profit_price']
        else:  # short
            return mark_price <= pos['take_profit_price']

    @staticmethod
    def _is_time_limit(pos: dict) -> bool:
        """True if position has been held past max_hold_until (5-day hard exit, §8.1)."""
        return int(time.time()) >= pos['max_hold_until']

    @staticmethod
    def _is_funding_cost_exit(pos: dict, funding_rate: Optional[float]) -> bool:
        """
        True if the settled funding rate is a net cost against this position
        exceeding the 0.1%/8H threshold (Section 9.1).
        Positive rate = longs pay shorts. Negative rate = shorts pay longs.
        """
        if funding_rate is None:
            return False
        if pos['direction'] == 'long' and funding_rate > FUNDING_COST_THRESHOLD:
            return True
        if pos['direction'] == 'short' and funding_rate < -FUNDING_COST_THRESHOLD:
            return True
        return False

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
        and log the exit event. For SL, also writes a stop_loss_exit event
        so Module 5 can enforce the §19.2 4H same-asset blackout.
        """
        trade_id  = pos['trade_id']
        pos_id    = pos['id']
        symbol    = pos['symbol']
        direction = pos['direction']
        size      = pos['size_contracts']

        # Determine the close order side and exit price
        close_side = 'sell' if direction == 'long' else 'buy'

        if exit_reason == 'take_profit':
            target_price = pos['take_profit_price']
        else:
            target_price = None  # market order — no limit price

        if PAPER_MODE:
            # For paper: TP uses exact target; others use mark_price (market approximation)
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
                log.error('Close order failed for trade %d (%s), exit deferred', trade_id, symbol)
                log_event(MODULE, 'error', 'execution_error',
                          f'Close order failed for trade {trade_id} ({symbol}) — exit deferred',
                          {'trade_id': trade_id, 'symbol': symbol, 'exit_reason': exit_reason})
                return

        exit_ts   = int(time.time())
        pnl_gross = self._compute_pnl_gross(pos, exit_price)

        with get_connection() as conn:
            conn.execute(
                """UPDATE trades
                   SET exit_price=?, exit_timestamp=?, exit_reason=?,
                       pnl_gross=?, status='closed'
                   WHERE id=?""",
                (exit_price, exit_ts, exit_reason, pnl_gross, trade_id),
            )
            conn.execute(
                "UPDATE positions SET status='closing' WHERE id=?",
                (pos_id,),
            )

        # Build full exit payload shared across all typed events.
        # Specific event_type per exit reason lets Module 10/11 filter without
        # parsing data JSON exit_reason.
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
        }

        if exit_reason == 'stop_loss':
            # §19.2: blackout_until added so Module 5 enforces 4H same-asset block.
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
                      f'{symbol} {direction.upper()} time limit exit @ {exit_price:.4f}, '
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
        Place a close order via CCXT. Returns the fill price or None on failure.
        Retries once after RETRY_DELAY_SEC on NetworkError (§10.4).
        On partial fill, updates positions.size_contracts so the next cycle
        closes only the remaining amount rather than repeating the full size.
        """
        for attempt in range(2):
            try:
                # reduceOnly ensures this order only closes the existing position
                # and cannot open a new one in the opposite direction (§10.1).
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

                # §10.1: cancel remainder immediately on partial fill.
                # Update positions.size_contracts to remaining so next 15-min
                # cycle closes the correct reduced size, not the original full size.
                if filled > 0 and remaining > 0:
                    order_id = order.get('id')
                    try:
                        self._exchange.cancel_order(order_id, ccxt_sym)
                        log.warning(
                            'Close order %s partial fill (%.6f/%.6f) — '
                            'remainder cancelled, position re-checked next cycle',
                            order_id, filled, amount,
                        )
                        log_event(MODULE, 'warning', 'execution_error',
                                  f'Partial close {order_id} on {ccxt_sym}: '
                                  f'{filled}/{amount} filled, remainder cancelled',
                                  {'order_id': order_id, 'filled': filled,
                                   'amount': amount, 'ccxt_symbol': ccxt_sym})
                    except Exception as cancel_exc:
                        log.error('Failed to cancel partial close %s: %s',
                                  order_id, cancel_exc)
                    if pos_id is not None:
                        new_size      = amount - filled
                        delta_sym     = _CCXT_TO_DELTA.get(ccxt_sym, '')
                        contract_size = self._contract_sizes.get(delta_sym, 1.0)
                        if mark_price_fallback is not None:
                            new_notional = new_size * contract_size * mark_price_fallback * USD_INR_RATE
                        else:
                            # No mark price available — scale existing notional proportionally
                            with get_connection() as conn:
                                nb_row = conn.execute(
                                    'SELECT notional_value FROM positions WHERE id=?',
                                    (pos_id,),
                                ).fetchone()
                            old_notional = float(nb_row['notional_value'] or 0.0) if nb_row else 0.0
                            new_notional = old_notional * (new_size / amount) if amount > 0 else 0.0
                        with get_connection() as conn:
                            conn.execute(
                                """UPDATE positions
                                   SET size_contracts=?, notional_value=?
                                   WHERE id=?""",
                                (new_size, new_notional, pos_id),
                            )
                    return None  # leave position open for next 15-min cycle

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
        """Update current_price and unrealised_pnl in the positions table."""
        pnl = self._compute_pnl_gross(pos, mark_price)
        try:
            with get_connection() as conn:
                conn.execute(
                    "UPDATE positions SET current_price=?, unrealised_pnl=? WHERE id=?",
                    (mark_price, pnl, pos['id']),
                )
        except Exception as exc:
            log.warning('Failed to update unrealised_pnl for position %d: %s', pos['id'], exc)

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _get_mark_price(self, delta_sym: str) -> Optional[float]:
        """Latest mark_price from orderbook_snapshots for the given Delta symbol."""
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
        """
        Most recent settled funding rate for the symbol (Section 9.4).

        after_ts (required for position checks): only returns a rate whose
        settlement timestamp is AFTER the position's entry_timestamp.  This
        prevents exiting a position based on a funding rate that was settled
        before the position was even opened — the exchange only charges funding
        to holders at the moment of each 8H settlement, so a pre-entry rate
        is irrelevant to the current position's cost.

        If after_ts is None (called without a position context) the most recent
        rate is returned regardless of timestamp.
        """
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
