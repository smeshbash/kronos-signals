"""
Kronos Trading System — Module 8: Portfolio Manager
Sections 8.2, 9.4, 11.1–11.5, 18.2 of the spec (v2.9).

Tracks portfolio value, peak, and drawdown. Fires Yellow/Orange/Red alert
events when drawdown thresholds are breached and executes the corresponding
protective actions. Writes 4H portfolio snapshots for downstream sizing
reference (Module 6 reads these). Computes monthly withdrawal figures on
the last calendar day.

Exit reason values written to trades.exit_reason: 'drawdown_alert'
Event types written: alert_yellow, alert_orange, alert_red, alert_cleared,
                     position_reduced, drawdown_alert_exit, withdrawal_calculation,
                     recuperation_milestone, rr_alert_7d, execution_error

Schedule (UTC):
  Cron: minute='3,18,33,48' — 3 min after Position Monitor (Module 7 at :00,:15,:30,:45)
  4H portfolio snapshot: written only on runs where hour in {0,4,8,12,16,20}
                         and minute==3, i.e., 00:03, 04:03, 08:03, 12:03, 16:03, 20:03 UTC
                         — fires before Module 5 (:07) and Module 6 (:09) each 4H cycle
  Monthly calculation: last calendar day, first cycle of the UTC day
"""

import asyncio
import calendar
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import get_connection, init_db, log_event
from tax_utils import incremental_tax, effective_reserve_rate, SECTION_87A_LIMIT

log = logging.getLogger(__name__)
MODULE = 'portfolio_manager'

# ── Constants ──────────────────────────────────────────────────────────────────

STARTING_CAPITAL_INR      = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', '100000.0'))
USD_INR_RATE              = float(os.environ.get('KRONOS_USD_INR_RATE', '84.0'))

# Each model has its own STARTING_CAPITAL_INR pool (Rs 1L each).
# Aggregate portfolio base = STARTING_CAPITAL_INR × len(_MODELS) = Rs 5L total.
# All 5 models active from regime v3 — v3 is the primary benchmark dataset.
_MODELS: tuple[str, ...] = ('custom', 'kronos-mini', 'kronos-base',
                             'kronos-mini-4h', 'kronos-base-4h')
PAPER_MODE                = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'
PHASE                     = os.environ.get('KRONOS_PHASE', 'pre_live')
MONTHLY_FIXED_COSTS_INR   = float(os.environ.get('KRONOS_MONTHLY_FIXED_COSTS_INR', '915.0'))
DELTA_REST_BASE           = 'https://api.india.delta.exchange'
RETRY_DELAY_SEC           = 30          # §10.4 — single retry after 30s

# §8.2 drawdown thresholds (percentage points, not fractions)
YELLOW_ALERT_PCT = 5.0
ORANGE_ALERT_PCT = 10.0
RED_ALERT_PCT    = 15.0

# Alert level ordering for escalation / de-escalation comparisons
_LEVEL_ORDER: dict[str, int] = {'green': 0, 'yellow': 1, 'orange': 2, 'red': 3}

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


# ── Main class ─────────────────────────────────────────────────────────────────

class PortfolioManager:
    """
    Module 8 — Portfolio Manager.

    Runs every 15 minutes. Computes current portfolio value and drawdown from
    peak. Fires alert events and executes protective position actions when
    drawdown crosses Yellow (5%), Orange (10%), or Red (15%) thresholds.
    De-escalates and writes alert_cleared when drawdown recovers below the
    current threshold. Writes 4H snapshots to portfolio_snapshots for Module 6.
    Runs monthly P&L allocation on the last day of each calendar month.
    """

    def __init__(self) -> None:
        self._exchange:            Optional[ccxt.Exchange]    = None
        self._scheduler:           Optional[AsyncIOScheduler] = None
        self._current_alert_level: str                        = 'green'
        self._contract_sizes:      dict[str, float]           = dict(_DEFAULT_CONTRACT_SIZES)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _build_exchange(self) -> ccxt.Exchange:
        ex = ccxt.delta({
            'apiKey':          os.environ.get('KRONOS_API_KEY', ''),
            'secret':          os.environ.get('KRONOS_API_SECRET', ''),
            'enableRateLimit': True,
        })
        ex.set_sandbox_mode(False)
        ex.urls['api']['public']  = DELTA_REST_BASE
        ex.urls['api']['private'] = DELTA_REST_BASE
        return ex

    def start(self) -> None:
        init_db()
        if not PAPER_MODE:
            self._exchange = self._build_exchange()
            try:
                markets = self._exchange.load_markets()
                for sym, ccxt_sym in ASSETS.items():
                    mkt = markets.get(ccxt_sym, {})
                    cs = mkt.get('contractSize') or _DEFAULT_CONTRACT_SIZES.get(sym)
                    if cs:
                        self._contract_sizes[sym] = float(cs)
            except Exception as exc:
                log.warning('Failed to load markets on start: %s — using defaults', exc)

        self._current_alert_level = self._get_current_alert_level()
        log.info('Portfolio Manager started. Current alert level: %s',
                 self._current_alert_level)

        self._scheduler = AsyncIOScheduler(timezone='UTC')
        self._scheduler.add_job(
            self._job_cycle,
            CronTrigger(minute='3,18,33,48', timezone='UTC'),
            id='portfolio_cycle',
            name='Portfolio Manager 15-min cycle',
            max_instances=1,
            misfire_grace_time=60,
        )
        self._scheduler.start()
        log.info('Portfolio Manager scheduler running.')

    def stop(self) -> None:
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    # ── Portfolio value ────────────────────────────────────────────────────────

    def _compute_portfolio_value(self) -> tuple[float, float, float, int]:
        """
        Returns (total_value_inr, active_margin_inr, unrealised_pnl_inr, open_count)
        for the AGGREGATE portfolio (sum of all three model pools).

        total_value = len(_MODELS) × STARTING_CAPITAL + sum(clean closed pnl_gross)
                    + sum(open unrealised_pnl)

        Funding accumulation is not included — Module 9's responsibility.
        """
        with get_connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(COALESCE(pnl_net, pnl_gross)), 0.0) AS total
                   FROM trades
                   WHERE status='closed' AND quality_flag IS NULL"""
            ).fetchone()
            # Use pnl_net (after TDS + fees + funding_paid) when M9 has processed
            # the trade; fall back to pnl_gross for trades closed in the last minute
            # that M9 has not yet processed (pnl_net IS NULL).
            closed_pnl: float = float(row['total']) if row else 0.0

            row2 = conn.execute(
                """SELECT COALESCE(SUM(unrealised_pnl), 0.0) AS upnl,
                          COALESCE(SUM(margin_used),    0.0) AS margin,
                          COUNT(*) AS cnt
                   FROM positions WHERE status='open'"""
            ).fetchone()
            unrealised_pnl: float = float(row2['upnl'])   if row2 else 0.0
            active_margin:  float = float(row2['margin'])  if row2 else 0.0
            open_count:     int   = int(row2['cnt'])        if row2 else 0

        # Each model has its own ₹1L pool — aggregate base = STARTING_CAPITAL × 3
        total_value = len(_MODELS) * STARTING_CAPITAL_INR + closed_pnl + unrealised_pnl
        return total_value, active_margin, unrealised_pnl, open_count

    def _compute_portfolio_value_for_model(
        self, model_source: str
    ) -> tuple[float, float, float, int]:
        """
        Returns (total_value_inr, active_margin_inr, unrealised_pnl_inr, open_count)
        for a single model's ₹1L capital pool.

        Traces trades and positions back to their originating signal via the
        signal_id → signals.model_source JOIN chain — no schema changes to
        trades or positions are required.
        """
        with get_connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(COALESCE(t.pnl_net, t.pnl_gross)), 0.0) AS total
                   FROM trades t
                   JOIN signals s ON t.signal_id = s.id
                   WHERE t.status        = 'closed'
                     AND t.quality_flag IS NULL
                     AND s.model_source  = ?""",
                (model_source,),
            ).fetchone()
            # pnl_net preferred (TDS + fees + funding_paid already deducted by M9);
            # falls back to pnl_gross for trades M9 has not yet processed.
            closed_pnl: float = float(row['total']) if row else 0.0

            row2 = conn.execute(
                """SELECT COALESCE(SUM(p.unrealised_pnl), 0.0) AS upnl,
                          COALESCE(SUM(p.margin_used),    0.0) AS margin,
                          COUNT(*)                              AS cnt
                   FROM positions p
                   JOIN trades t  ON p.trade_id  = t.id
                   JOIN signals s ON t.signal_id = s.id
                   WHERE p.status       = 'open'
                     AND s.model_source = ?""",
                (model_source,),
            ).fetchone()
            unrealised_pnl: float = float(row2['upnl'])  if row2 else 0.0
            active_margin:  float = float(row2['margin']) if row2 else 0.0
            open_count:     int   = int(row2['cnt'])       if row2 else 0

        total_value = STARTING_CAPITAL_INR + closed_pnl + unrealised_pnl
        return total_value, active_margin, unrealised_pnl, open_count

    def _get_peak_value(self, model_source: Optional[str] = None) -> float:
        """
        Read the highest peak_value ever stored in portfolio_snapshots.

        model_source=None  → aggregate peak; default = len(_MODELS) × STARTING_CAPITAL_INR.
        model_source='...' → per-model peak; default = STARTING_CAPITAL_INR.

        Stale-snapshot guard: aggregate snapshots written before per-model tracking
        was introduced have peak_value ≈ ₹1L (single-model era). Any aggregate peak
        below 1.5 × STARTING_CAPITAL_INR is treated as stale and ignored so the
        drawdown baseline resets correctly to ₹3L from the first new snapshot.
        """
        fallback = (STARTING_CAPITAL_INR if model_source
                    else len(_MODELS) * STARTING_CAPITAL_INR)
        try:
            with get_connection() as conn:
                if model_source is None:
                    row = conn.execute(
                        'SELECT COALESCE(MAX(peak_value), 0) AS peak'
                        ' FROM portfolio_snapshots WHERE model_source IS NULL'
                    ).fetchone()
                else:
                    row = conn.execute(
                        'SELECT COALESCE(MAX(peak_value), 0) AS peak'
                        ' FROM portfolio_snapshots WHERE model_source = ?',
                        (model_source,),
                    ).fetchone()
            peak = float(row['peak']) if row and row['peak'] else 0.0
            # Stale-snapshot guard for aggregate: pre-per-model rows had ≈₹1L peak
            if model_source is None and 0 < peak < 1.5 * STARTING_CAPITAL_INR:
                return fallback
            return peak if peak > 0 else fallback
        except Exception:
            return fallback

    def _estimate_accumulated_funding_inr(self) -> float:
        """
        Best-effort estimate of net accumulated funding for all open positions.
        Positive = net received (benefit to portfolio); negative = net paid (cost).

        Uses the most recent funding rate per symbol as a proxy for the average
        rate over all 8H periods since entry. Precise per-period accounting is
        Module 9's responsibility. Called for informational purposes only — the
        primary portfolio value formula does not depend on this estimate.
        """
        total = 0.0
        with get_connection() as conn:
            positions = conn.execute(
                "SELECT symbol, direction, size_contracts, entry_timestamp FROM positions WHERE status='open'"
            ).fetchall()
            now_ts = int(time.time())
            for pos in positions:
                sym   = pos['symbol']
                cs    = self._contract_sizes.get(sym, 1.0)
                size  = float(pos['size_contracts'])
                entry = int(pos['entry_timestamp'])
                periods = max(0.0, (now_ts - entry) / (8 * 3600))

                fr_row = conn.execute(
                    "SELECT rate FROM funding_rates WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
                    (sym,)
                ).fetchone()
                if not fr_row:
                    continue
                rate = float(fr_row['rate'])

                mp_row = conn.execute(
                    "SELECT mark_price FROM orderbook_snapshots WHERE symbol=? ORDER BY id DESC LIMIT 1",
                    (sym,)
                ).fetchone()
                mark = float(mp_row['mark_price']) if mp_row and mp_row['mark_price'] else 0.0
                if mark <= 0:
                    continue

                notional_usd = size * cs * mark
                funding_usd  = notional_usd * rate * periods
                # Long: positive rate = cost (paid), negative rate = revenue (received)
                # Short: positive rate = revenue, negative rate = cost
                if pos['direction'] == 'long':
                    total -= funding_usd * USD_INR_RATE
                else:
                    total += funding_usd * USD_INR_RATE
        return total

    # ── Alert level ────────────────────────────────────────────────────────────

    def _get_current_alert_level(self) -> str:
        """
        Read the current alert level from the most recent alert event in the DB.
        Returns 'green' if no alert events exist or the most recent was alert_cleared.
        """
        with get_connection() as conn:
            row = conn.execute(
                """SELECT event_type FROM events
                   WHERE event_type IN ('alert_yellow','alert_orange','alert_red','alert_cleared')
                   ORDER BY id DESC LIMIT 1"""
            ).fetchone()
        if not row:
            return 'green'
        et = row['event_type']
        if et == 'alert_cleared':
            return 'green'
        return et.replace('alert_', '')  # 'yellow' | 'orange' | 'red'

    def _level_from_drawdown(self, drawdown_pct: float) -> str:
        """Map a drawdown percentage to the corresponding alert level."""
        if drawdown_pct >= RED_ALERT_PCT:
            return 'red'
        if drawdown_pct >= ORANGE_ALERT_PCT:
            return 'orange'
        if drawdown_pct >= YELLOW_ALERT_PCT:
            return 'yellow'
        return 'green'

    # ── Position helpers ───────────────────────────────────────────────────────

    def _get_open_positions(self) -> list[dict]:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM positions WHERE status='open'"
            ).fetchall()
        return [dict(r) for r in rows]

    def _get_slot3_symbol(self) -> Optional[str]:
        with get_connection() as conn:
            row = conn.execute(
                """SELECT data FROM events WHERE event_type='slot3_selection'
                   ORDER BY id DESC LIMIT 1"""
            ).fetchone()
        if not row or not row['data']:
            return None
        try:
            return json.loads(row['data']).get('slot3_symbol')
        except Exception:
            return None

    def _get_mark_price(self, symbol: str, fallback: float) -> float:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT mark_price FROM orderbook_snapshots WHERE symbol=? ORDER BY id DESC LIMIT 1",
                (symbol,)
            ).fetchone()
        mp = float(row['mark_price']) if row and row['mark_price'] else None
        if mp is None and PAPER_MODE:
            log.warning('mark_price unavailable for %s — using entry_price as exit_price '
                        'fallback; paper pnl_gross will be 0', symbol)
            return fallback
        return mp if mp is not None else fallback

    # ── Yellow Alert: 50% position size reduction ─────────────────────────────

    def _reduce_position_by_half(self, pos: dict) -> bool:
        """
        Close 50% of a position via market order (Yellow Alert §8.2).
        In paper mode, directly halves the DB row. Returns True on success.
        """
        symbol    = pos['symbol']
        trade_id  = int(pos['trade_id'])
        size      = float(pos['size_contracts'])
        direction = pos['direction']
        side      = 'sell' if direction == 'long' else 'buy'
        ccxt_sym  = ASSETS.get(symbol)
        if not ccxt_sym:
            log.error('Unknown symbol %s — cannot reduce position', symbol)
            return False

        close_size = size * 0.5

        if PAPER_MODE:
            with get_connection() as conn:
                conn.execute(
                    """UPDATE positions
                       SET size_contracts = size_contracts * 0.5,
                           notional_value = CASE WHEN notional_value IS NOT NULL
                                            THEN notional_value * 0.5 ELSE NULL END,
                           margin_used    = CASE WHEN margin_used IS NOT NULL
                                            THEN margin_used * 0.5 ELSE NULL END
                       WHERE trade_id=? AND status='open'""",
                    (trade_id,)
                )
            log_event(MODULE, 'info', 'position_reduced',
                      f'Yellow Alert: {symbol} reduced 50% (paper)',
                      {'trade_id': trade_id, 'symbol': symbol,
                       'original_size': size, 'new_size': close_size, 'paper': True})
            return True

        # Live mode — single retry on NetworkError (§10.4)
        for attempt in range(2):
            try:
                order = self._exchange.create_order(
                    symbol=ccxt_sym, type='market', side=side,
                    amount=close_size, params={'reduceOnly': True},
                )
                filled = float(order.get('filled') or 0)
                if filled > 0:
                    with get_connection() as conn:
                        conn.execute(
                            """UPDATE positions
                               SET size_contracts = size_contracts - ?,
                                   notional_value = CASE WHEN notional_value IS NOT NULL
                                                    THEN notional_value * 0.5 ELSE NULL END,
                                   margin_used    = CASE WHEN margin_used IS NOT NULL
                                                    THEN margin_used * 0.5 ELSE NULL END
                               WHERE trade_id=? AND status='open'""",
                            (filled, trade_id)
                        )
                    log_event(MODULE, 'info', 'position_reduced',
                              f'Yellow Alert: {symbol} reduced 50%',
                              {'trade_id': trade_id, 'symbol': symbol,
                               'original_size': size, 'filled': filled,
                               'order_id': order.get('id'), 'paper': False})
                    return True
                log.warning('Reduce order for %s filled=0 (attempt %d)', symbol, attempt + 1)
                break  # do not retry a filled=0 order — prevents double position reduction
            except ccxt.NetworkError as exc:
                if attempt == 0:
                    log.warning('NetworkError reducing %s, retrying after %ds: %s',
                                symbol, RETRY_DELAY_SEC, exc)
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    log.error('Reduce position %s failed after retry: %s', symbol, exc)
                    log_event(MODULE, 'error', 'execution_error',
                              f'Yellow Alert reduce failed for {symbol}: {exc}',
                              {'trade_id': trade_id, 'symbol': symbol})
                    return False
            except Exception as exc:
                log.error('Reduce position %s error: %s', symbol, exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Yellow Alert reduce error for {symbol}: {exc}',
                          {'trade_id': trade_id, 'symbol': symbol})
                return False
        return False

    # ── Orange / Red Alert: full position close ────────────────────────────────

    def _close_position_drawdown(self, pos: dict) -> bool:
        """
        Close a full position at market due to drawdown alert (Orange/Red §8.2).
        Sets trade.status='closed', position.status='closing', writes trades P&L.
        Returns True on success.
        """
        symbol      = pos['symbol']
        trade_id    = int(pos['trade_id'])
        size        = float(pos['size_contracts'])
        direction   = pos['direction']
        side        = 'sell' if direction == 'long' else 'buy'
        entry_price = float(pos['entry_price'])
        cs          = self._contract_sizes.get(symbol, 1.0)
        ccxt_sym    = ASSETS.get(symbol)
        if not ccxt_sym:
            log.error('Unknown symbol %s — cannot close position', symbol)
            return False

        exit_ts    = int(time.time())
        exit_price = self._get_mark_price(symbol, entry_price)

        def _pnl(ep: float) -> float:
            if direction == 'long':
                return round((ep - entry_price) * size * cs * USD_INR_RATE, 2)
            return round((entry_price - ep) * size * cs * USD_INR_RATE, 2)

        if PAPER_MODE:
            pnl_gross = _pnl(exit_price)
            with get_connection() as conn:
                conn.execute(
                    """UPDATE trades SET status='closed', exit_price=?,
                       exit_timestamp=?, exit_reason='drawdown_alert', pnl_gross=?
                       WHERE id=?""",
                    (exit_price, exit_ts, pnl_gross, trade_id)
                )
                conn.execute(
                    "UPDATE positions SET status='closing' WHERE trade_id=?",
                    (trade_id,)
                )
            log_event(MODULE, 'warning', 'drawdown_alert_exit',
                      f'{symbol} closed by drawdown alert (paper)',
                      {'trade_id': trade_id, 'symbol': symbol, 'direction': direction,
                       'exit_price': exit_price, 'entry_price': entry_price,
                       'pnl_gross': pnl_gross, 'exit_timestamp': exit_ts, 'paper': True})
            return True

        # Live mode — single retry
        for attempt in range(2):
            try:
                order = self._exchange.create_order(
                    symbol=ccxt_sym, type='market', side=side,
                    amount=size, params={'reduceOnly': True},
                )
                exit_price = float(order.get('average') or order.get('price') or exit_price)
                break
            except ccxt.NetworkError as exc:
                if attempt == 0:
                    log.warning('NetworkError closing %s, retrying: %s', symbol, exc)
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    log.error('Close position %s failed after retry: %s', symbol, exc)
                    log_event(MODULE, 'error', 'execution_error',
                              f'Drawdown close failed for {symbol}: {exc}',
                              {'trade_id': trade_id, 'symbol': symbol})
                    return False
            except Exception as exc:
                log.error('Close position %s error: %s', symbol, exc)
                log_event(MODULE, 'error', 'execution_error',
                          f'Drawdown close error for {symbol}: {exc}',
                          {'trade_id': trade_id, 'symbol': symbol})
                return False

        pnl_gross = _pnl(exit_price)
        with get_connection() as conn:
            conn.execute(
                """UPDATE trades SET status='closed', exit_price=?,
                   exit_timestamp=?, exit_reason='drawdown_alert', pnl_gross=?
                   WHERE id=?""",
                (exit_price, exit_ts, pnl_gross, trade_id)
            )
            conn.execute(
                "UPDATE positions SET status='closing' WHERE trade_id=?",
                (trade_id,)
            )
        log_event(MODULE, 'warning', 'drawdown_alert_exit',
                  f'{symbol} closed by drawdown alert',
                  {'trade_id': trade_id, 'symbol': symbol, 'direction': direction,
                   'exit_price': exit_price, 'entry_price': entry_price,
                   'pnl_gross': pnl_gross, 'exit_timestamp': exit_ts, 'paper': False})
        return True

    # ── Alert firing ──────────────────────────────────────────────────────────

    def _fire_alert(self, level: str, positions: list[dict],
                    drawdown_pct: float, portfolio_value: float) -> None:
        """Write alert event and execute the corresponding protective actions."""
        event_type = f'alert_{level}'
        log_event(MODULE, 'warning', event_type,
                  f'{level.upper()} Alert: {drawdown_pct:.2f}% drawdown from peak',
                  {'drawdown_pct': round(drawdown_pct, 4),
                   'portfolio_value': round(portfolio_value, 2),
                   'alert_level': level, 'open_positions': len(positions)})
        log.warning('ALERT %s fired — drawdown=%.2f%%', level.upper(), drawdown_pct)

        if level == 'yellow':
            # §8.2: Reduce all open positions by 50%. No new Slot 3 entries
            # (Slot 3 block enforced by Module 5 reading the alert_yellow event).
            for pos in positions:
                self._reduce_position_by_half(pos)

        elif level == 'orange':
            # §8.2: Close Slot 3 position. No new entries (enforced by Module 5).
            slot3_sym = self._get_slot3_symbol()
            if slot3_sym:
                for pos in positions:
                    if pos['symbol'] == slot3_sym:
                        self._close_position_drawdown(pos)
                        break
            else:
                log.info('Orange Alert: no active Slot 3 position found')

        elif level == 'red':
            # §8.2: Close ALL positions at market. Write forced_override.
            for pos in positions:
                self._close_position_drawdown(pos)
            log_event(MODULE, 'critical', 'forced_override',
                      'Red Alert (15% drawdown): all positions closed — system halted '
                      'pending mandatory human review (§19.1)',
                      {'drawdown_pct': round(drawdown_pct, 4),
                       'portfolio_value': round(portfolio_value, 2),
                       'alert_level': 'red'})
            log.critical('RED ALERT — forced_override written. Awaiting human review.')

    def _fire_alert_cleared(self, recovered_level: str, drawdown_pct: float,
                             portfolio_value: float) -> None:
        log_event(MODULE, 'info', 'alert_cleared',
                  f'Alert cleared — drawdown recovered to {drawdown_pct:.2f}%',
                  {'drawdown_pct': round(drawdown_pct, 4),
                   'portfolio_value': round(portfolio_value, 2),
                   'recovered_to': recovered_level})
        log.info('Alert cleared — drawdown %.2f%%, level now %s',
                 drawdown_pct, recovered_level)

    def _write_level_event_only(self, level: str, drawdown_pct: float,
                                portfolio_value: float) -> None:
        """
        Write alert_{level} event for a partial de-escalation (e.g. Red→Orange,
        Orange→Yellow). Does NOT execute protective actions — those fired when the
        level was first reached. Keeps the event stream coherent so Module 5 reads
        the current enforcement level without re-triggering actions.
        """
        event_type = f'alert_{level}'
        log_event(MODULE, 'info', event_type,
                  f'Drawdown de-escalated to {level.upper()} — {drawdown_pct:.2f}% from peak',
                  {'drawdown_pct': round(drawdown_pct, 4),
                   'portfolio_value': round(portfolio_value, 2),
                   'alert_level': level, 'de_escalation': True})
        log.info('De-escalated to %s — drawdown %.2f%%', level.upper(), drawdown_pct)

    # ── Portfolio snapshot ─────────────────────────────────────────────────────

    def _should_write_4h_snapshot(self) -> bool:
        """True on runs at 4H boundaries: hour ∈ {0,4,8,12,16,20}, minute==3 UTC."""
        now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        return now_utc.hour % 4 == 0 and now_utc.minute == 3

    def _write_portfolio_snapshot(
        self, total_value: float, active_margin: float, unrealised_pnl: float,
        open_positions: int, drawdown_pct: float, peak_value: float,
        model_source: Optional[str] = None,
    ) -> None:
        """
        Write one portfolio snapshot row.
        model_source=None  → aggregate snapshot.
        model_source='...' → per-model snapshot (one of _MODELS).
        """
        now = int(time.time())
        available_margin = total_value - active_margin
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO portfolio_snapshots
                   (timestamp, total_value, active_margin, available_margin,
                    unrealised_pnl, drawdown_pct, peak_value, open_positions,
                    phase, model_source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now,
                 round(total_value, 2), round(active_margin, 2),
                 round(available_margin, 2), round(unrealised_pnl, 2),
                 round(drawdown_pct, 4), round(peak_value, 2),
                 open_positions, PHASE, model_source)
            )
        tag = model_source or 'aggregate'
        log.info('Portfolio snapshot [%s]: value=₹%.2f peak=₹%.2f dd=%.2f%% positions=%d',
                 tag, total_value, peak_value, drawdown_pct, open_positions)

    def _get_last_snapshot_value(self) -> float:
        """
        Return total_value from the most recent AGGREGATE portfolio snapshot
        (model_source IS NULL). Returns 0.0 when none exists, disabling the
        10%-change extra-snapshot trigger until the first aggregate snapshot is written.
        """
        with get_connection() as conn:
            row = conn.execute(
                'SELECT total_value FROM portfolio_snapshots'
                ' WHERE model_source IS NULL ORDER BY id DESC LIMIT 1'
            ).fetchone()
        return float(row['total_value']) if row and row['total_value'] else 0.0

    # ── Monthly withdrawal calculation ─────────────────────────────────────────

    def _is_last_day_of_month(self) -> bool:
        now_utc  = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        last_day = calendar.monthrange(now_utc.year, now_utc.month)[1]
        return now_utc.day == last_day

    def _monthly_calc_already_run_today(self) -> bool:
        now_utc   = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        day_start = int(datetime(now_utc.year, now_utc.month, now_utc.day,
                                 tzinfo=timezone.utc).timestamp())
        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM events WHERE event_type='withdrawal_calculation' AND timestamp>=? LIMIT 1",
                (day_start,)
            ).fetchone()
        return row is not None

    def _get_cumulative_withdrawals(self) -> float:
        """
        Sum all withdrawal_made events to compute cumulative human withdrawals.
        withdrawal_made events are written by the human withdrawal confirmation
        flow (Module 10 / manual admin) — not by Module 8 itself.
        """
        with get_connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(CAST(json_extract(data,'$.amount') AS REAL)), 0.0) AS total
                   FROM events WHERE event_type='withdrawal_made'"""
            ).fetchone()
        return float(row['total']) if row else 0.0

    def _run_monthly_calculation(self, now_utc: datetime,
                                 accumulated_funding: float = 0.0) -> None:
        """
        Compute monthly P&L allocation per §11.4. Writes withdrawal_calculation
        event — the human initiates the actual withdrawal (§11.4: human-initiated
        ONLY, never automated). Also writes recuperation_milestone events on
        50% and 100% crossovers (§11.4 / §11.5). Writes tax_reserve credit row.

        Survival benchmark uses COALESCE(pnl_net, pnl_gross) so settled TDS is
        accounted for when available; allocation formula always uses gross (spec:
        "30% of gross"). accumulated_funding is informational only — funding is
        Module 9's domain.
        """
        month_start = int(datetime(now_utc.year, now_utc.month, 1,
                                   tzinfo=timezone.utc).timestamp())
        month_end   = int(time.time())

        with get_connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(pnl_gross), 0.0)                    AS gross,
                          COALESCE(SUM(COALESCE(pnl_net, pnl_gross)), 0.0) AS net_or_gross
                   FROM trades
                   WHERE status          = 'closed'
                     AND quality_flag    IS NULL
                     AND exit_timestamp >= ? AND exit_timestamp <= ?""",
                (month_start, month_end)
            ).fetchone()
        gross_pnl     = float(row['gross'])       if row else 0.0
        benchmark_pnl = float(row['net_or_gross']) if row else 0.0

        # §11.4 survival benchmark: pnl_net (settled) preferred over gross
        net_before_tax = benchmark_pnl - MONTHLY_FIXED_COSTS_INR
        month_label    = f'{now_utc.year}-{now_utc.month:02d}'

        if net_before_tax <= 0:
            log_event(MODULE, 'warning', 'withdrawal_calculation',
                      f'Month {month_label}: P&L below survival benchmark — no withdrawal',
                      {'month': month_label,
                       'gross_pnl': round(gross_pnl, 2),
                       'benchmark_pnl': round(benchmark_pnl, 2),
                       'fixed_costs': MONTHLY_FIXED_COSTS_INR,
                       'net_before_tax': round(net_before_tax, 2),
                       'accumulated_funding_est': round(accumulated_funding, 2),
                       'withdrawal_eligible': False})
            log.info('Monthly calc %s: no withdrawal (benchmark=₹%.2f below fixed costs)',
                     month_label, benchmark_pnl)
            return

        # §11.4 Dynamic tax reserve — based on actual Indian slab rate, not flat 30%.
        # Futures/options: speculative income, no TDS. Tax = 0% until total income
        # (base + YTD trading profit) exceeds Rs 12L Section 87A rebate cliff.
        # Reserve is computed as incremental tax on this month's profit given YTD context.
        base_income = float(os.environ.get('KRONOS_BASE_INCOME_INR', '0'))
        fy_start_ts = int(datetime(
            now_utc.year if now_utc.month >= 4 else now_utc.year - 1,
            4, 1, tzinfo=timezone.utc
        ).timestamp())
        with get_connection() as conn:
            ytd_rows = conn.execute(
                """SELECT COALESCE(SUM(pnl_gross + funding_received - funding_paid), 0.0)
                          AS ytd_taxable
                   FROM trades
                   WHERE status='closed' AND pnl_net IS NOT NULL
                     AND exit_timestamp >= ? AND exit_timestamp <= ?""",
                (fy_start_ts, month_end)
            ).fetchone()
        ytd_taxable    = float(ytd_rows['ytd_taxable'])
        ytd_before     = ytd_taxable - max(0.0, gross_pnl)  # YTD before this month
        tax_reserve_credit = incremental_tax(ytd_before, ytd_taxable, base_income)
        reserve_rate       = effective_reserve_rate(ytd_taxable, base_income)

        # §11.4 allocation formula
        net_profit       = round(gross_pnl - tax_reserve_credit, 2)
        system_retention = round(net_profit * 0.20, 2)   # 20% of net
        human_withdrawal = round(net_profit * 0.80, 2)   # 80% of net

        # Write credit to tax_reserve table (§11.4 — earmarked for ITR, non-tradeable)
        with get_connection() as conn:
            bal_row = conn.execute(
                'SELECT balance_after FROM tax_reserve ORDER BY id DESC LIMIT 1'
            ).fetchone()
            prev_balance = float(bal_row['balance_after']) if bal_row else 0.0
            new_balance  = round(prev_balance + tax_reserve_credit, 2)
            conn.execute(
                """INSERT INTO tax_reserve
                   (transaction_type, amount, balance_after, reference_trade_id, notes, timestamp)
                   VALUES ('reserve', ?, ?, NULL, ?, ?)""",
                (tax_reserve_credit, new_balance,
                 f'Monthly reserve credit — {month_label} '
                 f'(rate {reserve_rate:.1%}, base_income Rs{base_income:.0f})',
                 int(time.time()))
            )

        cumulative_withdrawn   = self._get_cumulative_withdrawals()
        recuperation_pct       = round(cumulative_withdrawn / STARTING_CAPITAL_INR * 100, 2)
        recuperation_remaining = round(max(0.0, STARTING_CAPITAL_INR - cumulative_withdrawn), 2)

        log_event(MODULE, 'info', 'withdrawal_calculation',
                  f'Month {month_label}: ₹{human_withdrawal:.2f} available for withdrawal',
                  {'month':                    month_label,
                   'gross_pnl':                round(gross_pnl, 2),
                   'benchmark_pnl':            round(benchmark_pnl, 2),
                   'fixed_costs':              MONTHLY_FIXED_COSTS_INR,
                   'tax_reserve_credit':       tax_reserve_credit,
                   'tax_reserve_rate':         reserve_rate,
                   'base_income_inr':          base_income,
                   'ytd_taxable_income':       round(ytd_taxable, 2),
                   'section_87a_limit':        SECTION_87A_LIMIT,
                   'net_profit':               net_profit,
                   'system_retention':         system_retention,
                   'human_withdrawal':         human_withdrawal,
                   'withdrawal_eligible':      True,
                   'cumulative_withdrawn':     round(cumulative_withdrawn, 2),
                   'recuperation_pct':         recuperation_pct,
                   'recuperation_remaining':   recuperation_remaining,
                   'accumulated_funding_est':  round(accumulated_funding, 2)})
        log.info('Monthly calc %s: gross=₹%.2f tax_reserve=₹%.2f (rate %.1f%%) '
                 'human_withdrawal=₹%.2f recuperation=%.1f%%',
                 month_label, gross_pnl, tax_reserve_credit,
                 reserve_rate * 100, human_withdrawal, recuperation_pct)

        # §11.4 / §11.5 recuperation milestones
        new_total = cumulative_withdrawn + human_withdrawal
        if cumulative_withdrawn < STARTING_CAPITAL_INR * 0.5 <= new_total:
            log_event(MODULE, 'warning', 'recuperation_milestone',
                      '50% capital recuperated — Transition Phase activation checkpoint',
                      {'milestone': '50pct',
                       'cumulative_after_withdrawal': round(new_total, 2)})
        if cumulative_withdrawn < STARTING_CAPITAL_INR <= new_total:
            log_event(MODULE, 'warning', 'recuperation_milestone',
                      '100% capital recuperated — Compound Phase activation checkpoint',
                      {'milestone': '100pct',
                       'cumulative_after_withdrawal': round(new_total, 2)})

    # ── Weekly R:R audit ──────────────────────────────────────────────────────

    def _is_first_cycle_of_week(self) -> bool:
        """True on the first 15-min cycle of each Monday UTC (hour=0, minute=3)."""
        now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        return now_utc.weekday() == 0 and now_utc.hour == 0 and now_utc.minute == 3

    def _rr_alert_already_run_this_week(self) -> bool:
        """True if an rr_alert_7d event exists since Monday 00:00 UTC this week."""
        now_utc    = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        week_start = int(datetime(now_utc.year, now_utc.month, now_utc.day,
                                  tzinfo=timezone.utc).timestamp()) - now_utc.weekday() * 86400
        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM events WHERE event_type='rr_alert_7d' AND timestamp>=? LIMIT 1",
                (week_start,)
            ).fetchone()
        return row is not None

    def _run_rr_check(self, now_utc: datetime) -> None:
        """
        Weekly R:R audit (§11.5). Computes average realised R:R over the trailing
        7 calendar days across all closed trades. Writes rr_alert_7d warning event
        if the average falls below 1.5 — the minimum threshold for positive
        expectancy at a 40% win rate.

        R:R per trade = |exit_price - entry_price| / |entry_price - sl_price|.
        Trades with zero risk (entry == sl) are skipped.
        """
        week_ago = int(now_utc.timestamp()) - 7 * 86400
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT t.exit_price, t.entry_price, p.stop_loss_price
                   FROM trades t
                   JOIN positions p ON p.trade_id = t.id
                   WHERE t.status = 'closed'
                     AND t.exit_timestamp >= ?
                     AND t.exit_price        IS NOT NULL
                     AND t.entry_price       IS NOT NULL
                     AND p.stop_loss_price   IS NOT NULL""",
                (week_ago,)
            ).fetchall()

        if not rows:
            log.info('Weekly R:R check: no closed trades in the last 7 days')
            return

        ratios: list[float] = []
        for r in rows:
            risk = abs(float(r['entry_price']) - float(r['stop_loss_price']))
            if risk <= 0:
                continue
            reward = abs(float(r['exit_price']) - float(r['entry_price']))
            ratios.append(reward / risk)

        if not ratios:
            log.info('Weekly R:R check: no valid R:R values (all trades had zero risk)')
            return

        avg_rr = round(sum(ratios) / len(ratios), 4)
        if avg_rr < 1.5:
            log_event(MODULE, 'warning', 'rr_alert_7d',
                      f'7-day average R:R = {avg_rr:.2f} — below 1.5 threshold (§11.5)',
                      {'avg_rr': avg_rr, 'trade_count': len(ratios),
                       'period_days': 7, 'threshold': 1.5})
            log.warning('Weekly R:R alert: avg=%.2f below 1.5 threshold', avg_rr)
        else:
            log.info('Weekly R:R check: avg=%.2f (%d trades) — OK', avg_rr, len(ratios))

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def _job_cycle(self) -> None:
        """Async scheduler wrapper — runs sync _run_cycle() in thread executor."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_cycle)

    def _run_cycle(self) -> None:
        """
        15-min portfolio management cycle:
        1. Compute portfolio value, peak, drawdown
        2. Evaluate alert level — escalate or de-escalate with actions
        3. Write 4H portfolio snapshot on boundary cycles;
           also write an extra snapshot if portfolio value changed >= 10% since last snapshot
        4. Run monthly withdrawal calculation on the last day of each month
        5. Run weekly R:R audit on the first cycle of each Monday
        """
        try:
            accumulated_funding = self._estimate_accumulated_funding_inr()

            total_value, active_margin, unrealised_pnl, open_count = self._compute_portfolio_value()

            peak_value   = self._get_peak_value()
            new_peak     = max(peak_value, total_value)
            drawdown_pct = max(0.0, (new_peak - total_value) / new_peak * 100) if new_peak > 0 else 0.0

            new_level = self._level_from_drawdown(drawdown_pct)
            if new_level != self._current_alert_level:
                positions = self._get_open_positions()
                if _LEVEL_ORDER[new_level] > _LEVEL_ORDER[self._current_alert_level]:
                    self._fire_alert(new_level, positions, drawdown_pct, total_value)
                else:
                    if new_level == 'green':
                        self._fire_alert_cleared(new_level, drawdown_pct, total_value)
                    else:
                        self._write_level_event_only(new_level, drawdown_pct, total_value)
                self._current_alert_level = new_level

            # §11.3: 4H boundary snapshot + extra snapshot on >=10% aggregate change.
            # When writing, always emit one aggregate row (model_source=None) plus
            # one per-model row for each of the three model capital pools.
            should_snap = self._should_write_4h_snapshot()
            if not should_snap:
                last_val = self._get_last_snapshot_value()
                if last_val > 0 and abs(total_value - last_val) / last_val >= 0.10:
                    should_snap = True
            if should_snap:
                # Aggregate snapshot (model_source=None)
                self._write_portfolio_snapshot(
                    total_value, active_margin, unrealised_pnl,
                    open_count, drawdown_pct, new_peak,
                    model_source=None,
                )
                # Per-model snapshots — each model has its own ₹1L pool
                for _msrc in _MODELS:
                    mv, mm, mu, mc = self._compute_portfolio_value_for_model(_msrc)
                    mpeak    = self._get_peak_value(model_source=_msrc)
                    mnew_peak = max(mpeak, mv)
                    mdd      = (max(0.0, (mnew_peak - mv) / mnew_peak * 100)
                                if mnew_peak > 0 else 0.0)
                    self._write_portfolio_snapshot(
                        mv, mm, mu, mc, mdd, mnew_peak,
                        model_source=_msrc,
                    )

            now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)

            if self._is_last_day_of_month() and not self._monthly_calc_already_run_today():
                self._run_monthly_calculation(now_utc, accumulated_funding)

            if self._is_first_cycle_of_week() and not self._rr_alert_already_run_this_week():
                self._run_rr_check(now_utc)

            log.debug('Cycle [aggregate]: value=₹%.2f peak=₹%.2f dd=%.2f%% level=%s funding_est=₹%.2f',
                      total_value, new_peak, drawdown_pct, self._current_alert_level,
                      accumulated_funding)

        except Exception as exc:
            log.error('Portfolio Manager cycle error: %s', exc, exc_info=True)
            log_event(MODULE, 'error', 'execution_error',
                      f'Portfolio cycle failed: {exc}', {'error': str(exc)})

    # ── Standalone runner ──────────────────────────────────────────────────────

    async def run(self) -> None:
        self.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except (KeyboardInterrupt, SystemExit):
            self.stop()


def main() -> None:
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s — %(message)s',
        stream=sys.stdout,
    )
    asyncio.run(PortfolioManager().run())


if __name__ == '__main__':
    main()
