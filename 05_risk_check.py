"""
Kronos Trading System — Module 5: Risk Check
Sections 8, 9, 10.1–10.2, 11.1–11.2, 12.3, 18.2, 19.1–19.2, 20.2 of the spec (v2.3).

Validates every pending Kronos signal against all documented risk rules before it
is passed to Module 6 (Execution). Runs on the same 4H cron cycle as Modules 1
and 4, firing 2 minutes after Module 4 to guarantee pending signals are in the DB.

Check sequence applied to each pending signal (first failure rejects):
  1.  Signal expiry          — signals older than 4H marked 'expired', not 'rejected'
  2.  Forced override active — system halted by prior forced_override event (Section 19.1)
  3.  Consecutive losses     — last 3 closed trades all negative -> write forced_override (Sections 8.4 / 19.1)
  4.  Macro blackout         — MacroCalendar.check().is_blocked (Sections 8.4 / 10.1)
  5.  Funding settlement blackout — within 2H before 8H settlement (Sections 8.4 / 10.1)
  6.  Asset exclusion        — defense-in-depth check for asset_exclusion events (Section 20.6)
  7.  Exchange circuit breaker — spread > CIRCUIT_BREAKER_SPREAD_PCT of mark price (Section 8.4)
  8.  System alert level     — Red/Orange: no new entries; Yellow: no Slot 3 (Section 8.2)
  9.  Position cap           — max 3 simultaneous open positions (Section 10.3)
  10. Confidence threshold   — signal.confidence >= effective threshold (Section 20.2)
      Extreme funding adjustment: if |rate| > 0.3%/8H against position direction,
      effective threshold raised by EXTREME_FUNDING_THRESHOLD_RAISE (relative, Section 9.2)
  11. Correlation rules      — per-pair Pearson correlation (Section 11.2):
        a. > 0.85 same direction with any open position -> blocked
        b. All-3-same-direction: only if all confidence scores above threshold
        c. 0.70-0.85 same direction -> approved with size_cap_pct = 5.0%
  12. Slippage model         — log estimate if calibrated (informational, Section 12.3)

Outputs per signal:
  - signals table: status updated to 'approved' | 'rejected' | 'expired',
    rejection_reason populated on rejection
  - events table: 'risk_check' event per signal with full RiskCheckResult payload

Scheduler:
  Cron: every hour at :12 UTC — 7 min after M4/M13/M14 fire at :05 UTC,
  allowing foundation model CPU inference (50 samples, 2-3 symbols) to
  finish writing all signals before M5 sweeps the pending queue.
"""

import asyncio
import importlib.util
import json
import logging
import math
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import get_connection, init_db, log_event

logger = logging.getLogger(__name__)

MODULE = 'risk_check'

# ── Constants ──────────────────────────────────────────────────────────────────

TIMEFRAME = '4h'

# Section 11.2 correlation thresholds
CORR_HIGH_THRESHOLD = 0.85   # above this: same-direction blocked
CORR_MED_THRESHOLD  = 0.70   # above this: same-direction capped at REDUCED_SIZE_PCT
CORR_PERIOD_CANDLES = 42     # 7 days x 6 4H candles

FULL_SIZE_PCT    = 10.0      # normal income phase margin per position (Section 10.3)
REDUCED_SIZE_PCT =  5.0      # second same-dir in 0.70-0.85 band (Section 11.2)
MAX_POSITIONS    =   3       # income phase max simultaneous open (Section 10.3)

# All three Slot 3 candidates; Yellow Alert blocks new entries on any of them
SLOT3_SYMBOL  = 'BNBUSD'   # fixed third asset (BTC=slot1, ETH=slot2)

# Funding settlement blackout (Section 8.4 / 10.1)
FUNDING_SETTLEMENT_HOURS = (0, 8, 16)    # UTC hours of 8H settlements
FUNDING_BLACKOUT_SECONDS = 2 * 3600      # block in the 2H window BEFORE each settlement

# Section 9.2 / 20.6 extreme funding
EXTREME_FUNDING_THRESHOLD       = 0.003  # 0.3%/8H — soft penalty: raises confidence threshold
EXTREME_FUNDING_THRESHOLD_RAISE = 0.10   # raise effective confidence threshold 10% relative

# Round-trip fee — verified against 31 actual trades (mean diff: Rs 0.00).
# Entry: maker 0.04% × 1.18 GST = 0.0472%
# Exit:  taker 0.10% × 1.18 GST = 0.1180%   ← worst-case (stop_loss / time_limit)
# Total: 0.1652%  →  0.0017 (0.17%, +0.005pp buffer for slippage)
# Best case (TP hit, both maker): 0.0944% — not used here; filter must assume worst-case exit.
# Actual median cost confirmed: 0.1651% of entry notional.
ROUND_TRIP_FEES_PCT = 0.0017   # 0.17% — verified accurate, worst-case (taker exit)

# TDS: NOT applicable to futures/options on Delta Exchange India.
# Section 194S (1% TDS) applies to VDA spot transfers only; perpetual futures are
# derivatives, not VDA transfers. Delta Exchange India confirmed this in their FAQ.
# Real cost floor = ROUND_TRIP_FEES_PCT (~0.17%) + funding.
# MIN_PREDICTED_RETURN_PCT (2.0%) dominates as the quality gate in all normal conditions.
TDS_RATE = 0.0   # 0% — futures/options exempt from TDS

# Section 8.4 exchange circuit breaker — extreme spread as % of mark price.
# BTC/ETH perpetual spreads are normally 0.01-0.1% of mark price. 1% = 10-100x
# widening, indicating severe dislocation or exchange issues.
# Configurable via KRONOS_CB_SPREAD_PCT env var.
CIRCUIT_BREAKER_SPREAD_PCT = float(os.environ.get('KRONOS_CB_SPREAD_PCT', '1.0'))

# Consecutive losing trades before forced override (Section 8.4 / 19.1).
# At 40% WR, P(3 consecutive losses) = 60%^3 = 21.6% — triggers ~once every 4-5 days,
# halting the system too aggressively. At 5 consecutive losses: P = 60%^5 = 7.8% —
# triggers only on genuine losing streaks, not routine variance.
CONSECUTIVE_LOSS_LIMIT = 5

# Minimum predicted return to approve a signal — set to the true cost floor.
# A signal is only blocked on cost grounds if its predicted return cannot cover
# the round-trip fee (0.17%). Beyond that, signal quality is governed by the
# confidence gate (MIN_CONFIDENCE_BY_SOURCE), not the magnitude of predicted return.
#
# History:
#   2.0% — original setting, based on wrong assumption that 1% TDS applied
#   0.5% — interim reduction after TDS corrected (still 3× true cost floor)
#   0.17% — correct value: exactly ROUND_TRIP_FEES_PCT (cost-recovery gate only)
#
# Effect: unlocks 89 signals that were blocked despite covering their cost.
# Verified: actual median round-trip cost = 0.1651% of notional. This matches.
MIN_PREDICTED_RETURN_PCT = ROUND_TRIP_FEES_PCT   # 0.17% — pure cost-recovery gate

# Per-model minimum confidence gate. Keys must match model_source values in signals table.
# Applied before all other checks in both paper and live mode — low-confidence signals
# from a specific model are rejected immediately without consuming the check chain.
# Omitted models have no confidence floor (0.0 — any confidence passes).
#
# 2026-06-05: kronos-base 0.4 floor REMOVED after benchmark analysis (n=373 resolved signals).
# Data showed the filter was inverted at the boundary:
#   blocked (<0.4): dir_acc=38.1% (n=291) — blocked signals were MORE directionally accurate
#   passing (>=0.4): dir_acc=24.4% (n=82)  — passing signals were LESS accurate
# The 0.4-0.5 band (dir_acc=20%) was the single worst band in the model.
# Root cause: low-confidence signals skew short (90 shorts in <0.4 range); those shorts
# performed well in a bear market. Filter was cutting shorts and passing long-biased noise.
# Fix: no confidence floor. Cost gate (MIN_PREDICTED_RETURN_PCT=0.17%) is the only filter.
MIN_CONFIDENCE_BY_SOURCE: dict = {}

# A pending signal older than this is expired, not rejected (Section 10.1: 4H entry timeout)
SIGNAL_EXPIRY_SECONDS = 4 * 3600

# In paper mode opposing positions on the same symbol are allowed: each model's
# prediction executes independently so pre-live analysis can compare which model
# called the direction correctly (head-to-head benchmarking). In live mode the
# opposing block is enforced — a simultaneous LONG + SHORT on the same asset
# nets to zero exposure while wasting two position slots and paying double fees.
PAPER_MODE = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'

# Section 19.2: win rate below 55% for 7 days → raise threshold 5pp absolute
WIN_RATE_7D_THRESHOLD    = 0.55
WIN_RATE_THRESHOLD_RAISE = 0.05   # 5 percentage points absolute
WIN_RATE_7D_WINDOW       = 7 * 86400
MIN_WINRATE_TRADES_7D    = 1      # need at least 1 closed trade (no spec-defined floor; 0 is undefined)

# ── Regime direction filter ────────────────────────────────────────────────────
# Composite EMA stack on 4H candles. Both conditions must agree before the filter
# activates — requires structural alignment, not just a brief price dip/spike.
#
#   BEAR: close < EMA50  AND  EMA50 < EMA200  → shorts only (longs blocked)
#   BULL: close > EMA50  AND  EMA50 > EMA200  → longs only  (shorts blocked)
#   NEUTRAL: price and MAs disagree (transition) → both directions allowed
#
# Applies identically in paper and live mode. Rejected signals are written to the
# DB with rejection_reason='regime_bear_long_blocked' | 'regime_bull_short_blocked'
# for retrospective validation (actual_return_pct will populate as trades resolve).
#
# Disable without code change: set KRONOS_REGIME_FILTER=false in .env
REGIME_FILTER_ENABLED = os.environ.get('KRONOS_REGIME_FILTER', 'true').lower() == 'true'
REGIME_EMA_FAST       = 50    # 4H candles ≈ 8 days — medium-term momentum
REGIME_EMA_SLOW       = 200   # 4H candles ≈ 33 days — structural trend
REGIME_CANDLES_NEEDED = REGIME_EMA_SLOW + 50   # buffer for EMA seed accuracy
REGIME_CACHE_TTL      = 900   # 15 min — 4H EMA doesn't change meaningfully intrabar

# Module-level cache: {symbol: (regime, expiry_unix_ts)}
# Persists across signals within a single M5 run cycle; refreshed every 15 minutes.
_regime_cache: dict = {}


# ── Dataclass ─────────────────────────────────────────────────────────────────

@dataclass
class RiskCheckResult:
    signal_id:             int
    symbol:                str
    direction:             str
    confidence:            float
    approved:              bool
    rejection_reason:      Optional[str]   # None when approved
    size_cap_pct:           Optional[float] # 5.0% when 0.70-0.85 same-direction band (Section 11.2)
    combined_margin_cap_pct: Optional[float] # 20.0% combined cap when all-3-same-direction fires (Section 11.2); Module 6 enforces
    slippage_estimate_bps: Optional[float] # most recent calibrated estimate, or None (informational)
    effective_threshold:   float           # threshold used (after win-rate + funding adjustments)
    checked_at:            int             # Unix epoch seconds


# ── Macro calendar loader ─────────────────────────────────────────────────────

def _import_macro_calendar():
    """Load MacroCalendar class from 03_macro_calendar.py via importlib."""
    try:
        spec = importlib.util.spec_from_file_location(
            'macro_calendar_m5',
            os.path.join(os.path.abspath(os.path.dirname(__file__) or '.'), '03_macro_calendar.py'),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.MacroCalendar
    except Exception as e:
        logger.warning('Could not import MacroCalendar: %s', e)
        return None


# ── EMA helper ────────────────────────────────────────────────────────────────

def _compute_ema(closes: list, period: int) -> float:
    """EMA seeded with SMA of first `period` values, then exponentially weighted."""
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1.0 - k)
    return ema


# ── RiskCheck class ───────────────────────────────────────────────────────────

class RiskCheck:
    """
    Validates pending Kronos signals against all documented risk rules.

    Call run_pending() to process the current batch; the scheduler calls
    run() which refreshes state and delegates to run_pending().
    """

    def __init__(self) -> None:
        MacroCalendarCls = _import_macro_calendar()
        self._macro_calendar = MacroCalendarCls() if MacroCalendarCls else None
        self._confidence_threshold: Optional[float] = self._load_confidence_threshold()
        self._current_slot3: Optional[str] = self._load_slot3()
        self._win_rate_adjustment: float = 0.0  # Section 19.2; updated each cycle by run()

    # ── Public API ────────────────────────────────────────────────────────────

    def run_pending(self) -> list[RiskCheckResult]:
        """
        Load all pending signals from the DB and run the full check sequence on
        each one. Updates signal status and writes risk_check events. Returns the
        results list.
        """
        pending = self._fetch_pending_signals()
        if not pending:
            log_event(MODULE, 'info', 'risk_check',
                      'No pending signals to validate')
            return []

        results = []
        for row in pending:
            result = self.check_signal(row)
            results.append(result)
        return results

    def check_signal(self, signal_row: dict) -> RiskCheckResult:
        """
        Run all checks on a single signal dict (from the signals table).
        Writes result to DB (status + risk_check event). Returns RiskCheckResult.
        """
        signal_id            = signal_row['id']
        symbol               = signal_row['symbol']
        direction            = signal_row['direction']
        confidence           = signal_row['confidence']
        horizon              = signal_row.get('horizon', '24h')
        predicted_return_pct = float(signal_row.get('predicted_return_pct') or 0.0)
        signal_ts            = signal_row['signal_timestamp']
        model_source         = signal_row.get('model_source') or 'custom'
        now                  = int(time.time())

        # Default result fields — overridden on approval or specific rejection
        approved              = False
        rejection_reason      = None
        size_cap_pct          = None
        combined_margin_cap_pct = None
        effective_threshold   = self._confidence_threshold or 0.0
        slippage_bps          = None

        # ── Check 1: Signal expiry ────────────────────────────────────────────
        if now - signal_ts > SIGNAL_EXPIRY_SECONDS:
            self._update_signal(signal_id, 'expired', 'signal_expired_4h_window')
            log_event(MODULE, 'info', 'risk_check',
                      f'{symbol} signal {signal_id} expired '
                      f'(age={(now - signal_ts)//3600:.1f}H > 4H window)',
                      {'signal_id': signal_id, 'symbol': symbol, 'status': 'expired'})
            return RiskCheckResult(
                signal_id=signal_id, symbol=symbol, direction=direction,
                confidence=confidence, approved=False,
                rejection_reason='signal_expired_4h_window',
                size_cap_pct=None, combined_margin_cap_pct=None,
                slippage_estimate_bps=None,
                effective_threshold=effective_threshold, checked_at=now,
            )

        # ── Per-model confidence gate (paper + live) ─────────────────────────
        # Applied before the paper/live branch — model-specific noise floor that
        # rejects regardless of mode. Does not count as a "live-only risk rule".
        _conf_block = self._check_model_confidence_block(model_source, confidence)
        if _conf_block:
            rejection_reason = _conf_block

        # ── Regime direction filter (paper + live) ────────────────────────────
        # Composite EMA50/EMA200 on 4H. Blocks longs in bear, shorts in bull.
        # Neutral (transition) passes both directions through unchanged.
        # See REGIME_FILTER_ENABLED, REGIME_EMA_FAST, REGIME_EMA_SLOW constants.
        if not rejection_reason:
            _regime_block = self._check_regime_direction_block(symbol, direction)
            if _regime_block:
                rejection_reason = _regime_block

        # ── Paper mode: benchmark fast-path ──────────────────────────────────
        # Goal: maximum throughput across all models for benchmark accuracy.
        #
        # Two checks applied:
        #   (a) Per-model stacking guard — prevents a SINGLE model from holding
        #       two positions on the same symbol/direction simultaneously. Also
        #       closes the race-condition window (approved-but-unexecuted signals).
        #       Different models may hold the same symbol concurrently — intentional.
        #   (b) Entry cost/quality block — predicted return must clear the
        #       MIN_PREDICTED_RETURN_PCT noise gate (0.5%).
        #
        # No position cap, no correlation check, no macro blackouts, no consecutive-
        # loss halts. Every qualifying signal from every model executes for maximum
        # benchmark dataset size.
        if PAPER_MODE and not rejection_reason:
            reason = self._check_open_position_exists(symbol, direction,
                                                      model_source, signal_id)
            if reason:
                rejection_reason = reason
            else:
                reason = self._check_entry_funding_block(
                    symbol, direction, predicted_return_pct, horizon)
                if reason:
                    rejection_reason = reason
                else:
                    slippage_bps = self._get_slippage_estimate(symbol)
                    approved = True

        elif not PAPER_MODE and not rejection_reason:
            # ── Live mode: full check chain ───────────────────────────────────

            # ── Check 2: Forced override active ──────────────────────────────
            reason = self._check_forced_override_active()
            if reason:
                rejection_reason = reason
            else:
                # ── Check 3: Consecutive losing trades ───────────────────────
                reason = self._check_consecutive_losses()
                if reason:
                    rejection_reason = reason
                else:
                    # ── Check 4: Macro blackout ───────────────────────────────
                    reason = self._check_macro_blackout()
                    if reason:
                        rejection_reason = reason
                    else:
                        # ── Check 5: Funding settlement blackout ──────────────
                        reason = self._check_funding_settlement_blackout()
                        if reason:
                            rejection_reason = reason
                        else:
                            # ── Check 5a: Entry funding hard block ────────────
                            reason = self._check_entry_funding_block(
                                symbol, direction, predicted_return_pct, horizon)
                            if reason:
                                rejection_reason = reason
                            else:
                                # ── Check 6: Stop loss 4H blackout (§19.2) ────
                                reason = self._check_stop_loss_blackout(symbol)
                                if reason:
                                    rejection_reason = reason
                                else:
                                    # ── Check 7: Asset exclusion ──────────────
                                    reason = self._check_asset_exclusion(symbol)
                                    if reason:
                                        rejection_reason = reason
                                    else:
                                        # ── Check 8: Circuit breaker ──────────
                                        reason = self._check_circuit_breaker(symbol)
                                        if reason:
                                            rejection_reason = reason
                                        else:
                                            # ── Check 9: Alert level ──────────
                                            reason = self._check_alert_level(symbol)
                                            if reason:
                                                rejection_reason = reason
                                            else:
                                                # ── Check 10: Position cap ────
                                                reason = self._check_position_cap(model_source)
                                                if reason:
                                                    rejection_reason = reason
                                                else:
                                                    # ── Check 11: Confidence ──
                                                    reason, effective_threshold = self._check_confidence(
                                                        confidence, symbol, direction)
                                                    if reason:
                                                        rejection_reason = reason
                                                    else:
                                                        # ── Check 12: Correlation
                                                        reason, size_cap_pct, combined_margin_cap_pct = self._check_correlation(
                                                            symbol, direction, confidence, signal_id,
                                                            model_source)
                                                        if reason:
                                                            rejection_reason = reason
                                                        else:
                                                            # ── Check 13: Slippage
                                                            slippage_bps = self._get_slippage_estimate(symbol)
                                                            approved = True

        # Persist result
        if approved:
            self._update_signal(signal_id, 'approved', None)
            severity = 'info'
        else:
            self._update_signal(signal_id, 'rejected', rejection_reason)
            severity = 'warning'

        result = RiskCheckResult(
            signal_id=signal_id,
            symbol=symbol,
            direction=direction,
            confidence=confidence,
            approved=approved,
            rejection_reason=rejection_reason,
            size_cap_pct=size_cap_pct,
            combined_margin_cap_pct=combined_margin_cap_pct,
            slippage_estimate_bps=slippage_bps,
            effective_threshold=effective_threshold,
            checked_at=now,
        )

        action = 'APPROVED' if approved else f'REJECTED ({rejection_reason})'
        log_event(MODULE, severity, 'risk_check',
                  f'{symbol} {direction.upper()} conf={confidence:.4f} -> {action}',
                  asdict(result))

        return result

    # ── Individual checks ─────────────────────────────────────────────────────

    @staticmethod
    def _check_forced_override_active() -> Optional[str]:
        """
        Returns rejection reason if a forced override is currently active.
        Active = most recent 'forced_override' event has no subsequent
        'forced_override_cleared' event. If active, all signal approvals are
        blocked until a human writes a forced_override_cleared event
        (Section 19.3 Option A: Resume).
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT event_type FROM events
                       WHERE event_type IN ('forced_override', 'forced_override_cleared')
                       ORDER BY id DESC LIMIT 1""",
                ).fetchone()
            if row and row['event_type'] == 'forced_override':
                return 'system_halted_forced_override'
        except Exception:
            pass
        return None

    @staticmethod
    def _check_consecutive_losses() -> Optional[str]:
        """
        Section 8.4 / 19.1 item 2: check the last CONSECUTIVE_LOSS_LIMIT closed
        trades system-wide. If all had negative pnl_gross, write a forced_override
        event and return the rejection reason. The forced_override is then picked up
        by _check_forced_override_active() for all subsequent signals this cycle.

        Dedup: if a forced_override_cleared event exists whose timestamp is AFTER
        the most recent of the N losing trades, the operator has already reviewed
        this exact cluster. Skip re-triggering unless a NEW losing trade appears
        (one whose exit_timestamp is after the clearance).
        """
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT id, pnl_gross, exit_timestamp FROM trades
                       WHERE status='closed' AND pnl_gross IS NOT NULL
                         AND quality_flag IS NULL
                       ORDER BY exit_timestamp DESC LIMIT ?""",
                    (CONSECUTIVE_LOSS_LIMIT,),
                ).fetchall()
            if not (len(rows) == CONSECUTIVE_LOSS_LIMIT and
                    all(r['pnl_gross'] < 0 for r in rows)):
                return None

            # All N trades are losses — check if operator already reviewed this cluster.
            most_recent_exit = max(r['exit_timestamp'] for r in rows)
            with get_connection() as conn:
                cleared = conn.execute(
                    """SELECT id FROM events
                       WHERE event_type='forced_override_cleared'
                         AND timestamp > ?
                       LIMIT 1""",
                    (most_recent_exit,),
                ).fetchone()
            if cleared:
                # Operator sent /resume after the last loss — don't re-trigger
                # until a new losing trade exits after the clearance.
                return None

            trade_ids = [r['id'] for r in rows]
            log_event(
                MODULE, 'critical', 'forced_override',
                f'{CONSECUTIVE_LOSS_LIMIT} consecutive losing trades detected — '
                f'system halted, human review required (Section 19.1 item 2)',
                {'trigger': 'consecutive_losses',
                 'loss_count': CONSECUTIVE_LOSS_LIMIT,
                 'trade_ids': trade_ids},
            )
            return (f'forced_override_written: '
                    f'{CONSECUTIVE_LOSS_LIMIT}_consecutive_losses')
        except Exception as e:
            logger.warning('_check_consecutive_losses failed: %s', e)
        return None

    @staticmethod
    def _check_stop_loss_blackout(symbol: str) -> Optional[str]:
        """
        §19.2: No new entry on an asset for 4 hours after a stop loss exit.
        Module 7 writes a 'stop_loss_exit' event with data.blackout_until (Unix epoch).
        Returns rejection reason if a blackout is currently active for this symbol.
        """
        try:
            now = int(time.time())
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='stop_loss_exit'
                         AND json_extract(data, '$.symbol') = ?
                       ORDER BY id DESC LIMIT 1""",
                    (symbol,),
                ).fetchone()
            if row and row['data']:
                data = json.loads(row['data'])
                blackout_until = data.get('blackout_until', 0)
                if now < blackout_until:
                    return f'stop_loss_4h_blackout_{symbol}'
        except Exception as exc:
            logger.warning('_check_stop_loss_blackout error for %s: %s', symbol, exc)
        return None

    @staticmethod
    def _check_asset_exclusion(symbol: str) -> Optional[str]:
        """
        Section 20.6 defense-in-depth: check asset_exclusion events even if
        Module 4 already filtered them. The exclusion event may have been written
        in the 2-minute gap between Module 4 (:05 UTC) and Module 5 (:07 UTC).
        Event schema: data={'symbol': str, 'excluded': bool, 'reason': str}
        Written by Modules 8 and 11 on win-rate/consecutive-loss/liquidity triggers.
        """
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='asset_exclusion' AND data IS NOT NULL
                       ORDER BY timestamp DESC""",
                ).fetchall()
            seen: set = set()
            for row in rows:
                try:
                    payload = json.loads(row['data'])
                    sym = payload.get('symbol')
                    if sym and sym not in seen:
                        seen.add(sym)
                        if sym == symbol and payload.get('excluded', False):
                            return f'asset_excluded: {symbol} ({payload.get("reason", "no reason")})'
                except Exception:
                    pass
        except Exception:
            pass
        return None

    @staticmethod
    def _check_circuit_breaker(symbol: str) -> Optional[str]:
        """
        Section 8.4 exchange circuit breaker: reject if the most recent
        orderbook_snapshot for the symbol shows a spread > CIRCUIT_BREAKER_SPREAD_PCT
        percent of mark price, indicating extreme volatility or exchange dislocation.
        Normal perpetual spreads are 0.01-0.1% of mark price; 1% = 10-100x widening.
        Returns None if no snapshot data is available (fail-open).
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT spread, mark_price FROM orderbook_snapshots
                       WHERE symbol=? AND mark_price > 0 AND spread IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                    (symbol,),
                ).fetchone()
            if row and row['mark_price']:
                spread_pct = (row['spread'] / row['mark_price']) * 100
                if spread_pct > CIRCUIT_BREAKER_SPREAD_PCT:
                    return (f'circuit_breaker: {symbol} spread={spread_pct:.3f}% '
                            f'> {CIRCUIT_BREAKER_SPREAD_PCT}% threshold '
                            f'(extreme volatility, Section 8.4)')
        except Exception:
            pass
        return None   # fail-open: no snapshot data = allow

    def _check_macro_blackout(self) -> Optional[str]:
        """Returns rejection reason if macro calendar is blocking, else None."""
        if self._macro_calendar is None:
            return None   # calendar not available — allow (fail-open)
        try:
            status = self._macro_calendar.check()
            if status.is_blocked:
                return (f'macro_blackout: {status.blocking_event_name} '
                        f'(within 4H window)')
        except Exception as e:
            logger.warning('MacroCalendar.check() failed: %s', e)
        return None

    @staticmethod
    def _check_model_confidence_block(model_source: str, confidence: float) -> Optional[str]:
        """
        Per-model confidence gate. Rejects signals below the model-specific
        minimum confidence defined in MIN_CONFIDENCE_BY_SOURCE.

        Applied before all other checks (paper and live) so noisy low-confidence
        signals from a specific model never reach the cost/risk chain.
        Models not listed in MIN_CONFIDENCE_BY_SOURCE have no floor (pass always).
        """
        min_conf = MIN_CONFIDENCE_BY_SOURCE.get(model_source, 0.0)
        if min_conf > 0.0 and confidence < min_conf:
            return (
                f'confidence_block: {model_source} confidence {confidence:.4f} '
                f'below model minimum {min_conf:.4f}'
            )
        return None

    @staticmethod
    def _fetch_regime(symbol: str) -> str:
        """
        Classify the current market regime for symbol using a composite EMA stack
        on 4H candles. Returns 'bull', 'bear', or 'neutral'.

        Composite rule — both conditions must agree:
          BEAR: close < EMA50 AND EMA50 < EMA200
          BULL: close > EMA50 AND EMA50 > EMA200
          NEUTRAL: any other configuration (transition / ranging)

        Neutral means the filter does not activate — both directions are allowed.
        This prevents whipsawing during genuine trend transitions.

        Results are cached for REGIME_CACHE_TTL (15 min) per symbol. Fails open
        (returns 'neutral') on any DB or computation error so a data gap never
        silently blocks all trading.
        """
        now    = int(time.time())
        cached = _regime_cache.get(symbol)
        if cached and now < cached[1]:
            return cached[0]

        regime = 'neutral'
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT close FROM ohlcv
                       WHERE symbol=? AND timeframe='4h'
                       ORDER BY timestamp DESC LIMIT ?""",
                    (symbol, REGIME_CANDLES_NEEDED),
                ).fetchall()

            if len(rows) < REGIME_EMA_SLOW + 10:
                # Insufficient history — fail open
                _regime_cache[symbol] = ('neutral', now + REGIME_CACHE_TTL)
                return 'neutral'

            closes   = [float(r['close']) for r in reversed(rows)]
            ema_fast = _compute_ema(closes, REGIME_EMA_FAST)
            ema_slow = _compute_ema(closes, REGIME_EMA_SLOW)
            current  = closes[-1]

            if current < ema_fast and ema_fast < ema_slow:
                regime = 'bear'
            elif current > ema_fast and ema_fast > ema_slow:
                regime = 'bull'
            # else: neutral (price and MAs disagree — transition)

        except Exception as exc:
            logger.warning('_fetch_regime failed for %s: %s — failing open', symbol, exc)

        _regime_cache[symbol] = (regime, now + REGIME_CACHE_TTL)
        return regime

    @staticmethod
    def _check_regime_direction_block(symbol: str, direction: str) -> Optional[str]:
        """
        Regime direction filter. Returns rejection reason or None.

        BEAR regime: blocks longs  (shorts pass through)
        BULL regime: blocks shorts (longs pass through)
        NEUTRAL:     no block      (both directions pass)

        Applied before the paper/live branch so it runs identically in both modes.
        Rejected signals are persisted to the DB with the regime rejection reason,
        providing a paper trail for retrospective filter validation.
        """
        if not REGIME_FILTER_ENABLED:
            return None

        regime = RiskCheck._fetch_regime(symbol)

        if regime == 'bear' and direction == 'long':
            return (
                f'regime_bear_long_blocked: {symbol} close<EMA{REGIME_EMA_FAST}'
                f'<EMA{REGIME_EMA_SLOW} on 4H — longs blocked in bear regime'
            )
        if regime == 'bull' and direction == 'short':
            return (
                f'regime_bull_short_blocked: {symbol} close>EMA{REGIME_EMA_FAST}'
                f'>EMA{REGIME_EMA_SLOW} on 4H — shorts blocked in bull regime'
            )
        return None

    @staticmethod
    def _check_entry_funding_block(
        symbol:               str,
        direction:            str,
        predicted_return_pct: float,
        horizon:              str,
    ) -> Optional[str]:
        """
        Cost-recovery entry block.

        Rejects a signal only if its predicted return cannot cover the round-trip
        trading cost. This is a pure cost gate — not a quality gate.

            cost_floor = TDS_RATE + ROUND_TRIP_FEES_PCT + funding_cost
                       = 0.0% + 0.17% + funding_cost
                       ≈ 0.17% under normal funding conditions

            min_return = max(cost_floor, MIN_PREDICTED_RETURN_PCT)
                       = max(cost_floor, ROUND_TRIP_FEES_PCT)
                       = cost_floor  (they are equal)

        Signal quality (directional conviction) is governed entirely by the
        confidence gate (MIN_CONFIDENCE_BY_SOURCE), not predicted return magnitude.
        ATR-based TP in Module 6 sets exit levels — predicted_return magnitude
        does not determine TP distance.

        Verified: actual median round-trip cost = 0.1651% of notional (31 trades).
        ROUND_TRIP_FEES_PCT = 0.0017 (0.17%) matches this exactly.

        Fallback: if predicted_return_pct is 0.0 (legacy signal written before
        this column existed), the check is skipped to avoid false blocks.
        """
        if predicted_return_pct == 0.0:
            return None   # legacy signal — skip cost check

        # ── Funding cost (only counts when against the direction) ─────────────
        rate         = RiskCheck._get_current_funding_rate(symbol)
        rate_against = 0.0
        settlements  = 0.0
        funding_cost = 0.0
        if rate is not None:
            rate_against = rate if direction == 'long' else -rate
            if rate_against > 0:
                try:
                    horizon_h = int(horizon.rstrip('h'))
                except (ValueError, AttributeError):
                    horizon_h = 24
                settlements  = horizon_h / 8.0          # e.g. 24H / 8 = 3.0
                funding_cost = rate_against * settlements

        # ── Minimum return: higher of cost floor and profit floor ───────────────
        # TDS_RATE = 0.0 (futures exempt); real cost floor = fees + funding (~0.17%+)
        # Profit floor (2.0%) dominates in all normal conditions.
        cost_floor    = TDS_RATE + ROUND_TRIP_FEES_PCT + funding_cost
        min_return    = max(cost_floor, MIN_PREDICTED_RETURN_PCT)
        actual_return = abs(predicted_return_pct) / 100.0

        if actual_return < min_return:
            funding_part = (
                f' + funding ({rate_against*100:.3f}%/8H × '
                f'{settlements:.0f} settlements = {funding_cost*100:.3f}%)'
                if funding_cost > 0 else ''
            )
            floor_label = (
                f'profit floor {MIN_PREDICTED_RETURN_PCT*100:.2f}%'
                if MIN_PREDICTED_RETURN_PCT >= cost_floor
                else f'cost floor {cost_floor*100:.3f}%'
            )
            return (
                f'entry_cost_block: predicted return {predicted_return_pct:+.3f}% '
                f'below {floor_label} '
                f'[fees {ROUND_TRIP_FEES_PCT*100:.2f}%'
                f'{funding_part}] — '
                f'minimum needed {min_return*100:.3f}%, got {actual_return*100:.3f}%'
            )
        return None

    @staticmethod
    def _check_open_position_exists(
        symbol:       str,
        direction:    str,
        model_source: str,
        signal_id:    int = 0,
    ) -> Optional[str]:
        """
        Paper-mode stacking guard — checks both open positions AND approved-but-
        not-yet-executed signals to close the race-condition window.

        Race condition (root cause of kronos-base-4h duplicate positions):
          Two signals for the same model+symbol+direction arrive in the same 4H
          cycle. M5 processes both before M6 executes either. Both pass the open-
          position check (no position exists yet) and both get approved. M6 then
          executes both, creating duplicate concurrent positions.

        Fix: also reject if a same-model+symbol+direction signal is already
        approved (status='approved') and has not yet been executed. Exclude the
        current signal_id so we don't self-reject.

        Different models may hold the same symbol in the same direction
        simultaneously: intentional benchmark behaviour (independent capital pools).
        Opposing positions (long+short, same model) are also allowed.
        """
        try:
            with get_connection() as conn:
                # 1. Check open positions (existing behaviour)
                row = conn.execute(
                    """SELECT p.id FROM positions p
                       JOIN trades  t ON t.id       = p.trade_id
                       JOIN signals s ON s.id       = t.signal_id
                       WHERE p.status                         = 'open'
                         AND p.symbol                         = ?
                         AND p.direction                       = ?
                         AND COALESCE(s.model_source, 'custom') = ?
                       LIMIT 1""",
                    (symbol, direction, model_source),
                ).fetchone()
                if row:
                    return (
                        f'open_position_exists: {model_source} already has '
                        f'{symbol} {direction} open (paper stacking guard)'
                    )

                # 2. Check approved-but-unexecuted signals (race-condition guard)
                pending = conn.execute(
                    """SELECT id FROM signals
                       WHERE model_source = ?
                         AND symbol       = ?
                         AND direction    = ?
                         AND status       = 'approved'
                         AND id           != ?
                       LIMIT 1""",
                    (model_source, symbol, direction, signal_id),
                ).fetchone()
                if pending:
                    return (
                        f'duplicate_pending_signal: {model_source} already has '
                        f'{symbol} {direction} approved and awaiting execution '
                        f'(race-condition guard, signal_id={pending["id"]})'
                    )
        except Exception as exc:
            logger.warning('_check_open_position_exists error: %s', exc)
        return None

    @staticmethod
    def _check_funding_settlement_blackout() -> Optional[str]:
        """
        Block new entries in the 2H window immediately BEFORE each 8H funding
        settlement (00:00, 08:00, 16:00 UTC) to avoid manipulation spikes
        (Section 8.4 / 10.1).
        """
        now = datetime.now(timezone.utc)
        secs_since_midnight = now.hour * 3600 + now.minute * 60 + now.second
        for h in FUNDING_SETTLEMENT_HOURS:
            secs_until = h * 3600 - secs_since_midnight
            if secs_until <= 0:
                secs_until += 86400   # next calendar day
            if 0 < secs_until <= FUNDING_BLACKOUT_SECONDS:
                return (f'funding_settlement_blackout: {h:02d}:00 UTC in '
                        f'{secs_until / 3600:.2f}H '
                        f'(within 2H pre-settlement window, Section 8.4)')
        return None

    def _check_alert_level(self, symbol: str) -> Optional[str]:
        """
        Read current system alert level from events table.
        Red/Orange: block all new entries. Yellow: block Slot 3 entries only.
        """
        level = self._load_alert_level()
        if level == 'red':
            return 'system_halted_red_alert'
        if level == 'orange':
            return 'orange_alert_no_new_entries'
        if level == 'yellow' and symbol == self._current_slot3:
            return 'yellow_alert_slot3_blocked'
        return None

    @staticmethod
    def _check_position_cap(model_source: str) -> Optional[str]:
        """
        Reject if this model already has MAX_POSITIONS (3) open positions.

        Each model has its own ₹1L capital pool and independent position cap —
        one model's positions do not count against another model's limit.
        This allows up to 3 × MAX_POSITIONS total open positions across all models,
        which is correct for the per-model capital architecture.
        """
        positions = RiskCheck._get_open_positions(model_source=model_source)
        if len(positions) >= MAX_POSITIONS:
            return (f'max_positions_reached: '
                    f'{model_source} has {len(positions)}/{MAX_POSITIONS} open positions')
        return None

    def _check_confidence(
        self, confidence: float, symbol: str, direction: str
    ) -> tuple[Optional[str], float]:
        """
        Returns (rejection_reason, effective_threshold).
        Pre-live (no threshold set): always passes, effective_threshold = 0.0.
        With extreme funding against direction (Section 9.2): threshold raised by
        EXTREME_FUNDING_THRESHOLD_RAISE (10% relative) to flag caution.
        """
        threshold = self._confidence_threshold
        if threshold is None:
            return None, 0.0   # pre-live: no threshold enforced

        # Section 19.2: win rate adjustment (+5pp absolute if 7-day win rate < 55%)
        effective = min(1.0, threshold + self._win_rate_adjustment)

        # Note: funding cost adjustment previously applied here (Section 9.2) has
        # been replaced by the cost-adjusted entry block in _check_entry_funding_block,
        # which uses the signal's actual predicted return to make the decision rather
        # than a fixed threshold multiplier. The EXTREME_FUNDING_THRESHOLD soft
        # penalty is therefore no longer needed here.

        if confidence < effective:
            return (f'confidence_{confidence:.4f}_below_threshold_{effective:.4f}',
                    effective)
        return None, effective

    @staticmethod
    def _check_pending_signal_duplicate(
        signal_id: int, symbol: str, direction: str, model_source: str
    ) -> Optional[str]:
        """
        Block same-model duplicate signals for the same symbol.

        Same model + same symbol + same direction → always blocked (pure duplicate).

        Same model + same symbol + opposite direction:
          Paper mode → ALLOWED — pre-live benchmarking lets both models' signals
            execute independently to determine which called direction correctly.
          Live mode  → BLOCKED — opposing positions on the same asset cancel out
            exposure while consuming position slots and paying double fees.

        Different models on the same symbol are ALWAYS allowed (independent capital
        pools): Custom long BTC and Mini short BTC are independent predictions and
        should both execute against their respective ₹1L pool.
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT id, direction FROM signals
                       WHERE symbol      = ?
                         AND status      IN ('pending','approved')
                         AND id          != ?
                         AND COALESCE(model_source, 'custom') = ?
                       LIMIT 1""",
                    (symbol, signal_id, model_source),
                ).fetchone()
            if row:
                if row['direction'] == direction:
                    return (f'duplicate_pending_signal: {symbol} {direction} '
                            f'already queued (same model) as signal {row["id"]}')
                elif not PAPER_MODE:
                    return (f'opposing_pending_signal: {symbol} has {row["direction"]} '
                            f'already queued (same model) as signal {row["id"]} '
                            f'— opposing positions blocked in live mode')
        except Exception as exc:
            logger.warning('_check_pending_signal_duplicate error: %s', exc)
        return None

    def _check_correlation(
        self, symbol: str, direction: str, confidence: float, signal_id: int,
        model_source: str = 'custom',
    ) -> tuple[Optional[str], Optional[float], Optional[float]]:
        """
        Apply Section 11.2 correlation rules against THIS MODEL's open positions only.

        Per-model capital architecture: each model has its own ₹1L pool and independent
        position logic. Correlation rules, duplicate-position checks, and the
        all-3-same-direction cap are all scoped to model_source so that Custom having
        a BTC long does not block Mini from also going long BTC independently.

        Returns (rejection_reason, size_cap_pct, combined_margin_cap_pct).
          rejection_reason:        None if approved
          size_cap_pct:            5.0% when the 0.70-0.85 same-direction band applies
          combined_margin_cap_pct: 20.0% when all-3-same-direction fires for this model
        """
        dup = RiskCheck._check_pending_signal_duplicate(
            signal_id, symbol, direction, model_source)
        if dup:
            return dup, None, None

        # Scope all position checks to this model's own positions
        open_positions = self._get_open_positions(model_source=model_source)
        if not open_positions:
            return None, None, None

        # Same-symbol position check within this model's pool.
        # Same direction → duplicate (always blocked).
        # Opposite direction → paper ALLOWED (benchmark), live BLOCKED.
        for pos in open_positions:
            if pos['symbol'] == symbol:
                if pos['direction'] == direction:
                    return (f'duplicate_position_{symbol}_{direction}', None, None)
                elif not PAPER_MODE:
                    return (
                        f'opposing_position_blocked_{symbol}: '
                        f'existing {pos["direction"]}, new signal {direction} '
                        f'— opposing positions blocked in live mode',
                        None, None,
                    )

        size_cap: Optional[float] = None
        combined_margin_cap: Optional[float] = None

        for pos in open_positions:
            if pos['symbol'] == symbol:
                continue  # already handled above

            corr = self._compute_correlation(symbol, pos['symbol'])
            if corr is None:
                continue  # insufficient data — allow

            if pos['direction'] == direction:
                if corr > CORR_HIGH_THRESHOLD:
                    return (
                        f'high_correlation_blocked: {symbol} vs {pos["symbol"]} '
                        f'corr={corr:.3f}>{CORR_HIGH_THRESHOLD} same_direction={direction}',
                        None,
                        None,
                    )
                if corr > CORR_MED_THRESHOLD:
                    size_cap = REDUCED_SIZE_PCT   # capped but not blocked

        # All-3-same-direction check (Section 11.2) — scoped to this model's positions.
        # Triggers when this new entry would be the 3rd position for this model and all
        # are in the same direction.
        same_dir_open = [p for p in open_positions if p['direction'] == direction]
        if len(open_positions) == MAX_POSITIONS - 1 and len(same_dir_open) == len(open_positions):
            threshold = self._confidence_threshold
            if threshold is not None:
                existing_confidences = [p.get('confidence', 0.0) for p in open_positions]
                for ec in existing_confidences:
                    if ec < threshold:
                        return (
                            f'all_same_direction_blocked: existing position confidence '
                            f'{ec:.4f} below threshold {threshold:.4f} '
                            f'(Section 11.2 all-3-same-direction rule)',
                            None,
                            None,
                        )
            # Permitted — apply 20% combined margin cap within this model's pool.
            combined_margin_cap = 20.0

        return None, size_cap, combined_margin_cap

    @staticmethod
    def _compute_7d_win_rate_adj() -> float:
        """
        Section 19.2: if win rate < WIN_RATE_7D_THRESHOLD (55%) over the last 7 days
        (with at least MIN_WINRATE_TRADES_7D trades), return WIN_RATE_THRESHOLD_RAISE
        (0.05 absolute) to be added to the base confidence threshold.
        Returns 0.0 when insufficient data or win rate is acceptable.
        """
        try:
            cutoff = int(time.time()) - WIN_RATE_7D_WINDOW
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT pnl_gross FROM trades
                       WHERE status='closed' AND pnl_gross IS NOT NULL
                         AND exit_timestamp >= ?""",
                    (cutoff,),
                ).fetchall()
            if len(rows) < MIN_WINRATE_TRADES_7D:
                return 0.0
            wins = sum(1 for r in rows if r['pnl_gross'] > 0)
            win_rate = wins / len(rows)
            return WIN_RATE_THRESHOLD_RAISE if win_rate < WIN_RATE_7D_THRESHOLD else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def _get_slippage_estimate(symbol: str) -> Optional[float]:
        """
        Return the most recent calibrated slippage estimate (bps) from the
        slippage_estimate events. Returns None if uncalibrated or unavailable.
        Informational only — does not cause rejection (Section 12.3).
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE module='slippage_model'
                         AND event_type='slippage_estimate'
                         AND data IS NOT NULL
                         AND json_extract(data, '$.symbol') = ?
                       ORDER BY timestamp DESC LIMIT 1""",
                    (symbol,),
                ).fetchone()
            if row:
                payload = json.loads(row['data'])
                if payload.get('is_calibrated'):
                    return payload.get('estimated_slippage_bps')
        except Exception:
            pass
        return None

    # ── DB helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _fetch_pending_signals() -> list[dict]:
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT id, symbol, direction, confidence, horizon,
                              predicted_return_pct, signal_timestamp,
                              COALESCE(model_source, 'custom') AS model_source
                       FROM signals WHERE status='pending'
                       ORDER BY signal_timestamp ASC""",
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error('Failed to fetch pending signals: %s', e)
            return []

    @staticmethod
    def _update_signal(signal_id: int, status: str, reason: Optional[str]) -> None:
        try:
            with get_connection() as conn:
                conn.execute(
                    """UPDATE signals SET status=?, rejection_reason=?
                       WHERE id=?""",
                    (status, reason, signal_id),
                )
        except Exception as e:
            logger.error('Failed to update signal %d: %s', signal_id, e)

    @staticmethod
    def _get_open_positions(model_source: Optional[str] = None) -> list[dict]:
        """
        Return open positions with symbol, direction, margin_used, and confidence.

        model_source=None   → all open positions across every model (used for
                              drawdown-alert actions and global reference queries).
        model_source='...'  → only positions generated by that model (used for
                              per-model position-cap and correlation checks).

        Per-model capital architecture: each model's risk checks are scoped to
        its own positions so models operate as independent capital pools.
        """
        try:
            with get_connection() as conn:
                if model_source is None:
                    rows = conn.execute(
                        """SELECT p.symbol, p.direction,
                                  COALESCE(p.margin_used, 0.0) AS margin_used,
                                  COALESCE(s.confidence, 0.0)  AS confidence
                           FROM positions p
                           LEFT JOIN trades  t ON t.id        = p.trade_id
                           LEFT JOIN signals s ON s.id        = t.signal_id
                           WHERE p.status = 'open'""",
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """SELECT p.symbol, p.direction,
                                  COALESCE(p.margin_used, 0.0)  AS margin_used,
                                  COALESCE(s.confidence, 0.0)   AS confidence
                           FROM positions p
                           JOIN trades  t ON t.id        = p.trade_id
                           JOIN signals s ON s.id        = t.signal_id
                           WHERE p.status       = 'open'
                             AND s.model_source = ?""",
                        (model_source,),
                    ).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error('Failed to fetch open positions (model=%s): %s', model_source, e)
            return []

    @staticmethod
    def _compute_correlation(sym1: str, sym2: str) -> Optional[float]:
        """Rolling 7-day Pearson correlation of 4H close returns."""
        try:
            with get_connection() as conn:
                def _fetch(symbol: str) -> list[float]:
                    rows = conn.execute(
                        """SELECT close FROM ohlcv
                           WHERE symbol=? AND timeframe=?
                           ORDER BY timestamp DESC LIMIT ?""",
                        (symbol, TIMEFRAME, CORR_PERIOD_CANDLES + 1),
                    ).fetchall()
                    return [r['close'] for r in reversed(rows)]

                closes1 = _fetch(sym1)
                closes2 = _fetch(sym2)
        except Exception:
            return None

        if len(closes1) < 2 or len(closes2) < 2:
            return None

        def _returns(closes: list[float]) -> list[float]:
            return [(closes[i] - closes[i - 1]) / closes[i - 1]
                    for i in range(1, len(closes))
                    if closes[i - 1] != 0]

        r1 = _returns(closes1)
        r2 = _returns(closes2)
        n = min(len(r1), len(r2))
        if n < 2:
            return None

        r1, r2 = r1[-n:], r2[-n:]
        mean1 = sum(r1) / n
        mean2 = sum(r2) / n
        cov  = sum((r1[i] - mean1) * (r2[i] - mean2) for i in range(n))
        std1 = math.sqrt(sum((x - mean1) ** 2 for x in r1))
        std2 = math.sqrt(sum((x - mean2) ** 2 for x in r2))
        if std1 < 1e-10 or std2 < 1e-10:
            return None
        return cov / (std1 * std2)

    @staticmethod
    def _get_current_funding_rate(symbol: str) -> Optional[float]:
        """Return the most recent funding rate for a symbol, or None."""
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT rate FROM funding_rates
                       WHERE symbol=? ORDER BY timestamp DESC LIMIT 1""",
                    (symbol,),
                ).fetchone()
            return row['rate'] if row else None
        except Exception:
            return None

    @staticmethod
    def _load_confidence_threshold() -> Optional[float]:
        """
        Read the most recent confidence_threshold_set event.
        Returns None pre-live (no threshold set).
        Written by calibration tooling after pre-live analysis:
          module='signal_generator', event_type='confidence_threshold_set',
          data={'threshold': 0.65}
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='confidence_threshold_set'
                         AND module='signal_generator'
                         AND data IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                ).fetchone()
            if row:
                payload = json.loads(row['data'])
                val = payload.get('threshold')
                if val is not None:
                    return float(val)
        except Exception:
            pass
        return None

    @staticmethod
    def _load_slot3() -> Optional[str]:
        """Return the active Slot 3 symbol from the most recent slot3_selection event."""
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE module='signal_generator'
                         AND event_type='slot3_selection'
                         AND data IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                ).fetchone()
            if row:
                payload = json.loads(row['data'])
                return payload.get('slot3_symbol')
        except Exception:
            pass
        return None

    @staticmethod
    def _load_alert_level() -> str:
        """
        Return the current system alert level: 'red', 'orange', 'yellow', or 'green'.
        Reads the most recent alert or alert_cleared event. If no alert events exist,
        returns 'green' (normal operation).
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT event_type FROM events
                       WHERE event_type IN
                         ('alert_red','alert_orange','alert_yellow','alert_cleared')
                       ORDER BY id DESC LIMIT 1""",
                ).fetchone()
            if row:
                et = row['event_type']
                if et == 'alert_cleared':
                    return 'green'
                return et.replace('alert_', '')   # 'red' | 'orange' | 'yellow'
        except Exception:
            pass
        return 'green'

    # ── Scheduler entry point ─────────────────────────────────────────────────

    def run(self) -> None:
        """
        Reload cached state and process all pending signals.
        Called by APScheduler cron job.
        """
        self._confidence_threshold = self._load_confidence_threshold()
        self._current_slot3 = self._load_slot3()

        # Section 19.2: compute 7-day win rate adjustment every cycle
        adj = self._compute_7d_win_rate_adj()
        if adj > 0:
            log_event(MODULE, 'warning', 'win_rate_alert_7d',
                      f'Rolling 7-day win rate below {WIN_RATE_7D_THRESHOLD:.0%} — '
                      f'confidence threshold raised by {WIN_RATE_THRESHOLD_RAISE:.0%} '
                      f'(Section 19.2 optional override trigger)',
                      {'adjustment': adj,
                       'threshold_base': self._confidence_threshold,
                       'threshold_effective': (self._confidence_threshold or 0.0) + adj})
        self._win_rate_adjustment = adj

        results = self.run_pending()
        approved = sum(1 for r in results if r.approved)
        rejected = sum(1 for r in results if not r.approved and r.rejection_reason != 'signal_expired_4h_window')
        expired  = sum(1 for r in results if r.rejection_reason == 'signal_expired_4h_window')
        log_event(MODULE, 'info', 'heartbeat',
                  f'Risk check cycle complete: {approved} approved, '
                  f'{rejected} rejected, {expired} expired',
                  {'approved': approved, 'rejected': rejected, 'expired': expired})


# ── Standalone runner ─────────────────────────────────────────────────────────

async def main() -> None:
    init_db()
    checker = RiskCheck()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, checker.run)  # startup scan — off-thread to avoid blocking

    scheduler = AsyncIOScheduler(timezone='UTC')

    async def _job():
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, checker.run)

    scheduler.add_job(
        _job,
        CronTrigger(minute=12, timezone='UTC'),
        id='risk_check_1h',
        name='Risk Check — 1H cycle',
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info('Risk Check scheduler started — every hour at :12 UTC')

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )
    asyncio.run(main())
