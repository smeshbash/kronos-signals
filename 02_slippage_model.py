"""
Kronos Trading System — Module 2: Slippage Model
Section 12 of the requirements spec (v2.0).

Builds a regime-dependent slippage estimate per asset from self-collected
Delta Exchange data stored in SQLite by Module 1.

Outputs per asset (SlippageEstimate dataclass):
  - Liquidity regime:        high_liquidity | normal | low_liquidity
  - estimated_slippage_bps:  expected limit-entry slippage in basis points
  - fill_probability_4h:     estimated probability of limit fill within 4H
  - regime_spread_p50/p95:   historical spread percentiles in this regime
  - is_calibrated:           False if < MIN_HISTORY_WINDOWS data points exist

Not active during pre-live (is_calibrated=False until MIN_HISTORY_WINDOWS
fill_window rows exist per symbol — approx 25 hours of Module 1 collection).

Writes latest estimates to events table (event_type='slippage_estimate') after
each update cycle for audit trail and cross-module access. Also exposes
SlippageModel class for direct import by downstream modules (Module 5, 6).
"""

import asyncio
import bisect
import logging
import time
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import get_connection, log_event

logger = logging.getLogger(__name__)

MODULE = 'slippage_model'

DELTA_SYMBOLS = ['BTCUSD', 'ETHUSD', 'BNBUSD', 'XRPUSD']

# Minimum fill_window rows per symbol before model is considered calibrated.
# 100 windows ≈ 25 hours at 4 windows/hour. Pre-live runs 6 weeks so this
# threshold is crossed within the first day of Module 1 running.
MIN_HISTORY_WINDOWS = 100

# Regime thresholds — Section 12.3 liquidity regime classifier + Section 9.2
# OI spike: current OI > rolling-mean * REGIME_OI_HIGH_MULT → low liquidity
REGIME_OI_HIGH_MULT = 1.5
# |funding_rate| per 8H — aligned to Section 9.2 defined boundaries:
#   >= 0.003 (0.30%)  → extreme (Section 9.2: "overleveraged longs/shorts — flag caution")
#                       → low liquidity classification
#   <= 0.0005 (0.05%) → near-zero (Section 9.2: "balanced market")
#                       → high liquidity candidate
#   0.05%–0.30%       → moderate (Section 9.2: "normal processing — cost factored in")
#                       → normal regime
REGIME_FUNDING_LOW_LIQ  = 0.003
REGIME_FUNDING_HIGH_LIQ = 0.0005

# Rolling window for historical statistics (rows of orderbook_snapshots)
# 672 rows = 7 days at 4 snapshots/hour * 24h
ROLLING_WINDOW_ROWS = 672

# Spread thresholds for fill probability penalty (in basis points)
SPREAD_DEGRADED_BPS = 10.0   # above this → fill probability reduced
SPREAD_EXTREME_BPS  = 30.0   # above this → fill probability severely reduced


@dataclass
class SlippageEstimate:
    symbol:                str
    timestamp:             int
    regime:                str    # 'high_liquidity' | 'normal' | 'low_liquidity'
    current_spread_bps:    float  # live bid-ask spread in basis points
    estimated_slippage_bps: float # p50 empirical VWAP-vs-mark deviation in this regime (bps)
    fill_probability_4h:   float  # estimated probability of limit fill within 4H (0.0–1.0)
    regime_slip_p50_bps:   float  # p50 of empirical fill-vs-mid deviation in this regime
    regime_slip_p95_bps:   float  # p95 of empirical fill-vs-mid deviation in this regime
    data_points:           int    # total fill_window rows in rolling window
    regime_data_points:    int    # regime-matched deviation points used for p50/p95
                                  # 0 = fallback to spread proxy despite is_calibrated
    is_calibrated:         bool   # False if data_points < MIN_HISTORY_WINDOWS


class SlippageModel:
    """
    Regime-dependent slippage estimator from self-collected Delta Exchange data.

    Usage (direct import by Module 5 / Module 6):
        model = SlippageModel()
        model.update_cache()              # call once on startup, then every 15 min
        est = model.get_estimate('BTCUSD')

    Usage (standalone service):
        asyncio.run(SlippageModel().run())
    """

    def __init__(self) -> None:
        self._cache: Dict[str, SlippageEstimate] = {}
        self._scheduler: Optional[AsyncIOScheduler] = None

    # ── Public query API ────────────────────────────────────────────────────────

    def get_estimate(self, symbol: str) -> Optional[SlippageEstimate]:
        """
        Return latest calibrated SlippageEstimate for symbol, or None if not calibrated.
        Returns None during pre-live (< MIN_HISTORY_WINDOWS data points) per Section 12.2.
        Callers must handle None — do not use uncalibrated estimates in live decisions.
        """
        est = self._cache.get(symbol)
        if est is None or not est.is_calibrated:
            return None
        return est

    def get_raw_estimate(self, symbol: str) -> Optional[SlippageEstimate]:
        """Return the cached estimate regardless of calibration state — for diagnostics only."""
        return self._cache.get(symbol)

    def get_all_estimates(self) -> Dict[str, SlippageEstimate]:
        """Return calibrated estimates only. Symbols with uncalibrated state are excluded."""
        return {sym: est for sym, est in self._cache.items() if est.is_calibrated}

    # ── Cache update ────────────────────────────────────────────────────────────

    def update_cache(self) -> None:
        """
        Recalculate slippage estimates for all symbols from latest DB data.
        Writes results to events table and updates in-memory cache.
        Called every 15 minutes after Module 1's snapshot job.
        """
        for symbol in DELTA_SYMBOLS:
            try:
                estimate = self._calculate_estimate(symbol)
                self._cache[symbol] = estimate
                self._persist_estimate(estimate)
            except Exception:
                logger.exception('estimate failed for %s', symbol)
                log_event(MODULE, 'error', 'error',
                          f'{symbol}: slippage estimate calculation failed')

        calibrated = sum(1 for e in self._cache.values() if e.is_calibrated)
        log_event(MODULE, 'info', 'heartbeat',
                  f'slippage cache updated — {calibrated}/{len(DELTA_SYMBOLS)} symbols calibrated')

    # ── Core estimation ─────────────────────────────────────────────────────────

    def _calculate_estimate(self, symbol: str) -> SlippageEstimate:
        now = int(time.time())

        # Latest orderbook snapshot — current spread, OI, funding rate
        with get_connection() as conn:
            snap = conn.execute(
                """SELECT best_bid, best_ask, spread, mark_price,
                          open_interest, funding_rate
                   FROM orderbook_snapshots
                   WHERE symbol = ?
                   ORDER BY timestamp DESC LIMIT 1""",
                (symbol,),
            ).fetchone()

        if snap is None:
            return self._uncalibrated(symbol, now)

        mark_price = snap['mark_price'] or (
            ((snap['best_bid'] or 0) + (snap['best_ask'] or 0)) / 2
        )
        if not mark_price or mark_price <= 0:
            return self._uncalibrated(symbol, now)

        raw_spread       = snap['spread'] or 0.0
        current_spread_bps = (raw_spread / mark_price) * 10_000
        oi               = snap['open_interest'] or 0.0
        funding_rate     = snap['funding_rate']  or 0.0

        # Rolling OI mean over last ROLLING_WINDOW_ROWS snapshots
        with get_connection() as conn:
            oi_rows = conn.execute(
                """SELECT open_interest FROM orderbook_snapshots
                   WHERE symbol = ? AND open_interest IS NOT NULL
                   ORDER BY timestamp DESC LIMIT ?""",
                (symbol, ROLLING_WINDOW_ROWS),
            ).fetchall()

        oi_values       = [r['open_interest'] for r in oi_rows if r['open_interest']]
        rolling_oi_mean = (sum(oi_values) / len(oi_values)) if oi_values else oi

        # Current regime
        regime = self._classify_regime(oi, rolling_oi_mean, funding_rate)

        # Fill_window rows with timestamp — used for calibration count, fill
        # activity, and VWAP-vs-mark deviation (Section 12.3: "historical fill
        # data vs mid-price → slippage distribution per asset per market condition")
        with get_connection() as conn:
            fw_rows = conn.execute(
                """SELECT json_extract(data, '$.vwap')            AS vwap,
                          json_extract(data, '$.taker_buy_ratio') AS taker_buy_ratio,
                          json_extract(data, '$.fill_count')      AS fill_count,
                          timestamp
                   FROM events
                   WHERE event_type = 'fill_window'
                     AND json_extract(data, '$.symbol') = ?
                   ORDER BY timestamp DESC LIMIT ?""",
                (symbol, ROLLING_WINDOW_ROWS),
            ).fetchall()

        data_points   = len(fw_rows)
        is_calibrated = data_points >= MIN_HISTORY_WINDOWS

        # Historical snapshots for regime-filtered deviation calculation
        with get_connection() as conn:
            snap_rows = conn.execute(
                """SELECT timestamp, mark_price, open_interest, funding_rate
                   FROM orderbook_snapshots
                   WHERE symbol = ?
                     AND mark_price IS NOT NULL
                     AND mark_price > 0
                   ORDER BY timestamp DESC LIMIT ?""",
                (symbol, ROLLING_WINDOW_ROWS),
            ).fetchall()

        # Match each fill_window to its nearest orderbook_snapshot by timestamp,
        # then compute |VWAP - mark_price| / mark_price * 10_000 in basis points.
        # Filter matched pairs to those whose snapshot falls in the current regime.
        # This produces the empirical fill-vs-mid slippage distribution per Section 12.3.
        regime_deviations_bps = self._compute_fill_deviations(
            fw_rows, snap_rows, regime, rolling_oi_mean
        )

        regime_data_points = len(regime_deviations_bps)

        if regime_deviations_bps:
            s   = sorted(regime_deviations_bps)
            n   = len(s)
            p50 = s[int((n - 1) * 0.50)]
            p95 = s[int((n - 1) * 0.95)]
        else:
            # No regime-matched data — fall back to half-spread as conservative proxy.
            # regime_data_points=0 signals to callers that this is fallback, not empirical.
            p50 = current_spread_bps * 0.5
            p95 = current_spread_bps

        estimated_slippage_bps = p50

        fill_prob = self._estimate_fill_probability(
            current_spread_bps, regime, fw_rows
        )

        return SlippageEstimate(
            symbol=symbol,
            timestamp=now,
            regime=regime,
            current_spread_bps=round(current_spread_bps, 4),
            estimated_slippage_bps=round(estimated_slippage_bps, 4),
            fill_probability_4h=round(fill_prob, 4),
            regime_slip_p50_bps=round(p50, 4),
            regime_slip_p95_bps=round(p95, 4),
            data_points=data_points,
            regime_data_points=regime_data_points,
            is_calibrated=is_calibrated,
        )

    # ── Regime classification ───────────────────────────────────────────────────

    @staticmethod
    def _classify_regime(
        oi: float, rolling_oi_mean: float, funding_rate: float
    ) -> str:
        """
        Classify liquidity regime per Section 12.3 and Section 9.2.

        Low liquidity:  OI spike above rolling mean, OR extreme funding rate.
        High liquidity: OI near mean AND funding rate near zero.
        Normal:         everything else.
        """
        abs_fr      = abs(funding_rate)
        oi_elevated = (rolling_oi_mean > 0 and oi > rolling_oi_mean * REGIME_OI_HIGH_MULT)

        if oi_elevated or abs_fr >= REGIME_FUNDING_LOW_LIQ:
            return 'low_liquidity'
        if not oi_elevated and abs_fr <= REGIME_FUNDING_HIGH_LIQ:
            return 'high_liquidity'
        return 'normal'

    # ── Fill-vs-mid deviation ───────────────────────────────────────────────────

    def _compute_fill_deviations(
        self,
        fw_rows: list,
        snap_rows: list,
        target_regime: str,
        rolling_oi_mean: float,
    ) -> List[float]:
        """
        Match each fill_window row to the nearest orderbook_snapshot by timestamp.
        Compute |VWAP - mark_price| / mark_price * 10_000 (deviation in bps).
        Return deviations only for windows whose matched snapshot falls in target_regime.

        This implements Section 12.3: "Historical fill data vs mid-price →
        Slippage distribution per asset per market condition."
        """
        if not fw_rows or not snap_rows:
            return []

        # Build ascending sorted list of snapshot timestamps for bisect
        snaps_asc = sorted(snap_rows, key=lambda r: r['timestamp'])
        snap_ts_asc = [r['timestamp'] for r in snaps_asc]

        deviations: List[float] = []
        for fw in fw_rows:
            vwap = fw['vwap']
            if vwap is None:
                continue
            try:
                vwap = float(vwap)
            except (TypeError, ValueError):
                continue

            fw_ts = fw['timestamp']

            # Find nearest snapshot by timestamp using binary search
            idx = bisect.bisect_left(snap_ts_asc, fw_ts)
            candidates = []
            if idx < len(snap_ts_asc):
                candidates.append(idx)
            if idx > 0:
                candidates.append(idx - 1)
            if not candidates:
                continue

            best = min(candidates, key=lambda i: abs(snap_ts_asc[i] - fw_ts))
            matched_snap = snaps_asc[best]

            mark = matched_snap['mark_price']
            if not mark or mark <= 0:
                continue

            row_oi = matched_snap['open_interest'] or 0.0
            row_fr = matched_snap['funding_rate']  or 0.0
            if self._classify_regime(row_oi, rolling_oi_mean, row_fr) != target_regime:
                continue

            deviation_bps = abs(vwap - mark) / mark * 10_000
            deviations.append(deviation_bps)

        return deviations

    # ── Fill probability ────────────────────────────────────────────────────────

    @staticmethod
    def _estimate_fill_probability(
        current_spread_bps: float,
        regime: str,
        fw_rows: list,
    ) -> float:
        """
        Estimate probability of a limit order filling within 4H.

        Base probability is regime-driven. Penalties applied for extreme spreads
        and thin fill activity (low fill count per window).
        Fill data is fill_window aggregates only — raw ticks do not exist.
        """
        base = {
            'high_liquidity': 0.85,
            'normal':         0.75,
            'low_liquidity':  0.55,
        }
        prob = base.get(regime, 0.75)

        # Spread penalty
        if current_spread_bps >= SPREAD_EXTREME_BPS:
            prob *= 0.60
        elif current_spread_bps >= SPREAD_DEGRADED_BPS:
            prob *= 0.85

        # Fill activity adjustment
        if fw_rows:
            avg_fills = sum((r['fill_count'] or 0) for r in fw_rows) / len(fw_rows)
            if avg_fills < 5:
                prob *= 0.80    # very thin market
            elif avg_fills > 50:
                prob = min(prob * 1.05, 0.95)   # active market, small boost

        return round(min(max(prob, 0.10), 0.95), 4)

    # ── Helpers ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _uncalibrated(symbol: str, now: int) -> SlippageEstimate:
        """Return a zeroed-out uncalibrated estimate when no DB data exists."""
        return SlippageEstimate(
            symbol=symbol, timestamp=now, regime='normal',
            current_spread_bps=0.0, estimated_slippage_bps=0.0,
            fill_probability_4h=0.0, regime_slip_p50_bps=0.0,
            regime_slip_p95_bps=0.0, data_points=0, regime_data_points=0,
            is_calibrated=False,
        )

    def _persist_estimate(self, est: SlippageEstimate) -> None:
        """Write estimate to events table for audit trail and cross-module access."""
        log_event(
            MODULE, 'debug', 'slippage_estimate',
            (f'{est.symbol}: regime={est.regime} '
             f'spread={est.current_spread_bps:.2f}bps '
             f'slip_p50={est.regime_slip_p50_bps:.2f}bps '
             f'slip_p95={est.regime_slip_p95_bps:.2f}bps '
             f'fill_p={est.fill_probability_4h:.2f} '
             f'calibrated={est.is_calibrated}'),
            asdict(est),
        )

    # ── Standalone runner ───────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Run slippage model as a standalone service.
        Updates cache every 15 minutes — same cadence as Module 1's snapshot job.
        """
        log_event(MODULE, 'info', 'heartbeat', 'slippage model service starting')
        self._scheduler = AsyncIOScheduler(timezone='UTC')
        self._scheduler.add_job(
            self._job_update,
            'interval',
            minutes=15,
            id='slippage_update',
            max_instances=1,
        )
        self._scheduler.start()

        # Run immediately on startup
        await asyncio.get_running_loop().run_in_executor(None, self.update_cache)

        try:
            while True:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def _job_update(self) -> None:
        await asyncio.get_running_loop().run_in_executor(None, self.update_cache)

    async def shutdown(self) -> None:
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        log_event(MODULE, 'info', 'info', 'slippage model service stopped')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(SlippageModel().run())
