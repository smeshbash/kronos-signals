"""
Kronos Trading System -- Module 11: System Health Monitor
Sections 14.2, 20.6, 19.1 (item 7) of the spec.

Runs in a 5-minute blocking loop (managed by Supervisord -- not APScheduler).
Checks all 10 other modules for staleness, anomalies, and performance degradation.

Checks performed every cycle (collect_health_status):
  1. heartbeat freshness       -- Module 1 data_collection (stale > 30 min -> restart)
  2. slippage_estimate fresh   -- Module 2 slippage_model  (stale > 30 min -> restart)
  3. portfolio snapshot fresh  -- Module 8 portfolio_manager (stale > 5H -> alert)
  4. notifier cursor fresh     -- Module 10 notifier (cursor stale > 30 min -> alert)
  5. orphaned orders           -- Module 6 open trades with no position row > 5H (live only)
  6. forced_override frequency -- > 3 forced_overrides in 24H (critical alert)
  7. system win rate           -- S.19.1 item 7: win rate < 50% over 30d (forced_override)
  8. unresponded override      -- S.19.1 preamble: forced_override > 24H with no response
                                  (-> position_close_required; Module 11 cannot close orders)

Per-cycle side work:
  - Asset win rate exclusion (S.20.6): per-asset win rate < 50% over 30 days
    (min 5 trades, min 23-day spread) -> writes asset_exclusion{excluded:True} consumed by Module 4
  - Asset win rate reinstatement (S.20.6): per-asset win rate > 55% over 14 days
    (min 3 trades, min 10-day spread) -> writes asset_exclusion{excluded:False} consumed by Module 4

Event types written:
  health_check            -- every cycle (summary heartbeat for Module 11)
  module_stale            -- per stale module (consumed by Module 10 for Telegram alert)
  orphaned_order          -- per orphaned trade (consumed by Module 10 for Telegram alert)
  asset_exclusion         -- per excluded/reinstated symbol; payload keys 'symbol', 'excluded'
                             consumed by Module 4 _load_exclusions()
  forced_override         -- S.19.1 item 7: system-wide win rate below threshold
  position_close_required -- S.19.1 preamble: forced_override unresponded > 24H
                             (consumed by Module 10; actual order close requires Module 7)
  health_error            -- unhandled exception or forced_override frequency alert

Environment variables:
  KRONOS_PAPER_MODE                 -- true/false (default: false)
  KRONOS_SUPERVISORD_PREFIX         -- prefix for supervisorctl process names (default: kronos-)
  KRONOS_NOTIFIER_STATE_PATH        -- path to notifier_state.json
  KRONOS_HM_HEARTBEAT_STALE_S      -- heartbeat staleness threshold, seconds (default: 1800)
  KRONOS_HM_SLIPPAGE_STALE_S       -- slippage staleness threshold (default: 1800)
  KRONOS_HM_PORTFOLIO_STALE_S      -- portfolio snapshot staleness threshold (default: 18000)
  KRONOS_HM_NOTIFIER_STALE_S       -- notifier cursor staleness threshold (default: 1800)
  KRONOS_HM_ORPHAN_STALE_S         -- orphaned order age threshold (default: 18000)
  KRONOS_HM_FO_WINDOW_S            -- forced_override frequency window (default: 86400)
  KRONOS_HM_FO_MAX                 -- max forced_overrides in window before alert (default: 3)
"""

import json
import logging
import os
import subprocess
import time
from typing import Optional

from db import get_connection, init_db, log_event

log = logging.getLogger(__name__)
MODULE = 'health_monitor'

# ── Environment configuration ──────────────────────────────────────────────────

PAPER_MODE = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'

SUPERVISORD_PREFIX  = os.environ.get('KRONOS_SUPERVISORD_PREFIX', 'kronos-')
NOTIFIER_STATE_PATH = os.environ.get(
    'KRONOS_NOTIFIER_STATE_PATH',
    os.path.join(os.path.dirname(__file__), 'data', 'notifier_state.json'),
)

HEARTBEAT_STALE_S       = int(os.environ.get('KRONOS_HM_HEARTBEAT_STALE_S',  '1800'))
SLIPPAGE_STALE_S        = int(os.environ.get('KRONOS_HM_SLIPPAGE_STALE_S',   '1800'))
PORTFOLIO_SNAP_STALE_S  = int(os.environ.get('KRONOS_HM_PORTFOLIO_STALE_S', '18000'))
NOTIFIER_CURSOR_STALE_S = int(os.environ.get('KRONOS_HM_NOTIFIER_STALE_S',  '1800'))
ORPHAN_ORDER_STALE_S    = int(os.environ.get('KRONOS_HM_ORPHAN_STALE_S',   '18000'))
FO_WINDOW_S             = int(os.environ.get('KRONOS_HM_FO_WINDOW_S',      '86400'))
FO_MAX_COUNT            = int(os.environ.get('KRONOS_HM_FO_MAX',              '3'))

WIN_RATE_PERIOD_DAYS = 30
WIN_RATE_THRESHOLD   = 0.50
WIN_RATE_MIN_TRADES  = 5   # kept at 5 per §20.6 practical floor; span check handles false positives
EXCLUSION_DEDUP_S    = 86400   # 24H -- don't re-exclude same symbol within this window

# Span guard: oldest trade must fall at least this many days before now_ts so
# a cluster of recent trades cannot trigger a 30-day or 14-day threshold.
WIN_RATE_MIN_SPAN_DAYS        = 23   # 30-day check: oldest trade must be >= 23d old
REINSTATEMENT_PERIOD_DAYS     = 14
REINSTATEMENT_THRESHOLD       = 0.55
REINSTATEMENT_MIN_SPAN_DAYS   = 10   # 14-day check: oldest trade must be >= 10d old
REINSTATEMENT_MIN_TRADES      = 3

# Unresponded forced_override: raise position_close_required after this window (S.19.1 preamble)
UNRESPONDED_OVERRIDE_WINDOW_S = 86400  # 24H

# Only stateless modules are safe to auto-restart (no in-memory APScheduler jobs lost).
_AUTO_RESTART_PROCESSES = {
    'data_collection': 'data-collection',
    'slippage_model':  'slippage-model',
}

# Staleness check names that write module_stale events
_STALE_CHECKS = frozenset({
    'heartbeat_stale', 'slippage_estimate_stale',
    'portfolio_snap_stale', 'notifier_cursor_stale',
    'notifier_state_missing',
})


# ── Main class ─────────────────────────────────────────────────────────────────

class HealthMonitor:
    """
    Module 11 -- System Health Monitor.

    collect_health_status() is synchronous, side-effect-free, fully testable.
    _run_cycle() calls it, then writes events and triggers restarts.
    run() loops every 5 minutes (Supervisord manages process restarts).
    """

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _get_latest_event_ts(self, event_type: str) -> Optional[int]:
        """Return timestamp of the most recent event of the given type, or None."""
        with get_connection() as conn:
            row = conn.execute(
                'SELECT timestamp FROM events WHERE event_type=? '
                'ORDER BY id DESC LIMIT 1',
                (event_type,)
            ).fetchone()
        return int(row['timestamp']) if row else None

    # ── Checks (all return list[dict] -- empty = no issue) ────────────────────

    def _check_event_freshness(
        self, event_type: str, stale_s: int, module_name: str, now_ts: int
    ) -> list[dict]:
        """Return issue if the most recent event of event_type is absent or stale."""
        last_ts = self._get_latest_event_ts(event_type)
        if last_ts is None:
            return [{
                'module': module_name,
                'check': f'{event_type}_stale',
                'message': f'No {event_type} event found -- module may not have started',
                'severity': 'warning',
                'stale_s': stale_s,
            }]
        age = now_ts - last_ts
        if age > stale_s:
            return [{
                'module': module_name,
                'check': f'{event_type}_stale',
                'message': (f'{module_name} stale -- last {event_type} '
                            f'{age // 60}m ago (threshold {stale_s // 60}m)'),
                'severity': 'warning',
                'age_s': age,
                'stale_s': stale_s,
            }]
        return []

    def _check_portfolio_snapshots(self, now_ts: int) -> list[dict]:
        """Return issue if the most recent portfolio_snapshots row is absent or stale."""
        with get_connection() as conn:
            row = conn.execute(
                'SELECT timestamp FROM portfolio_snapshots ORDER BY id DESC LIMIT 1'
            ).fetchone()
        if row is None:
            return [{
                'module': 'portfolio_manager',
                'check': 'portfolio_snap_stale',
                'message': 'No portfolio snapshots found -- portfolio_manager may not have run',
                'severity': 'warning',
                'stale_s': PORTFOLIO_SNAP_STALE_S,
            }]
        age = now_ts - int(row['timestamp'])
        if age > PORTFOLIO_SNAP_STALE_S:
            return [{
                'module': 'portfolio_manager',
                'check': 'portfolio_snap_stale',
                'message': (f'portfolio_manager stale -- last snapshot '
                            f'{age // 3600:.1f}h ago (threshold {PORTFOLIO_SNAP_STALE_S // 3600}h)'),
                'severity': 'warning',
                'age_s': age,
                'stale_s': PORTFOLIO_SNAP_STALE_S,
            }]
        return []

    def _check_notifier_cursor(self, now_ts: int) -> list[dict]:
        """
        Return issue if Module 10's event cursor has stalled while new events exist.
        Reads last_event_id from NOTIFIER_STATE_PATH; compares with oldest unprocessed event.
        """
        try:
            with open(NOTIFIER_STATE_PATH, encoding='utf-8') as fh:
                last_id = int(json.load(fh).get('last_event_id', 0))
        except FileNotFoundError:
            return [{
                'module': 'notifier',
                'check': 'notifier_state_missing',
                'message': f'Notifier state file not found at {NOTIFIER_STATE_PATH}',
                'severity': 'warning',
            }]
        except (ValueError, KeyError, json.JSONDecodeError):
            return [{
                'module': 'notifier',
                'check': 'notifier_state_missing',
                'message': 'Notifier state file is corrupt or unreadable',
                'severity': 'warning',
            }]

        with get_connection() as conn:
            oldest = conn.execute(
                'SELECT id, timestamp FROM events WHERE id > ? ORDER BY id ASC LIMIT 1',
                (last_id,)
            ).fetchone()

        if oldest is None:
            return []   # cursor is current

        age = now_ts - int(oldest['timestamp'])
        if age > NOTIFIER_CURSOR_STALE_S:
            return [{
                'module': 'notifier',
                'check': 'notifier_cursor_stale',
                'message': (f'Notifier cursor stale -- oldest unprocessed event '
                            f'(id={oldest["id"]}) is {age // 60}m old '
                            f'(threshold {NOTIFIER_CURSOR_STALE_S // 60}m)'),
                'severity': 'warning',
                'age_s': age,
                'stale_s': NOTIFIER_CURSOR_STALE_S,
                'last_event_id': last_id,
            }]
        return []

    def _check_orphaned_orders(self, now_ts: int) -> list[dict]:
        """
        Return one issue per open trade that has no corresponding position row and
        is older than ORPHAN_ORDER_STALE_S.  These are Module 6 orders where the
        APScheduler 4H timeout job was lost on restart (live mode only).
        """
        cutoff = now_ts - ORPHAN_ORDER_STALE_S
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT t.id, t.entry_timestamp, t.symbol "
                "FROM trades t "
                "LEFT JOIN positions p ON p.trade_id = t.id "
                "WHERE t.status = 'open' AND p.id IS NULL "
                "AND t.entry_timestamp IS NOT NULL AND t.entry_timestamp < ? "
                "ORDER BY t.entry_timestamp ASC",
                (cutoff,)
            ).fetchall()
        issues = []
        for row in rows:
            age_s = now_ts - int(row['entry_timestamp'])
            issues.append({
                'module': 'execution',
                'check': 'orphaned_order',
                'message': (f'Orphaned order -- trade_id={row["id"]} '
                            f'symbol={row["symbol"] or "?"} '
                            f'age={age_s / 3600:.1f}h'),
                'severity': 'warning',
                'trade_id': row['id'],
                'symbol': row['symbol'] or '',
                'age_s': age_s,
                'age_hours': age_s / 3600,
            })
        return issues

    def _check_forced_override_frequency(self, now_ts: int) -> list[dict]:
        """
        Return critical issue if >= FO_MAX_COUNT forced_override events were written
        in the last FO_WINDOW_S (24H) -- indicates systemic instability.
        """
        cutoff = now_ts - FO_WINDOW_S
        with get_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM events "
                "WHERE event_type = 'forced_override' AND timestamp > ?",
                (cutoff,)
            ).fetchone()
        count = row['cnt'] if row else 0
        if count >= FO_MAX_COUNT:
            return [{
                'module': 'all_modules',
                'check': 'forced_override_frequency',
                'message': (f'{count} forced_override events in last '
                            f'{FO_WINDOW_S // 3600}H (max {FO_MAX_COUNT})'),
                'severity': 'critical',
                'fo_count': count,
                'window_s': FO_WINDOW_S,
            }]
        return []

    def _check_system_win_rate(self, now_ts: int) -> list[dict]:
        """
        S.19.1 item 7: system-wide win rate < 50% over last WIN_RATE_PERIOD_DAYS days
        (min WIN_RATE_MIN_TRADES closed trades, min WIN_RATE_MIN_SPAN_DAYS calendar spread).
        Span guard prevents a cluster of recent trades from triggering the 30-day threshold.
        Returns critical issue so _run_cycle() can write forced_override.
        """
        cutoff = now_ts - WIN_RATE_PERIOD_DAYS * 86400
        with get_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN pnl_gross > 0 THEN 1 ELSE 0 END) AS wins, "
                "MIN(exit_timestamp) AS oldest_ts "
                "FROM trades WHERE status = 'closed' AND exit_timestamp > ? "
                "AND quality_flag IS NULL",
                (cutoff,)
            ).fetchone()
        if row is None:
            return []
        total     = row['total'] or 0
        wins      = row['wins']  or 0
        oldest_ts = row['oldest_ts']
        if total < WIN_RATE_MIN_TRADES:
            return []
        # Span guard: oldest trade must be at least WIN_RATE_MIN_SPAN_DAYS old
        if oldest_ts is None or oldest_ts > now_ts - WIN_RATE_MIN_SPAN_DAYS * 86400:
            return []
        win_rate = wins / total
        if win_rate >= WIN_RATE_THRESHOLD:
            return []
        return [{
            'module': 'all_modules',
            'check': 'system_win_rate',
            'message': (f'System win rate {win_rate:.1%} below {WIN_RATE_THRESHOLD:.0%} '
                        f'over {WIN_RATE_PERIOD_DAYS}d ({total} trades)'),
            'severity': 'critical',
            'win_rate': win_rate,
            'trade_count': total,
            'wins': wins,
        }]

    def _check_unresponded_override(self, now_ts: int) -> list[dict]:
        """
        S.19.1 preamble: if the most recent win-rate forced_override is older than
        UNRESPONDED_OVERRIDE_WINDOW_S and no forced_override_cleared follows it,
        return a critical issue so _run_cycle() can write position_close_required.

        Deduped: only fires once per override instance (skips if position_close_required
        was already written after the forced_override timestamp).
        Module 11 cannot place close orders -- actual position closure requires Module 7.
        """
        with get_connection() as conn:
            fo_row = conn.execute(
                "SELECT id, timestamp FROM events "
                "WHERE event_type = 'forced_override' "
                "ORDER BY id DESC LIMIT 1",
            ).fetchone()
        if fo_row is None:
            return []
        fo_ts = int(fo_row['timestamp'])
        fo_id = fo_row['id']
        if now_ts - fo_ts < UNRESPONDED_OVERRIDE_WINDOW_S:
            return []
        with get_connection() as conn:
            cleared = conn.execute(
                "SELECT id FROM events WHERE event_type = 'forced_override_cleared' "
                "AND timestamp > ? LIMIT 1",
                (fo_ts,)
            ).fetchone()
        if cleared:
            return []
        # Dedup: skip if position_close_required was already written for this override
        with get_connection() as conn:
            already = conn.execute(
                "SELECT id FROM events WHERE event_type = 'position_close_required' "
                "AND timestamp > ? LIMIT 1",
                (fo_ts,)
            ).fetchone()
        if already:
            return []
        age_h = (now_ts - fo_ts) / 3600
        return [{
            'module': 'all_modules',
            'check': 'unresponded_override',
            'message': (f'forced_override unresponded for {age_h:.1f}h '
                        f'(event id={fo_id}) -- manual position close required'),
            'severity': 'critical',
            'fo_id': fo_id,
            'fo_ts': fo_ts,
            'age_h': age_h,
        }]

    # ── Public synchronous health aggregator ──────────────────────────────────

    def collect_health_status(self, now_ts: int = None) -> list[dict]:
        """
        Run all health checks synchronously.  No DB writes, no side effects.
        Returns list of issue dicts -- each has module, check, message, severity.
        """
        if now_ts is None:
            now_ts = int(time.time())

        issues: list[dict] = []
        issues.extend(self._check_event_freshness(
            'heartbeat', HEARTBEAT_STALE_S, 'data_collection', now_ts))
        issues.extend(self._check_event_freshness(
            'slippage_estimate', SLIPPAGE_STALE_S, 'slippage_model', now_ts))
        issues.extend(self._check_portfolio_snapshots(now_ts))
        issues.extend(self._check_notifier_cursor(now_ts))
        if not PAPER_MODE:
            issues.extend(self._check_orphaned_orders(now_ts))
        issues.extend(self._check_forced_override_frequency(now_ts))
        issues.extend(self._check_system_win_rate(now_ts))
        issues.extend(self._check_unresponded_override(now_ts))
        return issues

    # ── Asset win rate exclusion (S.20.6) ─────────────────────────────────────

    def check_asset_win_rates(self) -> list[str]:
        """
        S.20.6: per-asset win rate check.  Writes asset_exclusion events (excluded=True)
        for symbols with win rate < WIN_RATE_THRESHOLD over the last WIN_RATE_PERIOD_DAYS
        days (min WIN_RATE_MIN_TRADES trades, min WIN_RATE_MIN_SPAN_DAYS calendar spread).
        Deduped: same symbol not re-excluded within EXCLUSION_DEDUP_S (24H).
        excluded=True is required by Module 4 _load_exclusions() to activate exclusion.
        Returns list of newly excluded symbol names.
        """
        now_ts       = int(time.time())
        cutoff       = now_ts - WIN_RATE_PERIOD_DAYS * 86400
        dedup_cutoff = now_ts - EXCLUSION_DEDUP_S

        with get_connection() as conn:
            rows = conn.execute(
                "SELECT symbol, COUNT(*) AS total, "
                "SUM(CASE WHEN pnl_gross > 0 THEN 1 ELSE 0 END) AS wins, "
                "MIN(exit_timestamp) AS oldest_ts "
                "FROM trades WHERE status = 'closed' AND exit_timestamp > ? "
                "AND quality_flag IS NULL "
                "GROUP BY symbol",
                (cutoff,)
            ).fetchall()

        excluded: list[str] = []
        for row in rows:
            symbol    = row['symbol']
            total     = row['total'] or 0
            wins      = row['wins']  or 0
            oldest_ts = row['oldest_ts']
            if total < WIN_RATE_MIN_TRADES:
                continue
            # Span guard: oldest trade must be at least WIN_RATE_MIN_SPAN_DAYS old
            if oldest_ts is None or oldest_ts > now_ts - WIN_RATE_MIN_SPAN_DAYS * 86400:
                continue
            win_rate = wins / total
            if win_rate >= WIN_RATE_THRESHOLD:
                continue

            # Dedup: skip if the same symbol was excluded within the last 24H
            with get_connection() as conn:
                recent = conn.execute(
                    "SELECT id FROM events WHERE event_type = 'asset_exclusion' "
                    "AND json_extract(data, '$.symbol') = ? "
                    "AND json_extract(data, '$.excluded') = 1 "
                    "AND timestamp > ? LIMIT 1",
                    (symbol, dedup_cutoff)
                ).fetchone()
            if recent:
                continue

            log_event(MODULE, 'warning', 'asset_exclusion',
                      f'Asset {symbol} excluded: win rate {win_rate:.1%} over '
                      f'{WIN_RATE_PERIOD_DAYS}d ({total} trades)',
                      {'symbol': symbol,
                       'excluded': True,
                       'reason': 'win_rate_below_50pct_30d',
                       'win_rate': round(win_rate, 4),
                       'trade_count': total,
                       'wins': wins,
                       'timestamp': now_ts})
            log.warning('Asset exclusion written: %s (win rate %.1f%%, %d trades)',
                        symbol, win_rate * 100, total)
            excluded.append(symbol)
        return excluded

    def check_asset_reinstatements(self) -> list[str]:
        """
        S.20.6 reinstatement: writes asset_exclusion{excluded:False} for symbols whose
        per-asset win rate > REINSTATEMENT_THRESHOLD over REINSTATEMENT_PERIOD_DAYS
        (min REINSTATEMENT_MIN_TRADES trades, min REINSTATEMENT_MIN_SPAN_DAYS spread).
        Only fires if the symbol's most recent asset_exclusion event has excluded=True.
        Module 4 _load_exclusions() processes excluded=False as reinstatement (docstring
        confirmed: 'Re-instatement: same structure with excluded=False').
        Returns list of newly reinstated symbol names.
        """
        now_ts = int(time.time())
        cutoff = now_ts - REINSTATEMENT_PERIOD_DAYS * 86400

        with get_connection() as conn:
            rows = conn.execute(
                "SELECT symbol, COUNT(*) AS total, "
                "SUM(CASE WHEN pnl_gross > 0 THEN 1 ELSE 0 END) AS wins, "
                "MIN(exit_timestamp) AS oldest_ts "
                "FROM trades WHERE status = 'closed' AND exit_timestamp > ? "
                "AND quality_flag IS NULL "
                "GROUP BY symbol",
                (cutoff,)
            ).fetchall()

        reinstated: list[str] = []
        for row in rows:
            symbol    = row['symbol']
            total     = row['total'] or 0
            wins      = row['wins']  or 0
            oldest_ts = row['oldest_ts']
            if total < REINSTATEMENT_MIN_TRADES:
                continue
            # Span guard: oldest trade must be at least REINSTATEMENT_MIN_SPAN_DAYS old
            if oldest_ts is None or oldest_ts > now_ts - REINSTATEMENT_MIN_SPAN_DAYS * 86400:
                continue
            win_rate = wins / total
            if win_rate <= REINSTATEMENT_THRESHOLD:
                continue

            # Only reinstate if symbol is currently excluded (most recent event excluded=True)
            with get_connection() as conn:
                latest = conn.execute(
                    "SELECT json_extract(data, '$.excluded') AS excluded_flag "
                    "FROM events WHERE event_type = 'asset_exclusion' "
                    "AND json_extract(data, '$.symbol') = ? "
                    "ORDER BY id DESC LIMIT 1",
                    (symbol,)
                ).fetchone()
            if latest is None:
                continue  # never excluded, nothing to reinstate
            # json_extract returns 1/0 for JSON booleans in SQLite
            if latest['excluded_flag'] not in (True, 1):
                continue

            # Dedup: skip if a reinstatement event was already written within 24H
            with get_connection() as conn:
                recent_reinstate = conn.execute(
                    "SELECT id FROM events WHERE event_type = 'asset_exclusion' "
                    "AND json_extract(data, '$.symbol') = ? "
                    "AND json_extract(data, '$.excluded') = 0 "
                    "AND timestamp > ? LIMIT 1",
                    (symbol, now_ts - EXCLUSION_DEDUP_S),
                ).fetchone()
            if recent_reinstate:
                continue

            log_event(MODULE, 'info', 'asset_exclusion',
                      f'Asset {symbol} reinstated: win rate {win_rate:.1%} over '
                      f'{REINSTATEMENT_PERIOD_DAYS}d ({total} trades)',
                      {'symbol': symbol,
                       'excluded': False,
                       'reason': 'win_rate_above_55pct_14d',
                       'win_rate': round(win_rate, 4),
                       'trade_count': total,
                       'wins': wins,
                       'timestamp': now_ts})
            log.info('Asset reinstatement written: %s (win rate %.1f%%, %d trades)',
                     symbol, win_rate * 100, total)
            reinstated.append(symbol)
        return reinstated

    # ── Supervisord restart ────────────────────────────────────────────────────

    def _try_supervisord_restart(self, module_name: str) -> bool:
        """
        Attempt restart of module_name via supervisorctl.
        Only called for modules in _AUTO_RESTART_PROCESSES.
        Returns True on success (exit code 0).
        """
        if module_name not in _AUTO_RESTART_PROCESSES:
            return False
        process = f'{SUPERVISORD_PREFIX}{_AUTO_RESTART_PROCESSES[module_name]}'
        try:
            result = subprocess.run(
                ['supervisorctl', 'restart', process],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                log.info('supervisorctl restarted %s', process)
                return True
            log.warning('supervisorctl restart %s failed: %s', process, result.stderr.strip())
            return False
        except Exception as exc:
            log.error('supervisorctl restart error for %s: %s', process, exc)
            return False

    # ── Cycle logic ────────────────────────────────────────────────────────────

    def _fo_win_rate_active(self) -> bool:
        """
        Return True if a win-rate forced_override is currently active -- i.e., the
        most recent win_rate_below_50pct_30d forced_override has no subsequent
        forced_override_cleared event.  Prevents duplicate forced_override events
        while the override is still in effect (§19.1 item 7).
        """
        with get_connection() as conn:
            fo_row = conn.execute(
                "SELECT id, timestamp FROM events "
                "WHERE event_type = 'forced_override' "
                "AND json_extract(data, '$.reason') = 'win_rate_below_50pct_30d' "
                "ORDER BY id DESC LIMIT 1",
            ).fetchone()
        if fo_row is None:
            return False
        fo_ts = int(fo_row['timestamp'])
        with get_connection() as conn:
            cleared = conn.execute(
                "SELECT id FROM events WHERE event_type = 'forced_override_cleared' "
                "AND timestamp > ? LIMIT 1",
                (fo_ts,)
            ).fetchone()
        return cleared is None

    def _run_cycle(self) -> None:
        """Execute one health cycle: check -> write events -> attempt restarts."""
        now_ts = int(time.time())
        try:
            issues = self.collect_health_status(now_ts)

            written_stale: set[str] = set()   # dedup module_stale per module per cycle

            for issue in issues:
                check    = issue['check']
                module   = issue['module']
                severity = issue['severity']
                message  = issue['message']

                if check in _STALE_CHECKS:
                    if module not in written_stale:
                        log_event(MODULE, severity, 'module_stale', message, issue)
                        written_stale.add(module)
                        if module in _AUTO_RESTART_PROCESSES:
                            self._try_supervisord_restart(module)

                elif check == 'orphaned_order':
                    log_event(MODULE, 'warning', 'orphaned_order', message,
                              {'trade_id':  issue.get('trade_id'),
                               'symbol':    issue.get('symbol', ''),
                               'age_hours': issue.get('age_hours', 0),
                               'timestamp': now_ts})

                elif check == 'system_win_rate':
                    if not self._fo_win_rate_active():
                        log_event(MODULE, 'critical', 'forced_override', message,
                                  {'reason':       'win_rate_below_50pct_30d',
                                   'win_rate':     issue.get('win_rate', 0),
                                   'trade_count':  issue.get('trade_count', 0),
                                   'period_days':  WIN_RATE_PERIOD_DAYS,
                                   'timestamp':    now_ts})
                        log.critical('forced_override written: system win rate below threshold')

                elif check == 'unresponded_override':
                    log_event(MODULE, 'critical', 'position_close_required', message,
                              {'fo_id':     issue.get('fo_id'),
                               'fo_ts':     issue.get('fo_ts'),
                               'age_h':     issue.get('age_h', 0),
                               'timestamp': now_ts})
                    log.critical('position_close_required written: fo_id=%s age=%.1fh',
                                 issue.get('fo_id'), issue.get('age_h', 0))

                elif check == 'forced_override_frequency':
                    log_event(MODULE, 'critical', 'health_error', message,
                              {'check':     check,
                               'fo_count':  issue.get('fo_count', 0),
                               'window_s':  FO_WINDOW_S,
                               'timestamp': now_ts})
                    log.critical('Forced override frequency alert: %s', message)

            # Per-asset win rate exclusions and reinstatements (S.20.6)
            self.check_asset_win_rates()
            self.check_asset_reinstatements()

            # health_check event every cycle (Module 11's own heartbeat)
            log_event(MODULE, 'info', 'health_check',
                      f'Health cycle complete -- {len(issues)} issue(s)',
                      {'timestamp':    now_ts,
                       'issue_count':  len(issues),
                       'checks_failed': [i['check'] for i in issues]})

        except Exception as exc:
            log.error('Health monitor cycle error: %s', exc, exc_info=True)
            try:
                log_event(MODULE, 'error', 'health_error',
                          f'Unhandled error in health cycle: {exc}',
                          {'error': str(exc), 'timestamp': int(time.time())})
            except Exception:
                pass

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Block forever in a 5-minute loop.
        Supervisord restarts the process on crash -- no APScheduler needed (S.14.2).
        """
        init_db()
        log.info('Health monitor starting (5-minute loop)...')
        while True:
            self._run_cycle()
            time.sleep(300)

    def stop(self) -> None:
        log.info('Health monitor stopping.')


def main() -> None:
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s -- %(message)s',
        stream=sys.stdout,
    )
    HealthMonitor().run()


if __name__ == '__main__':
    main()
