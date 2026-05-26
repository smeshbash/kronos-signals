"""
Kronos Trading System — Module 10: Notification
Sections 19.6, 14.2, 19.3 of the spec (v3.3).

Sends all alerts and summaries to human via Telegram (primary) and email backup.

Real-time event notifications (polled every 15 min):
  Trade fills:  paper_fill, order_filled, order_timeout, execution_error
  Exit events:  stop_loss_exit, take_profit_exit, time_limit_exit, funding_cost_exit
  Alerts:       alert_yellow, alert_orange, alert_red, alert_cleared
  Critical:     forced_override  — Telegram + Email simultaneously (§19.3)
  Strategy:     rr_alert_7d, win_rate_alert_7d
  Milestones:   recuperation_milestone
  Tax:          advance_tax_alert, annual_vda_report
  Monthly:      withdrawal_calculation  — triggers combined monthly summary
  Weekly:       slot3_selection (Sunday events only)

Scheduled (via job_queue):
  8H cyclic summary: 00:02 / 08:02 / 16:02 UTC — portfolio, positions, P&L,
    drawdown level, funding rate status (§19.6)

Telegram commands:
  /withdraw <amount>  — confirms human withdrawal; writes withdrawal_made (§11.4)
  /resume [reason]    — Option A resume after forced override; writes forced_override_cleared (§19.3)
  /halt [reason]      — Option C permanent halt; writes forced_override (§19.3)
  /status             — current system snapshot

Event types written: withdrawal_made, forced_override_cleared, notification_error

Environment variables required:
  KRONOS_TELEGRAM_BOT_TOKEN   — Telegram bot token
  KRONOS_TELEGRAM_CHAT_ID     — Telegram chat ID (integer)
  KRONOS_EMAIL_FROM           — Gmail sender address (forced override email)
  KRONOS_EMAIL_TO             — Recipient email address
  KRONOS_EMAIL_APP_PASSWORD   — Gmail app password
  KRONOS_NOTIFIER_STATE_PATH  — path to state JSON (default: data/notifier_state.json)
"""

import json
import logging
import os
import smtplib
import time
from datetime import datetime, time as dtime, timezone
from email.mime.text import MIMEText
from typing import Optional

from db import get_connection, init_db, log_event

log = logging.getLogger(__name__)
MODULE = 'notifier'

# ── Environment configuration ──────────────────────────────────────────────────

BOT_TOKEN      = os.environ.get('KRONOS_TELEGRAM_BOT_TOKEN', '')
CHAT_ID        = os.environ.get('KRONOS_TELEGRAM_CHAT_ID', '')
EMAIL_FROM     = os.environ.get('KRONOS_EMAIL_FROM', '')
EMAIL_TO       = os.environ.get('KRONOS_EMAIL_TO', '')
EMAIL_PASSWORD = os.environ.get('KRONOS_EMAIL_APP_PASSWORD', '')
STATE_PATH     = os.environ.get(
    'KRONOS_NOTIFIER_STATE_PATH',
    os.path.join(os.path.dirname(__file__), 'data', 'notifier_state.json'),
)
STARTING_CAPITAL_INR = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', '100000.0'))

# Events that trigger a Telegram notification when seen in the event poller
NOTIFY_EVENTS = frozenset({
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
})

# Events that ALSO send email backup simultaneously (§19.3)
EMAIL_EVENTS = frozenset({'forced_override'})

# Alert events: deduped within a short window to suppress rapid-fire spam
_ALERT_DEDUP_EVENTS = frozenset({'alert_yellow', 'alert_orange', 'alert_red', 'alert_cleared'})
_DEDUP_WINDOW_S = 300   # 5 minutes

# Strategy alert events: deduped within a long window — Module 5 writes
# win_rate_alert_7d every 4H cycle while the condition is active (up to 6/day)
_STRATEGY_DEDUP_EVENTS = frozenset({'win_rate_alert_7d', 'rr_alert_7d'})
_STRATEGY_DEDUP_WINDOW_S = 82800   # 23 hours

try:
    from telegram import Bot, Update
    from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
    _TELEGRAM_AVAILABLE = True
except ImportError:
    _TELEGRAM_AVAILABLE = False
    log.warning('python-telegram-bot not installed — Telegram sends disabled')


# ── Main class ─────────────────────────────────────────────────────────────────

class NotificationModule:
    """
    Module 10 — Notification.

    Core sync methods handle DB interaction and message composition.
    Async methods wrap the actual Telegram / email sends.
    `start()` builds and runs the python-telegram-bot Application which
    owns the asyncio event loop for the process lifetime.
    """

    def __init__(self) -> None:
        self._last_event_id: int     = self._load_last_event_id()
        self._last_sent_ts: dict     = {}   # event_type -> last send unix ts (dedup)
        self._app: Optional[object]  = None

    # ── State persistence ──────────────────────────────────────────────────────

    def _load_last_event_id(self) -> int:
        """Read last processed event ID from state file. Returns 0 if absent."""
        try:
            with open(STATE_PATH, encoding='utf-8') as fh:
                return int(json.load(fh).get('last_event_id', 0))
        except (FileNotFoundError, ValueError, KeyError, json.JSONDecodeError):
            return 0

    def _save_last_event_id(self, event_id: int) -> None:
        os.makedirs(os.path.dirname(STATE_PATH) or '.', exist_ok=True)
        with open(STATE_PATH, 'w', encoding='utf-8') as fh:
            json.dump({'last_event_id': event_id}, fh)

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _get_new_events(self) -> list[dict]:
        """Return all events with id > last_event_id, ordered oldest first."""
        with get_connection() as conn:
            rows = conn.execute(
                'SELECT id, event_type, data, timestamp FROM events '
                'WHERE id > ? ORDER BY id ASC',
                (self._last_event_id,)
            ).fetchall()
        return [dict(r) for r in rows]

    def _get_latest_event_payload(self, event_type: str) -> dict:
        """Return parsed payload of the most recent event of the given type."""
        with get_connection() as conn:
            row = conn.execute(
                'SELECT data FROM events WHERE event_type=? ORDER BY id DESC LIMIT 1',
                (event_type,)
            ).fetchone()
        if row and row['data']:
            try:
                return json.loads(row['data'])
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    @staticmethod
    def _parse_payload(event: dict) -> dict:
        data = event.get('data')
        if data:
            try:
                return json.loads(data)
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    # ── Message formatters ─────────────────────────────────────────────────────

    def _fmt_fill(self, p: dict, event_type: str) -> str:
        mode    = 'PAPER' if p.get('paper') else 'LIVE'
        symbol  = p.get('symbol', '?')
        direction = (p.get('direction') or '').upper()
        price   = p.get('fill_price') or p.get('entry_price') or p.get('mark_price', 0)
        size    = p.get('size_contracts', '?')
        return (f'[{mode}] Trade filled\n'
                f'{symbol} {direction}  size={size} contracts  price={price}')

    def _fmt_timeout(self, p: dict) -> str:
        symbol = p.get('symbol', '?')
        side   = p.get('direction', '').upper()
        return (f'Order timeout — {symbol} {side}\n'
                f'Order cancelled: no fill within 4H window.')

    def _fmt_execution_error(self, p: dict) -> str:
        return (f'Execution error\n'
                f'{p.get("error", "unknown error")}\n'
                f'Trade ID: {p.get("trade_id", "?")}')

    def _fmt_exit(self, p: dict, event_type: str) -> str:
        reason   = event_type.replace('_exit', '').replace('_', ' ').upper()
        symbol   = p.get('symbol', '?')
        direction = (p.get('direction') or '').upper()
        pnl      = p.get('pnl_gross', 0)
        sign     = '+' if pnl >= 0 else ''
        entry_px = p.get('entry_price', '?')
        exit_px  = p.get('exit_price', '?')
        mode     = ' [PAPER]' if p.get('paper') else ''
        msg      = (f'{reason} exit{mode}\n'
                    f'{symbol} {direction}  entry={entry_px}  exit={exit_px}\n'
                    f'P&L: {sign}INR {pnl:.2f}')
        if event_type == 'stop_loss_exit':
            blackout = p.get('blackout_until', 0)
            if blackout:
                dt = datetime.fromtimestamp(blackout, tz=timezone.utc)
                msg += f'\n4H blackout until {dt.strftime("%H:%M UTC")}'
        return msg

    def _fmt_alert(self, p: dict, event_type: str) -> str:
        dd     = p.get('drawdown_pct', 0)
        val    = p.get('portfolio_value', 0)
        de_esc = p.get('de_escalation', False)

        if event_type == 'alert_cleared':
            return (f'ALERT CLEARED — System GREEN\n'
                    f'Portfolio: INR {val:,.0f}  Drawdown: {dd:.1f}%')

        level  = event_type.replace('alert_', '').upper()
        prefix = 'DE-ESCALATED TO' if de_esc else f'{level} ALERT'

        notes = {
            'alert_yellow': 'Slot 3 entries halted. Positions reduced 50%.',
            'alert_orange': 'All new entries halted. Slot 3 closed.',
            'alert_red':    'ALL positions closed. System HALTED.',
        }.get(event_type, '')

        header = f'{prefix} — {dd:.1f}% drawdown'
        body   = f'Portfolio: INR {val:,.0f}'
        return f'{header}\n{body}' + (f'\n{notes}' if notes and not de_esc else '')

    def _fmt_forced_override(self, p: dict) -> tuple[str, str, str]:
        """Returns (telegram_text, email_subject, email_body)."""
        reason = p.get('reason') or p.get('trigger') or 'See logs for details'
        val    = p.get('portfolio_value', 0)
        dd     = p.get('drawdown_pct', 0)

        # Gather open positions for context
        try:
            with get_connection() as conn:
                positions = conn.execute(
                    'SELECT symbol, direction, unrealised_pnl FROM positions '
                    'WHERE status = ? ORDER BY id',
                    ('open',)
                ).fetchall()
        except Exception:
            positions = []

        pos_lines = '\n'.join(
            f'  {r["symbol"]} {r["direction"].upper()}  '
            f'PnL: INR {(r["unrealised_pnl"] or 0):.0f}'
            for r in positions
        ) or '  (none)'

        tg_text = (
            f'!! FORCED OVERRIDE TRIGGERED !!\n'
            f'Reason: {reason}\n'
            f'Portfolio: INR {val:,.0f}  Drawdown: {dd:.1f}%\n'
            f'Open positions:\n{pos_lines}\n'
            f'\n'
            f'Response options (S.19.3):\n'
            f'  /resume         -- Option A: Resume normal operation\n'
            f'  /resume <reason>-- Option A: Resume with logged reasoning\n'
            f'  /halt           -- Option C: Permanent halt (close positions manually)\n'
            f'  Option B: Adjust parameters (env vars), then use /resume'
        )
        subject = f'Kronos FORCED OVERRIDE — {reason[:60]}'
        body    = tg_text + '\n\nThis is an automated safety alert from Kronos.'
        return tg_text, subject, body

    def _fmt_monthly_summary(self, w: dict, tax: dict) -> str:
        month    = w.get('month', '?')
        gross    = w.get('gross_pnl', 0)
        eligible = w.get('withdrawal_eligible', False)
        human_w  = w.get('human_withdrawal', 0)
        reserve  = w.get('tax_reserve_credit', 0)
        cumul    = w.get('cumulative_withdrawn', 0)
        recap    = w.get('recuperation_pct', 0)

        tax_bal  = tax.get('tax_reserve_balance', 0)
        tds      = tax.get('tds_total', 0)
        net_taxable = tax.get('net_taxable_income', 0)
        liability   = tax.get('tax_liability_30pct', 0)

        lines = [
            f'MONTHLY SUMMARY — {month}',
            f'Gross P&L:          INR {gross:,.2f}',
            f'Tax Reserve (30%%): INR {reserve:,.2f}',
            f'TDS Deducted:        INR {tds:,.2f}',
            f'Net Taxable:         INR {net_taxable:,.2f}',
            f'Tax Liability:       INR {liability:,.2f}',
            f'Reserve Balance:     INR {tax_bal:,.2f}',
            '',
            f'Withdrawal eligible: {"YES" if eligible else "NO"}',
        ]
        if eligible:
            lines.append(f'Withdrawal amount:   INR {human_w:,.2f}')
        lines += [
            '',
            f'Recuperation: INR {cumul:,.0f} / INR {STARTING_CAPITAL_INR:,.0f} ({recap:.1f}%)',
        ]
        return '\n'.join(lines)

    def _fmt_slot3(self, p: dict) -> str:
        symbol   = p.get('slot3_symbol', '?')
        conf     = p.get('confidence', 0)
        ranking  = p.get('ranking', {})
        rank_str = ''
        if ranking:
            rank_str = '\nAll candidates:\n' + '\n'.join(
                f'  {s}: conf={v:.3f}' for s, v in ranking.items()
            )
        return (f'Weekly Slot 3 selection\n'
                f'Selected: {symbol}  (confidence {conf:.3f}){rank_str}')

    def _fmt_strategy_alert(self, p: dict, event_type: str) -> str:
        if event_type == 'rr_alert_7d':
            avg_rr = p.get('avg_rr', 0)
            count  = p.get('trade_count', 0)
            return (f'R:R ALERT — Weekly average R:R below threshold\n'
                    f'Avg realised R:R: {avg_rr:.2f} (min 1.5)  '
                    f'Trades: {count}\nReview strategy.')
        else:  # win_rate_alert_7d
            wr   = p.get('win_rate', 0)
            adj  = p.get('threshold_adjustment', 0)
            return (f'WIN RATE ALERT — 7-day win rate below 55%%\n'
                    f'Win rate: {wr:.1%}  '
                    f'Confidence threshold raised by {adj:.2f}')

    def _fmt_milestone(self, p: dict) -> str:
        milestone = p.get('milestone', '?')
        cumul     = p.get('cumulative_after_withdrawal', 0)
        pct       = cumul / STARTING_CAPITAL_INR * 100
        return (f'RECUPERATION MILESTONE — {milestone}\n'
                f'Cumulative withdrawn: INR {cumul:,.0f} ({pct:.1f}% of INR {STARTING_CAPITAL_INR:,.0f})\n'
                f'Target: INR {STARTING_CAPITAL_INR:,.0f}')

    def _fmt_advance_tax(self, p: dict) -> str:
        fy        = p.get('fiscal_year', '?')
        liability = p.get('ytd_tax_liability', 0)
        tds       = p.get('ytd_tds_credit', 0)
        due       = p.get('advance_tax_due', 0)
        reserve   = p.get('tax_reserve_balance', 0)
        return (f'ADVANCE TAX REMINDER — {fy} (March 15 deadline)\n'
                f'YTD tax liability: INR {liability:,.2f}\n'
                f'TDS credit so far: INR {tds:,.2f}\n'
                f'Advance tax due:   INR {due:,.2f}\n'
                f'Tax reserve bal:   INR {reserve:,.2f}\n'
                f'Action required: pay advance tax if due.')

    def _fmt_annual_vda(self, p: dict) -> str:
        fy      = p.get('fiscal_year', '?')
        gains   = p.get('total_gains', 0)
        losses  = p.get('total_losses', 0)
        tax     = p.get('tax_liability', 0)
        count   = p.get('trade_count', 0)
        path    = p.get('report_file', 'data/reports/')
        return (f'ANNUAL VDA REPORT READY — {fy}\n'
                f'Total gains:  INR {gains:,.2f}\n'
                f'Total losses: INR {losses:,.2f}\n'
                f'Tax (30%%):   INR {tax:,.2f}\n'
                f'Trades:       {count}\n'
                f'File: {path}')

    def _fmt_module_stale(self, p: dict) -> str:
        module  = p.get('module', '?')
        check   = p.get('check', '?')
        message = p.get('message', 'Module output is stale')
        return (f'MODULE STALE ALERT\n'
                f'Module: {module}\n'
                f'Check: {check}\n'
                f'{message}')

    def _fmt_orphaned_order(self, p: dict) -> str:
        trade_id = p.get('trade_id', '?')
        symbol   = p.get('symbol') or '?'
        age_h    = p.get('age_hours', 0)
        return (f'ORPHANED ORDER DETECTED\n'
                f'Trade #{trade_id}  Symbol: {symbol}\n'
                f'Order age: {age_h:.1f}h -- review Delta Exchange manually')

    def _fmt_position_close_required(self, p: dict) -> str:
        fo_id = p.get('fo_id', '?')
        age_h = p.get('age_h', 0)
        return (f'!! POSITION CLOSE REQUIRED !!\n'
                f'forced_override (id={fo_id}) unresponded for {age_h:.1f}h\n'
                f'Action: manually close all open positions in Delta Exchange.\n'
                f'Then use /resume to clear the override, or /halt to stop permanently.')

    def _fmt_8h_summary(self) -> str:
        """Build 8H cyclic portfolio summary from current DB state (§19.6)."""
        now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)

        # Portfolio value + drawdown
        with get_connection() as conn:
            snap = conn.execute(
                'SELECT total_value, peak_value FROM portfolio_snapshots '
                'ORDER BY id DESC LIMIT 1'
            ).fetchone()
            alert_row = conn.execute(
                "SELECT event_type, data FROM events "
                "WHERE event_type IN ('alert_yellow','alert_orange','alert_red','alert_cleared') "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            positions = conn.execute(
                'SELECT symbol, direction, unrealised_pnl, entry_price '
                'FROM positions WHERE status = ? ORDER BY id',
                ('open',)
            ).fetchall()
            # Latest funding rate per symbol
            funding_rows = conn.execute(
                'SELECT symbol, rate FROM funding_rates '
                'WHERE timestamp = (SELECT MAX(timestamp) FROM funding_rates f2 '
                '                   WHERE f2.symbol = funding_rates.symbol) '
                'GROUP BY symbol ORDER BY symbol'
            ).fetchall()

        total_val   = float(snap['total_value']) if snap else 0.0
        peak_val    = float(snap['peak_value'])  if snap else total_val
        dd_pct      = (peak_val - total_val) / peak_val * 100 if peak_val > 0 else 0.0
        pnl_vs_start = total_val - STARTING_CAPITAL_INR

        alert_level = 'GREEN'
        if alert_row:
            level_map = {
                'alert_yellow': 'YELLOW', 'alert_orange': 'ORANGE',
                'alert_red': 'RED', 'alert_cleared': 'GREEN',
            }
            alert_level = level_map.get(alert_row['event_type'], 'GREEN')

        pos_lines = '\n'.join(
            f'  {r["symbol"]} {r["direction"].upper()}'
            f'  unrealised=INR {(r["unrealised_pnl"] or 0):+.0f}'
            f'  entry={r["entry_price"]}'
            for r in positions
        ) or '  (none open)'

        fund_lines = '\n'.join(
            f'  {r["symbol"]}: {float(r["rate"])*100:.4f}%/8H'
            + (' [EXIT THRESHOLD]' if abs(float(r['rate'])) > 0.001 else '')
            for r in funding_rows
        ) or '  (no data)'

        pnl_sign = '+' if pnl_vs_start >= 0 else ''
        return (
            f'=== 8H PORTFOLIO SUMMARY ===\n'
            f'{now_utc.strftime("%Y-%m-%d %H:%M UTC")}\n'
            f'\n'
            f'Portfolio:    INR {total_val:,.0f}\n'
            f'P&L vs start: INR {pnl_sign}{pnl_vs_start:,.0f}\n'
            f'Drawdown:     {dd_pct:.1f}% from peak\n'
            f'Alert:        {alert_level}\n'
            f'\n'
            f'Open positions:\n{pos_lines}\n'
            f'\n'
            f'Funding rates:\n{fund_lines}'
        )

    # ── Core notification routing ──────────────────────────────────────────────

    def collect_notifications(self, events: list[dict]) -> list[dict]:
        """
        Route a list of DB event dicts to notification messages.

        Returns list of dicts:
          {'text': str, 'also_email': bool, 'email_subject': str, 'email_body': str}

        Deduplicates repeated alert events within _DEDUP_WINDOW_S seconds.
        slot3_selection events are only notified when they fall on a Sunday (UTC).
        withdrawal_calculation events compose a full monthly summary by also
        querying the DB for the latest monthly_tax_summary event.
        """
        notifications = []
        now_ts = int(time.time())

        for event in events:
            et      = event.get('event_type', '')
            ts      = event.get('timestamp', now_ts)
            payload = self._parse_payload(event)

            if et not in NOTIFY_EVENTS:
                continue

            # Dedup: forced_override (EMAIL_EVENTS) always goes through.
            # Alert events: skip if same type sent within _DEDUP_WINDOW_S (5 min).
            # Strategy alerts: skip if same type sent within _STRATEGY_DEDUP_WINDOW_S (23h)
            # because Module 5 writes win_rate_alert_7d on every 4H cycle while active.
            last = self._last_sent_ts.get(et, 0)
            if et not in EMAIL_EVENTS:
                if et in _ALERT_DEDUP_EVENTS and (now_ts - last) < _DEDUP_WINDOW_S:
                    continue
                if et in _STRATEGY_DEDUP_EVENTS and (now_ts - last) < _STRATEGY_DEDUP_WINDOW_S:
                    continue

            # slot3_selection — only notify on Sundays (UTC)
            if et == 'slot3_selection':
                if datetime.fromtimestamp(ts, tz=timezone.utc).weekday() != 6:
                    continue
                text = self._fmt_slot3(payload)

            elif et == 'withdrawal_calculation':
                tax_payload = self._get_latest_event_payload('monthly_tax_summary')
                text = self._fmt_monthly_summary(payload, tax_payload)

            elif et == 'forced_override':
                text, email_subj, email_body = self._fmt_forced_override(payload)
                notifications.append({
                    'text': text, 'also_email': True,
                    'email_subject': email_subj, 'email_body': email_body,
                })
                self._last_sent_ts[et] = now_ts
                continue

            elif et in ('paper_fill', 'order_filled'):
                text = self._fmt_fill(payload, et)
            elif et == 'order_timeout':
                text = self._fmt_timeout(payload)
            elif et == 'execution_error':
                text = self._fmt_execution_error(payload)
            elif et in ('stop_loss_exit', 'take_profit_exit',
                        'time_limit_exit', 'funding_cost_exit'):
                text = self._fmt_exit(payload, et)
            elif et in ('alert_yellow', 'alert_orange', 'alert_red', 'alert_cleared'):
                text = self._fmt_alert(payload, et)
            elif et in ('rr_alert_7d', 'win_rate_alert_7d'):
                text = self._fmt_strategy_alert(payload, et)
            elif et == 'recuperation_milestone':
                text = self._fmt_milestone(payload)
            elif et == 'advance_tax_alert':
                text = self._fmt_advance_tax(payload)
            elif et == 'annual_vda_report':
                text = self._fmt_annual_vda(payload)
            elif et == 'module_stale':
                text = self._fmt_module_stale(payload)
            elif et == 'orphaned_order':
                text = self._fmt_orphaned_order(payload)
            elif et == 'position_close_required':
                text = self._fmt_position_close_required(payload)
            else:
                continue

            notifications.append({
                'text': text, 'also_email': False,
                'email_subject': '', 'email_body': '',
            })
            self._last_sent_ts[et] = now_ts

        return notifications

    # ── Business logic commands ────────────────────────────────────────────────

    def process_withdrawal(self, amount: float) -> str:
        """
        Record a confirmed human withdrawal.  Writes withdrawal_made event
        (payload key 'amount') consumed by Modules 8 and 9 for recuperation
        ledger tracking (§11.4).
        Returns confirmation message string.
        """
        if amount <= 0:
            return f'Invalid amount: {amount}. Must be > 0.'
        now_ts = int(time.time())
        log_event(MODULE, 'info', 'withdrawal_made',
                  f'Human withdrawal confirmed: INR {amount:.2f}',
                  {'amount': round(amount, 2), 'timestamp': now_ts})
        log.info('withdrawal_made event written: INR %.2f', amount)
        return f'Withdrawal recorded: INR {amount:,.2f}. Recuperation ledger updated.'

    def process_resume(self, reasoning: str = '') -> str:
        """
        Resume trading after forced override (§19.3 Option A).
        Writes forced_override_cleared event consumed by Module 5 to unblock signals.
        reasoning: optional human explanation logged in payload (§19.3 Step 4).
        Returns confirmation message string.
        """
        now_ts = int(time.time())
        log_event(MODULE, 'info', 'forced_override_cleared',
                  'Human resumed system via /resume command',
                  {'timestamp': now_ts, 'resumed_by': 'human_telegram_command',
                   'decision': 'option_a',
                   'reasoning': reasoning or 'none provided'})
        log.info('forced_override_cleared event written — system resuming')
        return 'System resumed. Forced override cleared. Trading will resume on next signal cycle.'

    def process_halt(self, reasoning: str = '') -> str:
        """
        Permanently halt the system (§19.3 Option C).
        Writes forced_override event so Module 5 blocks all signals indefinitely.
        Positions must be closed manually in Delta Exchange — Module 10 cannot
        issue close orders directly.
        reasoning: optional human explanation logged in payload (§19.3 Step 4).
        Returns confirmation message string.
        """
        now_ts = int(time.time())
        log_event(MODULE, 'warning', 'forced_override',
                  'Human initiated permanent halt via /halt command',
                  {'timestamp': now_ts, 'reason': 'permanent_halt_by_human',
                   'halt_type': 'permanent', 'decision': 'option_c',
                   'reasoning': reasoning or 'none provided'})
        log.warning('forced_override (permanent halt) event written by human')
        return (
            'System halted permanently. Signal processing blocked.\n'
            'Action required: manually close any open positions in Delta Exchange.\n'
            'Use /resume to restart the system when ready.'
        )

    def get_status_snapshot(self) -> str:
        """Current system snapshot for /status command."""
        return self._fmt_8h_summary()

    # ── Async sends ────────────────────────────────────────────────────────────

    async def _send_telegram(self, text: str) -> bool:
        if not _TELEGRAM_AVAILABLE or not BOT_TOKEN or not CHAT_ID:
            log.info('Telegram not configured — message logged: %.80s', text)
            return False
        try:
            async with Bot(BOT_TOKEN) as bot:
                await bot.send_message(chat_id=int(CHAT_ID), text=text)
            return True
        except Exception as exc:
            log.error('Telegram send failed: %s', exc)
            log_event(MODULE, 'error', 'notification_error',
                      f'Telegram send failed: {exc}', {'channel': 'telegram'})
            return False

    def _send_email_sync(self, subject: str, body: str) -> bool:
        """Send email via Gmail SMTP (synchronous — only for rare forced overrides)."""
        if not EMAIL_FROM or not EMAIL_TO or not EMAIL_PASSWORD:
            log.info('Email not configured — email logged: %s', subject)
            return False
        try:
            msg = MIMEText(body, 'plain', 'utf-8')
            msg['Subject'] = subject
            msg['From']    = EMAIL_FROM
            msg['To']      = EMAIL_TO
            with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=15) as smtp:
                smtp.login(EMAIL_FROM, EMAIL_PASSWORD)
                smtp.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
            log.info('Email sent: %s', subject)
            return True
        except Exception as exc:
            log.error('Email send failed: %s', exc)
            log_event(MODULE, 'error', 'notification_error',
                      f'Email send failed: {exc}', {'channel': 'email'})
            return False

    # ── Async job handlers ─────────────────────────────────────────────────────

    async def _job_event_poll(self, context) -> None:
        """Poll events table for new events and dispatch notifications."""
        try:
            new_events = self._get_new_events()
            if not new_events:
                return
            notifications = self.collect_notifications(new_events)
            for notif in notifications:
                await self._send_telegram(notif['text'])
                if notif['also_email']:
                    self._send_email_sync(notif['email_subject'], notif['email_body'])
            # Advance cursor regardless of send outcome (avoid replay on restart)
            self._last_event_id = new_events[-1]['id']
            self._save_last_event_id(self._last_event_id)
            if notifications:
                log.info('Event poll: %d new events, %d notifications sent',
                         len(new_events), len(notifications))
        except Exception as exc:
            log.error('Event poll error: %s', exc, exc_info=True)
            log_event(MODULE, 'error', 'notification_error',
                      f'Event poll failed: {exc}', {'error': str(exc)})

    async def _job_8h_summary(self, context) -> None:
        """Send 8H cyclic portfolio summary (§19.6)."""
        try:
            text = self._fmt_8h_summary()
            await self._send_telegram(text)
            log.info('8H summary sent')
        except Exception as exc:
            log.error('8H summary error: %s', exc, exc_info=True)

    # ── Telegram command handlers ──────────────────────────────────────────────

    async def _cmd_withdraw(self, update, context) -> None:
        """Handle /withdraw <amount> — confirms a human withdrawal."""
        args = context.args
        if not args:
            await update.message.reply_text('Usage: /withdraw <amount_inr>')
            return
        try:
            amount = float(args[0])
        except ValueError:
            await update.message.reply_text(f'Invalid amount: {args[0]}')
            return
        reply = self.process_withdrawal(amount)
        await update.message.reply_text(reply)

    async def _cmd_resume(self, update, context) -> None:
        """Handle /resume [reason] — Option A: clears forced override and resumes trading."""
        reasoning = ' '.join(context.args) if context.args else ''
        reply = self.process_resume(reasoning)
        await update.message.reply_text(reply)

    async def _cmd_halt(self, update, context) -> None:
        """Handle /halt [reason] — Option C: permanent system halt (§19.3)."""
        reasoning = ' '.join(context.args) if context.args else ''
        reply = self.process_halt(reasoning)
        await update.message.reply_text(reply)

    async def _cmd_status(self, update, context) -> None:
        """Handle /status — send current system snapshot."""
        try:
            text = self.get_status_snapshot()
        except Exception as exc:
            text = f'Status unavailable: {exc}'
        await update.message.reply_text(text)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Build and run the Telegram Application (blocks until stopped)."""
        init_db()
        os.makedirs(os.path.dirname(STATE_PATH) or '.', exist_ok=True)

        if not BOT_TOKEN or not CHAT_ID:
            log.warning('BOT_TOKEN or CHAT_ID not set — running in log-only mode (no Telegram)')
            self._app = None
            # Block indefinitely in log-only mode; events are still written to DB.
            while True:
                time.sleep(3600)
            return

        if not _TELEGRAM_AVAILABLE:
            raise RuntimeError('python-telegram-bot not installed — cannot start')

        app = ApplicationBuilder().token(BOT_TOKEN).build()

        # Command handlers
        app.add_handler(CommandHandler('withdraw', self._cmd_withdraw))
        app.add_handler(CommandHandler('resume',   self._cmd_resume))
        app.add_handler(CommandHandler('halt',     self._cmd_halt))
        app.add_handler(CommandHandler('status',   self._cmd_status))

        # 8H cyclic summaries: 00:02, 08:02, 16:02 UTC (§19.6)
        for hour in (0, 8, 16):
            app.job_queue.run_daily(
                self._job_8h_summary,
                time=dtime(hour, 2, 0, tzinfo=timezone.utc),
                name=f'8h_summary_{hour:02d}h',
            )

        # Event poller: every 15 min (1 min after Module 9 at :04)
        app.job_queue.run_repeating(
            self._job_event_poll,
            interval=900,
            first=30,
            name='event_poller',
        )

        self._app = app
        log.info('Notification module starting (polling)...')
        app.run_polling(allowed_updates=Update.ALL_TYPES)

    def stop(self) -> None:
        log.info('Notification module stopping.')


def main() -> None:
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s -- %(message)s',
        stream=sys.stdout,
    )
    NotificationModule().start()


if __name__ == '__main__':
    main()
