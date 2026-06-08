"""
Kronos Trading System — Module 9: Tax & TDS Tracker
Sections 9.3, 16.1, 16.2, 16.3, 18.2 of the spec (v3.2).

Processes every closed trade (status='closed', pnl_net IS NULL) to compute:
  - TDS deducted: ZERO — Delta Exchange India does NOT deduct 1% TDS on
    futures/options contracts. Section 194S TDS applies only to VDA spot
    transfers; perpetual futures are derivatives, not VDA transfers. The
    tds_deducted field is always 0 for futures trades. (Confirmed: Delta
    Exchange India FAQ — "1% TDS not applicable on futures/options".)
  - Trading fees: maker (0.04%) or taker (0.10%) × notional + 18% GST (§16.1/§3.3)
  - Funding paid/received: per 8H settlement during hold period (§9.3/§9.4)
  - pnl_net = pnl_gross − fees − funding_paid  (§9.3: funding paid is a
    deductible cost offset against trade P&L before tax calculation;
    funding_received tracked separately — taxable income included in monthly summary)

Tax treatment (§16.1 corrected):
  - Profits from crypto futures/options = speculative income under Indian IT Act
  - Taxed at income slab rate, NOT flat 30% VDA rate (30% VDA = spot only)
  - TAX_RATE reflects the user's marginal slab rate (currently 15% — 12-16L bracket,
    new tax regime FY 2025-26 per Section 115BAC; update if income bracket changes)
  - Loss offsetting allowed against other speculative income in same FY
  - Loss carry-forward available to next FY

Updates trades.pnl_net, trades.tds_deducted (always 0), trades.fees,
trades.funding_paid, trades.funding_received.
tds_log table is retained but will remain empty for futures trades.

Generates monthly tax summaries and recuperation ledger (§16.3: last calendar day),
advance tax alert (§16.3: March 15), annual speculative income report and
reconciliation (§16.3: July 31 ITR filing deadline, covering the just-ended
Indian FY — April 1 → March 31).

Event types written: tax_record, monthly_tax_summary, recuperation_ledger,
                     advance_tax_alert, annual_vda_report, tds_reconciliation,
                     execution_error

Schedule (UTC):
  Cron: minute='4,19,34,49' — 1 min after Portfolio Manager (:3,:18,:33,:48)
  Monthly summary + recuperation ledger: last calendar day, first cycle of UTC day
  Advance tax alert: March 15, first cycle of UTC day
  Annual report + reconciliation: July 31, first cycle of UTC day (ITR filing)
"""

import asyncio
import calendar
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import get_connection, init_db, log_event

log = logging.getLogger(__name__)
MODULE = 'tax_tracker'

# ── Constants ──────────────────────────────────────────────────────────────────

USD_INR_RATE          = float(os.environ.get('KRONOS_USD_INR_RATE', '84.0'))
PAPER_MODE            = os.environ.get('KRONOS_PAPER_MODE', 'false').lower() == 'true'
STARTING_CAPITAL_INR  = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', '100000.0'))

# §16.1 Indian tax rules — CORRECTED for crypto futures/options
# TDS: Section 194S (1% TDS) applies to VDA spot transfers ONLY.
#      Delta Exchange India perpetual futures are derivatives, NOT VDA transfers.
#      Delta Exchange India has confirmed TDS is NOT deducted on F&O contracts.
TDS_RATE      = 0.0      # 0% — TDS not applicable to futures/options on Delta Exchange India
TDS_THRESHOLD = 10000.0  # retained for reference; effectively unused since TDS_RATE = 0
GST_RATE      = 0.18     # 18% GST applied to exchange fees by Delta Exchange

# §16.1 Dynamic slab tax — imported from shared tax_utils module.
# Set KRONOS_BASE_INCOME_INR in .env each FY to your non-trading annual income (salary etc.).
# Set to 0 once India income ceases. If NRI: set KRONOS_TAX_RESERVE_ENABLED=false instead.
BASE_INCOME_INR = float(os.environ.get('KRONOS_BASE_INCOME_INR', '0'))

from tax_utils import (  # noqa: E402
    _total_tax,
    _marginal_rate,
    incremental_tax,
    SECTION_87A_LIMIT,
    HEALTH_ED_CESS,
)

# §3.3 fee rates
MAKER_FEE_RATE = 0.0004    # 0.04% for limit orders (entry, take_profit exit)
TAKER_FEE_RATE = 0.0010    # 0.10% for market orders (stop_loss, time_limit, etc.)

# Exit reasons that use market (taker) orders on exit — all others use maker
TAKER_EXIT_REASONS = frozenset({
    'stop_loss', 'time_limit', 'funding_cost',
    'drawdown_alert', 'forced_override', 'manual',
})

_DEFAULT_CONTRACT_SIZES: dict[str, float] = {
    'BTCUSD': 0.001,
    'ETHUSD': 0.01,
    'SOLUSD': 1.0,
    'BNBUSD': 0.1,
    'XRPUSD': 1.0,    # 1 XRP per contract — corrected 2026-06-07 (was 10.0, inflated fees 10×)
}

REPORT_DIR = os.path.join(os.path.dirname(__file__), 'data', 'reports')


# ── Main class ─────────────────────────────────────────────────────────────────

class TaxTracker:
    """
    Module 9 — Tax & TDS Tracker.

    Runs every 15 minutes. Processes all newly closed trades (pnl_net IS NULL)
    to compute TDS, fees, funding paid/received, and pnl_net. Updates trades
    table and tds_log. Generates monthly tax summaries (with recuperation ledger),
    advance tax alerts (March 15), annual Schedule VDA reports and TDS
    reconciliation (July 31) per §16.2/§16.3.

    Note (§18.3): Module 9 is not active during pre-live when there are no
    real trades, but the scheduler runs regardless so that paper trades are
    also tracked for testing. PAPER_MODE is reflected in tax_record payloads.
    """

    def __init__(self) -> None:
        self._scheduler:      Optional[AsyncIOScheduler] = None
        self._contract_sizes: dict[str, float]           = dict(_DEFAULT_CONTRACT_SIZES)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        init_db()
        os.makedirs(REPORT_DIR, exist_ok=True)
        self._scheduler = AsyncIOScheduler(timezone='UTC')
        self._scheduler.add_job(
            self._job_cycle,
            CronTrigger(minute='4,19,34,49', timezone='UTC'),
            id='tax_tracker_cycle',
            name='Tax & TDS Tracker 15-min cycle',
            max_instances=1,
            misfire_grace_time=60,
        )
        self._scheduler.start()
        log.info('Tax Tracker started.')

    def stop(self) -> None:
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    # ── Notional helpers ──────────────────────────────────────────────────────

    def _sell_notional_inr(self, trade: dict) -> float:
        """Exit-leg notional in INR: size × contract_size × exit_price × USD_INR."""
        sym = trade['symbol']
        cs  = self._contract_sizes.get(sym, 1.0)
        return float(trade['size_contracts']) * cs * float(trade['exit_price'] or 0.0) * USD_INR_RATE

    def _entry_notional_inr(self, trade: dict) -> float:
        """Entry-leg notional in INR: size × contract_size × entry_price × USD_INR."""
        sym = trade['symbol']
        cs  = self._contract_sizes.get(sym, 1.0)
        return float(trade['size_contracts']) * cs * float(trade['entry_price'] or 0.0) * USD_INR_RATE

    # ── Tax component computations ────────────────────────────────────────────

    def _compute_tds(self, sell_notional_inr: float) -> float:
        """TDS on sell notional.

        Delta Exchange India does NOT deduct TDS on futures/options contracts.
        Section 194S (1% TDS) applies to VDA spot transfers only; perpetual
        futures are derivatives, not VDA transfers. Always returns 0.0.
        TDS_RATE is set to 0.0 at the module level to enforce this.
        """
        if sell_notional_inr > TDS_THRESHOLD:
            return round(sell_notional_inr * TDS_RATE, 2)  # TDS_RATE = 0.0
        return 0.0

    def _compute_fees(self, trade: dict,
                      entry_notional_inr: float,
                      sell_notional_inr: float) -> float:
        """
        Round-trip fee including 18% GST (§16.1/§3.3).
        Entry: maker 0.04% (limit orders only per §10.1).
        Exit: maker 0.04% for take_profit (limit); taker 0.10% for all other
              exits (stop_loss, time_limit, funding_cost, drawdown_alert — market).
        """
        exit_reason   = (trade.get('exit_reason') or '').lower()
        exit_fee_rate = TAKER_FEE_RATE if exit_reason in TAKER_EXIT_REASONS else MAKER_FEE_RATE
        raw = entry_notional_inr * MAKER_FEE_RATE + sell_notional_inr * exit_fee_rate
        return round(raw * (1.0 + GST_RATE), 2)

    def _compute_funding_for_trade(self, trade: dict) -> tuple[float, float]:
        """
        Estimate funding paid and received (INR) during the trade's hold period.

        Looks up all funding_rates rows written between entry_timestamp and
        exit_timestamp + 130s (§9.4 grace: rate written at :02 past settlement).
        Each row represents one 8H settlement.

        Notional is approximated as entry notional for the full period — size
        changes from Yellow Alert reductions are not tracked in funding_rates
        (best-effort per §18.2).

        Returns (funding_paid_inr, funding_received_inr) — both non-negative.
        """
        sym       = trade['symbol']
        direction = trade['direction']
        entry_ts  = int(trade.get('entry_timestamp') or 0)
        exit_ts   = int(trade.get('exit_timestamp') or 0)
        cs        = self._contract_sizes.get(sym, 1.0)
        size      = float(trade['size_contracts'])
        entry_px  = float(trade.get('entry_price') or 0.0)
        notional_usd = size * cs * entry_px

        if exit_ts <= entry_ts or notional_usd <= 0:
            return 0.0, 0.0

        with get_connection() as conn:
            rows = conn.execute(
                """SELECT rate FROM funding_rates
                   WHERE symbol   =  ?
                     AND timestamp >  ?
                     AND timestamp <= ?
                   ORDER BY timestamp ASC""",
                (sym, entry_ts, exit_ts + 130)
            ).fetchall()

        paid = 0.0
        received = 0.0
        for r in rows:
            rate        = float(r['rate'])
            f_usd       = notional_usd * rate          # positive rate = longs pay
            f_inr       = f_usd * USD_INR_RATE
            if direction == 'long':
                if f_inr > 0:
                    paid     += f_inr
                else:
                    received += abs(f_inr)
            else:  # short
                if f_inr > 0:
                    received += f_inr
                else:
                    paid     += abs(f_inr)

        return round(paid, 2), round(received, 2)

    # ── Trade processing ──────────────────────────────────────────────────────

    def _process_trade(self, trade: dict) -> None:
        """
        Compute and persist TDS, fees, funding, and pnl_net for one closed trade.

        pnl_net = pnl_gross - fees - funding_paid  (§9.3/§16.1:
        TDS is always 0 for futures/options; funding paid is a deductible cost
        offset against trade P&L before tax calculation; funding_received is
        separate taxable income logged in its own column and included in monthly
        net_taxable).

        tds_deducted is written as 0. No tds_log row is written. Writes tax_record event.
        """
        trade_id   = int(trade['id'])
        symbol     = trade['symbol']
        pnl_gross  = float(trade.get('pnl_gross') or 0.0)
        exit_price = float(trade.get('exit_price') or 0.0)

        if exit_price <= 0:
            log.warning('Trade %d has no exit_price — skipping', trade_id)
            return

        sell_notional  = self._sell_notional_inr(trade)
        entry_notional = self._entry_notional_inr(trade)
        tds_deducted   = self._compute_tds(sell_notional)
        fees           = self._compute_fees(trade, entry_notional, sell_notional)
        funding_paid, funding_received = self._compute_funding_for_trade(trade)

        # §9.3/§16.1: funding paid is a deductible cost — offset at trade level
        pnl_net = round(pnl_gross - tds_deducted - fees - funding_paid, 2)

        now_ts = int(time.time())
        with get_connection() as conn:
            conn.execute(
                """UPDATE trades
                   SET pnl_net = ?, tds_deducted = ?, fees = ?,
                       funding_paid = ?, funding_received = ?
                   WHERE id = ?""",
                (pnl_net, tds_deducted, fees,
                 funding_paid, funding_received, trade_id)
            )
            if tds_deducted > 0:
                conn.execute(
                    """INSERT INTO tds_log
                       (trade_id, symbol, sell_notional, tds_amount, utr_reference, timestamp)
                       VALUES (?, ?, ?, ?, NULL, ?)""",
                    (trade_id, symbol,
                     round(sell_notional, 2), tds_deducted, now_ts)
                )

        log_event(MODULE, 'info', 'tax_record',
                  f'Trade {trade_id} ({symbol}): gross=INR {pnl_gross:.2f} '
                  f'tds=INR {tds_deducted:.2f} fees=INR {fees:.2f} '
                  f'f_paid=INR {funding_paid:.2f} net=INR {pnl_net:.2f}',
                  {'trade_id':          trade_id,
                   'symbol':            symbol,
                   'pnl_gross':         round(pnl_gross, 2),
                   'tds_deducted':      tds_deducted,
                   'fees':              fees,
                   'funding_paid':      funding_paid,
                   'funding_received':  funding_received,
                   'pnl_net':           pnl_net,
                   'sell_notional_inr': round(sell_notional, 2),
                   'paper':             PAPER_MODE})
        log.info('Tax record: trade=%d %s gross=INR %.2f tds=INR %.2f fees=INR %.2f '
                 'f_paid=INR %.2f net=INR %.2f',
                 trade_id, symbol, pnl_gross, tds_deducted, fees, funding_paid, pnl_net)

    def _get_unprocessed_trades(self) -> list[dict]:
        """All closed trades that have not yet had pnl_net computed."""
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT * FROM trades
                   WHERE status = 'closed' AND pnl_net IS NULL
                   ORDER BY exit_timestamp ASC"""
            ).fetchall()
        return [dict(r) for r in rows]

    def _process_unprocessed_trades(self) -> int:
        """Process all unprocessed closed trades. Returns count processed."""
        trades = self._get_unprocessed_trades()
        for t in trades:
            try:
                self._process_trade(t)
            except Exception as exc:
                log.error('Error processing trade %d: %s', t.get('id'), exc, exc_info=True)
                log_event(MODULE, 'error', 'execution_error',
                          f"Trade {t.get('id')} tax processing failed: {exc}",
                          {'trade_id': t.get('id'), 'error': str(exc)})
        return len(trades)

    # ── Fiscal year helpers ───────────────────────────────────────────────────

    @staticmethod
    def _get_fy_start_ts(reference_ts: int | None = None) -> int:
        """
        Unix timestamp for April 1 00:00:00 UTC of the Indian fiscal year
        that contains `reference_ts` (defaults to now).
        Indian FY: April 1 → March 31.
        """
        ts  = reference_ts if reference_ts is not None else int(time.time())
        dt  = datetime.fromtimestamp(ts, tz=timezone.utc)
        fy_year = dt.year if dt.month >= 4 else dt.year - 1
        return int(datetime(fy_year, 4, 1, tzinfo=timezone.utc).timestamp())

    @staticmethod
    def _get_fy_label(reference_ts: int | None = None) -> str:
        """e.g. 'FY2025-26' for the FY containing reference_ts."""
        ts  = reference_ts if reference_ts is not None else int(time.time())
        dt  = datetime.fromtimestamp(ts, tz=timezone.utc)
        fy_start = dt.year if dt.month >= 4 else dt.year - 1
        return f'FY{fy_start}-{str(fy_start + 1)[-2:]}'

    # ── Monthly tax summary ───────────────────────────────────────────────────

    def _is_last_day_of_month(self) -> bool:
        now_utc  = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        last_day = calendar.monthrange(now_utc.year, now_utc.month)[1]
        return now_utc.day == last_day

    def _monthly_summary_already_run_today(self) -> bool:
        now_utc   = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        day_start = int(datetime(now_utc.year, now_utc.month, now_utc.day,
                                 tzinfo=timezone.utc).timestamp())
        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM events WHERE event_type='monthly_tax_summary' AND timestamp>=? LIMIT 1",
                (day_start,)
            ).fetchone()
        return row is not None

    def _get_tax_reserve_balance(self) -> float:
        """Read the running tax reserve balance from the tax_reserve table."""
        with get_connection() as conn:
            row = conn.execute(
                'SELECT COALESCE(balance_after, 0.0) AS bal FROM tax_reserve ORDER BY id DESC LIMIT 1'
            ).fetchone()
        return float(row['bal']) if row else 0.0

    def _generate_monthly_summary(self, now_utc: datetime) -> None:
        """
        Aggregate all processed closed trades in the current month and write a
        monthly_tax_summary event per §16.2/§16.3.

        net_taxable_income = pnl_gross + funding_received - funding_paid  (§9.3:
        funding received is taxable income; funding paid is a deductible cost).
        Note: funding_paid is already deducted in pnl_net at the trade level, but
        net_taxable uses pnl_gross as the taxable base to avoid double-deducting.

        tax_liability = net_taxable × TAX_RATE (15% — speculative income at user's
        marginal slab rate, new regime FY 2025-26; loss years = 0 liability with
        carry-forward available per §16.1 corrected).
        tds_advance_credit: always 0 for futures/options (TDS not applicable).
        net_withdrawal_amount: 80% of pnl_net_sum — human withdrawal share (§13.4).

        Does NOT write to tax_reserve — Module 8 handles the reserve credit.
        """
        month_start = int(datetime(now_utc.year, now_utc.month, 1,
                                   tzinfo=timezone.utc).timestamp())
        month_end   = int(time.time())
        month_label = f'{now_utc.year}-{now_utc.month:02d}'

        with get_connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(pnl_gross),        0.0) AS gross,
                          COALESCE(SUM(pnl_net),          0.0) AS net,
                          COALESCE(SUM(tds_deducted),     0.0) AS tds,
                          COALESCE(SUM(fees),              0.0) AS fees,
                          COALESCE(SUM(funding_paid),     0.0) AS f_paid,
                          COALESCE(SUM(funding_received), 0.0) AS f_recv,
                          COUNT(*) AS trade_count
                   FROM trades
                   WHERE status          = 'closed'
                     AND pnl_net        IS NOT NULL
                     AND exit_timestamp >= ?
                     AND exit_timestamp <= ?""",
                (month_start, month_end)
            ).fetchone()

        gross       = float(row['gross'])
        net         = float(row['net'])
        tds_total   = float(row['tds'])   # always 0 for futures/options
        fees_total  = float(row['fees'])
        f_paid      = float(row['f_paid'])
        f_recv      = float(row['f_recv'])
        trade_count = int(row['trade_count'])

        # §9.3: funding received = taxable income; funding paid = deductible cost
        # net_taxable = this month's taxable contribution
        net_taxable = round(gross + f_recv - f_paid, 2)

        # ── Dynamic slab tax (incremental) ───────────────────────────────────
        # Compute tax on YTD-including-this-month minus tax on YTD-before-this-month.
        # This correctly handles slab transitions across months — e.g. if cumulative
        # profit crosses Rs 4L in month 3, only the portion above 4L gets taxed at 5%.
        fy_start_ts = self._get_fy_start_ts()
        with get_connection() as conn:
            ytd_row = conn.execute(
                """SELECT COALESCE(SUM(pnl_gross + funding_received - funding_paid), 0.0)
                          AS ytd_taxable
                   FROM trades
                   WHERE status = 'closed'
                     AND pnl_net IS NOT NULL
                     AND exit_timestamp >= ?
                     AND exit_timestamp <= ?""",
                (fy_start_ts, month_end)
            ).fetchone()
        ytd_taxable = float(ytd_row['ytd_taxable'])

        # Refresh BASE_INCOME_INR from env each calculation cycle (may change FY to FY)
        base_income     = float(os.environ.get('KRONOS_BASE_INCOME_INR', str(BASE_INCOME_INR)))
        ytd_prior       = ytd_taxable - net_taxable          # YTD before this month

        income_with     = base_income + max(0.0, ytd_taxable)
        income_prior    = base_income + max(0.0, ytd_prior)
        tax_ytd_total   = _total_tax(income_with)            # cumulative FY liability
        tax_ytd_prior   = _total_tax(income_prior)           # liability before this month
        tax_liability   = round(max(0.0, tax_ytd_total - tax_ytd_prior), 2)
        effective_rate  = _marginal_rate(income_with)        # rate at current income level

        tax_reserve_bal       = self._get_tax_reserve_balance()
        remaining_liability   = tax_liability                 # no TDS credit for futures
        net_withdrawal_amount = round(max(0.0, net) * 0.80, 2)

        log_event(MODULE, 'info', 'monthly_tax_summary',
                  f'Month {month_label}: gross=INR {gross:.2f} '
                  f'net_taxable=INR {net_taxable:.2f} '
                  f'ytd_taxable=INR {ytd_taxable:.2f} '
                  f'base_income=INR {base_income:.0f} '
                  f'marginal_rate={effective_rate:.1%} '
                  f'tax_liability=INR {tax_liability:.2f}',
                  {'month':                 month_label,
                   'gross_pnl':             round(gross, 2),
                   'pnl_net_sum':           round(net, 2),
                   'tds_total':             0.0,
                   'fees_total':            round(fees_total, 2),
                   'funding_paid':          round(f_paid, 2),
                   'funding_received':      round(f_recv, 2),
                   'net_taxable_income':    net_taxable,
                   'ytd_taxable_income':    round(ytd_taxable, 2),
                   'base_income_inr':       round(base_income, 2),
                   'total_income_ytd':      round(income_with, 2),
                   'effective_marginal_rate': effective_rate,
                   'tax_liability_30pct':   tax_liability,   # key retained for compat
                   'tax_liability':         tax_liability,
                   'tds_advance_credit':    0.0,
                   'remaining_liability':   remaining_liability,
                   'tax_reserve_balance':   round(tax_reserve_bal, 2),
                   'net_withdrawal_amount': net_withdrawal_amount,
                   'trade_count':           trade_count})
        log.info('Monthly tax summary %s: net_taxable=INR %.2f ytd=INR %.2f '
                 'base=INR %.0f marginal=%.1f%% liability=INR %.2f reserve=INR %.2f',
                 month_label, net_taxable, ytd_taxable, base_income,
                 effective_rate * 100, tax_liability, tax_reserve_bal)

    # ── Recuperation ledger ───────────────────────────────────────────────────

    def _generate_recuperation_ledger(self, now_utc: datetime) -> None:
        """
        Monthly recuperation ledger per §16.2: cumulative withdrawals vs
        INR 1,00,000 starting capital. Phase transition tracker.

        Reads withdrawal_made events written by the human withdrawal flow
        (Module 10 / manual admin) and sums the 'amount' field from each payload.
        """
        month_label = f'{now_utc.year}-{now_utc.month:02d}'

        with get_connection() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(CAST(json_extract(data,'$.amount') AS REAL)), 0.0) AS total
                   FROM events WHERE event_type='withdrawal_made'"""
            ).fetchone()
        cumulative = round(float(row['total']) if row else 0.0, 2)

        remaining        = round(max(0.0, STARTING_CAPITAL_INR - cumulative), 2)
        recuperation_pct = round(min(100.0, cumulative / STARTING_CAPITAL_INR * 100), 2)

        log_event(MODULE, 'info', 'recuperation_ledger',
                  f'Month {month_label}: cumulative_withdrawals=INR {cumulative:.2f} '
                  f'({recuperation_pct:.1f}% of INR 1,00,000)',
                  {'month':                  month_label,
                   'cumulative_withdrawals': cumulative,
                   'target_recuperation':    STARTING_CAPITAL_INR,
                   'remaining':              remaining,
                   'recuperation_pct':       recuperation_pct})
        log.info('Recuperation ledger %s: withdrawn=INR %.2f (%.1f%%) remaining=INR %.2f',
                 month_label, cumulative, recuperation_pct, remaining)

    # ── Advance tax alert (March 15) ──────────────────────────────────────────

    def _is_advance_tax_date(self) -> bool:
        """True on March 15 UTC — advance tax review deadline (§16.3)."""
        now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        return now_utc.month == 3 and now_utc.day == 15

    def _advance_tax_alert_already_sent_this_year(self) -> bool:
        now_utc     = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        march_start = int(datetime(now_utc.year, 3, 1, tzinfo=timezone.utc).timestamp())
        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM events WHERE event_type='advance_tax_alert' AND timestamp>=? LIMIT 1",
                (march_start,)
            ).fetchone()
        return row is not None

    def _send_advance_tax_alert(self, now_utc: datetime) -> None:
        """
        Advance tax reminder per §16.3: March 15 — review tax reserve balance
        and pay advance tax if annual liability has been triggered.

        Computes YTD tax liability directly from the trades table using the
        current slab engine — more accurate than summing past monthly_tax_summary
        payloads (which may have used different rates in earlier months).
        """
        now_ts      = int(time.time())
        fy_start_ts = self._get_fy_start_ts()
        fy_label    = self._get_fy_label()

        with get_connection() as conn:
            ytd_row = conn.execute(
                """SELECT COALESCE(SUM(pnl_gross + funding_received - funding_paid), 0.0)
                          AS ytd_taxable
                   FROM trades
                   WHERE status = 'closed'
                     AND pnl_net IS NOT NULL
                     AND exit_timestamp >= ?
                     AND exit_timestamp <= ?""",
                (fy_start_ts, now_ts)
            ).fetchone()
        ytd_taxable = float(ytd_row['ytd_taxable'])

        base_income     = float(os.environ.get('KRONOS_BASE_INCOME_INR', str(BASE_INCOME_INR)))
        total_income    = base_income + max(0.0, ytd_taxable)
        ytd_liability   = _total_tax(total_income) - _total_tax(base_income)
        ytd_liability   = round(max(0.0, ytd_liability), 2)
        effective_rate  = _marginal_rate(total_income)
        advance_tax_due = ytd_liability  # no TDS credit for futures
        tax_reserve_bal = self._get_tax_reserve_balance()

        log_event(MODULE, 'info', 'advance_tax_alert',
                  f'March 15 advance tax reminder ({fy_label}): '
                  f'ytd_taxable=INR {ytd_taxable:.2f} '
                  f'base_income=INR {base_income:.0f} '
                  f'marginal_rate={effective_rate:.1%} '
                  f'YTD_liability=INR {ytd_liability:.2f} '
                  f'advance_due=INR {advance_tax_due:.2f}',
                  {'fiscal_year':           fy_label,
                   'ytd_taxable_income':    round(ytd_taxable, 2),
                   'base_income_inr':       round(base_income, 2),
                   'total_income_ytd':      round(total_income, 2),
                   'effective_marginal_rate': effective_rate,
                   'ytd_tax_liability':     ytd_liability,
                   'ytd_tds_credit':        0.0,
                   'advance_tax_due':       advance_tax_due,
                   'tax_reserve_balance':   round(tax_reserve_bal, 2),
                   'note': 'Human action required: review and pay advance tax if liability triggered'})
        log.info('Advance tax alert %s: ytd_taxable=INR %.2f base=INR %.0f '
                 'marginal=%.1f%% YTD_liability=INR %.2f advance_due=INR %.2f reserve=INR %.2f',
                 fy_label, ytd_taxable, base_income, effective_rate * 100,
                 ytd_liability, advance_tax_due, tax_reserve_bal)

    # ── Annual Schedule VDA report ─────────────────────────────────────────────

    def _is_itr_filing_date(self) -> bool:
        """
        True on July 31 UTC — ITR filing deadline per §16.3.
        The report covers the just-ended Indian FY (April 1 prior year → March 31).
        """
        now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        return now_utc.month == 7 and now_utc.day == 31

    def _annual_vda_already_run_this_year(self) -> bool:
        """
        Dedup check: True if an annual_vda_report event was already written
        since April 1 of the current year (covers one report per Indian FY).
        """
        now_utc    = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        year_start = int(datetime(now_utc.year, 4, 1, tzinfo=timezone.utc).timestamp())
        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM events WHERE event_type='annual_vda_report' AND timestamp>=? LIMIT 1",
                (year_start,)
            ).fetchone()
        return row is not None

    def _generate_annual_vda_report(self, now_utc: datetime) -> None:
        """
        Generate ITR Schedule VDA report for the just-ended Indian fiscal year
        (April 1 prev_year → March 31 this_year). Writes JSON to data/reports/
        and an annual_vda_report event per §16.3. Also generates TDS reconciliation.

        tax_liability = slab tax on (base_income + net_trading_gains) minus slab tax
        on base_income alone — incremental tax attributable to trading.
        Loss offset IS permitted for speculative income within the same FY.
        Section 87A rebate applied — zero tax if total income <= Rs 12L.

        One row per trade with cost of acquisition, sale value, and profit/loss.
        """
        fy_end_year   = now_utc.year
        fy_start_year = fy_end_year - 1
        fy_label      = f'FY{fy_start_year}-{str(fy_end_year)[-2:]}'
        fy_start_ts   = int(datetime(fy_start_year, 4, 1, tzinfo=timezone.utc).timestamp())
        fy_end_ts     = int(datetime(fy_end_year,   3, 31, 23, 59, 59,
                                     tzinfo=timezone.utc).timestamp())

        with get_connection() as conn:
            trades = conn.execute(
                """SELECT id, symbol, direction,
                          entry_price, exit_price, size_contracts,
                          entry_timestamp, exit_timestamp, exit_reason,
                          pnl_gross, pnl_net, tds_deducted, fees,
                          funding_paid, funding_received
                   FROM trades
                   WHERE status = 'closed'
                     AND exit_timestamp >= ? AND exit_timestamp <= ?
                   ORDER BY exit_timestamp ASC""",
                (fy_start_ts, fy_end_ts)
            ).fetchall()
            tds_row = conn.execute(
                """SELECT COALESCE(SUM(tds_amount), 0.0) AS total
                   FROM tds_log WHERE timestamp >= ? AND timestamp <= ?""",
                (fy_start_ts, fy_end_ts)
            ).fetchone()

        total_gains           = 0.0
        total_losses          = 0.0
        total_funding_received = 0.0
        trade_rows            = []
        for t in trades:
            pnl = float(t['pnl_gross'] or 0.0)
            if pnl >= 0:
                total_gains  += pnl
            else:
                total_losses += abs(pnl)
            # §9.3: funding_received is taxable income — include in VDA taxable base
            total_funding_received += float(t['funding_received'] or 0.0)
            trade_rows.append({
                'trade_id':         int(t['id']),
                'symbol':           t['symbol'],
                'direction':        t['direction'],
                'entry_price':      t['entry_price'],
                'exit_price':       t['exit_price'],
                'size_contracts':   t['size_contracts'],
                'entry_date':       (datetime.fromtimestamp(t['entry_timestamp'],
                                     tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                                     if t['entry_timestamp'] else None),
                'exit_date':        (datetime.fromtimestamp(t['exit_timestamp'],
                                     tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                                     if t['exit_timestamp'] else None),
                'exit_reason':      t['exit_reason'],
                'pnl_gross':        t['pnl_gross'],
                'pnl_net':          t['pnl_net'],
                'tds_deducted':     t['tds_deducted'],
                'fees':             t['fees'],
                'funding_paid':     t['funding_paid'],
                'funding_received': t['funding_received'],
            })

        tds_total = float(tds_row['total']) if tds_row else 0.0
        # §16.1: speculative income — loss offset allowed within same FY
        # §9.3: funding_received is taxable income
        net_gains        = round(total_gains - total_losses, 2)
        taxable_base     = net_gains + total_funding_received    # net after loss offset
        taxable_positive = max(0.0, taxable_base)

        # Dynamic slab tax: total_income = base_income + trading taxable
        base_income  = float(os.environ.get('KRONOS_BASE_INCOME_INR', str(BASE_INCOME_INR)))
        total_income = base_income + taxable_positive
        tax_liability = round(
            max(0.0, _total_tax(total_income) - _total_tax(base_income)), 2
        )
        effective_rate = _marginal_rate(total_income)

        report = {
            'report_type':  'Speculative Income — Indian Income Tax (Section 43(5))',
            'fiscal_year':  fy_label,
            'tax_regime':   'New regime Section 115BAC — slab rates with Section 87A rebate',
            'generated_at': datetime.fromtimestamp(time.time(),
                            tz=timezone.utc).isoformat(),
            'summary': {
                'total_gains_inr':            round(total_gains, 2),
                'total_losses_inr':           round(total_losses, 2),
                'total_funding_received_inr': round(total_funding_received, 2),
                'net_gains_inr':              round(net_gains, 2),
                'taxable_base_inr':           round(taxable_base, 2),
                'base_income_inr':            round(base_income, 2),
                'total_income_inr':           round(total_income, 2),
                'effective_marginal_rate':    effective_rate,
                'section_87a_limit':          SECTION_87A_LIMIT,
                'tax_liability':              tax_liability,
                'tax_liability_30pct':        tax_liability,   # key retained for compat
                'tds_deducted_total':         round(tds_total, 2),  # always 0 for futures
                'net_tax_due':                tax_liability,
                'trade_count':                len(trade_rows),
                'note': ('Loss carry-forward available if net_gains < 0. '
                         'Offset against other speculative income in same FY.'),
            },
            'trades': trade_rows,
        }

        os.makedirs(REPORT_DIR, exist_ok=True)
        report_path = os.path.join(REPORT_DIR, f'speculative_income_{fy_label}.json')
        with open(report_path, 'w', encoding='utf-8') as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)

        log_event(MODULE, 'info', 'annual_vda_report',
                  f'{fy_label} speculative income report: '
                  f'{len(trade_rows)} trades net_gains=INR {net_gains:.2f} '
                  f'base_income=INR {base_income:.0f} '
                  f'total_income=INR {total_income:.0f} '
                  f'marginal={effective_rate:.1%} tax=INR {tax_liability:.2f}',
                  {'fiscal_year':              fy_label,
                   'total_gains':              round(total_gains, 2),
                   'total_losses':             round(total_losses, 2),
                   'total_funding_received':   round(total_funding_received, 2),
                   'net_gains':                round(net_gains, 2),
                   'taxable_base':             round(taxable_base, 2),
                   'base_income_inr':          round(base_income, 2),
                   'total_income':             round(total_income, 2),
                   'effective_marginal_rate':  effective_rate,
                   'tax_liability':            tax_liability,
                   'tds_total':                round(tds_total, 2),
                   'trade_count':              len(trade_rows),
                   'report_file':              report_path})
        log.info('Annual speculative income report %s: %d trades net_gains=INR %.2f '
                 'base=INR %.0f total_income=INR %.0f marginal=%.1f%% tax=INR %.2f',
                 fy_label, len(trade_rows), net_gains,
                 base_income, total_income, effective_rate * 100, tax_liability)

        # Generate TDS reconciliation alongside (shares FY data — §16.2: Annual)
        self._generate_tds_reconciliation(fy_label, fy_start_ts, fy_end_ts,
                                          round(tds_total, 2), tax_liability)

    def _generate_tds_reconciliation(self, fy_label: str, fy_start_ts: int,
                                     fy_end_ts: int, tds_deducted_total: float,
                                     tax_liability: float) -> None:
        """
        Annual TDS reconciliation per §16.2: total TDS deducted by exchange vs
        tax liability, for ITR offset claim. Generated alongside Schedule VDA report.
        """
        net_tax_due  = round(max(0.0, tax_liability - tds_deducted_total), 2)
        tds_surplus  = round(max(0.0, tds_deducted_total - tax_liability), 2)

        log_event(MODULE, 'info', 'tds_reconciliation',
                  f'{fy_label} TDS reconciliation: '
                  f'TDS_deducted=INR {tds_deducted_total:.2f} '
                  f'tax_liability=INR {tax_liability:.2f} '
                  f'net_tax_due=INR {net_tax_due:.2f}',
                  {'fiscal_year':         fy_label,
                   'tds_deducted_total':  tds_deducted_total,
                   'tax_liability_30pct': tax_liability,
                   'net_tax_due':         net_tax_due,
                   'tds_surplus':         tds_surplus,
                   'note': 'Cross-reference with TDS certificates from Delta Exchange for ITR filing'})
        log.info('TDS reconciliation %s: TDS=INR %.2f liability=INR %.2f net_due=INR %.2f',
                 fy_label, tds_deducted_total, tax_liability, net_tax_due)

    # ── Main cycle ─────────────────────────────────────────────────────────────

    async def _job_cycle(self) -> None:
        """Async scheduler wrapper — runs sync _run_cycle() in thread executor."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_cycle)

    def _run_cycle(self) -> None:
        """
        15-min tax processing cycle:
        1. Process all newly closed trades (pnl_net IS NULL)
        2. Generate monthly tax summary + recuperation ledger (last calendar day)
        3. Send advance tax alert (March 15)
        4. Generate annual Schedule VDA report + TDS reconciliation (July 31)
        """
        try:
            processed = self._process_unprocessed_trades()
            if processed:
                log.info('Tax Tracker: processed %d closed trade(s)', processed)

            now_utc = datetime.fromtimestamp(time.time(), tz=timezone.utc)

            if self._is_last_day_of_month() and not self._monthly_summary_already_run_today():
                self._generate_monthly_summary(now_utc)
                self._generate_recuperation_ledger(now_utc)

            if self._is_advance_tax_date() and not self._advance_tax_alert_already_sent_this_year():
                self._send_advance_tax_alert(now_utc)

            if self._is_itr_filing_date() and not self._annual_vda_already_run_this_year():
                self._generate_annual_vda_report(now_utc)

            log.debug('Tax Tracker cycle complete. Processed: %d trades.', processed)

        except Exception as exc:
            log.error('Tax Tracker cycle error: %s', exc, exc_info=True)
            log_event(MODULE, 'error', 'execution_error',
                      f'Tax cycle failed: {exc}', {'error': str(exc)})

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
        format='%(asctime)s %(levelname)s %(name)s -- %(message)s',
        stream=sys.stdout,
    )
    asyncio.run(TaxTracker().run())


if __name__ == '__main__':
    main()
