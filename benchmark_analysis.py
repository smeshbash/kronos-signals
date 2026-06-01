"""
Kronos Trading System — Benchmark Analysis
Run manually at week 6 of pre-live to compare Kronos-mini, Kronos-base, and
the custom model on directional accuracy.

Usage:
    python benchmark_analysis.py

Output:
    - Ranked accuracy table to stdout (overall + per confidence band)
    - CSV to data/reports/benchmark_YYYYMMDD.csv

Data source:
    All three models write to the shared `signals` table with distinct
    model_source values: 'custom' (M4), 'kronos-mini' (M13), 'kronos-base' (M14).
    The legacy `shadow_signals` table is no longer written to.
    Only non-pending, non-corrupted signals (quality_flag IS NULL) are evaluated.
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone

# Force UTF-8 output so box-drawing characters print correctly on Windows.
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

DB_PATH = os.environ.get(
    'KRONOS_DB_PATH',
    os.path.join(os.path.dirname(__file__), 'data', 'kronos.db'),
)

TIMEFRAME        = '4h'
HORIZON_SECONDS  = 24 * 3600   # 6 × 4H = 24H

CONFIDENCE_BANDS = [
    (0.0, 0.2),
    (0.2, 0.4),
    (0.4, 0.6),
    (0.6, 0.8),
    (0.8, 1.01),   # upper bound >1.0 catches confidence==1.0
]
BAND_LABELS = ['0.0–0.2', '0.2–0.4', '0.4–0.6', '0.6–0.8', '0.8–1.0']


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _close_before(conn, symbol: str, ts: int):
    """Latest 4H candle close at or before ts."""
    row = conn.execute(
        """SELECT close FROM ohlcv
           WHERE symbol=? AND timeframe=? AND timestamp <= ?
           ORDER BY timestamp DESC LIMIT 1""",
        (symbol, TIMEFRAME, ts),
    ).fetchone()
    return row['close'] if row else None


def _close_after(conn, symbol: str, ts: int):
    """Earliest 4H candle close at or after ts."""
    row = conn.execute(
        """SELECT close FROM ohlcv
           WHERE symbol=? AND timeframe=? AND timestamp >= ?
           ORDER BY timestamp ASC LIMIT 1""",
        (symbol, TIMEFRAME, ts),
    ).fetchone()
    return row['close'] if row else None


# ── Signal loaders ─────────────────────────────────────────────────────────────

def _fetch_signals_by_model(conn, regime_version: int) -> dict[str, list[dict]]:
    """
    Returns {model_source: [row_dict, ...]} for all generators.

    Reads from the shared `signals` table, grouped by model_source.

    Filters applied:
      - status NOT 'pending'      (M5 has evaluated the signal)
      - quality_flag IS NULL      (excludes test artifacts / corrupted records)
      - regime_version = N        (only signals from the target pipeline regime)

    regime_version controls which rule set the signals were generated under:
      v1 — trailing stop active, 1.17% return floor, no per-model confidence gate
      v2 — trailing stop disabled (paper), 2% return floor, kronos-base conf ≥ 0.4

    Pass regime_version=0 to include ALL versions (cross-regime comparison).

    confidence is CAST to REAL in the query to prevent bytes/blob return values
    that would cause a TypeError when comparing against float band boundaries.
    """
    regime_clause = (
        f"AND COALESCE(regime_version, 1) = {regime_version}"
        if regime_version > 0
        else ""
    )
    rows = conn.execute(
        f"""SELECT
               id,
               symbol,
               model_source,
               direction,
               CAST(confidence AS REAL) AS confidence,
               signal_timestamp,
               COALESCE(regime_version, 1) AS regime_version,
               status,
               rejection_reason,
               actual_return_pct
           FROM signals
           WHERE status     NOT IN ('pending')
             AND quality_flag IS NULL
             {regime_clause}
           ORDER BY signal_timestamp""",
    ).fetchall()

    result: dict[str, list] = {}
    for row in rows:
        ms = row['model_source'] or 'custom'   # NULL guard for pre-migration rows
        result.setdefault(ms, []).append(dict(row))
    return result


# ── Evaluation ─────────────────────────────────────────────────────────────────

def _evaluate(conn, signals: list[dict]) -> dict:
    """
    Returns {
        total:    int,
        resolved: int,
        correct:  int,
        accuracy: float | None,
        by_symbol: {symbol: {total, resolved, correct, accuracy}},
        bands:    {label: {total, correct, accuracy}},
    }
    """
    band_data  = {lbl: {'correct': 0, 'total': 0} for lbl in BAND_LABELS}
    sym_data:  dict[str, dict] = {}
    resolved   = 0
    correct    = 0

    for sig in signals:
        symbol    = str(sig['symbol'])
        direction = str(sig['direction'])
        ts        = int(sig['signal_timestamp'])
        conf      = float(sig['confidence'])   # guard: SQLite may return bytes/blob

        close_at    = _close_before(conn, symbol, ts)
        close_after = _close_after(conn, symbol, ts + HORIZON_SECONDS)

        if symbol not in sym_data:
            sym_data[symbol] = {'total': 0, 'resolved': 0, 'correct': 0}
        sym_data[symbol]['total'] += 1

        if close_at is None or close_after is None or close_at <= 0:
            continue

        resolved += 1
        sym_data[symbol]['resolved'] += 1

        actual_dir = 'long' if close_after > close_at else 'short'
        hit        = (direction == actual_dir)
        if hit:
            correct += 1
            sym_data[symbol]['correct'] += 1

        for i, (lo, hi) in enumerate(CONFIDENCE_BANDS):
            if lo <= conf < hi:
                band_data[BAND_LABELS[i]]['total'] += 1
                if hit:
                    band_data[BAND_LABELS[i]]['correct'] += 1
                break

    def _acc(c, t):
        return round(c / t, 4) if t > 0 else None

    for sd in sym_data.values():
        sd['accuracy'] = _acc(sd['correct'], sd['resolved'])
    for bd in band_data.values():
        bd['accuracy'] = _acc(bd['correct'], bd['total'])

    return {
        'total':     len(signals),
        'resolved':  resolved,
        'correct':   correct,
        'accuracy':  _acc(correct, resolved),
        'by_symbol': sym_data,
        'bands':     band_data,
    }


# ── Output ─────────────────────────────────────────────────────────────────────

def _normalize_rejection_reason(reason: str) -> str:
    """
    Collapse verbose rejection reason strings to their short type label.

    M5 writes full diagnostic strings like:
        "entry_cost_block: predicted return +1.107% does not cover TDS..."
        "confidence_block: kronos-base confidence 0.0997 below model minimum 0.4000"
        "duplicate_position_ETHUSD_short"
        "stop_loss_4h_blackout_BNBUSD"

    We want the type only so the breakdown table stays compact:
        "entry_cost_block"          (all return-floor / cost variants)
        "confidence_block"          (below model minimum gate)
        "duplicate_position"        (symbol stripped)
        "stop_loss_4h_blackout"     (symbol stripped)
    """
    # Strip everything after the first colon (e.g. verbose amount detail)
    colon_pos = reason.find(':')
    if colon_pos > 0:
        return reason[:colon_pos].strip()
    # Strip trailing asset identifiers like _ETHUSD, _BTCUSD_long, _BNBUSD_short, etc.
    return re.sub(r'_[A-Z]{2,5}USD(_(long|short))?$', '', reason)


def _print_signal_filter_summary(signals_by_model: dict[str, list]) -> None:
    """
    Print a per-model table showing total signals, how many were executed vs
    rejected, and the rejection rate.  Also breaks down rejection reasons to
    reveal whether M5 is over-filtering (confidence gate / return floor) or
    legitimately blocking (blackout, funding, stacking).
    """
    models = sorted(signals_by_model.keys())
    print(f"\n{'Model':<22} {'Total':>7} {'Executed':>9} {'Rejected':>9} {'Rej%':>7}")
    print('─' * 58)
    for m in models:
        sigs     = signals_by_model[m]
        total    = len(sigs)
        executed = sum(1 for s in sigs if s.get('status') == 'executed')
        rejected = total - executed
        rej_pct  = rejected / total * 100 if total else 0.0
        print(f"{m:<22} {total:>7} {executed:>9} {rejected:>9} {rej_pct:>6.0f}%")

    # ── Rejection reason breakdown ──────────────────────────────────────────
    # Shows WHY M5 blocks signals — distinguishes a too-tight confidence gate
    # or return floor (model problem / regime mismatch) from legitimate blocks
    # like blackout windows or correlation de-duplication (filter working fine).
    any_rejections = any(
        any(s.get('status') != 'executed' for s in sigs)
        for sigs in signals_by_model.values()
    )
    if not any_rejections:
        return

    print(f"\n  Rejection reason breakdown (rejected signals only):")
    for m in models:
        sigs     = signals_by_model[m]
        rejected = [s for s in sigs if s.get('status') != 'executed']
        if not rejected:
            print(f"  {m}: none rejected")
            continue
        reasons: dict[str, int] = {}
        for s in rejected:
            raw   = s.get('rejection_reason') or '(no reason recorded)'
            label = _normalize_rejection_reason(raw)
            reasons[label] = reasons.get(label, 0) + 1
        n = len(rejected)
        print(f"  {m} ({n} rejected):")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            pct = count / n * 100
            print(f"    {reason:<34} {count:>3}  ({pct:.0f}%)")


def _write_back_outcomes(conn, signals_by_model: dict[str, list]) -> int:
    """
    Compute and persist actual_return_pct for every resolved signal that does
    not yet have one.

    actual_return_pct = (close_at_horizon − close_at_signal) / close_at_signal × 100

    Interpretation:
      positive  → price rose over the 24H horizon
      negative  → price fell
      compare sign vs direction column to check hit/miss independently of
      the benchmark (e.g. SELECT * FROM signals WHERE actual_return_pct IS NOT NULL
      ORDER BY ABS(predicted_return_pct - actual_return_pct)).

    Signals are skipped when:
      - actual_return_pct is already populated (idempotent — safe to re-run)
      - OHLCV data for the horizon candle is not yet available (future signal)

    Returns the number of rows written.
    """
    updated = 0
    for signals in signals_by_model.values():
        for sig in signals:
            if sig.get('actual_return_pct') is not None:
                continue   # already written — idempotent skip

            symbol = str(sig['symbol'])
            ts     = int(sig['signal_timestamp'])

            close_at    = _close_before(conn, symbol, ts)
            close_after = _close_after(conn, symbol, ts + HORIZON_SECONDS)

            if close_at is None or close_after is None or close_at <= 0:
                continue   # horizon candle not yet available

            actual_return = round((close_after - close_at) / close_at * 100, 4)
            sig_id        = int(sig['id'])

            try:
                conn.execute(
                    'UPDATE signals SET actual_return_pct=? WHERE id=?',
                    (actual_return, sig_id),
                )
                updated += 1
            except Exception as exc:
                print(f"  Warning: could not write outcome for signal {sig_id}: {exc}")

    if updated:
        conn.commit()

    return updated


def _print_report(results: dict[str, dict], title: str = '') -> None:
    if title:
        print(f"\n  {title}")
    models = sorted(results.keys())

    print(f"\n{'Model':<18} {'Signals':>8} {'Resolved':>9} {'Correct':>8} {'Accuracy':>10}")
    print('─' * 57)
    for m in models:
        r   = results[m]
        acc = f"{r['accuracy']:.1%}" if r['accuracy'] is not None else 'N/A'
        print(f"{m:<18} {r['total']:>8} {r['resolved']:>9} {r['correct']:>8} {acc:>10}")

    print(f"\n{'Band':<12}", end='')
    for m in models:
        print(f"  {m:>18}", end='')
    print()
    print('─' * (12 + 20 * len(models)))

    for lbl in BAND_LABELS:
        print(f"{lbl:<12}", end='')
        for m in models:
            b = results[m]['bands'][lbl]
            if b['total'] == 0:
                cell = 'N/A'
            else:
                cell = f"{b['accuracy']:.1%} ({b['total']})"
            print(f"  {cell:>18}", end='')
        print()

    # Per-symbol breakdown
    all_symbols: set[str] = set()
    for r in results.values():
        all_symbols.update(r['by_symbol'].keys())

    if all_symbols:
        print(f"\n{'Symbol':<12}", end='')
        for m in models:
            print(f"  {m:>18}", end='')
        print()
        print('─' * (12 + 20 * len(models)))
        for sym in sorted(all_symbols):
            print(f"{sym:<12}", end='')
            for m in models:
                sd = results[m]['by_symbol'].get(sym)
                if sd is None or sd['resolved'] == 0:
                    cell = 'N/A'
                else:
                    cell = f"{sd['accuracy']:.1%} ({sd['resolved']})"
                print(f"  {cell:>18}", end='')
            print()


def _save_csv(results: dict[str, dict], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = ['model', 'scope', 'total', 'resolved', 'correct', 'accuracy']
    rows = []

    for model, r in results.items():
        rows.append({
            'model':    model,
            'scope':    'overall',
            'total':    r['total'],
            'resolved': r['resolved'],
            'correct':  r['correct'],
            'accuracy': r['accuracy'],
        })
        for lbl in BAND_LABELS:
            b = r['bands'][lbl]
            rows.append({
                'model':    model,
                'scope':    f'band:{lbl}',
                'total':    b['total'],
                'resolved': b['total'],
                'correct':  b['correct'],
                'accuracy': b['accuracy'],
            })
        for sym, sd in r['by_symbol'].items():
            rows.append({
                'model':    model,
                'scope':    f'symbol:{sym}',
                'total':    sd['total'],
                'resolved': sd['resolved'],
                'correct':  sd['correct'],
                'accuracy': sd['accuracy'],
            })

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nCSV saved: {output_path}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='Kronos benchmark analysis')
    parser.add_argument(
        '--all-versions', action='store_true',
        help='Include signals from ALL pipeline regime versions (default: current regime only)',
    )
    parser.add_argument(
        '--regime', type=int, default=None,
        help='Evaluate a specific regime version (overrides --all-versions)',
    )
    args = parser.parse_args()

    # Resolve which regime to query
    from db import SIGNAL_REGIME_VERSION as _CURRENT_REGIME
    if args.regime is not None:
        regime_version = args.regime
    elif args.all_versions:
        regime_version = 0   # 0 = no filter
    else:
        regime_version = _CURRENT_REGIME   # default: current regime only

    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    print(f"Database: {DB_PATH}")
    conn = _get_conn()

    signals_by_model = _fetch_signals_by_model(conn, regime_version)

    # Split into executed-only and all-signals pools.
    executed_by_model: dict[str, list] = {}
    for ms, sigs in signals_by_model.items():
        executed_by_model[ms] = [s for s in sigs if s.get('status') == 'executed']

    # Evaluate both pools.
    results_all:      dict[str, dict] = {}
    results_executed: dict[str, dict] = {}
    for model_source, signals in signals_by_model.items():
        results_all[model_source]      = _evaluate(conn, signals)
        results_executed[model_source] = _evaluate(conn, executed_by_model[model_source])

    # Write actual_return_pct back for all newly-resolved signals.
    # Must happen before conn.close() — runs idempotently (skips already-set rows).
    n_written = _write_back_outcomes(conn, signals_by_model)

    conn.close()

    any_resolved = any(r['resolved'] > 0 for r in results_all.values())
    if not any_resolved:
        regime_note = (
            f'regime v{regime_version}' if regime_version > 0
            else 'any regime'
        )
        print(f"No resolved signals found for {regime_note}. "
              f"Use --all-versions to include pre-regime data.")
        return

    regime_label = (
        f'Regime v{regime_version} only'
        if regime_version > 0
        else 'ALL regimes (--all-versions)'
    )
    stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    print(f"\n{'=' * 60}")
    print(f"  Shadow Benchmarking Report — {stamp}")
    print(f"  Pipeline filter: {regime_label}")
    if n_written:
        print(f"  Wrote actual_return_pct to {n_written} newly-resolved signal(s).")
    print(f"{'=' * 60}")

    # ── Signal filter summary ─────────────────────────────────────────────────
    print("\n── M5 Filter Summary (how many signals reach execution) ──")
    _print_signal_filter_summary(signals_by_model)

    # ── Primary: executed signals only ────────────────────────────────────────
    print("\n── PRIMARY: Executed Signals Only (what actually affects P&L) ──")
    print("   Use this to evaluate deployed system performance.")
    _print_report(results_executed)

    # ── Diagnostic: all signals ───────────────────────────────────────────────
    print("\n── DIAGNOSTIC: All Signals incl. Rejected (raw model capability) ──")
    print("   Use this to evaluate the model's prediction quality in isolation.")
    print("   WARNING: rejected signals with correlated predictions inflate accuracy.")
    _print_report(results_all)

    # ── Save CSV (all signals — preserves full history) ───────────────────────
    date_str    = datetime.now(timezone.utc).strftime('%Y%m%d')
    output_path = os.path.join(
        os.path.dirname(__file__), 'data', 'reports', f'benchmark_{date_str}.csv',
    )
    _save_csv(results_all, output_path)


if __name__ == '__main__':
    main()
