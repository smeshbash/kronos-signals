"""
Kronos Trading System — Benchmark Analysis
Run manually at week 6 of pre-live to compare Kronos-mini, Kronos-base, and
the custom model on directional accuracy.

Usage:
    python benchmark_analysis.py

Output:
    - Ranked accuracy table to stdout (overall + per confidence band)
    - CSV to data/reports/benchmark_YYYYMMDD.csv
"""

import csv
import os
import sqlite3
from datetime import datetime, timezone

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

def _fetch_shadow_signals(conn) -> dict[str, list[dict]]:
    """Returns {model_name: [row_dict, ...]}"""
    rows = conn.execute(
        """SELECT symbol, model_name, direction, confidence, signal_timestamp
           FROM shadow_signals
           ORDER BY signal_timestamp""",
    ).fetchall()
    result: dict[str, list] = {}
    for row in rows:
        mn = row['model_name']
        result.setdefault(mn, []).append(dict(row))
    return result


def _fetch_custom_signals(conn) -> list[dict]:
    """Approved/executed signals from the custom model."""
    rows = conn.execute(
        """SELECT symbol, direction, confidence, signal_timestamp
           FROM signals
           WHERE status IN ('approved', 'executed')
           ORDER BY signal_timestamp""",
    ).fetchall()
    return [dict(r) for r in rows]


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
        symbol    = sig['symbol']
        direction = sig['direction']
        ts        = sig['signal_timestamp']
        conf      = sig['confidence']

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

def _print_report(results: dict[str, dict]) -> None:
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
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    print(f"Database: {DB_PATH}")
    conn = _get_conn()

    shadow_by_model = _fetch_shadow_signals(conn)
    custom_signals  = _fetch_custom_signals(conn)

    results: dict[str, dict] = {}
    for model_name, signals in shadow_by_model.items():
        results[model_name] = _evaluate(conn, signals)
    if custom_signals:
        results['custom'] = _evaluate(conn, custom_signals)

    conn.close()

    if not results:
        print("No signals found. Run shadow inference for at least one 4H cycle first.")
        return

    stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    print(f"\n{'=' * 60}")
    print(f"  Shadow Benchmarking Report — {stamp}")
    print(f"{'=' * 60}")

    _print_report(results)

    date_str    = datetime.now(timezone.utc).strftime('%Y%m%d')
    output_path = os.path.join(
        os.path.dirname(__file__), 'data', 'reports', f'benchmark_{date_str}.csv',
    )
    _save_csv(results, output_path)


if __name__ == '__main__':
    main()
