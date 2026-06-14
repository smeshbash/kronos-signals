"""
One-off backfill script — re-fetches last 500 4H OHLCV candles per symbol
from Delta Exchange India via CCXT and overwrites existing rows (including
the volume column) using INSERT OR REPLACE.

Purpose: normalise volume unit after the OHLCV feed changed units ~June 5-6,
2026. Historical rows stored via INSERT OR IGNORE still hold the old unit.
This script overwrites them with whatever the exchange currently returns for
those timestamps, bringing all rows onto a consistent unit.

Usage (on the server):
    KRONOS_DB_PATH=/app/kronos/data/kronos.db python3 backfill_ohlcv_4h.py

Dry-run (no writes):
    KRONOS_DB_PATH=/app/kronos/data/kronos.db python3 backfill_ohlcv_4h.py --dry-run
"""

import argparse
import os
import sqlite3
import sys
import time
from contextlib import contextmanager

import ccxt

# ── Config ─────────────────────────────────────────────────────────────────────

DELTA_REST_BASE  = 'https://api.india.delta.exchange'
OHLCV_TIMEFRAME  = '4h'
OHLCV_LIMIT      = 500   # 500 × 4H = ~83 days — covers all 20 same-hour ref slots

ASSETS = {
    'BTC': {'delta': 'BTCUSD', 'ccxt': 'BTC/USD:USD'},
    'ETH': {'delta': 'ETHUSD', 'ccxt': 'ETH/USD:USD'},
    'BNB': {'delta': 'BNBUSD', 'ccxt': 'BNB/USD:USD'},
    'XRP': {'delta': 'XRPUSD', 'ccxt': 'XRP/USD:USD'},
}

# Candles to spot-check before and after (BTC 4H at 00:00 UTC around unit change)
# Jun 4 00:00 UTC = 1780531200  (old unit — should be millions before, thousands after)
# Jun 6 00:00 UTC = 1780704000  (transition)
# Jun 13 00:00 UTC = 1781308800 (new unit — reference point)
SPOT_CHECK = {
    'BTCUSD': [1780531200, 1780617600, 1780704000, 1781308800],
}

DB_PATH = os.environ.get('KRONOS_DB_PATH', '')

# ── DB helpers ─────────────────────────────────────────────────────────────────

@contextmanager
def get_conn(path: str):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=5000')
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_spot(path: str) -> dict:
    """Read spot-check candles from DB. Returns {(symbol, ts): volume}."""
    result = {}
    with get_conn(path) as conn:
        for sym, tss in SPOT_CHECK.items():
            for ts in tss:
                row = conn.execute(
                    "SELECT volume FROM ohlcv WHERE symbol=? AND timeframe='4h' AND timestamp=?",
                    (sym, ts),
                ).fetchone()
                result[(sym, ts)] = float(row['volume']) if row else None
    return result


def ts_label(ts: int) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M')


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Backfill 4H OHLCV from Delta Exchange India')
    parser.add_argument('--dry-run', action='store_true',
                        help='Fetch candles but do not write to DB')
    args = parser.parse_args()

    if not DB_PATH:
        print('ERROR: KRONOS_DB_PATH env var is not set.')
        print('Run: KRONOS_DB_PATH=/app/kronos/data/kronos.db python3 backfill_ohlcv_4h.py')
        sys.exit(1)

    if not os.path.exists(DB_PATH):
        print(f'ERROR: DB not found at {DB_PATH}')
        sys.exit(1)

    print(f'DB: {DB_PATH}')
    print(f'Mode: {"DRY RUN (no writes)" if args.dry_run else "LIVE (will overwrite rows)"}')
    print()

    # ── Spot-check: before ──────────────────────────────────────────────────────
    before = read_spot(DB_PATH)
    print('=== Spot-check BEFORE backfill (BTC 4H 00:00 UTC) ===')
    for (sym, ts), vol in before.items():
        label = ts_label(ts)
        print(f'  {sym} {label}: {vol:,.0f}' if vol is not None else f'  {sym} {label}: (not in DB)')
    print()

    # ── CCXT exchange ──────────────────────────────────────────────────────────
    exchange = ccxt.delta({'options': {'defaultType': 'swap'}})
    exchange.urls['api']['public'] = DELTA_REST_BASE

    total_rows = 0
    results = {}

    for asset, info in ASSETS.items():
        ccxt_sym  = info['ccxt']
        delta_sym = info['delta']

        print(f'Fetching {asset} ({ccxt_sym}) — limit={OHLCV_LIMIT} ...')
        try:
            candles = exchange.fetch_ohlcv(ccxt_sym, OHLCV_TIMEFRAME, limit=OHLCV_LIMIT)
        except Exception as exc:
            print(f'  ERROR fetching {asset}: {exc}')
            results[asset] = 'FETCH_ERROR'
            continue

        if not candles:
            print(f'  WARNING: no candles returned for {asset}')
            results[asset] = 'EMPTY'
            continue

        rows = [
            (delta_sym, OHLCV_TIMEFRAME, c[0] // 1000, c[1], c[2], c[3], c[4], c[5])
            for c in candles
        ]

        oldest = ts_label(rows[0][2])
        newest = ts_label(rows[-1][2])
        min_vol = min(r[7] for r in rows)
        max_vol = max(r[7] for r in rows)
        print(f'  {len(rows)} candles: {oldest} → {newest}')
        print(f'  Volume range: {min_vol:,.0f} – {max_vol:,.0f}')

        if not args.dry_run:
            with get_conn(DB_PATH) as conn:
                conn.executemany(
                    """INSERT OR REPLACE INTO ohlcv
                       (symbol, timeframe, timestamp, open, high, low, close, volume)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    rows,
                )
            print(f'  Written {len(rows)} rows to DB.')
        else:
            print(f'  DRY RUN: skipped write.')

        total_rows += len(rows)
        results[asset] = f'OK ({len(rows)} rows)'
        time.sleep(0.5)   # gentle rate limiting between symbols

    # ── Spot-check: after ──────────────────────────────────────────────────────
    print()
    if not args.dry_run:
        after = read_spot(DB_PATH)
        print('=== Spot-check AFTER backfill (BTC 4H 00:00 UTC) ===')
        for (sym, ts), vol_before in before.items():
            vol_after = after.get((sym, ts))
            label = ts_label(ts)
            changed = '  <-- CHANGED' if vol_before != vol_after else ''
            before_str = f'{vol_before:,.0f}' if vol_before is not None else 'missing'
            after_str  = f'{vol_after:,.0f}'  if vol_after  is not None else 'missing'
            print(f'  {sym} {label}: {before_str} → {after_str}{changed}')
        print()

    # ── Summary ─────────────────────────────────────────────────────────────────
    print('=== Summary ===')
    for asset, status in results.items():
        print(f'  {asset}: {status}')
    print(f'  Total rows processed: {total_rows}')
    print()

    if args.dry_run:
        print('DRY RUN complete — no data was changed.')
        print('Remove --dry-run to apply the backfill.')
    else:
        print('Backfill complete.')
        print('Now check the before/after volumes above.')
        print('If June 4 BTC volume changed from ~millions to ~thousands, the unit is normalised.')
        print('If June 4 is still millions, Delta serves old-unit history — use the cutoff fix instead.')


if __name__ == '__main__':
    main()
