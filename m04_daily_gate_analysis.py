"""
M04 Custom Model — Synthetic Daily Gate Analysis
=================================================
Tests whether a bullish-daily gate on M04 longs would have improved
performance across all regime versions (v1–v5).

Methodology mirrors volume_base_4h_analysis.py:
  - Synthetic daily = last 6 × 4H bars grouped as single OHLC (= 24H)
  - Bullish:  close > open AND body >= 30% of range
  - Bearish:  close < open AND body >= 30% of range
  - Neutral:  body < 30% of range

Run on Linux machine:
  sudo -u kronos /app/kronos/venv/bin/python3 m04_daily_gate_analysis.py
"""
import sqlite3, sys, datetime
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

NEUTRAL_BODY_RATIO = 0.30
HIT_THR            = 0.15   # % — directional accuracy threshold

SEP  = '=' * 72
SEP2 = '-' * 72

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ── Pull all resolved M04 signals across every regime version ─────────────────
signals = conn.execute("""
    SELECT s.id, s.symbol, s.direction, s.model_source,
           s.signal_timestamp, s.actual_return_pct,
           COALESCE(s.regime_version, 1) AS regime_version,
           t.pnl_net, t.pnl_gross
    FROM signals s
    LEFT JOIN trades t ON t.signal_id = s.id
    WHERE s.model_source = 'custom'
      AND s.actual_return_pct IS NOT NULL
      AND s.quality_flag IS NULL
    ORDER BY s.signal_timestamp
""").fetchall()

print(SEP)
print('  M04 CUSTOM — SYNTHETIC DAILY GATE ANALYSIS (all regime versions)')
print(f'  {datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")}')
print(SEP)
print(f'\n  Total resolved M04 signals: {len(signals)}')

# ── Compute daily state for each signal ───────────────────────────────────────
rows = []
skipped = 0

for sig in signals:
    ts  = int(sig['signal_timestamp'])
    sym = sig['symbol']

    h4c = conn.execute("""
        SELECT open, high, low, close FROM ohlcv
        WHERE symbol=? AND timeframe='4h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT 6
    """, (sym, ts)).fetchall()

    if len(h4c) < 6:
        skipped += 1
        continue

    day_open  = float(h4c[-1]['open'])
    day_close = float(h4c[0]['close'])
    day_high  = max(float(r['high']) for r in h4c)
    day_low   = min(float(r['low']  ) for r in h4c)
    day_rng   = day_high - day_low
    body_ratio = abs(day_close - day_open) / day_rng if day_rng > 0 else 1.0

    if body_ratio < NEUTRAL_BODY_RATIO:
        daily_state = 'neutral'
    elif day_close > day_open:
        daily_state = 'bullish'
    else:
        daily_state = 'bearish'

    ret     = float(sig['actual_return_pct'])
    drx     = sig['direction']
    correct = (ret > HIT_THR) if drx == 'long' else (ret < -HIT_THR)
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)

    rows.append(dict(
        regime  = sig['regime_version'],
        symbol  = sym,
        direction = drx,
        daily_state = daily_state,
        ret     = ret,
        correct = correct,
        pnl     = pnl,
    ))

conn.close()

if skipped:
    print(f'  Skipped (insufficient 4H OHLCV): {skipped}')
print(f'  Analysed: {len(rows)}\n')

# ── Helper ────────────────────────────────────────────────────────────────────
def stats(subset):
    n = len(subset)
    if n == 0:
        return n, '--', '--'
    wr  = sum(1 for r in subset if r['correct']) / n * 100
    ev  = sum(r['pnl'] for r in subset) / n
    return n, f'{wr:.1f}%', f'{ev:+.0f}'

def section(title):
    print(f'\n{SEP}')
    print(f'  {title}')
    print(SEP)

# ── 1. Overall M04 by direction ───────────────────────────────────────────────
section('1. OVERALL M04 — ALL REGIMES, BY DIRECTION')
for drx in ('long', 'short', 'all'):
    subset = [r for r in rows if drx == 'all' or r['direction'] == drx]
    n, wr, ev = stats(subset)
    print(f'  {drx:<6}  n={n:<4}  WR={wr:<8}  EV={ev} Rs/trade')

# ── 2. By regime version ──────────────────────────────────────────────────────
section('2. BY REGIME VERSION × DIRECTION')
for rv in sorted(set(r['regime'] for r in rows)):
    rv_rows = [r for r in rows if r['regime'] == rv]
    n_all, wr_all, ev_all = stats(rv_rows)
    print(f'\n  v{rv}  (n={n_all}  WR={wr_all}  EV={ev_all} Rs/trade)')
    for drx in ('long', 'short'):
        subset = [r for r in rv_rows if r['direction'] == drx]
        n, wr, ev = stats(subset)
        print(f'    {drx:<6}  n={n:<4}  WR={wr:<8}  EV={ev} Rs/trade')

# ── 3. Longs by daily state (all regimes) ─────────────────────────────────────
section('3. M04 LONGS — BY SYNTHETIC DAILY STATE (all regimes combined)')
longs = [r for r in rows if r['direction'] == 'long']
print(f'  {"Daily state":<12}  {"n":<5}  {"WR":<8}  {"EV Rs/trade"}')
print(f'  {SEP2}')
for state in ('bullish', 'neutral', 'bearish'):
    subset = [r for r in longs if r['daily_state'] == state]
    n, wr, ev = stats(subset)
    print(f'  {state:<12}  {n:<5}  {wr:<8}  {ev}')

# ── 4. Longs by daily state PER regime version ────────────────────────────────
section('4. M04 LONGS — DAILY STATE × REGIME VERSION')
print(f'  {"Regime":<8}  {"Daily state":<12}  {"n":<5}  {"WR":<8}  {"EV Rs/trade"}')
print(f'  {SEP2}')
for rv in sorted(set(r['regime'] for r in rows)):
    rv_longs = [r for r in longs if r['regime'] == rv]
    if not rv_longs:
        continue
    for state in ('bullish', 'neutral', 'bearish'):
        subset = [r for r in rv_longs if r['daily_state'] == state]
        n, wr, ev = stats(subset)
        if n > 0:
            print(f'  v{rv:<7}  {state:<12}  {n:<5}  {wr:<8}  {ev}')
    print()

# ── 5. Gate simulation: block longs on non-bullish daily ──────────────────────
section('5. GATE SIMULATION — block M04 longs unless daily is bullish')
all_longs  = longs
gated_in   = [r for r in longs if r['daily_state'] == 'bullish']
gated_out  = [r for r in longs if r['daily_state'] != 'bullish']

print(f'\n  Without gate (all longs):')
n, wr, ev = stats(all_longs)
print(f'    n={n}  WR={wr}  EV={ev} Rs/trade  Total PnL={sum(r["pnl"] for r in all_longs):+.0f}')

print(f'\n  With gate (bullish daily only):')
n, wr, ev = stats(gated_in)
print(f'    n={n}  WR={wr}  EV={ev} Rs/trade  Total PnL={sum(r["pnl"] for r in gated_in):+.0f}')

print(f'\n  Blocked by gate (neutral + bearish daily):')
n, wr, ev = stats(gated_out)
print(f'    n={n}  WR={wr}  EV={ev} Rs/trade  Total PnL={sum(r["pnl"] for r in gated_out):+.0f}')

pnl_gain = sum(r['pnl'] for r in gated_in) - sum(r['pnl'] for r in all_longs)
print(f'\n  Gate PnL impact: {pnl_gain:+.0f} Rs '
      f'(positive = gate removes losing trades)')

# ── 6. Shorts by daily state (all regimes) ────────────────────────────────────
section('6. M04 SHORTS — BY SYNTHETIC DAILY STATE (all regimes combined)')
shorts = [r for r in rows if r['direction'] == 'short']
if shorts:
    print(f'  {"Daily state":<12}  {"n":<5}  {"WR":<8}  {"EV Rs/trade"}')
    print(f'  {SEP2}')
    for state in ('bullish', 'neutral', 'bearish'):
        subset = [r for r in shorts if r['daily_state'] == state]
        n, wr, ev = stats(subset)
        print(f'  {state:<12}  {n:<5}  {wr:<8}  {ev}')
else:
    print('  No short signals found (consistent with known long bias).')

# ── 7. Per-symbol breakdown for longs ─────────────────────────────────────────
section('7. M04 LONGS — PER SYMBOL × DAILY STATE')
for sym in sorted(set(r['symbol'] for r in longs)):
    sym_longs = [r for r in longs if r['symbol'] == sym]
    n_sym, wr_sym, ev_sym = stats(sym_longs)
    print(f'\n  {sym}  (n={n_sym}  WR={wr_sym}  EV={ev_sym} Rs/trade)')
    for state in ('bullish', 'neutral', 'bearish'):
        subset = [r for r in sym_longs if r['daily_state'] == state]
        n, wr, ev = stats(subset)
        if n > 0:
            print(f'    {state:<12}  n={n:<4}  WR={wr:<8}  EV={ev}')

print(f'\n{SEP}')
print('  END OF ANALYSIS')
print(SEP)
