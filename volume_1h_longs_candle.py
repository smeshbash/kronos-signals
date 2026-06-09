"""
Split 1H LONG signals by 4H candle state: bearish / neutral / bullish.
Neutral = candle body < 30% of total range.
Shows what each state produces with and without RVOL filters.
"""
import sqlite3, sys, math
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

HIT_THR            = 0.15
AVG_PERIOD         = 20
NEUTRAL_BODY_RATIO = 0.30

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

signals = conn.execute("""
    SELECT s.id, s.symbol, s.direction, s.model_source,
           s.signal_timestamp, s.actual_return_pct,
           t.pnl_net, t.pnl_gross
    FROM signals s
    LEFT JOIN trades t ON t.signal_id = s.id
    WHERE s.status = 'executed'
      AND s.actual_return_pct IS NOT NULL
      AND s.quality_flag IS NULL
      AND s.model_source IN ('kronos-mini', 'kronos-base')
      AND s.direction = 'long'
    ORDER BY s.signal_timestamp
""").fetchall()

rows = []
for sig in signals:
    ts = int(sig['signal_timestamp'])

    h1 = conn.execute("""
        SELECT volume FROM ohlcv
        WHERE symbol=? AND timeframe='1h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT ?
    """, (sig['symbol'], ts, AVG_PERIOD+1)).fetchall()
    if len(h1) < 2: continue
    avg_vol = sum(float(c['volume']) for c in h1[1:]) / len(h1[1:])
    if avg_vol <= 0: continue
    rvol = float(h1[0]['volume']) / avg_vol

    h4 = conn.execute("""
        SELECT open, high, low, close FROM ohlcv
        WHERE symbol=? AND timeframe='4h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT 2
    """, (sig['symbol'], ts)).fetchall()
    if not h4: continue

    o = float(h4[0]['open'])
    h = float(h4[0]['high'])
    l = float(h4[0]['low'])
    c = float(h4[0]['close'])
    body       = abs(c - o)
    rng        = h - l
    body_ratio = body / rng if rng > 0 else 1.0

    if body_ratio < NEUTRAL_BODY_RATIO:
        state = 'neutral'
    elif c > o:
        state = 'bullish'
    else:
        state = 'bearish'

    ret     = float(sig['actual_return_pct'])
    correct = ret > HIT_THR
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)

    rows.append(dict(
        symbol=sig['symbol'], model=sig['model_source'],
        rvol=rvol, correct=correct, pnl=pnl,
        state=state, body_ratio=body_ratio,
    ))
conn.close()

n = len(rows)

def wilson_ci(wins, total, z=1.96):
    if total == 0: return (0, 0)
    p = wins / total
    lo = (p + z*z/(2*total) - z*math.sqrt((p*(1-p)+z*z/(4*total))/total)) / (1+z*z/total)
    hi = (p + z*z/(2*total) + z*math.sqrt((p*(1-p)+z*z/(4*total))/total)) / (1+z*z/total)
    return max(0,lo)*100, min(100,hi)*100

def show(label, subset):
    if not subset:
        print(f"  {label:50}  n=  0  —")
        return
    wins = sum(1 for r in subset if r['correct'])
    wr   = wins / len(subset) * 100
    ev   = sum(r['pnl'] for r in subset) / len(subset)
    tot  = sum(r['pnl'] for r in subset)
    lo, hi = wilson_ci(wins, len(subset))
    print(f"  {label:50}  n={len(subset):>3}  WR={wr:>5.1f}%  "
          f"95%CI=[{lo:.0f}%-{hi:.0f}%]  EV=Rs {ev:>+8.1f}  Total=Rs {tot:>+8.1f}")

print(f"1H LONG signals: {n}")
counts = {s: sum(1 for r in rows if r['state']==s) for s in ('bearish','neutral','bullish')}
print(f"  bearish 4H: {counts['bearish']}  neutral 4H: {counts['neutral']}  bullish 4H: {counts['bullish']}")
print()

for state in ('bullish', 'neutral', 'bearish'):
    grp = [r for r in rows if r['state'] == state]
    print("=" * 100)
    print(f"  4H CANDLE: {state.upper()}  ({len(grp)} longs)")
    print("=" * 100)
    show("No filter",               grp)
    show("RVOL < 0.75x  (low)",     [r for r in grp if r['rvol'] < 0.75])
    show("RVOL 0.75x – 1.50x",      [r for r in grp if 0.75 <= r['rvol'] <= 1.50])
    show("RVOL 0.75x – 2.00x",      [r for r in grp if 0.75 <= r['rvol'] <= 2.00])
    show("RVOL 1.00x – 1.50x",      [r for r in grp if 1.00 <= r['rvol'] <= 1.50])
    show("RVOL 1.00x – 2.00x",      [r for r in grp if 1.00 <= r['rvol'] <= 2.00])
    show("RVOL >= 1.50x",            [r for r in grp if r['rvol'] >= 1.50])
    show("RVOL >= 2.00x (high)",     [r for r in grp if r['rvol'] >= 2.00])
    print()

# ── Summary ────────────────────────────────────────────────────────────────
print("=" * 100)
print("  SUMMARY — all three states, all filter combos")
print("=" * 100)
print(f"  {'Label':52}  {'N':>4}  {'WR':>6}  {'95% CI':>16}  {'EV/trade':>10}  {'Total PnL':>10}")
print("  " + "-"*96)
combos = [
    ("ALL longs — no filter",                    rows),
    ("Bullish — no filter",                      [r for r in rows if r['state']=='bullish']),
    ("Bullish — RVOL 0.75-1.50x",               [r for r in rows if r['state']=='bullish' and 0.75<=r['rvol']<=1.50]),
    ("Bullish — RVOL 0.75-2.00x",               [r for r in rows if r['state']=='bullish' and 0.75<=r['rvol']<=2.00]),
    ("Bullish — RVOL 1.00-1.50x",               [r for r in rows if r['state']=='bullish' and 1.00<=r['rvol']<=1.50]),
    ("Bullish — RVOL < 2.0x  (no floor)",       [r for r in rows if r['state']=='bullish' and r['rvol'] < 2.00]),
    ("Neutral — no filter",                      [r for r in rows if r['state']=='neutral']),
    ("Neutral — RVOL 0.75-1.50x",               [r for r in rows if r['state']=='neutral' and 0.75<=r['rvol']<=1.50]),
    ("Neutral — RVOL < 2.0x",                   [r for r in rows if r['state']=='neutral' and r['rvol']<2.00]),
    ("Bearish — no filter",                      [r for r in rows if r['state']=='bearish']),
    ("Bearish — RVOL 0.75-1.50x",               [r for r in rows if r['state']=='bearish' and 0.75<=r['rvol']<=1.50]),
]
for label, subset in combos:
    if not subset:
        print(f"  {label:52}  {'n=0':>4}"); continue
    wins = sum(1 for r in subset if r['correct'])
    wr   = wins / len(subset) * 100
    ev   = sum(r['pnl'] for r in subset) / len(subset)
    tot  = sum(r['pnl'] for r in subset)
    lo, hi = wilson_ci(wins, len(subset))
    print(f"  {label:52}  {len(subset):>4}  {wr:>5.1f}%  [{lo:>4.1f}%–{hi:>4.1f}%]  "
          f"Rs {ev:>+8.1f}  Rs {tot:>+8.1f}")

# ── Per-symbol for bullish candle longs ────────────────────────────────────
print()
print("=" * 100)
print("  PER-SYMBOL — bullish 4H candle longs  (where does edge/damage come from?)")
print("=" * 100)
bull_longs = [r for r in rows if r['state']=='bullish']
for sym in sorted(set(r['symbol'] for r in bull_longs)):
    sr = [r for r in bull_longs if r['symbol']==sym]
    show(sym, sr)
