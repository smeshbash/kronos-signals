"""
Per-model breakdown: kronos-mini vs kronos-base (both 1H).
Splits by direction AND 4H candle state for each model separately.
No pooling — each model stands on its own data.
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

    o = float(h4[0]['open']); h_ = float(h4[0]['high'])
    l = float(h4[0]['low']);  c  = float(h4[0]['close'])
    body  = abs(c - o)
    rng   = h_ - l
    ratio = body / rng if rng > 0 else 1.0

    if ratio < NEUTRAL_BODY_RATIO:
        state = 'neutral'
    elif c > o:
        state = 'bullish'
    else:
        state = 'bearish'

    ret     = float(sig['actual_return_pct'])
    drx     = sig['direction']
    correct = (ret > HIT_THR) if drx == 'long' else (ret < -HIT_THR)
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)

    rows.append(dict(
        model=sig['model_source'], symbol=sig['symbol'],
        direction=drx, rvol=rvol, correct=correct, pnl=pnl,
        state=state,
    ))
conn.close()

def wilson_ci(wins, n, z=1.96):
    if n == 0: return 0.0, 0.0
    p = wins / n
    lo = (p + z*z/(2*n) - z*math.sqrt((p*(1-p)+z*z/(4*n))/n)) / (1+z*z/n)
    hi = (p + z*z/(2*n) + z*math.sqrt((p*(1-p)+z*z/(4*n))/n)) / (1+z*z/n)
    return max(0,lo)*100, min(100,hi)*100

def row_str(label, subset, indent=4):
    pad = ' ' * indent
    if not subset:
        return f"{pad}{label:52}  n=  0"
    wins = sum(1 for r in subset if r['correct'])
    wr   = wins / len(subset) * 100
    ev   = sum(r['pnl'] for r in subset) / len(subset)
    tot  = sum(r['pnl'] for r in subset)
    lo, hi = wilson_ci(wins, len(subset))
    return (f"{pad}{label:52}  n={len(subset):>3}  WR={wr:>5.1f}%  "
            f"CI=[{lo:.0f}%-{hi:.0f}%]  EV=Rs {ev:>+8.1f}  Total=Rs {tot:>+9.1f}")

MODELS = ['kronos-mini', 'kronos-base']

for model in MODELS:
    mr = [r for r in rows if r['model'] == model]
    ml = [r for r in mr if r['direction'] == 'long']
    ms = [r for r in mr if r['direction'] == 'short']

    print()
    print("█" * 92)
    print(f"  MODEL: {model.upper()}   total signals={len(mr)}  longs={len(ml)}  shorts={len(ms)}")
    print("█" * 92)

    # ── LONGS ──────────────────────────────────────────────────────────────
    print()
    print(f"  ── LONGS ({len(ml)} total) ──────────────────────────────────────────")
    print(row_str("ALL longs — no filter", ml))
    print()
    for state in ('bullish', 'neutral', 'bearish'):
        grp = [r for r in ml if r['state'] == state]
        n_str = f"({len(grp)} signals)"
        print(f"    4H {state.upper()} {n_str}")
        print(row_str(f"  No filter", grp, 6))
        print(row_str(f"  RVOL 0.75x–1.50x", [r for r in grp if 0.75<=r['rvol']<=1.50], 6))
        print(row_str(f"  RVOL 0.75x–2.00x", [r for r in grp if 0.75<=r['rvol']<=2.00], 6))
        print(row_str(f"  RVOL < 2.0x (cap only)", [r for r in grp if r['rvol']<2.00], 6))

    # ── SHORTS ─────────────────────────────────────────────────────────────
    print()
    print(f"  ── SHORTS ({len(ms)} total) ──────────────────────────────────────────")
    print(row_str("ALL shorts — no filter", ms))
    print()
    for state in ('bearish', 'neutral', 'bullish'):
        grp = [r for r in ms if r['state'] == state]
        n_str = f"({len(grp)} signals)"
        print(f"    4H {state.upper()} {n_str}")
        print(row_str(f"  No filter",             grp, 6))
        print(row_str(f"  RVOL 0.75x–1.50x",     [r for r in grp if 0.75<=r['rvol']<=1.50], 6))
        print(row_str(f"  RVOL 0.75x–2.00x",     [r for r in grp if 0.75<=r['rvol']<=2.00], 6))
        print(row_str(f"  RVOL < 2.0x (cap only)",[r for r in grp if r['rvol']<2.00], 6))

# ── Cross-model comparison table ──────────────────────────────────────────
print()
print("=" * 92)
print("  CROSS-MODEL COMPARISON — key numbers side by side")
print("=" * 92)
print(f"  {'Metric':52}  {'kronos-mini':>22}  {'kronos-base':>22}")
print("  " + "-"*88)

def fmt(subset):
    if not subset: return f"{'n=0':>22}"
    wins = sum(1 for r in subset if r['correct'])
    wr   = wins / len(subset) * 100
    ev   = sum(r['pnl'] for r in subset) / len(subset)
    lo, hi = wilson_ci(wins, len(subset))
    return f"n={len(subset):>3} WR={wr:.0f}% EV=Rs{ev:>+7.0f}"

checks = [
    ("Longs — all, no filter",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='long']),
    ("Longs — 4H bullish, no filter",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='long' and r['state']=='bullish']),
    ("Longs — 4H bullish, RVOL 0.75-1.50x",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='long' and r['state']=='bullish' and 0.75<=r['rvol']<=1.50]),
    ("Longs — 4H neutral, no filter",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='long' and r['state']=='neutral']),
    ("Longs — 4H bearish, no filter",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='long' and r['state']=='bearish']),
    ("Shorts — all, no filter",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='short']),
    ("Shorts — 4H bearish + RVOL 0.75-1.50x",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='short' and r['state']=='bearish' and 0.75<=r['rvol']<=1.50]),
    ("Shorts — 4H neutral, no filter",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='short' and r['state']=='neutral']),
    ("Shorts — 4H bearish+neutral, RVOL 0.75-1.50x",
        lambda m: [r for r in rows if r['model']==m and r['direction']=='short' and r['state'] in ('bearish','neutral') and 0.75<=r['rvol']<=1.50]),
]

for label, fn in checks:
    mini = fn('kronos-mini')
    base = fn('kronos-base')
    print(f"  {label:52}  {fmt(mini):>22}  {fmt(base):>22}")
