"""
Full analysis for kronos-mini-4h:
  1. 4H RVOL band analysis (same as 1H study)
  2. HTF direction from 4H data (no 1D candles in DB):
       a. Synthetic daily candle (last 6 × 4H = 24H grouped)
       b. 4H SMA-20  direction  (80H  ≈ 3.3 days — medium trend)
       c. 4H SMA-42  direction  (168H = 7 days  — weekly-equivalent trend)
       d. Daily momentum        (close vs 6 bars ago = 24H change)
  3. Filter combinations — longs and shorts separately
  4. Per-symbol breakdown
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
      AND s.model_source = 'kronos-mini-4h'
    ORDER BY s.signal_timestamp
""").fetchall()

rows = []
skipped = 0
for sig in signals:
    ts  = int(sig['signal_timestamp'])
    sym = sig['symbol']

    # ── 4H RVOL ──────────────────────────────────────────────────────────────
    h4v = conn.execute("""
        SELECT volume FROM ohlcv
        WHERE symbol=? AND timeframe='4h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT ?
    """, (sym, ts, AVG_PERIOD+1)).fetchall()
    if len(h4v) < 2:
        skipped += 1; continue
    avg_vol = sum(float(c['volume']) for c in h4v[1:]) / len(h4v[1:])
    if avg_vol <= 0:
        skipped += 1; continue
    rvol_4h = float(h4v[0]['volume']) / avg_vol

    # ── 4H candles for HTF analysis ───────────────────────────────────────────
    h4c = conn.execute("""
        SELECT open, high, low, close FROM ohlcv
        WHERE symbol=? AND timeframe='4h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT 50
    """, (sym, ts)).fetchall()
    if len(h4c) < 10:
        skipped += 1; continue

    closes = [float(r['close']) for r in h4c]
    curr_close = closes[0]

    # ── a. Synthetic daily candle (last 6 × 4H = 24H) ────────────────────────
    day_bars = h4c[:6]
    day_open  = float(day_bars[-1]['open'])
    day_close = float(day_bars[0]['close'])
    day_high  = max(float(r['high'])  for r in day_bars)
    day_low   = min(float(r['low'])   for r in day_bars)
    day_body  = abs(day_close - day_open)
    day_range = day_high - day_low
    day_ratio = day_body / day_range if day_range > 0 else 1.0
    if day_ratio < NEUTRAL_BODY_RATIO:
        daily_state = 'neutral'
    elif day_close > day_open:
        daily_state = 'bullish'
    else:
        daily_state = 'bearish'

    # ── b. 4H SMA-20 direction (80H ≈ 3.3 days) ─────────────────────────────
    sma20 = sum(closes[:20]) / 20
    above_sma20 = curr_close > sma20

    # ── c. 4H SMA-42 direction (168H = 7 days, weekly-equivalent) ────────────
    sma42 = sum(closes[:42]) / min(42, len(closes))
    above_sma42 = curr_close > sma42 if len(closes) >= 42 else None

    # ── d. Daily momentum (close vs 6 bars ago = 24H) ─────────────────────────
    daily_momentum_bull = closes[0] > closes[6] if len(closes) > 6 else None

    ret     = float(sig['actual_return_pct'])
    drx     = sig['direction']
    correct = (ret > HIT_THR) if drx == 'long' else (ret < -HIT_THR)
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)

    rows.append(dict(
        symbol=sym, direction=drx, rvol=rvol_4h,
        correct=correct, pnl=pnl,
        daily_state=daily_state,
        above_sma20=above_sma20,
        above_sma42=above_sma42,
        daily_momentum_bull=daily_momentum_bull,
    ))

conn.close()
n = len(rows)

def wilson_ci(wins, total, z=1.96):
    if total == 0: return 0.0, 0.0
    p = wins/total
    lo = (p+z*z/(2*total)-z*math.sqrt((p*(1-p)+z*z/(4*total))/total))/(1+z*z/total)
    hi = (p+z*z/(2*total)+z*math.sqrt((p*(1-p)+z*z/(4*total))/total))/(1+z*z/total)
    return max(0,lo)*100, min(100,hi)*100

def show(label, subset, indent=2):
    pad = ' '*indent
    if not subset:
        print(f"{pad}{label:54}  n=  0"); return
    wins = sum(1 for r in subset if r['correct'])
    wr   = wins/len(subset)*100
    ev   = sum(r['pnl'] for r in subset)/len(subset)
    tot  = sum(r['pnl'] for r in subset)
    lo,hi= wilson_ci(wins,len(subset))
    print(f"{pad}{label:54}  n={len(subset):>3}  WR={wr:>5.1f}%  "
          f"CI=[{lo:.0f}%-{hi:.0f}%]  EV=Rs {ev:>+8.1f}  Total=Rs {tot:>+9.1f}")

longs  = [r for r in rows if r['direction']=='long']
shorts = [r for r in rows if r['direction']=='short']

print(f"kronos-mini-4h resolved signals: {n}  (skipped {skipped})")
print(f"  longs: {len(longs)}   shorts: {len(shorts)}")
print()

# ════════════════════════════════════════════════════════════════════════
print("="*90)
print("  SECTION 1 — 4H RVOL BAND ANALYSIS (both directions)")
print("="*90)
show("ALL signals — no filter", rows)
print()
print("  Fine RVOL bands (all signals):")
bands = [(i*0.25,(i+1)*0.25) for i in range(0,12)]+[(3.0,999)]
for lo_,hi_ in bands:
    b=[r for r in rows if lo_<=r['rvol']<hi_]
    if not b: continue
    wr=sum(1 for r in b if r['correct'])/len(b)*100
    ev=sum(r['pnl'] for r in b)/len(b)
    lbl=f"{lo_:.2f}x–{hi_:.2f}x" if hi_<999 else f"{lo_:.2f}x+"
    bar='█'*int(max(0,ev)/15) if ev>0 else '░'*int(min(20,abs(ev)/15))
    print(f"  {lbl:>15}  n={len(b):>3}  WR={wr:>5.1f}%  EV=Rs {ev:>+8.1f}  {bar}")

# ════════════════════════════════════════════════════════════════════════
print()
print("="*90)
print("  SECTION 2 — LONGS: direction + HTF filters")
print("="*90)
show("ALL longs — no filter", longs)
print()
print("  ── Volume bands ──")
for lo_,hi_ in [(0,0.75),(0.75,1.5),(1.0,2.0),(0.75,2.0),(2.0,999)]:
    lbl = f"RVOL {lo_:.2f}x–{hi_:.2f}x" if hi_<999 else f"RVOL>={lo_:.2f}x"
    show(lbl,[r for r in longs if lo_<=r['rvol']<(hi_ if hi_<999 else 9999)])
print()
print("  ── Synthetic daily candle (last 24H) ──")
for state in ('bullish','neutral','bearish'):
    show(f"Daily {state}", [r for r in longs if r['daily_state']==state])
print()
print("  ── 4H SMA directions ──")
show("4H SMA-20 above (uptrend ~3d)",  [r for r in longs if r['above_sma20']])
show("4H SMA-20 below (downtrend ~3d)",[r for r in longs if not r['above_sma20']])
show("4H SMA-42 above (uptrend ~7d)",  [r for r in longs if r['above_sma42']])
show("4H SMA-42 below (downtrend ~7d)",[r for r in longs if r['above_sma42']==False])
show("Daily momentum bullish (24H)",   [r for r in longs if r['daily_momentum_bull']])
show("Daily momentum bearish (24H)",   [r for r in longs if r['daily_momentum_bull']==False])
print()
print("  ── Combined: HTF direction + volume ──")
combos_long = [
    ("Daily bullish + RVOL 0.75-2.0x",
     [r for r in longs if r['daily_state']=='bullish' and 0.75<=r['rvol']<=2.0]),
    ("Daily bullish + RVOL 0.75-1.5x",
     [r for r in longs if r['daily_state']=='bullish' and 0.75<=r['rvol']<=1.5]),
    ("Daily bullish + RVOL 1.0-2.0x",
     [r for r in longs if r['daily_state']=='bullish' and 1.0<=r['rvol']<=2.0]),
    ("Daily neutral + RVOL 0.75-1.5x",
     [r for r in longs if r['daily_state']=='neutral' and 0.75<=r['rvol']<=1.5]),
    ("Daily neutral + RVOL <2.0x",
     [r for r in longs if r['daily_state']=='neutral' and r['rvol']<2.0]),
    ("SMA20 above + RVOL 0.75-1.5x",
     [r for r in longs if r['above_sma20'] and 0.75<=r['rvol']<=1.5]),
    ("SMA20 above + RVOL 0.75-2.0x",
     [r for r in longs if r['above_sma20'] and 0.75<=r['rvol']<=2.0]),
    ("SMA42 above + RVOL 0.75-1.5x",
     [r for r in longs if r['above_sma42'] and 0.75<=r['rvol']<=1.5]),
    ("SMA42 above + RVOL 0.75-2.0x",
     [r for r in longs if r['above_sma42'] and 0.75<=r['rvol']<=2.0]),
    ("Momentum bull + RVOL 0.75-1.5x",
     [r for r in longs if r['daily_momentum_bull'] and 0.75<=r['rvol']<=1.5]),
    ("Momentum bull + RVOL 0.75-2.0x",
     [r for r in longs if r['daily_momentum_bull'] and 0.75<=r['rvol']<=2.0]),
]
for lbl,sub in combos_long:
    show(lbl, sub)

# ════════════════════════════════════════════════════════════════════════
print()
print("="*90)
print("  SECTION 3 — SHORTS: direction + HTF filters")
print("="*90)
show("ALL shorts — no filter", shorts)
print()
print("  ── Volume bands ──")
for lo_,hi_ in [(0,0.75),(0.75,1.5),(1.0,2.0),(0.75,2.0),(2.0,999)]:
    lbl = f"RVOL {lo_:.2f}x–{hi_:.2f}x" if hi_<999 else f"RVOL>={lo_:.2f}x"
    show(lbl,[r for r in shorts if lo_<=r['rvol']<(hi_ if hi_<999 else 9999)])
print()
print("  ── Synthetic daily candle (last 24H) ──")
for state in ('bearish','neutral','bullish'):
    show(f"Daily {state}", [r for r in shorts if r['daily_state']==state])
print()
print("  ── 4H SMA directions ──")
show("4H SMA-20 below (downtrend ~3d)", [r for r in shorts if not r['above_sma20']])
show("4H SMA-20 above (uptrend ~3d)",   [r for r in shorts if r['above_sma20']])
show("4H SMA-42 below (downtrend ~7d)", [r for r in shorts if r['above_sma42']==False])
show("4H SMA-42 above (uptrend ~7d)",   [r for r in shorts if r['above_sma42']])
show("Daily momentum bearish (24H)",    [r for r in shorts if r['daily_momentum_bull']==False])
show("Daily momentum bullish (24H)",    [r for r in shorts if r['daily_momentum_bull']])
print()
print("  ── Combined: HTF direction + volume ──")
combos_short = [
    ("Daily bearish + RVOL 0.75-1.5x",
     [r for r in shorts if r['daily_state']=='bearish' and 0.75<=r['rvol']<=1.5]),
    ("Daily bearish + RVOL 0.75-2.0x",
     [r for r in shorts if r['daily_state']=='bearish' and 0.75<=r['rvol']<=2.0]),
    ("Daily neutral + RVOL 0.75-1.5x",
     [r for r in shorts if r['daily_state']=='neutral' and 0.75<=r['rvol']<=1.5]),
    ("Daily neutral + RVOL <2.0x",
     [r for r in shorts if r['daily_state']=='neutral' and r['rvol']<2.0]),
    ("SMA20 below + RVOL 0.75-1.5x",
     [r for r in shorts if not r['above_sma20'] and 0.75<=r['rvol']<=1.5]),
    ("SMA20 below + RVOL 0.75-2.0x",
     [r for r in shorts if not r['above_sma20'] and 0.75<=r['rvol']<=2.0]),
    ("SMA42 below + RVOL 0.75-1.5x",
     [r for r in shorts if r['above_sma42']==False and 0.75<=r['rvol']<=1.5]),
    ("SMA42 below + RVOL 0.75-2.0x",
     [r for r in shorts if r['above_sma42']==False and 0.75<=r['rvol']<=2.0]),
    ("Momentum bear + RVOL 0.75-1.5x",
     [r for r in shorts if r['daily_momentum_bull']==False and 0.75<=r['rvol']<=1.5]),
    ("Momentum bear + RVOL 0.75-2.0x",
     [r for r in shorts if r['daily_momentum_bull']==False and 0.75<=r['rvol']<=2.0]),
    ("Daily bear+neutral + RVOL 0.75-1.5x",
     [r for r in shorts if r['daily_state'] in ('bearish','neutral') and 0.75<=r['rvol']<=1.5]),
]
for lbl,sub in combos_short:
    show(lbl, sub)

# ════════════════════════════════════════════════════════════════════════
print()
print("="*90)
print("  SECTION 4 — SUMMARY TABLE: top combinations ranked by EV/trade")
print("="*90)
print(f"  {'Filter':56}  {'N':>4}  {'WR':>6}  {'CI':>14}  {'EV/trade':>10}  {'TotalPnL':>10}")
print("  "+"-"*102)

all_combos = [
    ("Baseline — no filter",           rows),
    ("RVOL 0.75-1.5x only",            [r for r in rows if 0.75<=r['rvol']<=1.5]),
    ("RVOL 0.75-2.0x only",            [r for r in rows if 0.75<=r['rvol']<=2.0]),
    ("Longs — no filter",              longs),
    ("Longs — daily bullish + RVOL 0.75-1.5x",
     [r for r in longs if r['daily_state']=='bullish' and 0.75<=r['rvol']<=1.5]),
    ("Longs — daily bullish + RVOL 0.75-2.0x",
     [r for r in longs if r['daily_state']=='bullish' and 0.75<=r['rvol']<=2.0]),
    ("Longs — SMA20 above + RVOL 0.75-1.5x",
     [r for r in longs if r['above_sma20'] and 0.75<=r['rvol']<=1.5]),
    ("Longs — SMA42 above + RVOL 0.75-2.0x",
     [r for r in longs if r['above_sma42'] and 0.75<=r['rvol']<=2.0]),
    ("Shorts — no filter",             shorts),
    ("Shorts — daily bearish + RVOL 0.75-1.5x",
     [r for r in shorts if r['daily_state']=='bearish' and 0.75<=r['rvol']<=1.5]),
    ("Shorts — SMA20 below + RVOL 0.75-1.5x",
     [r for r in shorts if not r['above_sma20'] and 0.75<=r['rvol']<=1.5]),
    ("Shorts — SMA20 below + RVOL 0.75-2.0x",
     [r for r in shorts if not r['above_sma20'] and 0.75<=r['rvol']<=2.0]),
    ("Shorts — momentum bear + RVOL 0.75-2.0x",
     [r for r in shorts if r['daily_momentum_bull']==False and 0.75<=r['rvol']<=2.0]),
    ("Best combined (longs+shorts filtered)",
     [r for r in rows if
        (r['direction']=='long'  and r['daily_state']=='bullish' and 0.75<=r['rvol']<=1.5) or
        (r['direction']=='short' and r['daily_state']=='bearish' and 0.75<=r['rvol']<=1.5)]),
]
for lbl,sub in sorted(all_combos, key=lambda x: sum(r['pnl'] for r in x[1])/len(x[1]) if x[1] else -9999, reverse=True):
    if not sub:
        print(f"  {lbl:56}  {'n=0':>4}"); continue
    wins=sum(1 for r in sub if r['correct'])
    wr=wins/len(sub)*100; ev=sum(r['pnl'] for r in sub)/len(sub)
    tot=sum(r['pnl'] for r in sub); lo,hi=wilson_ci(wins,len(sub))
    print(f"  {lbl:56}  {len(sub):>4}  {wr:>5.1f}%  [{lo:>4.1f}%-{hi:>4.1f}%]  Rs {ev:>+8.1f}  Rs {tot:>+9.1f}")

# ════════════════════════════════════════════════════════════════════════
print()
print("="*90)
print("  SECTION 5 — PER-SYMBOL BREAKDOWN")
print("="*90)
for sym in sorted(set(r['symbol'] for r in rows)):
    sr=[r for r in rows if r['symbol']==sym]
    sl=[r for r in sr if r['direction']=='long']
    ss=[r for r in sr if r['direction']=='short']
    print(f"\n  {sym}")
    show("  All — no filter", sr)
    show("  Longs — no filter", sl)
    show("  Longs — daily bullish + RVOL 0.75-1.5x",
         [r for r in sl if r['daily_state']=='bullish' and 0.75<=r['rvol']<=1.5])
    show("  Shorts — no filter", ss)
    show("  Shorts — daily bearish + RVOL 0.75-1.5x",
         [r for r in ss if r['daily_state']=='bearish' and 0.75<=r['rvol']<=1.5])
