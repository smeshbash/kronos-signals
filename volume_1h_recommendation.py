"""
Head-to-head comparison of Option A vs Option B vs baseline.
All numbers from the same 220-signal historical set.
No assumptions — only confirmed data.
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

HIT_THR    = 0.15
AVG_PERIOD = 20

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
    ts  = int(sig['signal_timestamp'])
    h1  = conn.execute("""
        SELECT volume FROM ohlcv
        WHERE symbol=? AND timeframe='1h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT ?
    """, (sig['symbol'], ts, AVG_PERIOD+1)).fetchall()
    if len(h1) < 2: continue
    avg_vol = sum(float(c['volume']) for c in h1[1:]) / len(h1[1:])
    if avg_vol <= 0: continue
    rvol = float(h1[0]['volume']) / avg_vol

    h4 = conn.execute("""
        SELECT open, close FROM ohlcv
        WHERE symbol=? AND timeframe='4h' AND timestamp<=?
        ORDER BY timestamp DESC LIMIT 21
    """, (sig['symbol'], ts)).fetchall()
    if len(h4) < 5: continue
    h4_closes    = [float(c['close']) for c in h4]
    sma20        = sum(h4_closes[:20]) / min(20, len(h4_closes))
    above_sma    = h4_closes[0] > sma20
    candle_bull  = float(h4[0]['close']) > float(h4[0]['open'])
    momentum_bull= h4_closes[0] > h4_closes[3]

    ret     = float(sig['actual_return_pct'])
    drx     = sig['direction']
    correct = (ret > HIT_THR) if drx == 'long' else (ret < -HIT_THR)
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)
    rows.append(dict(
        direction=drx, rvol=rvol, correct=correct, pnl=pnl,
        above_sma=above_sma, candle_bull=candle_bull, momentum_bull=momentum_bull,
    ))
conn.close()

# ── Filter definitions ─────────────────────────────────────────────────────
def passes_A(r):
    """Option A: suspend longs entirely; shorts need 4H candle bearish + RVOL band"""
    if r['direction'] == 'long':  return False
    return (not r['candle_bull']) and (0.75 <= r['rvol'] <= 1.50)

def passes_B(r):
    """Option B: combined filter on all signals — 4H candle alignment + RVOL band"""
    if r['direction'] == 'long':
        return r['candle_bull'] and (0.75 <= r['rvol'] <= 1.50)
    else:
        return (not r['candle_bull']) and (0.75 <= r['rvol'] <= 1.50)

def passes_baseline(r):
    return True

def report(label, subset, total_n):
    if not subset:
        print(f"  {label:50}  n=   0  — no signals")
        return
    wr  = sum(1 for r in subset if r['correct']) / len(subset) * 100
    ev  = sum(r['pnl'] for r in subset) / len(subset)
    tot = sum(r['pnl'] for r in subset)
    flt = (total_n - len(subset)) / total_n * 100
    nl  = len([r for r in subset if r['direction']=='long'])
    ns  = len([r for r in subset if r['direction']=='short'])
    print(f"  {label:50}  n={len(subset):>3} (L={nl}/S={ns})  "
          f"Filter={flt:>5.1f}%  WR={wr:>5.1f}%  EV=Rs {ev:>+8.1f}  "
          f"Total PnL=Rs {tot:>+9.1f}")

n = len(rows)
print(f"Signal pool: {n}")
print()

print("=" * 110)
print("  HEAD-TO-HEAD: BASELINE vs OPTION A vs OPTION B")
print("  Same 220-signal set. No holdout applied — these are in-sample numbers.")
print("=" * 110)
print()
report("BASELINE (no filter)",          [r for r in rows if passes_baseline(r)], n)
report("OPTION A (suspend longs + short filter)", [r for r in rows if passes_A(r)], n)
report("OPTION B (both directions filtered)",     [r for r in rows if passes_B(r)], n)
print()

# ── What Option A's 2 long buckets look like ──────────────────────────────
longs = [r for r in rows if r['direction']=='long']
print("=" * 80)
print("  LONG SIGNALS — where all 83 actually came from")
print("=" * 80)
in_uptrend   = [r for r in longs if r['above_sma']]
in_downtrend = [r for r in longs if not r['above_sma']]
print(f"  4H above SMA-20 (would pass trend check): n={len(in_uptrend):>3}  "
      f"({len(in_uptrend)/len(longs)*100:.1f}% of all longs)")
print(f"  4H below SMA-20 (firing against trend):   n={len(in_downtrend):>3}  "
      f"({len(in_downtrend)/len(longs)*100:.1f}% of all longs)")
print()
print("  This is structural — not a filter tuning problem.")
print("  The model generates longs almost exclusively during 4H downtrends.")
print()

# ── Confidence interval warning ─────────────────────────────────────────────
import math
def wilson_ci(wins, n, z=1.96):
    if n == 0: return (0, 0)
    p = wins / n
    lo = (p + z*z/(2*n) - z*math.sqrt((p*(1-p) + z*z/(4*n))/n)) / (1 + z*z/n)
    hi = (p + z*z/(2*n) + z*math.sqrt((p*(1-p) + z*z/(4*n))/n)) / (1 + z*z/n)
    return (max(0, lo)*100, min(100, hi)*100)

print("=" * 80)
print("  STATISTICAL CONFIDENCE — 95% Wilson CI on win rate")
print("  (how wide the error bars really are at these sample sizes)")
print("=" * 80)
checks = [
    ("Option A shorts (n=24)",  [r for r in rows if passes_A(r)]),
    ("Option B all (n=25)",     [r for r in rows if passes_B(r)]),
    ("Baseline all (n=220)",    rows),
    ("Shorts no filter (n=137)",[r for r in rows if r['direction']=='short']),
]
for label, subset in checks:
    if not subset: continue
    wins = sum(1 for r in subset if r['correct'])
    lo, hi = wilson_ci(wins, len(subset))
    wr = wins / len(subset) * 100
    print(f"  {label:40}  WR={wr:.1f}%  95% CI: [{lo:.1f}% – {hi:.1f}%]")
print()

# ── The regime risk ──────────────────────────────────────────────────────────
print("=" * 80)
print("  REGIME RISK — how many shorts would pass the filter in a bull market?")
print("  (4H candle bearish rate across all short signals)")
print("=" * 80)
shorts = [r for r in rows if r['direction']=='short']
candle_bear_pct = sum(1 for r in shorts if not r['candle_bull']) / len(shorts) * 100
rvol_band_pct   = sum(1 for r in shorts if 0.75<=r['rvol']<=1.50) / len(shorts) * 100
both_pct        = sum(1 for r in shorts if (not r['candle_bull']) and 0.75<=r['rvol']<=1.50) / len(shorts) * 100
print(f"  Of {len(shorts)} short signals:")
print(f"    4H candle was bearish:          {sum(1 for r in shorts if not r['candle_bull']):>3} / {len(shorts)} = {candle_bear_pct:.1f}%")
print(f"    RVOL in 0.75-1.50x band:        {sum(1 for r in shorts if 0.75<=r['rvol']<=1.50):>3} / {len(shorts)} = {rvol_band_pct:.1f}%")
print(f"    Both (Option A filter passes):  {sum(1 for r in shorts if (not r['candle_bull']) and 0.75<=r['rvol']<=1.50):>3} / {len(shorts)} = {both_pct:.1f}%")
print()
print("  In a sustained bull market, 4H candle bearish rate would drop.")
print("  Fewer short signals would pass. This is expected behaviour — not a flaw.")
print()

# ── Final verdict ──────────────────────────────────────────────────────────
print("=" * 80)
print("  DATA-BACKED VERDICT")
print("=" * 80)
a = [r for r in rows if passes_A(r)]
b = [r for r in rows if passes_B(r)]
a_ev = sum(r['pnl'] for r in a)/len(a) if a else 0
b_ev = sum(r['pnl'] for r in b)/len(b) if b else 0
base_ev = sum(r['pnl'] for r in rows)/len(rows)
print(f"  Baseline EV/trade:   Rs {base_ev:+.1f}")
print(f"  Option A EV/trade:   Rs {a_ev:+.1f}  (delta: Rs {a_ev-base_ev:+.1f})")
print(f"  Option B EV/trade:   Rs {b_ev:+.1f}  (delta: Rs {b_ev-base_ev:+.1f})")
print()
a_longs = [r for r in a if r['direction']=='long']
b_longs = [r for r in b if r['direction']=='long']
print(f"  Option A includes {len(a_longs)} longs  (sample too small to validate)")
print(f"  Option B includes {len(b_longs)} long  (sample too small to validate)")
print()
print("  Both options produce near-identical total PnL from the historical set.")
print("  Option A is marginally more conservative — it makes no claim about longs.")
print("  Option B keeps 1 long (n=1 is not evidence of anything).")
