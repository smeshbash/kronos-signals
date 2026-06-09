"""
Deep dive into volume filter thresholds for 1H signals only.
Analyses RVOL bands and directional breakdown.
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
           s.signal_timestamp, s.actual_return_pct, s.regime_version,
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
    candles = conn.execute("""
        SELECT volume FROM ohlcv
        WHERE symbol=? AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT ?
    """, (sig['symbol'], ts, AVG_PERIOD + 1)).fetchall()
    if len(candles) < 2: continue
    curr_vol = float(candles[0]['volume'])
    avg_vol  = sum(float(c['volume']) for c in candles[1:]) / len(candles[1:])
    if avg_vol <= 0: continue
    rvol    = curr_vol / avg_vol
    ret     = float(sig['actual_return_pct'])
    correct = (ret > HIT_THR) if sig['direction'] == 'long' else (ret < -HIT_THR)
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)
    rows.append(dict(
        model=sig['model_source'], symbol=sig['symbol'],
        direction=sig['direction'], rvol=rvol, correct=correct, pnl=pnl,
    ))
conn.close()

n = len(rows)
print(f"1H signals analysed: {n}")
print()

# ── Fine RVOL bands ───────────────────────────────────────────────────────────
print("=" * 68)
print("  RVOL BAND ANALYSIS — win rate and EV per band")
print("=" * 68)
print(f"  {'Band':>18}  {'N':>4}  {'WR':>6}  {'EV/trade':>10}  {'Cumul EV trend'}")
print("  " + "-"*60)
bands = [(i*0.25, (i+1)*0.25) for i in range(0, 16)] + [(4.0, 999)]
for lo, hi in bands:
    b = [r for r in rows if lo <= r['rvol'] < hi]
    if not b: continue
    wr = sum(1 for r in b if r['correct']) / len(b) * 100
    ev = sum(r['pnl'] for r in b) / len(b)
    bar = '█' * int(max(0, ev) / 10) if ev > 0 else '░' * int(min(20, abs(ev) / 10))
    lbl = f"{lo:.2f}x–{hi:.2f}x" if hi < 999 else f"{lo:.2f}x+"
    print(f"  {lbl:>18}  {len(b):>4}  {wr:>5.1f}%  Rs {ev:>+8.1f}  {bar}")

# ── Gate sweep: simple lower bound ───────────────────────────────────────────
print()
print("=" * 68)
print("  LOWER-BOUND GATE SWEEP  (execute if RVOL >= threshold)")
print("=" * 68)
print(f"  {'Threshold':>10}  {'Kept':>5}  {'Filter%':>8}  {'WR':>6}  {'EV/trade':>10}")
print("  " + "-"*50)
for thr in [t*0.25 for t in range(0, 13)]:
    kept = [r for r in rows if r['rvol'] >= thr]
    if not kept: break
    wr = sum(1 for r in kept if r['correct']) / len(kept) * 100
    ev = sum(r['pnl'] for r in kept) / len(kept)
    marker = ' <- baseline' if thr == 0 else ''
    print(f"  {thr:>9.2f}x  {len(kept):>5}  {(n-len(kept))/n*100:>7.1f}%  "
          f"{wr:>5.1f}%  Rs {ev:>+8.1f}{marker}")

# ── Band gate: execute only if lo <= RVOL <= hi ───────────────────────────────
print()
print("=" * 68)
print("  BAND GATE  (execute only if threshold_lo <= RVOL <= threshold_hi)")
print("  Rationale: very high RVOL = panic/capitulation, often reverses")
print("=" * 68)
print(f"  {'Band':>18}  {'Kept':>5}  {'Filter%':>8}  {'WR':>6}  {'EV/trade':>10}")
print("  " + "-"*58)
hi_caps = [1.5, 2.0, 2.5, 3.0, 999]
lo_floors = [0.75, 1.0, 1.25]
for lo in lo_floors:
    for hi in hi_caps:
        kept = [r for r in rows if lo <= r['rvol'] <= hi]
        if not kept: continue
        wr = sum(1 for r in kept if r['correct']) / len(kept) * 100
        ev = sum(r['pnl'] for r in kept) / len(kept)
        lbl = f"{lo:.2f}x – {hi:.2f}x" if hi < 999 else f"{lo:.2f}x – no cap"
        print(f"  {lbl:>18}  {len(kept):>5}  {(n-len(kept))/n*100:>7.1f}%  "
              f"{wr:>5.1f}%  Rs {ev:>+8.1f}")
    print()

# ── Direction split at best band ─────────────────────────────────────────────
print("=" * 68)
print("  DIRECTION SPLIT at RVOL 1.0x–2.0x band")
print("=" * 68)
band = [r for r in rows if 1.0 <= r['rvol'] <= 2.0]
for drx in ('long', 'short'):
    d = [r for r in band if r['direction'] == drx]
    if not d: continue
    wr = sum(1 for r in d if r['correct']) / len(d) * 100
    ev = sum(r['pnl'] for r in d) / len(d)
    print(f"  {drx:6}  n={len(d):>3}  WR={wr:.1f}%  EV=Rs {ev:+.1f}")
