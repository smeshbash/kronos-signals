"""
Split 1H short signals by 4H candle state: bearish / neutral / bullish.
Neutral = candle body is less than 30% of the total wick range (doji-like).
Shows what each state is worth with and without the RVOL band.
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

HIT_THR    = 0.15
AVG_PERIOD = 20
NEUTRAL_BODY_RATIO = 0.30   # body/range < this → neutral candle

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
      AND s.direction = 'short'
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

    o, h, l, c = float(h4[0]['open']), float(h4[0]['high']), float(h4[0]['low']), float(h4[0]['close'])
    body  = abs(c - o)
    range_ = h - l
    body_ratio = body / range_ if range_ > 0 else 1.0

    if body_ratio < NEUTRAL_BODY_RATIO:
        candle_state = 'neutral'
    elif c < o:
        candle_state = 'bearish'
    else:
        candle_state = 'bullish'

    ret     = float(sig['actual_return_pct'])
    correct = ret < -HIT_THR
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)

    rows.append(dict(
        symbol=sig['symbol'], model=sig['model_source'],
        rvol=rvol, correct=correct, pnl=pnl,
        candle_state=candle_state, body_ratio=body_ratio,
    ))
conn.close()

n = len(rows)

def stats(subset):
    if not subset: return f"n={0:>3}  WR=  n/a   EV=      n/a"
    wr  = sum(1 for r in subset if r['correct']) / len(subset) * 100
    ev  = sum(r['pnl'] for r in subset) / len(subset)
    tot = sum(r['pnl'] for r in subset)
    return f"n={len(subset):>3}  WR={wr:>5.1f}%  EV=Rs {ev:>+8.1f}  TotalPnL=Rs {tot:>+8.1f}"

def show(label, subset):
    print(f"  {label:48}  {stats(subset)}")

print(f"1H short signals: {n}")
print(f"  bearish: {sum(1 for r in rows if r['candle_state']=='bearish')}  "
      f"neutral: {sum(1 for r in rows if r['candle_state']=='neutral')}  "
      f"bullish: {sum(1 for r in rows if r['candle_state']=='bullish')}")
print(f"  (neutral = 4H candle body < {NEUTRAL_BODY_RATIO*100:.0f}% of range)")
print()

for state in ('bearish', 'neutral', 'bullish'):
    grp = [r for r in rows if r['candle_state'] == state]
    print("=" * 88)
    print(f"  4H CANDLE: {state.upper()}  ({len(grp)} signals)")
    print("=" * 88)
    show("No filter",              grp)
    show("RVOL >= 0.75x",          [r for r in grp if r['rvol'] >= 0.75])
    show("RVOL >= 1.00x",          [r for r in grp if r['rvol'] >= 1.00])
    show("RVOL 0.75x – 1.50x",     [r for r in grp if 0.75 <= r['rvol'] <= 1.50])
    show("RVOL 0.75x – 2.00x",     [r for r in grp if 0.75 <= r['rvol'] <= 2.00])
    show("RVOL 1.00x – 1.50x",     [r for r in grp if 1.00 <= r['rvol'] <= 1.50])
    show("RVOL >= 2.00x  (high)",   [r for r in grp if r['rvol'] >= 2.00])
    show("RVOL < 0.75x   (low)",    [r for r in grp if r['rvol'] < 0.75])
    print()

# ── Summary table ─────────────────────────────────────────────────────────
print("=" * 88)
print("  SUMMARY — all three states, best filter for each")
print("=" * 88)
print(f"  {'State + Filter':48}  {'N':>4}  {'WR':>6}  {'EV/trade':>10}  {'Total PnL':>10}")
print("  " + "-"*82)
combos = [
    ("Bearish — no filter",          [r for r in rows if r['candle_state']=='bearish']),
    ("Bearish — RVOL 0.75-1.50x",    [r for r in rows if r['candle_state']=='bearish' and 0.75<=r['rvol']<=1.50]),
    ("Neutral — no filter",          [r for r in rows if r['candle_state']=='neutral']),
    ("Neutral — RVOL 0.75-1.50x",    [r for r in rows if r['candle_state']=='neutral' and 0.75<=r['rvol']<=1.50]),
    ("Neutral — RVOL 0.75-2.00x",    [r for r in rows if r['candle_state']=='neutral' and 0.75<=r['rvol']<=2.00]),
    ("Bullish — no filter",          [r for r in rows if r['candle_state']=='bullish']),
    ("Bullish — RVOL 0.75-1.50x",    [r for r in rows if r['candle_state']=='bullish' and 0.75<=r['rvol']<=1.50]),
    ("ALL shorts — no filter",       rows),
    ("ALL shorts — bearish only + RVOL 0.75-1.50x",
                                     [r for r in rows if r['candle_state']=='bearish' and 0.75<=r['rvol']<=1.50]),
    ("ALL shorts — bearish+neutral + RVOL 0.75-1.50x",
                                     [r for r in rows if r['candle_state'] in ('bearish','neutral') and 0.75<=r['rvol']<=1.50]),
    ("ALL shorts — all states + RVOL 0.75-1.50x",
                                     [r for r in rows if 0.75<=r['rvol']<=1.50]),
]
for label, subset in combos:
    if not subset:
        print(f"  {label:48}  {'n=0':>4}")
        continue
    wr  = sum(1 for r in subset if r['correct']) / len(subset) * 100
    ev  = sum(r['pnl'] for r in subset) / len(subset)
    tot = sum(r['pnl'] for r in subset)
    print(f"  {label:48}  {len(subset):>4}  {wr:>5.1f}%  Rs {ev:>+8.1f}  Rs {tot:>+8.1f}")
