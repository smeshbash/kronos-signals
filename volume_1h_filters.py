"""
1H signal filter combination backtest.
Uses existing v1-v4 signal history to test:
  1. 4H trend alignment (close vs 20-period 4H SMA)
  2. 4H candle direction at signal time
  3. Volume band (0.75x – 1.50x)
  4. Combinations of the above

Separate analysis for LONG and SHORT directions.
"""
import sqlite3, sys, statistics
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
skipped = 0
for sig in signals:
    ts = int(sig['signal_timestamp'])

    # ── 1H RVOL ──────────────────────────────────────────────────────────────
    h1 = conn.execute("""
        SELECT volume FROM ohlcv
        WHERE symbol=? AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT ?
    """, (sig['symbol'], ts, AVG_PERIOD + 1)).fetchall()
    if len(h1) < 2:
        skipped += 1; continue
    curr_vol = float(h1[0]['volume'])
    avg_vol  = sum(float(c['volume']) for c in h1[1:]) / len(h1[1:])
    if avg_vol <= 0:
        skipped += 1; continue
    rvol_1h = curr_vol / avg_vol

    # ── 4H candles for trend ──────────────────────────────────────────────────
    h4 = conn.execute("""
        SELECT open, high, low, close, volume FROM ohlcv
        WHERE symbol=? AND timeframe='4h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 25
    """, (sig['symbol'], ts)).fetchall()
    if len(h4) < 5:
        skipped += 1; continue

    # 4H SMA-20 trend: current 4H close vs 20-period 4H SMA
    h4_closes = [float(c['close']) for c in h4]
    h4_curr_close = h4_closes[0]
    h4_sma20 = sum(h4_closes[:20]) / min(20, len(h4_closes))
    tf4h_above_sma = h4_curr_close > h4_sma20   # True = 4H uptrend

    # 4H candle direction: is the most recent 4H candle bullish?
    tf4h_candle_bull = float(h4[0]['close']) > float(h4[0]['open'])

    # 4H momentum: close vs close 3 candles ago (recent 4H direction)
    if len(h4) >= 4:
        tf4h_momentum_bull = h4_closes[0] > h4_closes[3]
    else:
        tf4h_momentum_bull = None

    ret     = float(sig['actual_return_pct'])
    drx     = sig['direction']
    correct = (ret > HIT_THR) if drx == 'long' else (ret < -HIT_THR)
    pnl     = float(sig['pnl_net'] or sig['pnl_gross'] or 0)

    rows.append(dict(
        model=sig['model_source'], symbol=sig['symbol'],
        direction=drx, rvol=rvol_1h, correct=correct, pnl=pnl,
        tf4h_above_sma=tf4h_above_sma,
        tf4h_candle_bull=tf4h_candle_bull,
        tf4h_momentum_bull=tf4h_momentum_bull,
    ))

conn.close()
n = len(rows)
print(f"Signals: {n}  (skipped {skipped})")
print()

def stats(subset):
    if not subset: return "  n=0"
    wr = sum(1 for r in subset if r['correct']) / len(subset) * 100
    ev = sum(r['pnl'] for r in subset) / len(subset)
    return f"  n={len(subset):>3}  WR={wr:>5.1f}%  EV=Rs {ev:>+7.1f}"

def show(label, subset):
    print(f"  {label:45}{stats(subset)}")

# ── Baseline ──────────────────────────────────────────────────────────────────
longs  = [r for r in rows if r['direction'] == 'long']
shorts = [r for r in rows if r['direction'] == 'short']
print(f"Baseline  ALL:   {stats(rows)}")
print(f"Baseline  LONG:  {stats(longs)}")
print(f"Baseline  SHORT: {stats(shorts)}")
print()

# ─────────────────────────────────────────────────────────────────────────────
print("=" * 72)
print("  LONG SIGNALS — filter combinations")
print("=" * 72)
show("No filter (baseline)",              longs)
print()
print("  — 4H TREND ALIGNMENT —")
show("4H above SMA-20 (uptrend)",         [r for r in longs if r['tf4h_above_sma']])
show("4H below SMA-20 (downtrend)",       [r for r in longs if not r['tf4h_above_sma']])
show("4H candle bullish",                 [r for r in longs if r['tf4h_candle_bull']])
show("4H candle bearish",                 [r for r in longs if not r['tf4h_candle_bull']])
show("4H momentum bullish (3-bar)",       [r for r in longs if r['tf4h_momentum_bull']])
show("4H momentum bearish (3-bar)",       [r for r in longs if r['tf4h_momentum_bull'] == False])
print()
print("  — VOLUME BAND —")
show("RVOL 0.75x–1.50x",                 [r for r in longs if 0.75 <= r['rvol'] <= 1.50])
show("RVOL 1.00x–2.00x",                 [r for r in longs if 1.00 <= r['rvol'] <= 2.00])
show("RVOL >= 1.00x",                    [r for r in longs if r['rvol'] >= 1.00])
print()
print("  — COMBINED: 4H TREND + VOLUME —")
show("4H above SMA + RVOL 0.75-1.50x",  [r for r in longs if r['tf4h_above_sma'] and 0.75 <= r['rvol'] <= 1.50])
show("4H above SMA + RVOL 1.00-2.00x",  [r for r in longs if r['tf4h_above_sma'] and 1.00 <= r['rvol'] <= 2.00])
show("4H above SMA + RVOL >= 1.00x",    [r for r in longs if r['tf4h_above_sma'] and r['rvol'] >= 1.00])
show("4H candle bull + RVOL 0.75-1.50x",[r for r in longs if r['tf4h_candle_bull'] and 0.75 <= r['rvol'] <= 1.50])
show("4H candle bull + RVOL >= 1.00x",  [r for r in longs if r['tf4h_candle_bull'] and r['rvol'] >= 1.00])
show("4H momentum bull + RVOL 0.75-1.5x",[r for r in longs if r['tf4h_momentum_bull'] and 0.75<=r['rvol']<=1.50])
show("4H momentum bull + RVOL >= 1.00x",[r for r in longs if r['tf4h_momentum_bull'] and r['rvol'] >= 1.00])
print()
print("  — TRIPLE: SMA + CANDLE + VOLUME —")
show("SMA bull + candle bull + RVOL 0.75-1.5x",
     [r for r in longs if r['tf4h_above_sma'] and r['tf4h_candle_bull'] and 0.75<=r['rvol']<=1.50])
show("SMA bull + candle bull + RVOL >=1.0x",
     [r for r in longs if r['tf4h_above_sma'] and r['tf4h_candle_bull'] and r['rvol']>=1.0])
show("SMA bull + momentum bull + RVOL >=1.0x",
     [r for r in longs if r['tf4h_above_sma'] and r['tf4h_momentum_bull'] and r['rvol']>=1.0])

# ─────────────────────────────────────────────────────────────────────────────
print()
print("=" * 72)
print("  SHORT SIGNALS — filter combinations")
print("=" * 72)
show("No filter (baseline)",              shorts)
print()
print("  — 4H TREND ALIGNMENT —")
show("4H below SMA-20 (downtrend)",       [r for r in shorts if not r['tf4h_above_sma']])
show("4H above SMA-20 (uptrend)",         [r for r in shorts if r['tf4h_above_sma']])
show("4H candle bearish",                 [r for r in shorts if not r['tf4h_candle_bull']])
show("4H candle bullish",                 [r for r in shorts if r['tf4h_candle_bull']])
show("4H momentum bearish (3-bar)",       [r for r in shorts if r['tf4h_momentum_bull'] == False])
show("4H momentum bullish (3-bar)",       [r for r in shorts if r['tf4h_momentum_bull']])
print()
print("  — COMBINED: 4H TREND + VOLUME —")
show("4H below SMA + RVOL 0.75-1.50x",  [r for r in shorts if not r['tf4h_above_sma'] and 0.75<=r['rvol']<=1.50])
show("4H below SMA + RVOL >= 1.00x",    [r for r in shorts if not r['tf4h_above_sma'] and r['rvol']>=1.00])
show("4H candle bear + RVOL 0.75-1.50x",[r for r in shorts if not r['tf4h_candle_bull'] and 0.75<=r['rvol']<=1.50])
show("4H candle bear + RVOL >= 1.00x",  [r for r in shorts if not r['tf4h_candle_bull'] and r['rvol']>=1.00])
show("4H momentum bear + RVOL >=1.00x", [r for r in shorts if r['tf4h_momentum_bull']==False and r['rvol']>=1.00])
print()

# ─────────────────────────────────────────────────────────────────────────────
print("=" * 72)
print("  PER-MODEL BREAKDOWN — best filter applied")
print("  (4H above SMA + RVOL >= 1.0x for longs; 4H below SMA + RVOL >= 1.0x for shorts)")
print("=" * 72)
for model in sorted(set(r['model'] for r in rows)):
    mr = [r for r in rows if r['model'] == model]
    ml = [r for r in mr if r['direction'] == 'long']
    ms = [r for r in mr if r['direction'] == 'short']
    ml_f = [r for r in ml if r['tf4h_above_sma'] and r['rvol'] >= 1.0]
    ms_f = [r for r in ms if not r['tf4h_above_sma'] and r['rvol'] >= 1.0]
    print(f"\n  {model}")
    show("  longs  — no filter",  ml)
    show("  longs  — filtered",   ml_f)
    show("  shorts — no filter",  ms)
    show("  shorts — filtered",   ms_f)

# ─────────────────────────────────────────────────────────────────────────────
print()
print("=" * 72)
print("  SUMMARY — what each filter does to signal count")
print("=" * 72)
filters = [
    ("No filter",                        rows),
    ("Volume band 0.75-1.50x only",      [r for r in rows if 0.75<=r['rvol']<=1.50]),
    ("4H SMA alignment only",            [r for r in rows if (r['direction']=='long' and r['tf4h_above_sma']) or (r['direction']=='short' and not r['tf4h_above_sma'])]),
    ("4H candle alignment only",         [r for r in rows if (r['direction']=='long' and r['tf4h_candle_bull']) or (r['direction']=='short' and not r['tf4h_candle_bull'])]),
    ("Volume + 4H SMA align",            [r for r in rows if 0.75<=r['rvol']<=1.50 and ((r['direction']=='long' and r['tf4h_above_sma']) or (r['direction']=='short' and not r['tf4h_above_sma']))]),
    ("Volume + 4H candle align",         [r for r in rows if 0.75<=r['rvol']<=1.50 and ((r['direction']=='long' and r['tf4h_candle_bull']) or (r['direction']=='short' and not r['tf4h_candle_bull']))]),
]
print(f"  {'Filter':40}  {'N':>4}  {'Filter%':>8}  {'WR':>6}  {'EV/trade':>10}")
print("  " + "-"*72)
for label, subset in filters:
    if not subset: continue
    wr = sum(1 for r in subset if r['correct']) / len(subset) * 100
    ev = sum(r['pnl'] for r in subset) / len(subset)
    print(f"  {label:40}  {len(subset):>4}  {(n-len(subset))/n*100:>7.1f}%  {wr:>5.1f}%  Rs {ev:>+8.1f}")
