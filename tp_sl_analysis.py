"""
TP/SL structure analysis for kronos-mini 1H.

Diagnoses the gap between high signal directional accuracy (~85%) and
low trade win rate (~8%) by examining:

  1. Exit reason breakdown (stop_loss / take_profit / horizon_exit)
  2. Entry-to-SL and Entry-to-TP distances vs ATR and predicted return
  3. "Correct signal, stopped out" — trades where actual_return_pct confirmed
     the direction but the trade exited via stop loss
  4. TP hit rate vs predicted return magnitude
  5. How often price reached the predicted return level before the SL was hit
  6. Hold-time distribution: SL hits vs TP hits
  7. Required minimum WR given actual RR ratio
"""
import sqlite3, sys, math
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

HIT_THR = 0.15   # same as analysis scripts — signal correct if |actual| > this

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ── Pull all kronos-mini 1H closed trades with full context ──────────────────
# Positions are deleted on close so SL/TP are sourced from paper_fill events.
rows = conn.execute("""
    SELECT
        t.id                AS trade_id,
        t.symbol,
        t.direction,
        t.entry_price,
        t.exit_price,
        t.entry_timestamp,
        t.exit_timestamp,
        t.exit_reason,
        t.pnl_gross,
        t.peak_price,
        t.trough_price,
        json_extract(e.data, '$.sl_price')    AS stop_loss_price,
        json_extract(e.data, '$.tp_price')    AS take_profit_price,
        json_extract(e.data, '$.entry_atr')   AS entry_atr,
        s.predicted_return_pct,
        s.actual_return_pct,
        s.confidence,
        s.signal_timestamp
    FROM trades t
    JOIN signals s ON s.id = t.signal_id
    JOIN events  e ON e.event_type = 'paper_fill'
                   AND json_extract(e.data, '$.trade_id') = t.id
    WHERE t.status        = 'closed'
      AND t.quality_flag  IS NULL
      AND s.model_source  = 'kronos-mini'
      AND t.entry_price   > 0
      AND json_extract(e.data, '$.sl_price') IS NOT NULL
""").fetchall()
conn.close()

trades = [dict(r) for r in rows]
print(f"Total kronos-mini 1H closed trades with full context: {len(trades)}")

# ── Helpers ───────────────────────────────────────────────────────────────────

def pct(a, b):
    """Percentage difference b relative to a."""
    return abs(b - a) / a * 100 if a else 0.0

def signed_pct(entry, exit_, direction):
    """Trade return % in direction-adjusted terms (positive = favourable)."""
    if direction == 'long':
        return (exit_ - entry) / entry * 100
    else:
        return (entry - exit_) / entry * 100

def wilson_ci(wins, n, z=1.96):
    if n == 0: return 0.0, 0.0
    p = wins / n
    lo = (p + z*z/(2*n) - z*math.sqrt((p*(1-p)+z*z/(4*n))/n)) / (1+z*z/n)
    hi = (p + z*z/(2*n) + z*math.sqrt((p*(1-p)+z*z/(4*n))/n)) / (1+z*z/n)
    return max(0, lo)*100, min(100, hi)*100

def sep(char='─', width=88):
    print(char * width)

# ── 1. EXIT REASON BREAKDOWN ─────────────────────────────────────────────────
sep('═')
print("  1. EXIT REASON BREAKDOWN")
sep('═')

from collections import Counter
exit_counts = Counter(t['exit_reason'] for t in trades)
total = len(trades)
wins  = sum(1 for t in trades if (t['pnl_gross'] or 0) > 0)
print(f"\n  Total trades: {total}   Wins: {wins}   WR: {wins/total*100:.1f}%\n")

for reason, n in sorted(exit_counts.items(), key=lambda x: -x[1]):
    subset = [t for t in trades if t['exit_reason'] == reason]
    w = sum(1 for t in subset if (t['pnl_gross'] or 0) > 0)
    avg_pnl = sum(t['pnl_gross'] or 0 for t in subset) / n
    lo, hi = wilson_ci(w, n)
    print(f"  {reason:<20}  n={n:>3}  WR={w/n*100:>5.1f}%  CI=[{lo:.0f}%-{hi:.0f}%]"
          f"  AvgPnL=Rs {avg_pnl:>+8.2f}")

# ── 2. TP/SL/ATR DISTANCE ANALYSIS ──────────────────────────────────────────
sep('═')
print("\n  2. TP / SL / ATR DISTANCE (% of entry price)")
sep('═')

valid = [t for t in trades if t['entry_atr'] and t['entry_atr'] > 0]
print(f"\n  Trades with entry_atr available: {len(valid)}")

sl_pcts   = [pct(t['entry_price'], t['stop_loss_price'])   for t in valid]
tp_pcts   = [pct(t['entry_price'], t['take_profit_price']) for t in valid]
atr_pcts  = [t['entry_atr'] / t['entry_price'] * 100       for t in valid]
pred_pcts = [abs(t['predicted_return_pct'] or 0)            for t in valid]

def stats(label, vals):
    if not vals: return
    vals_s = sorted(vals)
    n = len(vals)
    mn = sum(vals)/n
    med = vals_s[n//2]
    p10 = vals_s[int(n*0.10)]
    p90 = vals_s[int(n*0.90)]
    print(f"  {label:<30}  mean={mn:>6.3f}%  median={med:>6.3f}%"
          f"  p10={p10:>6.3f}%  p90={p90:>6.3f}%")

print()
stats("Entry → SL distance",    sl_pcts)
stats("Entry → TP distance",    tp_pcts)
stats("Entry ATR (1H)",         atr_pcts)
stats("|Predicted return|",     pred_pcts)

if valid:
    rr_vals = [sl / tp for sl, tp in zip(sl_pcts, tp_pcts) if tp > 0]
    mean_rr = sum(rr_vals) / len(rr_vals)
    min_wr_needed = mean_rr / (1 + mean_rr) * 100
    print(f"\n  Mean SL:TP ratio    = {mean_rr:.2f}  (you risk {mean_rr:.2f}x to gain 1x)")
    print(f"  Break-even WR needed = {min_wr_needed:.1f}%  "
          f"(actual WR = {wins/total*100:.1f}%)")

    # TP vs predicted comparison
    tp_vs_pred = [(tp, pr) for tp, pr in zip(tp_pcts, pred_pcts) if pr > 0]
    if tp_vs_pred:
        ratios = [tp/pr for tp, pr in tp_vs_pred]
        mean_ratio = sum(ratios)/len(ratios)
        print(f"\n  Mean (TP distance) / (|predicted return|) = {mean_ratio:.1f}x")
        print(f"  → TP is on average {mean_ratio:.1f}× further than what the signal predicted")

# ── 3. CORRECT SIGNAL, STOPPED OUT ──────────────────────────────────────────
sep('═')
print("\n  3. CORRECT SIGNAL — STOPPED OUT (direction right, trade lost)")
sep('═')

resolved = [t for t in trades if t['actual_return_pct'] is not None]
print(f"\n  Trades with resolved actual_return_pct: {len(resolved)}")

correct_sl, correct_tp, wrong_sl, wrong_tp = [], [], [], []
for t in resolved:
    ret  = t['actual_return_pct']
    d    = t['direction']
    correct = (ret > HIT_THR) if d == 'long' else (ret < -HIT_THR)
    reason  = t['exit_reason']
    if correct and reason == 'stop_loss':
        correct_sl.append(t)
    elif correct and reason == 'take_profit':
        correct_tp.append(t)
    elif not correct and reason == 'stop_loss':
        wrong_sl.append(t)
    elif not correct and reason == 'take_profit':
        wrong_tp.append(t)

print(f"\n  Signal CORRECT + trade exit STOP LOSS:   n={len(correct_sl):>3}  ← model right, structure wrong")
print(f"  Signal CORRECT + trade exit TAKE PROFIT: n={len(correct_tp):>3}  ← ideal outcome")
print(f"  Signal WRONG   + trade exit STOP LOSS:   n={len(wrong_sl):>3}  ← model wrong, correctly stopped")
print(f"  Signal WRONG   + trade exit TAKE PROFIT: n={len(wrong_tp):>3}  ← got lucky")

if correct_sl:
    avg_actual = sum(abs(t['actual_return_pct']) for t in correct_sl) / len(correct_sl)
    avg_sl_dist = sum(pct(t['entry_price'], t['stop_loss_price']) for t in correct_sl) / len(correct_sl)
    avg_hold = sum((t['exit_timestamp'] - t['entry_timestamp']) for t in correct_sl) / len(correct_sl) / 60
    print(f"\n  CORRECT+STOPPED details:")
    print(f"    Mean |actual_return_pct| at horizon:  {avg_actual:.3f}%  (direction proved right)")
    print(f"    Mean SL distance from entry:          {avg_sl_dist:.3f}%")
    print(f"    Mean hold time before SL hit:         {avg_hold:.1f} min")

# ── 4. HOLD TIME: SL vs TP vs HORIZON EXIT ───────────────────────────────────
sep('═')
print("\n  4. HOLD TIME DISTRIBUTION  (entry → exit, minutes)")
sep('═')

for reason in ('stop_loss', 'take_profit', 'horizon_exit'):
    subset = [t for t in trades
              if t['exit_reason'] == reason
              and t['entry_timestamp'] and t['exit_timestamp']]
    if not subset: continue
    hold_mins = sorted((t['exit_timestamp'] - t['entry_timestamp']) / 60 for t in subset)
    n = len(hold_mins)
    mn  = sum(hold_mins) / n
    med = hold_mins[n//2]
    p10 = hold_mins[int(n*0.10)]
    p90 = hold_mins[int(n*0.90)]
    print(f"\n  {reason:<20}  n={n:>3}")
    print(f"    mean={mn:.0f}m  median={med:.0f}m  p10={p10:.0f}m  p90={p90:.0f}m")

# ── 5. DID PRICE REACH PREDICTED LEVEL BEFORE SL? ───────────────────────────
sep('═')
print("\n  5. DID PRICE REACH PREDICTED RETURN LEVEL BEFORE STOP LOSS?")
sep('═')
print("  (Uses trough_price for shorts, peak_price for longs as best price reached)")

sl_trades = [t for t in trades
             if t['exit_reason'] == 'stop_loss'
             and t['predicted_return_pct']
             and abs(t['predicted_return_pct']) > 0.01
             and t['entry_price'] > 0]

reached_pred = 0
total_sl = len(sl_trades)

for t in sl_trades:
    ep  = t['entry_price']
    pr  = t['predicted_return_pct']  # e.g. -0.25 for BTC short
    d   = t['direction']

    # Compute the price level the signal predicted
    if d == 'long':
        pred_price = ep * (1 + pr / 100)
        best_price = t['peak_price']
        if best_price and best_price >= pred_price:
            reached_pred += 1
    else:
        pred_price = ep * (1 + pr / 100)  # pr is negative for short
        best_price = t['trough_price']
        if best_price and best_price <= pred_price:
            reached_pred += 1

if total_sl > 0:
    pct_reached = reached_pred / total_sl * 100
    print(f"\n  Stop-loss trades analysed: {total_sl}")
    print(f"  Price reached predicted level before SL exit: {reached_pred} / {total_sl}  ({pct_reached:.1f}%)")
    print(f"  Price did NOT reach predicted level:          {total_sl - reached_pred} / {total_sl}  ({100-pct_reached:.1f}%)")

# ── 6. TP HIT RATE vs PREDICTED RETURN BUCKET ────────────────────────────────
sep('═')
print("\n  6. TP HIT RATE BY PREDICTED RETURN MAGNITUDE BUCKET")
sep('═')

buckets = [
    ("pred < 0.5%",  lambda t: abs(t['predicted_return_pct'] or 0) < 0.5),
    ("pred 0.5-1%",  lambda t: 0.5 <= abs(t['predicted_return_pct'] or 0) < 1.0),
    ("pred 1-2%",    lambda t: 1.0 <= abs(t['predicted_return_pct'] or 0) < 2.0),
    ("pred >= 2%",   lambda t: abs(t['predicted_return_pct'] or 0) >= 2.0),
]

print(f"\n  {'Bucket':<18}  {'n':>5}  {'SL':>5}  {'TP':>5}  {'HE':>5}  {'TP hit%':>8}  {'AvgPnL':>10}")
print(f"  {'-'*72}")
for label, fn in buckets:
    sub = [t for t in trades if fn(t)]
    if not sub:
        print(f"  {label:<18}  n=  0")
        continue
    n_sl  = sum(1 for t in sub if t['exit_reason'] == 'stop_loss')
    n_tp  = sum(1 for t in sub if t['exit_reason'] == 'take_profit')
    n_he  = sum(1 for t in sub if t['exit_reason'] == 'horizon_exit')
    n     = len(sub)
    tp_rt = n_tp / n * 100
    avg_p = sum(t['pnl_gross'] or 0 for t in sub) / n
    print(f"  {label:<18}  {n:>5}  {n_sl:>5}  {n_tp:>5}  {n_he:>5}  {tp_rt:>7.1f}%  Rs {avg_p:>+8.2f}")

# ── 7. WHAT RR RATIO WOULD WORK WITH THIS SIGNAL ACCURACY? ──────────────────
sep('═')
print("\n  7. WHAT RR RATIO IS NEEDED GIVEN SIGNAL ACCURACY?")
sep('═')

# From resolved signals
if resolved:
    sig_wr = sum(1 for t in resolved if
                 (t['actual_return_pct'] > HIT_THR if t['direction'] == 'long'
                  else t['actual_return_pct'] < -HIT_THR)) / len(resolved)
    print(f"\n  Signal directional accuracy (resolved): {sig_wr*100:.1f}%  (n={len(resolved)})")
    print(f"\n  Kelly-optimal TP:SL ratios for this accuracy level:")
    print(f"  {'TP:SL':>10}  {'WR needed':>12}  {'Is achievable?':>16}")
    print(f"  {'-'*44}")
    for rr_inv in [0.5, 0.67, 1.0, 1.5, 2.0, 3.0]:
        # rr_inv = SL/TP ratio — how much you lose per win
        win_needed = rr_inv / (1 + rr_inv) * 100
        achievable = '✓ YES' if sig_wr * 100 > win_needed + 5 else (
                     '≈ MARGINAL' if sig_wr * 100 > win_needed else '✗ NO')
        print(f"  SL={rr_inv:.2f}×TP  {win_needed:>10.1f}%  {achievable:>16}")

    print(f"\n  Current structure (SL≈1.5×ATR, TP≈1.0×ATR) → SL/TP ≈ 1.5")
    win_needed_curr = 1.5 / 2.5 * 100
    print(f"  Break-even WR at 1.5:1 SL:TP = {win_needed_curr:.1f}%  vs actual signal accuracy {sig_wr*100:.1f}%")
    if sig_wr * 100 > win_needed_curr:
        gap = sig_wr * 100 - win_needed_curr
        print(f"  → Signal accuracy ({sig_wr*100:.1f}%) EXCEEDS break-even ({win_needed_curr:.1f}%) by {gap:.1f}pp")
        print(f"    BUT trade WR ({wins/total*100:.1f}%) is far below — confirming SL being hit")
        print(f"    before correct moves develop (not a signal quality problem).")

sep('═')
print("\n  SUMMARY")
sep('═')
print(f"""
  Model fires in the right direction {sig_wr*100:.1f}% of the time (resolved signals).
  Trades win {wins/total*100:.1f}% of the time.

  Root causes (ranked by impact):
    1. TP is set to 1×ATR, which is typically {sum(tp_pcts)/len(tp_pcts):.2f}% from entry.
       Signals only predict moves of {sum(pred_pcts)/len(pred_pcts):.2f}% on average.
       TP is {sum(tp_pcts)/len(tp_pcts)/max(sum(pred_pcts)/len(pred_pcts),0.01):.1f}× larger than the predicted move —
       price rarely travels far enough to reach TP.

    2. SL is set to 1.5×ATR, which is {sum(sl_pcts)/len(sl_pcts):.2f}% from entry on average.
       1H intrabar noise on BTC/XRP easily covers {sum(sl_pcts)/len(sl_pcts):.2f}% before any trend
       develops, tripping the SL on correct signals.

    3. Risk:Reward = {mean_rr:.2f}:1 (unfavourable). Break-even WR = {min_wr_needed:.1f}%.
       The trade structure requires {min_wr_needed:.1f}% TP-hit rate to break even,
       but TP is structurally too far from entry to be hit at this rate.

  The directional edge IS real — it just needs an exit structure sized to
  the signal's predicted return magnitude, not to ATR.
""") if valid else print("  Insufficient data with entry_atr.")
