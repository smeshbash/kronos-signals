"""
TP/SL grid search for kronos-mini XRPUSD — post-fix trades only.
Only includes trades from 2026-06-07 09:14 onwards (after contract size fix).
Fee threshold: post-fix trades have fee% <= 0.3% of notional.
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

ATR_PERIOD   = 14
HORIZON_SECS = 6 * 3600
FIX_THRESH   = 0.003   # fee/notional > 0.3% = pre-fix inflated trade

# Current configured values for kronos-mini XRPUSD (set based on old contaminated data)
CURR_TP = 2.00
CURR_SL = 0.25
# Original default (before any tuning)
ORIG_TP = 1.00
ORIG_SL = 1.50

trades = conn.execute("""
    SELECT t.id, t.entry_price, t.exit_price, t.entry_timestamp, t.exit_timestamp,
           t.fees, t.notional_value, t.pnl_net, t.peak_price, t.trough_price,
           t.exit_reason, s.direction, s.actual_return_pct,
           datetime(t.entry_timestamp,'unixepoch') as entry_dt
    FROM trades t JOIN signals s ON s.id = t.signal_id
    WHERE s.model_source='kronos-mini' AND s.symbol='XRPUSD'
      AND s.quality_flag IS NULL AND t.exit_timestamp IS NOT NULL
    ORDER BY t.entry_timestamp
""").fetchall()

# Filter to post-fix only
post_fix = [t for t in trades
            if float(t['fees'] or 0) / max(float(t['notional_value'] or 1), 1) <= FIX_THRESH]

print(f"Total closed XRPUSD trades (kronos-mini): {len(trades)}")
print(f"Post-fix trades (correct fees):            {len(post_fix)}")
print(f"Pre-fix trades excluded:                   {len(trades) - len(post_fix)}")
print()

# Build simulation data
data = []
for t in post_fix:
    ep  = float(t['entry_price'] or 0)
    xp  = float(t['exit_price']  or ep)
    pk  = float(t['peak_price']  or ep)
    tr  = float(t['trough_price'] or ep)
    drx = t['direction']
    ets = int(t['entry_timestamp'] or 0)
    xts = int(t['exit_timestamp']  or 0)

    candles = conn.execute("""
        SELECT high,low,close FROM ohlcv
        WHERE symbol='XRPUSD' AND timeframe='1h' AND timestamp < ?
        ORDER BY timestamp DESC LIMIT ?
    """, (ets, ATR_PERIOD+1)).fetchall()
    candles = list(reversed(candles))
    if len(candles) >= 2:
        trs = [max(float(candles[i]['high'])-float(candles[i]['low']),
                   abs(float(candles[i]['high'])-float(candles[i-1]['close'])),
                   abs(float(candles[i]['low'])-float(candles[i-1]['close'])))
               for i in range(1, len(candles))]
        atr = sum(trs) / len(trs)
    else:
        atr = ep * 0.008

    hold = conn.execute("""
        SELECT high,low,close FROM ohlcv
        WHERE symbol='XRPUSD' AND timeframe='1h'
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
    """, (ets, ets + HORIZON_SECS + 3600)).fetchall()

    if drx == 'long':
        mfe = (pk-ep)/ep*100; mae = (ep-tr)/ep*100; cap = (xp-ep)/ep*100
    else:
        mfe = (ep-tr)/ep*100; mae = (pk-ep)/ep*100; cap = (ep-xp)/ep*100

    data.append(dict(
        dir=drx, ep=ep, xp=xp, mfe=mfe, mae=mae, cap=cap,
        hold=(xts-ets)/3600,
        pnl=float(t['pnl_net'] or 0), fees=float(t['fees'] or 0),
        rsn=t['exit_reason'] or '?', atr=atr,
        notional=float(t['notional_value'] or 0),
        fee_abs=float(t['fees'] or 0),
        hold_candles=[dict(r) for r in hold],
        entry_dt=t['entry_dt']
    ))

conn.close()

# ── Per-trade breakdown ───────────────────────────────────────────────────────
n = len(data)
wins = [d for d in data if d['pnl'] > 0]
losses = [d for d in data if d['pnl'] <= 0]
sl_tr = [d for d in data if d['rsn'] == 'stop_loss']
tp_tr = [d for d in data if d['rsn'] == 'take_profit']

avg_mfe = sum(d['mfe'] for d in data) / n
avg_mae = sum(d['mae'] for d in data) / n
avg_cap = sum(d['cap'] for d in data) / n
avg_mkt = 0  # skip market-fav, not needed here
tot_pnl = sum(d['pnl'] for d in data)
tot_fee = sum(d['fees'] for d in data)
avg_wp  = sum(d['pnl'] for d in wins)   / len(wins)   if wins   else 0
avg_lp  = sum(d['pnl'] for d in losses) / len(losses) if losses else 0
rr      = abs(avg_wp / avg_lp) if avg_lp else 0
be_wr   = 1 / (1+rr) * 100 if rr else 50
wr_pnl  = len(wins) / n * 100

print("=" * 72)
print(f"  KRONOS-MINI  XRPUSD  —  POST-FIX TRADES ONLY ({n} trades)")
print("=" * 72)
print(f"  {'#':>2}  {'Dir':5}  {'ATR%':>5}  {'MFE%':>6}  {'MAE%':>6}  {'Cap%':>6}  "
      f"{'Exit':14}  {'Fee':>7}  {'PnL':>9}  {'Date'}")
print("  " + "-"*78)
for i, d in enumerate(data, 1):
    atr_pct = d['atr'] / d['ep'] * 100
    print(f"  {i:>2}  {d['dir']:5}  {atr_pct:>5.3f}%  {d['mfe']:>+6.2f}%  {d['mae']:>6.2f}%  "
          f"{d['cap']:>+6.2f}%  {d['rsn']:14}  {d['fees']:>7.2f}  "
          f"{d['pnl']:>+9.2f}  {d['entry_dt']}")

print()
print(f"  WR (PnL-based): {wr_pnl:.0f}%  ({len(wins)}W / {len(losses)}L)")
print(f"  Exits: {len(sl_tr)} SL  |  {len(tp_tr)} TP")
print(f"  Avg MFE: {avg_mfe:+.3f}%  |  Avg MAE: {avg_mae:.3f}%  |  Avg captured: {avg_cap:+.3f}%")
print(f"  Total PnL: Rs {tot_pnl:+,.2f}  |  Total fees: Rs {tot_fee:.2f}  |  EV/trade: Rs {tot_pnl/n:+,.2f}")
print(f"  Avg winner: Rs {avg_wp:+,.2f}  |  Avg loser: Rs {avg_lp:+,.2f}  |  R:R: {rr:.2f}:1")
print(f"  Break-even WR: {be_wr:.1f}%  |  Current WR: {wr_pnl:.0f}%  "
      f"({'ABOVE' if wr_pnl > be_wr else 'BELOW'} BE by {abs(wr_pnl-be_wr):.1f}pp)")

# ── Simulation function ───────────────────────────────────────────────────────
def sim_trade(d, tp_m, sl_m):
    ep, drx, atr = d['ep'], d['dir'], d['atr']
    tp_px = (ep + tp_m*atr) if drx == 'long' else (ep - tp_m*atr)
    sl_px = (ep - sl_m*atr) if drx == 'long' else (ep + sl_m*atr)
    rx, rsn = None, 'horizon'
    for c in d['hold_candles']:
        h, l = float(c['high']), float(c['low'])
        if drx == 'long':
            if l <= sl_px: rx = sl_px; rsn = 'sl'; break
            if h >= tp_px: rx = tp_px; rsn = 'tp'; break
        else:
            if h >= sl_px: rx = sl_px; rsn = 'sl'; break
            if l <= tp_px: rx = tp_px; rsn = 'tp'; break
    if rx is None:
        rx = float(d['hold_candles'][-1]['close']) if d['hold_candles'] else ep
    cap = ((rx-ep)/ep) if drx == 'long' else ((ep-rx)/ep)
    pnl = (cap * d['notional'] - d['fee_abs']) if d['notional'] > 0 else cap
    return cap*100, rsn, pnl

# ── Baseline: current configured values (TP=2.0x SL=0.25x) ───────────────────
print()
print("=" * 72)
print(f"  BASELINE — current config: TP={CURR_TP}x  SL={CURR_SL}x  (set from old contaminated data)")
print("=" * 72)
base_res = [sim_trade(d, CURR_TP, CURR_SL) for d in data]
base_pnl = sum(r[2] for r in base_res)
for i, (cap, rsn, pnl) in enumerate(base_res, 1):
    atr_pct = data[i-1]['atr'] / data[i-1]['ep'] * 100
    tp_pct  = CURR_TP * atr_pct
    sl_pct  = CURR_SL * atr_pct
    print(f"  #{i:>2}  {data[i-1]['dir']:5}  TP={tp_pct:.3f}%  SL={sl_pct:.3f}%  -> {rsn:8}  Rs {pnl:+,.2f}")
print(f"  Total: Rs {base_pnl:+,.2f}   ({sum(1 for r in base_res if r[1]=='tp')} TP / "
      f"{sum(1 for r in base_res if r[1]=='sl')} SL / "
      f"{sum(1 for r in base_res if r[1]=='horizon')} horizon)")

# ── Baseline: original default (TP=1.0x SL=1.5x) ─────────────────────────────
orig_res = [sim_trade(d, ORIG_TP, ORIG_SL) for d in data]
orig_pnl = sum(r[2] for r in orig_res)
print()
print(f"  ORIGINAL default: TP={ORIG_TP}x  SL={ORIG_SL}x  ->  Rs {orig_pnl:+,.2f}   "
      f"({sum(1 for r in orig_res if r[1]=='tp')} TP / "
      f"{sum(1 for r in orig_res if r[1]=='sl')} SL / "
      f"{sum(1 for r in orig_res if r[1]=='horizon')} horizon)")

# ── Fine grid search ──────────────────────────────────────────────────────────
mults = [round(x*0.25, 2) for x in range(1, 21)]

best_pos = []
for tp_m in mults:
    for sl_m in mults:
        results = [sim_trade(d, tp_m, sl_m) for d in data]
        pnl = sum(r[2] for r in results)
        if pnl > 0:
            # Distance from BOTH reference points
            dist_curr = ((tp_m-CURR_TP)**2 + (sl_m-CURR_SL)**2)**0.5
            dist_orig = ((tp_m-ORIG_TP)**2 + (sl_m-ORIG_SL)**2)**0.5
            n_tp = sum(1 for r in results if r[1] == 'tp')
            n_sl = sum(1 for r in results if r[1] == 'sl')
            rr_v = tp_m / sl_m
            be   = 1/(1+rr_v)*100
            best_pos.append((dist_curr, dist_orig, tp_m, sl_m, pnl, rr_v, be, n_tp, n_sl))

best_pos.sort()  # sorted by closeness to current config

print()
print("=" * 80)
print("  GRID SEARCH RESULTS  (0.25x–5.0x, post-fix trades only, correct fees)")
print("=" * 80)
if best_pos:
    print(f"  {'TP':>6}  {'SL':>6}  {'PnL':>9}  {'R:R':>5}  {'BE-WR':>6}  "
          f"{'TP-h':>4}  {'SL-h':>4}  {'Dist-curr':>10}  {'Dist-orig':>10}")
    print("  " + "-"*72)
    for row in best_pos[:15]:
        dc, do, tp_m, sl_m, pnl, rr_v, be, n_tp, n_sl = row
        marker = ''
        if abs(tp_m-CURR_TP)<0.01 and abs(sl_m-CURR_SL)<0.01:
            marker = '  <- current config'
        elif abs(tp_m-ORIG_TP)<0.01 and abs(sl_m-ORIG_SL)<0.01:
            marker = '  <- original default'
        print(f"  {tp_m:>5.2f}x  {sl_m:>5.2f}x  {pnl:>+9.1f}  {rr_v:>5.1f}  "
              f"{be:>5.1f}%  {n_tp:>4}  {n_sl:>4}  {dc:>10.3f}  {do:>10.3f}{marker}")
    print()
    print(f"  Total profitable combos: {len(best_pos)} out of {len(mults)**2} tested")

    # Best by PnL
    by_pnl = sorted(best_pos, key=lambda x: -x[4])
    print(f"\n  Top-5 by PnL (not by distance):")
    for row in by_pnl[:5]:
        dc, do, tp_m, sl_m, pnl, rr_v, be, n_tp, n_sl = row
        print(f"  TP={tp_m:.2f}x  SL={sl_m:.2f}x  -> Rs {pnl:+,.1f}  R:R={rr_v:.1f}:1  "
              f"BE-WR={be:.1f}%  ({n_tp} TP / {n_sl} SL)")

    # Deep dive: closest combo to current
    best = best_pos[0]
    dc, do, tp_m, sl_m, pnl, rr_v, be, n_tp, n_sl = best
    print()
    print("=" * 72)
    print(f"  CLOSEST TO CURRENT CONFIG: TP={tp_m:.2f}x  SL={sl_m:.2f}x  ->  Rs {pnl:+,.1f}")
    print("=" * 72)
    results = [sim_trade(d, tp_m, sl_m) for d in data]
    for i, ((cap, rsn, pnl_t), d) in enumerate(zip(results, data), 1):
        atr_pct = d['atr'] / d['ep'] * 100
        print(f"  #{i:>2}  {d['dir']:5}  ATR={atr_pct:.3f}%  "
              f"TP={tp_m*atr_pct:.3f}%  SL={sl_m*atr_pct:.3f}%  -> {rsn:8}  Rs {pnl_t:+,.2f}")
    print(f"  Total: Rs {sum(r[2] for r in results):+,.2f}   R:R={rr_v:.1f}:1   BE-WR={be:.1f}%")
else:
    print("  NO PROFITABLE COMBINATION FOUND in post-fix data")
    print("  (This means even with correct fees, no TP/SL config produces positive PnL)")
    print("  -> XRP halt decision stands for direction-accuracy reasons.")
