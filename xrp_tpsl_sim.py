"""
Simulate XRP kronos-mini trades with different TP/SL multipliers.
Uses actual 1H OHLCV candles to replay each trade chronologically.
Finds the minimum TP/SL widening that produces positive net PnL.
"""
import sqlite3, sys, itertools
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ── Fetch trades ─────────────────────────────────────────────────────────────
trades = conn.execute("""
    SELECT t.id, t.signal_id, t.entry_price, t.entry_timestamp, t.exit_timestamp,
           t.fees, t.notional_value, t.size_contracts,
           s.direction, s.signal_timestamp
    FROM trades t
    JOIN signals s ON s.id = t.signal_id
    WHERE s.model_source = 'kronos-mini'
      AND s.symbol = 'XRPUSD'
      AND s.quality_flag IS NULL
      AND t.exit_timestamp IS NOT NULL        -- skip open trade
    ORDER BY t.entry_timestamp
""").fetchall()

ATR_PERIOD   = 14
HORIZON_SECS = 6 * 3600          # 6H horizon
FEE_RT_PCT   = 0.165 / 100       # round-trip fee rate

# ── Per-trade setup ───────────────────────────────────────────────────────────
trade_data = []
for t in trades:
    ep  = float(t['entry_price'])
    ets = int(t['entry_timestamp'])
    drx = t['direction']

    # Reconstruct 1H ATR at entry (14-period from candles before entry)
    candles = conn.execute("""
        SELECT high, low, close FROM ohlcv
        WHERE symbol='XRPUSD' AND timeframe='1h'
          AND timestamp < ?
        ORDER BY timestamp DESC LIMIT ?
    """, (ets, ATR_PERIOD + 1)).fetchall()
    candles = list(reversed(candles))

    if len(candles) >= 2:
        trs = []
        for i in range(1, len(candles)):
            h, l, pc = float(candles[i]['high']), float(candles[i]['low']), float(candles[i-1]['close'])
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        atr = sum(trs) / len(trs)
    else:
        atr = ep * 0.008   # ~0.8% fallback

    # Fetch 1H OHLCV candles covering the hold window (entry → entry+6H+buffer)
    hold_candles = conn.execute("""
        SELECT timestamp, open, high, low, close FROM ohlcv
        WHERE symbol='XRPUSD' AND timeframe='1h'
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    """, (ets, ets + HORIZON_SECS + 3600)).fetchall()
    hold_candles = [dict(r) for r in hold_candles]

    # Fee rate from actual trade (fee / notional), fall back to flat rate
    notional = float(t['notional_value'] or 0)
    fee_rate = (float(t['fees'] or 0) / notional) if notional > 0 else FEE_RT_PCT

    trade_data.append({
        'ep': ep, 'ets': ets, 'dir': drx, 'atr': atr,
        'candles': hold_candles,
        'notional': notional,
        'fee_rate': fee_rate,
        'fee_abs': float(t['fees'] or 0),
    })

print(f"Loaded {len(trade_data)} closed trades")
print(f"\nReconstructed ATR at entry:")
for i, d in enumerate(trade_data, 1):
    print(f"  #{i:>2}  {d['dir']:5}  entry={d['ep']:.4f}  1H-ATR={d['atr']:.5f} ({d['atr']/d['ep']*100:.3f}%)")


def simulate_trade(d, tp_mult, sl_mult):
    """
    Replay a single trade with given TP/SL multipliers.
    Returns (captured_pct, exit_reason, pnl_net) using notional-scaled PnL.
    """
    ep  = d['ep']
    drx = d['dir']
    atr = d['atr']
    fee = d['fee_abs'] if d['notional'] > 0 else d['fee_rate'] * (d['notional'] or 1)

    tp_dist = tp_mult * atr
    sl_dist = sl_mult * atr

    if drx == 'long':
        tp_px = ep + tp_dist
        sl_px = ep - sl_dist
    else:
        tp_px = ep - tp_dist
        sl_px = ep + sl_dist

    # Walk candles: check SL first (conservative — SL fires before TP within same candle)
    result_px  = None
    exit_rsn   = 'horizon'
    for c in d['candles']:
        h, l = float(c['high']), float(c['low'])
        if drx == 'long':
            if l <= sl_px:
                result_px = sl_px;  exit_rsn = 'sl';  break
            if h >= tp_px:
                result_px = tp_px;  exit_rsn = 'tp';  break
        else:
            if h >= sl_px:
                result_px = sl_px;  exit_rsn = 'sl';  break
            if l <= tp_px:
                result_px = tp_px;  exit_rsn = 'tp';  break

    if result_px is None:
        # Horizon exit — use last candle close
        if d['candles']:
            result_px = float(d['candles'][-1]['close'])
        else:
            result_px = ep
        exit_rsn = 'horizon'

    if drx == 'long':
        cap_pct = (result_px - ep) / ep
    else:
        cap_pct = (ep - result_px) / ep

    # PnL: scale by notional if available, else use cap_pct directly
    if d['notional'] > 0:
        pnl_gross = cap_pct * d['notional']
        pnl_net   = pnl_gross - d['fee_abs']
    else:
        pnl_net   = cap_pct - FEE_RT_PCT   # raw pct after fee

    return cap_pct * 100, exit_rsn, pnl_net


# ── Baseline: current config (TP=1.0x, SL=1.5x) ─────────────────────────────
print("\n" + "=" * 70)
print("BASELINE  TP=1.0x ATR  SL=1.5x ATR")
print("=" * 70)
base_results = [simulate_trade(d, 1.0, 1.5) for d in trade_data]
print(f"{'#':>2}  {'Dir':5}  {'Cap%':>6}  {'Exit':7}  {'PnL(Rs)':>9}")
print("-" * 42)
for i, (cap, rsn, pnl) in enumerate(base_results, 1):
    print(f"{i:>2}  {trade_data[i-1]['dir']:5}  {cap:>+6.2f}%  {rsn:7}  {pnl:>+9.1f}")
base_total = sum(r[2] for r in base_results)
base_tps   = sum(1 for r in base_results if r[1]=='tp')
base_sls   = sum(1 for r in base_results if r[1]=='sl')
base_hrs   = sum(1 for r in base_results if r[1]=='horizon')
print(f"\nTotal PnL: Rs {base_total:+,.1f}   ({base_tps} TP / {base_sls} SL / {base_hrs} horizon)")


# ── Grid search ───────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("GRID SEARCH  — net PnL at each (TP_mult, SL_mult) combination")
print("=" * 70)

# Sweep multipliers
tp_mults = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 4.0]
sl_mults = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 4.0]

grid = {}
for tp_m in tp_mults:
    for sl_m in sl_mults:
        results = [simulate_trade(d, tp_m, sl_m) for d in trade_data]
        total_pnl  = sum(r[2] for r in results)
        n_tp   = sum(1 for r in results if r[1]=='tp')
        n_sl   = sum(1 for r in results if r[1]=='sl')
        n_hor  = sum(1 for r in results if r[1]=='horizon')
        n_wins = sum(1 for r in results if r[0] > FEE_RT_PCT * 100)
        grid[(tp_m, sl_m)] = {
            'pnl': total_pnl, 'tp': n_tp, 'sl': n_sl, 'hor': n_hor,
            'wr': n_wins / len(results) * 100,
        }

# Print grid as heatmap of PnL
print(f"\n{'TP\\ SL':>8}", end='')
for sl_m in sl_mults:
    print(f"  SL={sl_m:.2f}x", end='')
print()
print("-" * (10 + 11 * len(sl_mults)))

profitable = []
for tp_m in tp_mults:
    print(f"TP={tp_m:.2f}x ", end='')
    for sl_m in sl_mults:
        g = grid[(tp_m, sl_m)]
        pnl = g['pnl']
        marker = '***' if pnl > 0 else '   '
        print(f"  {pnl:>+6.0f}{marker}", end='')
        if pnl > 0:
            profitable.append((tp_m, sl_m, pnl, g['wr'], g['tp'], g['sl'], g['hor']))
    print()

# ── Profitable combinations sorted by minimum change from current ─────────────
print(f"\n{'='*70}")
print("PROFITABLE COMBINATIONS (sorted by closeness to current TP=1.0x SL=1.5x)")
print(f"{'='*70}")
print(f"{'TP_mult':>8}  {'SL_mult':>8}  {'PnL(Rs)':>9}  {'WR%':>6}  {'TP':>4}  {'SL':>4}  {'Hor':>4}  Change from current")
print("-" * 75)

CURR_TP, CURR_SL = 1.0, 1.5
for tp_m, sl_m, pnl, wr, n_tp, n_sl, n_hor in sorted(profitable, key=lambda x: (x[0]-CURR_TP)**2 + (x[1]-CURR_SL)**2):
    delta_tp = tp_m - CURR_TP
    delta_sl = sl_m - CURR_SL
    change = f"TP {delta_tp:+.2f}x  SL {delta_sl:+.2f}x"
    print(f"{tp_m:>8.2f}x  {sl_m:>8.2f}x  {pnl:>+9.1f}  {wr:>6.1f}%  {n_tp:>4}  {n_sl:>4}  {n_hor:>4}  {change}")

# ── Deep dive on the minimum-change profitable config ─────────────────────────
if profitable:
    best = sorted(profitable, key=lambda x: (x[0]-CURR_TP)**2 + (x[1]-CURR_SL)**2)[0]
    tp_m, sl_m = best[0], best[1]
    print(f"\n{'='*70}")
    print(f"MINIMUM CHANGE WINNER:  TP={tp_m:.2f}x ATR   SL={sl_m:.2f}x ATR")
    print(f"{'='*70}")
    results = [simulate_trade(d, tp_m, sl_m) for d in trade_data]
    print(f"{'#':>2}  {'Dir':5}  {'ATR%':>5}  {'TP%':>6}  {'SL%':>6}  {'Cap%':>6}  {'Exit':7}  {'PnL(Rs)':>9}")
    print("-" * 62)
    for i, ((cap, rsn, pnl), d) in enumerate(zip(results, trade_data), 1):
        atr_pct = d['atr'] / d['ep'] * 100
        tp_pct  = tp_m * atr_pct
        sl_pct  = sl_m * atr_pct
        print(f"{i:>2}  {d['dir']:5}  {atr_pct:>5.3f}%  {tp_pct:>+6.3f}%  {sl_pct:>6.3f}%  "
              f"{cap:>+6.2f}%  {rsn:7}  {pnl:>+9.1f}")
    total = sum(r[2] for r in results)
    print(f"\nTotal net PnL: Rs {total:+,.1f}")
    rr = tp_m / sl_m
    be_wr = 1 / (1 + rr) * 100
    print(f"R:R = {rr:.2f}:1   Break-even WR = {be_wr:.1f}%")

conn.close()
