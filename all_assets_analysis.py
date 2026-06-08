"""
Full MFE/MAE + TP/SL grid analysis for all kronos-mini 1H assets.
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

ASSETS       = ['BTCUSD', 'BNBUSD', 'ETHUSD', 'XRPUSD']
ATR_PERIOD   = 14
HORIZON_SECS = 6 * 3600
HIT_THR      = 0.15  # directional win threshold %

def get_trade_data(symbol):
    trades = conn.execute("""
        SELECT t.id, t.entry_price, t.exit_price, t.entry_timestamp, t.exit_timestamp,
               t.fees, t.notional_value, t.pnl_net, t.peak_price, t.trough_price,
               t.exit_reason, s.direction, s.actual_return_pct
        FROM trades t JOIN signals s ON s.id = t.signal_id
        WHERE s.model_source='kronos-mini' AND s.symbol=?
          AND s.quality_flag IS NULL AND t.exit_timestamp IS NOT NULL
        ORDER BY t.entry_timestamp
    """, (symbol,)).fetchall()

    result = []
    for t in trades:
        ep  = float(t['entry_price'] or 0)
        xp  = float(t['exit_price']  or ep)
        pk  = float(t['peak_price']  or ep)
        tr  = float(t['trough_price'] or ep)
        drx = t['direction']
        ets = int(t['entry_timestamp'] or 0)
        xts = int(t['exit_timestamp']  or 0)

        # Reconstruct 1H ATR at entry
        candles = conn.execute("""
            SELECT high,low,close FROM ohlcv
            WHERE symbol=? AND timeframe='1h' AND timestamp < ?
            ORDER BY timestamp DESC LIMIT ?
        """, (symbol, ets, ATR_PERIOD+1)).fetchall()
        candles = list(reversed(candles))
        if len(candles) >= 2:
            trs = [max(float(candles[i]['high'])-float(candles[i]['low']),
                       abs(float(candles[i]['high'])-float(candles[i-1]['close'])),
                       abs(float(candles[i]['low'])-float(candles[i-1]['close'])))
                   for i in range(1, len(candles))]
            atr = sum(trs)/len(trs)
        else:
            atr = ep*0.008

        # Candles over hold window
        hold = conn.execute("""
            SELECT high,low,close FROM ohlcv
            WHERE symbol=? AND timeframe='1h'
              AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
        """, (symbol, ets, ets+HORIZON_SECS+3600)).fetchall()

        if drx == 'long':
            mfe = (pk-ep)/ep*100; mae = (ep-tr)/ep*100; cap = (xp-ep)/ep*100
        else:
            mfe = (ep-tr)/ep*100; mae = (pk-ep)/ep*100; cap = (ep-xp)/ep*100

        ohlcv = conn.execute("""
            SELECT MAX(high) mh, MIN(low) ml FROM ohlcv
            WHERE symbol=? AND timeframe='1h' AND timestamp >= ? AND timestamp <= ?
        """, (symbol, ets, xts)).fetchone()
        mh = float(ohlcv['mh'] or ep)
        ml = float(ohlcv['ml'] or ep)
        mkt_fav = ((mh-ep)/ep*100) if drx == 'long' else ((ep-ml)/ep*100)

        actual = float(t['actual_return_pct'] or 0)
        win = (actual > HIT_THR) if drx == 'long' else (actual < -HIT_THR)

        result.append(dict(
            dir=drx, ep=ep, xp=xp, mfe=mfe, mae=mae, cap=cap,
            mkt_fav=mkt_fav, hold=(xts-ets)/3600,
            pnl=float(t['pnl_net'] or 0), fees=float(t['fees'] or 0),
            rsn=t['exit_reason'] or '?', win=win, actual=actual,
            atr=atr, notional=float(t['notional_value'] or 0),
            fee_abs=float(t['fees'] or 0),
            hold_candles=[dict(r) for r in hold]
        ))
    return result


def sim_trade(d, tp_m, sl_m):
    ep, drx, atr = d['ep'], d['dir'], d['atr']
    tp_px = (ep+tp_m*atr) if drx == 'long' else (ep-tp_m*atr)
    sl_px = (ep-sl_m*atr) if drx == 'long' else (ep+sl_m*atr)
    rx, rsn = None, 'horizon'
    for c in d['hold_candles']:
        h, l = float(c['high']), float(c['low'])
        if drx == 'long':
            if l <= sl_px: rx=sl_px; rsn='sl'; break
            if h >= tp_px: rx=tp_px; rsn='tp'; break
        else:
            if h >= sl_px: rx=sl_px; rsn='sl'; break
            if l <= tp_px: rx=tp_px; rsn='tp'; break
    if rx is None:
        rx = float(d['hold_candles'][-1]['close']) if d['hold_candles'] else ep
    cap = ((rx-ep)/ep) if drx == 'long' else ((ep-rx)/ep)
    pnl = (cap*d['notional'] - d['fee_abs']) if d['notional'] > 0 else cap
    return cap*100, rsn, pnl


# Fine multipliers: 0.25 to 5.0 in steps of 0.25
mults = [round(x*0.25, 2) for x in range(1, 21)]

for symbol in ASSETS:
    data = get_trade_data(symbol)
    if not data:
        continue
    n = len(data)
    wins   = [d for d in data if d['win']]
    losses = [d for d in data if not d['win']]
    sl_tr  = [d for d in data if d['rsn'] == 'stop_loss']
    tp_tr  = [d for d in data if d['rsn'] == 'take_profit']
    hor_tr = [d for d in data if d['rsn'] == 'horizon_exit']
    longs  = [d for d in data if d['dir'] == 'long']
    shorts = [d for d in data if d['dir'] == 'short']

    tot_pnl  = sum(d['pnl']  for d in data)
    tot_fees = sum(d['fees'] for d in data)
    avg_mfe  = sum(d['mfe']     for d in data)/n
    avg_mae  = sum(d['mae']     for d in data)/n
    avg_cap  = sum(d['cap']     for d in data)/n
    avg_mkt  = sum(d['mkt_fav'] for d in data)/n

    avg_wp = sum(d['pnl'] for d in wins)/len(wins)     if wins   else 0
    avg_lp = sum(d['pnl'] for d in losses)/len(losses) if losses else 0
    rr     = abs(avg_wp/avg_lp) if avg_lp else 0
    be_wr  = 1/(1+rr)*100 if rr else 50
    cur_wr = len(wins)/n*100

    lwr = len([d for d in longs  if d['win']])/len(longs)*100  if longs  else 0
    swr = len([d for d in shorts if d['win']])/len(shorts)*100 if shorts else 0

    # Grid search — profitable combos
    best_pos = []
    for tp_m in mults:
        for sl_m in mults:
            results = [sim_trade(d, tp_m, sl_m) for d in data]
            pnl = sum(r[2] for r in results)
            if pnl > 0:
                dist = ((tp_m-1.0)**2+(sl_m-1.5)**2)**0.5
                n_tp = sum(1 for r in results if r[1] == 'tp')
                n_sl = sum(1 for r in results if r[1] == 'sl')
                wr_s = sum(1 for r in results if r[0] > 0.165)/n*100
                best_pos.append((dist, tp_m, sl_m, pnl, tp_m/sl_m,
                                  1/(1+tp_m/sl_m)*100, n_tp, n_sl, wr_s))
    best_pos.sort()

    # Single-param feasibility
    sl15_best = None
    for tp_m in mults:
        res = [sim_trade(d, tp_m, 1.5) for d in data]
        if sum(r[2] for r in res) > 0 and sl15_best is None:
            sl15_best = (tp_m, sum(r[2] for r in res))

    tp10_best = None
    for sl_m in mults:
        res = [sim_trade(d, 1.0, sl_m) for d in data]
        if sum(r[2] for r in res) > 0 and tp10_best is None:
            tp10_best = (sl_m, sum(r[2] for r in res))

    # Per-trade table
    print()
    print("=" * 78)
    print(f"  {symbol}  |  {n} trades  |  {len(longs)}L / {len(shorts)}S  |  Net PnL: Rs {tot_pnl:+,.0f}  |  Fees: Rs {tot_fees:,.0f}")
    print("=" * 78)
    print(f"  WR: {cur_wr:.0f}%  (longs {lwr:.0f}% | shorts {swr:.0f}%)   BE-WR: {be_wr:.1f}%   R:R: {rr:.2f}:1")
    print(f"  Exits: {len(sl_tr)} SL | {len(tp_tr)} TP | {len(hor_tr)} horizon")
    print()
    print(f"  {'#':>2}  {'Dir':5}  {'MFE%':>6}  {'MAE%':>6}  {'Cap%':>6}  {'MktFav%':>8}  {'Left%':>6}  {'Exit':12}  {'PnL':>8}")
    print("  " + "-"*72)
    for i, d in enumerate(data, 1):
        left = d['mkt_fav'] - d['cap']
        print(f"  {i:>2}  {d['dir']:5}  {d['mfe']:>+6.2f}%  {d['mae']:>6.2f}%  {d['cap']:>+6.2f}%  "
              f"{d['mkt_fav']:>+8.2f}%  {left:>+6.2f}%  {d['rsn']:12}  {d['pnl']:>+8.0f}")
    print()
    print(f"  Avg MFE: {avg_mfe:+.3f}%  |  Avg MAE: {avg_mae:.3f}%  |  Avg captured: {avg_cap:+.3f}%")
    print(f"  Avg mkt-favorable move: {avg_mkt:+.3f}%  |  Avg left on table: {avg_mkt-avg_cap:+.3f}%")
    print(f"  Avg winner PnL: Rs {avg_wp:+,.0f}  |  Avg loser PnL: Rs {avg_lp:+,.0f}")
    print()

    # TP/SL grid advice
    if sl15_best:
        print(f"  Single-param: TP-only (keep SL=1.5x): TP={sl15_best[0]:.2f}x -> Rs {sl15_best[1]:+.0f}")
    else:
        print(f"  Single-param: TP-only (keep SL=1.5x): NO profitable TP found")

    if tp10_best:
        print(f"  Single-param: SL-only (keep TP=1.0x): SL={tp10_best[0]:.2f}x -> Rs {tp10_best[1]:+.0f}")
    else:
        print(f"  Single-param: SL-only (keep TP=1.0x): NO profitable SL found")

    if best_pos:
        print(f"  Closest profitable (both params): TP={best_pos[0][1]:.2f}x SL={best_pos[0][2]:.2f}x -> Rs {best_pos[0][3]:+.0f}  R:R={best_pos[0][4]:.1f}:1  BE-WR={best_pos[0][5]:.1f}%")
        print(f"  Top-5 by closeness to current (TP=1.0x SL=1.5x):")
        print(f"    {'TP':>6}  {'SL':>6}  {'PnL':>9}  {'R:R':>5}  {'BE-WR':>6}  {'TP-h':>4}  {'SL-h':>4}")
        for row in best_pos[:5]:
            print(f"    {row[1]:>5.2f}x  {row[2]:>5.2f}x  {row[3]:>+9.0f}  {row[4]:>5.1f}  {row[5]:>5.1f}%  {row[6]:>4}  {row[7]:>4}")
    else:
        print(f"  *** NO PROFITABLE COMBO FOUND IN ENTIRE GRID (0.25x-5.0x range) ***")

conn.close()
