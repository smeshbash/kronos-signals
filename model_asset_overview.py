"""
Complete model × asset overview for the Kronos trading system.
Shows: process status, signal-level halt, TP/SL config, trade stats,
       WR, PnL, fee drag, EV/trade, direction bias, overlaps.
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
from db import DB_PATH

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# ── Static config (mirrors 06_execution.py) ──────────────────────────────────
PROCESS_STATUS = {
    'custom':         ('M4',  'HALTED',  '—',    '—'),   # halted 2026-06-05
    'kronos-mini':    ('M13', 'ACTIVE',  '1H',   '6H'),
    'kronos-base':    ('M14', 'HALTED',  '1H',   '6H'),  # halted 2026-06-07
    'kronos-mini-4h': ('M15', 'HALTED',  '4H',  '24H'),  # halted 2026-06-07
    'kronos-base-4h': ('M16', 'ACTIVE',  '4H',  '24H'),
}

# (timeframe, tp_mult, sl_mult)
SYMBOL_CONFIG = {
    ('kronos-mini',    'BNBUSD'): ('1h', 1.25, 0.50),
    ('kronos-mini',    'BTCUSD'): ('1h', 2.00, 0.50),
    ('kronos-mini',    'XRPUSD'): ('1h', 2.00, 0.25),
    ('kronos-base',    'BNBUSD'): ('1h', 1.00, 0.25),
    ('kronos-base',    'ETHUSD'): ('1h', 1.00, 0.25),
    ('kronos-mini-4h', 'BTCUSD'): ('4h', 2.00, 0.25),
}
DEFAULT_CONFIG = {
    '1h': ('1h', 1.00, 1.50),
    '4h': ('4h', 2.00, 1.50),
}
HALTED_SYMBOLS = {
    'kronos-mini':    {'ETHUSD'},
    'kronos-base':    {'BTCUSD', 'XRPUSD'},
    'kronos-mini-4h': {'BNBUSD'},
    'kronos-base-4h': {'ETHUSD', 'XRPUSD'},
}

SYMBOLS = ['BTCUSD', 'BNBUSD', 'ETHUSD', 'XRPUSD']
MODELS  = ['kronos-mini', 'kronos-base', 'kronos-mini-4h', 'kronos-base-4h']

# ── DB queries ────────────────────────────────────────────────────────────────
stats = {}
for model in MODELS + ['custom']:
    for sym in SYMBOLS:
        row = conn.execute("""
            SELECT
                COUNT(*) as n_closed,
                SUM(t.pnl_net) as pnl,
                SUM(t.fees)    as fees,
                SUM(CASE WHEN t.exit_reason='stop_loss'   THEN 1 ELSE 0 END) as n_sl,
                SUM(CASE WHEN t.exit_reason='take_profit' THEN 1 ELSE 0 END) as n_tp,
                SUM(CASE WHEN t.exit_reason='horizon_exit' THEN 1 ELSE 0 END) as n_hor,
                SUM(CASE WHEN s.direction='long'  THEN 1 ELSE 0 END) as n_long,
                SUM(CASE WHEN s.direction='short' THEN 1 ELSE 0 END) as n_short,
                SUM(CASE WHEN ((s.direction='long'  AND s.actual_return_pct >  0.15)
                            OR (s.direction='short' AND s.actual_return_pct < -0.15))
                         THEN 1 ELSE 0 END) as n_wins,
                SUM(CASE WHEN t.pnl_net > 0 THEN t.pnl_net ELSE 0 END) as gross_win,
                SUM(CASE WHEN t.pnl_net < 0 THEN t.pnl_net ELSE 0 END) as gross_loss
            FROM signals s JOIN trades t ON t.signal_id = s.id
            WHERE s.model_source=? AND s.symbol=?
              AND s.quality_flag IS NULL AND t.exit_timestamp IS NOT NULL
        """, (model, sym)).fetchone()
        stats[(model, sym)] = dict(row)

# Open positions
open_pos = conn.execute("""
    SELECT s.model_source, s.symbol, s.direction, t.entry_price, t.entry_timestamp
    FROM signals s JOIN trades t ON t.signal_id = s.id
    WHERE s.quality_flag IS NULL AND t.exit_timestamp IS NULL
    ORDER BY s.model_source, s.symbol
""").fetchall()

conn.close()

def get_config(model, sym):
    key = (model, sym)
    if key in SYMBOL_CONFIG:
        return SYMBOL_CONFIG[key]
    tf = PROCESS_STATUS.get(model, ('?','?','1h','?'))[2].lower()
    return DEFAULT_CONFIG.get(tf, ('?', 2.0, 1.5))

def signal_status(model, sym):
    proc = PROCESS_STATUS.get(model, ('?', 'HALTED', '?', '?'))[1]
    if proc == 'HALTED':
        return 'PROC-HALT'
    if sym in HALTED_SYMBOLS.get(model, set()):
        return 'SIG-HALT'
    return 'ACTIVE'

def fmt_pnl(v):
    if v is None or v == 0: return '      —'
    return f'{v:>+7,.0f}'

def bar(v, total, width=8):
    if total == 0: return '.' * width
    filled = round(abs(v) / total * width)
    return ('█' * filled).ljust(width)

# ── SECTION 1: Master model × asset matrix ───────────────────────────────────
print()
print("━"*120)
print("  KRONOS  —  MODEL × ASSET MASTER TABLE")
print("━"*120)
header = f"  {'Model':18}  {'Mod':4}  {'Proc':9}  {'Asset':7}  {'Status':10}  {'TP':5}  {'SL':5}  " \
         f"{'Trades':7}  {'L/S':7}  {'WR%':5}  {'PnL(Rs)':9}  {'Fees':7}  {'EV/tr':7}  {'SL/TP/H':8}  {'R:R':5}"
print(header)
print("  " + "-"*118)

for model in MODELS:
    mod_id, proc, tf, horizon = PROCESS_STATUS[model]
    first = True
    for sym in SYMBOLS:
        s = stats[(model, sym)]
        n = s['n_closed']
        status = signal_status(model, sym)
        cfg_tf, tp_m, sl_m = get_config(model, sym)

        if n == 0:
            pnl_str = '      —'
            wr_str  = '  —'
            ls_str  = '  — / —'
            ev_str  = '      —'
            exit_str= '—/—/—'
            rr_str  = '  —'
        else:
            pnl  = s['pnl']   or 0
            fees = s['fees']  or 0
            wins = s['n_wins'] or 0
            wr   = wins / n * 100
            nl   = s['n_long']  or 0
            ns   = s['n_short'] or 0
            ev   = pnl / n
            gw   = s['gross_win']  or 0
            gl   = s['gross_loss'] or 0
            avg_w = gw / wins          if wins       else 0
            avg_l = gl / (n - wins)    if (n-wins)   else 0
            rr_v  = abs(avg_w / avg_l) if avg_l      else 0
            pnl_str  = fmt_pnl(pnl)
            wr_str   = f'{wr:>4.0f}%'
            ls_str   = f'{nl:>2}L/{ns:>2}S'
            ev_str   = f'{ev:>+7.0f}'
            exit_str = f"{s['n_sl']}/{s['n_tp']}/{s['n_hor']}"
            rr_str   = f'{rr_v:>4.2f}' if rr_v else '   —'

        status_icon = '✓' if status == 'ACTIVE' else ('●' if status == 'SIG-HALT' else '✗')
        model_label = model if first else ''
        mod_label   = mod_id if first else ''
        proc_label  = proc   if first else ''
        first = False

        print(f"  {model_label:18}  {mod_label:4}  {proc_label:9}  {sym:7}  "
              f"{status_icon} {status:9}  {tp_m:>4.2f}x  {sl_m:>4.2f}x  "
              f"{n:>5}tr  {ls_str:7}  {wr_str:5}  {pnl_str}  "
              f"{fmt_pnl(s['fees'] if n else None):>7}  {ev_str}  "
              f"{exit_str:8}  {rr_str}")
    print("  " + "-"*118)

# ── SECTION 2: Per-asset aggregation across ALL models ───────────────────────
print()
print("━"*90)
print("  ASSET-LEVEL AGGREGATION  (all models, closed trades)")
print("━"*90)
print(f"  {'Asset':8}  {'Models':6}  {'Trades':7}  {'Wins':6}  {'WR%':5}  "
      f"{'TotalPnL':10}  {'TotalFees':10}  {'EV/tr':7}  {'Best model':20}  {'Worst model':20}")
print("  " + "-"*88)

for sym in SYMBOLS:
    sym_trades = sym_pnl = sym_fees = sym_wins = 0
    model_pnls = {}
    for model in MODELS + ['custom']:
        s = stats[(model, sym)]
        n = s['n_closed']
        if n:
            sym_trades += n
            sym_pnl    += (s['pnl']    or 0)
            sym_fees   += (s['fees']   or 0)
            sym_wins   += (s['n_wins'] or 0)
            model_pnls[model] = (s['pnl'] or 0, n)
    if not sym_trades:
        continue
    wr = sym_wins / sym_trades * 100
    ev = sym_pnl / sym_trades
    n_models = len(model_pnls)
    best  = max(model_pnls, key=lambda m: model_pnls[m][0]) if model_pnls else '—'
    worst = min(model_pnls, key=lambda m: model_pnls[m][0]) if model_pnls else '—'
    best_pnl  = model_pnls[best][0]  if model_pnls else 0
    worst_pnl = model_pnls[worst][0] if model_pnls else 0
    print(f"  {sym:8}  {n_models:>5}m  {sym_trades:>5}tr  {sym_wins:>5}W  "
          f"{wr:>4.0f}%  {sym_pnl:>+10,.0f}  {sym_fees:>10,.0f}  {ev:>+7.0f}  "
          f"{best:20} ({best_pnl:+,.0f})  {worst:20} ({worst_pnl:+,.0f})")

# ── SECTION 3: Active model overlap per asset ─────────────────────────────────
print()
print("━"*80)
print("  ACTIVE SIGNAL COVERAGE  (which active models fire on each asset)")
print("  [ACTIVE=generates trades now | SIG-HALT=process runs but asset blocked | PROC-HALT=process off]")
print("━"*80)
print(f"  {'Asset':8}", end='')
for model in MODELS:
    print(f"  {model:18}", end='')
print(f"  {'Overlap':8}  {'Risk note':30}")
print("  " + "-"*78)

for sym in SYMBOLS:
    active_count = 0
    print(f"  {sym:8}", end='')
    for model in MODELS:
        st = signal_status(model, sym)
        s  = stats[(model, sym)]
        n  = s['n_closed']
        icon = {'ACTIVE': '✓ ACTIVE  ', 'SIG-HALT': '● SIG-HALT', 'PROC-HALT': '✗ P-HALT  '}[st]
        print(f"  {icon:18}", end='')
        if st == 'ACTIVE':
            active_count += 1
    overlap = f'{active_count} active'
    note = ''
    if active_count > 1:
        note = 'OVERLAP — multiple models trading'
    elif active_count == 0:
        note = 'NO COVERAGE — all halted'
    print(f"  {overlap:8}  {note}")

# ── SECTION 4: Direction bias per model ──────────────────────────────────────
print()
print("━"*80)
print("  DIRECTION BIAS  (long vs short across all closed trades per model)")
print("━"*80)
print(f"  {'Model':18}  {'Mod':4}  {'Proc':9}  {'Long':6}  {'Short':6}  {'Bias':22}  "
      f"{'Long WR':8}  {'Short WR':9}  {'Long PnL':10}  {'Short PnL':10}")
print("  " + "-"*78)

for model in MODELS:
    mod_id, proc = PROCESS_STATUS[model][:2]
    nl = ns = lw = sw = lpnl = spnl = 0
    for sym in SYMBOLS:
        s = stats[(model, sym)]
        nl   += s['n_long']  or 0
        ns   += s['n_short'] or 0
        lpnl += sum((stats[(model, sym)]['pnl'] or 0) * ((stats[(model, sym)]['n_long'] or 0) /
                     max(stats[(model, sym)]['n_closed'], 1)) for sym in SYMBOLS
                    if stats[(model, sym)]['n_closed'] > 0)
    # recalc cleanly
    nl = ns = 0
    for sym in SYMBOLS:
        s = stats[(model, sym)]
        nl   += s['n_long']  or 0
        ns   += s['n_short'] or 0

    # Direction WR from DB
    lw_count = lw_wins = sw_count = sw_wins = 0
    for sym in SYMBOLS:
        s = stats[(model, sym)]
        lw_count += s['n_long']  or 0
        sw_count += s['n_short'] or 0
    # wins per direction need separate query - use available data
    total = nl + ns
    bias_bar = '█' * round(nl/total*20) + '░' * round(ns/total*20) if total else '—'
    bias_label = f'{nl}L / {ns}S  ({nl/total*100:.0f}% long)' if total else '—'
    print(f"  {model:18}  {mod_id:4}  {proc:9}  {nl:>5}  {ns:>5}  "
          f"  {bias_label:22}")

# ── SECTION 5: Fee analysis ───────────────────────────────────────────────────
print()
print("━"*80)
print("  FEE DRAG ANALYSIS  (fees as % of gross wins)")
print("━"*80)
print(f"  {'Model':18}  {'Asset':7}  {'Status':10}  {'NetPnL':9}  {'Fees':8}  "
      f"{'GrossWin':9}  {'Fee/GrossWin':13}  {'Fee/trade':10}")
print("  " + "-"*78)

for model in MODELS:
    for sym in SYMBOLS:
        s = stats[(model, sym)]
        n = s['n_closed']
        if n == 0: continue
        status = signal_status(model, sym)
        pnl    = s['pnl']       or 0
        fees   = s['fees']      or 0
        gw     = s['gross_win'] or 0
        fee_pct = fees / gw * 100 if gw > 0 else 0
        fee_pt  = fees / n
        flag = '  <<< HIGH' if fee_pct > 50 or fee_pt > 150 else ''
        print(f"  {model:18}  {sym:7}  {status:10}  {pnl:>+9,.0f}  {fees:>8,.0f}  "
              f"{gw:>9,.0f}  {fee_pct:>11.1f}%  {fee_pt:>9.1f}{flag}")

# ── SECTION 6: Open positions right now ──────────────────────────────────────
print()
print("━"*80)
print("  OPEN POSITIONS  (live / unrealised)")
print("━"*80)
if open_pos:
    print(f"  {'Model':18}  {'Symbol':8}  {'Dir':6}  {'Entry':10}  {'Open since':14}")
    print("  " + "-"*60)
    import time
    now = int(time.time())
    for p in open_pos:
        hrs = (now - int(p['entry_timestamp'])) / 3600
        print(f"  {p['model_source']:18}  {p['symbol']:8}  {p['direction']:6}  "
              f"{float(p['entry_price']):>10.4f}  {hrs:>6.1f}h ago")
else:
    print("  No open positions.")

# ── SECTION 7: Model health scorecard ────────────────────────────────────────
print()
print("━"*90)
print("  MODEL HEALTH SCORECARD  (active assets only, closed trades)")
print("━"*90)
print(f"  {'Model':18}  {'Mod':4}  {'Proc':9}  {'ActiveAssets':13}  {'TotalTrades':11}  "
      f"{'OverallWR':9}  {'TotalPnL':10}  {'TotalFees':10}  {'EV/trade':9}  {'Health':8}")
print("  " + "-"*88)

for model in MODELS:
    mod_id, proc = PROCESS_STATUS[model][:2]
    tot_n = tot_wins = tot_pnl = tot_fees = 0
    active_assets = []
    for sym in SYMBOLS:
        s = stats[(model, sym)]
        n = s['n_closed']
        if n == 0: continue
        status = signal_status(model, sym)
        tot_n    += n
        tot_wins += s['n_wins'] or 0
        tot_pnl  += s['pnl']   or 0
        tot_fees += s['fees']  or 0
        if status == 'ACTIVE':
            active_assets.append(sym)

    if tot_n == 0:
        print(f"  {model:18}  {mod_id:4}  {proc:9}  {'no trades':13}")
        continue

    wr  = tot_wins / tot_n * 100
    ev  = tot_pnl  / tot_n
    astr = '+'.join([s[:3] for s in active_assets]) if active_assets else 'none'

    if proc == 'HALTED':
        health = 'OFFLINE'
    elif tot_pnl > 0 and wr >= 40:
        health = 'GOOD'
    elif tot_pnl > 0:
        health = 'MARGINAL'
    elif ev > -50:
        health = 'WATCH'
    else:
        health = 'POOR'

    print(f"  {model:18}  {mod_id:4}  {proc:9}  {astr:13}  {tot_n:>9}tr  "
          f"{wr:>7.0f}%  {tot_pnl:>+10,.0f}  {tot_fees:>10,.0f}  {ev:>+9.0f}  {health}")

print()
print("━"*90)
print(f"  Legend:  ✓ ACTIVE = generating trades   ● SIG-HALT = asset blocked in execution")
print(f"           ✗ PROC-HALT = module process disabled   SL/TP/H = stop-loss/take-profit/horizon exits")
print("━"*90)

conn.close()
