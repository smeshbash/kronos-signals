"""
Regime Analysis — Three Questions
1. Do any models have positive expectancy in any regime?
2. Is the long bias consistent across all models or concentrated?
3. Does directional accuracy improve when daily EMA is pointing up?
"""
import sys
import datetime
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
from db import get_connection, BENCHMARK_MODEL_SOURCE

SEP  = '=' * 72
SEP2 = '-' * 72

def compute_ema(prices, period):
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema

def dir_acc_str(correct, total):
    if total == 0: return '--'
    return f'{correct/total*100:.1f}%'

print(SEP)
print('  REGIME ANALYSIS — Three Questions')
print(f'  {datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")}')
print(SEP)

# ── Load all data in one connection block ──────────────────────────────────────
with get_connection() as conn:

    # Available timeframes
    tfs = [r['timeframe'] for r in conn.execute(
        "SELECT DISTINCT timeframe FROM ohlcv ORDER BY timeframe"
    ).fetchall()]
    print(f'\n  Available OHLCV timeframes: {tfs}')

    daily_count = conn.execute(
        "SELECT COUNT(*) as n FROM ohlcv WHERE timeframe='1d'"
    ).fetchone()['n']

    tf_regime  = '1d' if daily_count >= 50 else '4h'
    ema_period = 20   if tf_regime == '1d' else 50
    print(f'  Regime timeframe: {tf_regime}  EMA period: {ema_period}  '
          f'(daily candles in DB: {daily_count})')

    # Closed trades with model info
    trades = conn.execute('''
        SELECT t.id, t.symbol, t.direction, t.exit_reason,
               t.pnl_gross, t.pnl_net, t.entry_price, t.exit_price,
               t.created_at,
               s.model_source, s.confidence, s.actual_return_pct,
               s.signal_timestamp, s.horizon
        FROM trades t
        JOIN signals s ON t.signal_id = s.id
        WHERE t.status = 'closed'
          AND (t.quality_flag IS NULL OR t.quality_flag = '')
          AND s.model_source IS NOT NULL
        ORDER BY s.model_source, t.created_at
    ''').fetchall()

    # All signals (for Q2 long-bias)
    all_sigs = conn.execute('''
        SELECT id, model_source, symbol, direction, confidence,
               actual_return_pct, signal_timestamp, status
        FROM signals
        WHERE (quality_flag IS NULL OR quality_flag = '')
          AND model_source IS NOT NULL
        ORDER BY model_source, signal_timestamp
    ''').fetchall()

    # Resolved signals (actual_return_pct not null) — for Q3
    resolved_sigs = [s for s in all_sigs if s['actual_return_pct'] is not None]

    # OHLCV for regime — all symbols
    symbols = list(set(r['symbol'] for r in resolved_sigs))
    ohlcv_by_sym = {}
    for sym in symbols:
        rows = conn.execute('''
            SELECT timestamp, close FROM ohlcv
            WHERE symbol=? AND timeframe=?
            ORDER BY timestamp ASC
        ''', (sym, tf_regime)).fetchall()
        ohlcv_by_sym[sym] = [(r['timestamp'], float(r['close'])) for r in rows]

    # Also need OHLCV for trade entries (for Q1 regime-split expectancy)
    # Use signal_timestamp to determine regime at entry
    trade_sym_ts = list(set((t['symbol'], t['signal_timestamp']) for t in trades))

# ── Regime helper ──────────────────────────────────────────────────────────────
def get_regime(symbol, signal_ts):
    candles = ohlcv_by_sym.get(symbol, [])
    relevant = [c for c in candles if c[0] <= signal_ts]
    if len(relevant) < ema_period + 5:
        return 'unknown'
    closes = [c[1] for c in relevant[-300:]]
    ema = compute_ema(closes, ema_period)
    if ema is None:
        return 'unknown'
    return 'bull' if closes[-1] > ema else 'bear'

# Attach regime to each trade and resolved signal
trades_with_regime = []
for t in trades:
    regime = get_regime(t['symbol'], t['signal_timestamp'])
    trades_with_regime.append(dict(t) | {'regime': regime})

resolved_with_regime = []
for s in resolved_sigs:
    regime = get_regime(s['symbol'], s['signal_timestamp'])
    resolved_with_regime.append(dict(s) | {'regime': regime})

models = sorted(set(t['model_source'] for t in trades_with_regime))

# ══════════════════════════════════════════════════════════════════════════════
# Q1: EXPECTANCY BY MODEL — overall + regime split
# ══════════════════════════════════════════════════════════════════════════════
print(f'\n{SEP}')
print('  Q1: EXPECTANCY BY MODEL')
print(f'  Expectancy = win_rate × avg_win + loss_rate × avg_loss  (per trade, pnl_net Rs)')
print(f'  Δ columns = delta vs benchmark ({BENCHMARK_MODEL_SOURCE})')
print(SEP)

def expectancy_stats(trade_list):
    if not trade_list: return None
    n     = len(trade_list)
    wins  = [t for t in trade_list if (t['pnl_net'] or 0) > 0]
    loss  = [t for t in trade_list if (t['pnl_net'] or 0) <= 0]
    wr    = len(wins) / n
    avg_w = sum(t['pnl_net'] for t in wins)  / len(wins)  if wins else 0
    avg_l = sum(t['pnl_net'] for t in loss)  / len(loss)  if loss else 0
    exp   = wr * avg_w + (1 - wr) * avg_l
    total = sum(t['pnl_net'] or 0 for t in trade_list)
    return dict(n=n, wr=wr, avg_w=avg_w, avg_l=avg_l, exp=exp, total=total)

# Compute benchmark stats first so deltas can be shown inline
bm_trades_q1 = [t for t in trades_with_regime if t['model_source'] == BENCHMARK_MODEL_SOURCE]
bm_s         = expectancy_stats(bm_trades_q1)

print(f'\n  {"Model":<22} {"n":>4} {"WR":>6} {"AvgWin":>8} {"AvgLoss":>8} '
      f'{"Expect/trade":>13} {"Δ Expect":>10} {"Total Net":>10}')
print(f'  {SEP2}')
for model in models:
    mt = [t for t in trades_with_regime if t['model_source'] == model]
    s  = expectancy_stats(mt)
    if not s: continue
    is_bm   = (model == BENCHMARK_MODEL_SOURCE)
    flag    = '  ★ BENCHMARK' if is_bm else ('  ✓ POSITIVE' if s['exp'] > 0 else '')
    d_exp   = '' if is_bm or not bm_s else f'{s["exp"]-bm_s["exp"]:+.0f}'
    print(f'  {model:<22} {s["n"]:>4} {s["wr"]*100:>5.0f}% '
          f'{s["avg_w"]:>8.0f} {s["avg_l"]:>8.0f} '
          f'{s["exp"]:>13.0f} {d_exp:>10} {s["total"]:>10.0f}{flag}')

# Regime split per model
print(f'\n  REGIME SPLIT — Expectancy in BULL vs BEAR at entry:')
print(f'  {"Model":<22} {"Regime":<7} {"n":>4} {"WR":>6} {"AvgWin":>8} '
      f'{"AvgLoss":>8} {"Expect":>8} {"Total":>10}')
print(f'  {SEP2}')
for model in models:
    mt = [t for t in trades_with_regime if t['model_source'] == model]
    for regime in ['bull', 'bear', 'unknown']:
        rt = [t for t in mt if t['regime'] == regime]
        s  = expectancy_stats(rt)
        if not s or s['n'] < 3: continue
        flag = '  ✓' if s['exp'] > 0 else ''
        print(f'  {model:<22} {regime:<7} {s["n"]:>4} {s["wr"]*100:>5.0f}% '
              f'{s["avg_w"]:>8.0f} {s["avg_l"]:>8.0f} '
              f'{s["exp"]:>8.0f} {s["total"]:>10.0f}{flag}')
    print()

# ══════════════════════════════════════════════════════════════════════════════
# Q2: LONG BIAS — per model × symbol
# ══════════════════════════════════════════════════════════════════════════════
print(f'\n{SEP}')
print('  Q2: LONG BIAS — ALL SIGNALS (including unexecuted)')
print(SEP)

all_models = sorted(set(s['model_source'] for s in all_sigs))
for model in all_models:
    ms = [s for s in all_sigs if s['model_source'] == model]
    total_l = sum(1 for s in ms if s['direction'] == 'long')
    total_s = sum(1 for s in ms if s['direction'] == 'short')
    total   = total_l + total_s
    lp      = total_l / total * 100 if total > 0 else 0
    print(f'\n  {model.upper()} — {total} signals  long={total_l}({lp:.0f}%)  short={total_s}({100-lp:.0f}%)')
    print(f'  {"Symbol":<12} {"Long":>5} {"Short":>5} {"Total":>5} {"Long%":>6}  '
          f'{"Exec_L":>6} {"Exec_S":>6}')
    print(f'  {"-"*55}')
    syms = sorted(set(s['symbol'] for s in ms))
    for sym in syms:
        ss  = [s for s in ms if s['symbol'] == sym]
        l   = sum(1 for s in ss if s['direction'] == 'long')
        sh  = sum(1 for s in ss if s['direction'] == 'short')
        t   = l + sh
        lp2 = l / t * 100 if t > 0 else 0
        # executed only
        exec_l = sum(1 for s in ss if s['direction'] == 'long'
                     and s['status'] in ('executed', 'approved'))
        exec_s = sum(1 for s in ss if s['direction'] == 'short'
                     and s['status'] in ('executed', 'approved'))
        print(f'  {sym:<12} {l:>5} {sh:>5} {t:>5} {lp2:>5.0f}%  {exec_l:>6} {exec_s:>6}')

# ══════════════════════════════════════════════════════════════════════════════
# Q3: DIRECTIONAL ACCURACY vs REGIME at signal time
# ══════════════════════════════════════════════════════════════════════════════
print(f'\n{SEP}')
print(f'  Q3: DIRECTIONAL ACCURACY BY REGIME  (regime={tf_regime} EMA{ema_period})')
print(f'  n={len(resolved_with_regime)} resolved signals with actual_return_pct')
print(SEP)

def da_stats(sig_list):
    if not sig_list: return None
    n = len(sig_list)
    correct = sum(
        1 for s in sig_list
        if (s['direction'] == 'long'  and s['actual_return_pct'] > 0) or
           (s['direction'] == 'short' and s['actual_return_pct'] < 0)
    )
    longs  = [s for s in sig_list if s['direction'] == 'long']
    shorts = [s for s in sig_list if s['direction'] == 'short']
    l_correct = sum(1 for s in longs
                    if s['actual_return_pct'] > 0)
    s_correct = sum(1 for s in shorts
                    if s['actual_return_pct'] < 0)
    return dict(
        n=n, correct=correct, da=correct/n,
        n_long=len(longs),   l_da=l_correct/len(longs)   if longs  else None,
        n_short=len(shorts), s_da=s_correct/len(shorts)  if shorts else None,
    )

# Overall
print(f'\n  OVERALL across all models:')
print(f'  {"Regime":<8} {"n":>5} {"Dir_acc":>8} {"n_long":>7} {"Long_acc":>9} {"n_short":>7} {"Short_acc":>10}')
print(f'  {"-"*60}')
for regime in ['bull', 'bear', 'unknown']:
    rs = [s for s in resolved_with_regime if s['regime'] == regime]
    s  = da_stats(rs)
    if not s or s['n'] < 3: continue
    l_da  = f'{s["l_da"]*100:.1f}%' if s['l_da']  is not None else '--'
    sh_da = f'{s["s_da"]*100:.1f}%' if s['s_da'] is not None else '--'
    print(f'  {regime:<8} {s["n"]:>5} {s["da"]*100:>7.1f}% '
          f'{s["n_long"]:>7} {l_da:>9} '
          f'{s["n_short"]:>7} {sh_da:>10}')

# Per model
print(f'\n  BY MODEL:')
print(f'  {"Model":<22} {"Regime":<8} {"n":>4} {"DA":>6} {"nL":>4} {"L_acc":>7} {"nS":>4} {"S_acc":>7}')
print(f'  {SEP2}')
for model in sorted(set(s['model_source'] for s in resolved_with_regime)):
    ms = [s for s in resolved_with_regime if s['model_source'] == model]
    for regime in ['bull', 'bear', 'unknown']:
        rs = [s for s in ms if s['regime'] == regime]
        st = da_stats(rs)
        if not st or st['n'] < 3: continue
        l_da  = f'{st["l_da"]*100:.1f}%'  if st['l_da']  is not None else ' --'
        sh_da = f'{st["s_da"]*100:.1f}%'  if st['s_da'] is not None else ' --'
        print(f'  {model:<22} {regime:<8} {st["n"]:>4} {st["da"]*100:>5.1f}% '
              f'{st["n_long"]:>4} {l_da:>7} {st["n_short"]:>4} {sh_da:>7}')
    print()

# Summary: does bull regime → better dir_acc for longs?
print(f'\n  SUMMARY: Does bull regime improve long accuracy?')
print(f'  {"Model":<22} {"Bear_Long_acc":>14} {"Bull_Long_acc":>14} {"Delta":>8}')
print(f'  {"-"*65}')
for model in sorted(set(s['model_source'] for s in resolved_with_regime)):
    ms = [s for s in resolved_with_regime if s['model_source'] == model]
    bear_longs = [s for s in ms if s['regime'] == 'bear' and s['direction'] == 'long']
    bull_longs = [s for s in ms if s['regime'] == 'bull' and s['direction'] == 'long']
    def long_da(lst):
        if not lst: return None
        return sum(1 for s in lst if s['actual_return_pct'] > 0) / len(lst)
    bld = long_da(bear_longs)
    bud = long_da(bull_longs)
    bl_str = f'{bld*100:.1f}% (n={len(bear_longs)})' if bld is not None else f'-- (n={len(bear_longs)})'
    bu_str = f'{bud*100:.1f}% (n={len(bull_longs)})' if bud is not None else f'-- (n={len(bull_longs)})'
    if bld is not None and bud is not None:
        delta = f'{(bud-bld)*100:+.1f}pp'
    else:
        delta = '--'
    print(f'  {model:<22} {bl_str:>14} {bu_str:>14} {delta:>8}')

# ══════════════════════════════════════════════════════════════════════════════
# BENCHMARK SCORECARD — all models ranked against kronos-base-4h
# ══════════════════════════════════════════════════════════════════════════════
print(f'\n{SEP}')
print(f'  BENCHMARK SCORECARD — ranked vs {BENCHMARK_MODEL_SOURCE}')
print(f'  Positive delta = model beats benchmark on that metric')
print(SEP)

all_model_names = sorted(set(t['model_source'] for t in trades_with_regime))
bm_all = expectancy_stats([t for t in trades_with_regime
                            if t['model_source'] == BENCHMARK_MODEL_SOURCE])

# DA for benchmark
bm_res  = [s for s in resolved_with_regime if s['model_source'] == BENCHMARK_MODEL_SOURCE]
bm_da_r = None
if bm_res:
    bm_correct = sum(
        1 for s in bm_res
        if (s['direction'] == 'long'  and s['actual_return_pct'] > 0) or
           (s['direction'] == 'short' and s['actual_return_pct'] < 0)
    )
    bm_da_r = bm_correct / len(bm_res) * 100

# Bear regime DA for benchmark
bm_bear = [s for s in bm_res if s['regime'] == 'bear']
bm_bear_da = None
if bm_bear:
    bm_bear_correct = sum(
        1 for s in bm_bear
        if (s['direction'] == 'long'  and s['actual_return_pct'] > 0) or
           (s['direction'] == 'short' and s['actual_return_pct'] < 0)
    )
    bm_bear_da = bm_bear_correct / len(bm_bear) * 100

print(f'\n  {"Model":<22} {"Expect/t":>10} {"Δ Exp":>8} {"WR":>6} {"ΔWR":>6} '
      f'{"DA":>6} {"ΔDA":>7} {"Bear DA":>8} {"ΔBearDA":>8} {"Verdict":>10}')
print(f'  {SEP2}')

for model in all_model_names:
    mt  = [t for t in trades_with_regime if t['model_source'] == model]
    s   = expectancy_stats(mt)
    if not s: continue

    # Overall dir accuracy
    m_res = [r for r in resolved_with_regime if r['model_source'] == model]
    if m_res:
        m_correct = sum(
            1 for r in m_res
            if (r['direction'] == 'long'  and r['actual_return_pct'] > 0) or
               (r['direction'] == 'short' and r['actual_return_pct'] < 0)
        )
        m_da = m_correct / len(m_res) * 100
    else:
        m_da = None

    # Bear regime dir accuracy
    m_bear = [r for r in m_res if r['regime'] == 'bear']
    if m_bear:
        m_bear_correct = sum(
            1 for r in m_bear
            if (r['direction'] == 'long'  and r['actual_return_pct'] > 0) or
               (r['direction'] == 'short' and r['actual_return_pct'] < 0)
        )
        m_bear_da = m_bear_correct / len(m_bear) * 100
    else:
        m_bear_da = None

    is_bm    = (model == BENCHMARK_MODEL_SOURCE)
    d_exp    = '--' if is_bm or not bm_all else f'{s["exp"]-bm_all["exp"]:+.0f}'
    d_wr     = '--' if is_bm or not bm_all else f'{(s["wr"]-bm_all["wr"])*100:+.0f}pp'
    d_da     = '--' if is_bm or bm_da_r is None or m_da is None else f'{m_da-bm_da_r:+.1f}pp'
    d_bda    = '--' if is_bm or bm_bear_da is None or m_bear_da is None else f'{m_bear_da-bm_bear_da:+.1f}pp'
    da_str   = f'{m_da:.1f}%'      if m_da      is not None else '--'
    bda_str  = f'{m_bear_da:.1f}%' if m_bear_da is not None else '--'

    if is_bm:
        verdict = '★ BENCHMARK'
    elif bm_all and s['exp'] > bm_all['exp']:
        verdict = 'AHEAD'
    elif bm_all and s['exp'] > bm_all['exp'] - 100:
        verdict = 'CLOSE'
    else:
        verdict = 'BEHIND'

    print(f'  {model:<22} {s["exp"]:>10.0f} {d_exp:>8} '
          f'{s["wr"]*100:>5.0f}% {d_wr:>6} '
          f'{da_str:>6} {d_da:>7} '
          f'{bda_str:>8} {d_bda:>8} '
          f'{verdict:>10}')

print(f'\n{SEP}')
print('  END')
print(SEP)
