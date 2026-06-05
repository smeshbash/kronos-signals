"""
Deep benchmark analysis — runs against the live database and prints a full report.
"""
import sys, time, datetime
sys.stdout.reconfigure(encoding='utf-8')
from db import get_connection, BENCHMARK_MODEL_SOURCE

def ts(epoch):
    if not epoch: return '--'
    return datetime.datetime.fromtimestamp(int(epoch), datetime.UTC).strftime('%Y-%m-%d %H:%M')

def pct(v):
    return f'{v:+.2f}%'

def rs(v):
    return f'Rs{v:+.2f}'

SEP  = '=' * 70
SEP2 = '-' * 70

with get_connection() as conn:

    # ── 0. RAW COUNTS ─────────────────────────────────────────────────────────
    all_trades = conn.execute('''
        SELECT t.*, s.confidence, s.predicted_return_pct, s.actual_return_pct,
               s.model_source, s.regime_version, s.horizon, s.direction as sig_direction
        FROM trades t
        LEFT JOIN signals s ON t.signal_id = s.id
        WHERE t.status = "closed"
        ORDER BY t.entry_timestamp ASC
    ''').fetchall()

    clean  = [t for t in all_trades if not t['quality_flag']]
    flagged = [t for t in all_trades if t['quality_flag']]

    print(SEP)
    print('  KRONOS — DEEP ANALYSIS REPORT')
    print(f'  Generated: {datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")}')
    print(SEP)
    print(f'\n  Total closed trades : {len(all_trades)}')
    print(f'  Clean (analysed)    : {len(clean)}')
    print(f'  Flagged (excluded)  : {len(flagged)}')

    # ── 1. OVERALL SUMMARY ────────────────────────────────────────────────────
    print(f'\n{SEP}')
    print('  1. OVERALL P&L SUMMARY (clean trades)')
    print(SEP)
    wins   = [t for t in clean if (t['pnl_gross'] or 0) > 0]
    losses = [t for t in clean if (t['pnl_gross'] or 0) <= 0]
    gross  = sum(t['pnl_gross'] or 0 for t in clean)
    net    = sum(t['pnl_net']   or 0 for t in clean)
    fees   = sum(t['fees']      or 0 for t in clean)
    tds    = sum(t['tds_deducted'] or 0 for t in clean)
    fund   = sum((t['funding_paid'] or 0) - (t['funding_received'] or 0) for t in clean)
    avg_w  = sum(t['pnl_gross'] or 0 for t in wins)   / len(wins)   if wins   else 0
    avg_l  = sum(t['pnl_gross'] or 0 for t in losses) / len(losses) if losses else 0
    rr     = abs(avg_w / avg_l) if avg_l else float('inf')
    wr     = len(wins) / len(clean) * 100 if clean else 0

    print(f'  Trades     : {len(clean)}  ({len(wins)}W / {len(losses)}L)')
    print(f'  Win rate   : {wr:.1f}%')
    print(f'  Avg win    : {rs(avg_w)}')
    print(f'  Avg loss   : {rs(avg_l)}')
    print(f'  Reward/Risk: {rr:.2f}x')
    print(f'  Gross P&L  : {rs(gross)}')
    print(f'  Fees drag  : Rs{-fees:.2f}')
    print(f'  Funding    : Rs{-fund:.2f}  (paid - received)')
    print(f'  TDS        : Rs{-tds:.2f}')
    print(f'  Net P&L    : {rs(net)}')
    print(f'  Cost ratio : {abs(gross-net)/abs(gross)*100:.1f}% of gross eaten by costs' if gross else '')

    # ── 2. PER-MODEL ──────────────────────────────────────────────────────────
    print(f'\n{SEP}')
    print('  2. PER-MODEL BREAKDOWN')
    print(SEP)
    models = {}
    for t in clean:
        m = t['model_source'] or 'custom'
        models.setdefault(m, []).append(t)

    for m, ts_list in sorted(models.items()):
        n       = len(ts_list)
        w_list  = [t for t in ts_list if (t['pnl_gross'] or 0) > 0]
        l_list  = [t for t in ts_list if (t['pnl_gross'] or 0) <= 0]
        gross_m = sum(t['pnl_gross'] or 0 for t in ts_list)
        net_m   = sum(t['pnl_net']   or 0 for t in ts_list)
        avg_wm  = sum(t['pnl_gross'] or 0 for t in w_list) / len(w_list) if w_list else 0
        avg_lm  = sum(t['pnl_gross'] or 0 for t in l_list) / len(l_list) if l_list else 0
        rr_m    = abs(avg_wm / avg_lm) if avg_lm else float('inf')
        wr_m    = len(w_list) / n * 100 if n else 0
        holds   = [(t['exit_timestamp'] or 0) - (t['entry_timestamp'] or 0)
                   for t in ts_list if t['entry_timestamp'] and t['exit_timestamp']]
        avg_h   = sum(holds) / len(holds) / 3600 if holds else 0

        print(f'\n  [{m}]')
        print(f'    Trades     : {n}  ({len(w_list)}W / {len(l_list)}L)  WR={wr_m:.1f}%')
        print(f'    Gross/Net  : {rs(gross_m)} / {rs(net_m)}')
        print(f'    Avg win    : {rs(avg_wm)}  |  Avg loss: {rs(avg_lm)}  |  RR: {rr_m:.2f}x')
        print(f'    Avg hold   : {avg_h:.1f}h')

        # Direction split
        longs  = [t for t in ts_list if t['direction'] == 'long']
        shorts = [t for t in ts_list if t['direction'] == 'short']
        def dir_stats(lst, label):
            if not lst: return
            w = sum(1 for t in lst if (t['pnl_gross'] or 0) > 0)
            g = sum(t['pnl_gross'] or 0 for t in lst)
            print(f'    {label:5s}      : n={len(lst)} WR={w/len(lst)*100:.0f}% Gross={rs(g)}')
        dir_stats(longs,  'Long')
        dir_stats(shorts, 'Short')

        # Symbol split
        syms_m = {}
        for t in ts_list:
            syms_m.setdefault(t['symbol'], []).append(t)
        sym_parts = []
        for sym, sl in sorted(syms_m.items()):
            w  = sum(1 for t in sl if (t['pnl_gross'] or 0) > 0)
            g  = sum(t['pnl_gross'] or 0 for t in sl)
            sym_parts.append(f'{sym.replace("USD","")}: {len(sl)}t {w/len(sl)*100:.0f}%WR {rs(g)}')
        print(f'    By symbol  : {" | ".join(sym_parts)}')

        # Confidence split
        with_c = [(t['confidence'], t['pnl_gross'] or 0) for t in ts_list if t['confidence'] is not None]
        if with_c:
            bands = [(0,0.2,'0.0-0.2'),(0.2,0.4,'0.2-0.4'),(0.4,0.6,'0.4-0.6'),
                     (0.6,0.8,'0.6-0.8'),(0.8,1.1,'0.8-1.0')]
            band_parts = []
            for lo, hi, lbl in bands:
                b = [(c,g) for c,g in with_c if lo <= c < hi]
                if not b: continue
                w_b = sum(1 for _,g in b if g > 0)
                g_b = sum(g for _,g in b)
                band_parts.append(f'{lbl}[n={len(b)} WR={w_b/len(b)*100:.0f}% {rs(g_b)}]')
            if band_parts:
                print(f'    Conf bands : {" ".join(band_parts)}')

    # ── 3. EXIT REASON ANALYSIS ───────────────────────────────────────────────
    print(f'\n{SEP}')
    print('  3. EXIT REASON ANALYSIS')
    print(SEP)
    exit_map = {}
    for t in clean:
        r = t['exit_reason'] or 'unknown'
        exit_map.setdefault(r, []).append(t)
    for r, lst in sorted(exit_map.items(), key=lambda x: -len(x[1])):
        n_r   = len(lst)
        wins_r = sum(1 for t in lst if (t['pnl_gross'] or 0) > 0)
        gross_r = sum(t['pnl_gross'] or 0 for t in lst)
        avg_r  = gross_r / n_r
        avg_h  = sum(((t['exit_timestamp'] or 0)-(t['entry_timestamp'] or 0))/3600
                     for t in lst if t['entry_timestamp'] and t['exit_timestamp']) / n_r
        print(f'\n  {r}  (n={n_r}, WR={wins_r/n_r*100:.0f}%, Gross={rs(gross_r)}, '
              f'AvgTrade={rs(avg_r)}, AvgHold={avg_h:.1f}h)')
        by_m = {}
        for t in lst:
            by_m.setdefault(t['model_source'] or 'custom', []).append(t['pnl_gross'] or 0)
        for mm, pnls in sorted(by_m.items()):
            ww = sum(1 for p in pnls if p > 0)
            print(f'    {mm}: n={len(pnls)} WR={ww/len(pnls)*100:.0f}% Gross={rs(sum(pnls))}')

    # ── 4. MAE / MFE (price excursion) ───────────────────────────────────────
    print(f'\n{SEP}')
    print('  4. MAX ADVERSE / FAVOURABLE EXCURSION  (peak & trough vs entry)')
    print('     MAE = worst drawdown during trade  |  MFE = best point reached')
    print(SEP)
    for m, ts_list in sorted(models.items()):
        maes, mfes = [], []
        for t in ts_list:
            ep = t['entry_price']
            pk = t['peak_price']
            tr = t['trough_price']
            if not ep: continue
            d = t['direction']
            mfe = ((pk-ep)/ep*100) if pk and d=='long'  else ((ep-tr)/ep*100) if tr and d=='short' else None
            mae = ((tr-ep)/ep*100) if tr and d=='long'  else ((ep-pk)/ep*100) if pk and d=='short' else None
            if mfe is not None: mfes.append(mfe)
            if mae is not None: maes.append(mae)
        if not mfes: continue
        # What % of trades had MFE > 1% but still lost?
        had_good_mfe_lost = sum(1 for i, t in enumerate(ts_list)
                                if i < len(mfes) and mfes[i] > 1.0 and (t['pnl_gross'] or 0) <= 0)
        print(f'\n  [{m}]  n={len(mfes)}')
        print(f'    MFE: avg={sum(mfes)/len(mfes):+.2f}%  max={max(mfes):+.2f}%  '
              f'min={min(mfes):+.2f}%')
        print(f'    MAE: avg={sum(maes)/len(maes):+.2f}%  worst={min(maes):+.2f}%  '
              f'best={max(maes):+.2f}%')
        # Classify: hit TP vs hit SL vs time exit
        sl_exits = [t for t in ts_list if (t['exit_reason'] or '') == 'stop_loss']
        tp_exits = [t for t in ts_list if (t['exit_reason'] or '') == 'take_profit']
        ho_exits = [t for t in ts_list if (t['exit_reason'] or '') not in ('stop_loss','take_profit')]
        print(f'    Exits: SL={len(sl_exits)} TP={len(tp_exits)} Other={len(ho_exits)}')

    # ── 5. PREDICTED vs ACTUAL RETURN ────────────────────────────────────────
    print(f'\n{SEP}')
    print('  5. PREDICTED vs ACTUAL RETURN  (all resolved signals, clean)')
    print(SEP)
    resolved = conn.execute('''
        SELECT model_source, direction, confidence,
               predicted_return_pct pred, actual_return_pct actual, status
        FROM signals
        WHERE actual_return_pct IS NOT NULL AND quality_flag IS NULL
        ORDER BY model_source
    ''').fetchall()
    print(f'  Total resolved signals: {len(resolved)}')

    for m in sorted(set(r['model_source'] for r in resolved)):
        rs_m = [r for r in resolved if r['model_source'] == m]
        correct = sum(1 for r in rs_m if
                      (r['direction']=='long'  and (r['actual'] or 0) > 0) or
                      (r['direction']=='short' and (r['actual'] or 0) < 0))
        executed_rs = [r for r in rs_m if r['status'] not in ('rejected','expired')]
        correct_ex  = sum(1 for r in executed_rs if
                          (r['direction']=='long'  and (r['actual'] or 0) > 0) or
                          (r['direction']=='short' and (r['actual'] or 0) < 0))

        preds   = [abs(r['pred'] or 0)   for r in rs_m if r['pred']   is not None]
        actuals = [abs(r['actual'] or 0) for r in rs_m if r['actual'] is not None]
        mae     = sum(abs((r['pred'] or 0)-(r['actual'] or 0)) for r in rs_m
                      if r['pred'] is not None and r['actual'] is not None) / len(rs_m)

        print(f'\n  [{m}]  resolved={len(rs_m)}  executed_resolved={len(executed_rs)}')
        print(f'    Dir accuracy (all)      : {correct}/{len(rs_m)} = {correct/len(rs_m)*100:.1f}%')
        if executed_rs:
            print(f'    Dir accuracy (executed) : {correct_ex}/{len(executed_rs)} = '
                  f'{correct_ex/len(executed_rs)*100:.1f}%')
        print(f'    Avg predicted magnitude : {sum(preds)/len(preds):.2f}%')
        print(f'    Avg actual magnitude    : {sum(actuals)/len(actuals):.2f}%')
        print(f'    Mean Abs Error (pred-act): {mae:.2f}%')
        # Overconfident? predicted >> actual?
        avg_pred = sum(r['pred'] or 0 for r in rs_m) / len(rs_m)
        avg_act  = sum(r['actual'] or 0 for r in rs_m) / len(rs_m)
        if avg_pred != 0:
            bias = (avg_pred - avg_act) / abs(avg_pred) * 100
            print(f'    Prediction bias         : {"over" if bias > 0 else "under"}estimates '
                  f'actual return by {abs(bias):.0f}%')

    # ── 6. FILTER PIPELINE — WHAT IS BEING BLOCKED ────────────────────────────
    print(f'\n{SEP}')
    print('  6. SIGNAL FILTER PIPELINE — EXECUTION RATES')
    print(SEP)
    all_sigs = conn.execute('''
        SELECT model_source, status, rejection_reason,
               confidence, predicted_return_pct, COALESCE(regime_version,1) rv
        FROM signals WHERE quality_flag IS NULL
    ''').fetchall()
    for m in sorted(set(r['model_source'] for r in all_sigs)):
        ms = [r for r in all_sigs if r['model_source'] == m]
        total_m = len(ms)
        rej_m   = [r for r in ms if r['status'] == 'rejected']
        exec_m  = [r for r in ms if r['status'] not in ('rejected','expired')]
        v2_m    = [r for r in ms if r['rv'] == 2]
        v2_exec = [r for r in v2_m if r['status'] not in ('rejected','expired')]
        print(f'\n  [{m}]  total={total_m}  executed={len(exec_m)} ({len(exec_m)/total_m*100:.0f}%)'
              f'  v2_total={len(v2_m)}  v2_executed={len(v2_exec)}')
        # Top rejection reasons
        rej_counts = {}
        for r in rej_m:
            rr = (r['rejection_reason'] or 'unknown').split(':')[0].strip()
            rej_counts[rr] = rej_counts.get(rr, 0) + 1
        top_rejs = sorted(rej_counts.items(), key=lambda x: -x[1])[:5]
        for rr, cnt in top_rejs:
            print(f'    {rr}: {cnt} ({cnt/total_m*100:.0f}%)')
        # Avg confidence of rejected vs executed
        conf_rej = [r['confidence'] for r in rej_m  if r['confidence'] is not None]
        conf_ex  = [r['confidence'] for r in exec_m if r['confidence'] is not None]
        if conf_rej and conf_ex:
            print(f'    Avg conf rejected={sum(conf_rej)/len(conf_rej):.3f}  '
                  f'executed={sum(conf_ex)/len(conf_ex):.3f}')

    # ── 7. REGIME v1 vs v2 TRADE QUALITY ─────────────────────────────────────
    print(f'\n{SEP}')
    print('  7. REGIME v1 vs v2 — TRADE QUALITY COMPARISON')
    print(SEP)
    for rv_label, rv_val in [('v1', 1), ('v2', 2)]:
        rv_trades = conn.execute('''
            SELECT t.pnl_gross, t.pnl_net, t.exit_reason, s.model_source, s.confidence
            FROM trades t JOIN signals s ON t.signal_id = s.id
            WHERE t.status="closed" AND t.quality_flag IS NULL
              AND COALESCE(s.regime_version,1) = ?
        ''', (rv_val,)).fetchall()
        if not rv_trades:
            print(f'\n  Regime {rv_label}: no trades')
            continue
        n_rv = len(rv_trades)
        w_rv = sum(1 for t in rv_trades if (t['pnl_gross'] or 0) > 0)
        g_rv = sum(t['pnl_gross'] or 0 for t in rv_trades)
        print(f'\n  Regime {rv_label}: n={n_rv}  WR={w_rv/n_rv*100:.0f}%  Gross={rs(g_rv)}')

    # ── 8. OPEN POSITIONS — CURRENT EXPOSURE ─────────────────────────────────
    print(f'\n{SEP}')
    print('  8. CURRENT OPEN POSITIONS')
    print(SEP)
    open_pos = conn.execute('''
        SELECT p.symbol, p.direction, p.entry_price, p.current_price,
               p.unrealised_pnl, p.entry_timestamp, p.size_contracts,
               p.stop_loss_price, p.take_profit_price, p.leverage,
               p.running_high, p.running_low, p.entry_atr,
               s.model_source, s.confidence, s.predicted_return_pct
        FROM positions p
        LEFT JOIN trades t ON t.id = p.trade_id
        LEFT JOIN signals s ON s.id = t.signal_id
        WHERE p.status IN ("open","closing")
        ORDER BY p.entry_timestamp ASC
    ''').fetchall()
    now = time.time()
    total_upnl = 0
    total_notional = 0
    if not open_pos:
        print('  No open positions.')
    for p in open_pos:
        ep   = p['entry_price'] or 0
        cp   = p['current_price'] or ep
        upnl = p['unrealised_pnl'] or 0
        total_upnl += upnl
        sl   = p['stop_loss_price'] or 0
        tp   = p['take_profit_price'] or 0
        held = (now - (p['entry_timestamp'] or now)) / 3600
        pct_move = ((cp-ep)/ep*100*(1 if p['direction']=='long' else -1)) if ep else 0
        sl_dist  = abs(cp - sl) / cp * 100 if sl and cp else 0
        tp_dist  = abs(tp - cp) / cp * 100 if tp and cp else 0
        rh = p['running_high'] or ep
        rl = p['running_low']  or ep
        peak_pct = abs(rh-ep)/ep*100 if ep else 0
        trough_pct = abs(rl-ep)/ep*100 if ep else 0
        print(f'\n  {p["symbol"]} {p["direction"].upper()}  [{p["model_source"]}]  '
              f'conf={p["confidence"] or 0:.3f}')
        print(f'    Entry={ep:,.2f}  Current={cp:,.2f}  Move={pct_move:+.2f}%  '
              f'uPnL={rs(upnl)}  Held={held:.1f}h')
        print(f'    SL={sl:,.2f} ({sl_dist:.2f}% away)  TP={tp:,.2f} ({tp_dist:.2f}% away)')
        print(f'    Peak excursion={peak_pct:+.2f}%  Trough excursion={trough_pct:.2f}%')
    print(f'\n  Total unrealised PnL : {rs(total_upnl)}')

    # ── 9. SHADOW SIGNALS (foundation model comparison) ──────────────────────
    print(f'\n{SEP}')
    print('  9. SHADOW SIGNALS (Kronos foundation model comparison)')
    print(SEP)
    shadow = conn.execute('''
        SELECT model_name, direction, confidence, predicted_return,
               signal_timestamp, context_candles
        FROM shadow_signals
        ORDER BY signal_timestamp DESC
    ''').fetchall()
    if not shadow:
        print('  No shadow signals recorded yet.')
    else:
        by_model = {}
        for s in shadow:
            by_model.setdefault(s['model_name'], []).append(s)
        for mn, sl in sorted(by_model.items()):
            longs  = sum(1 for s in sl if s['direction'] == 'long')
            shorts = sum(1 for s in sl if s['direction'] == 'short')
            try:
                avg_conf = sum(float(s['confidence'] or 0) for s in sl) / len(sl)
                conf_str = f'  AvgConf={avg_conf:.3f}'
            except Exception:
                conf_str = ''
            print(f'  {mn}: n={len(sl)}  Long={longs} Short={shorts}{conf_str}')

    # ── 10. FUNDING RATE ENVIRONMENT ─────────────────────────────────────────
    print(f'\n{SEP}')
    print('  10. CURRENT FUNDING RATES')
    print(SEP)
    funding = conn.execute('''
        SELECT f.symbol, f.rate, f.timestamp
        FROM funding_rates f
        INNER JOIN (SELECT symbol, MAX(timestamp) mx FROM funding_rates GROUP BY symbol) l
               ON f.symbol=l.symbol AND f.timestamp=l.mx
        ORDER BY f.symbol
    ''').fetchall()
    for fr in funding:
        rate = fr['rate'] or 0
        ann  = rate * 3 * 365 * 100  # 3 funding periods/day * 365 days
        bias = 'LONGS PAY' if rate > 0 else 'SHORTS PAY'
        print(f'  {fr["symbol"]}: {rate*100:+.4f}%/8H ({ann:+.1f}% annualised)  — {bias}')

    # ── 11. STREAK ANALYSIS ───────────────────────────────────────────────────
    print(f'\n{SEP}')
    print('  11. WIN/LOSS STREAK ANALYSIS')
    print(SEP)
    all_chrono = sorted(clean, key=lambda t: t['entry_timestamp'] or 0)
    results_all = ['W' if (t['pnl_gross'] or 0) > 0 else 'L' for t in all_chrono]
    print(f'  Overall sequence : {"".join(results_all)}')
    # Max streaks
    cur, cnt, max_w, max_l = None, 0, 0, 0
    for r in results_all:
        cnt = cnt+1 if r == cur else 1
        cur = r
        if cur == 'W': max_w = max(max_w, cnt)
        else: max_l = max(max_l, cnt)
    print(f'  Max win streak   : {max_w}')
    print(f'  Max loss streak  : {max_l}')

    for m, ts_list in sorted(models.items()):
        chrono = sorted(ts_list, key=lambda t: t['entry_timestamp'] or 0)
        res = ['W' if (t['pnl_gross'] or 0) > 0 else 'L' for t in chrono]
        cur, cnt, mw, ml = None, 0, 0, 0
        for r in res:
            cnt = cnt+1 if r == cur else 1; cur = r
            if cur == 'W': mw = max(mw, cnt)
            else: ml = max(ml, cnt)
        print(f'  {m:20s}: {"".join(res)}  maxW={mw} maxL={ml}')

    # ── 12. LAST 7 DAYS ───────────────────────────────────────────────────────
    print(f'\n{SEP}')
    print('  12. LAST 7 DAYS')
    print(SEP)
    cutoff = time.time() - 7 * 86400
    recent = [t for t in clean if (t['entry_timestamp'] or 0) >= cutoff]
    if not recent:
        print('  No clean trades in last 7 days.')
    else:
        rw = sum(1 for t in recent if (t['pnl_gross'] or 0) > 0)
        rg = sum(t['pnl_gross'] or 0 for t in recent)
        rn = sum(t['pnl_net']   or 0 for t in recent)
        print(f'  Trades={len(recent)}  WR={rw/len(recent)*100:.0f}%  '
              f'Gross={rs(rg)}  Net={rs(rn)}')
        by_m = {}
        for t in recent:
            by_m.setdefault(t['model_source'] or 'custom', []).append(t)
        for mm, lst in sorted(by_m.items()):
            ww = sum(1 for t in lst if (t['pnl_gross'] or 0) > 0)
            gg = sum(t['pnl_gross'] or 0 for t in lst)
            print(f'  {mm}: n={len(lst)} WR={ww/len(lst)*100:.0f}% Gross={rs(gg)}')
        # Most recent 5 trades
        print('\n  Most recent 5 trades:')
        for t in sorted(recent, key=lambda x: x['exit_timestamp'] or 0, reverse=True)[:5]:
            result = 'WIN ' if (t['pnl_gross'] or 0) > 0 else 'LOSS'
            print(f'    {ts(t["exit_timestamp"])}  {t["symbol"]:8s} {t["direction"]:5s} '
                  f'[{t["model_source"] or "custom":15s}] {result} {rs(t["pnl_gross"] or 0)}  '
                  f'exit={t["exit_reason"] or "?"}')

    # ── 13. BENCHMARK COMPARISON ──────────────────────────────────────────────
    print(f'\n{SEP}')
    print(f'  13. BENCHMARK COMPARISON — all models vs {BENCHMARK_MODEL_SOURCE}')
    print(SEP)

    def model_stats(ts_list):
        if not ts_list:
            return None
        n      = len(ts_list)
        wins   = [t for t in ts_list if (t['pnl_net'] or 0) > 0]
        losses = [t for t in ts_list if (t['pnl_net'] or 0) <= 0]
        wr     = len(wins) / n
        avg_w  = sum(t['pnl_net'] or 0 for t in wins)   / len(wins)   if wins   else 0
        avg_l  = sum(t['pnl_net'] or 0 for t in losses) / len(losses) if losses else 0
        exp    = wr * avg_w + (1 - wr) * avg_l
        net    = sum(t['pnl_net'] or 0 for t in ts_list)
        longs  = sum(1 for t in ts_list if t['direction'] == 'long')
        return dict(n=n, wr=wr, avg_w=avg_w, avg_l=avg_l, exp=exp, net=net,
                    long_pct=longs/n*100)

    bm_trades = models.get(BENCHMARK_MODEL_SOURCE, [])
    bm        = model_stats(bm_trades)

    if not bm:
        print(f'  Benchmark model {BENCHMARK_MODEL_SOURCE} has no closed trades yet.')
    else:
        print(f'\n  Benchmark  : {BENCHMARK_MODEL_SOURCE}')
        print(f'  n={bm["n"]}  WR={bm["wr"]*100:.0f}%  '
              f'Expect/trade=Rs{bm["exp"]:+.0f}  Net=Rs{bm["net"]:+.0f}  '
              f'Long%={bm["long_pct"]:.0f}%')

        # Directional accuracy from resolved signals
        bm_resolved = conn.execute('''
            SELECT s.direction, s.actual_return_pct
            FROM signals s
            WHERE s.model_source = ?
              AND s.actual_return_pct IS NOT NULL
              AND (s.quality_flag IS NULL OR s.quality_flag = '')
        ''', (BENCHMARK_MODEL_SOURCE,)).fetchall()
        bm_da = None
        if bm_resolved:
            bm_correct = sum(
                1 for s in bm_resolved
                if (s['direction'] == 'long'  and s['actual_return_pct'] > 0) or
                   (s['direction'] == 'short' and s['actual_return_pct'] < 0)
            )
            bm_da = bm_correct / len(bm_resolved) * 100

        print(f'\n  {"Metric":<22} {"BENCHMARK":>12}', end='')
        other_models = [m for m in sorted(models.keys()) if m != BENCHMARK_MODEL_SOURCE]
        for m in other_models:
            label = m.replace('kronos-', 'k-')
            print(f'  {label:>14}', end='')
        print()
        print(f'  {"-"*22}', end='')
        print(f'  {"------------":>12}', end='')
        for _ in other_models:
            print(f'  {"----------":>14}', end='')
        print()

        rows_bm = [
            ('Trades (n)',        f'{bm["n"]}',                    lambda s: (s["n"], s["n"]-bm["n"],         False)),
            ('Win rate',          f'{bm["wr"]*100:.0f}%',          lambda s: (f'{s["wr"]*100:.0f}%', s["wr"]-bm["wr"], True)),
            ('Long bias',         f'{bm["long_pct"]:.0f}%',        lambda s: (f'{s["long_pct"]:.0f}%', (s["long_pct"]-bm["long_pct"])/100.0, True)),
            ('Avg win  (Rs)',      f'{bm["avg_w"]:+.0f}',           lambda s: (f'{s["avg_w"]:+.0f}', s["avg_w"]-bm["avg_w"], False)),
            ('Avg loss (Rs)',      f'{bm["avg_l"]:+.0f}',           lambda s: (f'{s["avg_l"]:+.0f}', s["avg_l"]-bm["avg_l"], False)),
            ('Expect/trade (Rs)', f'{bm["exp"]:+.0f}',             lambda s: (f'{s["exp"]:+.0f}', s["exp"]-bm["exp"], False)),
            ('Net P&L (Rs)',       f'{bm["net"]:+.0f}',             lambda s: (f'{s["net"]:+.0f}', s["net"]-bm["net"], False)),
        ]

        for label, bm_val, fn in rows_bm:
            print(f'  {label:<22} {bm_val:>12}', end='')
            for m in other_models:
                s = model_stats(models.get(m, []))
                if not s:
                    print(f'  {"--":>14}', end='')
                    continue
                val, delta, is_pct = fn(s)
                if is_pct:
                    d_str = f'Δ{delta*100:+.0f}pp'
                else:
                    d_str = f'Δ{delta:+.0f}'
                cell = f'{val} {d_str}'
                print(f'  {cell:>14}', end='')
            print()

        # Dir accuracy row from signals
        print(f'  {"Dir accuracy":<22}', end='')
        if bm_da is not None:
            print(f' {bm_da:>11.1f}%', end='')
        else:
            print(f' {"--":>12}', end='')
        for m in other_models:
            m_resolved = conn.execute('''
                SELECT direction, actual_return_pct FROM signals
                WHERE model_source=? AND actual_return_pct IS NOT NULL
                  AND (quality_flag IS NULL OR quality_flag='')
            ''', (m,)).fetchall()
            if not m_resolved or bm_da is None:
                print(f'  {"--":>14}', end='')
                continue
            m_correct = sum(
                1 for s in m_resolved
                if (s['direction'] == 'long'  and s['actual_return_pct'] > 0) or
                   (s['direction'] == 'short' and s['actual_return_pct'] < 0)
            )
            m_da   = m_correct / len(m_resolved) * 100
            delta  = m_da - bm_da
            cell   = f'{m_da:.1f}% Δ{delta:+.1f}pp'
            print(f'  {cell:>14}', end='')
        print()

        # Verdict row
        print(f'\n  {"Verdict":<22} {"BENCHMARK":>12}', end='')
        for m in other_models:
            s = model_stats(models.get(m, []))
            if not s:
                print(f'  {"NO DATA":>14}', end='')
                continue
            gap = s['exp'] - bm['exp']
            if gap >= 0:
                verdict = 'AHEAD'
            elif gap >= -100:
                verdict = 'CLOSE'
            else:
                verdict = 'BEHIND'
            print(f'  {verdict:>14}', end='')
        print()

    print(f'\n{SEP}')
    print('  END OF REPORT')
    print(SEP)
