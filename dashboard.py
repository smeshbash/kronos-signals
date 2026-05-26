"""
Kronos Trading System — Module 12: Live Dashboard
Flask web dashboard served on port 8050 (configurable via KRONOS_DASHBOARD_PORT).

Two tabs:
  Tab 1 — Summary  : 4 headline cards + Open Positions + Trade History
  Tab 2 — Ops      : Signal Pipeline + Shadow Signals + Model Accuracy + Funding Rates

Auto-refreshes every 30 seconds.

Run standalone:
    python dashboard.py
"""

import os
import sqlite3
import time
from datetime import datetime, timezone

from flask import Flask

from db import DB_PATH

app   = Flask(__name__)
PORT  = int(os.environ.get('KRONOS_DASHBOARD_PORT', 8050))
PAPER = os.environ.get('KRONOS_PAPER_MODE', 'true').lower() == 'true'
PHASE = os.environ.get('KRONOS_PHASE', 'pre_live')
START = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', 100000.0))

# ── Tiny helpers ──────────────────────────────────────────────────────────────

def _f(v, d=0.0):
    try:    return float(v) if v is not None else d
    except: return d

def _inr(v, sign=True):
    try:
        v = float(v)
        s = ('+' if v >= 0 else '') if sign else ''
        return f'{s}₹{v:,.2f}'
    except:
        return '--'

def _pct(v, decimals=2):
    try:
        v = float(v)
        return f'{"+" if v >= 0 else ""}{v:.{decimals}f}%'
    except:
        return '--'

def _ts(epoch):
    try:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime('%d %b %H:%M')
    except:
        return '--'

def _elapsed(epoch):
    try:
        s = int(time.time()) - int(epoch)
        if s < 60:   return f'{s}s'
        if s < 3600: return f'{s//60}m'
        h, m = s//3600, (s%3600)//60
        return f'{h}h {m}m' if m else f'{h}h'
    except:
        return '--'

def _gain(v):
    """CSS class for a numeric value."""
    try:
        return 'pos' if float(v) > 0 else ('neg' if float(v) < 0 else 'neu')
    except:
        return 'neu'

def _dir(d):
    d = str(d).lower()
    if d == 'long':  return '<span class="badge b-long">&#9650; Long</span>'
    if d == 'short': return '<span class="badge b-short">&#9660; Short</span>'
    return f'<span class="badge b-neu">{d}</span>'

def _status(s):
    m = {'pending':  ('b-warn',  'Pending'),
         'approved': ('b-pos',   'Approved'),
         'rejected': ('b-neg',   'Rejected'),
         'executed': ('b-blue',  'Executed'),
         'expired':  ('b-neu',   'Expired')}
    cls, lbl = m.get(str(s).lower(), ('b-neu', s))
    return f'<span class="badge {cls}">{lbl}</span>'

def _model(n):
    m = {'custom':      ('b-gold',   'Custom'),
         'kronos-mini': ('b-blue',   'Kronos-mini'),
         'kronos-base': ('b-purple', 'Kronos-base')}
    cls, lbl = m.get(str(n).lower(), ('b-neu', n))
    return f'<span class="badge {cls}">{lbl}</span>'

# ── Database ──────────────────────────────────────────────────────────────────

def _q(sql, p=()):
    try:
        c = sqlite3.connect(DB_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL')
        rows = [dict(r) for r in c.execute(sql, p).fetchall()]
        c.close()
        return rows
    except:
        return []

def get_data():
    # Latest portfolio snapshot
    pf = (_q("SELECT * FROM portfolio_snapshots ORDER BY timestamp DESC LIMIT 1") or [{}])[0]

    # Closed trades — quality_flag IS NULL excludes test artifacts and bug-corrupted records
    closed = _q("SELECT pnl_gross, pnl_net, tds_deducted FROM trades WHERE status='closed' AND quality_flag IS NULL")
    gross   = sum(_f(r['pnl_gross']) for r in closed)
    net     = sum(_f(r['pnl_net'])   for r in closed)
    tds     = sum(_f(r['tds_deducted']) for r in closed)
    n       = len(closed)
    wins    = sum(1 for r in closed if _f(r['pnl_gross']) > 0)
    losses  = n - wins
    wr      = wins / n * 100 if n else 0.0

    # Flagged trade count (shown as context note in dashboard)
    flagged_trades = (_q("SELECT COUNT(*) AS c FROM trades WHERE quality_flag IS NOT NULL") or [{'c':0}])[0]['c']

    # Max drawdown — from snapshots only after clean data epoch
    dd_row  = _q("SELECT MAX(drawdown_pct) AS v FROM portfolio_snapshots")
    max_dd  = _f(dd_row[0]['v']) if dd_row else 0.0

    # Open positions — positions table has no quality_flag; it's live state only
    positions = _q("""
        SELECT p.symbol, p.direction, p.entry_price, p.current_price,
               p.size_contracts, p.unrealised_pnl, p.entry_timestamp,
               p.max_hold_until, p.leverage, p.stop_loss_price, p.take_profit_price
        FROM positions p
        WHERE p.status IN ('open','closing')
        ORDER BY p.entry_timestamp DESC
    """)

    # Trade history (last 20 clean trades)
    history = _q("""
        SELECT t.symbol, t.direction, t.entry_price, t.exit_price,
               t.size_contracts, t.pnl_gross, t.pnl_net, t.tds_deducted,
               t.exit_reason, t.entry_timestamp, t.exit_timestamp,
               s.confidence, s.predicted_return_pct
        FROM trades t
        LEFT JOIN signals s ON t.signal_id = s.id
        WHERE t.status = 'closed'
          AND t.quality_flag IS NULL
        ORDER BY t.exit_timestamp DESC
        LIMIT 20
    """)

    # --- Ops tab data ---

    # Signal pipeline (last 20) — show all including flagged, but badge the flagged ones
    pipeline = _q("""
        SELECT id, symbol, direction, confidence, horizon, status,
               rejection_reason, predicted_return_pct, signal_timestamp, quality_flag
        FROM signals
        ORDER BY signal_timestamp DESC LIMIT 20
    """)

    # Shadow signals (last 30)
    shadow = _q("""
        SELECT symbol, model_name, direction, confidence,
               predicted_return, context_candles, signal_timestamp
        FROM shadow_signals
        ORDER BY signal_timestamp DESC LIMIT 30
    """)

    # Model accuracy (24H horizon)
    hz       = 6 * 4 * 3600
    cutoff   = int(time.time()) - hz

    def _acc_query(tbl, dir_col, ts_col, ret_col, extra_where='', model_filter=()):
        where = f"AND {extra_where}" if extra_where else ''
        rows = _q(f"""
            SELECT s.{dir_col} AS direction, s.{ts_col} AS sig_ts,
                   oe.close AS ec, ox.close AS xc
            FROM {tbl} s
            LEFT JOIN ohlcv oe ON oe.symbol=s.symbol AND oe.timeframe='4h'
              AND oe.timestamp=(SELECT MAX(timestamp) FROM ohlcv
                  WHERE symbol=s.symbol AND timeframe='4h' AND timestamp<=s.{ts_col})
            LEFT JOIN ohlcv ox ON ox.symbol=s.symbol AND ox.timeframe='4h'
              AND ox.timestamp=(SELECT MIN(timestamp) FROM ohlcv
                  WHERE symbol=s.symbol AND timeframe='4h' AND timestamp>=s.{ts_col}+{hz})
            WHERE s.{ts_col} < {cutoff}
              AND oe.close IS NOT NULL AND ox.close IS NOT NULL
              {where}
        """, model_filter)
        correct = sum(1 for r in rows
                      if (r['xc'] > r['ec']) == (r['direction'].lower() == 'long')
                      and _f(r['ec']) > 0)
        return correct, len(rows)

    # quality_flag IS NULL — exclude corrupted/test signals from accuracy measurement
    c_cor, c_tot = _acc_query('signals',      'direction', 'signal_timestamp', 'predicted_return_pct',
                               "status NOT IN ('rejected','expired') AND quality_flag IS NULL")
    m_cor, m_tot = _acc_query('shadow_signals','direction','signal_timestamp','predicted_return',
                               "model_name='kronos-mini'")
    b_cor, b_tot = _acc_query('shadow_signals','direction','signal_timestamp','predicted_return',
                               "model_name='kronos-base'")

    # Pending counts (clean signals only for custom model)
    c_pend = (_q("SELECT COUNT(*) AS v FROM signals WHERE status NOT IN ('rejected','expired') AND quality_flag IS NULL AND signal_timestamp>=?", (cutoff,)) or [{'v':0}])[0]['v']
    m_pend = (_q("SELECT COUNT(*) AS v FROM shadow_signals WHERE model_name='kronos-mini' AND signal_timestamp>=?", (cutoff,)) or [{'v':0}])[0]['v']
    b_pend = (_q("SELECT COUNT(*) AS v FROM shadow_signals WHERE model_name='kronos-base' AND signal_timestamp>=?", (cutoff,)) or [{'v':0}])[0]['v']

    accuracy = [
        {'name':'Custom',      'c':c_cor, 't':c_tot, 'pend':int(_f(c_pend))},
        {'name':'kronos-mini', 'c':m_cor, 't':m_tot, 'pend':int(_f(m_pend))},
        {'name':'kronos-base', 'c':b_cor, 't':b_tot, 'pend':int(_f(b_pend))},
    ]

    # Funding rates
    funding = _q("""
        SELECT f.symbol, f.rate, f.timestamp FROM funding_rates f
        INNER JOIN (SELECT symbol, MAX(timestamp) mx FROM funding_rates GROUP BY symbol) l
               ON f.symbol=l.symbol AND f.timestamp=l.mx
        ORDER BY f.symbol
    """)

    return dict(pf=pf, gross=gross, net=net, tds=tds, n=n, wins=wins, losses=losses,
                wr=wr, max_dd=max_dd, positions=positions, history=history,
                pipeline=pipeline, shadow=shadow, accuracy=accuracy, funding=funding,
                flagged_trades=flagged_trades, ts=int(time.time()))


# ── HTML ──────────────────────────────────────────────────────────────────────

CSS = """
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f4f5f7;font-family:'Segoe UI',system-ui,sans-serif;font-size:.87rem;color:#172b4d}

/* ── Nav ── */
.topbar{background:#fff;border-bottom:1px solid #dfe1e6;padding:10px 20px;
        display:flex;align-items:center;justify-content:space-between;
        box-shadow:0 1px 3px rgba(0,0,0,.08)}
.topbar-brand{font-weight:700;font-size:.95rem;letter-spacing:.05em;color:#172b4d}
.topbar-right{display:flex;align-items:center;gap:12px;font-size:.75rem;color:#6b778c}
.pill{display:inline-block;padding:2px 10px;border-radius:20px;font-size:.7rem;font-weight:600}
.pill-paper{background:#fffae6;color:#172b4d;border:1px solid #ffe380}
.pill-live {background:#ffebe6;color:#bf2600;border:1px solid #ffbdad}
.pill-phase{background:#f4f5f7;color:#5e6c84;border:1px solid #dfe1e6}

/* ── Tabs ── */
.tabs{background:#fff;border-bottom:2px solid #dfe1e6;padding:0 20px;display:flex;gap:0}
.tab-btn{background:none;border:none;padding:10px 18px;font-size:.83rem;font-weight:600;
         color:#6b778c;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:.15s}
.tab-btn:hover{color:#172b4d}
.tab-btn.active{color:#0052cc;border-bottom-color:#0052cc}
.tab-pane{display:none;padding:20px}
.tab-pane.active{display:block}

/* ── Cards ── */
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr)}}
@media(max-width:500px){.cards{grid-template-columns:1fr}}
.card{background:#fff;border:1px solid #dfe1e6;border-radius:8px;
      padding:16px 18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.card-accent{border-top:3px solid #0052cc}
.card-lbl{font-size:.68rem;text-transform:uppercase;letter-spacing:.07em;color:#6b778c;margin-bottom:6px;font-weight:600}
.card-val{font-size:1.6rem;font-weight:700;line-height:1;color:#172b4d}
.card-sub{font-size:.75rem;margin-top:5px;color:#6b778c}

/* ── Sections ── */
.section{background:#fff;border:1px solid #dfe1e6;border-radius:8px;
         margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,.05);overflow:hidden}
.section-hdr{padding:10px 16px;border-bottom:1px solid #f0f1f3;
             font-size:.72rem;font-weight:700;text-transform:uppercase;
             letter-spacing:.07em;color:#5e6c84;background:#fafbfc;
             display:flex;align-items:center;gap:8px}
.section-hdr .count{background:#dfe1e6;color:#5e6c84;border-radius:10px;
                    padding:1px 7px;font-size:.68rem}
.empty{padding:18px 16px;color:#97a0af;font-size:.82rem}

/* ── Tables ── */
table{width:100%;border-collapse:collapse}
th{background:#fafbfc;border-bottom:2px solid #dfe1e6;padding:7px 10px;
   font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:#5e6c84;
   font-weight:700;white-space:nowrap;text-align:left}
td{padding:7px 10px;border-bottom:1px solid #f4f5f7;color:#172b4d;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f8f9fb}

/* ── Colour classes ── */
.pos{color:#006644!important;font-weight:600}
.neg{color:#bf2600!important;font-weight:600}
.neu{color:#6b778c!important}
.warn{color:#974f0c!important}

/* ── Badges ── */
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.7rem;font-weight:600}
.b-long  {background:#e3fcef;color:#006644;border:1px solid #abf5d1}
.b-short {background:#ffebe6;color:#bf2600;border:1px solid #ffbdad}
.b-pos   {background:#e3fcef;color:#006644}
.b-neg   {background:#ffebe6;color:#bf2600}
.b-warn  {background:#fffae6;color:#172b4d}
.b-blue  {background:#deebff;color:#0747a6}
.b-purple{background:#eae6ff;color:#403294}
.b-gold  {background:#fffae6;color:#7a5200}
.b-neu   {background:#f4f5f7;color:#5e6c84}

/* ── Misc ── */
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:18px}
@media(max-width:900px){.two-col{grid-template-columns:1fr}}
.tag{font-size:.7rem;color:#6b778c}
.wr-bar{height:6px;border-radius:3px;background:#e3fcef;overflow:hidden;margin-top:5px;width:100%}
.wr-fill{height:100%;border-radius:3px}
.footer{text-align:center;padding:14px;font-size:.7rem;color:#97a0af}
</style>
"""

JS = """
<script>
function showTab(name){
  document.querySelectorAll('.tab-pane').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('pane-'+name).classList.add('active');
  document.querySelector('[data-tab="'+name+'"]').classList.add('active');
  localStorage.setItem('kronos-tab',name);
}
window.onload=function(){
  var t=localStorage.getItem('kronos-tab')||'summary';
  showTab(t);
};
</script>
"""


def render(d):
    pf       = d['pf']
    pval     = _f(pf.get('total_value'), START)
    pval_chg = pval - START
    unreal   = _f(pf.get('unrealised_pnl'))

    mode_pill = f'<span class="pill pill-paper">PAPER</span>' if PAPER else '<span class="pill pill-live">LIVE</span>'
    phase_map = {'pre_live':'Pre-Live','income':'Income','compound':'Compound'}
    phase_lbl = phase_map.get(PHASE, PHASE.replace('_',' ').title())

    # ── Tab 1: Summary ────────────────────────────────────────────────────────

    # Data quality notice
    flagged = d.get('flagged_trades', 0)
    if flagged > 0 and d['n'] == 0:
        quality_notice = f"""
<div style="background:#fffae6;border:1px solid #ffe380;border-radius:6px;
            padding:9px 14px;margin-bottom:14px;font-size:.8rem;color:#172b4d">
  <strong>&#9888; Clean baseline starting now.</strong>
  &nbsp;{flagged} historical trade{'s' if flagged!=1 else ''} flagged and excluded
  (test artifacts + bug-corrupted exits from duplicate process issue, fixed 2026-05-27).
  All metrics below reflect only clean data going forward.
</div>"""
    elif flagged > 0:
        quality_notice = f"""
<div style="background:#f4f5f7;border:1px solid #dfe1e6;border-radius:6px;
            padding:7px 14px;margin-bottom:14px;font-size:.75rem;color:#6b778c">
  &#128274; {flagged} flagged trade{'s' if flagged!=1 else ''} excluded from all metrics
  &mdash; see Operational tab for details.
</div>"""
    else:
        quality_notice = ''

    # 4 headline cards
    wr_color = '#36b37e' if d['wr'] >= 50 else '#ff5630'
    cards = f"""
<div class="cards">
  <div class="card card-accent">
    <div class="card-lbl">Portfolio Value</div>
    <div class="card-val">₹{pval:,.0f}</div>
    <div class="card-sub {_gain(pval_chg)}">
      {"+" if pval_chg >= 0 else ""}₹{pval_chg:,.2f} from start
      {"&nbsp;|&nbsp;" + _pct(unreal) + " unreal." if unreal != 0 else ""}
    </div>
  </div>
  <div class="card">
    <div class="card-lbl">Gross P&amp;L &mdash; {d['n']} trades</div>
    <div class="card-val {_gain(d['gross'])}">{_inr(d['gross'])}</div>
    <div class="card-sub neu">Net after TDS: {_inr(d['net'])}
      &nbsp;&nbsp;TDS paid: ₹{d['tds']:,.2f}</div>
  </div>
  <div class="card">
    <div class="card-lbl">Win Rate &mdash; {d['wins']}W / {d['losses']}L</div>
    <div class="card-val" style="color:{wr_color}">{d['wr']:.1f}%</div>
    <div class="wr-bar"><div class="wr-fill" style="width:{d['wr']:.0f}%;background:{wr_color}"></div></div>
  </div>
  <div class="card">
    <div class="card-lbl">Max Drawdown</div>
    <div class="card-val {'neg' if d['max_dd']>5 else 'warn' if d['max_dd']>2 else 'pos'}">
      -{d['max_dd']:.2f}%
    </div>
    <div class="card-sub neu">Current DD: -{_f(pf.get('drawdown_pct')):.2f}%
      &nbsp;&nbsp;Open: {len(d['positions'])}</div>
  </div>
</div>"""

    # Open positions
    if d['positions']:
        rows = ''
        for p in d['positions']:
            ep   = _f(p['entry_price'])
            cp   = _f(p['current_price'], ep)
            upnl = _f(p['unrealised_pnl'])
            upct = ((cp - ep) / ep * 100 * (1 if p['direction'].lower()=='long' else -1)) if ep else 0.0
            rows += f"""<tr>
  <td>{_dir(p['direction'])}</td>
  <td><strong>{p['symbol']}</strong></td>
  <td>₹{ep:,.2f}</td>
  <td>₹{cp:,.2f}</td>
  <td class="{_gain(upnl)}">{_inr(upnl)} <span class="tag">({_pct(upct)})</span></td>
  <td class="neu">{_elapsed(p['entry_timestamp'])}</td>
  <td class="tag">{_ts(p['max_hold_until'])}</td>
</tr>"""
        pos_html = f"""
<div class="section">
  <div class="section-hdr">Open Positions <span class="count">{len(d['positions'])}</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Dir</th><th>Symbol</th><th>Entry</th><th>Current</th>
    <th>Unrealised P&amp;L</th><th>Held</th><th>Max Hold (UTC)</th></tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        pos_html = '<div class="section"><div class="section-hdr">Open Positions <span class="count">0</span></div><div class="empty">No open positions.</div></div>'

    # Trade history
    if d['history']:
        rows = ''
        for t in d['history']:
            ep   = _f(t['entry_price'])
            xp   = _f(t['exit_price'])
            g    = _f(t['pnl_gross'])
            reason = str(t.get('exit_reason') or '--').replace('_',' ')
            rows += f"""<tr>
  <td class="tag">{_ts(t['exit_timestamp'])}</td>
  <td><strong>{t['symbol']}</strong></td>
  <td>{_dir(t['direction'])}</td>
  <td class="tag">₹{ep:,.2f} &rarr; ₹{xp:,.2f}</td>
  <td class="{_gain(g)}">{_inr(g)}</td>
  <td class="neu" style="font-size:.78rem">{reason}</td>
</tr>"""
        hist_html = f"""
<div class="section">
  <div class="section-hdr">Trade History <span class="count">last {len(d['history'])}</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Closed (UTC)</th><th>Symbol</th><th>Dir</th>
    <th>Entry &rarr; Exit</th><th>Gross P&amp;L</th><th>Closed Because</th></tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        hist_html = '<div class="section"><div class="section-hdr">Trade History</div><div class="empty">No closed trades yet.</div></div>'

    summary_pane = quality_notice + cards + pos_html + hist_html

    # ── Tab 2: Ops ────────────────────────────────────────────────────────────

    # Model accuracy
    acc_rows = ''
    for m in d['accuracy']:
        t, c, pend = m['t'], m['c'], m['pend']
        if t == 0:
            acc_pct  = '<span class="neu">--</span>'
            acc_bar  = ''
            evaluated = f'<span class="neu">0 evaluated</span>'
        else:
            pct = c / t * 100
            bar_color = '#36b37e' if pct >= 55 else ('#f59e0b' if pct >= 45 else '#ff5630')
            acc_pct  = f'<span style="color:{bar_color};font-weight:700">{pct:.1f}%</span>'
            acc_bar  = f'<div style="height:6px;border-radius:3px;background:#f4f5f7;width:100px;overflow:hidden;display:inline-block;vertical-align:middle;margin-left:6px"><div style="height:100%;width:{pct:.0f}%;background:{bar_color};border-radius:3px"></div></div>'
            evaluated = f'{c}&nbsp;/&nbsp;{t} evaluated'
        pend_str = f'<span class="tag"> +{pend} maturing</span>' if pend else ''
        acc_rows += f'<tr><td>{_model(m["name"])}</td><td>{acc_pct}{acc_bar}</td><td class="neu">{evaluated}{pend_str}</td></tr>'

    acc_html = f"""
<div class="section">
  <div class="section-hdr">Model Accuracy <span class="tag" style="font-size:.68rem;text-transform:none;font-weight:400;margin-left:4px">24H horizon &mdash; signals need 24H to mature</span></div>
  <table><thead><tr><th>Model</th><th>Accuracy</th><th>Sample</th></tr></thead>
  <tbody>{acc_rows}</tbody></table>
</div>"""

    # Signal pipeline
    if d['pipeline']:
        rows = ''
        for s in d['pipeline']:
            ret    = _f(s.get('predicted_return_pct'))
            reason = str(s.get('rejection_reason') or '').replace('_',' ')
            if len(reason) > 55: reason = reason[:52] + '...'
            qf = s.get('quality_flag')
            qf_badge = f'&nbsp;<span class="badge" style="background:#ffebe6;color:#bf2600;font-size:.65rem">excluded</span>' if qf else ''
            row_style = ' style="opacity:.55"' if qf else ''
            rows += f"""<tr{row_style}>
  <td class="tag">{_ts(s['signal_timestamp'])}</td>
  <td><strong>{s['symbol']}</strong>{qf_badge}</td>
  <td>{_dir(s['direction'])}</td>
  <td>{_f(s['confidence']):.3f}</td>
  <td class="{_gain(ret)}">{_pct(ret)}</td>
  <td>{_status(s['status'])}</td>
  <td class="tag" style="font-size:.72rem" title="{s.get('rejection_reason') or ''}">{reason}</td>
</tr>"""
        pipe_html = f"""
<div class="section">
  <div class="section-hdr">Signal Pipeline <span class="count">last {len(d['pipeline'])}</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Time (UTC)</th><th>Symbol</th><th>Dir</th><th>Conf.</th>
    <th>Pred. Return</th><th>Status</th><th>Rejection Reason</th></tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        pipe_html = '<div class="section"><div class="section-hdr">Signal Pipeline</div><div class="empty">No signals yet.</div></div>'

    # Shadow signals
    if d['shadow']:
        rows = ''
        for s in d['shadow']:
            ret = _f(s['predicted_return'])
            rows += f"""<tr>
  <td class="tag">{_ts(s['signal_timestamp'])}</td>
  <td>{_model(s['model_name'])}</td>
  <td><strong>{s['symbol']}</strong></td>
  <td>{_dir(s['direction'])}</td>
  <td>{_f(s['confidence']):.3f}</td>
  <td class="{_gain(ret)}">{_pct(ret)}</td>
  <td class="tag">{s['context_candles']}</td>
</tr>"""
        shadow_html = f"""
<div class="section">
  <div class="section-hdr">Shadow Signals <span class="count">last {len(d['shadow'])}</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Time (UTC)</th><th>Model</th><th>Symbol</th><th>Dir</th>
    <th>Confidence</th><th>Pred. Return</th><th>Context</th></tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        shadow_html = '<div class="section"><div class="section-hdr">Shadow Signals</div><div class="empty">No shadow signals yet.</div></div>'

    # Funding rates
    if d['funding']:
        chips = ''
        for fr in d['funding']:
            rate = _f(fr['rate'])
            cls  = 'neg' if rate > 0.001 else ('pos' if rate < -0.001 else 'neu')
            chips += f'<span style="margin-right:20px"><strong>{fr["symbol"]}</strong>&nbsp;<span class="{cls}">{rate*100:+.4f}%/8H</span></span>'
        fund_html = f'<div class="section"><div class="section-hdr">Latest Funding Rates</div><div style="padding:10px 16px;font-size:.82rem">{chips}</div></div>'
    else:
        fund_html = ''

    ops_pane = acc_html + f'<div class="two-col">{pipe_html}{shadow_html}</div>' + fund_html

    # ── Full page ─────────────────────────────────────────────────────────────
    updated = datetime.fromtimestamp(d['ts'], tz=timezone.utc).strftime('%d %b %H:%M UTC')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>Kronos</title>
  {CSS}
</head>
<body>

<div class="topbar">
  <span class="topbar-brand">&#9654;&nbsp; KRONOS</span>
  <span class="topbar-right">
    {mode_pill}
    <span class="pill pill-phase">{phase_lbl}</span>
    <span>Updated {updated}</span>
    <span style="color:#dfe1e6">|</span>
    <span>Auto-refresh 30s</span>
  </span>
</div>

<div class="tabs">
  <button class="tab-btn" data-tab="summary" onclick="showTab('summary')">Summary</button>
  <button class="tab-btn" data-tab="ops"     onclick="showTab('ops')">Operational</button>
</div>

<div id="pane-summary" class="tab-pane">{summary_pane}</div>
<div id="pane-ops"     class="tab-pane">{ops_pane}</div>

<div class="footer">Kronos Trading System &mdash; {('Paper trading' if PAPER else 'Live trading')} &mdash; Not financial advice</div>

{JS}
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render(get_data())

@app.route('/health')
def health():
    return {'status': 'ok', 'ts': int(time.time())}


if __name__ == '__main__':
    from db import init_db
    init_db()
    print(f'Kronos Dashboard  ->  http://0.0.0.0:{PORT}')
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
