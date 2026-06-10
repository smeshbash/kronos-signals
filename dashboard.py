"""
Kronos Trading System — Module 12: Live Dashboard
Flask web dashboard served on port 8050 (configurable via KRONOS_DASHBOARD_PORT).

Tabs:
  Tab 1 — Summary    : Filter bar + Model capital cards + Open Positions + Trade History
  Tab 2 — Analysis   : Model×Asset matrix, equity curves, confidence calibration, funnel
  Tab 3 — Signals    : Full signal explorer (filtered + paginated)
  Tab 4 — Operational: Signal pipeline + Model accuracy + Funding rates + Events log

Filter bar (persistent, below tab strip):
  Model / Symbol / Direction / Period / Regime
  URL-parameter based — bookmarkable, auto-preserved on 30s refresh.
  Chips auto-submit on click; selects auto-submit on change.

Run standalone:
    python dashboard.py
"""

import base64
import hmac
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone

from flask import Flask, request, Response

from db import DB_PATH

app   = Flask(__name__)
PORT  = int(os.environ.get('KRONOS_DASHBOARD_PORT', 8050))
PAPER = os.environ.get('KRONOS_PAPER_MODE', 'true').lower() == 'true'
PHASE = os.environ.get('KRONOS_PHASE', 'pre_live')
START = float(os.environ.get('KRONOS_STARTING_CAPITAL_INR', 100000.0))

PAGE_SIZE     = 25    # trade history rows per page
SIG_PAGE_SIZE = 50    # signals explorer rows per page

# Minimum |actual_return_pct| for a signal to be counted as "correct".
# A direction win that doesn't clear round-trip fees (~maker+taker+GST ≈ 0.165%)
# is economically wrong even if the price moved the right way.
_HIT_THR = 0.15   # %

# Ordered list of known model sources — drives filter chips, cards, analysis views.
_MODEL_OPTS = [
    ('custom',         'Custom ⊘', 'b-gold',    '#7a5200'),   # halted 2026-06-05
    ('kronos-mini',    'Mini 1H',  'b-blue',    '#0747a6'),
    ('kronos-base',    'Base 1H',  'b-purple',  '#403294'),
    ('kronos-mini-4h', 'Mini 4H',  'b-teal',    '#087f5b'),
    ('kronos-base-4h', 'Base 4H ★','b-teal-dk', '#2b8a3e'),   # benchmark model
]

# Chart.js line colours for equity curves.
_MODEL_CHART_COLORS = {
    'custom':         '#f0c030',
    'kronos-mini':    '#4c9aff',
    'kronos-base':    '#998dd9',
    'kronos-mini-4h': '#36b37e',
    'kronos-base-4h': '#4caf50',
}

# Confidence calibration bands.
_CONF_BANDS  = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
_BAND_LABELS = ['0.0–0.2', '0.2–0.4', '0.4–0.6', '0.6–0.8', '0.8–1.0']


# ── HTTP Basic Auth ────────────────────────────────────────────────────────────
_DASH_USER = os.environ.get('KRONOS_DASHBOARD_USER', '').strip()
_DASH_PASS = os.environ.get('KRONOS_DASHBOARD_PASS', '').strip()
_AUTH_ON   = bool(_DASH_USER and _DASH_PASS)

def _auth_required() -> Response:
    return Response(
        'Kronos Dashboard — authentication required.',
        401,
        {'WWW-Authenticate': 'Basic realm="Kronos"', 'Cache-Control': 'no-store'},
    )

@app.before_request
def _require_auth():
    if not _AUTH_ON:
        return
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Basic '):
        return _auth_required()
    try:
        decoded  = base64.b64decode(auth_header[6:]).decode('utf-8', errors='replace')
        username, _, password = decoded.partition(':')
    except Exception:
        return _auth_required()
    user_ok = hmac.compare_digest(username.encode(), _DASH_USER.encode())
    pass_ok = hmac.compare_digest(password.encode(), _DASH_PASS.encode())
    if not (user_ok and pass_ok):
        return _auth_required()


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

def _parse_horizon_secs(horizon: str) -> int:
    try:
        if horizon and horizon.endswith('h'):
            return int(horizon[:-1]) * 3600
    except (ValueError, AttributeError):
        pass
    return 0

def _maturity(signal_ts, horizon, status: str) -> str:
    try:
        hz_secs  = _parse_horizon_secs(str(horizon or ''))
        if hz_secs == 0:
            return '<span style="color:#97a0af">—</span>'
        mature_at = int(signal_ts) + hz_secs
        remaining = mature_at - int(time.time())
        if remaining <= 0:
            elapsed_s = -remaining
            h, m = elapsed_s // 3600, (elapsed_s % 3600) // 60
            ago = f'{h}h {m}m' if h else f'{m}m'
            return f'<span style="color:#97a0af;font-size:.78rem">&#10003; {ago} ago</span>'
        h, m = remaining // 3600, (remaining % 3600) // 60
        label = f'{h}h {m}m' if h else f'{m}m'
        if remaining < 3600:
            return f'<span style="color:#ff8b00;font-weight:600">{label}</span>'
        return label
    except:
        return '<span style="color:#97a0af">—</span>'

def _gain(v):
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
    m = {'custom':         ('b-gold',    'Custom'),
         'kronos-mini':    ('b-blue',    'Mini 1H'),
         'kronos-base':    ('b-purple',  'Base 1H'),
         'kronos-mini-4h': ('b-teal',    'Mini 4H'),
         'kronos-base-4h': ('b-teal-dk', 'Base 4H')}
    cls, lbl = m.get(str(n).lower(), ('b-neu', n))
    return f'<span class="badge {cls}">{lbl}</span>'


# ── Filter helpers ─────────────────────────────────────────────────────────────

def _get_filters() -> dict:
    """Parse and sanitise filter params from request.args."""
    valid_models = {v for v, *_ in _MODEL_OPTS}
    models    = [m for m in request.args.getlist('model')  if m in valid_models]
    symbols   = [s for s in request.args.getlist('symbol')
                 if s and s == s.upper() and s.isalpha()]
    direction = request.args.get('direction', '')
    if direction not in ('long', 'short'):
        direction = ''
    try:    days    = int(request.args.get('days',    30))
    except: days    = 30
    try:    regime  = int(request.args.get('regime',   0))
    except: regime  = 0
    try:    page    = max(0, int(request.args.get('page',    0)))
    except: page    = 0
    try:    sigpage = max(0, int(request.args.get('sigpage', 0)))
    except: sigpage = 0
    tab = request.args.get('tab', '')
    sig_status = request.args.get('sig_status', '')
    if sig_status not in ('hit', 'miss', 'pending', 'rej-hit', 'rej-miss', 'executed', 'rejected', 'expired'):
        sig_status = ''
    return dict(models=models, symbols=symbols, direction=direction,
                days=days, regime=regime, page=page, sigpage=sigpage, tab=tab,
                sig_status=sig_status)


def _trade_where(f: dict):
    """
    Build the WHERE clause fragment for trade queries.
    Trades are joined as 't', signals as 's'.
    'both' direction means no direction filter.
    regime=0 means all regimes (no filter); regime>0 filters to that version.
    """
    parts, params = [], []
    if f.get('models'):
        ph = ','.join('?' * len(f['models']))
        parts.append(f's.model_source IN ({ph})')
        params.extend(f['models'])
    if f.get('symbols'):
        ph = ','.join('?' * len(f['symbols']))
        parts.append(f't.symbol IN ({ph})')
        params.extend(f['symbols'])
    d = f.get('direction', 'both')
    if d and d not in ('both', ''):           # 'both' = no direction filter
        parts.append('t.direction = ?')
        params.append(d)
    if f.get('days', 0) > 0:
        parts.append('t.entry_timestamp >= ?')
        params.append(int(time.time()) - f['days'] * 86400)
    regime = f.get('regime', 5)
    if regime and regime > 0:                  # 0 = all regimes (no filter)
        parts.append('COALESCE(s.regime_version, 1) = ?')
        params.append(regime)
    return (' AND ' + ' AND '.join(parts)) if parts else '', params


def _signal_where(f: dict):
    """WHERE fragment for signal queries (no join aliases)."""
    parts, params = [], []
    if f.get('models'):
        ph = ','.join('?' * len(f['models']))
        parts.append(f'model_source IN ({ph})')
        params.extend(f['models'])
    if f.get('symbols'):
        ph = ','.join('?' * len(f['symbols']))
        parts.append(f'symbol IN ({ph})')
        params.extend(f['symbols'])
    d = f.get('direction', 'both')
    if d and d not in ('both', ''):
        parts.append('direction = ?')
        params.append(d)
    if f.get('days', 0) > 0:
        parts.append('signal_timestamp >= ?')
        params.append(int(time.time()) - f['days'] * 86400)
    regime = f.get('regime', 5)
    if regime and regime > 0:
        parts.append('COALESCE(regime_version, 1) = ?')
        params.append(regime)
    # Signal status filter — applied only in signal explorer queries
    ss = f.get('sig_status', '')
    thr = _HIT_THR
    if ss == 'hit':
        parts.append(f"(status='executed' AND actual_return_pct IS NOT NULL"
                     f" AND ((direction='long'  AND actual_return_pct > {thr})"
                     f"   OR (direction='short' AND actual_return_pct < -{thr})))")
    elif ss == 'miss':
        parts.append(f"(status='executed' AND actual_return_pct IS NOT NULL"
                     f" AND NOT ((direction='long'  AND actual_return_pct > {thr})"
                     f"       OR (direction='short' AND actual_return_pct < -{thr})))")
    elif ss == 'pending':
        parts.append("(status='executed' AND actual_return_pct IS NULL)")
    elif ss == 'rej-hit':
        parts.append(f"(status='rejected' AND actual_return_pct IS NOT NULL"
                     f" AND ((direction='long'  AND actual_return_pct > {thr})"
                     f"   OR (direction='short' AND actual_return_pct < -{thr})))")
    elif ss == 'rej-miss':
        parts.append(f"(status='rejected' AND actual_return_pct IS NOT NULL"
                     f" AND NOT ((direction='long'  AND actual_return_pct > {thr})"
                     f"       OR (direction='short' AND actual_return_pct < -{thr})))")
    elif ss in ('executed', 'rejected', 'expired'):
        parts.append("status = ?")
        params.append(ss)
    return (' AND ' + ' AND '.join(parts)) if parts else '', params


def _pos_where(f: dict):
    parts, params = [], []
    if f.get('models'):
        ph = ','.join('?' * len(f['models']))
        parts.append(f's.model_source IN ({ph})')
        params.extend(f['models'])
    if f.get('symbols'):
        ph = ','.join('?' * len(f['symbols']))
        parts.append(f'p.symbol IN ({ph})')
        params.extend(f['symbols'])
    return (' AND ' + ' AND '.join(parts)) if parts else '', params


def _filter_count(f: dict) -> int:
    n = len(f.get('models', [])) + len(f.get('symbols', []))
    if f.get('direction'):        n += 1
    if f.get('days', 30) != 30:  n += 1
    if f.get('regime', 5)  != 5: n += 1
    if f.get('sig_status'):       n += 1
    return n


def _url_with(f: dict, **overrides) -> str:
    m = {**f, **overrides}
    parts  = [f'model={v}'  for v in m.get('models',  [])]
    parts += [f'symbol={v}' for v in m.get('symbols', [])]
    if m.get('direction'):
        parts.append(f"direction={m['direction']}")
    parts.append(f"days={m.get('days', 30)}")
    parts.append(f"regime={m.get('regime', 5)}")
    if m.get('page', 0):
        parts.append(f"page={m['page']}")
    if m.get('sigpage', 0):
        parts.append(f"sigpage={m['sigpage']}")
    if m.get('tab'):
        parts.append(f"tab={m['tab']}")
    if m.get('sig_status'):
        parts.append(f"sig_status={m['sig_status']}")
    return '/?' + '&'.join(parts)


def _active_filter_desc(f: dict) -> str:
    model_map = {v: lbl for v, lbl, *_ in _MODEL_OPTS}
    parts = []
    if f.get('models'):
        parts.append('Model: ' + ', '.join(model_map.get(m, m) for m in f['models']))
    if f.get('symbols'):
        parts.append('Symbol: ' + ', '.join(s.replace('USD', '') for s in f['symbols']))
    if f.get('direction'):
        parts.append(f"Dir: {f['direction'].capitalize()}")
    if f.get('days', 30) != 30:
        parts.append('All time' if f.get('days') == 0 else f"Last {f['days']}d")
    if f.get('regime', 5) != 5:
        parts.append('All regimes' if f.get('regime') == 0 else f"Regime v{f['regime']}")
    if f.get('sig_status'):
        _ss_labels = {
            'hit':     'Executed — Correct',
            'miss':    'Executed — Wrong',
            'pending': 'Pending',
            'rej-hit': 'Rejected — Correct',
            'rej-miss':'Rejected — Wrong',
            'executed':'Executed (all)',
            'rejected':'Rejected (all)',
            'expired': 'Expired',
        }
        parts.append('Signals: ' + _ss_labels.get(f['sig_status'], f['sig_status'].capitalize()))
    return ' &nbsp;·&nbsp; '.join(parts)


def _make_notice(f: dict) -> str:
    fc = _filter_count(f)
    if fc:
        desc = _active_filter_desc(f)
        return (f'<div class="flt-notice">'
                f'<span><strong>Filters active:</strong> &nbsp;{desc}</span>'
                f'<a href="/">&#10005; Clear all</a></div>')
    return ''


# ── Database ──────────────────────────────────────────────────────────────────

def _q(sql: str, p=()):
    try:
        c = sqlite3.connect(DB_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        rows = [dict(r) for r in c.execute(sql, tuple(p)).fetchall()]
        c.close()
        return rows
    except:
        return []


# ── Data layer ────────────────────────────────────────────────────────────────

def get_data(f: dict) -> dict:
    _MODEL_KEYS = [v for v, *_ in _MODEL_OPTS]

    # ── Per-model portfolio (regime-filtered) ─────────────────────────────────
    # Apply the same regime filter as the aggregate metrics so the capital cards
    # reflect the selected regime's P&L, not all-time cumulative.
    tw_m, tp_m = _trade_where(f)
    model_pf: dict = {}
    for _mk in _MODEL_KEYS:
        row = (_q("SELECT total_value, drawdown_pct FROM portfolio_snapshots"
                  " WHERE model_source=? AND regime_version=?"
                  " ORDER BY timestamp DESC LIMIT 1",
                  (_mk, f.get('regime', 5))) or [{}])[0]
        prow = (_q(f"""SELECT COALESCE(SUM(t.pnl_gross), 0) AS gross,
                              COALESCE(SUM(t.pnl_net),   0) AS net,
                              COUNT(*) AS n,
                              COALESCE(SUM(CASE WHEN t.pnl_gross>0 THEN 1 ELSE 0 END), 0) AS wins
                       FROM trades t JOIN signals s ON t.signal_id=s.id
                       WHERE t.status='closed' AND t.quality_flag IS NULL
                         AND s.model_source=? {tw_m}""",
                   (_mk,) + tuple(tp_m)) or [{}])[0]
        gross = _f(prow.get('gross'))
        total = START + gross
        model_pf[_mk] = {
            'total': total,
            'chg':   total - START,
            'dd':    _f(row.get('drawdown_pct')),
            'gross': gross,
            'net':   _f(prow.get('net')),
            'n':     int(_f(prow.get('n'))),
            'wins':  int(_f(prow.get('wins'))),
        }

    # ── Latest aggregate snapshot ──────────────────────────────────────────────
    pf = (_q("SELECT * FROM portfolio_snapshots"
             " WHERE model_source IS NULL AND regime_version=?"
             " ORDER BY timestamp DESC LIMIT 1",
             (f.get('regime', 5),)) or [{}])[0]

    # ── Aggregate metrics (filtered) ───────────────────────────────────────────
    tw, tp = _trade_where(f)
    closed = _q(f"""
        SELECT t.pnl_gross, t.pnl_net, t.tds_deducted
        FROM trades t LEFT JOIN signals s ON t.signal_id = s.id
        WHERE t.status='closed' AND t.quality_flag IS NULL {tw}
    """, tp)
    gross  = sum(_f(r['pnl_gross'])    for r in closed)
    net    = sum(_f(r['pnl_net'])      for r in closed)
    tds    = sum(_f(r['tds_deducted']) for r in closed)
    n      = len(closed)
    wins   = sum(1 for r in closed if _f(r['pnl_gross']) > 0)
    losses = n - wins
    wr     = wins / n * 100 if n else 0.0

    flagged_trades = (_q("SELECT COUNT(*) AS c FROM trades"
                         " WHERE quality_flag IS NOT NULL") or [{'c': 0}])[0]['c']
    dd_row = _q("SELECT MAX(drawdown_pct) AS v FROM portfolio_snapshots"
                " WHERE model_source IS NULL AND regime_version=?",
                (f.get('regime', 5),))
    max_dd = _f(dd_row[0]['v']) if dd_row else 0.0

    # ── Open positions (filtered) ─────────────────────────────────────────────
    pw, pp = _pos_where(f)
    positions = _q(f"""
        SELECT p.symbol, p.direction, p.entry_price, p.current_price,
               p.size_contracts, p.notional_value, p.unrealised_pnl,
               p.entry_timestamp, p.max_hold_until, p.leverage,
               p.stop_loss_price, p.take_profit_price,
               p.running_high, p.running_low, s.model_source
        FROM positions p
        LEFT JOIN trades t  ON t.id  = p.trade_id
        LEFT JOIN signals s ON s.id  = t.signal_id
        WHERE p.status IN ('open','closing') {pw}
        ORDER BY p.entry_timestamp DESC
    """, pp)

    # ── Trade history (filtered + paginated) ───────────────────────────────────
    offset = f['page'] * PAGE_SIZE
    history_raw = _q(f"""
        SELECT t.symbol, t.direction, t.entry_price, t.exit_price,
               t.size_contracts, t.pnl_gross, t.pnl_net, t.tds_deducted,
               t.exit_reason, t.entry_timestamp, t.exit_timestamp,
               s.confidence, s.predicted_return_pct, s.actual_return_pct,
               s.model_source, t.peak_price, t.trough_price
        FROM trades t LEFT JOIN signals s ON t.signal_id = s.id
        WHERE t.status='closed' AND t.quality_flag IS NULL {tw}
        ORDER BY t.exit_timestamp DESC
        LIMIT ? OFFSET ?
    """, tuple(tp) + (PAGE_SIZE + 1, offset))
    has_next = len(history_raw) > PAGE_SIZE
    history  = history_raw[:PAGE_SIZE]

    # ── Signal pipeline (filtered, last 50) ────────────────────────────────────
    sw, sp = _signal_where(f)
    pipeline = _q(f"""
        SELECT id, symbol, direction, confidence, horizon, status,
               rejection_reason, predicted_return_pct, actual_return_pct,
               signal_timestamp, quality_flag, model_source
        FROM signals
        WHERE 1=1 {sw}
        ORDER BY signal_timestamp DESC LIMIT 50
    """, sp)

    # ── Ops: model accuracy ───────────────────────────────────────────────────
    _MODEL_HZ = {
        'custom':         (86400, '4h'),
        'kronos-mini':    (21600, '1h'),
        'kronos-base':    (21600, '1h'),
        'kronos-mini-4h': (86400, '4h'),
        'kronos-base-4h': (86400, '4h'),
    }

    def _acc_query(model_source):
        hz_secs, tf = _MODEL_HZ.get(model_source, (86400, '4h'))
        cutoff_q = int(time.time()) - hz_secs
        rows = _q(f"""
            SELECT s.direction, oe.close AS ec, ox.close AS xc
            FROM signals s
            LEFT JOIN ohlcv oe ON oe.symbol=s.symbol AND oe.timeframe='{tf}'
              AND oe.timestamp=(SELECT MAX(timestamp) FROM ohlcv
                  WHERE symbol=s.symbol AND timeframe='{tf}'
                    AND timestamp<=s.signal_timestamp)
            LEFT JOIN ohlcv ox ON ox.symbol=s.symbol AND ox.timeframe='{tf}'
              AND ox.timestamp=(SELECT MIN(timestamp) FROM ohlcv
                  WHERE symbol=s.symbol AND timeframe='{tf}'
                    AND timestamp>=s.signal_timestamp+{hz_secs})
            WHERE s.signal_timestamp < {cutoff_q}
              AND s.quality_flag IS NULL
              AND s.model_source='{model_source}'
              AND oe.close IS NOT NULL AND ox.close IS NOT NULL
        """)
        correct = sum(1 for r in rows
                      if (r['xc'] > r['ec']) == (r['direction'].lower() == 'long')
                      and _f(r['ec']) > 0)
        return correct, len(rows)

    def _pend_count(model_source):
        hz_secs, _ = _MODEL_HZ.get(model_source, (86400, '4h'))
        cutoff_q   = int(time.time()) - hz_secs
        v = (_q("SELECT COUNT(*) AS v FROM signals"
                " WHERE status NOT IN ('rejected','expired')"
                "   AND quality_flag IS NULL AND model_source=?"
                "   AND signal_timestamp>=?",
                (model_source, cutoff_q)) or [{'v': 0}])[0]['v']
        return int(_f(v))

    _ACC_MODELS = [
        ('kronos-mini',    'Mini 1H',  '6H'),
        ('kronos-base',    'Base 1H',  '6H'),
        ('kronos-mini-4h', 'Mini 4H',  '24H'),
        ('kronos-base-4h', 'Base 4H ★','24H'),
    ]
    accuracy = []
    for _ms, _lbl, _hz_lbl in _ACC_MODELS:
        _c, _t = _acc_query(_ms)
        accuracy.append({'name': _ms, 'label': _lbl, 'horizon': _hz_lbl,
                         'c': _c, 't': _t, 'pend': _pend_count(_ms)})

    # Funding rates
    funding = _q("""
        SELECT f.symbol, f.rate, f.timestamp FROM funding_rates f
        INNER JOIN (SELECT symbol, MAX(timestamp) mx FROM funding_rates GROUP BY symbol) l
               ON f.symbol=l.symbol AND f.timestamp=l.mx
        ORDER BY f.symbol
    """)

    # Symbols for filter bar
    sym_rows    = _q("SELECT DISTINCT symbol FROM signals WHERE symbol IS NOT NULL ORDER BY symbol")
    all_symbols = [r['symbol'] for r in sym_rows]

    # ══════════════════════════════════════════════════════════════════════════
    # Phase 2: Analysis tab data
    # ══════════════════════════════════════════════════════════════════════════

    # 1. Model × Asset matrix — trade win rates
    matrix_raw = _q(f"""
        SELECT COALESCE(s.model_source,'custom') ms, t.symbol,
               COUNT(*) n, SUM(CASE WHEN t.pnl_gross>0 THEN 1 ELSE 0 END) wins
        FROM trades t LEFT JOIN signals s ON t.signal_id=s.id
        WHERE t.status='closed' AND t.quality_flag IS NULL {tw}
        GROUP BY ms, t.symbol
    """, tp)
    matrix_wr = defaultdict(dict)
    for r in matrix_raw:
        ms = r['ms'] or 'custom'
        matrix_wr[ms][r['symbol']] = {'n': int(r['n']), 'wins': int(r['wins'])}

    # 2. Directional accuracy per (model, symbol) from resolved signals
    dir_acc_raw = _q(f"""
        SELECT COALESCE(model_source,'custom') ms, symbol, direction, actual_return_pct
        FROM signals
        WHERE actual_return_pct IS NOT NULL AND quality_flag IS NULL {sw}
    """, sp)
    matrix_acc = defaultdict(lambda: defaultdict(lambda: {'c': 0, 't': 0}))
    for r in dir_acc_raw:
        ms  = r['ms'] or 'custom'
        sym = r['symbol']
        direction  = str(r['direction'] or '').lower()
        _ar = _f(r['actual_return_pct'])
        actual_dir = 'long' if _ar > _HIT_THR else ('short' if _ar < -_HIT_THR else None)
        matrix_acc[ms][sym]['t'] += 1
        if actual_dir and actual_dir == direction:
            matrix_acc[ms][sym]['c'] += 1

    # Sorted list of all symbols seen in matrix data
    all_mx_syms = sorted(
        {r['symbol'] for r in matrix_raw} |
        {r['symbol'] for r in dir_acc_raw}
    )

    # 3. Equity curves — cumulative gross P&L per model
    eq_raw = _q(f"""
        SELECT COALESCE(s.model_source,'custom') ms,
               t.exit_timestamp ts, t.pnl_gross pnl
        FROM trades t LEFT JOIN signals s ON t.signal_id=s.id
        WHERE t.status='closed' AND t.quality_flag IS NULL {tw}
        ORDER BY t.exit_timestamp ASC
    """, tp)
    _eq_by = defaultdict(list)
    for r in eq_raw:
        _eq_by[r['ms']].append((int(r['ts']), _f(r['pnl'])))
    equity_chart: dict = {}
    for ms, pts in _eq_by.items():
        cum = 0.0
        series = []
        for ts, pnl in sorted(pts, key=lambda x: x[0]):
            cum += pnl
            series.append([datetime.fromtimestamp(ts, tz=timezone.utc)
                           .strftime('%d %b %H:%M'), round(cum, 2)])
        equity_chart[ms] = series

    # 4. Confidence calibration — accuracy per confidence band per model
    cal_raw = _q(f"""
        SELECT COALESCE(model_source,'custom') ms,
               direction, confidence, actual_return_pct
        FROM signals
        WHERE actual_return_pct IS NOT NULL AND quality_flag IS NULL
          AND confidence IS NOT NULL {sw}
    """, sp)
    # Pre-initialise all known models so table shows every column
    cal_data: dict = {mk: [{'c': 0, 't': 0} for _ in _CONF_BANDS]
                      for mk, *_ in _MODEL_OPTS}
    for r in cal_raw:
        ms   = r['ms'] or 'custom'
        conf = _f(r['confidence'])
        _ar  = _f(r['actual_return_pct'])
        actual_dir = 'long' if _ar > _HIT_THR else ('short' if _ar < -_HIT_THR else None)
        direction  = str(r['direction'] or '').lower()
        if ms not in cal_data:
            cal_data[ms] = [{'c': 0, 't': 0} for _ in _CONF_BANDS]
        for i, (lo, hi) in enumerate(_CONF_BANDS):
            if lo <= conf < hi:
                cal_data[ms][i]['t'] += 1
                if actual_dir and actual_dir == direction:
                    cal_data[ms][i]['c'] += 1
                break

    # 5. Rejection funnel per model (four GROUP BY queries, then pivot)
    def _funnel_count(extra_where: str) -> dict:
        rows = _q(f"""
            SELECT COALESCE(model_source,'custom') ms, COUNT(*) c
            FROM signals WHERE quality_flag IS NULL {extra_where}
            GROUP BY ms
        """)
        return {r['ms']: r['c'] for r in rows}

    def _trade_count(extra_where: str) -> dict:
        rows = _q(f"""
            SELECT COALESCE(s.model_source,'custom') ms, COUNT(*) c
            FROM trades t JOIN signals s ON t.signal_id=s.id
            WHERE t.quality_flag IS NULL {extra_where}
            GROUP BY ms
        """)
        return {r['ms']: r['c'] for r in rows}

    fg = _funnel_count('')
    fp = _funnel_count("AND status != 'rejected'")
    fe = _trade_count('')
    fw = _trade_count('AND t.pnl_gross > 0')

    funnel_data: dict = {}
    for mk, *_ in _MODEL_OPTS:
        funnel_data[mk] = {
            'gen':    fg.get(mk, 0),
            'passed': fp.get(mk, 0),
            'exec':   fe.get(mk, 0),
            'won':    fw.get(mk, 0),
        }

    # 6. Signals explorer (filtered + paginated)
    sig_offset = f.get('sigpage', 0) * SIG_PAGE_SIZE
    sig_raw = _q(f"""
        SELECT id, symbol, direction, confidence, horizon, status,
               rejection_reason, predicted_return_pct, actual_return_pct,
               signal_timestamp, quality_flag, model_source,
               COALESCE(regime_version, 1) regime_version
        FROM signals
        WHERE 1=1 {sw}
        ORDER BY signal_timestamp DESC
        LIMIT ? OFFSET ?
    """, tuple(sp) + (SIG_PAGE_SIZE + 1, sig_offset))
    has_next_sig = len(sig_raw) > SIG_PAGE_SIZE
    sigs_list    = sig_raw[:SIG_PAGE_SIZE]

    # 6b. Signal accuracy breakdown (base filters only — sig_status excluded so stats
    #     are always the full picture even when a chip filter narrows the table view)
    sw_base, sp_base = _signal_where({**f, 'sig_status': ''})
    _raw_acc = _q(f"""
        SELECT status, direction,
               CASE WHEN actual_return_pct IS NULL THEN 'unresolved'
                    WHEN (direction='long'  AND actual_return_pct >  {_HIT_THR})
                      OR (direction='short' AND actual_return_pct < -{_HIT_THR}) THEN 'correct'
                    ELSE 'wrong'
               END AS outcome,
               COUNT(*) AS n
        FROM signals
        WHERE quality_flag IS NULL {sw_base}
        GROUP BY status, direction, outcome
    """, tuple(sp_base))
    _sc: dict = defaultdict(int)
    for _r in _raw_acc:
        _sc[(str(_r['status']), str(_r['direction'] or ''), str(_r['outcome']))] += int(_r['n'])
    sig_stats = {
        # Executed
        'exec_corr_long':   _sc[('executed', 'long',  'correct')],
        'exec_corr_short':  _sc[('executed', 'short', 'correct')],
        'exec_wrong_long':  _sc[('executed', 'long',  'wrong')],
        'exec_wrong_short': _sc[('executed', 'short', 'wrong')],
        'exec_pend':        _sc[('executed', 'long',  'unresolved')]
                          + _sc[('executed', 'short', 'unresolved')],
        # Rejected
        'rej_corr_long':    _sc[('rejected', 'long',  'correct')],
        'rej_corr_short':   _sc[('rejected', 'short', 'correct')],
        'rej_wrong_long':   _sc[('rejected', 'long',  'wrong')],
        'rej_wrong_short':  _sc[('rejected', 'short', 'wrong')],
        'rej_unres':        _sc[('rejected', 'long',  'unresolved')]
                          + _sc[('rejected', 'short', 'unresolved')],
        # Expired / pending
        'expired': sum(_sc[(s, d, o)]
                       for s in ['expired']
                       for d in ['long', 'short']
                       for o in ['correct', 'wrong', 'unresolved']),
    }

    # 6c. Weekly directional accuracy trend — last 12 weeks, all resolved signals.
    # Applies model/direction/regime filters from the filter bar but uses a fixed
    # 12-week window so the trend is always visible regardless of the Period selector.
    _wt_parts  = ['actual_return_pct IS NOT NULL', 'quality_flag IS NULL',
                  f'signal_timestamp >= {int(time.time()) - 84 * 86400}']
    _wt_params = []
    if f.get('models'):
        ph = ','.join('?' * len(f['models']))
        _wt_parts.append(f"COALESCE(model_source,'custom') IN ({ph})")
        _wt_params.extend(f['models'])
    if f.get('direction') and f['direction'] not in ('both', ''):
        _wt_parts.append('direction = ?')
        _wt_params.append(f['direction'])
    _wt_regime = f.get('regime', 5)
    if _wt_regime and _wt_regime > 0:
        _wt_parts.append('COALESCE(regime_version, 1) = ?')
        _wt_params.append(_wt_regime)
    _wt_where = ' AND '.join(_wt_parts)
    _wt_raw = _q(f"""
        SELECT strftime('%Y-W%W', datetime(signal_timestamp, 'unixepoch')) AS week,
               COALESCE(model_source,'custom') AS ms,
               direction,
               SUM(CASE WHEN (direction='long'  AND actual_return_pct >  {_HIT_THR})
                            OR (direction='short' AND actual_return_pct < -{_HIT_THR})
                        THEN 1 ELSE 0 END) AS correct,
               COUNT(*) AS total
        FROM signals
        WHERE {_wt_where}
        GROUP BY week, ms, direction
        ORDER BY week ASC
    """, _wt_params)
    _wt_pivot: dict = defaultdict(dict)
    for _r in _wt_raw:
        _wt_pivot[_r['week']][(_r['ms'], _r['direction'])] = {
            'c': int(_r['correct']), 't': int(_r['total'])
        }
    weekly_trend = dict(_wt_pivot)

    # 7. Events log (last 30)
    events_log = _q("""
        SELECT created_at, event_type, message
        FROM events
        ORDER BY created_at DESC LIMIT 30
    """)

    # 8. 24h activity summary
    activity = _q("""
        SELECT event_type, COUNT(*) cnt, MAX(created_at) last_ts
        FROM events
        WHERE created_at >= ?
        GROUP BY event_type
        ORDER BY cnt DESC LIMIT 12
    """, (int(time.time()) - 86400,))

    # 9. Generator health — per-model signal counts + rejection reasons (last 24H)
    _gh_ts = int(time.time())
    _gh_24h = {r['model']: r for r in _q("""
        SELECT COALESCE(model_source,'custom') AS model,
               COUNT(*) AS total,
               SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) AS rejected,
               SUM(CASE WHEN status IN ('executed','approved','pending') THEN 1 ELSE 0 END) AS generated,
               MAX(signal_timestamp) AS last_ts
        FROM signals
        WHERE signal_timestamp > ?
        GROUP BY COALESCE(model_source,'custom')
    """, (_gh_ts - 86400,))}
    _gh_ever = {r['model']: r['last_ts'] for r in _q("""
        SELECT COALESCE(model_source,'custom') AS model,
               MAX(signal_timestamp) AS last_ts
        FROM signals
        GROUP BY COALESCE(model_source,'custom')
    """)}
    _gh_rr_raw = _q("""
        SELECT COALESCE(model_source,'custom') AS model,
               rejection_reason,
               COUNT(*) AS cnt
        FROM signals
        WHERE status='rejected' AND signal_timestamp > ?
        GROUP BY COALESCE(model_source,'custom'), rejection_reason
        ORDER BY cnt DESC
    """, (_gh_ts - 86400,))
    _gh_reasons: dict = defaultdict(list)
    for _r in _gh_rr_raw:
        _gh_reasons[_r['model']].append((_r['rejection_reason'] or '', int(_r['cnt'])))
    _gh_logs = {db_key: _read_gen_log(log_path, cycle_secs)
                for db_key, _, cycle_secs, _, _, log_path in _GH_MODELS}
    gen_health = {
        'now_ts':     _gh_ts,
        'mh_24h':     _gh_24h,
        'mh_ever':    _gh_ever,
        'mh_reasons': dict(_gh_reasons),
        'mh_logs':    _gh_logs,
    }

    return dict(
        pf=pf, model_pf=model_pf,
        gross=gross, net=net, tds=tds,
        n=n, wins=wins, losses=losses, wr=wr, max_dd=max_dd,
        positions=positions,
        history=history, has_next=has_next, has_prev=(f['page'] > 0),
        pipeline=pipeline, accuracy=accuracy, funding=funding,
        flagged_trades=flagged_trades, all_symbols=all_symbols,
        # Phase 2
        matrix_wr=dict(matrix_wr),
        matrix_acc={k: dict(v) for k, v in matrix_acc.items()},
        all_mx_syms=all_mx_syms,
        equity_chart=equity_chart,
        cal_data=cal_data,
        funnel_data=funnel_data,
        sigs_list=sigs_list,
        has_next_sig=has_next_sig,
        has_prev_sig=(f.get('sigpage', 0) > 0),
        sig_stats=sig_stats,
        weekly_trend=weekly_trend,
        events_log=events_log,
        activity=activity,
        gen_health=gen_health,
        ts=int(time.time()),
    )


# ── CSS ───────────────────────────────────────────────────────────────────────

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
.pill-phase {background:#f4f5f7;color:#5e6c84;border:1px solid #dfe1e6}
.pill-regime{background:#e3fcef;color:#006644;border:1px solid #57d9a3}

/* ── Tabs ── */
.tabs{background:#fff;border-bottom:2px solid #dfe1e6;padding:0 20px;display:flex;gap:0}
.tab-btn{background:none;border:none;padding:10px 18px;font-size:.83rem;font-weight:600;
         color:#6b778c;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:.15s}
.tab-btn:hover{color:#172b4d}
.tab-btn.active{color:#0052cc;border-bottom-color:#0052cc}
.tab-pane{display:none;padding:20px}
.tab-pane.active{display:block}

/* ── Filter bar ── */
.filter-bar{background:#fff;border-bottom:1px solid #dfe1e6;
            padding:7px 16px;display:flex;align-items:center;flex-wrap:wrap;gap:10px}
.flt-group{display:flex;align-items:center;gap:4px;flex-wrap:wrap}
.flt-label{font-size:.66rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
           color:#5e6c84;white-space:nowrap;margin-right:2px}
.flt-div{width:1px;height:18px;background:#dfe1e6;flex-shrink:0;align-self:center}
.chip{display:inline-flex;align-items:center;gap:3px;padding:3px 9px;border-radius:20px;
      border:1px solid #dfe1e6;font-size:.72rem;font-weight:600;cursor:pointer;
      transition:background .1s,border-color .1s;user-select:none;white-space:nowrap;
      color:#5e6c84;background:#fff}
.chip:hover{background:#f4f5f7}
.chip input{display:none}
.chip.on{background:#deebff;border-color:#4c9aff;color:#0052cc}
.chip-custom      {opacity:.55;text-decoration:line-through}
.chip-custom.on   {background:#fffae6;border-color:#f0c030;color:#7a5200;opacity:.55;text-decoration:line-through}
.chip-mini1h.on   {background:#deebff;border-color:#4c9aff;color:#0747a6}
.chip-base1h.on   {background:#eae6ff;border-color:#998dd9;color:#403294}
.chip-mini4h.on   {background:#e6fcf5;border-color:#36b37e;color:#087f5b}
.chip-base4h.on   {background:#d3f9d8;border-color:#4caf50;color:#2b8a3e}
.chip-long.on {background:#e3fcef;border-color:#36b37e;color:#006644}
.chip-short.on{background:#ffebe6;border-color:#ff5630;color:#bf2600}
.flt-sel{padding:3px 6px;border:1px solid #dfe1e6;border-radius:4px;
         font-size:.75rem;color:#172b4d;background:#fff;cursor:pointer}
.flt-apply{padding:4px 12px;background:#0052cc;color:#fff;border:none;border-radius:4px;
           font-size:.74rem;font-weight:600;cursor:pointer}
.flt-apply:hover{background:#0065ff}
.flt-clear{font-size:.72rem;color:#6b778c;text-decoration:none;padding:4px 6px}
.flt-clear:hover{color:#172b4d}
.flt-notice{background:#deebff;border:1px solid #4c9aff;border-radius:6px;
            padding:7px 14px;margin-bottom:12px;font-size:.78rem;color:#0052cc;
            display:flex;align-items:center;justify-content:space-between}
.flt-notice a{color:#0052cc;font-weight:600;text-decoration:none;font-size:.72rem}

/* ── Cards ── */
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
@media(max-width:900px){.cards{grid-template-columns:repeat(2,1fr)}}
@media(max-width:500px){.cards{grid-template-columns:1fr}}
.card{background:#fff;border:1px solid #dfe1e6;border-radius:8px;
      padding:16px 18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
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
.b-teal  {background:#e6fcf5;color:#087f5b;border:1px solid #96f2d7}
.b-teal-dk{background:#d3f9d8;color:#2b8a3e;border:1px solid #8ce99a}

/* ── Layout helpers ── */
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:18px}
@media(max-width:900px){.two-col{grid-template-columns:1fr}}
.tag{font-size:.7rem;color:#6b778c}
.wr-bar{height:6px;border-radius:3px;background:#e3fcef;overflow:hidden;margin-top:5px;width:100%}
.wr-fill{height:100%;border-radius:3px}

/* ── Pagination ── */
.pagination{display:flex;align-items:center;gap:10px;padding:10px 16px;
            font-size:.78rem;color:#5e6c84;border-top:1px solid #f0f1f3;flex-wrap:wrap}
.page-btn{padding:4px 11px;border:1px solid #dfe1e6;border-radius:4px;
          background:#fff;color:#172b4d;font-size:.75rem;text-decoration:none}
.page-btn:hover{background:#f4f5f7}
.page-info{font-size:.74rem;color:#6b778c}

/* ── Trade history outcome cell ── */
.outcome-hit {color:#006644;font-weight:600}
.outcome-miss{color:#bf2600;font-weight:600}

/* ── Analysis: matrix cells ── */
.mx-cell{text-align:center;padding:6px 8px!important;min-width:80px}
.mx-hit  {background:#e3fcef}
.mx-warn {background:#fffae6}
.mx-miss {background:#ffebe6}
.mx-none {background:#fafbfc;color:#97a0af}
.mx-val  {font-weight:700;font-size:.85rem}
.mx-sub  {font-size:.65rem;color:#6b778c;margin-top:1px}

/* ── Analysis: confidence calibration inline bar ── */
.cal-bar-wrap{display:inline-flex;align-items:center;gap:5px;vertical-align:middle}
.cal-bar-bg{height:7px;width:60px;background:#f4f5f7;border-radius:4px;overflow:hidden;display:inline-block}
.cal-bar-fill{height:100%;border-radius:4px}

/* ── Analysis: funnel ── */
.funnel-row{display:flex;align-items:center;gap:0;font-size:.78rem;flex-wrap:wrap;padding:6px 0}
.funnel-step{display:inline-flex;flex-direction:column;align-items:center;
             padding:4px 12px;border-radius:4px;min-width:80px;text-align:center}
.funnel-arrow{color:#97a0af;font-size:1rem;padding:0 2px}
.funnel-count{font-size:1.05rem;font-weight:700;line-height:1}
.funnel-label{font-size:.62rem;text-transform:uppercase;letter-spacing:.06em;
              color:#6b778c;margin-top:2px}
.funnel-pct{font-size:.7rem;color:#6b778c}

/* ── Signals explorer row colours ── */
.sig-hit     {background:#f0fff4!important}
.sig-hit  td {background:#f0fff4!important}
.sig-miss    {background:#fff5f5!important}
.sig-miss td {background:#fff5f5!important}
.sig-rej     {opacity:.45}
.sig-pending {background:#f0f4ff!important}
.sig-pending td{background:#f0f4ff!important}

/* ── Signal accuracy bar ── */
.sig-acc-bar{display:flex;gap:8px;padding:12px 16px;background:#f7f8fa;
             border-bottom:1px solid #ebecf0;flex-wrap:wrap}
.sig-acc-card{display:flex;flex-direction:column;gap:4px;background:#fff;
              border:1.5px solid #dfe1e6;border-radius:8px;padding:10px 14px;
              flex:1;min-width:130px;text-decoration:none;color:inherit;
              transition:box-shadow .12s,border-color .12s;cursor:pointer}
.sig-acc-card:hover{box-shadow:0 2px 8px rgba(0,0,0,.08);text-decoration:none;color:inherit}
.sig-acc-card.acc-hit  {border-color:#36b37e}
.sig-acc-card.acc-miss {border-color:#ff5630}
.sig-acc-card.acc-rh   {border-color:#00875a;border-style:dashed}
.sig-acc-card.acc-rm   {border-color:#de350b;border-style:dashed}
.sig-acc-card.acc-other{border-color:#dfe1e6;cursor:default}
.acc-head{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em;color:#5e6c84}
.acc-head.c-hit {color:#006644}
.acc-head.c-miss{color:#bf2600}
.acc-pct{font-size:1.35rem;font-weight:800;line-height:1.1}
.acc-pct.c-hit {color:#006644}
.acc-pct.c-miss{color:#bf2600}
.acc-pct.c-neu {color:#42526e}
.acc-total{font-size:.68rem;color:#97a0af}
.acc-dir{display:flex;justify-content:space-between;font-size:.7rem;color:#5e6c84;
         border-top:1px solid #f0f1f3;padding-top:3px;margin-top:2px}
.acc-dir span:last-child{font-weight:600;color:#172b4d}
.acc-other-row{font-size:.75rem;color:#42526e;display:flex;justify-content:space-between}
.acc-other-row span:last-child{font-weight:600}

/* ── Signal explorer status chips ── */
.sig-chips{display:flex;flex-wrap:wrap;gap:6px;padding:10px 16px 4px;border-bottom:1px solid #f0f1f3}
.sig-chip{display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:20px;
          font-size:.72rem;font-weight:600;border:1px solid #dfe1e6;color:#5e6c84;
          background:#fff;text-decoration:none;cursor:pointer;transition:background .12s}
.sig-chip:hover{background:#f4f5f7;text-decoration:none;color:#172b4d}
.sig-chip.sc-all{border-color:#dfe1e6}
.sig-chip.sc-all.on{background:#172b4d;border-color:#172b4d;color:#fff}
.sig-chip.sc-hit.on{background:#e3fcef;border-color:#36b37e;color:#006644}
.sig-chip.sc-miss.on{background:#ffebe6;border-color:#ff5630;color:#bf2600}
.sig-chip.sc-pending.on{background:#e6edff;border-color:#4c9aff;color:#0052cc}
.sig-chip.sc-rej-hit.on{background:#e3fcef;border-color:#36b37e;color:#006644;opacity:.7}
.sig-chip.sc-rej-miss.on{background:#ffebe6;border-color:#ff5630;color:#bf2600;opacity:.7}
.sig-chip.sc-expired.on{background:#f4f5f7;border-color:#97a0af;color:#42526e}

/* ── Events log ── */
.ev-error td{color:#bf2600}
.ev-warn  td{color:#974f0c}
.ev-info  td{color:#0052cc}

/* ── Chart wrapper ── */
.chart-wrap{padding:16px;height:260px;position:relative}

.footer{text-align:center;padding:14px;font-size:.7rem;color:#97a0af}

/* ── Generator health cards ── */
.mh-card{background:#fff;border-radius:6px;padding:10px 12px;border:1px solid #dfe1e6}
.mh-status{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px}
.mh-name{font-size:.78rem;font-weight:600;color:#172b4d;margin-bottom:4px}
.mh-detail{font-size:.71rem;color:#6b778c;margin-bottom:3px;line-height:1.4}
.mh-reasons{display:flex;flex-wrap:wrap;gap:3px;margin-top:3px}
.mh-rr-market  {font-size:.65rem;padding:2px 6px;border-radius:3px;background:#fffae6;color:#7a5200;border:1px solid #ffe380;display:inline-block}
.mh-rr-halted  {font-size:.65rem;padding:2px 6px;border-radius:3px;background:#ffebe6;color:#bf2600;border:1px solid #ffbdad;display:inline-block}
.mh-rr-system  {font-size:.65rem;padding:2px 6px;border-radius:3px;background:#ffe2de;color:#bf2600;border:1px solid #ff7452;display:inline-block}
.mh-rr-filter  {font-size:.65rem;padding:2px 6px;border-radius:3px;background:#f4f5f7;color:#5e6c84;border:1px solid #dfe1e6;display:inline-block}
.mh-rr-disabled{font-size:.65rem;padding:2px 6px;border-radius:3px;background:#f4f5f7;color:#97a0af;border:1px solid #dfe1e6;display:inline-block}
</style>
"""


# ── JavaScript ────────────────────────────────────────────────────────────────

def _build_js(equity_json: str) -> str:
    return f"""
<script>
// ── Tab management ────────────────────────────────────────────────────────────
function showTab(name) {{
    document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('pane-' + name).classList.add('active');
    document.querySelector('[data-tab="' + name + '"]').classList.add('active');
    localStorage.setItem('kronos-tab', name);
    // Keep filter-form's hidden tab field in sync so form submissions preserve the
    // active tab even when the URL was changed by history.replaceState.
    var fltTab = document.getElementById('flt-tab');
    if (fltTab) fltTab.value = name;
    try {{
        var u = new URL(window.location.href);
        u.searchParams.set('tab', name);
        history.replaceState(null, '', u.toString());
    }} catch(e) {{}}
    // Lazy-init chart when Analysis tab becomes visible
    if (name === 'analysis' && !window._chartInited) {{
        window._chartInited = true;
        setTimeout(initEquityChart, 50);
    }}
}}

// ── Equity curve chart ────────────────────────────────────────────────────────
var _EQUITY_DATA = {equity_json};

var _MODEL_COLORS = {{
    'custom':         '#f0c030',
    'kronos-mini':    '#4c9aff',
    'kronos-base':    '#998dd9',
    'kronos-mini-4h': '#36b37e',
    'kronos-base-4h': '#4caf50'
}};
var _MODEL_LABELS = {{
    'custom':         'Custom',
    'kronos-mini':    'Mini 1H',
    'kronos-base':    'Base 1H',
    'kronos-mini-4h': 'Mini 4H',
    'kronos-base-4h': 'Base 4H'
}};

function initEquityChart() {{
    var canvas = document.getElementById('equity-chart');
    if (!canvas || typeof Chart === 'undefined') return;
    var models = Object.keys(_EQUITY_DATA);
    if (models.length === 0) {{
        canvas.parentElement.innerHTML =
          '<div class="empty">No closed trades yet — equity curve will appear here.</div>';
        return;
    }}
    var datasets = models.map(function(m) {{
        var pts = _EQUITY_DATA[m];
        return {{
            label: _MODEL_LABELS[m] || m,
            data: pts.map(function(p) {{ return {{x: p[0], y: p[1]}}; }}),
            borderColor: _MODEL_COLORS[m] || '#999',
            backgroundColor: 'transparent',
            tension: 0.3,
            pointRadius: pts.length > 60 ? 0 : 3,
            borderWidth: 2,
        }};
    }});
    new Chart(canvas, {{
        type: 'line',
        data: {{ datasets: datasets }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{ mode: 'index', intersect: false }},
            scales: {{
                x: {{ type: 'category',
                      ticks: {{ maxRotation: 40, maxTicksLimit: 10,
                               font: {{ size: 10 }} }} }},
                y: {{ ticks: {{
                    font: {{ size: 10 }},
                    callback: function(v) {{
                        return (v >= 0 ? '+' : '') + '\\u20b9' +
                               Math.abs(v).toLocaleString('en-IN', {{minimumFractionDigits:0}});
                    }}
                }}}}
            }},
            plugins: {{
                legend: {{ position: 'top', labels: {{ font: {{ size: 11 }} }} }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            var v = ctx.parsed.y;
                            return ctx.dataset.label + ': ' +
                                   (v >= 0 ? '+' : '') + '\\u20b9' +
                                   Math.abs(v).toLocaleString('en-IN', {{minimumFractionDigits:2}});
                        }}
                    }}
                }}
            }}
        }}
    }});
}}

// ── On load ───────────────────────────────────────────────────────────────────
window.onload = function() {{
    var urlTab = '';
    try {{ urlTab = new URLSearchParams(window.location.search).get('tab') || ''; }} catch(e) {{}}
    var activeTab = urlTab || localStorage.getItem('kronos-tab') || 'summary';
    showTab(activeTab);

    document.querySelectorAll('.chip input[type=checkbox]').forEach(function(inp) {{
        if (inp.checked) inp.closest('.chip').classList.add('on');
        inp.addEventListener('change', function() {{
            inp.closest('.chip').classList.toggle('on', inp.checked);
            clearTimeout(window._fltT);
            window._fltT = setTimeout(function() {{
                document.getElementById('flt').submit();
            }}, 300);
        }});
    }});

    document.querySelectorAll('.chip input[type=radio]').forEach(function(inp) {{
        if (inp.checked) inp.closest('.chip').classList.add('on');
        inp.addEventListener('change', function() {{
            var name = inp.getAttribute('name');
            document.querySelectorAll('.chip input[name="' + name + '"]').forEach(function(r) {{
                r.closest('.chip').classList.toggle('on', r.checked);
            }});
            document.getElementById('flt').submit();
        }});
    }});

    document.querySelectorAll('#flt select').forEach(function(sel) {{
        sel.addEventListener('change', function() {{
            document.getElementById('flt').submit();
        }});
    }});

    document.getElementById('flt').addEventListener('submit', function() {{
        document.getElementById('flt-tab').value =
            localStorage.getItem('kronos-tab') || 'summary';
    }});

    // Auto-refresh every 30 s — use JS instead of <meta http-equiv="refresh"> so
    // that window.location.href is read at fire-time, after any history.replaceState
    // calls (tab switches).  Meta refresh uses the document's original URL and would
    // silently drop the user's current tab selection.
    setTimeout(function() {{
        window.location.assign(window.location.href);
    }}, 30000);
}};
</script>
"""


# ── Filter bar renderer ────────────────────────────────────────────────────────

def _render_filter_bar(f: dict, all_symbols: list) -> str:
    _chip_cls = {
        'custom':         'chip-custom',
        'kronos-mini':    'chip-mini1h',
        'kronos-base':    'chip-base1h',
        'kronos-mini-4h': 'chip-mini4h',
        'kronos-base-4h': 'chip-base4h',
    }
    model_chips = ''
    for val, lbl, *_ in _MODEL_OPTS:
        on  = ' on' if val in f['models'] else ''
        cls = _chip_cls.get(val, '')
        chk = ' checked' if val in f['models'] else ''
        model_chips += (f'<label class="chip {cls}{on}">'
                        f'<input type="checkbox" name="model" value="{val}"{chk}>'
                        f'{lbl}</label>')

    sym_chips = ''
    for sym in all_symbols:
        on  = ' on' if sym in f['symbols'] else ''
        chk = ' checked' if sym in f['symbols'] else ''
        sym_chips += (f'<label class="chip{on}">'
                      f'<input type="checkbox" name="symbol" value="{sym}"{chk}>'
                      f'{sym.replace("USD","")}</label>')

    dir_chips = ''
    for val, lbl in [('', 'Both'), ('long', '▲ Long'), ('short', '▼ Short')]:
        on  = ' on' if f['direction'] == val else ''
        cls = ' chip-long' if val == 'long' else (' chip-short' if val == 'short' else '')
        chk = ' checked' if f['direction'] == val else ''
        dir_chips += (f'<label class="chip{cls}{on}">'
                      f'<input type="radio" name="direction" value="{val}"{chk}>'
                      f'{lbl}</label>')

    day_opts = ''
    for v, lbl in [('7','Last 7d'),('14','Last 14d'),('30','Last 30d'),('0','All time')]:
        sel = ' selected' if str(f['days']) == v else ''
        day_opts += f'<option value="{v}"{sel}>{lbl}</option>'

    reg_opts = ''
    for v, lbl in [('5','Regime v5 (current)'),('4','Regime v4 (archive)'),('3','Regime v3 (archive)'),('2','Regime v2 (archive)'),('1','Regime v1 (archive)'),('0','All regimes')]:
        sel = ' selected' if str(f['regime']) == v else ''
        reg_opts += f'<option value="{v}"{sel}>{lbl}</option>'

    fc    = _filter_count(f)
    badge = (f'&nbsp;<span style="background:#ff5630;color:#fff;border-radius:10px;'
             f'padding:1px 6px;font-size:.62rem;font-weight:700">{fc}</span>') if fc else ''

    return f"""
<form method="GET" action="/" id="flt">
  <input type="hidden" name="tab" id="flt-tab" value="{f.get('tab','') or ''}">
  <div class="filter-bar">
    <div class="flt-group">
      <span class="flt-label">Model{badge}</span>
      {model_chips}
    </div>
    <div class="flt-div"></div>
    <div class="flt-group">
      <span class="flt-label">Symbol</span>
      {sym_chips}
    </div>
    <div class="flt-div"></div>
    <div class="flt-group">
      <span class="flt-label">Direction</span>
      {dir_chips}
    </div>
    <div class="flt-div"></div>
    <div class="flt-group">
      <span class="flt-label">Period</span>
      <select name="days" class="flt-sel">{day_opts}</select>
    </div>
    <div class="flt-group">
      <span class="flt-label">Regime</span>
      <select name="regime" class="flt-sel">{reg_opts}</select>
    </div>
    <div class="flt-group" style="margin-left:auto;gap:6px">
      <button type="submit" class="flt-apply">Apply</button>
      <a href="/" class="flt-clear">&#10005; Clear</a>
    </div>
  </div>
</form>"""


# ── Tab 1: Summary pane ───────────────────────────────────────────────────────

# ── Generator health ──────────────────────────────────────────────────────────

_GH_MODELS = [
    # (db_key,          label,                    cycle_secs, code_disabled, disabled_reason,               log_err)
    ('kronos-mini',    'M13 · kronos-mini · 1H',   3600,  False, '',                                         '/var/log/kronos/13_mini_generator.err'),
    ('kronos-base',    'M14 · kronos-base · 1H',   3600,  True,  'Disabled in code — DISABLED_MODEL_SOURCES (2026-06-09)', '/var/log/kronos/14_base_generator.err'),
    ('kronos-mini-4h', 'M15 · kronos-mini · 4H',  14400,  False, '',                                         '/var/log/kronos/15_mini_4h_generator.err'),
    ('kronos-base-4h', 'M16 · kronos-base · 4H',  14400,  False, '',                                         '/var/log/kronos/16_base_4h_generator.err'),
    ('custom',         'M04 · custom · 1H',         3600,  False, '',                                         '/var/log/kronos/04_signal_generator.err'),
]

_GH_REASON_MAP = [
    # (substring, category, friendly_label, css_class)
    ('longs_suspended',             'halted',  'Longs halted — poor WR, awaiting retrain', 'mh-rr-halted'),
    ('short_4h_bullish_blocked',    'market',  'Short blocked — 4H candle bullish',         'mh-rr-market'),
    ('short_daily_bullish_blocked', 'market',  'Short blocked — daily candle bullish',      'mh-rr-market'),
    ('regime_bull_short_blocked',   'market',  'Short blocked — bull regime (EMA200)',      'mh-rr-market'),
    ('regime_bear_long_blocked',    'market',  'Long blocked — bear regime (EMA200)',       'mh-rr-market'),
    ('short_rvol_gate',             'market',  'Short blocked — RVOL out of band',          'mh-rr-market'),
    ('model_disabled',              'disabled','Model disabled in code',                    'mh-rr-disabled'),
    ('system_halted',               'system',  'System halted — red alert',                 'mh-rr-system'),
    ('orange_alert',                'system',  'Orange alert — no new entries',             'mh-rr-system'),
    ('yellow_alert',                'system',  'Yellow alert — Slot 3 blocked',             'mh-rr-system'),
    ('forced_override',             'system',  'Forced override active',                    'mh-rr-system'),
    ('funding_settlement_blackout', 'filter',  'Funding settlement blackout',               'mh-rr-filter'),
    ('macro_blackout',              'filter',  'Macro event blackout',                      'mh-rr-filter'),
    ('stop_loss_4h_blackout',       'filter',  'Post stop-loss blackout (4H)',              'mh-rr-filter'),
    ('circuit_breaker',             'filter',  'Circuit breaker — extreme volatility',      'mh-rr-filter'),
    ('confidence_gate',             'filter',  'Below confidence threshold',                'mh-rr-filter'),
    ('return_floor',                'filter',  'Below return floor',                        'mh-rr-filter'),
    ('entry_cost',                  'filter',  'Entry cost too high',                       'mh-rr-filter'),
    ('position_cap',                'filter',  'Position cap reached',                      'mh-rr-filter'),
    ('stacking',                    'filter',  'Signal stacking guard',                     'mh-rr-filter'),
    ('correlation',                 'filter',  'Correlation guard',                         'mh-rr-filter'),
    ('asset_excluded',              'filter',  'Asset excluded',                            'mh-rr-filter'),
    ('signal_expired',              'filter',  'Signal expired',                            'mh-rr-filter'),
]


def _read_gen_log(log_path: str, cycle_secs: int = 3600) -> dict:
    """Read last 10 KB of a generator .err log and extract operational state.

    Returns a dict with keys:
      loaded        bool   — model loaded line found AND timestamp is recent (< 3 cycles old)
      loaded_ts     float  — unix ts of the most-recent 'loaded' line, or None
      scheduled     bool   — scheduler-started line found after loaded line
      last_job_ts   float  — unix ts of most-recent 'executed successfully' line, or None
      job_running   bool   — 'Running job' seen after most-recent 'executed successfully'
      not_ready     bool   — 'not ready — skipping' seen (model load failed, cycling no-ops)
      error_line    str    — first error/traceback line found, or ''
      log_ok        bool   — False only if file unreadable
    """
    result = dict(loaded=False, loaded_ts=None, scheduled=False, last_job_ts=None,
                  job_running=False, not_ready=False, error_line='', log_ok=True)
    try:
        with open(log_path, 'rb') as _f:
            _f.seek(0, 2)
            _f.seek(max(0, _f.tell() - 10240))
            lines = _f.read().decode('utf-8', errors='replace').splitlines()
    except (FileNotFoundError, PermissionError, OSError):
        result['log_ok'] = False
        return result

    now = time.time()
    stale_threshold = 3 * cycle_secs  # log confirmation older than 3 cycles is from a previous run

    # Scan bottom-up to find the most-recent loaded + scheduler pair.
    # Extract the timestamp from the line to guard against stale lines from old runs.
    _loaded_ts = None
    _scheduled_after_load = False
    for line in reversed(lines):
        ll = line.lower()
        if ('loaded — context=' in ll or 'model ready' in ll or 'model loaded' in ll) and _loaded_ts is None:
            try:
                _loaded_ts = time.mktime(time.strptime(line[:19], '%Y-%m-%d %H:%M:%S'))
            except Exception:
                _loaded_ts = 0.0  # unparseable but present
        if 'scheduler started' in ll and _loaded_ts is not None:
            _scheduled_after_load = True
        if _loaded_ts is not None and _scheduled_after_load:
            break

    if _loaded_ts is not None and (now - _loaded_ts) <= stale_threshold:
        result['loaded']    = True
        result['loaded_ts'] = _loaded_ts
        result['scheduled'] = _scheduled_after_load
    elif _loaded_ts is not None:
        # Loaded line exists but is stale — previous process run, not current
        result['loaded_ts'] = _loaded_ts  # keep ts so we can show "last loaded Xh ago"

    for line in reversed(lines):
        ll = line.lower()
        if 'executed successfully' in ll:
            try:
                result['last_job_ts'] = time.mktime(
                    time.strptime(line[:19], '%Y-%m-%d %H:%M:%S'))
            except Exception:
                pass
            break

    for line in reversed(lines):
        if 'Running job' in line:
            result['job_running'] = True
            break
        if 'executed successfully' in line:
            break  # job completed cleanly, not mid-run

    for line in lines[-60:]:
        ll = line.lower()
        if 'not ready' in ll and 'skipping' in ll:
            result['not_ready'] = True
        if ('traceback' in ll or 'error:' in ll or 'permissionerror' in ll
                or 'import failed' in ll) and not result['error_line']:
            result['error_line'] = line.strip()[:90]

    return result


def _gh_categorize(reason: str) -> tuple:
    if not reason:
        return 'filter', 'Unknown rejection reason', 'mh-rr-filter'
    rl = reason.lower()
    for substr, cat, label, css in _GH_REASON_MAP:
        if substr in rl:
            return cat, label, css
    return 'filter', (reason[:55] + '…' if len(reason) > 55 else reason), 'mh-rr-filter'


def _render_gen_health(d: dict) -> str:
    gh      = d.get('gen_health', {})
    mh_24h  = gh.get('mh_24h',    {})
    mh_ever = gh.get('mh_ever',   {})
    mh_reas = gh.get('mh_reasons', {})
    mh_logs = gh.get('mh_logs',   {})
    now_ts  = gh.get('now_ts', int(time.time()))

    def _age(ts):
        m = int((now_ts - ts) // 60)
        return f'{m}m ago' if m < 120 else f'{m//60}h {m%60:02d}m ago'

    cards = ''
    for db_key, label, cycle_secs, code_disabled, dis_reason, _ in _GH_MODELS:
        row      = mh_24h.get(db_key, {})
        last_ts  = mh_ever.get(db_key)
        total_24 = int(row.get('total',    0))
        rej_24   = int(row.get('rejected', 0))
        gen_24   = int(row.get('generated',0))
        reasons  = mh_reas.get(db_key, [])
        log      = mh_logs.get(db_key, {})

        log_ok      = log.get('log_ok', False)
        loaded      = log.get('loaded', False)
        loaded_ts   = log.get('loaded_ts')
        scheduled   = log.get('scheduled', False)
        not_ready   = log.get('not_ready', False)
        error_line  = log.get('error_line', '')
        last_job_ts = log.get('last_job_ts')
        job_running = log.get('job_running', False)
        stale_secs  = 2.5 * cycle_secs

        # ── Determine status ────────────────────────────────────────────────────
        if code_disabled:
            st, sc, bc = 'DISABLED', '#97a0af', '#dfe1e6'
            detail = f'<div class="mh-detail" style="color:#97a0af">{dis_reason}</div>'

        elif not log_ok:
            # Log file missing or unreadable — process may never have started
            st, sc, bc = 'NO LOG', '#ff5630', '#ff5630'
            detail = '<div class="mh-detail" style="color:#ff5630">Log file not found — process may not have started</div>'

        elif error_line and not loaded:
            # Error in log and model never confirmed loaded → crashed on startup
            st, sc, bc = 'ERROR', '#ff5630', '#ff5630'
            safe = error_line.replace('<', '&lt;').replace('>', '&gt;')
            detail = f'<div class="mh-detail" style="color:#ff5630">{safe}</div>'

        elif not_ready and not loaded:
            # Scheduler running but model never loaded — stuck skipping every cycle
            st, sc, bc = 'NOT READY', '#ff5630', '#ff5630'
            detail = '<div class="mh-detail" style="color:#ff5630">Model failed to load — every cycle is a no-op</div>'

        else:
            stale = last_ts is None or (now_ts - last_ts) > stale_secs

            # Build rejection pills (used by BLOCKED branch)
            reason_counts: dict = defaultdict(int)
            for reason_str, cnt in reasons:
                _, friendly, css = _gh_categorize(reason_str)
                reason_counts[(friendly, css)] += cnt
            pills = ''.join(
                f'<span class="{css}">{friendly} &times;{cnt}</span>'
                for (friendly, css), cnt in sorted(reason_counts.items(), key=lambda x: -x[1])
            )

            if not stale and total_24 > 0 and gen_24 > 0:
                # Recent signals and some passed risk — genuinely active
                st, sc, bc = 'ACTIVE', '#36b37e', '#36b37e'
                detail = (f'<div class="mh-detail">{total_24} signals today &mdash; '
                          f'{gen_24} passed risk &mdash; last {_age(last_ts)}</div>')

            elif not stale and total_24 > 0 and total_24 == rej_24:
                # Recent signals but M5 rejected every one — show why
                st, sc, bc = 'BLOCKED', '#ff991f', '#ff991f'
                detail = (f'<div class="mh-detail">{total_24} signals today, all blocked by M5'
                          f' &mdash; last {_age(last_ts)}</div>'
                          f'<div class="mh-reasons">{pills}</div>')

            else:
                # Stale or no recent signals — model alive per log, but not writing to DB
                job_info = ''
                if last_job_ts:
                    job_info = f' &mdash; last job ran {_age(last_job_ts)}'
                elif job_running:
                    job_info = ' &mdash; job currently running'

                if loaded and scheduled:
                    # Calculate missed cycles
                    if last_ts:
                        missed = int((now_ts - last_ts) // cycle_secs)
                        idle_str = f'no signals for {_age(last_ts)} ({missed} cycles)'
                    else:
                        idle_str = 'no signals ever written to DB'

                    # If multiple cycles missed, escalate to IDLE (yellow) — something is wrong
                    cycles_missed = int((now_ts - (last_ts or (now_ts - stale_secs))) // cycle_secs)
                    if cycles_missed >= 3:
                        st, sc, bc = 'IDLE', '#ff991f', '#ff991f'
                        pills_section = f'<div class="mh-reasons">{pills}</div>' if pills else ''
                        detail = (f'<div class="mh-detail" style="color:#ff991f">'
                                  f'Loaded &amp; scheduling but {idle_str}{job_info}'
                                  f' — generator not writing signals</div>{pills_section}')
                    else:
                        st, sc, bc = 'WAITING', '#36b37e', '#36b37e'
                        detail = (f'<div class="mh-detail">Model loaded &amp; scheduler active'
                                  f'{job_info}</div>')
                else:
                    st, sc, bc = 'SILENT', '#ff5630', '#ff5630'
                    if loaded_ts:
                        why = f'Log shows model loaded {_age(loaded_ts)} — that was a previous run. Current process has not confirmed ready.'
                    elif last_ts:
                        why = f'Last signal {_age(last_ts)} — current process has not loaded yet'
                    else:
                        why = 'No signals ever — model still loading or failed to start'
                    detail = f'<div class="mh-detail" style="color:#ff5630">{why}</div>'

        dot = '⊘' if code_disabled else '●'
        cards += f'''
<div class="mh-card" style="border-left:3px solid {bc}">
  <div class="mh-status" style="color:{sc}">{dot} {st}</div>
  <div class="mh-name">{label}</div>
  {detail}
</div>'''

    return f'''
<div class="section" style="margin-bottom:12px">
  <div class="section-hdr">Generator Health
    <span class="tag" style="text-transform:none;font-weight:400;font-size:.67rem">
      log + DB &nbsp;·&nbsp; auto-refresh
    </span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px">
    {cards}
  </div>
</div>'''


def _render_summary_pane(d: dict, f: dict, notice: str) -> str:
    fc = _filter_count(f)

    # Data quality notice
    flagged = d.get('flagged_trades', 0)
    if flagged > 0 and d['n'] == 0:
        quality_notice = f"""
<div style="background:#fffae6;border:1px solid #ffe380;border-radius:6px;
            padding:9px 14px;margin-bottom:14px;font-size:.8rem;color:#172b4d">
  <strong>&#9888; Clean baseline starting now.</strong>
  &nbsp;{flagged} historical trade{'s' if flagged!=1 else ''} flagged and excluded
  (test artifacts + bug-corrupted exits from duplicate process issue, fixed 2026-05-27).
</div>"""
    elif flagged > 0:
        quality_notice = f"""
<div style="background:#f4f5f7;border:1px solid #dfe1e6;border-radius:6px;
            padding:7px 14px;margin-bottom:14px;font-size:.75rem;color:#6b778c">
  &#128274; {flagged} flagged trade{'s' if flagged!=1 else ''} excluded from all metrics.
</div>"""
    else:
        quality_notice = ''

    # Per-model capital cards
    _model_cfg = {v: (lbl, cls, color) for v, lbl, cls, color in _MODEL_OPTS}
    model_cards_html = ''
    for _mk, (_mlabel, _mbadge_cls, _mbadge_color) in _model_cfg.items():
        mp   = d['model_pf'].get(_mk, {})
        mv   = mp.get('total', START)
        mchg = mp.get('chg',   0.0)
        mg   = mp.get('gross', 0.0)
        mn   = mp.get('n',     0)
        mw   = mp.get('wins',  0)
        ml   = mn - mw
        mwr  = mw / mn * 100 if mn else 0.0
        mwr_c = '#36b37e' if mwr >= 50 else ('#6b778c' if mn == 0 else '#ff5630')
        opacity = ' style="opacity:.35"' if (f['models'] and _mk not in f['models']) else ''
        model_cards_html += f"""
  <div class="card"{opacity} style="border-top:3px solid {_mbadge_color}">
    <div class="card-lbl">
      <span class="badge {_mbadge_cls}" style="font-size:.72rem">{_mlabel}</span>
      &nbsp;Capital
    </div>
    <div class="card-val">&#8377;{mv:,.0f}</div>
    <div class="card-sub {_gain(mchg)}">{"+" if mchg >= 0 else ""}&#8377;{mchg:,.2f} from &#8377;{START:,.0f}
      &nbsp;&mdash;&nbsp;<span class="{'pos' if mg>=0 else 'neg'}" style="font-weight:600">Gross {_inr(mg)}</span>
    </div>
    <div class="card-sub neu" style="margin-top:3px">{mn} trade{'s' if mn!=1 else ''}
      &nbsp;&nbsp;<span style="color:{mwr_c};font-weight:600">{mwr:.0f}% WR</span>
      &nbsp;({mw}W/{ml}L)</div>
  </div>"""

    model_cards = (f'<div class="cards" style="grid-template-columns:repeat(5,1fr);margin-bottom:12px">'
                   f'{model_cards_html}\n</div>')

    wr_color = '#36b37e' if d['wr'] >= 50 else '#ff5630'
    filter_note = ' (filtered)' if fc else ''
    agg_gross = f"""
  <div class="card">
    <div class="card-lbl">Combined P&amp;L{filter_note} &mdash; {d['n']} trades</div>
    <div class="card-val {_gain(d['gross'])}">{_inr(d['gross'])}</div>
    <div class="card-sub neu">Net: {_inr(d['net'])}
      &nbsp;&nbsp;TDS: &#8377;{d['tds']:,.2f}</div>
  </div>"""
    agg_wr = f"""
  <div class="card">
    <div class="card-lbl">Win Rate{filter_note} &mdash; {d['wins']}W / {d['losses']}L</div>
    <div class="card-val" style="color:{wr_color}">{d['wr']:.1f}%</div>
    <div class="wr-bar"><div class="wr-fill" style="width:{d['wr']:.0f}%;background:{wr_color}"></div></div>
  </div>"""
    agg_dd = f"""
  <div class="card">
    <div class="card-lbl">Max Drawdown &mdash; Combined &#8377;{5*START:,.0f}</div>
    <div class="card-val {'neg' if d['max_dd']>5 else 'warn' if d['max_dd']>2 else 'pos'}">
      -{d['max_dd']:.2f}%</div>
    <div class="card-sub neu">Open positions: {len(d['positions'])}</div>
  </div>"""

    cards = (model_cards +
             f'<div class="cards" style="grid-template-columns:repeat(3,1fr)">'
             f'{agg_gross}{agg_wr}{agg_dd}</div>')

    # Open positions
    pos_sfx = ' (filtered)' if (f['models'] or f['symbols']) else ''
    if d['positions']:
        rows = ''
        for p in d['positions']:
            ep   = _f(p['entry_price'])
            cp   = _f(p['current_price'], ep)
            upnl = _f(p['unrealised_pnl'])
            upct = ((cp-ep)/ep*100*(1 if p['direction'].lower()=='long' else -1)) if ep else 0.0
            sz   = _f(p['size_contracts'])
            notl = _f(p['notional_value'])
            sl   = _f(p['stop_loss_price'])
            tp_  = _f(p['take_profit_price'])
            rh   = _f(p.get('running_high'), ep)
            rl   = _f(p.get('running_low'),  ep)
            pk_pct = ((rh-ep)/ep*100) if ep else 0.0
            tr_pct = ((rl-ep)/ep*100) if ep else 0.0
            rows += f"""<tr>
  <td>{_dir(p['direction'])}</td>
  <td><strong>{p['symbol']}</strong></td>
  <td>{_model(p.get('model_source') or 'custom')}</td>
  <td style="font-size:.82rem"><strong>{sz:,.6f}</strong><br>
    <span class="tag">&#8377;{notl:,.0f}</span></td>
  <td>${ep:,.2f}</td><td>${cp:,.2f}</td>
  <td class="{_gain(upnl)}">{_inr(upnl)}<br><span class="tag">({_pct(upct)})</span></td>
  <td class="pos" style="font-size:.8rem">${rh:,.2f}<br><span class="tag">{_pct(pk_pct)}</span></td>
  <td class="neg" style="font-size:.8rem">${rl:,.2f}<br><span class="tag">{_pct(tr_pct)}</span></td>
  <td class="tag" style="font-size:.75rem">SL ${sl:,.2f}<br>TP ${tp_:,.2f}</td>
  <td class="neu">{_elapsed(p['entry_timestamp'])}</td>
  <td class="tag">{_ts(p['max_hold_until'])}</td>
</tr>"""
        pos_html = f"""
<div class="section">
  <div class="section-hdr">Open Positions{pos_sfx} <span class="count">{len(d['positions'])}</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Dir</th><th>Symbol</th><th>Model</th><th>Size</th><th>Entry</th><th>Current</th>
    <th>Unrealised P&amp;L</th><th>Peak</th><th>Trough</th><th>SL / TP</th>
    <th>Held</th><th>Max Hold (UTC)</th></tr></thead>
    <tbody>{rows}</tbody></table></div></div>"""
    else:
        pos_html = (f'<div class="section"><div class="section-hdr">Open Positions'
                    f'{pos_sfx} <span class="count">0</span></div>'
                    f'<div class="empty">No open positions.</div></div>')

    # Trade history
    hist_sfx = ' (filtered)' if fc else ''
    if d['history']:
        rows = ''
        for t in d['history']:
            ep     = _f(t['entry_price'])
            xp     = _f(t['exit_price'])
            g      = _f(t['pnl_gross'])
            sz     = _f(t['size_contracts'])
            reason = str(t.get('exit_reason') or '--').replace('_', ' ')
            conf   = t.get('confidence')
            pred   = _f(t.get('predicted_return_pct'))
            actual = t.get('actual_return_pct')
            pk     = t.get('peak_price')
            tr_    = t.get('trough_price')

            pk_cell = (f'${_f(pk):,.2f}<br><span class="tag">{_pct((_f(pk)-ep)/ep*100)}</span>'
                       if pk and ep else '<span class="neu">--</span>')
            tr_cell = (f'${_f(tr_):,.2f}<br><span class="tag">{_pct((_f(tr_)-ep)/ep*100)}</span>'
                       if tr_ and ep else '<span class="neu">--</span>')

            conf_str = f'{float(conf):.3f}' if conf is not None else '--'
            if actual is not None:
                actual_f  = float(actual)
                trade_dir = str(t.get('direction', '')).lower()
                hit       = ((trade_dir == 'long'  and actual_f >  _HIT_THR) or
                             (trade_dir == 'short' and actual_f < -_HIT_THR))
                icon      = '&#10003;' if hit else '&#10007;'
                cls_      = 'outcome-hit' if hit else 'outcome-miss'
                outcome   = (f'<span class="tag">{conf_str} conf</span><br>'
                             f'<span class="{_gain(pred)}">{_pct(pred)}</span> pred<br>'
                             f'<span class="{cls_}">{_pct(actual_f)} {icon}</span>')
            else:
                outcome = (f'<span class="tag">{conf_str} conf</span><br>'
                           f'<span class="{_gain(pred)}">{_pct(pred)}</span> pred<br>'
                           f'<span class="neu" style="font-size:.7rem">unresolved</span>')

            rows += f"""<tr>
  <td class="tag">{_ts(t['exit_timestamp'])}</td>
  <td><strong>{t['symbol']}</strong></td>
  <td>{_dir(t['direction'])}</td>
  <td>{_model(t.get('model_source') or 'custom')}</td>
  <td style="font-size:.82rem"><strong>{sz:,.6f}</strong></td>
  <td class="tag">${ep:,.2f} &rarr; ${xp:,.2f}</td>
  <td class="{_gain(g)}">{_inr(g)}</td>
  <td class="pos" style="font-size:.8rem">{pk_cell}</td>
  <td class="neg" style="font-size:.8rem">{tr_cell}</td>
  <td style="font-size:.76rem;line-height:1.4">{outcome}</td>
  <td class="neu" style="font-size:.78rem">{reason}</td>
</tr>"""

        page     = f['page']
        prev_url = _url_with({**f, 'tab': 'summary'}, page=page-1) if d['has_prev'] else None
        next_url = _url_with({**f, 'tab': 'summary'}, page=page+1) if d['has_next']  else None
        prev_btn = (f'<a href="{prev_url}" class="page-btn">&larr; Prev</a>'
                    if prev_url else '<span class="page-btn" style="opacity:.35;cursor:default">&larr; Prev</span>')
        next_btn = (f'<a href="{next_url}" class="page-btn">Next &rarr;</a>'
                    if next_url else '<span class="page-btn" style="opacity:.35;cursor:default">Next &rarr;</span>')
        showing  = f'{page*PAGE_SIZE+1}–{page*PAGE_SIZE+len(d["history"])}'
        pag_html = (f'<div class="pagination">{prev_btn}'
                    f'<span class="page-info">Showing {showing}</span>'
                    f'{next_btn}</div>')

        hist_html = f"""
<div class="section">
  <div class="section-hdr">Trade History{hist_sfx} <span class="count">{showing}</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Closed (UTC)</th><th>Symbol</th><th>Dir</th><th>Model</th><th>Size</th>
    <th>Entry &rarr; Exit</th><th>Gross P&amp;L</th><th>Peak</th><th>Trough</th>
    <th>Signal / Outcome</th><th>Closed Because</th></tr></thead>
    <tbody>{rows}</tbody></table></div>
  {pag_html}
</div>"""
    else:
        hist_html = (f'<div class="section"><div class="section-hdr">Trade History'
                     f'{hist_sfx}</div>'
                     f'<div class="empty">No closed trades match the current filters.</div></div>')

    # Recent signal activity — last 15 signals regardless of status
    recent_sigs = d.get('pipeline', [])[:15]
    if recent_sigs:
        sig_rows = ''
        for s in recent_sigs:
            status    = str(s.get('status') or '').lower()
            direction = str(s.get('direction') or '').lower()
            actual    = s.get('actual_return_pct')
            pred      = _f(s.get('predicted_return_pct'))
            reason    = str(s.get('rejection_reason') or '')
            reason_short = reason.replace('_', ' ')
            if len(reason_short) > 45: reason_short = reason_short[:42] + '...'
            if actual is not None:
                actual_f = float(actual)
                hit      = ((direction == 'long'  and actual_f >  _HIT_THR) or
                            (direction == 'short' and actual_f < -_HIT_THR))
                icon     = '&#10003;' if hit else '&#10007;'
                cls_     = 'outcome-hit' if hit else 'outcome-miss'
                actual_cell = f'<span class="{cls_}">{_pct(actual_f)}&thinsp;{icon}</span>'
            elif status in ('rejected', 'expired'):
                actual_cell = '<span class="neu">—</span>'
            else:
                actual_cell = '<span class="neu" style="font-size:.7rem">pending</span>'
            opacity = ' style="opacity:.45"' if status in ('rejected', 'expired') else ''
            sig_rows += f"""<tr{opacity}>
  <td class="tag">{_ts(s['signal_timestamp'])}</td>
  <td><strong>{s['symbol']}</strong></td>
  <td>{_model(s.get('model_source') or 'custom')}</td>
  <td>{_dir(direction)}</td>
  <td class="{_gain(pred)}">{_pct(pred)}</td>
  <td>{actual_cell}</td>
  <td>{_status(status)}</td>
  <td class="tag" style="font-size:.7rem" title="{reason}">{reason_short}</td>
</tr>"""
        sig_link = _url_with({**f, 'tab': 'signals'})
        recent_html = f"""
<div class="section">
  <div class="section-hdr">Recent Signal Activity
    <span class="tag" style="text-transform:none;font-weight:400;font-size:.67rem">
      last {len(recent_sigs)} &nbsp;·&nbsp;
      <span style="color:#97a0af">dimmed = rejected/expired</span>
    </span>
    <a href="{sig_link}" style="margin-left:auto;font-size:.72rem;color:#0052cc;
       text-decoration:none;font-weight:600">View all &rarr;</a>
  </div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Time (UTC)</th><th>Symbol</th><th>Model</th><th>Dir</th>
    <th>Predicted</th><th>Actual</th><th>Status</th><th>Rejection Reason</th></tr></thead>
    <tbody>{sig_rows}</tbody></table></div>
</div>"""
    else:
        recent_html = (
            '<div class="section"><div class="section-hdr">Recent Signal Activity</div>'
            '<div class="empty">No signals generated yet — generators may still be loading models '
            'or market conditions are blocking all directions (bullish market with longs suspended).'
            '</div></div>'
        )

    return notice + quality_notice + _render_gen_health(d) + cards + pos_html + hist_html + recent_html


# ── Tab 2: Analysis pane ──────────────────────────────────────────────────────

def _mx_cell(wr_data: dict, acc_data: dict, sym: str) -> str:
    n    = wr_data.get('n',    0)
    wins = wr_data.get('wins', 0)
    at   = acc_data.get('t',   0)
    ac   = acc_data.get('c',   0)

    if n == 0 and at == 0:
        return '<td class="mx-cell mx-none"><span style="color:#97a0af">—</span></td>'

    # Trade win rate
    wr_str = ''
    if n > 0:
        wr = wins / n * 100
        wr_cls = 'pos' if wr >= 60 else ('warn' if wr >= 45 else 'neg')
        wr_str = f'<div class="mx-val {wr_cls}">{wr:.0f}%</div><div class="mx-sub">{n} trades</div>'
        cell_cls = 'mx-hit' if wr >= 60 else ('mx-warn' if wr >= 45 else 'mx-miss')
    else:
        cell_cls = 'mx-none'

    # Directional accuracy
    acc_str = ''
    if at > 0:
        ap = ac / at * 100
        acc_cls = 'pos' if ap >= 55 else ('warn' if ap >= 45 else 'neg')
        acc_str = f'<div class="mx-sub" style="margin-top:3px;color:#5e6c84">Dir: <span class="{acc_cls}" style="font-weight:600">{ap:.0f}%</span> / {at}s</div>'

    return f'<td class="mx-cell {cell_cls}">{wr_str}{acc_str}</td>'


def _render_analysis_pane(d: dict, f: dict) -> str:
    model_keys  = [mk for mk, *_ in _MODEL_OPTS]
    model_labels = {mk: lbl for mk, lbl, *_ in _MODEL_OPTS}
    matrix_wr   = d.get('matrix_wr',  {})
    matrix_acc  = d.get('matrix_acc', {})
    all_syms    = d.get('all_mx_syms', [])

    # ── 1. Model × Asset matrix ───────────────────────────────────────────────
    if all_syms:
        hdr = '<th style="min-width:90px">Asset</th>'
        for mk in model_keys:
            opacity = ' style="opacity:.4"' if (f['models'] and mk not in f['models']) else ''
            hdr += f'<th class="mx-cell"{opacity}>{model_labels[mk]}</th>'

        rows = ''
        for sym in all_syms:
            rows += f'<tr><td><strong>{sym.replace("USD","")}</strong><br><span class="tag" style="font-size:.65rem">{sym}</span></td>'
            for mk in model_keys:
                wr_data  = matrix_wr.get(mk,  {}).get(sym, {})
                acc_data = matrix_acc.get(mk, {}).get(sym, {})
                rows += _mx_cell(wr_data, acc_data, sym)
            rows += '</tr>'

        matrix_html = f"""
<div class="section">
  <div class="section-hdr">Model × Asset Performance Matrix
    <span class="tag" style="font-size:.67rem;text-transform:none;font-weight:400">
      Win rate (trades) &nbsp;·&nbsp; Directional accuracy (resolved signals)
    </span>
  </div>
  <div style="overflow-x:auto"><table>
    <thead><tr>{hdr}</tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        matrix_html = ('<div class="section"><div class="section-hdr">Model × Asset Performance Matrix</div>'
                       '<div class="empty">No trade data yet — matrix will populate as trades close.</div></div>')

    # ── 2. Equity curves + Rejection funnel ───────────────────────────────────
    eq_chart_html = """
<div class="section">
  <div class="section-hdr">Equity Curves — Cumulative Gross P&amp;L</div>
  <div class="chart-wrap"><canvas id="equity-chart"></canvas></div>
</div>"""

    funnel_data = d.get('funnel_data', {})
    funnel_rows = ''
    for mk, lbl, *_ in _MODEL_OPTS:
        fd  = funnel_data.get(mk, {})
        gen = fd.get('gen', 0)
        pas = fd.get('passed', 0)
        exe = fd.get('exec',   0)
        won = fd.get('won',    0)
        if gen == 0:
            continue
        pass_pct = pas / gen * 100 if gen else 0
        exec_pct = exe / pas * 100 if pas else 0
        won_pct  = won / exe * 100 if exe else 0
        opacity  = ' style="opacity:.4"' if (f['models'] and mk not in f['models']) else ''
        funnel_rows += f"""
<tr{opacity}>
  <td style="white-space:nowrap">{_model(mk)}</td>
  <td style="text-align:right;font-weight:600">{gen:,}</td>
  <td>
    <span style="font-weight:600">{pas:,}</span>
    <span class="tag"> ({pass_pct:.1f}%)</span>
    <div style="height:5px;width:{min(pass_pct,100):.0f}%;background:#4c9aff;border-radius:2px;margin-top:3px"></div>
  </td>
  <td>
    <span style="font-weight:600">{exe:,}</span>
    <span class="tag"> ({exec_pct:.1f}%)</span>
    <div style="height:5px;width:{min(exec_pct,100):.0f}%;background:#36b37e;border-radius:2px;margin-top:3px"></div>
  </td>
  <td>
    <span class="{'pos' if won_pct>=50 else 'neg'}" style="font-weight:600">{won:,}</span>
    <span class="tag"> ({won_pct:.1f}%)</span>
    <div style="height:5px;width:{min(won_pct,100):.0f}%;background:{'#36b37e' if won_pct>=50 else '#ff5630'};border-radius:2px;margin-top:3px"></div>
  </td>
</tr>"""

    if funnel_rows:
        funnel_html = f"""
<div class="section">
  <div class="section-hdr">Rejection Funnel
    <span class="tag" style="font-size:.67rem;text-transform:none;font-weight:400">Generated → Passed filter → Executed → Won</span>
  </div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Model</th><th>Generated</th><th>Passed Filter</th><th>Executed</th><th>Won</th></tr></thead>
    <tbody>{funnel_rows}</tbody>
  </table></div>
</div>"""
    else:
        funnel_html = ('<div class="section"><div class="section-hdr">Rejection Funnel</div>'
                       '<div class="empty">No signal data yet.</div></div>')

    two_col = f'<div class="two-col">{eq_chart_html}{funnel_html}</div>'

    # ── 3. Confidence calibration ─────────────────────────────────────────────
    cal_data = d.get('cal_data', {})
    # Only show models that have any calibration data
    active_models = [mk for mk, *_ in _MODEL_OPTS
                     if any(cal_data.get(mk, [{'t':0}])[i].get('t',0) > 0
                            for i in range(len(_CONF_BANDS)))]

    if active_models:
        hdr = '<th>Confidence Band</th>'
        for mk in active_models:
            opacity = ' style="opacity:.4"' if (f['models'] and mk not in f['models']) else ''
            hdr += f'<th{opacity}>{model_labels[mk]}</th>'

        rows = ''
        for i, lbl in enumerate(_BAND_LABELS):
            rows += f'<tr><td><span class="badge b-neu">{lbl}</span></td>'
            for mk in active_models:
                band = cal_data.get(mk, [{'c':0,'t':0}]*len(_CONF_BANDS))[i]
                t, c = band.get('t', 0), band.get('c', 0)
                if t == 0:
                    rows += '<td class="neu" style="text-align:center">—</td>'
                else:
                    pct   = c / t * 100
                    color = '#36b37e' if pct >= 55 else ('#f59e0b' if pct >= 45 else '#ff5630')
                    bar_w = int(min(pct, 100))
                    rows += f'''<td>
  <div class="cal-bar-wrap">
    <span style="font-weight:600;color:{color}">{pct:.0f}%</span>
    <div class="cal-bar-bg">
      <div class="cal-bar-fill" style="width:{bar_w}%;background:{color}"></div>
    </div>
    <span class="tag">/{t}</span>
  </div>
</td>'''
            rows += '</tr>'

        cal_html = f"""
<div class="section">
  <div class="section-hdr">Confidence Calibration
    <span class="tag" style="font-size:.67rem;text-transform:none;font-weight:400">
      Directional accuracy per confidence band (resolved signals only)
    </span>
  </div>
  <div style="overflow-x:auto"><table>
    <thead><tr>{hdr}</tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        cal_html = ('<div class="section"><div class="section-hdr">Confidence Calibration</div>'
                    '<div class="empty">No resolved signals yet — calibration chart will appear after '
                    'signals mature past their horizon.</div></div>')

    return matrix_html + two_col + cal_html


# ── Weekly trend table ────────────────────────────────────────────────────────

def _render_weekly_trend(weekly_trend: dict) -> str:
    if not weekly_trend:
        return ('<div class="section"><div class="section-hdr">Weekly Directional Accuracy</div>'
                '<div class="empty">No resolved signals yet — trend will appear once signals '
                'mature past their horizon.</div></div>')

    all_md: set = set()
    for wk_data in weekly_trend.values():
        all_md.update(wk_data.keys())

    model_order = [mk for mk, *_ in _MODEL_OPTS]
    cols = [(mk, drx) for mk in model_order for drx in ('long', 'short')
            if (mk, drx) in all_md]
    if not cols:
        return ''

    _mlbl = {mk: lbl for mk, lbl, *_ in _MODEL_OPTS}
    hdr   = '<th>Week</th>'
    for mk, drx in cols:
        arrow = '&#9650;&nbsp;Long' if drx == 'long' else '&#9660;&nbsp;Short'
        color = '#006644' if drx == 'long' else '#bf2600'
        hdr  += (f'<th style="text-align:center;min-width:80px">'
                 f'{_mlbl.get(mk, mk)}<br>'
                 f'<span style="font-size:.63rem;color:{color};font-weight:600">{arrow}</span></th>')

    rows  = ''
    for wk in reversed(sorted(weekly_trend.keys())):
        wk_data = weekly_trend[wk]
        rows += f'<tr><td class="tag" style="white-space:nowrap;font-family:monospace">{wk}</td>'
        for mk, drx in cols:
            v = wk_data.get((mk, drx), {'c': 0, 't': 0})
            c, t = v['c'], v['t']
            if t == 0:
                rows += '<td style="text-align:center;color:#97a0af">—</td>'
            else:
                pct = c / t * 100
                if pct >= 55:
                    bg, fg = '#e3fcef', '#006644'
                elif pct >= 45:
                    bg, fg = '#fffae6', '#7a5200'
                else:
                    bg, fg = '#ffebe6', '#bf2600'
                rows += (f'<td style="text-align:center;background:{bg};padding:6px 4px">'
                         f'<span style="font-weight:700;color:{fg}">{pct:.0f}%</span>'
                         f'<br><span style="font-size:.63rem;color:#6b778c">n={t}</span></td>')
        rows += '</tr>'

    return f"""
<div class="section">
  <div class="section-hdr">Weekly Directional Accuracy — All Signals
    <span class="tag" style="text-transform:none;font-weight:400;font-size:.67rem">
      executed + rejected combined &nbsp;·&nbsp; last 12 weeks &nbsp;·&nbsp;
      <span style="color:#006644">&#9646;</span>&thinsp;≥55%&nbsp;
      <span style="color:#7a5200">&#9646;</span>&thinsp;45–55%&nbsp;
      <span style="color:#bf2600">&#9646;</span>&thinsp;&lt;45%
    </span>
  </div>
  <div style="overflow-x:auto"><table>
    <thead><tr>{hdr}</tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""


# ── Tab 3: Signals explorer ───────────────────────────────────────────────────

def _render_signals_pane(d: dict, f: dict, notice: str) -> str:
    sigs    = d.get('sigs_list', [])
    fc      = _filter_count(f)
    sfx     = ' (filtered)' if fc else ''
    sigpage = f.get('sigpage', 0)

    if sigs:
        rows = ''
        for s in sigs:
            conf     = s.get('confidence')
            pred     = s.get('predicted_return_pct')
            actual   = s.get('actual_return_pct')
            status   = str(s.get('status') or '').lower()
            direction = str(s.get('direction') or '').lower()
            conf_str = f'{float(conf):.3f}' if conf is not None else '—'

            # Predicted return
            pred_str = _pct(_f(pred)) if pred is not None else '—'
            pred_cls = _gain(_f(pred)) if pred is not None else 'neu'

            # Actual return + hit/miss
            if actual is not None:
                actual_f = float(actual)
                hit      = ((direction == 'long'  and actual_f >  _HIT_THR) or
                            (direction == 'short' and actual_f < -_HIT_THR))
                icon     = '&#10003;' if hit else '&#10007;'
                cls_     = 'outcome-hit' if hit else 'outcome-miss'
                actual_str = f'<span class="{cls_}">{_pct(actual_f)} {icon}</span>'
            elif status in ('rejected', 'expired'):
                actual_str = '<span class="neu">—</span>'
            else:
                actual_str = '<span class="neu" style="font-size:.7rem">pending</span>'

            # Row CSS class — maps to legend colours
            if status in ('rejected', 'expired'):
                row_cls = 'sig-rej'                          # dim
            elif actual is not None:
                actual_f2 = float(actual)
                row_cls = 'sig-hit' if ((direction == 'long'  and actual_f2 >  _HIT_THR) or
                                        (direction == 'short' and actual_f2 < -_HIT_THR)) else 'sig-miss'
            elif status == 'executed':
                row_cls = 'sig-pending'                      # blue — in-flight, not yet resolved
            else:
                row_cls = ''

            # Rejection reason (truncated)
            rr = str(s.get('rejection_reason') or '')
            rr_short = rr.replace('_', ' ')
            if len(rr_short) > 40:
                rr_short = rr_short[:37] + '...'

            mat = _maturity(s['signal_timestamp'], s.get('horizon'), status)

            rows += f"""<tr class="{row_cls}">
  <td class="tag">{_ts(s['signal_timestamp'])}</td>
  <td><strong>{s['symbol']}</strong></td>
  <td>{_model(s.get('model_source') or 'custom')}</td>
  <td>{_dir(direction)}</td>
  <td style="font-weight:600">{conf_str}</td>
  <td class="{pred_cls}">{pred_str}</td>
  <td>{actual_str}</td>
  <td>{_status(s['status'])}</td>
  <td class="tag" style="font-size:.7rem" title="{rr}">{rr_short}</td>
  <td style="font-size:.75rem">{mat}</td>
</tr>"""

        # Pagination
        prev_url = _url_with({**f, 'tab': 'signals'}, sigpage=sigpage-1) if d['has_prev_sig'] else None
        next_url = _url_with({**f, 'tab': 'signals'}, sigpage=sigpage+1) if d['has_next_sig'] else None
        prev_btn = (f'<a href="{prev_url}" class="page-btn">&larr; Prev</a>'
                    if prev_url else '<span class="page-btn" style="opacity:.35;cursor:default">&larr; Prev</span>')
        next_btn = (f'<a href="{next_url}" class="page-btn">Next &rarr;</a>'
                    if next_url else '<span class="page-btn" style="opacity:.35;cursor:default">Next &rarr;</span>')
        showing  = f'{sigpage*SIG_PAGE_SIZE+1}–{sigpage*SIG_PAGE_SIZE+len(sigs)}'
        pag_html = (f'<div class="pagination">{prev_btn}'
                    f'<span class="page-info">Showing {showing}</span>'
                    f'{next_btn}</div>')

        # ── Accuracy breakdown bar ────────────────────────────────────────────
        ss = d.get('sig_stats', {})

        _ecl  = ss.get('exec_corr_long',  0)
        _ecs  = ss.get('exec_corr_short', 0)
        _ewl  = ss.get('exec_wrong_long', 0)
        _ews  = ss.get('exec_wrong_short',0)
        _ep   = ss.get('exec_pend',       0)
        _rcl  = ss.get('rej_corr_long',   0)
        _rcs  = ss.get('rej_corr_short',  0)
        _rwl  = ss.get('rej_wrong_long',  0)
        _rws  = ss.get('rej_wrong_short', 0)
        _ru   = ss.get('rej_unres',       0)
        _exp  = ss.get('expired',         0)

        _ec   = _ecl + _ecs           # executed correct (all)
        _ew   = _ewl + _ews           # executed wrong (all)
        _et   = _ec  + _ew            # executed resolved total
        _rc   = _rcl + _rcs           # rejected correct (all)
        _rw   = _rwl + _rws           # rejected wrong (all)
        _rt   = _rc  + _rw            # rejected resolved total

        def _pstr(num, den):
            return f'{num/den*100:.1f}%' if den else '—'

        def _acc_card(href, card_cls, head_cls, head_txt,
                      pct_cls, pct_str, total_str,
                      long_n, long_d, short_n, short_d):
            ld = f'{long_n}&thinsp;/&thinsp;{long_d}'  if long_d  else '—'
            sd = f'{short_n}&thinsp;/&thinsp;{short_d}' if short_d else '—'
            return f"""<a href="{href}" class="sig-acc-card {card_cls}">
  <div class="acc-head {head_cls}">{head_txt}</div>
  <div class="acc-pct  {pct_cls}">{pct_str}</div>
  <div class="acc-total">{total_str}</div>
  <div class="acc-dir"><span>&#9650; Long</span><span>{ld}</span></div>
  <div class="acc-dir"><span>&#9660; Short</span><span>{sd}</span></div>
</a>"""

        _h = lambda sv: _url_with({**f, 'tab': 'signals', 'sigpage': 0}, sig_status=sv)

        acc_bar = f"""<div class="sig-acc-bar">
{_acc_card(_h('hit'),     'acc-hit',  'c-hit',  '&#10003; Executed',
           'c-hit',  _pstr(_ec, _et), f'{_ec} / {_et} resolved',
           _ecl, _ecl+_ewl, _ecs, _ecs+_ews)}
{_acc_card(_h('miss'),    'acc-miss', 'c-miss', '&#10007; Executed',
           'c-miss', _pstr(_ew, _et), f'{_ew} / {_et} resolved',
           _ewl, _ecl+_ewl, _ews, _ecs+_ews)}
{_acc_card(_h('rej-hit'), 'acc-rh',   'c-hit',  '&#10003; Rejected',
           'c-hit',  _pstr(_rc, _rt), f'{_rc} / {_rt} resolved',
           _rcl, _rcl+_rwl, _rcs, _rcs+_rws)}
{_acc_card(_h('rej-miss'),'acc-rm',   'c-miss', '&#10007; Rejected',
           'c-miss', _pstr(_rw, _rt), f'{_rw} / {_rt} resolved',
           _rwl, _rcl+_rwl, _rws, _rcs+_rws)}
<div class="sig-acc-card acc-other">
  <div class="acc-head">Other</div>
  <div class="acc-other-row"><span>&#9711; Exec pending</span><span>{_ep}</span></div>
  <div class="acc-other-row"><span>Rej unresolved</span><span>{_ru}</span></div>
  <div class="acc-other-row"><span>Expired</span><span>{_exp}</span></div>
</div>
</div>"""

        # Status filter chips (link-based, no form submission needed)
        cur_ss = f.get('sig_status', '')
        _chip_defs = [
            ('',        'sc-all',     'All'),
            ('hit',     'sc-hit',     '&#10003; Executed'),
            ('miss',    'sc-miss',    '&#10007; Executed'),
            ('pending', 'sc-pending', '&#9711; Pending'),
            ('rej-hit', 'sc-rej-hit', '&#10003; Rejected'),
            ('rej-miss','sc-rej-miss','&#10007; Rejected'),
            ('expired', 'sc-expired', 'Expired'),
        ]
        chip_html = ''
        for val, cls, label in _chip_defs:
            on   = ' on' if cur_ss == val else ''
            href = _url_with({**f, 'tab': 'signals', 'sigpage': 0}, sig_status=val)
            chip_html += f'<a href="{href}" class="sig-chip {cls}{on}">{label}</a>'

        tbl = f"""
<div class="section">
  <div class="section-hdr">Signal Explorer{sfx} <span class="count">{showing}</span>
    <span class="tag" style="text-transform:none;font-weight:400;font-size:.67rem;margin-left:8px">
      <span style="color:#006644">&#9646;</span> correct &nbsp;
      <span style="color:#bf2600">&#9646;</span> wrong &nbsp;
      <span style="color:#0052cc">&#9646;</span> pending &nbsp;
      <span style="color:#97a0af">&#9646;</span> rejected/expired
    </span>
  </div>
  {acc_bar}
  <div class="sig-chips">{chip_html}</div>
  <div style="overflow-x:auto"><table>
    <thead><tr>
      <th>Time (UTC)</th><th>Symbol</th><th>Model</th><th>Dir</th><th>Conf</th>
      <th>Predicted</th><th>Actual</th><th>Status</th>
      <th>Rejection Reason</th><th>Maturity</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
  {pag_html}
</div>"""
    else:
        tbl = ('<div class="section"><div class="section-hdr">Signal Explorer'
               f'{sfx}</div><div class="empty">No signals match the current filters.</div></div>')

    weekly_html = _render_weekly_trend(d.get('weekly_trend', {}))
    return notice + weekly_html + tbl


# ── Tab 4: Operational pane ───────────────────────────────────────────────────

def _render_ops_pane(d: dict, f: dict, notice: str) -> str:
    fc = _filter_count(f)

    # Model accuracy
    acc_rows = ''
    for m in d['accuracy']:
        t, c, pend = m['t'], m['c'], m['pend']
        hz_lbl = m.get('horizon', '?')
        if t == 0:
            acc_pct = '<span class="neu">--</span>'
            acc_bar = ''
            evaluated = '<span class="neu">0 evaluated</span>'
        else:
            pct = c / t * 100
            bar_color = '#36b37e' if pct >= 55 else ('#f59e0b' if pct >= 45 else '#ff5630')
            acc_pct   = f'<span style="color:{bar_color};font-weight:700">{pct:.1f}%</span>'
            acc_bar   = (f'<div style="height:6px;border-radius:3px;background:#f4f5f7;'
                         f'width:100px;overflow:hidden;display:inline-block;'
                         f'vertical-align:middle;margin-left:6px">'
                         f'<div style="height:100%;width:{pct:.0f}%;'
                         f'background:{bar_color};border-radius:3px"></div></div>')
            evaluated = f'{c}&nbsp;/&nbsp;{t} evaluated'
        pend_str = f'<span class="tag"> +{pend} maturing</span>' if pend else ''
        acc_rows += (f'<tr><td>{_model(m["name"])}</td>'
                     f'<td><span class="badge b-neu" style="font-size:.68rem">{hz_lbl}</span></td>'
                     f'<td>{acc_pct}{acc_bar}</td>'
                     f'<td class="neu">{evaluated}{pend_str}</td></tr>')

    acc_html = f"""
<div class="section">
  <div class="section-hdr">Model Accuracy
    <span class="tag" style="font-size:.68rem;text-transform:none;font-weight:400;margin-left:4px">
      1H models at 6H horizon &mdash; 4H models at 24H horizon &mdash; unfiltered
    </span>
  </div>
  <table><thead><tr><th>Model</th><th>Horizon</th><th>Accuracy</th><th>Sample</th></tr></thead>
  <tbody>{acc_rows}</tbody></table>
</div>"""

    # Signal pipeline
    _src_bg    = {'custom':'#e3f2fd','kronos-mini':'#e8f5e9','kronos-base':'#fff3e0',
                  'kronos-mini-4h':'#e6fcf5','kronos-base-4h':'#d3f9d8'}
    _src_label = {'custom':'custom','kronos-mini':'mini-1h','kronos-base':'base-1h',
                  'kronos-mini-4h':'mini-4h','kronos-base-4h':'base-4h'}
    pipe_sfx = ' (filtered)' if fc else ''
    if d['pipeline']:
        rows = ''
        for s in d['pipeline']:
            ret       = _f(s.get('predicted_return_pct'))
            actual    = s.get('actual_return_pct')
            direction = str(s.get('direction') or '').lower()
            reason    = str(s.get('rejection_reason') or '').replace('_', ' ')
            if len(reason) > 55: reason = reason[:52] + '...'
            qf        = s.get('quality_flag')
            qf_badge  = ('&nbsp;<span class="badge" style="background:#ffebe6;'
                         'color:#bf2600;font-size:.65rem">excluded</span>') if qf else ''
            row_style = ' style="opacity:.55"' if qf else ''
            src       = s.get('model_source') or 'custom'
            src_lbl   = _src_label.get(src, src)
            src_badge = (f'<span class="badge" style="background:{_src_bg.get(src,"#f4f5f7")};'
                         f'color:#172b4d;font-size:.65rem">{src_lbl}</span>')
            mat = _maturity(s['signal_timestamp'], s.get('horizon'), s.get('status'))
            if actual is not None:
                actual_f = float(actual)
                hit      = ((direction == 'long'  and actual_f >  _HIT_THR) or
                            (direction == 'short' and actual_f < -_HIT_THR))
                icon     = '&#10003;' if hit else '&#10007;'
                cls_     = 'outcome-hit' if hit else 'outcome-miss'
                actual_cell = f'<span class="{cls_}">{_pct(actual_f)}&thinsp;{icon}</span>'
            else:
                actual_cell = '<span class="neu">—</span>'
            rows += f"""<tr{row_style}>
  <td class="tag">{_ts(s['signal_timestamp'])}</td>
  <td><strong>{s['symbol']}</strong>{qf_badge}</td>
  <td>{src_badge}</td>
  <td>{_dir(s['direction'])}</td>
  <td>{_f(s['confidence']):.3f}</td>
  <td class="{_gain(ret)}">{_pct(ret)}</td>
  <td>{actual_cell}</td>
  <td>{_status(s['status'])}</td>
  <td style="font-size:.8rem;white-space:nowrap">{mat}</td>
  <td class="tag" style="font-size:.72rem" title="{s.get('rejection_reason') or ''}">{reason}</td>
</tr>"""
        pipe_html = f"""
<div class="section">
  <div class="section-hdr">Signal Pipeline{pipe_sfx} <span class="count">last {len(d['pipeline'])} of 50</span></div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Time (UTC)</th><th>Symbol</th><th>Model</th><th>Dir</th><th>Conf.</th>
    <th>Pred.</th><th>Actual</th><th>Status</th><th>Matures In</th><th>Rejection Reason</th></tr></thead>
    <tbody>{rows}</tbody></table></div>
</div>"""
    else:
        pipe_html = (f'<div class="section"><div class="section-hdr">Signal Pipeline'
                     f'{pipe_sfx}</div><div class="empty">No signals match.</div></div>')

    # Funding rates
    if d['funding']:
        chips = ''
        for fr in d['funding']:
            rate = _f(fr['rate'])
            cls  = 'neg' if rate > 0.001 else ('pos' if rate < -0.001 else 'neu')
            chips += (f'<span style="margin-right:20px"><strong>{fr["symbol"]}</strong>'
                      f'&nbsp;<span class="{cls}">{rate*100:+.4f}%/8H</span></span>')
        fund_html = (f'<div class="section"><div class="section-hdr">Latest Funding Rates</div>'
                     f'<div style="padding:10px 16px;font-size:.82rem">{chips}</div></div>')
    else:
        fund_html = ''

    # Events log
    events_log = d.get('events_log', [])
    if events_log:
        rows = ''
        for ev in events_log:
            et  = str(ev.get('event_type') or '').lower()
            msg = str(ev.get('message') or '').replace('→', '->')
            if len(msg) > 120: msg = msg[:117] + '...'
            # Severity from event_type keyword
            if any(k in et for k in ('error', 'exception', 'fail', 'crash')):
                row_cls = 'ev-error'
            elif any(k in et for k in ('warn',)):
                row_cls = 'ev-warn'
            elif any(k in et for k in ('start', 'init', 'heartbeat', 'ready')):
                row_cls = 'ev-info'
            else:
                row_cls = ''
            rows += f"""<tr class="{row_cls}">
  <td class="tag" style="white-space:nowrap">{_ts(ev['created_at'])}</td>
  <td style="font-size:.72rem;font-weight:600;white-space:nowrap">{ev.get('event_type','')}</td>
  <td style="font-size:.75rem">{msg}</td>
</tr>"""
        ev_html = f"""
<div class="section">
  <div class="section-hdr">Events Log <span class="count">last {len(events_log)}</span>
    <span class="tag" style="text-transform:none;font-weight:400;font-size:.67rem">
      &nbsp;<span class="ev-error" style="display:inline">&#9632;</span> error
      &nbsp;<span class="ev-warn" style="display:inline">&#9632;</span> warning
      &nbsp;<span class="ev-info" style="display:inline">&#9632;</span> startup/init
    </span>
  </div>
  <div style="overflow-x:auto"><table>
    <thead><tr><th>Time (UTC)</th><th>Event Type</th><th>Message</th></tr></thead>
    <tbody>{rows}</tbody>
  </table></div>
</div>"""
    else:
        ev_html = ''

    # 24h activity summary
    activity = d.get('activity', [])
    if activity:
        rows = ''
        for a in activity:
            rows += (f'<tr><td style="font-size:.75rem;font-family:monospace">'
                     f'{a.get("event_type","")}</td>'
                     f'<td style="text-align:right;font-weight:600">{a.get("cnt",0)}</td>'
                     f'<td class="tag">{_ts(a.get("last_ts", 0))}</td></tr>')
        act_html = f"""
<div class="section">
  <div class="section-hdr">24h Activity Summary</div>
  <table><thead><tr><th>Event Type</th><th style="text-align:right">Count</th><th>Last Seen (UTC)</th></tr></thead>
  <tbody>{rows}</tbody></table>
</div>"""
    else:
        act_html = ''

    return notice + acc_html + pipe_html + fund_html + '<div class="two-col">' + ev_html + act_html + '</div>'


# ── Main renderer ─────────────────────────────────────────────────────────────

def render(d: dict, f: dict) -> str:
    notice     = _make_notice(f)
    filter_bar = _render_filter_bar(f, d['all_symbols'])

    summary_pane  = _render_summary_pane(d, f, notice)
    analysis_pane = _render_analysis_pane(d, f)
    signals_pane  = _render_signals_pane(d, f, notice)
    ops_pane      = _render_ops_pane(d, f, notice)

    equity_json   = json.dumps(d.get('equity_chart', {}), separators=(',', ':'))

    mode_pill  = '<span class="pill pill-paper">PAPER</span>' if PAPER else '<span class="pill pill-live">LIVE</span>'
    phase_map  = {'pre_live': 'Pre-Live', 'income': 'Income', 'compound': 'Compound'}
    phase_lbl  = phase_map.get(PHASE, PHASE.replace('_', ' ').title())
    updated    = datetime.fromtimestamp(d['ts'], tz=timezone.utc).strftime('%d %b %H:%M UTC')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="x-refresh" content="30">
  <title>Kronos</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  {CSS}
</head>
<body>

<div class="topbar">
  <span class="topbar-brand">&#9654;&nbsp; KRONOS</span>
  <span class="topbar-right">
    {mode_pill}
    <span class="pill pill-regime">Regime v5</span>
    <span class="pill pill-phase">{phase_lbl}</span>
    <span>Updated {updated}</span>
    <span style="color:#dfe1e6">|</span>
    <span>Auto-refresh 30s</span>
  </span>
</div>

<div class="tabs">
  <button class="tab-btn" data-tab="summary"  onclick="showTab('summary')">Summary</button>
  <button class="tab-btn" data-tab="analysis" onclick="showTab('analysis')">Analysis</button>
  <button class="tab-btn" data-tab="signals"  onclick="showTab('signals')">Signals</button>
  <button class="tab-btn" data-tab="ops"      onclick="showTab('ops')">Operational</button>
</div>

{filter_bar}

<div id="pane-summary"  class="tab-pane">{summary_pane}</div>
<div id="pane-analysis" class="tab-pane">{analysis_pane}</div>
<div id="pane-signals"  class="tab-pane">{signals_pane}</div>
<div id="pane-ops"      class="tab-pane">{ops_pane}</div>

<div class="footer">Kronos Trading System &mdash; {'Paper trading' if PAPER else 'Live trading'} &mdash; Not financial advice</div>

{_build_js(equity_json)}
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    f = _get_filters()
    return render(get_data(f), f)

@app.route('/health')
def health():
    return {'status': 'ok', 'ts': int(time.time())}


if __name__ == '__main__':
    from db import init_db
    init_db()
    print(f'Kronos Dashboard  ->  http://0.0.0.0:{PORT}')
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
