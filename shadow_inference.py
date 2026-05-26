"""
Kronos Trading System — Shadow Inference
Runs Kronos foundation models (mini, base) in shadow mode alongside the custom
model every 4H cycle. Writes to shadow_signals table only — never touches
signals, trades, or positions. Used for week-6 directional accuracy benchmarking.
"""

import json
import logging
import os
import sys
import time
from typing import Optional

import pandas as pd

# Resolve vendor/kronos so 'from model import ...' works without installation.
_VENDOR_PATH = os.path.join(os.path.dirname(__file__), 'vendor', 'kronos')
if _VENDOR_PATH not in sys.path:
    sys.path.insert(0, _VENDOR_PATH)

# Force CPU unless explicitly opted into CUDA.
# On Windows, GPU inference with 50 samples exceeds the 2-second TDR timeout,
# causing CUDA context corruption that kills all subsequent GPU calls in the
# process. CPU is ~30s per cycle — well within the 4H window.
# To enable GPU (Linux only, TDR disabled): set KRONOS_SHADOW_DEVICE=cuda in .env
_SHADOW_DEVICE = os.environ.get('KRONOS_SHADOW_DEVICE', 'cpu').lower()
if _SHADOW_DEVICE != 'cuda':
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

from db import get_connection, log_event

logger = logging.getLogger(__name__)

MODULE = 'shadow_inference'

# ── Constants (mirror M4 where relevant for formula consistency) ───────────────

SLOT1_SYMBOL = 'BTCUSD'
SLOT2_SYMBOL = 'ETHUSD'
TIMEFRAME    = '4h'
PRED_LEN     = 6   # 6 × 4H = 24H horizon (identical to M4)
ATR_PERIOD   = 14  # must match M4

_SAMPLE_COUNT = int(os.environ.get('KRONOS_SHADOW_SAMPLE_COUNT', '50'))

# Per-model HuggingFace IDs, default context lengths, and env overrides.
_MODEL_SPECS: dict[str, dict] = {
    'kronos-mini': {
        'model_id':       'NeoQuasar/Kronos-mini',
        'tokenizer_id':   'NeoQuasar/Kronos-Tokenizer-2k',
        'context_env':    'KRONOS_SHADOW_MINI_CONTEXT',
        'context_default': 1024,
    },
    'kronos-base': {
        'model_id':       'NeoQuasar/Kronos-base',
        'tokenizer_id':   'NeoQuasar/Kronos-Tokenizer-base',
        'context_env':    'KRONOS_SHADOW_BASE_CONTEXT',
        'context_default': 512,
    },
}


# ── ATR helper (copied from M4 — kept separate to avoid import coupling) ──────

def _compute_atr_pct(rows: list[dict]) -> float:
    if len(rows) < 2:
        return 0.0
    trs = []
    for i in range(1, min(ATR_PERIOD + 1, len(rows))):
        high       = rows[i]['high']
        low        = rows[i]['low']
        prev_close = rows[i - 1]['close']
        tr = max(high - low,
                 abs(high - prev_close),
                 abs(low  - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    atr = sum(trs) / len(trs)
    final_close = rows[-1]['close']
    return (atr / final_close) if final_close > 0 else 0.0


# ── Main class ─────────────────────────────────────────────────────────────────

class ShadowInference:
    """
    Runs Kronos foundation models in shadow mode each 4H cycle.

    Models are lazy-loaded on the first run_all_symbols() call to avoid
    blocking M4 startup. If one model fails to load the other still runs.
    All exceptions are caught and logged — never raises to caller.
    """

    def __init__(self) -> None:
        self._mini_ready        = False
        self._base_ready        = False
        self._predictors: dict  = {}   # model_name → {predictor, context_len}
        self._models_attempted  = False

    # ── Public API ──────────────────────────────────────────────────────────

    def run_all_symbols(self) -> None:
        """Entry point called from M4 background task after each generate()."""
        self._load_models()
        if not self._mini_ready and not self._base_ready:
            return

        symbols    = self._active_symbols()
        signal_ts  = int(time.time())

        for symbol in symbols:
            for model_name, state in self._predictors.items():
                try:
                    self._run_one(symbol, model_name, state, signal_ts)
                except Exception as exc:
                    log_event(MODULE, 'error', 'error',
                              f'{model_name} {symbol} unhandled error: {exc}')
                    logger.exception('%s %s shadow run failed', model_name, symbol)

    def _load_models(self) -> None:
        """Load both foundation models (one-time; subsequent calls are no-ops)."""
        if self._models_attempted:
            return
        self._models_attempted = True

        try:
            from model import Kronos, KronosTokenizer, KronosPredictor
        except ImportError as exc:
            log_event(MODULE, 'error', 'error',
                      f'vendor/kronos import failed — shadow inference disabled: {exc}')
            return

        for model_name, spec in _MODEL_SPECS.items():
            context_len = int(os.environ.get(
                spec['context_env'], spec['context_default']))
            try:
                tokenizer = KronosTokenizer.from_pretrained(spec['tokenizer_id'])
                model     = Kronos.from_pretrained(spec['model_id'])
                predictor = KronosPredictor(model, tokenizer,
                                            max_context=context_len)
                self._predictors[model_name] = {
                    'predictor':   predictor,
                    'context_len': context_len,
                }
                if model_name == 'kronos-mini':
                    self._mini_ready = True
                else:
                    self._base_ready = True
                log_event(MODULE, 'info', 'info',
                          f'{model_name} loaded (context={context_len})')
                logger.info('%s loaded (context=%d)', model_name, context_len)
            except Exception as exc:
                log_event(MODULE, 'error', 'error',
                          f'{model_name} failed to load: {exc}')
                logger.exception('%s load failed', model_name)

    # ── Per-symbol dispatch ──────────────────────────────────────────────────

    def _run_one(
        self,
        symbol:     str,
        model_name: str,
        state:      dict,
        signal_ts:  int,
    ) -> None:
        context_len = state['context_len']
        predictor   = state['predictor']

        rows = self._fetch_ohlcv(symbol, context_len)
        if rows is None or len(rows) < context_len:
            log_event(MODULE, 'warning', 'warning',
                      f'{model_name} {symbol}: insufficient OHLCV '
                      f'({len(rows) if rows else 0}/{context_len} candles) — skipped')
            return

        result = self._predict_one_model(model_name, predictor, rows, context_len)
        if result is None:
            return

        direction, confidence, predicted_return_pct = result

        self._write_shadow(
            symbol=symbol,
            model_name=model_name,
            direction=direction,
            confidence=confidence,
            predicted_return=predicted_return_pct,
            context_candles=context_len,
            signal_timestamp=signal_ts,
        )
        logger.info(
            'Shadow signal: %s %s %s conf=%.4f ret=%+.2f%%',
            model_name, symbol, direction, confidence, predicted_return_pct,
        )

    # ── Foundation model predict ─────────────────────────────────────────────

    def _predict_one_model(
        self,
        model_name:  str,
        predictor,
        rows:        list[dict],
        context_len: int,
    ) -> Optional[tuple[str, float, float]]:
        """
        Calls KronosPredictor.predict() and extracts signal.
        Returns (direction, confidence, predicted_return_pct) or None on error.
        """
        try:
            ohlcv_df = pd.DataFrame([
                {
                    'open':   r['open'],
                    'high':   r['high'],
                    'low':    r['low'],
                    'close':  r['close'],
                    'volume': r['volume'],
                    'amount': 0.0,
                }
                for r in rows
            ])

            ts_unix   = [r['timestamp'] for r in rows]
            last_ts   = ts_unix[-1]
            future_ts = [last_ts + (i + 1) * 4 * 3600 for i in range(PRED_LEN)]

            x_timestamp = pd.Series(pd.to_datetime(ts_unix,   unit='s', utc=True))
            y_timestamp = pd.Series(pd.to_datetime(future_ts, unit='s', utc=True))

            pred_df = predictor.predict(
                df=ohlcv_df,
                x_timestamp=x_timestamp,
                y_timestamp=y_timestamp,
                pred_len=PRED_LEN,
                T=1.0,
                top_p=0.9,
                sample_count=_SAMPLE_COUNT,
                verbose=False,
            )
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'{model_name} predict() failed: {exc}')
            logger.exception('%s predict failed', model_name)
            return None

        return self._extract_signal(pred_df, rows)

    # ── Confidence formula (identical to KronosInference in M4) ─────────────

    def _extract_signal(
        self,
        pred_df: pd.DataFrame,
        rows:    list[dict],
    ) -> Optional[tuple[str, float, float]]:
        """
        Returns (direction, confidence, predicted_return_pct).
        Formula is byte-for-byte identical to M4 to ensure comparable scores.
        """
        try:
            pred_closes = pred_df['close'].values
        except Exception:
            return None

        if len(pred_closes) == 0:
            return None

        current_close = rows[-1]['close']
        if current_close <= 0:
            return None

        direction            = 'long' if pred_closes[-1] > current_close else 'short'
        predicted_return     = (pred_closes[-1] - current_close) / current_close
        predicted_return_pct = predicted_return * 100.0

        atr_pct = _compute_atr_pct(rows)
        if atr_pct <= 0:
            atr_pct = abs(predicted_return) if abs(predicted_return) > 0 else 1e-6

        base_conf   = min(1.0, abs(predicted_return) / (2.0 * atr_pct))
        n_agree     = sum(
            1 for pc in pred_closes
            if (pc > current_close) == (direction == 'long')
        )
        consistency = n_agree / len(pred_closes)
        confidence  = round(base_conf * consistency, 4)

        return direction, confidence, round(predicted_return_pct, 4)

    # ── DB helpers ───────────────────────────────────────────────────────────

    def _fetch_ohlcv(self, symbol: str, n_candles: int) -> Optional[list[dict]]:
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT open, high, low, close, volume, timestamp
                       FROM ohlcv
                       WHERE symbol=? AND timeframe=?
                       ORDER BY timestamp DESC
                       LIMIT ?""",
                    (symbol, TIMEFRAME, n_candles),
                ).fetchall()
            return [dict(r) for r in reversed(rows)]
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'OHLCV fetch failed ({symbol}, n={n_candles}): {exc}')
            return None

    def _write_shadow(
        self,
        symbol:           str,
        model_name:       str,
        direction:        str,
        confidence:       float,
        predicted_return: float,
        context_candles:  int,
        signal_timestamp: int,
    ) -> None:
        try:
            with get_connection() as conn:
                conn.execute(
                    """INSERT INTO shadow_signals
                           (symbol, model_name, direction, confidence,
                            predicted_return, context_candles, signal_timestamp)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (symbol, model_name, direction, float(confidence),
                     float(predicted_return), context_candles, signal_timestamp),
                )
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'Failed to write shadow signal ({model_name} {symbol}): {exc}')

    def _active_symbols(self) -> list[str]:
        symbols = [SLOT1_SYMBOL, SLOT2_SYMBOL]
        slot3   = self._load_slot3()
        if slot3:
            symbols.append(slot3)
        return symbols

    def _load_slot3(self) -> Optional[str]:
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE module='signal_generator'
                         AND event_type='slot3_selection'
                         AND data IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                ).fetchone()
            if row and row['data']:
                payload = json.loads(row['data'])
                return payload.get('slot3_symbol')
        except Exception:
            pass
        return None
