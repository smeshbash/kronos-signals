"""
Kronos Trading System — Module 14: Kronos-base Signal Generator

Standalone signal generator using the NeoQuasar/Kronos-base foundation model.
Runs every 1H at :05 UTC with full 512-candle 1H context (≈ 21 days).

Key differences from M4 (custom model):
  - Timeframe: 1H (vs 4H for custom)
  - Context: 512 candles (vs 96 for custom) — full capacity for Kronos-base
  - Horizon: 6H  (6 × 1H, vs 24H for custom)
  - Max hold: 1 day (4 × 6H, set by M6 from horizon)
  - model_source: 'kronos-base' in signals table

Writes to the shared signals table — processed by M5 and M6 identically to
M4 and M13 signals. model_source tag allows per-model analysis.
"""

import asyncio
import json
import logging
import os
import sys
import time
from typing import Optional

import numpy as np
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

_VENDOR_PATH = os.path.join(os.path.dirname(__file__), 'vendor', 'kronos')
if _VENDOR_PATH not in sys.path:
    sys.path.insert(0, _VENDOR_PATH)

_DEVICE = os.environ.get('KRONOS_SHADOW_DEVICE', 'cpu').lower()
if _DEVICE != 'cuda':
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

from db import get_connection, init_db, log_event, SIGNAL_REGIME_VERSION

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('base_generator')
MODULE = 'base_generator'

# ── Constants ──────────────────────────────────────────────────────────────────

MODEL_ID      = 'NeoQuasar/Kronos-base'
TOKENIZER_ID  = 'NeoQuasar/Kronos-Tokenizer-base'
MODEL_SOURCE  = 'kronos-base'
TIMEFRAME     = '1h'
PRED_LEN      = 6
HORIZON       = '6h'
ATR_PERIOD    = 14
SAMPLE_COUNT  = int(os.environ.get('KRONOS_SHADOW_SAMPLE_COUNT', '50'))

CONTEXT_LEN = int(os.environ.get('KRONOS_BASE_CONTEXT', '512'))  # full capacity

SLOT1_SYMBOL = 'BTCUSD'
SLOT2_SYMBOL = 'ETHUSD'


# ── ATR helper ─────────────────────────────────────────────────────────────────

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
    atr         = sum(trs) / len(trs)
    final_close = rows[-1]['close']
    return (atr / final_close) if final_close > 0 else 0.0


# ── Main class ─────────────────────────────────────────────────────────────────

class BaseGenerator:
    """
    Module 14 — Kronos-base signal generator.

    Loads the model once on startup. Generates signals every 1H at :05 UTC
    for BTC, ETH, and the current Slot 3 symbol.
    """

    def __init__(self) -> None:
        self._predictor       = None
        self._model_ready     = False
        self._model_attempted = False

    def _load_model(self) -> None:
        if self._model_attempted:
            return
        self._model_attempted = True

        try:
            from model import Kronos, KronosTokenizer, KronosPredictor
        except ImportError as exc:
            log_event(MODULE, 'error', 'error',
                      f'vendor/kronos import failed — base generator disabled: {exc}')
            log.error('vendor/kronos import failed: %s', exc)
            return

        try:
            tokenizer        = KronosTokenizer.from_pretrained(TOKENIZER_ID)
            model            = Kronos.from_pretrained(MODEL_ID)
            self._predictor  = KronosPredictor(model, tokenizer,
                                               max_context=CONTEXT_LEN)
            self._model_ready = True
            log_event(MODULE, 'info', 'info',
                      f'Kronos-base loaded (context={CONTEXT_LEN})')
            log.info('Kronos-base loaded — context=%d, device=%s',
                     CONTEXT_LEN, _DEVICE)
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'Kronos-base failed to load: {exc}')
            log.exception('Kronos-base load failed')

    def generate(self) -> None:
        self._load_model()
        if not self._model_ready:
            log_event(MODULE, 'warning', 'warning',
                      'Model not ready — skipping signal generation cycle')
            return

        symbols    = self._active_symbols()
        signal_ts  = int(time.time())
        generated  = 0

        for symbol in symbols:
            try:
                written = self._generate_one(symbol, signal_ts)
                if written:
                    generated += 1
            except Exception as exc:
                log_event(MODULE, 'error', 'error',
                          f'{symbol} unhandled error: {exc}')
                log.exception('%s signal generation failed', symbol)

        log_event(MODULE, 'info', 'heartbeat',
                  f'1H cycle complete: {generated}/{len(symbols)} signals generated',
                  {'symbols': symbols, 'generated': generated,
                   'context_len': CONTEXT_LEN, 'timeframe': TIMEFRAME})

    def _generate_one(self, symbol: str, signal_ts: int) -> bool:
        rows = self._fetch_ohlcv(symbol, CONTEXT_LEN)
        if rows is None or len(rows) < CONTEXT_LEN:
            log_event(MODULE, 'warning', 'warning',
                      f'{symbol}: insufficient 1H OHLCV '
                      f'({len(rows) if rows else 0}/{CONTEXT_LEN}) — skipped')
            return False

        result = self._predict(symbol, rows)
        if result is None:
            return False

        direction, confidence, predicted_return_pct = result
        self._write_signal(symbol, direction, confidence,
                           predicted_return_pct, signal_ts)
        log.info('Signal: %s %s conf=%.4f ret=%.2f%%',
                 symbol, direction.upper(), confidence, predicted_return_pct)
        return True

    def _predict(
        self,
        symbol: str,
        rows:   list[dict],
    ) -> Optional[tuple[str, float, float]]:
        try:
            ohlcv_df = pd.DataFrame([
                {'open':  r['open'],  'high':  r['high'],
                 'low':   r['low'],   'close': r['close'],
                 'volume': r['volume']}
                for r in rows
            ])

            # Build timestamp Series for context and prediction windows.
            # Kronos calc_time_stamps uses .dt accessor — must be datetime, not int.
            last_ts     = rows[-1]['timestamp']
            step_secs   = 3600
            x_timestamp = pd.to_datetime(pd.Series([
                last_ts - (CONTEXT_LEN - 1 - i) * step_secs
                for i in range(CONTEXT_LEN)
            ]), unit='s', utc=True)
            y_timestamp = pd.to_datetime(pd.Series([
                last_ts + (i + 1) * step_secs
                for i in range(PRED_LEN)
            ]), unit='s', utc=True)

            current_close = float(rows[-1]['close'])
            if current_close <= 0:
                return None

            # ── Sample-distribution confidence ────────────────────────────────
            # predict_samples() returns (SAMPLE_COUNT, PRED_LEN, 6_features).
            # Column order: open, high, low, close, volume, amount → close = idx 3.
            _CLOSE_IDX = 3
            raw_samples = self._predictor.predict_samples(
                df=ohlcv_df,
                x_timestamp=x_timestamp,
                y_timestamp=y_timestamp,
                pred_len=PRED_LEN,
                T=1.0,
                top_p=0.9,
                sample_count=SAMPLE_COUNT,
                verbose=False,
            )  # (SAMPLE_COUNT, PRED_LEN, 6)

            sample_finals = raw_samples[:, -1, _CLOSE_IDX]   # final close per sample
            if len(sample_finals) == 0:
                return None

            n_long    = int(np.sum(sample_finals > current_close))
            p_long    = n_long / len(sample_finals)
            direction = 'long' if p_long >= 0.5 else 'short'
            p_dir     = p_long if direction == 'long' else (1.0 - p_long)

            # directional_conf: 0.0 = pure 50/50 noise, 1.0 = all samples agree
            directional_conf = (p_dir - 0.5) * 2.0

            mean_final           = float(np.mean(sample_finals))
            predicted_return_pct = (mean_final - current_close) / current_close * 100.0

            atr_pct  = _compute_atr_pct(rows)
            mag_conf = min(1.0, abs(predicted_return_pct / 100.0) / (2.0 * atr_pct)) \
                       if atr_pct > 0 else 0.0
            confidence = round(directional_conf * mag_conf, 4)

            if abs(predicted_return_pct) > 20.0:
                log_event(MODULE, 'warning', 'warning',
                          f'{symbol}: predicted_return={predicted_return_pct:.2f}% '
                          f'exceeds 20% threshold — skipping')
                return None

            return direction, confidence, round(predicted_return_pct, 4)

        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'{symbol} inference failed: {exc}')
            log.exception('%s inference failed', symbol)
            return None

    def _fetch_ohlcv(self, symbol: str, n_candles: int) -> Optional[list[dict]]:
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT open, high, low, close, volume, timestamp
                       FROM ohlcv
                       WHERE symbol=? AND timeframe=?
                       ORDER BY timestamp DESC LIMIT ?""",
                    (symbol, TIMEFRAME, n_candles),
                ).fetchall()
            return [dict(r) for r in reversed(rows)]
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'OHLCV fetch failed ({symbol}): {exc}')
            return None

    def _write_signal(
        self,
        symbol:               str,
        direction:            str,
        confidence:           float,
        predicted_return_pct: float,
        signal_timestamp:     int,
    ) -> None:
        try:
            with get_connection() as conn:
                cur = conn.execute(
                    """INSERT INTO signals
                           (symbol, direction, confidence, horizon, status,
                            predicted_return_pct, signal_timestamp, model_source,
                            regime_version)
                       VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)""",
                    (symbol, direction, confidence, HORIZON,
                     predicted_return_pct, signal_timestamp, MODEL_SOURCE,
                     SIGNAL_REGIME_VERSION),
                )
                signal_id = cur.lastrowid

            log_event(MODULE, 'info', 'signal_generated',
                      f'{symbol} {direction.upper()} conf={confidence:.4f} '
                      f'ret={predicted_return_pct:+.2f}% horizon={HORIZON}',
                      {'signal_id':   signal_id, 'symbol': symbol,
                       'direction':   direction, 'confidence': confidence,
                       'predicted_return_pct': predicted_return_pct,
                       'horizon':     HORIZON,   'model_source': MODEL_SOURCE,
                       'context_len': CONTEXT_LEN})
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'Failed to write signal for {symbol}: {exc}')
            log.exception('signal write failed for %s', symbol)

    def _active_symbols(self) -> list[str]:
        symbols = [SLOT1_SYMBOL, SLOT2_SYMBOL]
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='slot3_selection'
                         AND data IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                ).fetchone()
            if row and row['data']:
                payload = json.loads(row['data'])
                slot3   = payload.get('slot3_symbol') or payload.get('symbol')
                if slot3 and slot3 not in symbols:
                    symbols.append(slot3)
        except Exception as exc:
            log.warning('Could not read slot3_selection: %s', exc)
        return symbols

    async def start(self) -> None:
        init_db()
        log_event(MODULE, 'info', 'info', 'Kronos-base generator starting')

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._load_model)

        scheduler = AsyncIOScheduler(timezone='UTC')
        scheduler.add_job(
            self._job_generate,
            CronTrigger(minute=5, timezone='UTC'),
            id='base_1h',
            name='Kronos-base — 1H signal cycle',
            max_instances=1,
        )
        scheduler.start()
        log.info('Kronos-base scheduler started — every 1H at :05 UTC, context=%d',
                 CONTEXT_LEN)

        await asyncio.Event().wait()

    async def _job_generate(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.generate)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    asyncio.run(BaseGenerator().start())
