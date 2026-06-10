"""
Kronos Trading System — Module 16: Kronos-base 4H Signal Generator

Standalone signal generator using the NeoQuasar/Kronos-base foundation model
at 4H resolution. Runs every 4H at :05 UTC with full 512-candle 4H context
(≈ 85 days — full context capacity of Kronos-base).

Relationship to M14 (Kronos-base 1H):
  Same model weights. Different timeframe, horizon, and schedule.
  M14 answers: "can Kronos-base predict 6H direction at fine resolution?"
  M16 answers: "is Kronos-base deployable at 24H horizon under the fee/funding cost structure?"

Key differences from M14:
  - Timeframe: 4H  (vs 1H for M14)
  - Context:   512 × 4H ≈ 85 days   (vs 512 × 1H ≈ 21 days)
  - Horizon:   24H (6 × 4H)          (vs 6H for M14)
  - Schedule:  every 4H at :05 UTC   (vs every 1H for M14)
  - model_source: 'kronos-base-4h'   (vs 'kronos-base')

Key differences from M4 (custom model, also 4H/24H):
  Same timeframe and horizon as M4 — benchmark is directly comparable.
  Deeper context (85 days vs 16 days for M4).
  Foundation model (102M params) vs purpose-trained model (~1.4M params).

Cross-evaluation matrix at week 6:
  kronos-mini (1H) vs kronos-mini-4h : same model, timeframe effect
  kronos-base (1H) vs kronos-base-4h : same model, timeframe effect
  kronos-mini-4h   vs kronos-base-4h : model size effect at same timeframe
  kronos-mini-4h   vs custom (M4)    : foundation vs custom at same horizon

Writes to the shared signals table — processed by M5, M6, M8 identically to
all other model signals.

CPU-only on Windows (GPU TDR timeout with 50-sample probabilistic inference).
On Linux VPS set KRONOS_SHADOW_DEVICE=cuda to enable GPU.
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
log = logging.getLogger('base_4h_generator')
MODULE = 'base_4h_generator'

# ── Constants ──────────────────────────────────────────────────────────────────

MODEL_ID      = 'NeoQuasar/Kronos-base'
TOKENIZER_ID  = 'NeoQuasar/Kronos-Tokenizer-base'
MODEL_SOURCE  = 'kronos-base-4h'
TIMEFRAME     = '4h'
PRED_LEN      = 6           # 6 × 4H = 24H horizon — matches M4 custom model
HORIZON       = '24h'
ATR_PERIOD    = 14
SAMPLE_COUNT  = int(os.environ.get('KRONOS_SHADOW_SAMPLE_COUNT', '50'))

# Full 512-candle 4H context ≈ 85 days. Override via env if needed.
CONTEXT_LEN = int(os.environ.get('KRONOS_BASE_4H_CONTEXT', '512'))

SLOT1_SYMBOL = 'BTCUSD'
SLOT2_SYMBOL = 'ETHUSD'


# ── ATR helper ─────────────────────────────────────────────────────────────────

def _compute_atr_pct(rows: list[dict]) -> float:
    """14-period ATR as fraction of last close. Identical across all generators."""
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

class Base4HGenerator:
    """
    Module 16 — Kronos-base 4H signal generator.

    Loads the model once on startup. Generates signals every 4H at :05 UTC
    for BTC, ETH, and the current Slot 3 symbol.

    Uses predict_samples() for confidence — fraction of 50 stochastic samples
    agreeing on direction, not the broken path-consistency formula.
    """

    def __init__(self) -> None:
        self._predictor       = None
        self._model_ready     = False
        self._model_attempted = False

    # ── Model loading ──────────────────────────────────────────────────────────

    def _load_model(self) -> None:
        """One-time model load. Subsequent calls are no-ops."""
        if self._model_attempted:
            return
        self._model_attempted = True

        try:
            from model import Kronos, KronosTokenizer, KronosPredictor
        except ImportError as exc:
            log_event(MODULE, 'error', 'error',
                      f'vendor/kronos import failed — base-4h generator disabled: {exc}')
            log.error('vendor/kronos import failed: %s', exc)
            return

        try:
            tokenizer        = KronosTokenizer.from_pretrained(TOKENIZER_ID)
            model            = Kronos.from_pretrained(MODEL_ID)
            self._predictor  = KronosPredictor(model, tokenizer,
                                               max_context=CONTEXT_LEN)
            self._model_ready = True
            log_event(MODULE, 'info', 'info',
                      f'Kronos-base loaded for 4H (context={CONTEXT_LEN})')
            log.info('Kronos-base (4H) loaded — context=%d candles = %d days, device=%s',
                     CONTEXT_LEN, CONTEXT_LEN * 4 // 24, _DEVICE)
        except Exception as exc:
            log_event(MODULE, 'error', 'error',
                      f'Kronos-base-4h failed to load: {exc}')
            log.exception('Kronos-base-4h load failed')

    # ── Signal generation ──────────────────────────────────────────────────────

    def generate(self) -> None:
        """Generate signals for all active symbols. Called every 4H from scheduler."""
        self._load_model()
        if not self._model_ready:
            log_event(MODULE, 'warning', 'warning',
                      'Model not ready — skipping signal generation cycle')
            return

        symbols   = self._active_symbols()
        signal_ts = int(time.time())
        generated = 0

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
                  f'4H cycle complete: {generated}/{len(symbols)} signals generated',
                  {'symbols': symbols, 'generated': generated,
                   'context_len': CONTEXT_LEN, 'timeframe': TIMEFRAME})

    def _generate_one(self, symbol: str, signal_ts: int) -> bool:
        """Generate and write one signal for symbol. Returns True if written."""
        rows = self._fetch_ohlcv(symbol, CONTEXT_LEN)
        if rows is None or len(rows) < CONTEXT_LEN:
            log_event(MODULE, 'warning', 'warning',
                      f'{symbol}: insufficient 4H OHLCV '
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
        """
        Run Kronos-base inference at 4H resolution.
        Returns (direction, confidence, predicted_return_pct) or None on failure.

        Confidence formula — sample-distribution based:
          sample_finals    = final close of each of the SAMPLE_COUNT stochastic samples
          p_direction      = fraction of samples agreeing with predicted direction
          directional_conf = (p_direction - 0.5) × 2   [0=50/50 split, 1=all agree]
          mag_conf         = min(1.0, |mean_return| / (2 × atr_pct))
          confidence       = directional_conf × mag_conf
        """
        try:
            ohlcv_df = pd.DataFrame([
                {'open':   r['open'],  'high':  r['high'],
                 'low':    r['low'],   'close': r['close'],
                 'volume': r['volume']}
                for r in rows
            ])

            last_ts     = rows[-1]['timestamp']
            step_secs   = 4 * 3600              # 4H in seconds
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

            # ── Sample-distribution confidence ──────────────────────────────
            # predict_batch() with SAMPLE_COUNT copies of the same input runs all
            # samples in one batched GPU forward pass. Each batch element samples
            # independently (T=1.0, top_p=0.9) → distinct stochastic paths.
            _CLOSE_IDX  = 3
            raw_samples = np.stack([
                df.values
                for df in self._predictor.predict_batch(
                    df_list=[ohlcv_df] * SAMPLE_COUNT,
                    x_timestamp_list=[x_timestamp] * SAMPLE_COUNT,
                    y_timestamp_list=[y_timestamp] * SAMPLE_COUNT,
                    pred_len=PRED_LEN,
                    T=1.0,
                    top_p=0.9,
                    sample_count=1,
                    verbose=False,
                )
            ], axis=0)  # (SAMPLE_COUNT, PRED_LEN, 6)

            sample_finals = raw_samples[:, -1, _CLOSE_IDX]
            if len(sample_finals) == 0:
                return None

            n_long    = int(np.sum(sample_finals > current_close))
            p_long    = n_long / len(sample_finals)
            direction = 'long' if p_long >= 0.5 else 'short'
            p_dir     = p_long if direction == 'long' else (1.0 - p_long)

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

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _fetch_ohlcv(self, symbol: str, n_candles: int) -> Optional[list[dict]]:
        """Fetch n_candles of 4H OHLCV from the ohlcv table, oldest first."""
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
        """Write signal to the shared signals table with model_source='kronos-base-4h'."""
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
        """Fixed 4-asset universe: BTC + ETH + BNB + XRP. Slot 5 reserved for LINK."""
        return ['BTCUSD', 'ETHUSD', 'BNBUSD', 'XRPUSD']

    # ── Scheduler ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start Module 16. Load model, then run on 4H cron at :05 UTC."""
        init_db()
        log_event(MODULE, 'info', 'info', 'Kronos-base 4H generator starting')

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._load_model)

        scheduler = AsyncIOScheduler(timezone='UTC')
        scheduler.add_job(
            self._job_generate,
            CronTrigger(hour='0,4,8,12,16,20', minute=5, timezone='UTC'),
            id='base_4h',
            name='Kronos-base 4H — 4H signal cycle',
            max_instances=1,
        )
        scheduler.start()
        log.info('Kronos-base 4H scheduler started — every 4H at :05 UTC, '
                 'context=%d candles ≈ %d days',
                 CONTEXT_LEN, CONTEXT_LEN * 4 // 24)

        await asyncio.Event().wait()

    async def _job_generate(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.generate)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    asyncio.run(Base4HGenerator().start())
