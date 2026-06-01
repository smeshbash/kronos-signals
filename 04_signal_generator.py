"""
Kronos Trading System — Module 4: Signal Generator
Sections 5.1, 5.2, 7.1, 11.2, 18.2, 19.1, 20.1–20.7 of the requirements spec (v2.2).

Runs Kronos PyTorch inference on the latest OHLCV data to produce directional
signals. Two operation modes:

1. Regular 4H cycle (cron: 00:05/04:05/08:05/12:05/16:05/20:05 UTC):
   Runs on BTC, ETH, and the active Slot 3 asset. Fires 3 minutes after
   Module 1's OHLCV cron (:02 UTC) — satisfies the Section 7.1 sequencing
   constraint: new candle is in the DB before inference runs.

2. Weekly Slot 3 selection (cron: every Sunday at 00:03 UTC):
   Runs inference on SOL, BNB, XRP and selects the symbol with the highest
   confidence score that also passes the confidence threshold (if set) and the
   correlation check (Section 5.2 / 11.2). Runs at 00:03 so the updated Slot 3
   is ready for Sunday's first 4H cycle at 00:05.

Output: KronosSignal dataclass
  symbol               — Delta symbol e.g. 'BTCUSD'
  direction            — 'long' | 'short'
  confidence           — float in [0.0, 1.0], derived from predicted return
                         relative to 14-period ATR. A move of 2x ATR = 1.0.
  horizon              — e.g. '24h' (PRED_LEN x 4H)
  predicted_return_pct — signed % predicted price change from current close
  candles_used         — number of OHLCV candles fed to the model
  signal_timestamp     — Unix epoch seconds of this signal

Model interface (KronosInference):
  Loads a PyTorch model from KRONOS_MODEL_PATH. Tries torch.jit.load() first
  (TorchScript), then torch.load() fallback. If neither succeeds or torch is
  not installed, is_ready=False and an empty signal list is returned.

  Input tensor:  [1, SEQ_LEN, 5]  — instance-normalised OHLCV
  Output tensor: [1, PRED_LEN, 5] — instance-denormalised predicted OHLCV

Confidence formula (Section 20.2 — calibrated empirically during pre-live):
  atr_pct       = mean 14-period ATR as fraction of close price
  base_conf     = |predicted_return| / (2 x atr_pct)  clamped [0, 1]
  consistency   = fraction of PRED_LEN steps whose sign matches direction
  confidence    = base_conf x consistency

Model output anomaly detection (Section 19.1 item 4):
  If predicted return exceeds MAX_PREDICTED_RETURN_PCT (env: KRONOS_MAX_PRED_RETURN,
  default 20%) or output contains NaN/Inf or non-positive close prices, the
  inference result is discarded and a critical 'forced_override' event is logged.

Asset exclusion (Section 20.6):
  1. Extreme funding rate (>EXTREME_FUNDING_THRESHOLD = 0.3%/8H): checked inline
     from the funding_rates table before each signal generation cycle.
  2. Win rate, consecutive losses, liquidity exclusions: Module 4 reads
     event_type='asset_exclusion' events (written by Module 8/11). The most
     recent event per symbol determines state: excluded=True skips generation.

Slot 3 selection (Section 5.2):
  Candidates ranked by confidence, then filtered by:
  a) Confidence threshold — None during pre-live (no threshold). When set via
     event_type='confidence_threshold_set', only candidates above it are eligible.
  b) Correlation check — rolling 7-day Pearson > 0.85 same-direction = BLOCKED
     (Section 11.2). Direction compared against most recent signal for BTC/ETH.
     If no reference signal exists, allow (cannot confirm violation).

Slot 3 tracking:
  Active Slot 3 symbol persisted in events table as event_type='slot3_selection'.
  Confidence threshold persisted as event_type='confidence_threshold_set'.
"""

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import asdict, dataclass
from typing import Optional

from db import get_connection, init_db, log_event, SIGNAL_REGIME_VERSION

try:
    from shadow_inference import ShadowInference
    _SHADOW_AVAILABLE = True
except ImportError:
    _SHADOW_AVAILABLE = False

logger = logging.getLogger(__name__)

MODULE = 'signal_generator'

# ── Constants ──────────────────────────────────────────────────────────────────

SLOT1_SYMBOL = 'BTCUSD'
SLOT2_SYMBOL = 'ETHUSD'
SLOT3_CANDIDATES = ['SOLUSD', 'BNBUSD', 'XRPUSD']

TIMEFRAME = '4h'

# Candles fed to Kronos (SEQ_LEN x 4H = look-back window).
SEQ_LEN = int(os.environ.get('KRONOS_SEQ_LEN', '96'))    # 96 x 4H = 16 days

# Candles predicted by Kronos (PRED_LEN x 4H = forecast horizon).
PRED_LEN = int(os.environ.get('KRONOS_PRED_LEN', '6'))   # 6 x 4H = 24H

HORIZON = f'{PRED_LEN * 4}h'   # '24h' at default PRED_LEN=6

# Path to Kronos model weights file. Must be set before live trading.
MODEL_PATH = os.environ.get(
    'KRONOS_MODEL_PATH',
    os.path.join(os.path.dirname(__file__), 'kronos_model.pt'),
)

ATR_PERIOD = 14   # ATR lookback for confidence normalisation

# Model output anomaly threshold (Section 19.1 item 4).
# Predicted moves above this % are treated as statistically impossible and
# trigger a forced_override event.
MAX_PREDICTED_RETURN_PCT = float(os.environ.get('KRONOS_MAX_PRED_RETURN', '20.0'))

# Correlation check constants (Section 5.2 / 11.2).
# Rolling 7-day Pearson correlation above this threshold + same direction = BLOCKED.
CORRELATION_BLOCK_THRESHOLD = 0.85
CORRELATION_PERIOD_CANDLES  = 42   # 7 days x 6 4H candles

# Asset-level exclusion: funding rate threshold (Section 20.6).
# Matches Module 2's REGIME_FUNDING_LOW_LIQ (0.3%/8H per Section 9.2).
EXTREME_FUNDING_THRESHOLD = 0.003  # 0.3% per 8H


# ── Dataclass ─────────────────────────────────────────────────────────────────

@dataclass
class KronosSignal:
    symbol:               str    # 'BTCUSD' etc.
    direction:            str    # 'long' | 'short'
    confidence:           float  # 0.0-1.0
    horizon:              str    # e.g. '24h'
    predicted_return_pct: float  # signed %, e.g. +1.5 or -0.8
    candles_used:         int    # number of input candles
    signal_timestamp:     int    # Unix epoch seconds


# ── Kronos model inference wrapper ────────────────────────────────────────────

class KronosInference:
    """
    Wraps the pretrained Kronos PyTorch model for OHLCV sequence inference.

    Tries to load the model from MODEL_PATH on construction. If torch is not
    installed or the file is missing/corrupt, is_ready=False. Callers must
    check is_ready before calling predict().
    """

    def __init__(self, model_path: str = MODEL_PATH) -> None:
        self._model_path = model_path
        self._model = None
        self._torch = None
        self._try_load()

    @property
    def is_ready(self) -> bool:
        return self._model is not None

    def _try_load(self) -> None:
        try:
            import torch
            self._torch = torch
        except ImportError:
            log_event(MODULE, 'warning', 'model_unavailable',
                      'torch not installed — Kronos inference unavailable; '
                      'install PyTorch to enable signal generation')
            return

        if not os.path.exists(self._model_path):
            log_event(MODULE, 'warning', 'model_unavailable',
                      f'Kronos model weights not found at {self._model_path} — '
                      f'signal generation disabled until weights are installed')
            return

        try:
            try:
                self._model = self._torch.jit.load(
                    self._model_path, map_location='cpu')
            except Exception:
                self._model = self._torch.load(
                    self._model_path, map_location='cpu')
                if hasattr(self._model, 'eval'):
                    self._model.eval()
            log_event(MODULE, 'info', 'model_loaded',
                      f'Kronos model loaded from {self._model_path}')
        except Exception as e:
            log_event(MODULE, 'error', 'error',
                      f'Failed to load Kronos model from {self._model_path}: {e}')
            logger.exception('Kronos model load failed')
            self._model = None

    def _predict_raw(
        self,
        ohlcv_matrix: list[list[float]],   # [seq_len][5] normalised
    ) -> Optional[list[list[float]]]:
        """
        Run model forward pass. Returns predicted [pred_len][5] or None.

        Intentionally a thin wrapper — the verify script monkey-patches this
        method with synthetic predictions to test the full pipeline without
        real model weights.
        """
        if not self.is_ready:
            return None
        torch = self._torch
        try:
            with torch.no_grad():
                x = torch.tensor(ohlcv_matrix, dtype=torch.float32).unsqueeze(0)
                out = self._model(x)
                return out.squeeze(0).tolist()   # [pred_len][5]
        except Exception as e:
            log_event(MODULE, 'error', 'error',
                      f'Kronos forward pass failed: {e}')
            logger.exception('Kronos forward pass error')
            return None

    def predict(
        self,
        rows: list[dict],   # SQLite Row dicts: open, high, low, close, volume
        symbol: str = '',   # for anomaly log context
    ) -> Optional[tuple[str, float, float, str, int]]:
        """
        Run inference on a list of OHLCV row dicts.

        Returns (direction, confidence, predicted_return_pct, horizon, candles_used)
        or None if inference is unavailable, fails, or output is anomalous.

        Normalisation: per-channel instance normalisation (mean/std over the
        input window, RevIN-style) applied before forward pass, reversed on output.
        """
        if not self.is_ready:
            return None
        if len(rows) < SEQ_LEN:
            return None

        rows = rows[-SEQ_LEN:]
        candles_used = len(rows)

        matrix = [
            [r['open'], r['high'], r['low'], r['close'], r['volume']]
            for r in rows
        ]

        # Per-channel instance normalisation
        n_features = 5
        means = [0.0] * n_features
        stds  = [1.0] * n_features

        for col in range(n_features):
            vals = [matrix[row][col] for row in range(candles_used)]
            m = sum(vals) / candles_used
            variance = sum((v - m) ** 2 for v in vals) / candles_used
            means[col] = m
            stds[col]  = math.sqrt(variance) + 1e-8

        norm_matrix = [
            [(matrix[row][col] - means[col]) / stds[col]
             for col in range(n_features)]
            for row in range(candles_used)
        ]

        raw_pred = self._predict_raw(norm_matrix)
        if raw_pred is None:
            return None

        # Denormalise predicted close prices (column index 3)
        pred_closes = [
            raw_pred[step][3] * stds[3] + means[3]
            for step in range(len(raw_pred))
        ]

        # ── Model output anomaly detection (Section 19.1 item 4) ──────────
        if not all(math.isfinite(pc) for pc in pred_closes):
            log_event(MODULE, 'critical', 'forced_override',
                      f'Kronos model output anomaly ({symbol}): '
                      f'non-finite predicted close values — halting inference',
                      {'symbol': symbol,
                       'pred_closes_preview': [str(p) for p in pred_closes[:3]]})
            return None

        if any(pc <= 0 for pc in pred_closes):
            log_event(MODULE, 'critical', 'forced_override',
                      f'Kronos model output anomaly ({symbol}): '
                      f'non-positive predicted close price',
                      {'symbol': symbol,
                       'pred_closes_preview': pred_closes[:3]})
            return None

        current_close = rows[-1]['close']
        if current_close <= 0:
            return None

        final_pred_close = pred_closes[-1]
        predicted_return = (final_pred_close - current_close) / current_close
        predicted_return_pct = predicted_return * 100.0

        if abs(predicted_return_pct) > MAX_PREDICTED_RETURN_PCT:
            log_event(MODULE, 'critical', 'forced_override',
                      f'Kronos model output anomaly ({symbol}): '
                      f'predicted return {predicted_return_pct:.2f}% exceeds '
                      f'maximum allowed {MAX_PREDICTED_RETURN_PCT:.1f}% — '
                      f'statistically impossible forecast',
                      {'symbol': symbol,
                       'predicted_return_pct': predicted_return_pct,
                       'current_close': current_close,
                       'final_pred_close': final_pred_close})
            return None
        # ── End anomaly detection ──────────────────────────────────────────

        direction = 'long' if predicted_return > 0 else 'short'

        atr_pct = self._compute_atr_pct(rows)
        if atr_pct <= 0:
            atr_pct = abs(predicted_return) if abs(predicted_return) > 0 else 1e-6

        base_confidence = min(1.0, abs(predicted_return) / (2.0 * atr_pct))

        n_agree = sum(
            1 for pc in pred_closes
            if (pc > current_close) == (direction == 'long')
        )
        consistency = n_agree / len(pred_closes)
        confidence = round(base_confidence * consistency, 4)

        return (direction, confidence, round(predicted_return_pct, 4),
                HORIZON, candles_used)

    @staticmethod
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


# ── Signal Generator ──────────────────────────────────────────────────────────

class SignalGenerator:
    """
    Orchestrates 4H signal generation and weekly Slot 3 selection.

    Usage (standalone service):
        gen = SignalGenerator()
        gen.run()

    Usage (called by orchestrator):
        gen = SignalGenerator()
        signals = gen.generate()      # list[KronosSignal]
        slot3   = gen.select_slot3()  # Optional[str]
    """

    def __init__(self) -> None:
        self._inference          = KronosInference(MODEL_PATH)
        self._current_slot3:     Optional[str]   = self._load_slot3()
        self._confidence_threshold: Optional[float] = self._load_confidence_threshold()
        self._excluded_symbols:  set             = self._load_exclusions()
        self._shadow = ShadowInference() if _SHADOW_AVAILABLE else None

    # ── Public API ──────────────────────────────────────────────────────────

    def generate(self) -> list[KronosSignal]:
        """
        Run 4H signal cycle on BTC, ETH, and active Slot 3.
        Skips assets that are excluded (Section 20.6) or have extreme
        funding rates. Returns empty list if the model is not ready.
        """
        if not self._inference.is_ready:
            log_event(MODULE, 'warning', 'signal_skipped',
                      'Signal generation skipped — Kronos model not loaded')
            return []

        # Refresh exclusion state each cycle
        self._excluded_symbols = self._load_exclusions()

        symbols = [SLOT1_SYMBOL, SLOT2_SYMBOL]
        if self._current_slot3:
            symbols.append(self._current_slot3)

        signals = []
        for symbol in symbols:
            # Check static exclusion (win rate, consecutive losses, liquidity)
            if symbol in self._excluded_symbols:
                log_event(MODULE, 'info', 'signal_skipped',
                          f'{symbol}: skipped — excluded from signal generation '
                          f'(Section 20.6 exclusion rule active)')
                continue

            # Check real-time extreme funding rate (Section 20.6)
            reason = self._check_extreme_funding(symbol)
            if reason:
                log_event(MODULE, 'warning', 'signal_skipped',
                          f'{symbol}: skipped — {reason}')
                continue

            sig = self._generate_for_symbol(symbol)
            if sig is not None:
                signals.append(sig)

        if not signals:
            log_event(MODULE, 'info', 'signal_skipped',
                      'No signals generated this cycle '
                      '(insufficient OHLCV data or all assets excluded/blocked)')

        return signals

    def select_slot3(self) -> Optional[str]:
        """
        Weekly Slot 3 selection (Section 5.2 — run every Sunday at 00:03 UTC).

        Generates signals for all SOL/BNB/XRP candidates (all written to DB
        per Section 20.2 — pre-live logs everything). Then selects the first
        candidate (highest confidence) that passes:
          a) Confidence threshold (if set — None = pre-live, no threshold)
          b) Correlation check: rolling 7-day Pearson > 0.85 same-direction
             with BTC or ETH = rejected (Section 11.2)

        Returns the winning symbol or None if Slot 3 is left empty.
        """
        if not self._inference.is_ready:
            log_event(MODULE, 'warning', 'slot3_selection',
                      'Slot 3 selection skipped — Kronos model not loaded')
            return self._current_slot3

        # Reload threshold in case it was calibrated since last run
        self._confidence_threshold = self._load_confidence_threshold()

        # Generate signals for all 3 candidates — all written to DB
        candidates: list[tuple[float, str, KronosSignal]] = []
        for symbol in SLOT3_CANDIDATES:
            sig = self._generate_for_symbol(symbol)
            if sig is not None:
                candidates.append((sig.confidence, symbol, sig))

        if not candidates:
            log_event(MODULE, 'info', 'slot3_selection',
                      'Slot 3 empty — no signals from SOL/BNB/XRP candidates')
            self._current_slot3 = None
            self._persist_slot3(None)
            return None

        candidates.sort(key=lambda t: t[0], reverse=True)

        threshold = self._confidence_threshold
        winner_symbol: Optional[str] = None
        winner_sig: Optional[KronosSignal] = None
        winner_conf: float = 0.0
        rejections: list[str] = []

        for conf, symbol, sig in candidates:
            # a) Confidence threshold check
            if threshold is not None and conf < threshold:
                reason = (f'confidence {conf:.4f} below '
                          f'calibrated threshold {threshold:.4f}')
                log_event(MODULE, 'info', 'slot3_selection',
                          f'{symbol}: rejected — {reason}')
                rejections.append(f'{symbol}(threshold)')
                continue

            # b) Correlation check (Section 5.2 / 11.2)
            if not self._correlation_check(sig):
                log_event(MODULE, 'info', 'slot3_selection',
                          f'{symbol}: rejected — correlation rule violated '
                          f'(rolling 7-day Pearson >{CORRELATION_BLOCK_THRESHOLD} '
                          f'same-direction with BTC or ETH, Section 11.2)')
                rejections.append(f'{symbol}(correlation)')
                continue

            # Passed all checks
            winner_symbol = symbol
            winner_sig    = sig
            winner_conf   = conf
            break

        if winner_symbol is None:
            log_event(MODULE, 'info', 'slot3_selection',
                      f'Slot 3 empty — all candidates rejected: {rejections}',
                      {'rejected': rejections, 'slot3_symbol': None})
            self._current_slot3 = None
            self._persist_slot3(None)
            return None

        self._current_slot3 = winner_symbol
        self._persist_slot3(winner_symbol)

        ranking = ', '.join(
            f'{sym}={c:.4f}' for c, sym, _ in candidates
        )
        log_event(MODULE, 'info', 'slot3_selection',
                  f'Slot 3 selected: {winner_symbol} '
                  f'(confidence={winner_conf:.4f}, '
                  f'direction={winner_sig.direction}) '
                  f'[ranking: {ranking}]',
                  {'selected': winner_symbol,
                   'confidence': winner_conf,
                   'direction': winner_sig.direction,
                   'ranking': {sym: c for c, sym, _ in candidates},
                   'slot3_symbol': winner_symbol})
        return winner_symbol

    @property
    def current_slot3(self) -> Optional[str]:
        return self._current_slot3

    # ── Internal: signal generation ─────────────────────────────────────────

    def _generate_for_symbol(self, symbol: str) -> Optional[KronosSignal]:
        rows = self._fetch_ohlcv(symbol)
        if rows is None or len(rows) < SEQ_LEN:
            log_event(MODULE, 'info', 'signal_generated',
                      f'{symbol}: insufficient OHLCV data '
                      f'({len(rows) if rows else 0}/{SEQ_LEN} candles)')
            return None

        result = self._inference.predict(rows, symbol=symbol)
        if result is None:
            return None

        direction, confidence, predicted_return_pct, horizon, candles_used = result

        signal = KronosSignal(
            symbol=symbol,
            direction=direction,
            confidence=confidence,
            horizon=horizon,
            predicted_return_pct=predicted_return_pct,
            candles_used=candles_used,
            signal_timestamp=int(time.time()),
        )

        signal_id = self._write_signal(signal)
        self._log_signal(signal, signal_id)
        return signal

    # ── Internal: asset exclusion ────────────────────────────────────────────

    @staticmethod
    def _load_exclusions() -> set:
        """
        Read the most recent asset_exclusion event per symbol.
        Returns a set of currently excluded symbol strings.

        Other modules (8, 11) write the exclusion state:
          event_type='asset_exclusion',
          data={'symbol': 'BTCUSD', 'excluded': True, 'reason': '...'}
        Re-instatement: same structure with excluded=False.
        """
        excluded = set()
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='asset_exclusion' AND data IS NOT NULL
                       ORDER BY timestamp DESC""",
                ).fetchall()
            seen: set = set()
            for row in rows:
                try:
                    payload = json.loads(row['data'])
                    sym = payload.get('symbol')
                    if sym and sym not in seen:
                        seen.add(sym)
                        if payload.get('excluded', False):
                            excluded.add(sym)
                except Exception:
                    pass
        except Exception:
            pass
        return excluded

    @staticmethod
    def _check_extreme_funding(symbol: str) -> Optional[str]:
        """
        Returns a human-readable reason string if the asset's latest funding
        rate exceeds EXTREME_FUNDING_THRESHOLD (0.3%/8H — Section 20.6).
        Returns None if the asset is safe to trade.
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT rate FROM funding_rates
                       WHERE symbol=? ORDER BY timestamp DESC LIMIT 1""",
                    (symbol,),
                ).fetchone()
            if row and abs(row['rate']) > EXTREME_FUNDING_THRESHOLD:
                return (f'extreme funding rate {row["rate"]:.4f}/8H '
                        f'(>{EXTREME_FUNDING_THRESHOLD:.3f} threshold, '
                        f'Section 20.6 exclusion)')
        except Exception:
            pass
        return None

    # ── Internal: Slot 3 correlation check ───────────────────────────────────

    def _correlation_check(self, candidate_sig: KronosSignal) -> bool:
        """
        Section 5.2 / 11.2: Returns True if the Slot 3 candidate passes the
        correlation check (can be selected); False if the rule is violated.

        Rule: rolling 7-day Pearson correlation > 0.85 AND same direction as
        the reference symbol's most recent signal = BLOCKED (Section 11.2).
        If the reference symbol has no recent signal, allow the candidate
        (cannot confirm a same-direction violation without direction data).
        """
        for ref_symbol in [SLOT1_SYMBOL, SLOT2_SYMBOL]:
            corr = self._compute_correlation(candidate_sig.symbol, ref_symbol)
            if corr is None or corr <= CORRELATION_BLOCK_THRESHOLD:
                continue
            # Correlation exceeds threshold — need direction to confirm violation
            ref_dir = self._get_last_signal_direction(ref_symbol)
            if ref_dir is not None and ref_dir == candidate_sig.direction:
                return False   # Same direction + high correlation = BLOCKED
        return True

    @staticmethod
    def _compute_correlation(sym1: str, sym2: str) -> Optional[float]:
        """
        Rolling 7-day Pearson correlation of 4H close returns between sym1 and sym2.
        Returns None if insufficient data (< 2 return observations).
        """
        try:
            with get_connection() as conn:
                def _fetch(symbol: str) -> list[float]:
                    rows = conn.execute(
                        """SELECT close FROM ohlcv
                           WHERE symbol=? AND timeframe=?
                           ORDER BY timestamp DESC LIMIT ?""",
                        (symbol, TIMEFRAME, CORRELATION_PERIOD_CANDLES + 1),
                    ).fetchall()
                    return [r['close'] for r in reversed(rows)]

                closes1 = _fetch(sym1)
                closes2 = _fetch(sym2)
        except Exception:
            return None

        if len(closes1) < 2 or len(closes2) < 2:
            return None

        def _returns(closes: list[float]) -> list[float]:
            return [(closes[i] - closes[i - 1]) / closes[i - 1]
                    for i in range(1, len(closes))
                    if closes[i - 1] != 0]

        r1 = _returns(closes1)
        r2 = _returns(closes2)
        n = min(len(r1), len(r2))
        if n < 2:
            return None

        r1 = r1[-n:]
        r2 = r2[-n:]

        mean1 = sum(r1) / n
        mean2 = sum(r2) / n
        cov   = sum((r1[i] - mean1) * (r2[i] - mean2) for i in range(n))
        std1  = math.sqrt(sum((x - mean1) ** 2 for x in r1))
        std2  = math.sqrt(sum((x - mean2) ** 2 for x in r2))

        if std1 < 1e-10 or std2 < 1e-10:
            return None
        return cov / (std1 * std2)

    @staticmethod
    def _get_last_signal_direction(symbol: str) -> Optional[str]:
        """Return the most recent signal direction for a symbol within the last 8H."""
        cutoff = int(time.time()) - 8 * 3600
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT direction FROM signals
                       WHERE symbol=? AND signal_timestamp >= ?
                       ORDER BY signal_timestamp DESC LIMIT 1""",
                    (symbol, cutoff),
                ).fetchone()
            return row['direction'] if row else None
        except Exception:
            return None

    # ── Internal: DB helpers ─────────────────────────────────────────────────

    def _fetch_ohlcv(self, symbol: str) -> Optional[list]:
        limit = SEQ_LEN + ATR_PERIOD + 1
        try:
            with get_connection() as conn:
                rows = conn.execute(
                    """SELECT open, high, low, close, volume
                       FROM ohlcv
                       WHERE symbol=? AND timeframe=?
                       ORDER BY timestamp DESC
                       LIMIT ?""",
                    (symbol, TIMEFRAME, limit),
                ).fetchall()
            return list(reversed(rows))
        except Exception as e:
            log_event(MODULE, 'error', 'error',
                      f'Failed to fetch OHLCV for {symbol}: {e}')
            return None

    def _write_signal(self, signal: KronosSignal) -> int:
        with get_connection() as conn:
            cur = conn.execute(
                """INSERT INTO signals
                       (symbol, direction, confidence, horizon, status,
                        predicted_return_pct, signal_timestamp, regime_version)
                   VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)""",
                (signal.symbol, signal.direction, signal.confidence,
                 signal.horizon, signal.predicted_return_pct,
                 signal.signal_timestamp, SIGNAL_REGIME_VERSION),
            )
            return cur.lastrowid

    def _log_signal(self, signal: KronosSignal, signal_id: int) -> None:
        sign = '+' if signal.predicted_return_pct >= 0 else ''
        message = (
            f'{signal.symbol} {signal.direction.upper()} '
            f'conf={signal.confidence:.4f} '
            f'ret={sign}{signal.predicted_return_pct:.2f}% '
            f'horizon={signal.horizon}'
        )
        payload = asdict(signal)
        payload['signal_id'] = signal_id
        log_event(MODULE, 'info', 'signal_generated', message, payload)

    def _persist_slot3(self, symbol: Optional[str]) -> None:
        log_event(MODULE, 'info', 'slot3_selection',
                  f'slot3={symbol}',
                  {'slot3_symbol': symbol})

    def _load_slot3(self) -> Optional[str]:
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE module=? AND event_type='slot3_selection'
                         AND data IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                    (MODULE,),
                ).fetchone()
            if row and row['data']:
                payload = json.loads(row['data'])
                return payload.get('slot3_symbol')
        except Exception:
            pass
        return None

    def _load_confidence_threshold(self) -> Optional[float]:
        """
        Read the most recent confidence_threshold_set event.
        Returns None during pre-live (no threshold calibrated yet).
        Written by external calibration tooling after pre-live analysis:
          event_type='confidence_threshold_set',
          data={'threshold': 0.65}  -- example calibrated value
        """
        try:
            with get_connection() as conn:
                row = conn.execute(
                    """SELECT data FROM events
                       WHERE event_type='confidence_threshold_set'
                         AND module='signal_generator'
                         AND data IS NOT NULL
                       ORDER BY timestamp DESC LIMIT 1""",
                ).fetchone()
            if row and row['data']:
                payload = json.loads(row['data'])
                val = payload.get('threshold')
                if val is not None:
                    return float(val)
        except Exception:
            pass
        return None

    # ── Standalone scheduler service ────────────────────────────────────────

    def run(self) -> None:
        """
        Start the APScheduler AsyncIOScheduler service.

        Schedule:
          4H cycle:    00:05/04:05/08:05/12:05/16:05/20:05 UTC
          Slot 3 weekly: every Sunday at 00:03 UTC (before 4H cycle at 00:05)
        """
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        async def _main() -> None:
            scheduler = AsyncIOScheduler(timezone='UTC')

            scheduler.add_job(
                self._job_generate,
                CronTrigger(hour='0,4,8,12,16,20', minute=5, timezone='UTC'),
                id='signal_4h',
                name='Kronos 4H signal cycle',
            )

            scheduler.add_job(
                self._job_select_slot3,
                CronTrigger(day_of_week='sun', hour=0, minute=3, timezone='UTC'),
                id='slot3_weekly',
                name='Weekly Slot 3 selection',
            )

            scheduler.start()
            logger.info('SignalGenerator scheduler started')

            try:
                await asyncio.Event().wait()
            finally:
                scheduler.shutdown()

        asyncio.run(_main())

    async def _job_generate(self) -> None:
        loop = asyncio.get_running_loop()
        signals = await loop.run_in_executor(None, self.generate)
        logger.info('4H signal cycle complete: %d signals generated', len(signals))
        if self._shadow is not None:
            asyncio.create_task(self._run_shadow())

    async def _run_shadow(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._shadow.run_all_symbols)

    async def _job_select_slot3(self) -> None:
        loop = asyncio.get_running_loop()
        slot3 = await loop.run_in_executor(None, self.select_slot3)
        logger.info('Slot 3 selection complete: %s', slot3)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    init_db()
    gen = SignalGenerator()
    print(f'Model ready:    {gen._inference.is_ready}')
    print(f'Current Slot 3: {gen.current_slot3}')
    gen.run()
