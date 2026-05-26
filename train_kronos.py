"""
Kronos Trading System — Training Script
Trains KronosForecaster on historical 4H OHLCV data and saves the model.

Data source: Binance (deepest 4H history for all 5 Kronos assets)
  BTC/USDT, ETH/USDT, SOL/USDT, BNB/USDT, XRP/USDT
  Fetches from SINCE_DATE back to now (~2+ years).

Training:
  - Instance normalisation (RevIN) per sliding window — matches Module 4 exactly
  - 80/20 time-ordered train/val split (no shuffle across the split boundary)
  - AdamW + CosineAnnealingLR
  - Early stopping (patience 10 epochs)

Output:
  models/kronos_model.pt  — TorchScript model (or full model if scripting fails)

Usage:
  python train_kronos.py
  python train_kronos.py --epochs 200 --batch 128 --lr 5e-4

The resulting model file is loaded by Module 4 (KronosInference._try_load).
After training, set KRONOS_MODEL_PATH=./models/kronos_model.pt in your .env.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

# ── Guard: check PyTorch first ────────────────────────────────────────────────
try:
    import torch
    import torch.nn as nn
    from torch.utils.data import ConcatDataset, DataLoader, Dataset
except ImportError:
    sys.exit('PyTorch not installed. Run: pip install torch')

try:
    import ccxt
except ImportError:
    sys.exit('ccxt not installed. Run: pip install ccxt')

try:
    import numpy as np
except ImportError:
    sys.exit('numpy not installed. Run: pip install numpy')

from kronos_model import KronosForecaster, SEQ_LEN, PRED_LEN, N_CHANNELS

# ── Constants ─────────────────────────────────────────────────────────────────
SYMBOLS    = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
TIMEFRAME  = '4h'
# Fetch ~2.5 years of 4H candles (2.5y × 365d × 6 candles/day ≈ 5475 candles)
MAX_CANDLES_PER_FETCH = 1000   # Binance API limit per call
TARGET_CANDLES        = 5500   # ~2.5 years
SINCE_DATE_DAYS       = 915    # fetch this many days of history

MODELS_DIR  = os.path.join(os.path.dirname(__file__), 'models')
MODEL_PATH  = os.path.join(MODELS_DIR, 'kronos_model.pt')

WINDOW_SIZE = SEQ_LEN + PRED_LEN   # 102 candles per sample
VAL_SPLIT   = 0.20                 # last 20% of each asset's data → val set
BATCH_SIZE  = 64
EPOCHS      = 100
LR          = 1e-4
WEIGHT_DECAY = 1e-4
EARLY_STOP_PATIENCE = 10


# ── Dataset ───────────────────────────────────────────────────────────────────

class OHLCVDataset(Dataset):
    """Sliding-window OHLCV dataset with instance normalisation (RevIN).

    Each sample: (x_norm, y_norm) where both are normalised using the
    per-channel mean/std of the input window (x). This exactly matches
    Module 4's pre-processing in KronosInference.predict().
    """

    def __init__(self, data: np.ndarray) -> None:
        # data: [T, 5] raw OHLCV (float32)
        self.data    = data
        self.n_windows = len(data) - WINDOW_SIZE + 1

    def __len__(self) -> int:
        return max(0, self.n_windows)

    def __getitem__(self, idx: int):
        window = self.data[idx : idx + WINDOW_SIZE]  # [WINDOW_SIZE, 5]
        x_raw  = window[:SEQ_LEN]                    # [SEQ_LEN, 5]
        y_raw  = window[SEQ_LEN:]                    # [PRED_LEN, 5]

        # Per-channel instance normalisation over the input window
        mean = x_raw.mean(axis=0, keepdims=True)     # [1, 5]
        std  = x_raw.std(axis=0, ddof=1, keepdims=True) + 1e-8  # [1, 5]

        x_norm = (x_raw - mean) / std                # [SEQ_LEN, 5]
        y_norm = (y_raw - mean) / std                # [PRED_LEN, 5]

        return (
            torch.tensor(x_norm, dtype=torch.float32),
            torch.tensor(y_norm, dtype=torch.float32),
        )


# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_ohlcv_paginated(exchange, symbol: str, timeframe: str, target: int) -> np.ndarray:
    """Fetch up to `target` 4H candles from Binance via CCXT pagination.

    Returns: [T, 5] float32 array — columns: open, high, low, close, volume
    """
    all_candles = []
    # Start from approx SINCE_DATE_DAYS ago
    since_ms = int((time.time() - SINCE_DATE_DAYS * 86400) * 1000)

    while len(all_candles) < target:
        try:
            candles = exchange.fetch_ohlcv(
                symbol, timeframe,
                since=since_ms,
                limit=MAX_CANDLES_PER_FETCH,
            )
        except Exception as e:
            print(f'  Fetch error for {symbol}: {e}')
            break

        if not candles:
            break

        all_candles.extend(candles)
        last_ts = candles[-1][0]

        if len(candles) < MAX_CANDLES_PER_FETCH:
            break   # no more data

        # Advance since_ms past the last candle
        since_ms = last_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    if not all_candles:
        return np.zeros((0, 5), dtype=np.float32)

    # columns: timestamp, open, high, low, close, volume
    arr = np.array([[c[1], c[2], c[3], c[4], c[5]] for c in all_candles], dtype=np.float32)
    return arr


def fetch_all_data() -> list:
    """Fetch 4H OHLCV for all 5 symbols.

    Returns a list of per-asset [T, 5] float32 arrays. Assets are NOT
    concatenated here — build_datasets() builds one OHLCVDataset per asset
    so windows never span two different assets.
    """
    exchange = ccxt.binance({'enableRateLimit': True})
    exchange.load_markets()

    all_arrays = []
    for sym in SYMBOLS:
        print(f'Fetching {sym} {TIMEFRAME} ...')
        arr = _fetch_ohlcv_paginated(exchange, sym, TIMEFRAME, TARGET_CANDLES)
        print(f'  Got {len(arr)} candles')
        if len(arr) >= WINDOW_SIZE:
            all_arrays.append(arr)

    if not all_arrays:
        raise RuntimeError('No data fetched — check network / Binance availability')

    return all_arrays


def build_datasets(arrays: list):
    """Build train and val datasets from a list of per-asset arrays.

    Each asset is split independently (80/20 time-ordered), then combined
    with ConcatDataset so no sliding window ever crosses an asset boundary.

    Returns: (train_dataset, val_dataset)
    """
    train_parts = []
    val_parts   = []

    for arr in arrays:
        n     = len(arr)
        split = int(n * (1.0 - VAL_SPLIT))
        split = max(WINDOW_SIZE, min(split, n - WINDOW_SIZE))

        train_arr = arr[:split]
        val_arr   = arr[split - SEQ_LEN:]   # overlap by SEQ_LEN to avoid a gap

        t_ds = OHLCVDataset(train_arr)
        v_ds = OHLCVDataset(val_arr)
        if len(t_ds) > 0:
            train_parts.append(t_ds)
        if len(v_ds) > 0:
            val_parts.append(v_ds)

    train_ds = ConcatDataset(train_parts)
    val_ds   = ConcatDataset(val_parts)

    print(f'Train windows: {len(train_ds)}  |  Val windows: {len(val_ds)}')
    return train_ds, val_ds


# ── Training loop ─────────────────────────────────────────────────────────────

def train(
    epochs:   int   = EPOCHS,
    batch:    int   = BATCH_SIZE,
    lr:       float = LR,
    wd:       float = WEIGHT_DECAY,
    patience: int   = EARLY_STOP_PATIENCE,
) -> None:

    # ── Setup ──
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(f'Device: {device}')
    if device == 'cuda':
        print(f'GPU: {torch.cuda.get_device_name(0)}')

    os.makedirs(MODELS_DIR, exist_ok=True)

    # ── Data ──
    print('\nFetching historical data...')
    arrays = fetch_all_data()
    total_candles = sum(len(a) for a in arrays)
    print(f'Total candles: {total_candles} across {len(arrays)} assets')

    train_ds, val_ds = build_datasets(arrays)

    train_loader = DataLoader(
        train_ds, batch_size=batch, shuffle=True,
        num_workers=0, drop_last=True,
    )
    val_loader = DataLoader(
        val_ds, batch_size=batch, shuffle=False,
        num_workers=0, drop_last=False,
    )

    # ── Model ──
    model = KronosForecaster().to(device)
    n_params = sum(p.numel() for p in model.parameters())
    print(f'\nKronosForecaster — {n_params:,} parameters')

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=wd)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=epochs, eta_min=lr * 0.01,
    )
    criterion = nn.MSELoss()

    # ── Training ──
    best_val_loss = float('inf')
    no_improve    = 0
    best_epoch    = 0

    print(f'\nTraining for up to {epochs} epochs (early stop patience={patience})\n')

    for epoch in range(1, epochs + 1):
        # Train
        model.train()
        train_loss = 0.0
        for x, y in train_loader:
            x, y = x.to(device), y.to(device)
            optimizer.zero_grad()
            pred = model(x)
            loss = criterion(pred, y)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            train_loss += loss.item() * x.size(0)

        train_loss /= len(train_ds)
        scheduler.step()

        # Validate
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for x, y in val_loader:
                x, y = x.to(device), y.to(device)
                pred = model(x)
                val_loss += criterion(pred, y).item() * x.size(0)
        val_loss /= len(val_ds)

        lr_cur = scheduler.get_last_lr()[0]
        print(f'Epoch {epoch:3d}/{epochs}  '
              f'train={train_loss:.6f}  val={val_loss:.6f}  '
              f'lr={lr_cur:.2e}')

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_epoch    = epoch
            no_improve    = 0
            # Save best model state
            torch.save(model.state_dict(), MODEL_PATH + '.best.pt')
        else:
            no_improve += 1
            if no_improve >= patience:
                print(f'\nEarly stopping at epoch {epoch} '
                      f'(best val={best_val_loss:.6f} at epoch {best_epoch})')
                break

    # ── Save final model ──
    print(f'\nLoading best weights from epoch {best_epoch}...')
    model.load_state_dict(torch.load(MODEL_PATH + '.best.pt', map_location='cpu'))
    model.eval().cpu()

    # Try TorchScript first (preferred — Module 4 tries jit.load first)
    saved_as = 'unknown'
    try:
        scripted = torch.jit.script(model)
        scripted.save(MODEL_PATH)
        saved_as = 'TorchScript'
    except Exception as e:
        print(f'TorchScript export failed ({e}), saving as full model...')
        torch.save(model, MODEL_PATH)
        saved_as = 'full model (torch.save)'

    # Timestamped backup
    ts  = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    bak = os.path.join(MODELS_DIR, f'kronos_model_{ts}.pt')
    import shutil
    shutil.copy(MODEL_PATH, bak)

    # Clean up temp best-weights file
    try:
        os.remove(MODEL_PATH + '.best.pt')
    except OSError:
        pass

    print(f'\nSaved as {saved_as}: {MODEL_PATH}')
    print(f'Backup:              {bak}')
    print(f'Best val MSE:        {best_val_loss:.6f}')
    print(f'\nSet KRONOS_MODEL_PATH={MODEL_PATH} in your .env')


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train KronosForecaster')
    parser.add_argument('--epochs',   type=int,   default=EPOCHS,      help='Max training epochs')
    parser.add_argument('--batch',    type=int,   default=BATCH_SIZE,  help='Batch size')
    parser.add_argument('--lr',       type=float, default=LR,          help='Initial learning rate')
    parser.add_argument('--patience', type=int,   default=EARLY_STOP_PATIENCE, help='Early stopping patience')
    args = parser.parse_args()

    train(
        epochs=args.epochs,
        batch=args.batch,
        lr=args.lr,
        patience=args.patience,
    )
