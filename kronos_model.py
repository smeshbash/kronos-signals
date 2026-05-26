"""
Kronos Trading System — KronosForecaster
Channel-independent PatchTST for 4H OHLCV forecasting.

Interface (matches Module 4 / KronosInference exactly):
  Input:  Tensor[batch, SEQ_LEN, 5]  — instance-normalised OHLCV (RevIN applied by caller)
  Output: Tensor[batch, PRED_LEN, 5] — predicted normalised OHLCV (denormalised by caller)

The caller (Module 4) is responsible for instance normalisation before calling forward()
and for denormalising the output. This model operates entirely in normalised space.

Architecture — channel-independent PatchTST:
  Each of the 5 OHLCV channels is processed independently through a shared
  patch-embedding + transformer encoder backbone.

  SEQ_LEN=96 split into overlapping patches:
    PATCH_LEN=16, STRIDE=8  →  n_patches = (96-16)//8 + 1 = 11

  Transformer encoder: 3 pre-LN layers, 8 attention heads, d_model=128, d_ff=256
  Projection head: n_patches * d_model → PRED_LEN (one per channel)

  Total parameters: ~1.4 M — fits in 150 MB RAM, no GPU required for inference.

Saving (training script):
  scripted = torch.jit.script(model)
  scripted.save('models/kronos_model.pt')

Loading (Module 4 KronosInference._try_load):
  torch.jit.load(path) — primary
  torch.load(path)     — fallback (works when saved with torch.save(model, ...))
"""

import math

import torch
import torch.nn as nn
from torch import Tensor

# ── Default hyperparameters ────────────────────────────────────────────────────
# These defaults must match Module 4 constants (KRONOS_SEQ_LEN / KRONOS_PRED_LEN).
SEQ_LEN:    int   = 96
PRED_LEN:   int   = 6
N_CHANNELS: int   = 5
PATCH_LEN:  int   = 16
STRIDE:     int   = 8
D_MODEL:    int   = 128
N_HEADS:    int   = 8
N_LAYERS:   int   = 3
D_FF:       int   = 256
DROPOUT:    float = 0.1

# n_patches = (SEQ_LEN - PATCH_LEN) // STRIDE + 1 = 11


# ── Sub-modules ───────────────────────────────────────────────────────────────

class _PatchEmbedding(nn.Module):
    """Project overlapping time-series patches to d_model dimensions.

    Input:  [B, L]
    Output: [B, n_patches, d_model]
    """

    def __init__(self, patch_len: int, stride: int, d_model: int) -> None:
        super().__init__()
        self.patch_len: int = patch_len
        self.stride:    int = stride
        self.proj = nn.Linear(patch_len, d_model)

    def forward(self, x: Tensor) -> Tensor:
        patches = x.unfold(-1, self.patch_len, self.stride)  # [B, n_patches, patch_len]
        return self.proj(patches)                             # [B, n_patches, d_model]


class _EncoderLayer(nn.Module):
    """Pre-norm transformer encoder layer (compatible with torch.jit.script)."""

    def __init__(self, d_model: int, n_heads: int, d_ff: int, dropout: float) -> None:
        super().__init__()
        self.norm1 = nn.LayerNorm(d_model)
        self.attn  = nn.MultiheadAttention(
            d_model, n_heads,
            dropout=dropout,
            batch_first=True,
        )
        self.norm2 = nn.LayerNorm(d_model)
        self.ff1   = nn.Linear(d_model, d_ff)
        self.ff2   = nn.Linear(d_ff, d_model)
        self.drop  = nn.Dropout(dropout)
        self.act   = nn.GELU()

    def forward(self, x: Tensor) -> Tensor:
        # Pre-LN self-attention
        residual = x
        h = self.norm1(x)
        h, _ = self.attn(h, h, h, need_weights=False)
        x = residual + self.drop(h)
        # Pre-LN feed-forward
        residual = x
        h = self.norm2(x)
        h = self.drop(self.act(self.ff1(h)))
        h = self.ff2(h)
        return residual + self.drop(h)


# ── Main model ────────────────────────────────────────────────────────────────

class KronosForecaster(nn.Module):
    """
    Channel-independent PatchTST for OHLCV direction forecasting.

    Usage:
        model = KronosForecaster()
        x = torch.randn(1, 96, 5)   # instance-normalised OHLCV
        y = model(x)                 # [1, 6, 5] predicted normalised OHLCV
    """

    def __init__(
        self,
        seq_len:    int   = SEQ_LEN,
        pred_len:   int   = PRED_LEN,
        n_channels: int   = N_CHANNELS,
        patch_len:  int   = PATCH_LEN,
        stride:     int   = STRIDE,
        d_model:    int   = D_MODEL,
        n_heads:    int   = N_HEADS,
        n_layers:   int   = N_LAYERS,
        d_ff:       int   = D_FF,
        dropout:    float = DROPOUT,
    ) -> None:
        super().__init__()

        self.seq_len:    int = seq_len
        self.pred_len:   int = pred_len
        self.n_channels: int = n_channels

        n_patches: int = (seq_len - patch_len) // stride + 1

        self.patch_embed = _PatchEmbedding(patch_len, stride, d_model)

        # Learnable positional embedding — one vector per patch position
        self.pos_embed = nn.Parameter(torch.zeros(1, n_patches, d_model))
        nn.init.trunc_normal_(self.pos_embed, std=0.02)

        self.drop = nn.Dropout(dropout)

        # Encoder: list of layers (TorchScript handles nn.ModuleList iteration)
        self.encoder = nn.ModuleList([
            _EncoderLayer(d_model, n_heads, d_ff, dropout)
            for _ in range(n_layers)
        ])

        self.norm = nn.LayerNorm(d_model)

        # Linear head: flatten all patch embeddings → pred_len outputs per channel
        self.head = nn.Linear(n_patches * d_model, pred_len)

        self._init_weights()

    def _init_weights(self) -> None:
        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.xavier_uniform_(m.weight)
                if m.bias is not None:
                    nn.init.zeros_(m.bias)

    def forward(self, x: Tensor) -> Tensor:
        """
        Args:
            x: Tensor[batch, seq_len, n_channels] — instance-normalised OHLCV

        Returns:
            Tensor[batch, pred_len, n_channels] — predicted normalised OHLCV
        """
        B: int = x.shape[0]
        L: int = x.shape[1]
        C: int = x.shape[2]

        # Channel-independent: merge batch and channel dims → [B*C, L]
        xc = x.permute(0, 2, 1).reshape(B * C, L)

        # Patch embedding → [B*C, n_patches, d_model]
        tokens = self.patch_embed(xc) + self.pos_embed
        tokens = self.drop(tokens)

        # Transformer encoder
        for layer in self.encoder:
            tokens = layer(tokens)

        tokens = self.norm(tokens)  # [B*C, n_patches, d_model]

        # Flatten and project → [B*C, pred_len]
        flat = tokens.reshape(B * C, -1)
        pred = self.head(flat)

        # Restore batch and channel dims → [B, pred_len, C]
        return pred.reshape(B, C, self.pred_len).permute(0, 2, 1)


# ── Convenience factory ───────────────────────────────────────────────────────

def build_model(
    seq_len:  int = SEQ_LEN,
    pred_len: int = PRED_LEN,
) -> KronosForecaster:
    """Return a KronosForecaster with default hyperparameters."""
    return KronosForecaster(seq_len=seq_len, pred_len=pred_len)


if __name__ == '__main__':
    model = build_model()
    model.eval()

    n_params = sum(p.numel() for p in model.parameters())
    print(f'KronosForecaster — {n_params:,} parameters')

    x = torch.randn(2, SEQ_LEN, N_CHANNELS)
    with torch.no_grad():
        y = model(x)
    print(f'Input shape:  {tuple(x.shape)}')
    print(f'Output shape: {tuple(y.shape)}')

    # Verify TorchScript export
    try:
        scripted = torch.jit.script(model)
        print('torch.jit.script: OK')
        y2 = scripted(x)
        assert y2.shape == y.shape, 'Shape mismatch after scripting'
        print('Scripted forward pass: OK')
    except Exception as e:
        print(f'torch.jit.script failed: {e} — model will be saved with torch.save instead')
