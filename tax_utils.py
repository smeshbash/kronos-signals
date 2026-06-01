"""
tax_utils.py — Shared Indian income tax helpers for Kronos modules.

Imported by Module 8 (portfolio_manager) and Module 9 (tax_tracker) so both
compute tax consistently without circular imports.

Tax regime: New regime FY 2025-26 (Section 115BAC)
Instrument: Crypto futures/options — speculative income (NOT VDA spot).
            1% TDS under Section 194S does NOT apply to derivatives.

Usage:
    from tax_utils import incremental_tax, _total_tax, _marginal_rate

    # Tax on this month's profit given YTD context
    reserve = incremental_tax(
        ytd_profit_before=500_000,   # YTD taxable income BEFORE this month
        ytd_profit_after=650_000,    # YTD taxable income INCLUDING this month
        base_income=400_000,         # non-trading income for the FY
    )
"""

import os

# ── Slab table — new tax regime FY 2025-26 (Section 115BAC) ───────────────────
# (lower_inclusive, upper_exclusive_or_None, rate)
_SLAB_TABLE: list[tuple[float, float | None, float]] = [
    (0,           400_000,  0.00),
    (400_000,     800_000,  0.05),
    (800_000,   1_200_000,  0.10),
    (1_200_000, 1_600_000,  0.15),
    (1_600_000, 2_000_000,  0.20),
    (2_000_000, 2_400_000,  0.25),
    (2_400_000,       None, 0.30),
]

# Section 87A rebate (Budget 2025, new regime).
# Cliff edge: total income <= Rs 12L → ZERO tax.  Rs 12L + 1 rupee → full slab tax.
SECTION_87A_LIMIT = 1_200_000.0
HEALTH_ED_CESS    = 0.04   # 4% health + education cess on computed tax


def _slab_tax_pre_cess(income: float) -> float:
    """
    Total income tax before cess on `income` under new regime (FY 2025-26).
    Returns 0 if income <= Rs 12L (Section 87A rebate — cliff, not taper).
    """
    if income <= 0:
        return 0.0
    tax = 0.0
    for lower, upper, rate in _SLAB_TABLE:
        if income <= lower:
            break
        ceiling = upper if upper is not None else income
        tax += (min(income, ceiling) - lower) * rate
    return 0.0 if income <= SECTION_87A_LIMIT else round(tax, 2)


def _total_tax(income: float) -> float:
    """Total income tax including 4% health + education cess."""
    return round(_slab_tax_pre_cess(income) * (1.0 + HEALTH_ED_CESS), 2)


def _marginal_rate(income: float) -> float:
    """
    Effective marginal rate (including cess) for the next rupee of income at `income`.
    Returns 0.0 if income <= Section 87A limit (Rs 12L).
    """
    if income <= SECTION_87A_LIMIT:
        return 0.0
    for lower, upper, rate in _SLAB_TABLE:
        if upper is None or income < upper:
            return round(rate * (1.0 + HEALTH_ED_CESS), 6)
    return round(0.30 * (1.0 + HEALTH_ED_CESS), 6)


def incremental_tax(
    ytd_profit_before: float,
    ytd_profit_after:  float,
    base_income:       float | None = None,
) -> float:
    """
    Compute the incremental tax attributable to the profit earned in the current
    period (month / trade / batch).

    Parameters
    ----------
    ytd_profit_before : float
        YTD taxable trading profit BEFORE this period's contribution (INR).
    ytd_profit_after  : float
        YTD taxable trading profit INCLUDING this period's contribution (INR).
    base_income       : float or None
        Non-trading annual income (salary etc.) for this FY in INR.
        Defaults to KRONOS_BASE_INCOME_INR env var, then 0.

    Returns
    -------
    float
        Tax due on this period's profit, Rs.  0.0 when the period produced a
        loss or when total income stays below the Section 87A rebate cliff.

    Example — month crosses the Rs 12L cliff
    -----------------------------------------
    base = 400_000
    ytd before = 7_500_000  →  total_income = 11.5L  →  tax = 0
    ytd after  = 8_200_000  →  total_income = 12.2L  →  tax = 73,008
    incremental = 73,008 - 0 = Rs 73,008  (entire cliff cost falls in this month)
    """
    if base_income is None:
        base_income = float(os.environ.get('KRONOS_BASE_INCOME_INR', '0'))

    # Only profitable YTD figures attract tax; losses reduce liability to 0
    income_before = base_income + max(0.0, ytd_profit_before)
    income_after  = base_income + max(0.0, ytd_profit_after)

    return round(max(0.0, _total_tax(income_after) - _total_tax(income_before)), 2)


def effective_reserve_rate(
    ytd_profit: float,
    base_income: float | None = None,
) -> float:
    """
    Marginal rate to apply to the NEXT rupee of profit at the current YTD level.
    Use this as the forward-looking tax reserve percentage for new profits.

    Returns 0.0 when total income is still under the Section 87A rebate cliff.
    """
    if base_income is None:
        base_income = float(os.environ.get('KRONOS_BASE_INCOME_INR', '0'))
    total = base_income + max(0.0, ytd_profit)
    return _marginal_rate(total)
