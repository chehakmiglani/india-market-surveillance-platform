"""XIRR: the annualised return that makes the present value of dated cash flows zero.

Cash-flow convention: money you invest is NEGATIVE, money you receive (or today's market
value of what you still hold) is POSITIVE.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from imp.db import query_df


def _npv(rate: float, flows: list[tuple[date, float]]) -> float:
    t0 = flows[0][0]
    return sum(cf / (1 + rate) ** ((d - t0).days / 365.0) for d, cf in flows)


def xirr(flows: list[tuple[date, float]], lo: float = -0.9999, hi: float = 100.0, tol: float = 1e-10) -> float:
    """Bisection on NPV (robust: no derivative, always converges when a sign change exists)."""
    flows = sorted(flows)
    if not (any(cf < 0 for _, cf in flows) and any(cf > 0 for _, cf in flows)):
        raise ValueError("XIRR needs at least one negative and one positive cash flow")
    f_lo, f_hi = _npv(lo, flows), _npv(hi, flows)
    if f_lo * f_hi > 0:
        raise ValueError("no sign change in NPV between the bounds; XIRR undefined")
    for _ in range(300):
        mid = (lo + hi) / 2
        f_mid = _npv(mid, flows)
        if abs(f_mid) < tol or (hi - lo) < tol:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


def portfolio_xirr(dsn: str, owner: str, as_of: date | None = None) -> float:
    """XIRR of an owner's portfolio: transactions plus current market value as a final inflow."""
    tx = query_df(dsn, "select txn_date, cash_flow from marts.fact_portfolio_txn where owner = %s", (owner,))
    mv = query_df(dsn, "select sum(market_value) as mv, max(price_date) as d from marts.mart_portfolio_holdings "
                       "where owner = %s", (owner,))
    flows = [(pd.Timestamp(r.txn_date).date(), float(r.cash_flow)) for r in tx.itertuples()]
    flows.append((as_of or pd.Timestamp(mv.at[0, "d"]).date(), float(mv.at[0, "mv"])))
    return xirr(flows)
