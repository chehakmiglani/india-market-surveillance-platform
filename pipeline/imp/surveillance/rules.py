"""Market-surveillance rules on split-adjusted daily data (pandas, one symbol at a time).

Each rule returns, per day, whether it fired, a severity in [0, 1] and a plain-English reason.
These are statistical signals worth a closer look, NOT evidence of manipulation.

Baselines always use the PREVIOUS 20 trading days (shifted by one), so today's spike is never
part of its own average.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

WINDOW, MIN_OBS = 20, 15


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """df: one symbol, sorted by trade_date, with adj_* columns, adj_return, deliv_per, mkt_return."""
    d = df.copy()
    prev = lambda s: s.shift(1).rolling(WINDOW, min_periods=MIN_OBS)  # noqa: E731
    d["avg_vol20"] = prev(d["adj_volume"]).mean()
    d["vol_ratio"] = d["adj_volume"] / d["avg_vol20"]
    logv = np.log(d["adj_volume"].clip(lower=1))
    d["vol_z"] = (logv - prev(logv).mean()) / prev(logv).std()
    d["sigma20"] = prev(d["adj_return"]).std()
    d["ret_z"] = d["adj_return"] / d["sigma20"]
    d["excess_z"] = (d["adj_return"] - d["mkt_return"]) / d["sigma20"]
    d["deliv_mean20"] = prev(d["deliv_per"]).mean()
    d["deliv_std20"] = prev(d["deliv_per"]).std()
    d["gap"] = d["adj_open"] / d["adj_close"].shift(1) - 1
    d["ret5"] = d["adj_close"] / d["adj_close"].shift(5) - 1
    d["ret3"] = d["adj_close"] / d["adj_close"].shift(3) - 1
    d["up_days5"] = (d["adj_return"] > 0).astype(int).rolling(5).sum()
    d["vol5_ratio"] = d["adj_volume"].rolling(5).mean() / d["avg_vol20"].shift(4)
    d["deliv5_drop"] = d["deliv_mean20"].shift(4) - d["deliv_per"].rolling(5).mean()
    return d


def _out(fired, sev, reason) -> pd.DataFrame:
    return pd.DataFrame({"fired": fired.fillna(False).astype(bool), "severity": sev.clip(0, 1).fillna(0),
                         "reason": reason})


def volume_spike(d):
    fired = (d["vol_ratio"] >= 5) | (d["vol_z"] >= 3)
    sev = 0.5 + (d["vol_ratio"] - 5) / 20
    sev = sev.where(d["vol_ratio"] >= 5, 0.5)
    reason = d.apply(lambda r: f"Volume {r.vol_ratio:.1f}x its 20-day average (z={r.vol_z:.1f})", axis=1)
    return _out(fired, sev, reason)


def price_shock(d):
    fired = (d["ret_z"].abs() > 3) & (d["excess_z"].abs() > 2)
    sev = 0.5 + (d["ret_z"].abs() - 3) / 6
    reason = d.apply(lambda r: f"Price {r.adj_return:+.1%} ({r.ret_z:+.1f} sigma) while the tracked market moved "
                                f"{r.mkt_return:+.1%}", axis=1)
    return _out(fired, sev, reason)


def pump(d):
    deliv_ok = d["deliv5_drop"].isna() | (d["deliv5_drop"] >= 10)
    fired = (d["up_days5"] == 5) & (d["ret5"] > 0.15) & (d["vol5_ratio"] >= 2) & deliv_ok
    sev = 0.6 + d["ret5"]
    reason = d.apply(lambda r: f"5 straight up days, {r.ret5:+.0%} on {r.vol5_ratio:.1f}x volume"
                                + ("" if pd.isna(r.deliv5_drop) else f", delivery down {r.deliv5_drop:.0f} pts"), axis=1)
    return _out(fired, sev, reason)


def dump_after_pump(d, pump_fired: pd.Series):
    recent_pump = pump_fired.astype(int).shift(1).rolling(5, min_periods=1).max().fillna(0).astype(bool)
    fired = recent_pump & (d["ret3"] <= -0.15)
    reason = d.apply(lambda r: f"Fell {r.ret3:.0%} in 3 days right after a pump pattern", axis=1)
    return _out(fired, pd.Series(0.9, index=d.index), reason)


def delivery_anomaly(d):
    fired = (d["deliv_per"] < d["deliv_mean20"] - 2 * d["deliv_std20"]) & (d["vol_ratio"] >= 2)
    reason = d.apply(lambda r: f"Delivery {r.deliv_per:.0f}% vs usual {r.deliv_mean20:.0f}% on {r.vol_ratio:.1f}x "
                                "volume (speculative churn)", axis=1)
    return _out(fired, pd.Series(0.6, index=d.index), reason)


def circuit_streak(d):
    bands = np.array([0.05, 0.10, 0.20])
    r = d["adj_return"].abs()
    near_band = r.apply(lambda x: bool(np.any(np.abs(bands - x) <= 0.0025)) if pd.notna(x) else False)
    at_high = (d["close"] - d["high"]).abs() <= 0.01
    at_low = (d["close"] - d["low"]).abs() <= 0.01
    hit = near_band & ((at_high & (d["adj_return"] > 0)) | (at_low & (d["adj_return"] < 0)))
    fired = hit & hit.shift(1, fill_value=False)
    reason = d.apply(lambda r: f"Second straight day closing at a {abs(r.adj_return):.0%} circuit", axis=1)
    return _out(fired, pd.Series(0.7, index=d.index), reason)


def gap(d):
    fired = d["gap"].abs() > 0.05
    sev = 0.4 + d["gap"].abs() / 0.25
    reason = d.apply(lambda r: f"Opened {r.gap:+.1%} away from the previous close", axis=1)
    return _out(fired, sev, reason)


def evaluate(df_symbol: pd.DataFrame) -> pd.DataFrame:
    """All rules for one symbol. Returns rows (trade_date, symbol, rule, score, reason, metrics)."""
    d = add_features(df_symbol.sort_values("trade_date").reset_index(drop=True))
    results = {"volume_spike": volume_spike(d), "price_shock": price_shock(d), "pump": pump(d),
               "delivery_anomaly": delivery_anomaly(d), "circuit_streak": circuit_streak(d), "gap": gap(d)}
    results["dump_after_pump"] = dump_after_pump(d, results["pump"]["fired"])
    rows = []
    metric_cols = ["vol_ratio", "ret_z", "excess_z", "ret5", "ret3", "gap", "deliv_per", "deliv_mean20"]
    for rule, res in results.items():
        for i in res.index[res["fired"]]:
            m = {k: (None if pd.isna(d.at[i, k]) else round(float(d.at[i, k]), 4)) for k in metric_cols}
            rows.append({"trade_date": d.at[i, "trade_date"], "symbol": d.at[i, "symbol"], "rule": rule,
                         "score": int(round(100 * float(res.at[i, "severity"]))), "reason": res.at[i, "reason"],
                         "metrics": m})
    return pd.DataFrame(rows, columns=["trade_date", "symbol", "rule", "score", "reason", "metrics"])
