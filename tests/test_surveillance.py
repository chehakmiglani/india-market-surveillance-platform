from datetime import date

import numpy as np
import pandas as pd
import pytest

from imp.portfolio.xirr import xirr
from imp.surveillance.alerts import format_message
from imp.surveillance.rules import evaluate


def make_series(n=80, seed=0, symbol="TEST"):
    rng = np.random.default_rng(seed)
    days = pd.bdate_range("2025-01-01", periods=n).date
    ret = rng.normal(0, 0.01, n)
    close = 100 * np.exp(np.cumsum(ret))
    vol = rng.lognormal(np.log(1e6), 0.2, n)
    df = pd.DataFrame({"trade_date": days, "symbol": symbol, "adj_close": close, "adj_volume": vol,
                       "deliv_per": rng.normal(50, 3, n), "mkt_return": 0.0})
    df["adj_open"] = df["adj_close"].shift(1).fillna(100)
    df["close"], df["high"], df["low"] = df["adj_close"], df["adj_close"] * 1.01, df["adj_close"] * 0.99
    df["adj_return"] = df["adj_close"].pct_change()
    return df


def rules_fired(df, day):
    a = evaluate(df)
    return set(a[a["trade_date"] == day]["rule"])


def test_quiet_series_has_few_alerts():
    a = evaluate(make_series())
    assert len(a) <= 3


def test_volume_spike_detected():
    df = make_series()
    df.loc[60, "adj_volume"] *= 8
    assert "volume_spike" in rules_fired(df, df.at[60, "trade_date"])


def test_price_shock_and_gap():
    df = make_series()
    df.loc[60, "adj_close"] = df.at[59, "adj_close"] * 1.09
    df.loc[60, "adj_open"] = df.at[59, "adj_close"] * 1.08
    df["adj_return"] = df["adj_close"].pct_change()
    fired = rules_fired(df, df.at[60, "trade_date"])
    assert {"price_shock", "gap"} <= fired


def test_pump_then_dump():
    df = make_series(n=90)
    for k in range(6):
        i = 60 + k
        df.loc[i, "adj_close"] = df.at[i - 1, "adj_close"] * (1.06 + 0.01 * k)
        df.loc[i, "adj_volume"] *= 3 + k
        df.loc[i, "deliv_per"] = 20 - k
    for k in range(3):
        i = 66 + k
        df.loc[i, "adj_close"] = df.at[i - 1, "adj_close"] * 0.9
    df["adj_return"] = df["adj_close"].pct_change()
    a = evaluate(df)
    assert "pump" in set(a["rule"]) and "dump_after_pump" in set(a["rule"])
    pump_day = a[a["rule"] == "pump"]["trade_date"].min()
    dump_day = a[a["rule"] == "dump_after_pump"]["trade_date"].min()
    assert pump_day < dump_day


def test_baseline_excludes_today():
    df = make_series()
    df.loc[60, "adj_volume"] *= 100     # if today were in its own average, the ratio would be capped near 20
    a = evaluate(df)
    m = a[(a["rule"] == "volume_spike") & (a["trade_date"] == df.at[60, "trade_date"])].iloc[0]["metrics"]
    assert m["vol_ratio"] > 50


def test_scores_in_range():
    df = make_series(seed=3)
    df.loc[[40, 55, 70], "adj_volume"] *= 10
    a = evaluate(df)
    assert a["score"].between(0, 100).all()


def test_message_format():
    alerts = pd.DataFrame({"symbol": ["A", "A", "B"], "rule": ["volume_spike", "gap", "pump"],
                           "score": [90, 60, 80], "reason": ["Volume 8.0x", "Opened +6%", "5 up days"]})
    msg = format_message(date(2026, 10, 9), alerts)
    assert "2 stocks flagged" in msg and "+1 more signals" in msg and "not evidence" in msg
    assert "no alerts" in format_message(date(2026, 10, 9), alerts.iloc[0:0])


@pytest.mark.parametrize("flows,expected", [
    ([(date(2024, 1, 1), -1000), (date(2025, 1, 1), 1100)], 0.0997),   # 366 days (leap year) -> slightly under 10%
    ([(date(2024, 1, 1), -1000), (date(2024, 7, 1), -1000), (date(2025, 1, 1), 2200)], None),
])
def test_xirr(flows, expected):
    r = xirr(flows)
    t0 = flows[0][0]
    npv = sum(cf / (1 + r) ** ((d - t0).days / 365) for d, cf in flows)
    assert abs(npv) < 1e-6
    if expected is not None:
        assert r == pytest.approx(expected, abs=1e-3)


def test_xirr_needs_both_signs():
    with pytest.raises(ValueError):
        xirr([(date(2024, 1, 1), -1), (date(2024, 2, 1), -1)])
