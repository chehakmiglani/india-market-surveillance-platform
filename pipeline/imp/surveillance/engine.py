"""Run surveillance over a date range and write alerts idempotently to raw.surveillance_alerts."""
from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd
import psycopg2

from imp.db import query_df

from .rules import evaluate

LOOKBACK_DAYS = 60  # calendar days of history needed for 20-trading-day baselines


def load_prices(dsn: str, start: date, end: date) -> pd.DataFrame:
    q = """select trade_date, symbol, open, high, low, close, adj_open, adj_high, adj_low, adj_close,
                  adj_volume, adj_return, deliv_per
           from marts.fact_stock_daily where trade_date between %s and %s order by symbol, trade_date"""
    df = query_df(dsn, q, (start - timedelta(days=LOOKBACK_DAYS), end))
    for c in ("open", "high", "low", "close", "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume",
              "adj_return", "deliv_per"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    # equal-weighted return of the tracked universe, as the "market" reference
    df["mkt_return"] = df.groupby("trade_date")["adj_return"].transform("mean")
    return df


def detect(prices: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    frames = [evaluate(g) for _, g in prices.groupby("symbol")]
    alerts = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if alerts.empty:
        return alerts
    return alerts[(alerts["trade_date"] >= start) & (alerts["trade_date"] <= end)].reset_index(drop=True)


def write_alerts(dsn: str, alerts: pd.DataFrame, start: date, end: date) -> int:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("delete from raw.surveillance_alerts where trade_date between %s and %s", (start, end))
        for r in alerts.itertuples(index=False):
            cur.execute("insert into raw.surveillance_alerts (trade_date, symbol, rule, score, reason, metrics) "
                        "values (%s, %s, %s, %s, %s, %s)",
                        (r.trade_date, r.symbol, r.rule, int(r.score), r.reason, json.dumps(r.metrics)))
    return len(alerts)


def run_surveillance(dsn: str, start: date, end: date) -> dict:
    alerts = detect(load_prices(dsn, start, end), start, end)
    n = write_alerts(dsn, alerts, start, end)
    by_rule = alerts["rule"].value_counts().to_dict() if n else {}
    return {"alerts": n, "by_rule": by_rule}
