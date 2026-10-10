"""Post-load data-quality checks that span days (dbt tests cover row-level rules).

Each check returns (name, passed, detail). The pipeline fails the run if any check fails,
so bad data never silently reaches the dashboards.
"""
from __future__ import annotations

from datetime import date

import psycopg2

from imp.calendar import is_trading_day, previous_trading_day


def _expected_latest(d: date) -> date:
    return d if is_trading_day(d) else previous_trading_day(d)


def run_checks(dsn: str, d: date) -> dict:
    expected = _expected_latest(d)
    checks = []
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("select max(trade_date) from raw.equity_daily where trade_date <= %s", (d,))
        latest = cur.fetchone()[0]
        checks.append(("freshness", latest is not None and latest >= expected,
                       f"latest trade_date {latest}, expected {expected}"))

        cur.execute("""
            with daily as (select trade_date, count(*) n from raw.equity_daily
                           where series = 'EQ' and trade_date <= %s group by trade_date order by trade_date desc limit 21)
            select (select n from daily order by trade_date desc limit 1),
                   percentile_cont(0.5) within group (order by n) from (select n from daily offset 1) x""", (d,))
        today_n, median_n = cur.fetchone()
        ok = today_n is not None and median_n is not None and 0.5 * median_n <= today_n <= 1.5 * median_n
        checks.append(("row_count_vs_20d_median", ok or median_n is None, f"latest {today_n} rows vs median {median_n}"))

        cur.execute("""select count(*) from raw.equity_daily where trade_date = %s
                       and (close is null or volume is null or close <= 0)""", (latest,))
        bad = cur.fetchone()[0]
        checks.append(("no_null_or_nonpositive_prices", bad == 0, f"{bad} bad rows on {latest}"))

        cur.execute("""select (select count(*) from raw.equity_daily where trade_date = %s and series = 'EQ'),
                              (select count(*) from marts.fact_stock_daily where trade_date = %s)""", (latest, latest))
        raw_n, fact_n = cur.fetchone()
        checks.append(("marts_match_raw", raw_n == fact_n, f"raw {raw_n} vs fact {fact_n} on {latest}"))

        cur.execute("select count(*) from marts.fact_stock_daily where stock_sk is null")
        orphans = cur.fetchone()[0]
        checks.append(("no_orphan_facts", orphans == 0, f"{orphans} facts without a stock dimension row"))
    return {"date": d, "passed": all(c[1] for c in checks),
            "checks": [{"name": n, "passed": bool(p), "detail": det} for n, p, det in checks]}
