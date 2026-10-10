from datetime import date

import pandas as pd
import psycopg2
import pytest

from imp.transform.silver_small import build_corp_actions, build_nav

pytestmark = pytest.mark.spark


def test_silver_both_formats_delivery_and_idempotency(spark, demo_lake):
    from imp.transform.silver_equity import build_silver
    bronze, silver = demo_lake / "bronze", demo_lake / "silver"
    r1 = build_silver(spark, bronze, silver, date(2024, 6, 3), date(2025, 7, 31))
    df = pd.read_parquet(silver / "equity_daily")
    assert r1["rows"] == len(df) and r1["dropped"] == 0
    assert set(df["series"]) == {"EQ"}                        # the N1 bond row is filtered out
    dates = pd.to_datetime(df["trade_date"].astype(str))
    assert dates.min() <= pd.Timestamp("2024-07-05") and dates.max() >= pd.Timestamp("2024-07-08")  # both formats
    assert df["deliv_per"].notna().all()                      # delivery joined for every row
    assert not df.duplicated(["trade_date", "symbol", "series"]).any()
    legacy_names = df[dates < pd.Timestamp("2024-07-08")]["company_name"]
    assert legacy_names.isna().all()                          # legacy format has no company name
    # re-run a sub-range: same rows, no duplicates (dynamic partition overwrite)
    build_silver(spark, bronze, silver, date(2024, 7, 1), date(2024, 7, 31))
    df2 = pd.read_parquet(silver / "equity_daily")
    assert len(df2) == len(df)


def test_symbol_filter(spark, demo_lake, tmp_path):
    from imp.transform.silver_equity import build_silver
    r = build_silver(spark, demo_lake / "bronze", tmp_path / "silver", date(2025, 1, 1), date(2025, 1, 31),
                     symbols=["TCS", "MRF"])
    df = pd.read_parquet(tmp_path / "silver" / "equity_daily")
    assert set(df["symbol"]) == {"TCS", "MRF"} and r["rows"] == len(df)


def test_small_sources(demo_lake, tmp_path):
    nav = build_nav(demo_lake / "bronze", tmp_path, date(2025, 1, 1), date(2025, 1, 31))
    assert nav["rows"] > 0
    ca = build_corp_actions(demo_lake / "bronze", tmp_path)
    assert ca["price_affecting"] == 2 and ca["rows"] == 3


@pytest.mark.integration
def test_load_is_idempotent(spark, demo_lake, pg_dsn):
    from imp.load.postgres import ensure_schema, load_corp_actions, load_equity, load_nav
    from imp.transform.silver_equity import build_silver
    bronze, silver = demo_lake / "bronze", demo_lake / "silver"
    build_silver(spark, bronze, silver, date(2024, 6, 3), date(2025, 7, 31))
    build_nav(bronze, silver, date(2024, 6, 3), date(2025, 7, 31))
    build_corp_actions(bronze, silver)
    ensure_schema(pg_dsn)
    n = load_equity(pg_dsn, silver, date(2024, 6, 3), date(2025, 7, 31))
    load_equity(pg_dsn, silver, date(2025, 1, 1), date(2025, 1, 31))     # reload a month
    assert load_nav(pg_dsn, silver, date(2024, 6, 3), date(2025, 7, 31)) > 0
    assert load_corp_actions(pg_dsn, silver) == 3
    with psycopg2.connect(pg_dsn) as c, c.cursor() as cur:
        cur.execute("select count(*), count(distinct (trade_date, symbol, series)) from raw.equity_daily")
        total, distinct = cur.fetchone()
    assert total == distinct == n
