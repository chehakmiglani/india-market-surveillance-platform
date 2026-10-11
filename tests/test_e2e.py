"""Full pipeline on synthetic data: bronze -> Spark silver -> Postgres -> dbt (seeds, SCD2 snapshot,
models, tests) -> surveillance -> dbt -> quality checks -> alert message."""
import shutil

import pytest

from imp.db import query_df

pytestmark = [pytest.mark.spark, pytest.mark.integration]


@pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt CLI not installed")
@pytest.mark.parametrize("engine", ["spark", "pandas"])
def test_full_pipeline(engine, pg_dsn, tmp_path, monkeypatch, capsys):
    if engine == "spark":
        pytest.importorskip("pyspark")
    monkeypatch.setenv("SILVER_ENGINE", engine)
    monkeypatch.setenv("PG_DSN", pg_dsn)
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    from imp.cli import main
    assert main(["run", "--demo", "--start", "2025-01-01", "--end", "2025-07-31"]) == 0

    adj = query_df(pg_dsn, """select trade_date, close, adj_close, adj_return from marts.fact_stock_daily
                              where symbol = 'SBIN' and trade_date between '2025-02-27' and '2025-03-04'
                              order by trade_date""")
    assert float(adj["close"].iloc[0]) / float(adj["close"].iloc[-1]) > 3      # raw price drops ~5x at the split
    assert adj["adj_return"].astype(float).abs().max() < 0.1                  # adjusted series is continuous

    rules = set(query_df(pg_dsn, "select rule from marts.fact_surveillance_alert where symbol = 'SUZLON'")["rule"])
    assert {"pump", "dump_after_pump"} <= rules                               # planted pump-and-dump found

    sbin = query_df(pg_dsn, "select quantity from marts.mart_portfolio_holdings where owner='sample' and instrument='SBIN'")
    assert float(sbin["quantity"].iloc[0]) == pytest.approx(150)              # 30 shares x 5 after the split

    # idempotent: running the same range again changes nothing
    before = query_df(pg_dsn, "select count(*) n from raw.equity_daily")["n"].iloc[0]
    assert main(["run", "--demo", "--start", "2025-01-01", "--end", "2025-07-31"]) == 0
    assert query_df(pg_dsn, "select count(*) n from raw.equity_daily")["n"].iloc[0] == before
