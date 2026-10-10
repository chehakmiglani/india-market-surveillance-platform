"""Daily mutual fund NAV pipeline (AMFI publishes NAVs late in the evening).

AMFI only serves the latest NAVs, so this DAG does not backfill (catchup=False); use
`imp extract`/mfapi.in for history.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

IST = pendulum.timezone("Asia/Kolkata")


@dag(dag_id="amfi_nav_daily", schedule="45 23 * * 1-5", start_date=pendulum.datetime(2026, 1, 1, tz=IST),
     catchup=False, max_active_runs=1, default_args={"owner": "imp", "retries": 3, "retry_delay": timedelta(minutes=20)},
     tags=["amfi", "mutual-funds", "daily"], doc_md=__doc__)
def amfi_nav_daily():
    @task
    def extract_and_load() -> dict:
        from imp.config import settings
        from imp.extract.amfi import extract_navall
        from imp.extract.http import Client
        from imp.load.postgres import ensure_schema, load_nav
        from imp.transform.silver_small import build_nav
        s = settings()
        d = get_current_context()["logical_date"].in_timezone(IST).date()
        extract_navall(d, s.bronze, Client())
        build_nav(s.bronze, s.silver, d, d, s.mf_schemes or None)
        ensure_schema(s.pg_dsn)
        return {"rows": load_nav(s.pg_dsn, s.silver, d, d)}

    @task
    def dbt_funds() -> None:
        from imp.cli import step_dbt
        if step_dbt(["build", "--select", "stg_fund_nav+", "--exclude", "fact_surveillance_alert"]) != 0:
            raise RuntimeError("dbt build for fund models failed")

    extract_and_load() >> dbt_funds()


amfi_nav_daily()
