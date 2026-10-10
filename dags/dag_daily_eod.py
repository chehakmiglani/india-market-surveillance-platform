"""Daily end-of-day pipeline for NSE equities (Airflow 3, TaskFlow API).

Runs at 19:15 IST Mon-Fri. `catchup=True` + `max_active_runs=1` means turning the DAG on
BACKFILLS every trading day since `start_date`, one day at a time, in order. Every task is
idempotent (keyed on the run's logical date), so any run can be cleared and re-run safely.

check_trading_day >> extract >> corp_actions >> silver >> load >> dbt_build
                  >> surveillance >> dbt_alerts >> quality >> notify
"""
from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, get_current_context, task

IST = pendulum.timezone("Asia/Kolkata")
DEFAULT_ARGS = {"owner": "imp", "retries": 2, "retry_delay": timedelta(minutes=10)}


def _business_date() -> date:
    # logical_date is the start of the data interval; for a 19:15 daily schedule that is the trading day itself
    return get_current_context()["logical_date"].in_timezone(IST).date()


@dag(dag_id="nse_daily_eod", schedule="15 19 * * 1-5", start_date=pendulum.datetime(2023, 10, 2, tz=IST),
     catchup=True, max_active_runs=1, default_args=DEFAULT_ARGS, tags=["nse", "equity", "daily"],
     doc_md=__doc__)
def nse_daily_eod():
    @task.short_circuit
    def check_trading_day() -> bool:
        from imp.calendar import is_trading_day
        return is_trading_day(_business_date())

    @task(retries=6, retry_delay=timedelta(minutes=30))
    def extract() -> dict:
        from imp.cli import step_extract
        from imp.orchestration import NothingToProcess, on_missing_bhavcopy
        d = _business_date()
        results = step_extract(d, d, ["bhavcopy", "delivery"])
        bhav = next(r for r in results if r["source"] == "nse_bhavcopy")
        if bhav["status"] == "not_published":
            try:
                on_missing_bhavcopy(d, pendulum.now(IST).date())
            except NothingToProcess as e:
                raise AirflowSkipException(str(e)) from e
        return {"bhavcopy": bhav["status"]}

    @task
    def corp_actions() -> dict:
        from imp.config import settings
        from imp.extract.http import Client
        from imp.extract.nse import extract_corp_actions
        from imp.orchestration import corp_action_window
        start, end = corp_action_window(_business_date())
        return extract_corp_actions(start, end, settings().bronze, Client())

    @task
    def silver() -> dict:
        from imp.cli import step_silver
        d = _business_date()
        return step_silver(d, d)

    @task
    def load() -> dict:
        from imp.cli import step_load
        d = _business_date()
        return step_load(d, d)

    @task
    def dbt_build() -> None:
        from imp.cli import step_dbt
        if step_dbt(["build", "--exclude", "fact_surveillance_alert"]) != 0:
            raise RuntimeError("dbt build failed - see task log for the failing model/test")

    @task
    def surveillance() -> dict:
        from imp.cli import step_surveil
        d = _business_date()
        return step_surveil(d, d)

    @task
    def dbt_alerts() -> None:
        from imp.cli import step_dbt
        if step_dbt(["build", "--select", "fact_surveillance_alert", "mart_pipeline_health"]) != 0:
            raise RuntimeError("dbt build of alert models failed")

    @task
    def quality() -> dict:
        from imp.cli import step_quality
        res = step_quality(_business_date())
        if not res["passed"]:
            failed = [c for c in res["checks"] if not c["passed"]]
            raise RuntimeError(f"data-quality checks failed: {failed}")
        return res

    @task
    def notify() -> dict:
        import os

        from imp.cli import step_alert
        d = _business_date()
        live = os.getenv("ALERTS_SEND", "false").lower() == "true" and d >= pendulum.now(IST).date() - timedelta(days=1)
        return step_alert(d, send=live)  # never spam alerts while backfilling history

    gate = check_trading_day()
    ex = extract()
    gate >> ex >> corp_actions() >> silver() >> load() >> dbt_build() >> surveillance() >> dbt_alerts() >> quality() >> notify()


nse_daily_eod()
