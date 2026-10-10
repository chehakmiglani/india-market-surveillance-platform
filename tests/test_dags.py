"""DAG integrity: files import cleanly in Airflow, tasks are wired in the intended order,
and the backfill settings are what the README promises. Skipped when Airflow isn't installed."""
from datetime import date
from pathlib import Path

import pytest

from imp.orchestration import NothingToProcess, NotPublishedYet, corp_action_window, on_missing_bhavcopy

DAGS = Path(__file__).resolve().parents[1] / "dags"


def test_missing_file_policy():
    with pytest.raises(NotPublishedYet):
        on_missing_bhavcopy(date(2026, 10, 9), today=date(2026, 10, 9))
    with pytest.raises(NothingToProcess):
        on_missing_bhavcopy(date(2025, 1, 6), today=date(2026, 10, 9))
    s, e = corp_action_window(date(2026, 10, 9))
    assert s < date(2026, 10, 9) < e


@pytest.fixture(scope="module")
def dagbag():
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"nse_daily_eod", "amfi_nav_daily"}


def test_daily_dag_shape(dagbag):
    dag = dagbag.dags["nse_daily_eod"]
    assert dag.catchup is True and dag.max_active_runs == 1
    order = ["check_trading_day", "extract", "corp_actions", "silver", "load", "dbt_build", "surveillance",
             "dbt_alerts", "quality", "notify"]
    assert set(dag.task_ids) == set(order)
    for up, down in zip(order, order[1:]):
        assert down in dag.get_task(up).downstream_task_ids, f"{up} should run before {down}"


def test_nav_dag_shape(dagbag):
    dag = dagbag.dags["amfi_nav_daily"]
    assert dag.catchup is False
    assert "dbt_funds" in dag.get_task("extract_and_load").downstream_task_ids
