"""Scheduling decisions kept out of the DAG files so they can be unit-tested without Airflow."""
from __future__ import annotations

from datetime import date


class NotPublishedYet(RuntimeError):
    """Today's file is not on NSE yet: the task should fail and be RETRIED later."""


class NothingToProcess(RuntimeError):
    """A past date has no file (unlisted holiday): downstream tasks should be SKIPPED."""


def on_missing_bhavcopy(business_date: date, today: date) -> None:
    """NSE usually publishes around 6-7 pm IST. Missing today -> retry; missing in the past -> skip."""
    if business_date >= today:
        raise NotPublishedYet(f"bhavcopy for {business_date} not published yet")
    raise NothingToProcess(f"no bhavcopy for {business_date} (holiday missing from the seed?)")


def corp_action_window(business_date: date) -> tuple[date, date]:
    """Corporate actions are announced weeks ahead: refresh ex-dates from a week back to 45 days ahead."""
    from datetime import timedelta
    return business_date - timedelta(days=7), business_date + timedelta(days=45)
