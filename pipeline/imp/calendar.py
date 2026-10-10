"""NSE trading calendar: weekdays minus exchange holidays (dbt/seeds/nse_holidays.csv)."""
from __future__ import annotations

import csv
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

HOLIDAYS_CSV = Path(__file__).resolve().parents[2] / "dbt" / "seeds" / "nse_holidays.csv"


@lru_cache(maxsize=1)
def holidays() -> frozenset[date]:
    if not HOLIDAYS_CSV.exists():
        return frozenset()
    with HOLIDAYS_CSV.open() as fh:
        return frozenset(date.fromisoformat(r["holiday_date"]) for r in csv.DictReader(fh))


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in holidays()


def trading_days(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def previous_trading_day(d: date) -> date:
    d -= timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d
