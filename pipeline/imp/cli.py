"""Command line for every pipeline step (Airflow calls the same functions).

  imp demo-data --start 2024-06-03 --end 2025-07-31      synthetic bronze (offline)
  imp extract   --start D --end D [--sources ...]        NSE/AMFI -> bronze
  imp silver    --start D --end D                        bronze -> silver (Spark + pandas)
  imp load      --start D --end D                        silver -> Postgres raw.*
  imp dbt       [build|test|...]                         run dbt with the project's profile
  imp quality   --date D                                 post-load data-quality checks
  imp surveil   --start D --end D                        surveillance rules -> raw.surveillance_alerts
  imp alert     --date D [--send]                        send (or print) alerts for a day
  imp run       --start D --end D [--demo] [--send]      everything above, in order
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from .config import settings

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"


def _d(s: str) -> date:
    return date.fromisoformat(s)


def step_extract(start: date, end: date, sources: list[str]) -> list[dict]:
    from .calendar import trading_days
    from .extract.amfi import extract_navall
    from .extract.http import Client
    from .extract.nse import extract_bhavcopy, extract_corp_actions, extract_delivery
    s, client, out = settings(), Client(), []
    for d in trading_days(start, end):
        if "bhavcopy" in sources:
            out.append(extract_bhavcopy(d, s.bronze, client))
        if "delivery" in sources:
            out.append(extract_delivery(d, s.bronze, client))
    if "nav" in sources:
        out.append(extract_navall(end, s.bronze, client))
    if "corp" in sources:
        out.append(extract_corp_actions(start, end, s.bronze, client))
    return out


def silver_engine() -> str:
    """SILVER_ENGINE=spark|pandas; default: spark when PySpark is installed, else pandas."""
    choice = os.getenv("SILVER_ENGINE", "").strip().lower()
    if choice in ("spark", "pandas"):
        return choice
    import importlib.util
    return "spark" if importlib.util.find_spec("pyspark") else "pandas"


def step_silver(start: date, end: date) -> dict:
    from .transform.silver_small import build_corp_actions, build_nav
    s = settings()
    if silver_engine() == "spark":
        from .transform.silver_equity import build_silver, spark_session
        spark = spark_session()
        try:
            eq = build_silver(spark, s.bronze, s.silver, start, end, s.symbols or None)
        finally:
            spark.stop()
    else:
        from .transform.silver_pandas import build_silver as build_silver_pd
        eq = build_silver_pd(s.bronze, s.silver, start, end, s.symbols or None)
    return {"equity": eq, "nav": build_nav(s.bronze, s.silver, start, end, s.mf_schemes or None),
            "corp_actions": build_corp_actions(s.bronze, s.silver)}


def step_load(start: date, end: date) -> dict:
    from .load.postgres import ensure_schema, load_corp_actions, load_equity, load_nav, log_run
    s = settings()
    ensure_schema(s.pg_dsn)
    res = {"equity": load_equity(s.pg_dsn, s.silver, start, end), "nav": load_nav(s.pg_dsn, s.silver, start, end),
           "corp_actions": load_corp_actions(s.pg_dsn, s.silver)}
    log_run(s.pg_dsn, "load", end, "ok", res["equity"], res)
    return res


def step_dbt(args: list[str]) -> int:
    dsn = settings().pg_dsn  # postgresql://user:pass@host:port/db -> PG* env vars for profiles.yml
    from urllib.parse import urlparse
    u = urlparse(dsn)
    env = dict(os.environ, PGHOST=u.hostname or "localhost", PGPORT=str(u.port or 5432), PGUSER=u.username or "",
               PGPASSWORD=u.password or "", PGDATABASE=(u.path or "/").lstrip("/"))
    cmd = [os.getenv("DBT_BIN", "dbt"), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    print("+", " ".join(cmd), flush=True)
    return subprocess.call(cmd, env=env)


def step_quality(d: date) -> dict:
    from .load.postgres import log_run
    from .quality.checks import run_checks
    s = settings()
    res = run_checks(s.pg_dsn, d)
    log_run(s.pg_dsn, "quality", d, "ok" if res["passed"] else "failed", None, res)
    return res


def step_surveil(start: date, end: date) -> dict:
    from .load.postgres import log_run
    from .surveillance.engine import run_surveillance
    s = settings()
    res = run_surveillance(s.pg_dsn, start, end)
    log_run(s.pg_dsn, "surveillance", end, "ok", res["alerts"], res)
    return res


def step_alert(d: date, send: bool) -> dict:
    from .surveillance.alerts import dispatch
    return dispatch(settings(), d, send=send)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="imp", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("demo-data", "extract", "silver", "load", "surveil", "run"):
        sp = sub.add_parser(name)
        sp.add_argument("--start", type=_d, required=True)
        sp.add_argument("--end", type=_d, required=True)
        if name == "extract":
            sp.add_argument("--sources", default="bhavcopy,delivery,nav,corp")
        if name == "run":
            sp.add_argument("--demo", action="store_true", help="use synthetic bronze instead of downloading")
            sp.add_argument("--send", action="store_true", help="really send alerts (default: print)")
    sp = sub.add_parser("dbt")
    sp.add_argument("dbt_args", nargs=argparse.REMAINDER)
    for name in ("quality", "alert"):
        sp = sub.add_parser(name)
        sp.add_argument("--date", type=_d, required=True)
        if name == "alert":
            sp.add_argument("--send", action="store_true")
    a = p.parse_args(argv)

    def show(x):
        print(json.dumps(x, indent=2, default=str))

    if a.cmd == "demo-data":
        from .demo import write_demo_bronze
        show(write_demo_bronze(settings().bronze, a.start, a.end))
    elif a.cmd == "extract":
        show(step_extract(a.start, a.end, a.sources.split(",")))
    elif a.cmd == "silver":
        show(step_silver(a.start, a.end))
    elif a.cmd == "load":
        show(step_load(a.start, a.end))
    elif a.cmd == "dbt":
        return step_dbt(a.dbt_args or ["build"])
    elif a.cmd == "quality":
        res = step_quality(a.date)
        show(res)
        return 0 if res["passed"] else 1
    elif a.cmd == "surveil":
        show(step_surveil(a.start, a.end))
    elif a.cmd == "alert":
        show(step_alert(a.date, a.send))
    elif a.cmd == "run":
        if a.demo:
            from .demo import write_demo_bronze
            show(write_demo_bronze(settings().bronze, a.start, a.end))
        else:
            show(step_extract(a.start, a.end, ["bhavcopy", "delivery", "nav", "corp"]))
        show(step_silver(a.start, a.end))
        show(step_load(a.start, a.end))
        if step_dbt(["build", "--exclude", "fact_surveillance_alert"]) != 0:
            return 1
        show(step_surveil(a.start, a.end))
        if step_dbt(["build", "--select", "fact_surveillance_alert", "mart_pipeline_health"]) != 0:
            return 1
        q = step_quality(a.end)
        show(q)
        show(step_alert(a.end, a.send))
        return 0 if q["passed"] else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
