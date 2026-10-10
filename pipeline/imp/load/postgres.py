"""Load Silver Parquet into Postgres `raw.*` tables, idempotently.

Each load runs in ONE transaction: delete the business dates being loaded, then COPY the new
rows. Re-running a date therefore replaces it exactly; a failure rolls back to the old data.
"""
from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2

DDL = Path(__file__).with_name("sql") / "raw_ddl.sql"
EQUITY_COLS = ["trade_date", "symbol", "isin", "series", "company_name", "open", "high", "low", "close",
               "prev_close", "volume", "turnover", "trades", "deliv_qty", "deliv_per"]
NAV_COLS = ["snapshot_date", "scheme_code", "isin_growth", "scheme_name", "nav", "nav_date", "amc", "category"]
CA_COLS = ["symbol", "isin", "company_name", "subject", "ex_date", "action_type", "adj_factor"]


def connect(dsn: str):
    return psycopg2.connect(dsn)


def ensure_schema(dsn: str) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL.read_text())


def _copy(cur, table: str, df: pd.DataFrame, cols: list[str]) -> None:
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(f"copy {table} ({', '.join(cols)}) from stdin with (format csv, null '\\N')", buf)


def _read_partitions(root: Path, key: str, start: date, end: date) -> pd.DataFrame:
    frames = []
    for p in sorted(root.glob(f"{key}=*")):
        d = date.fromisoformat(p.name.split("=", 1)[1])
        if start <= d <= end:
            df = pd.read_parquet(p)
            df[key] = d
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_equity(dsn: str, silver: Path, start: date, end: date) -> int:
    df = _read_partitions(silver / "equity_daily", "trade_date", start, end)
    if df.empty:
        return 0
    for c in ("volume", "trades", "deliv_qty"):
        df[c] = df[c].astype("Int64")
    dates = sorted(set(df["trade_date"]))
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("delete from raw.equity_daily where trade_date = any(%s)", (dates,))
        _copy(cur, "raw.equity_daily", df, EQUITY_COLS)
    return len(df)


def load_nav(dsn: str, silver: Path, start: date, end: date) -> int:
    df = _read_partitions(silver / "fund_nav", "snapshot_date", start, end)
    if df.empty:
        return 0
    df["scheme_code"] = df["scheme_code"].astype(int)
    df = df.drop_duplicates(["snapshot_date", "scheme_code"])
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("delete from raw.fund_nav where snapshot_date = any(%s)", (sorted(set(df["snapshot_date"])),))
        _copy(cur, "raw.fund_nav", df, NAV_COLS)
    return len(df)


def load_corp_actions(dsn: str, silver: Path) -> int:
    f = silver / "corp_actions.parquet"
    if not f.exists():
        return 0
    df = pd.read_parquet(f)
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("truncate raw.corp_actions")  # small reference table: full refresh
        _copy(cur, "raw.corp_actions", df, CA_COLS)
    return len(df)


def load_portfolio_csv(dsn: str, csv_path: Path, owner: str) -> int:
    """Private portfolio (kept out of git): columns txn_date,instrument,asset_type,txn_type,quantity,price."""
    df = pd.read_csv(csv_path)
    df["owner"] = owner
    cols = ["owner", "txn_date", "instrument", "asset_type", "txn_type", "quantity", "price"]
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("delete from raw.portfolio_txn where owner = %s", (owner,))
        _copy(cur, "raw.portfolio_txn", df, cols)
    return len(df)


def log_run(dsn: str, step: str, business_date: date | None, status: str, rows: int | None = None,
            details: dict | None = None) -> None:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("insert into raw.pipeline_runs (step, business_date, status, rows, details) values (%s,%s,%s,%s,%s)",
                    (step, business_date, status, rows, json.dumps(details or {}, default=str)))
