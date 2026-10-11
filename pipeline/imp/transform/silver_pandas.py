"""Silver equity with pandas: same output as the Spark job, without Java or PySpark.

Used by the lightweight setup (SILVER_ENGINE=pandas), e.g. on laptops where the Spark image is
too large. Output layout is identical: <silver>/equity_daily/trade_date=YYYY-MM-DD/part-0.parquet,
one directory per date, overwritten on re-run (idempotent).
"""
from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from .silver_schema import COLUMNS, LEGACY, UDIFF

NUMERIC = ["open", "high", "low", "close", "prev_close", "turnover"]
INTEGER = ["volume", "trades", "deliv_qty"]


def _dates(root: Path, start: date, end: date) -> list[tuple[date, Path]]:
    out = []
    for p in sorted(root.glob("trade_date=*")):
        d = date.fromisoformat(p.name.split("=", 1)[1])
        if start <= d <= end:
            out.append((d, p))
    return out


def _mmm(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.str.strip().str.title(), format="%d-%b-%Y").dt.date


def _read_bhavcopy(path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype=str, skipinitialspace=True)
    raw.columns = [c.strip() for c in raw.columns]
    if "TckrSymb" in raw.columns:
        df = raw[list(UDIFF)].rename(columns=UDIFF)
        df["trade_date"] = pd.to_datetime(df["trade_date"].str.strip(), format="%Y-%m-%d").dt.date
    else:
        df = raw[list(LEGACY)].rename(columns=LEGACY)
        df["company_name"] = None
        df["trade_date"] = _mmm(df["trade_date"])
    for c in ("symbol", "series"):
        df[c] = df[c].str.strip().str.upper()
    for c in ("isin", "company_name"):
        df[c] = df[c].where(df[c].isna(), df[c].astype(str).str.strip())
    for c in NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("volume", "trades"):
        df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("Int64")
    return df


def _read_delivery(path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype=str, skipinitialspace=True)
    raw.columns = [c.strip() for c in raw.columns]
    return pd.DataFrame({
        "trade_date": _mmm(raw["DATE1"]),
        "symbol": raw["SYMBOL"].str.strip().str.upper(),
        "series": raw["SERIES"].str.strip().str.upper(),
        "deliv_qty": pd.to_numeric(raw["DELIV_QTY"].str.strip(), errors="coerce").round().astype("Int64"),
        "deliv_per": pd.to_numeric(raw["DELIV_PER"].str.strip(), errors="coerce"),
    })


def clean(df: pd.DataFrame, series=("EQ", "BE"), symbols: list[str] | None = None) -> pd.DataFrame:
    df = df[df["series"].isin(series)]
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    valid = (df["trade_date"].notna() & (df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)
             & (df["high"] >= df["low"]) & (df["volume"].fillna(-1) >= 0))
    df = df[valid]
    # same tie-break as the Spark job: highest volume, then ISIN
    df = df.sort_values(["trade_date", "symbol", "series", "volume", "isin"], ascending=[True, True, True, False, True])
    return df.drop_duplicates(["trade_date", "symbol", "series"], keep="first")


def build_silver(bronze: Path, silver: Path, start: date, end: date, symbols: list[str] | None = None) -> dict:
    out_root = silver / "equity_daily"
    deliv_dirs = dict(_dates(bronze / "nse_delivery", start, end))
    rows = n_dates = dropped = 0
    for d, p in _dates(bronze / "nse_bhavcopy", start, end):
        if not (p / "bhavcopy.csv").exists():
            continue
        raw = _read_bhavcopy(p / "bhavcopy.csv")
        n_raw = int(raw["series"].isin(["EQ", "BE"]).sum())
        df = clean(raw, symbols=symbols)
        if d in deliv_dirs and (deliv_dirs[d] / "delivery.csv").exists():
            df = df.merge(_read_delivery(deliv_dirs[d] / "delivery.csv"), on=["trade_date", "symbol", "series"], how="left")
        else:
            df["deliv_qty"], df["deliv_per"] = pd.array([pd.NA] * len(df), dtype="Int64"), float("nan")
        target = out_root / f"trade_date={d.isoformat()}"
        if target.exists():
            shutil.rmtree(target)  # partition overwrite = idempotent
        target.mkdir(parents=True)
        out = df[[c for c in COLUMNS if c != "trade_date"]].copy()
        for c in ("symbol", "isin", "series", "company_name"):
            out[c] = out[c].astype("string")  # all-null columns must still be typed, or partitions won't merge
        out["deliv_per"] = out["deliv_per"].astype("float64")
        out.to_parquet(target / "part-0.parquet", index=False)
        rows, n_dates = rows + len(df), n_dates + 1
        dropped += n_raw - len(df) if not symbols else 0
    return {"rows": rows, "dates": n_dates, "dropped": None if symbols else dropped, "engine": "pandas"}
