"""Silver for the small sources (mutual fund NAVs, corporate actions): pandas -> Parquet.

These are a few thousand rows a day, so Spark would add overhead without benefit.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from imp.extract.amfi import parse_navall
from imp.extract.nse import parse_corp_action


def build_nav(bronze: Path, silver: Path, start: date, end: date, schemes: list[str] | None = None) -> dict:
    out_root = silver / "fund_nav"
    n = 0
    for p in sorted((bronze / "amfi_nav").glob("nav_date=*")):
        d = date.fromisoformat(p.name.split("=", 1)[1])
        if not (start <= d <= end) or not (p / "NAVAll.txt").exists():
            continue
        df = pd.DataFrame(parse_navall((p / "NAVAll.txt").read_text(encoding="utf-8", errors="replace")))
        if df.empty:
            continue
        if schemes:
            df = df[df["scheme_code"].astype(str).isin(schemes)]
        # AMFI's own date column is the NAV date (may lag the snapshot date on holidays)
        df["nav_date"] = pd.to_datetime(df.pop("nav_date_raw"), format="%d-%b-%Y").dt.date
        target = out_root / f"snapshot_date={d.isoformat()}"
        target.mkdir(parents=True, exist_ok=True)
        df.to_parquet(target / "part-0.parquet", index=False)  # overwrite = idempotent
        n += len(df)
    return {"rows": n}


def build_corp_actions(bronze: Path, silver: Path) -> dict:
    rows = []
    for f in sorted((bronze / "nse_corp_actions").rglob("*.json")):
        for ca in json.loads(f.read_text()):
            kind, factor = parse_corp_action(ca.get("subject", ""))
            rows.append({"symbol": (ca.get("symbol") or "").strip().upper(), "isin": ca.get("isin"),
                         "company_name": ca.get("comp"), "subject": ca.get("subject"),
                         "ex_date": datetime.strptime(ca["exDate"], "%d-%b-%Y").date() if ca.get("exDate") else None,
                         "action_type": kind, "adj_factor": factor})
    df = pd.DataFrame(rows, columns=["symbol", "isin", "company_name", "subject", "ex_date", "action_type", "adj_factor"])
    df = df.dropna(subset=["ex_date"]).drop_duplicates(["symbol", "ex_date", "subject"])
    silver.mkdir(parents=True, exist_ok=True)
    df.to_parquet(silver / "corp_actions.parquet", index=False)
    return {"rows": len(df), "price_affecting": int((df["adj_factor"] != 1).sum())}
