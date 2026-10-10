"""NSE extractors -> Bronze: equity bhavcopy, delivery data, corporate actions.

URL formats (NSE changes these occasionally; update here if known trading days return 404):
  bhavcopy UDiFF (from 2024-07-08): /content/cm/BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip
  bhavcopy legacy (before):         /content/historical/EQUITIES/YYYY/MON/cmDDMONYYYYbhav.csv.zip
  delivery (full bhavcopy):         /products/content/sec_bhavdata_full_DDMMYYYY.csv
  corporate actions (JSON API):     /api/corporates-corporateActions?index=equities&from_date=..&to_date=..
"""
from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import date
from pathlib import Path

from .bronze import partition_dir, record, write_atomic
from .http import Client

ARCHIVE = "https://nsearchives.nseindia.com"
UDIFF_START = date(2024, 7, 8)


def bhavcopy_url(d: date) -> str:
    if d >= UDIFF_START:
        return f"{ARCHIVE}/content/cm/BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
    mon = d.strftime("%b").upper()
    return f"{ARCHIVE}/content/historical/EQUITIES/{d.year}/{mon}/cm{d:%d}{mon}{d.year}bhav.csv.zip"


def delivery_url(d: date) -> str:
    return f"{ARCHIVE}/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"


def corp_actions_url(start: date, end: date) -> str:
    return ("https://www.nseindia.com/api/corporates-corporateActions?index=equities"
            f"&from_date={start:%d-%m-%Y}&to_date={end:%d-%m-%Y}")


def _unzip_csv(blob: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        return zf.read(name)


def extract_bhavcopy(d: date, bronze: Path, client: Client, force: bool = False) -> dict:
    """Download one day's bhavcopy into bronze. Idempotent: an existing file is kept unless force."""
    out = partition_dir(bronze, "nse_bhavcopy", d) / "bhavcopy.csv"
    if out.exists() and not force:
        return record(bronze, "nse_bhavcopy", d, "exists", out)
    blob = client.get(bhavcopy_url(d))
    if blob is None:
        return record(bronze, "nse_bhavcopy", d, "not_published", note="404 - holiday or not yet published")
    write_atomic(out.with_suffix(".csv.zip"), blob)
    csv = _unzip_csv(blob)
    write_atomic(out, csv)
    return record(bronze, "nse_bhavcopy", d, "ok", out, rows=csv.count(b"\n") - 1)


def extract_delivery(d: date, bronze: Path, client: Client, force: bool = False) -> dict:
    out = partition_dir(bronze, "nse_delivery", d) / "delivery.csv"
    if out.exists() and not force:
        return record(bronze, "nse_delivery", d, "exists", out)
    blob = client.get(delivery_url(d))
    if blob is None:
        return record(bronze, "nse_delivery", d, "not_published")
    write_atomic(out, blob)
    return record(bronze, "nse_delivery", d, "ok", out, rows=blob.count(b"\n") - 1)


def extract_corp_actions(start: date, end: date, bronze: Path, client: Client) -> dict:
    """Corporate actions announced for ex-dates in [start, end]; stored under as_of=end."""
    blob = client.get(corp_actions_url(start, end))
    out = partition_dir(bronze, "nse_corp_actions", end) / f"corp_actions_{start:%Y%m%d}_{end:%Y%m%d}.json"
    if blob is None:
        return record(bronze, "nse_corp_actions", end, "not_published")
    json.loads(blob)  # fail fast on an HTML error page
    write_atomic(out, blob)
    return record(bronze, "nse_corp_actions", end, "ok", out, rows=len(json.loads(blob)))


# ---------------------------------------------------------------- corporate action parsing
_SPLIT = re.compile(r"from\s*(?:rs\.?|re\.?|inr)?\s*(\d+(?:\.\d+)?).*?to\s*(?:rs\.?|re\.?|inr)?\s*(\d+(?:\.\d+)?)", re.I)
_BONUS = re.compile(r"bonus\s*(\d+)\s*:\s*(\d+)", re.I)


def parse_corp_action(subject: str) -> tuple[str, float]:
    """(action_type, price adjustment factor) from an NSE corporate-action subject line.

    Prices BEFORE the ex-date are divided by the factor (volumes multiplied) to make history comparable.
      "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share" -> ("split", 5.0)
      "Bonus 1:1"  (1 new share for every 1 held)                                     -> ("bonus", 2.0)
      anything else (dividends, AGMs...)                                              -> ("other", 1.0)
    """
    s = subject or ""
    if "split" in s.lower() or "sub-division" in s.lower():
        m = _SPLIT.search(s)
        if m and float(m.group(2)) > 0:
            return "split", float(m.group(1)) / float(m.group(2))
    m = _BONUS.search(s)
    if m and float(m.group(2)) > 0:
        a, b = float(m.group(1)), float(m.group(2))
        return "bonus", (a + b) / b
    return "other", 1.0
