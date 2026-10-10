"""AMFI mutual fund NAVs -> Bronze (raw NAVAll.txt per day) and a parser used by the Silver step."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from .bronze import partition_dir, record, write_atomic
from .http import Client

NAVALL_URL = "https://www.amfiindia.com/spages/NAVAll.txt"


def extract_navall(d: date, bronze: Path, client: Client, force: bool = False) -> dict:
    """AMFI publishes only the LATEST NAVs; ``d`` is the business date this snapshot is filed under."""
    out = partition_dir(bronze, "amfi_nav", d) / "NAVAll.txt"
    if out.exists() and not force:
        return record(bronze, "amfi_nav", d, "exists", out)
    blob = client.get(NAVALL_URL, nse=False)
    if blob is None:
        return record(bronze, "amfi_nav", d, "not_published")
    write_atomic(out, blob)
    return record(bronze, "amfi_nav", d, "ok", out, rows=blob.count(b"\n"))


def parse_navall(text: str) -> list[dict]:
    """Rows of NAVAll.txt: 6 ';'-separated fields; category and AMC lines act as section headers."""
    rows, category, amc = [], None, None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("Scheme Code"):
            continue
        parts = line.split(";")
        if len(parts) == 6:
            code, isin_g, isin_r, name, nav, dt = (p.strip() for p in parts)
            try:
                nav_val = float(nav)
            except ValueError:
                continue  # "N.A."
            rows.append({"scheme_code": int(code), "isin_growth": isin_g if isin_g not in ("", "-") else None,
                         "scheme_name": name, "nav": nav_val, "nav_date_raw": dt, "amc": amc, "category": category})
        elif "Schemes(" in line or line.endswith("Schemes"):
            category, amc = line, None
        else:
            amc = line
    return rows
