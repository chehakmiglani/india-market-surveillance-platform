"""Bronze layer: raw files exactly as downloaded, partitioned by business date, plus a manifest.

Layout:  <lake>/bronze/<source>/<partition_key>=<YYYY-MM-DD>/<file>
Writes are atomic (temp file + rename) so a crash never leaves a half-written file that a
later run would mistake for a finished download.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

PARTITION_KEY = {"nse_bhavcopy": "trade_date", "nse_delivery": "trade_date",
                 "nse_corp_actions": "as_of", "amfi_nav": "nav_date"}


def partition_dir(bronze: Path, source: str, d: date) -> Path:
    return bronze / source / f"{PARTITION_KEY[source]}={d.isoformat()}"


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def record(bronze: Path, source: str, d: date, status: str, path: Path | None = None, rows: int | None = None,
           note: str = "") -> dict:
    """Append one line to the manifest (an audit trail of every extraction attempt)."""
    entry = {"source": source, "date": d.isoformat(), "status": status,
             "path": str(path) if path else None,
             "sha256": hashlib.sha256(path.read_bytes()).hexdigest() if path and path.exists() else None,
             "rows": rows, "note": note, "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    bronze.mkdir(parents=True, exist_ok=True)
    with (bronze / "_manifest.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
    return entry


def read_manifest(bronze: Path) -> list[dict]:
    p = bronze / "_manifest.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
