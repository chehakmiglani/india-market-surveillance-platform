import json
from datetime import date

import pytest

from imp.calendar import is_trading_day, previous_trading_day, trading_days
from imp.demo import EVENTS, write_demo_bronze
from imp.extract.amfi import parse_navall
from imp.extract.bronze import partition_dir, read_manifest, record, write_atomic
from imp.extract.nse import bhavcopy_url, delivery_url, parse_corp_action


@pytest.mark.parametrize("subject,expected", [
    ("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share", ("split", 5.0)),
    ("Face Value Split (Sub-Division) - From Rs 2/- Per Share To Re 1/- Per Share", ("split", 2.0)),
    ("Bonus 1:1", ("bonus", 2.0)),
    ("Bonus 3:2", ("bonus", 2.5)),
    ("Dividend - Rs 6.50 Per Share", ("other", 1.0)),
    ("", ("other", 1.0)),
])
def test_parse_corp_action(subject, expected):
    kind, f = parse_corp_action(subject)
    assert kind == expected[0] and f == pytest.approx(expected[1])


def test_urls():
    assert bhavcopy_url(date(2026, 10, 6)).endswith("BhavCopy_NSE_CM_0_0_0_20261006_F_0000.csv.zip")
    assert bhavcopy_url(date(2024, 2, 5)).endswith("/2024/FEB/cm05FEB2024bhav.csv.zip")
    assert delivery_url(date(2026, 10, 6)).endswith("sec_bhavdata_full_06102026.csv")


def test_calendar():
    assert not is_trading_day(date(2026, 10, 2))   # Gandhi Jayanti
    assert not is_trading_day(date(2026, 10, 3))   # Saturday
    assert is_trading_day(date(2026, 10, 5))
    assert previous_trading_day(date(2026, 10, 5)) == date(2026, 10, 1)
    assert date(2026, 10, 2) not in trading_days(date(2026, 9, 28), date(2026, 10, 9))


def test_bronze_atomic_and_manifest(tmp_path):
    p = partition_dir(tmp_path, "nse_bhavcopy", date(2026, 10, 6)) / "bhavcopy.csv"
    write_atomic(p, b"a,b\n1,2\n")
    assert p.read_bytes() == b"a,b\n1,2\n" and not p.with_suffix(".csv.tmp").exists()
    assert "trade_date=2026-10-06" in str(p)
    record(tmp_path, "nse_bhavcopy", date(2026, 10, 6), "ok", p, rows=1)
    m = read_manifest(tmp_path)
    assert m[0]["status"] == "ok" and len(m[0]["sha256"]) == 64


def test_demo_bronze_formats(tmp_path):
    info = write_demo_bronze(tmp_path, date(2024, 7, 1), date(2024, 7, 12))
    assert info["trading_days"] == 10
    legacy = (partition_dir(tmp_path, "nse_bhavcopy", date(2024, 7, 5)) / "bhavcopy.csv").read_text()
    udiff = (partition_dir(tmp_path, "nse_bhavcopy", date(2024, 7, 8)) / "bhavcopy.csv").read_text()
    assert legacy.startswith("SYMBOL,SERIES") and udiff.startswith("TradDt,BizDt")
    nav = parse_navall((partition_dir(tmp_path, "amfi_nav", date(2024, 7, 8)) / "NAVAll.txt").read_text())
    assert len(nav) == 4 and all(r["amc"] and r["category"] for r in nav)
    ca = json.loads(next((tmp_path / "nse_corp_actions").rglob("*.json")).read_text())
    assert any(c["subject"].startswith("Dividend") for c in ca)


def test_demo_split_drops_raw_price(tmp_path):
    ex = EVENTS["SBIN"][0]
    write_demo_bronze(tmp_path, date(2025, 2, 24), date(2025, 3, 7))
    def close(d):
        txt = (partition_dir(tmp_path, "nse_bhavcopy", d) / "bhavcopy.csv").read_text().splitlines()
        row = next(line for line in txt if ",SBIN," in line).split(",")
        return float(row[17])
    before, after = close(previous_trading_day(ex)), close(ex)
    assert 3.5 < before / after < 7  # ~5x drop from the 10 -> 2 split
