"""Synthetic Bronze data in the REAL NSE/AMFI file formats, for offline runs, CI and tests.

NOT market data. It reproduces the file layouts (UDiFF and legacy bhavcopy, full bhavcopy
with delivery %, corporate-action JSON, AMFI NAVAll.txt) and plants known events so every
pipeline stage can be checked:
  * SBIN 10 -> 2 face-value split, INFY 1:1 bonus (raw prices jump; adjusted series must not)
  * a pump-and-dump in SUZLON, random volume spikes elsewhere
"""
from __future__ import annotations

import json
from datetime import date

import numpy as np

from .calendar import trading_days
from .extract.bronze import partition_dir, record, write_atomic
from .extract.nse import UDIFF_START

UNIVERSE = {  # symbol: (company, isin, start price, daily vol, typical volume)
    "RELIANCE": ("RELIANCE INDUSTRIES LTD", "INE002A01018", 2400, 0.015, 6e6),
    "TCS": ("TATA CONSULTANCY SERV LT", "INE467B01029", 3500, 0.013, 2e6),
    "INFY": ("INFOSYS LIMITED", "INE009A01021", 1450, 0.016, 7e6),
    "HDFCBANK": ("HDFC BANK LTD", "INE040A01034", 1550, 0.013, 1.5e7),
    "SBIN": ("STATE BANK OF INDIA", "INE062A01020", 580, 0.017, 1.6e7),
    "ITC": ("ITC LTD", "INE154A01025", 440, 0.012, 1.2e7),
    "MRF": ("MRF LTD", "INE883A01011", 110000, 0.012, 8e3),
    "YESBANK": ("YES BANK LIMITED", "INE528G01035", 17, 0.03, 2e8),
    "SUZLON": ("SUZLON ENERGY LIMITED", "INE040H01021", 30, 0.035, 9e7),
    "IRFC": ("INDIAN RAILWAY FIN CORP L", "INE053F01010", 75, 0.03, 4e7),
}
EVENTS = {  # planted corporate actions: symbol -> (ex_date, subject, factor)
    "SBIN": (date(2025, 3, 3), "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share", 5.0),
    "INFY": (date(2024, 9, 2), "Bonus 1:1", 2.0),
}
PUMP = ("SUZLON", date(2025, 6, 2))  # 6 green days on rising volume and falling delivery, then a crash
SCHEMES = [  # code, name, amc, category, start NAV
    (120465, "Axis Bluechip Fund - Direct Plan - Growth", "Axis Mutual Fund", "Equity Scheme - Large Cap Fund", 48.0),
    (118989, "HDFC Mid-Cap Opportunities Fund - Direct Plan - Growth", "HDFC Mutual Fund", "Equity Scheme - Mid Cap Fund", 120.0),
    (122639, "Parag Parikh Flexi Cap Fund - Direct Plan - Growth", "PPFAS Mutual Fund", "Equity Scheme - Flexi Cap Fund", 60.0),
    (119551, "Aditya Birla Sun Life Liquid Fund - Direct Plan - Growth", "Aditya Birla Sun Life Mutual Fund", "Debt Scheme - Liquid Fund", 380.0),
]


def _tick(x: float) -> float:
    return round(round(x * 20) / 20, 2)


def _simulate(days: list[date], seed: int) -> dict[str, list[dict]]:
    rng = np.random.default_rng(seed)
    out = {}
    pump_start = next((i for i, d in enumerate(days) if d >= PUMP[1]), None)
    for sym, (name, isin, p0, vol, v0) in UNIVERSE.items():
        rows, prev = [], p0
        for i, d in enumerate(days):
            r = rng.normal(0.0003, vol)
            vmult = rng.lognormal(0, 0.35) * (rng.uniform(4, 8) if rng.random() < 0.015 else 1.0)
            deliv = float(np.clip(rng.normal(48, 8), 5, 95))
            if sym == PUMP[0] and pump_start is not None:
                k = i - pump_start
                if 0 <= k < 6:
                    r, vmult, deliv = 0.06 + 0.01 * k, 2.0 + k, 18.0 - k
                elif 6 <= k < 9:
                    r, vmult, deliv = -0.09, 5.0, 25.0
            ev = EVENTS.get(sym)
            factor = ev[2] if ev and d == ev[0] else 1.0
            close = max(_tick(prev * np.exp(r) / factor), 0.05)
            ref = prev / factor
            open_ = _tick(ref * (1 + rng.normal(0, vol / 3)))
            high = _tick(max(open_, close) * (1 + abs(rng.normal(0, vol / 2))))
            low = max(_tick(min(open_, close) * (1 - abs(rng.normal(0, vol / 2)))), 0.05)
            volume = int(v0 * vmult * (factor if factor > 1 else 1))
            rows.append({"date": d, "symbol": sym, "name": name, "isin": isin, "open": open_, "high": high,
                         "low": low, "close": close, "prev_close": _tick(prev / factor) if factor > 1 else prev,
                         "volume": volume, "turnover": round(volume * (high + low + close) / 3, 2),
                         "trades": max(volume // 150, 1), "deliv_per": round(deliv, 2)})
            prev = close
        out[sym] = rows
    return out


def _udiff(rows: list[dict]) -> str:
    head = ("TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,"
            "StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,"
            "SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,"
            "Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4")
    lines = [head]
    for i, r in enumerate(rows):
        d = r["date"].isoformat()
        lines.append(f"{d},{d},CM,NSE,STK,{1000 + i},{r['isin']},{r['symbol']},EQ,,,,,{r['name']},{r['open']:.2f},"
                     f"{r['high']:.2f},{r['low']:.2f},{r['close']:.2f},{r['close']:.2f},{r['prev_close']:.2f},,"
                     f"{r['close']:.2f},,,{r['volume']},{r['turnover']:.2f},{r['trades']},F1,1,,,,,")
    lines.append(f"{d},{d},CM,NSE,STK,9999,INE000X00000,SOMEBOND,N1,,,,,SOME BOND,100,101,99,100.5,100.5,100,,100.5,,,10,1005,2,F1,1,,,,,")
    return "\n".join(lines) + "\n"


def _legacy(rows: list[dict]) -> str:
    lines = ["SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,"]
    for r in rows:
        ts = r["date"].strftime("%d-%b-%Y").upper()
        lines.append(f"{r['symbol']},EQ,{r['open']:.2f},{r['high']:.2f},{r['low']:.2f},{r['close']:.2f},{r['close']:.2f},"
                     f"{r['prev_close']:.2f},{r['volume']},{r['turnover']:.2f},{ts},{r['trades']},{r['isin']},")
    return "\n".join(lines) + "\n"


def _delivery(rows: list[dict]) -> str:
    lines = ["SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE, "
             "AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER"]
    for r in rows:
        dq = int(r["volume"] * r["deliv_per"] / 100)
        lines.append(f"{r['symbol']}, EQ, {r['date']:%d-%b-%Y}, {r['prev_close']:.2f}, {r['open']:.2f}, {r['high']:.2f}, "
                     f"{r['low']:.2f}, {r['close']:.2f}, {r['close']:.2f}, {(r['high'] + r['low'] + r['close']) / 3:.2f}, "
                     f"{r['volume']}, {r['turnover'] / 1e5:.2f}, {r['trades']}, {dq}, {r['deliv_per']:.2f}")
    return "\n".join(lines) + "\n"


def _navall(d: date, navs: dict[int, float]) -> str:
    lines = ["Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date", ""]
    by_cat: dict[str, list] = {}
    for code, name, amc, cat, _ in SCHEMES:
        by_cat.setdefault(cat, []).append((code, name, amc))
    for cat, items in by_cat.items():
        lines += [f"Open Ended Schemes({cat})", ""]
        for code, name, amc in items:
            lines += [amc, "", f"{code};INF{code:09d};-;{name};{navs[code]:.4f};{d:%d-%b-%Y}"]
    return "\n".join(lines) + "\n"


def write_demo_bronze(bronze, start: date, end: date, seed: int = 7) -> dict:
    """Write synthetic bronze files for every trading day in [start, end]. Returns counts."""
    days = trading_days(start, end)
    sim = _simulate(days, seed)
    rng = np.random.default_rng(seed + 1)
    navs = {code: nav for code, *_, nav in SCHEMES}
    for i, d in enumerate(days):
        rows = [sim[s][i] for s in UNIVERSE]
        bh = partition_dir(bronze, "nse_bhavcopy", d) / "bhavcopy.csv"
        write_atomic(bh, (_udiff(rows) if d >= UDIFF_START else _legacy(rows)).encode())
        record(bronze, "nse_bhavcopy", d, "ok", bh, rows=len(rows), note="synthetic demo")
        dl = partition_dir(bronze, "nse_delivery", d) / "delivery.csv"
        write_atomic(dl, _delivery(rows).encode())
        for code, *_ in SCHEMES:
            drift, vol = (0.00002, 0.0002) if code == 119551 else (0.0004, 0.01)
            navs[code] *= float(np.exp(rng.normal(drift, vol)))
        nv = partition_dir(bronze, "amfi_nav", d) / "NAVAll.txt"
        write_atomic(nv, _navall(d, navs).encode())
    ca = [{"symbol": s, "series": "EQ", "comp": UNIVERSE[s][0], "isin": UNIVERSE[s][1], "subject": subj,
           "exDate": ex.strftime("%d-%b-%Y")} for s, (ex, subj, _) in EVENTS.items() if start <= ex <= end]
    ca.append({"symbol": "ITC", "series": "EQ", "comp": UNIVERSE["ITC"][0], "isin": UNIVERSE["ITC"][1],
               "subject": "Dividend - Rs 6.50 Per Share", "exDate": days[len(days) // 2].strftime("%d-%b-%Y")})
    cp = partition_dir(bronze, "nse_corp_actions", end) / f"corp_actions_{start:%Y%m%d}_{end:%Y%m%d}.json"
    write_atomic(cp, json.dumps(ca).encode())
    return {"trading_days": len(days), "symbols": len(UNIVERSE), "corp_actions": len(ca)}
