# India Market Intelligence & Surveillance Platform

This is an end-to-end data engineering project on **real Indian market data**. Every trading day it:

- downloads NSE equity prices, delivery data, corporate actions and AMFI mutual fund NAVs
- runs them through a **Bronze → Silver → Gold** lakehouse (Python, **PySpark**, **Postgres**, **dbt**),
  orchestrated by **Airflow 3**
- adjusts history for splits and bonuses
- tracks a portfolio (P&L, XIRR)
- runs a **market-surveillance engine** (volume spikes, pump-and-dump, delivery anomalies) and sends alerts
- serves a **Power BI** model

Everything runs free on a laptop with Docker.

See [architecture](docs/architecture.md) and the [data dictionary](docs/data_dictionary.md).

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Airflow 3.1 (TaskFlow), daily DAG with catchup backfill |
| Extraction | Python + requests (NSE session handling, retries, atomic writes, manifest) |
| Lake | Bronze raw files and Silver Parquet, partitioned by date |
| Processing | PySpark 4 (both bhavcopy formats, delivery join, dynamic partition overwrite) |
| Warehouse | Postgres 16 |
| Modelling | dbt: staging → intermediate → marts, SCD2 snapshot, 31 tests |
| Surveillance and alerts | pandas rules engine → Telegram / email |
| BI | Power BI Desktop (measures, theme, RLS, page specs in `powerbi/`) |
| Quality and CI | pytest (unit, Spark, Postgres integration, full pipeline), ruff, GitHub Actions, DAG integrity tests |

## Run it

```bash
cp .env.example .env
make up            # Postgres + Airflow at http://localhost:8080
make demo          # full pipeline on synthetic data, no internet needed
```

For real data, unpause `nse_daily_eod` in the Airflow UI. It backfills every trading day since
2023-10-02, one day at a time, then runs daily at 19:15 IST. NSE often blocks cloud and datacentre IPs,
so run it from a home or office connection. Then connect Power BI to `localhost:5432/market`
(see `powerbi/README.md`).

**Without Docker** (needs Java 17+ and a local Postgres):

```bash
pip install -e ".[dev,spark,dbt]"
export PG_DSN=postgresql://imp:imp@localhost:5432/market LAKE_ROOT=./lake
imp run --demo --start 2024-06-03 --end 2025-07-31
pytest -q
```

## What's verified

The test suite runs the **whole pipeline** against a real Postgres, with Spark and dbt, on synthetic
data that contains planted events. The tests check that:

- the raw SBIN price drops about 5× at its 10→2 split while the adjusted series stays continuous
- a sample holding of 30 SBIN becomes 150 shares at one-fifth of the cost
- INFY's 1:1 bonus doubles the holding
- the planted SUZLON pump is flagged, then the dump that follows it
- re-running the same dates changes nothing (idempotency)
- the DAGs load in Airflow 3.1 with the intended task order

## What is real and what is synthetic

- The **pipeline code** targets the real NSE and AMFI file formats.
- The `demo` data is **synthetic**. It copies those formats so the project runs anywhere.
  Don't present demo numbers as market data.
- Holiday dates for 2026 come from published lists. The 2023–2025 entries in
  `dbt/seeds/nse_holidays.csv` are marked `memory-verify`; check them against NSE circulars.
- NSE URLs change occasionally. `imp/extract/nse.py` keeps them in one place.

## Repository

```
pipeline/imp/     extract (NSE/AMFI → bronze), transform (Spark silver), load, quality,
                  surveillance, portfolio (XIRR), demo data, CLI
dbt/              models (staging, intermediate, marts), snapshots, seeds, tests
dags/             Airflow DAGs: nse_daily_eod, amfi_nav_daily
powerbi/          measures.dax, theme.json, model and page specification
docs/             architecture, data dictionary
tests/            unit, Spark, Postgres integration, end-to-end, DAG integrity
```
