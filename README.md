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

### Low disk space? (Docker "no space left on device" or build failures)

The full image (Airflow, Java, Spark, dbt) needs about 4–5 GB, including Docker's build cache.
Pick the lightest option that works. All three give identical results: the pandas engine is tested
to produce exactly the same Silver table as Spark.

| Option | Disk needed | Command |
|---|---|---|
| Full stack | ~5 GB | `docker compose up -d --build` |
| **Lite**: Airflow without Java/Spark | ~3 GB | set `WITH_SPARK=false` in `.env`, then `docker compose up -d --build` |
| **DB only**: Postgres in Docker, pipeline in local Python | ~0.6 GB | see below |

**First, free the space Docker already uses.** Failed builds leave GBs behind.

```powershell
docker compose down
docker system prune -af        # unused images + stopped containers
docker builder prune -af       # build cache (often the biggest)
docker system df               # check what's left
```

On Windows, if C: is nearly full, open Docker Desktop → *Settings → Resources → Advanced* and move the
**disk image location** to another drive (e.g. D:), or raise the virtual disk limit.

**DB-only option (Windows PowerShell)**, no Java or Spark needed:

```powershell
docker compose up -d postgres
python -m venv .venv; .venv\Scripts\activate
pip install -e ".[dbt]"
$env:SILVER_ENGINE="pandas"; $env:PG_DSN="postgresql://imp:imp@localhost:5432/market"; $env:LAKE_ROOT="lake"
python -m imp.cli run --demo --start 2024-06-03 --end 2025-07-31
```

**No space on your laptop at all? Use GitHub Codespaces.** Push the repo to GitHub, then
*Code → Codespaces → Create codespace*. The dev container (`.devcontainer/`) installs Python, Java
and Docker. In its terminal, run `make up && make demo`, then open the forwarded port 8080 for
Airflow. Pick a 4-core machine if the 2-core one is slow. Codespaces hours are free up to your
account's monthly allowance; stop the codespace when you're done.

**Without Docker at all** (needs a local Postgres; Java 17+ only for the Spark engine):

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
