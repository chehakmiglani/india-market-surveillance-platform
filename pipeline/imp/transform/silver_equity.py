"""Silver equity: PySpark job turning raw bhavcopies (+ delivery files) into one clean, typed table.

* Both bhavcopy formats (legacy before 2024-07-08, UDiFF after) are mapped to one schema.
* Delivery quantity / % from the full bhavcopy is joined in when available (nullable).
* Rows with impossible prices are dropped and counted; duplicates resolved deterministically.
* Output: Parquet partitioned by trade_date, written with DYNAMIC partition overwrite, so
  re-running any date range replaces exactly those partitions (idempotent, backfill-safe).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

UDIFF = {"TradDt": "trade_date", "TckrSymb": "symbol", "ISIN": "isin", "SctySrs": "series",
         "FinInstrmNm": "company_name", "OpnPric": "open", "HghPric": "high", "LwPric": "low",
         "ClsPric": "close", "PrvsClsgPric": "prev_close", "TtlTradgVol": "volume", "TtlTrfVal": "turnover",
         "TtlNbOfTxsExctd": "trades"}
LEGACY = {"TIMESTAMP": "trade_date", "SYMBOL": "symbol", "ISIN": "isin", "SERIES": "series",
          "OPEN": "open", "HIGH": "high", "LOW": "low", "CLOSE": "close", "PREVCLOSE": "prev_close",
          "TOTTRDQTY": "volume", "TOTTRDVAL": "turnover", "TOTALTRADES": "trades"}
COLUMNS = ["trade_date", "symbol", "isin", "series", "company_name", "open", "high", "low", "close",
           "prev_close", "volume", "turnover", "trades", "deliv_qty", "deliv_per"]


def spark_session(app: str = "imp-silver", master: str = "local[*]") -> SparkSession:
    return (SparkSession.builder.appName(app).master(master)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.session.timeZone", "Asia/Kolkata")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def _partitions(bronze: Path, source: str, start: date, end: date) -> list[Path]:
    root = bronze / source
    out = []
    for p in sorted(root.glob("trade_date=*")):
        d = date.fromisoformat(p.name.split("=", 1)[1])
        if start <= d <= end and any(p.glob("*.csv")):
            out.append(p)
    return out


def _read_csv(spark: SparkSession, paths: list[str]) -> DataFrame:
    df = (spark.read.option("header", True).option("ignoreLeadingWhiteSpace", True)
          .option("ignoreTrailingWhiteSpace", True).csv(paths))
    return df.toDF(*[c.strip() for c in df.columns])


def _mmm_date(col, fmt: str):
    """Parse dd-MMM-yyyy dates whatever the month's case (NSE mixes "05-FEB-2024" and "05-Feb-2024")."""
    s = F.trim(col)
    if "MMM" not in fmt:
        return F.to_date(s, fmt)
    fixed = F.concat(F.substring(s, 1, 3), F.upper(F.substring(s, 4, 1)), F.lower(F.substring(s, 5, 2)),
                     F.substring(s, 7, 10))
    return F.to_date(fixed, fmt)


def _normalise(df: DataFrame, mapping: dict[str, str], date_fmt: str) -> DataFrame:
    sel = [F.col(src).alias(dst) for src, dst in mapping.items()]
    if "company_name" not in mapping.values():
        sel.append(F.lit(None).cast("string").alias("company_name"))
    out = df.select(*sel)
    return (out.withColumn("trade_date", _mmm_date(F.col("trade_date"), date_fmt))
            .withColumn("symbol", F.upper(F.trim("symbol"))).withColumn("series", F.upper(F.trim("series")))
            .withColumn("isin", F.trim("isin"))
            .withColumn("company_name", F.trim("company_name"))
            .withColumn("open", F.col("open").cast("double")).withColumn("high", F.col("high").cast("double"))
            .withColumn("low", F.col("low").cast("double")).withColumn("close", F.col("close").cast("double"))
            .withColumn("prev_close", F.col("prev_close").cast("double"))
            .withColumn("volume", F.col("volume").cast("double").cast("long"))
            .withColumn("turnover", F.col("turnover").cast("double"))
            .withColumn("trades", F.col("trades").cast("double").cast("long")))


def read_bhavcopies(spark: SparkSession, bronze: Path, start: date, end: date) -> DataFrame | None:
    parts = _partitions(bronze, "nse_bhavcopy", start, end)
    if not parts:
        return None
    by_format: dict[str, list[str]] = {"udiff": [], "legacy": []}
    for p in parts:
        f = p / "bhavcopy.csv"
        header = f.open().readline()
        by_format["udiff" if "TckrSymb" in header else "legacy"].append(str(f))
    frames = []
    if by_format["udiff"]:
        frames.append(_normalise(_read_csv(spark, by_format["udiff"]), UDIFF, "yyyy-MM-dd"))
    if by_format["legacy"]:
        frames.append(_normalise(_read_csv(spark, by_format["legacy"]), LEGACY, "dd-MMM-yyyy"))
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def read_delivery(spark: SparkSession, bronze: Path, start: date, end: date) -> DataFrame | None:
    parts = _partitions(bronze, "nse_delivery", start, end)
    if not parts:
        return None
    df = _read_csv(spark, [str(p / "delivery.csv") for p in parts])
    return (df.select(_mmm_date(F.col("DATE1"), "dd-MMM-yyyy").alias("trade_date"),
                      F.upper(F.trim("SYMBOL")).alias("symbol"), F.upper(F.trim("SERIES")).alias("series"),
                      F.trim("DELIV_QTY").cast("double").cast("long").alias("deliv_qty"),
                      F.trim("DELIV_PER").cast("double").alias("deliv_per")))


def clean(df: DataFrame, series: tuple[str, ...] = ("EQ", "BE"), symbols: list[str] | None = None) -> DataFrame:
    df = df.filter(F.col("series").isin(*series))
    if symbols:
        df = df.filter(F.col("symbol").isin(*symbols))
    valid = ((F.col("trade_date").isNotNull()) & (F.col("open") > 0) & (F.col("high") > 0) & (F.col("low") > 0)
             & (F.col("close") > 0) & (F.col("high") >= F.col("low")) & (F.col("volume") >= 0))
    df = df.filter(valid)
    # one row per (date, symbol, series); prefer the most-traded record if a file has duplicates
    w = Window.partitionBy("trade_date", "symbol", "series").orderBy(F.col("volume").desc(), F.col("isin"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_silver(spark: SparkSession, bronze: Path, silver: Path, start: date, end: date,
                 symbols: list[str] | None = None) -> dict:
    raw = read_bhavcopies(spark, bronze, start, end)
    if raw is None:
        return {"rows": 0, "dates": 0, "dropped": 0}
    n_raw = raw.filter(F.col("series").isin("EQ", "BE")).count()
    df = clean(raw, symbols=symbols)
    deliv = read_delivery(spark, bronze, start, end)
    if deliv is not None:
        df = df.join(deliv, ["trade_date", "symbol", "series"], "left")
    else:
        df = df.withColumn("deliv_qty", F.lit(None).cast("long")).withColumn("deliv_per", F.lit(None).cast("double"))
    df = df.select(*COLUMNS).cache()
    rows = df.count()
    dates = df.select("trade_date").distinct().count()
    df.write.mode("overwrite").partitionBy("trade_date").parquet(str(silver / "equity_daily"))
    df.unpersist()
    dropped = n_raw - rows if not symbols else None
    return {"rows": rows, "dates": dates, "dropped": dropped}
