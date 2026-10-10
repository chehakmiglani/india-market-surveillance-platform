"""Tiny query helper: run SQL with psycopg2 and return a DataFrame (no SQLAlchemy needed)."""
from __future__ import annotations

import pandas as pd
import psycopg2


def query_df(dsn: str, sql: str, params: tuple | None = None) -> pd.DataFrame:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
