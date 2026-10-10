import os
import uuid
from datetime import date

import pytest

DSN = os.getenv("PG_DSN", "postgresql://imp:imp@localhost:5432/market")


def _pg_available() -> bool:
    try:
        import psycopg2
        psycopg2.connect(DSN, connect_timeout=2).close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from imp.transform.silver_equity import spark_session
    s = spark_session("imp-tests", "local[2]")
    yield s
    s.stop()


@pytest.fixture
def pg_dsn():
    """A fresh, empty database per test (created and dropped around it)."""
    if not _pg_available():
        pytest.skip("Postgres not reachable at PG_DSN")
    import psycopg2
    name = f"t_{uuid.uuid4().hex[:10]}"
    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    admin.cursor().execute(f"create database {name}")
    dsn = DSN.rsplit("/", 1)[0] + "/" + name
    yield dsn
    admin.cursor().execute(f"drop database {name} with (force)")
    admin.close()


@pytest.fixture(scope="session")
def demo_lake(tmp_path_factory):
    """Synthetic bronze across the legacy -> UDiFF switch and both planted corporate actions."""
    from imp.demo import write_demo_bronze
    lake = tmp_path_factory.mktemp("lake")
    write_demo_bronze(lake / "bronze", date(2024, 6, 3), date(2025, 7, 31))
    return lake
