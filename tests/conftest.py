from datetime import date

import pytest

from fidc_lakehouse.config import load_funds
from fidc_lakehouse.generate import generate


@pytest.fixture(scope="session")
def funds():
    return load_funds()


@pytest.fixture(scope="session")
def spark():
    """One local Spark session for the whole run (starting the JVM takes seconds)."""
    pytest.importorskip("pyspark")
    from fidc_lakehouse.spark_session import JavaNotFoundError, get_spark

    try:
        session = get_spark()
    except JavaNotFoundError as error:
        pytest.skip(str(error))
    yield session
    session.stop()


@pytest.fixture()
def landing(tmp_path, funds):
    """A small, deterministic landing zone: two weeks of files for every fund."""
    out = tmp_path / "landing"
    generate(funds, date(2026, 1, 5), date(2026, 1, 16), out, seed=7)
    return out
