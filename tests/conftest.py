import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

import pytest

from fidc_lakehouse.config import load_funds
from fidc_lakehouse.generate import generate


@pytest.fixture(scope="session")
def funds():
    return load_funds()


def _find_java_home() -> str | None:
    """JAVA_HOME if set, else Homebrew's keg-only OpenJDK (it isn't on the PATH)."""
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    for candidate in ("/opt/homebrew/opt/openjdk@21", "/usr/local/opt/openjdk@21"):
        if Path(candidate, "bin", "java").exists():
            return candidate
    return None


def _java_runs() -> bool:
    """True if `java -version` works. macOS ships a `java` stub that exists but fails."""
    if not shutil.which("java"):
        return False
    return subprocess.run(["java", "-version"], capture_output=True).returncode == 0


@pytest.fixture(scope="session")
def spark():
    """One local Spark session for the whole run (starting the JVM takes seconds)."""
    pytest.importorskip("pyspark")
    java_home = _find_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
    elif not _java_runs():
        pytest.skip("Java 17 or 21 is required for the Spark tests")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("fidc-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def landing(tmp_path, funds):
    """A small, deterministic landing zone: two weeks of files for every fund."""
    out = tmp_path / "landing"
    generate(funds, date(2026, 1, 5), date(2026, 1, 16), out, seed=7)
    return out
