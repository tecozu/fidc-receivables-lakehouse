"""Start a local Spark session for the local runner and the tests.

Spark runs on the JVM, so it needs Java 17 or 21. On Databricks a session already
exists and this module is not used.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession


class JavaNotFoundError(RuntimeError):
    pass


def _java_runs() -> bool:
    """True if `java -version` works. macOS ships a `java` stub that exists but fails."""
    if not shutil.which("java"):
        return False
    return subprocess.run(["java", "-version"], capture_output=True).returncode == 0


def _find_java_home() -> str | None:
    """JAVA_HOME if set, else Homebrew's keg-only OpenJDK (it isn't on the PATH)."""
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    for candidate in ("/opt/homebrew/opt/openjdk@21", "/usr/local/opt/openjdk@21"):
        if Path(candidate, "bin", "java").exists():
            return candidate
    return None


def get_spark() -> SparkSession:
    """The local session, created on first use. Raises JavaNotFoundError without Java."""
    java_home = _find_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
    elif not _java_runs():
        raise JavaNotFoundError(
            "Spark needs Java 17 or 21. Install it (macOS: `brew install openjdk@21`) "
            "or set JAVA_HOME."
        )
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")  # no hostname lookup warning
    session = (
        SparkSession.builder.master(os.environ.get("FIDC_SPARK_MASTER", "local[2]"))
        .appName("fidc-lakehouse")
        .config("spark.ui.enabled", "false")
        # The rules make wide plans that exceed the JVM's method-size limit when fused
        # into one generated function. Spark would fall back to interpreting that stage
        # after logging a long error; at this data size interpreting costs nothing.
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session
