"""Build the wheel with a version that is unique to this build.

Serverless pipelines cache the Python environment they install, keyed by the wheel's path,
which contains its version. Deploying new code under an unchanged version (0.1.0) made the
pipeline keep running the old code, with no error. Every build therefore gets a
`.dev<timestamp>` suffix. The version in the source file is restored afterwards, so the
working tree stays clean. Set FIDC_BUILD_ID (for example a CI run number) to choose the
suffix instead of the timestamp.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "src" / "fidc_lakehouse" / "__init__.py"


def main() -> None:
    original = VERSION_FILE.read_text(encoding="utf-8")
    base = re.search(r'^__version__ = "([^"]+)"', original, re.MULTILINE).group(1)
    stamp = os.environ.get("FIDC_BUILD_ID") or time.strftime("%Y%m%d%H%M%S", time.gmtime())
    for old in (ROOT / "dist").glob("*.whl"):  # the bundle attaches every wheel it finds
        old.unlink()
    VERSION_FILE.write_text(
        original.replace(f'"{base}"', f'"{base}.dev{stamp}"', 1), encoding="utf-8"
    )
    try:
        subprocess.run(["uv", "build", "--wheel"], cwd=ROOT, check=True)
    finally:
        VERSION_FILE.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    main()
