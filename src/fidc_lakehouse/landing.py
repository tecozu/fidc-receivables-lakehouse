"""The landing zone: where the administrators' files arrive.

Layout on disk: `<landing>/<FUND>/<event>/<event>_<YYYY-MM-DD>.csv`.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from fidc_lakehouse.config import Fund

EVENT_TYPES = ("acquisitions", "settlements")
FILE_PATTERN = re.compile(r"^(?P<event>[a-z]+)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$")


@dataclass(frozen=True)
class LandingFile:
    fund: Fund
    event: str
    file_date: date
    path: Path


def discover_files(
    landing: Path, funds: list[Fund], up_to: date | None = None
) -> Iterator[LandingFile]:
    """Yield the landing files in chronological order, optionally only up to a date."""
    found = []
    for fund in funds:
        for event in EVENT_TYPES:
            for path in (landing / fund.code / event).glob("*.csv"):
                m = FILE_PATTERN.match(path.name)
                if not m or m["event"] != event:
                    continue
                file_date = date.fromisoformat(m["date"])
                if up_to is None or file_date <= up_to:
                    found.append(LandingFile(fund, event, file_date, path))
    yield from sorted(found, key=lambda f: (f.file_date, f.fund.code, f.event))
