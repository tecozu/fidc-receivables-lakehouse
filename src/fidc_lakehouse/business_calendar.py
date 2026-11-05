"""Brazilian exchange (B3) business-day calendar.

Receivables are priced on a 252 business-day basis, so every term (days to
maturity, days since acquisition) is counted in B3 business days. The holiday
list is the dbt seed `b3_holidays.csv`, so Python and SQL share one source of truth.
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

DEFAULT_HOLIDAYS = Path(__file__).resolve().parents[2] / "dbt" / "seeds" / "b3_holidays.csv"


def easter_sunday(year: int) -> date:
    """Anonymous Gregorian algorithm (Meeus/Jones/Butcher)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month, day = divmod(h + ell - 7 * m + 114, 31)
    return date(year, month, day + 1)


def b3_holidays(year: int) -> dict[date, str]:
    """Trading holidays of the Brazilian stock exchange for a given year."""
    easter = easter_sunday(year)
    holidays = {
        date(year, 1, 1): "New Year's Day",
        easter - timedelta(days=48): "Carnival Monday",
        easter - timedelta(days=47): "Carnival Tuesday",
        easter - timedelta(days=2): "Good Friday",
        date(year, 4, 21): "Tiradentes Day",
        date(year, 5, 1): "Labour Day",
        easter + timedelta(days=60): "Corpus Christi",
        date(year, 9, 7): "Independence Day",
        date(year, 10, 12): "Our Lady of Aparecida",
        date(year, 11, 2): "All Souls' Day",
        date(year, 11, 15): "Republic Proclamation Day",
        date(year, 12, 24): "Christmas Eve (no trading)",
        date(year, 12, 25): "Christmas Day",
        date(year, 12, 31): "Last business day of the year (no trading)",
    }
    if year >= 2024:
        holidays[date(year, 11, 20)] = "Black Consciousness Day"
    return holidays


class BusinessCalendar:
    def __init__(self, holidays: set[date]):
        self.holidays = holidays

    @classmethod
    def from_csv(cls, path: Path = DEFAULT_HOLIDAYS) -> BusinessCalendar:
        with open(path, encoding="utf-8") as f:
            return cls({date.fromisoformat(r["holiday_date"]) for r in csv.DictReader(f)})

    def is_business_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self.holidays

    def next_business_day(self, d: date) -> date:
        """`d` itself if it is a business day, otherwise the following one."""
        while not self.is_business_day(d):
            d += timedelta(days=1)
        return d

    def add_business_days(self, d: date, n: int) -> date:
        step = 1 if n >= 0 else -1
        remaining = abs(n)
        while remaining:
            d += timedelta(days=step)
            if self.is_business_day(d):
                remaining -= 1
        return d

    def business_days_between(self, start: date, end: date) -> int:
        """Business days in the interval (start, end] — the convention used for pricing."""
        if end <= start:
            return 0
        return len(self.business_days(start + timedelta(days=1), end))

    def business_days(self, start: date, end: date) -> list[date]:
        """All business days in [start, end]."""
        out, d = [], start
        while d <= end:
            if self.is_business_day(d):
                out.append(d)
            d += timedelta(days=1)
        return out


def write_holiday_seed(path: Path, first_year: int = 2020, last_year: int = 2035) -> int:
    rows = sorted(
        (d, name) for y in range(first_year, last_year + 1) for d, name in b3_holidays(y).items()
    )
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["holiday_date", "holiday_name"])
        w.writerows((d.isoformat(), name) for d, name in rows)
    return len(rows)
