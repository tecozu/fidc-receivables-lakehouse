"""Compare the daily fund summary computed by two engines.

The pipeline runs on DuckDB (local, CI) and on Databricks (cloud). A parity check runs the
same input through both and compares the results. This module holds only the comparison,
with no database code, so it is easy to test. `scripts/check_parity.py` fetches the rows.

Each column has its own rule, because a count must match exactly while a ratio computed
with floating-point arithmetic may differ in its last digits and still be correct.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

KEY = ("as_of_date", "fund_code")

# Exact: counts are integers, and a wrong count is never a rounding matter.
EXACT_COLUMNS = ("open_receivables", "distinct_debtors")
# Money: decimal, compared with an absolute tolerance in currency units (one cent by default).
MONEY_COLUMNS = ("outstanding_amount", "present_value", "provision_amount", "net_portfolio_value")
# Ratios: double precision, compared with a relative tolerance.
RATIO_COLUMNS = ("overdue_ratio", "provision_coverage", "weighted_avg_term_bd")

COLUMNS = EXACT_COLUMNS + MONEY_COLUMNS + RATIO_COLUMNS


@dataclass(frozen=True)
class Difference:
    key: tuple
    column: str
    local: object
    remote: object

    def __str__(self) -> str:
        day, fund = self.key
        return f"{day} {fund:<6} {self.column:<22} local={self.local}  databricks={self.remote}"


def _number(value) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _same(column: str, local, remote, money_tolerance: Decimal, ratio_tolerance: Decimal) -> bool:
    a, b = _number(local), _number(remote)
    if a is None or b is None:
        return a is None and b is None  # two NULLs agree; a NULL against a value does not
    if column in EXACT_COLUMNS:
        return a == b
    if column in MONEY_COLUMNS:
        return abs(a - b) <= money_tolerance
    return abs(a - b) <= ratio_tolerance * max(abs(a), abs(b), Decimal(1))


def compare(
    local: dict[tuple, dict],
    remote: dict[tuple, dict],
    money_tolerance: Decimal = Decimal("0.01"),
    ratio_tolerance: Decimal = Decimal("1e-9"),
) -> list[Difference]:
    """Rows are keyed by (as_of_date, fund_code). Returns every difference found.

    A row that exists on one side only is a difference too: a missing day is exactly the
    kind of problem this check is for.
    """
    differences: list[Difference] = []
    for key in sorted(local.keys() | remote.keys()):
        if key not in local or key not in remote:
            side = "local" if key in local else "databricks"
            differences.append(Difference(key, "(row)", f"only in {side}", ""))
            continue
        for column in COLUMNS:
            if not _same(
                column, local[key][column], remote[key][column], money_tolerance, ratio_tolerance
            ):
                differences.append(Difference(key, column, local[key][column], remote[key][column]))
    return differences
