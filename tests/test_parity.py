"""The parity comparison: what counts as a difference between two engines."""

from decimal import Decimal

from fidc_lakehouse.parity import compare

KEY = ("2026-01-02", "ALPHA")


def _row(**changes):
    row = {
        "open_receivables": 10,
        "distinct_debtors": 8,
        "outstanding_amount": Decimal("1000.00"),
        "present_value": Decimal("950.50"),
        "provision_amount": Decimal("12.34"),
        "net_portfolio_value": Decimal("938.16"),
        "overdue_ratio": 0.25,
        "provision_coverage": 0.013,
        "weighted_avg_term_bd": 85.5,
    }
    return {**row, **changes}


def _differences(local, remote):
    return compare({KEY: local}, {KEY: remote})


def test_identical_rows_agree():
    assert _differences(_row(), _row()) == []


def test_numbers_from_text_are_compared_by_value_not_by_spelling():
    # Databricks sends everything as text, with a different number of decimals.
    remote = _row(outstanding_amount="1000.0000", open_receivables="10", overdue_ratio="0.25")
    assert _differences(_row(), remote) == []


def test_a_wrong_count_is_always_a_difference():
    found = _differences(_row(), _row(open_receivables=11))
    assert [d.column for d in found] == ["open_receivables"]


def test_money_may_differ_by_up_to_one_cent_and_no_more():
    assert _differences(_row(), _row(present_value=Decimal("950.51"))) == []
    found = _differences(_row(), _row(present_value=Decimal("950.52")))
    assert [d.column for d in found] == ["present_value"]


def test_ratios_tolerate_the_last_digits_of_floating_point():
    assert _differences(_row(), _row(overdue_ratio=0.25 + 1e-12)) == []
    found = _differences(_row(), _row(overdue_ratio=0.26))
    assert [d.column for d in found] == ["overdue_ratio"]


def test_a_null_against_a_value_is_a_difference_but_two_nulls_agree():
    assert _differences(_row(overdue_ratio=None), _row(overdue_ratio=None)) == []
    found = _differences(_row(overdue_ratio=None), _row(overdue_ratio=0.0))
    assert [d.column for d in found] == ["overdue_ratio"]


def test_a_day_that_exists_on_one_side_only_is_reported():
    found = compare({KEY: _row()}, {})
    assert len(found) == 1 and found[0].column == "(row)"
    found = compare({}, {KEY: _row()})
    assert "databricks" in found[0].local
