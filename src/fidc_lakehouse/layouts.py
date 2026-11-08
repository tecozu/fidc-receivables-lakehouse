"""File layouts: how the files of one fund administrator look.

A layout names the columns each landing file must carry and how to read it (separator,
encoding). Funds whose administrators send different formats register a new layout here
and point to it in `funds.yaml`, so onboarding a format is configuration, not code.

This module has no Spark or pandas dependency, so the Databricks pipeline, the local
runner and the data generator all read the same definition.
"""

from __future__ import annotations

from dataclasses import dataclass, field

ACQUISITION_COLUMNS = (
    "contract_id", "installment_id", "installment_number", "receivable_type",
    "seller_name", "seller_doc", "originator_name", "originator_doc",
    "debtor_name", "debtor_doc", "issue_date", "due_date", "acquisition_date",
    "face_value", "purchase_price",
)  # fmt: skip
SETTLEMENT_COLUMNS = ("installment_id", "settlement_date", "settlement_type", "amount_paid")
# Some administrators also send their own payment ID (see docs/data-contract.md).
SETTLEMENT_COLUMNS_WITH_TRANSACTION_ID = (
    "installment_id", "transaction_id", "settlement_date", "settlement_type", "amount_paid",
)  # fmt: skip


@dataclass(frozen=True)
class Layout:
    name: str
    separator: str
    encoding: str
    columns: dict[str, tuple[str, ...]] = field(hash=False)  # event type -> required columns

    def has_transaction_id(self, event: str) -> bool:
        return "transaction_id" in self.columns[event]

    def expected_header(self, event: str) -> str:
        """The first line a file of this layout must have: the columns, in order."""
        return self.separator.join(self.columns[event])

    def reader_options(self) -> dict[str, str]:
        """Options for Spark's CSV reader and for Auto Loader."""
        return {
            "header": "true",
            "sep": self.separator,
            "encoding": self.encoding,
        }


LAYOUTS: dict[str, Layout] = {
    layout.name: layout
    for layout in (
        Layout(
            "csv_br_v1",
            separator=";",
            encoding="UTF-8",
            columns={"acquisitions": ACQUISITION_COLUMNS, "settlements": SETTLEMENT_COLUMNS},
        ),
        Layout(
            "csv_br_v2",
            separator=";",
            encoding="UTF-8",
            columns={
                "acquisitions": ACQUISITION_COLUMNS,
                "settlements": SETTLEMENT_COLUMNS_WITH_TRANSACTION_ID,
            },
        ),
    )
}


def get_layout(name: str) -> Layout:
    try:
        return LAYOUTS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown layout '{name}'. Registered: {sorted(LAYOUTS)}") from exc
