"""File layouts.

A layout turns the bytes of one landing file into a DataFrame of *strings* with the
canonical column names. Funds whose administrators send different formats just
register a new layout here and point to it in `funds.yaml`.
"""

from __future__ import annotations

import io
from collections.abc import Callable

import pandas as pd

Parser = Callable[[bytes], pd.DataFrame]


def csv_br_v1(content: bytes) -> pd.DataFrame:
    """`;`-separated UTF-8 CSV with canonical headers (decimal comma, dd/mm/yyyy)."""
    return pd.read_csv(
        io.BytesIO(content), sep=";", dtype=str, keep_default_na=False, encoding="utf-8"
    )


LAYOUTS: dict[str, Parser] = {
    "csv_br_v1": csv_br_v1,
}


def get_parser(layout: str) -> Parser:
    try:
        return LAYOUTS[layout]
    except KeyError as exc:
        raise ValueError(f"Unknown layout '{layout}'. Registered: {sorted(LAYOUTS)}") from exc
