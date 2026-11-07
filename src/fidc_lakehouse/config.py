"""Project paths and fund registry."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"
FUNDS_FILE = Path(__file__).with_name("funds.yaml")  # packaged, so the wheel carries it too


def landing_dir() -> Path:
    return Path(os.getenv("FIDC_LANDING_DIR", PROJECT_ROOT / "data" / "landing"))


def warehouse_path() -> Path:
    return Path(os.getenv("FIDC_DUCKDB_PATH", PROJECT_ROOT / "data" / "warehouse.duckdb"))


@dataclass(frozen=True)
class Fund:
    code: str
    name: str
    layouts: dict[str, str]  # event type -> parser layout


def load_funds(path: Path = FUNDS_FILE) -> list[Fund]:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return [
        Fund(
            code=item["code"],
            name=item["name"],
            layouts={event: spec["layout"] for event, spec in item["events"].items()},
        )
        for item in raw["funds"]
    ]
