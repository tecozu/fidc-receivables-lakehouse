"""Synthetic data generator.

Simulates what a fund administrator drops in the landing zone every business day:

* `acquisitions_<date>.csv` - receivables the fund bought that day (with a discount);
* `settlements_<date>.csv`  - payments received that day (full, partial or cancellation).

Each receivable gets a behaviour profile at purchase time (on time, late, partial
then full, default, cancelled), so the portfolio ages realistically and the
loss-provision buckets get exercised. A small share of rows is deliberately broken
(bad IDs, negative values, duplicates) to exercise the ingestion quality gate.

Files use the Brazilian conventions found in real administrator exports:
`;` separator, decimal comma and dd/mm/yyyy dates. Output is fully deterministic
for a given seed. All names and IDs are fake (Faker).
"""

from __future__ import annotations

import csv
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from faker import Faker

from fidc_lakehouse.business_calendar import BusinessCalendar
from fidc_lakehouse.config import Fund

ACQUISITION_COLUMNS = [
    "receivable_id", "contract_id", "receivable_type",
    "seller_name", "seller_doc", "originator_name", "originator_doc",
    "debtor_name", "debtor_doc",
    "issue_date", "due_date", "acquisition_date", "face_value", "purchase_price",
]  # fmt: skip
SETTLEMENT_COLUMNS = ["receivable_id", "settlement_date", "settlement_type", "amount_paid"]

RECEIVABLE_TYPES = ["TRADE_BILL", "SERVICE_BILL", "CREDIT_NOTE", "PROMISSORY_NOTE"]
PROFILES = ["on_time", "late", "partial_then_full", "default", "cancelled"]
PROFILE_WEIGHTS = [0.74, 0.13, 0.06, 0.05, 0.02]
ERROR_RATE = 0.01


def br_decimal(value: float) -> str:
    return f"{value:.2f}".replace(".", ",")


def br_date(d: date) -> str:
    return d.strftime("%d/%m/%Y")


@dataclass
class GenerationSummary:
    files: int
    acquisitions: int
    settlements: int
    injected_errors: int


class FundSimulator:
    def __init__(self, fund: Fund, cal: BusinessCalendar, rng: random.Random, fake: Faker):
        self.fund, self.cal, self.rng, self.fake = fund, cal, rng, fake
        self.counter = 0
        # Each fund buys from a small set of sellers/originators and a large set of debtors.
        self.sellers = [(fake.company(), fake.cnpj()) for _ in range(8)]
        self.originators = [(fake.company(), fake.cnpj()) for _ in range(3)]
        self.debtors = [
            (fake.name(), fake.cpf()) if rng.random() < 0.4 else (fake.company(), fake.cnpj())
            for _ in range(250)
        ]
        self.schedule: dict[date, list[dict]] = defaultdict(list)

    def _new_receivable(self, day: date) -> dict:
        rng, cal = self.rng, self.cal
        self.counter += 1
        term_bd = rng.randint(15, 160)
        due = cal.add_business_days(day, term_bd)
        annual_rate = rng.uniform(0.16, 0.42)
        face = round(rng.uniform(500, 45_000), 2)
        price = round(face / (1 + annual_rate) ** (term_bd / 252), 2)
        seller, originator, debtor = (
            rng.choice(self.sellers),
            rng.choice(self.originators),
            rng.choice(self.debtors),
        )
        row = {
            "receivable_id": f"{self.fund.code}-{self.counter:07d}",
            "contract_id": f"CT-{self.fund.code}-{rng.randint(1, 4000):05d}",
            "receivable_type": rng.choice(RECEIVABLE_TYPES),
            "seller_name": seller[0], "seller_doc": seller[1],
            "originator_name": originator[0], "originator_doc": originator[1],
            "debtor_name": debtor[0], "debtor_doc": debtor[1],
            "issue_date": br_date(cal.add_business_days(day, -rng.randint(0, 10))),
            "due_date": br_date(due),
            "acquisition_date": br_date(day),
            "face_value": br_decimal(face),
            "purchase_price": br_decimal(price),
        }  # fmt: skip
        self._schedule_settlements(row["receivable_id"], day, due, face)
        return row

    def _schedule_settlements(self, rid: str, day: date, due: date, face: float) -> None:
        rng, cal = self.rng, self.cal
        profile = rng.choices(PROFILES, PROFILE_WEIGHTS)[0]

        def pay(when: date, kind: str, amount: float) -> None:
            self.schedule[cal.next_business_day(when)].append(
                {
                    "receivable_id": rid,
                    "settlement_date": br_date(cal.next_business_day(when)),
                    "settlement_type": kind,
                    "amount_paid": br_decimal(amount),
                }
            )

        if profile == "on_time":
            pay(due, "FULL", face)
        elif profile == "late":
            pay(cal.add_business_days(due, rng.randint(3, 70)), "FULL", face)
        elif profile == "partial_then_full":
            first = round(face * rng.uniform(0.3, 0.7), 2)
            pay(due, "PARTIAL", first)
            pay(cal.add_business_days(due, rng.randint(5, 40)), "FULL", round(face - first, 2))
        elif profile == "cancelled":
            pay(cal.add_business_days(day, rng.randint(1, 10)), "CANCELLATION", 0.0)
        # "default": never pays -> ages through the provision buckets

    def _corrupt(self, row: dict, previous: list[dict]) -> dict:
        kind = self.rng.choice(["bad_doc", "negative", "missing", "price_above_face", "dup"])
        if kind == "dup" and not previous:
            kind = "bad_doc"
        row = dict(row)
        if kind == "bad_doc":
            row["debtor_doc"] = "12.ABC.345/0001-XX"
        elif kind == "negative":
            row["face_value"] = "-" + row["face_value"]
        elif kind == "missing":
            row["debtor_doc"] = ""
        elif kind == "price_above_face":
            row["purchase_price"] = br_decimal(float(row["face_value"].replace(",", ".")) * 1.1)
        else:  # dup: same receivable sent twice in one file
            row = dict(previous[-1])
        return row

    def day(self, d: date) -> tuple[list[dict], list[dict], int]:
        acquisitions, errors = [], 0
        for _ in range(self.rng.randint(12, 35)):
            row = self._new_receivable(d)
            if self.rng.random() < ERROR_RATE:
                row = self._corrupt(row, acquisitions)
                errors += 1
            acquisitions.append(row)
        return acquisitions, self.schedule.pop(d, []), errors


def _write(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, delimiter=";")
        w.writeheader()
        w.writerows(rows)


def generate(
    funds: list[Fund],
    start: date,
    end: date,
    out_dir: Path,
    seed: int = 42,
    cal: BusinessCalendar | None = None,
) -> GenerationSummary:
    cal = cal or BusinessCalendar.from_csv()
    rng = random.Random(seed)
    fake = Faker("pt_BR")
    fake.seed_instance(seed)

    sims = [FundSimulator(f, cal, rng, fake) for f in funds]
    files = n_acq = n_set = n_err = 0
    for d in cal.business_days(start, end):
        for sim in sims:
            acq, sett, errs = sim.day(d)
            base = out_dir / sim.fund.code
            _write(base / "acquisitions" / f"acquisitions_{d.isoformat()}.csv",
                   ACQUISITION_COLUMNS, acq)  # fmt: skip
            files += 1
            if sett:
                _write(base / "settlements" / f"settlements_{d.isoformat()}.csv",
                       SETTLEMENT_COLUMNS, sett)  # fmt: skip
                files += 1
            n_acq, n_set, n_err = n_acq + len(acq), n_set + len(sett), n_err + errs
    return GenerationSummary(files, n_acq, n_set, n_err)
