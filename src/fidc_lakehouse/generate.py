"""Synthetic data generator.

Simulates what a fund administrator drops in the landing zone every business day:

* `acquisitions_<date>.csv` - installments the fund bought that day (with a discount);
* `settlements_<date>.csv`  - payments received that day (full, partial or cancellation).

The grain is the installment (*parcela*): a contract has one to six installments, and each
one has its own ID (the *nosso numero*), due date and values. Settlements point to the
installment they pay through that ID. See docs/data-contract.md.

Each installment gets a behaviour profile at purchase time (on time, late, partial
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
from fidc_lakehouse.parsers import LAYOUTS_WITH_TRANSACTION_ID

ACQUISITION_COLUMNS = [
    "contract_id", "installment_id", "installment_number", "receivable_type",
    "seller_name", "seller_doc", "originator_name", "originator_doc",
    "debtor_name", "debtor_doc",
    "issue_date", "due_date", "acquisition_date", "face_value", "purchase_price",
]  # fmt: skip
SETTLEMENT_COLUMNS = ["installment_id", "settlement_date", "settlement_type", "amount_paid"]
# Layouts with a provider payment ID carry one more column.
SETTLEMENT_COLUMNS_WITH_TRANSACTION_ID = [
    "installment_id", "transaction_id", "settlement_date", "settlement_type", "amount_paid",
]  # fmt: skip

RECEIVABLE_TYPES = ["TRADE_BILL", "SERVICE_BILL", "CREDIT_NOTE", "PROMISSORY_NOTE"]
PROFILES = ["on_time", "late", "partial_then_full", "default", "cancelled"]
PROFILE_WEIGHTS = [0.74, 0.13, 0.06, 0.05, 0.02]
INSTALLMENT_COUNTS = [1, 2, 3, 4, 5, 6]
INSTALLMENT_COUNT_WEIGHTS = [30, 25, 20, 12, 8, 5]
BUSINESS_DAYS_BETWEEN_INSTALLMENTS = 21  # roughly one month
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
        self.contract_counter = 0
        self.installment_counter = 0
        self.transaction_counter = 0
        self.provider_transaction_id = fund.layouts["settlements"] in LAYOUTS_WITH_TRANSACTION_ID
        # Each fund buys from a small set of sellers/originators and a large set of debtors.
        self.sellers = [(fake.company(), fake.cnpj()) for _ in range(8)]
        self.originators = [(fake.company(), fake.cnpj()) for _ in range(3)]
        self.debtors = [
            (fake.name(), fake.cpf()) if rng.random() < 0.4 else (fake.company(), fake.cnpj())
            for _ in range(250)
        ]
        self.schedule: dict[date, list[dict]] = defaultdict(list)

    @property
    def settlement_columns(self) -> list[str]:
        if self.provider_transaction_id:
            return SETTLEMENT_COLUMNS_WITH_TRANSACTION_ID
        return SETTLEMENT_COLUMNS

    def _new_contract(self, day: date) -> list[dict]:
        """One contract bought on `day`, as one row per installment."""
        rng, cal = self.rng, self.cal
        self.contract_counter += 1
        contract_id = f"CT-{self.fund.code}-{self.contract_counter:06d}"
        count = rng.choices(INSTALLMENT_COUNTS, INSTALLMENT_COUNT_WEIGHTS)[0]
        first_term_bd = rng.randint(15, 60)
        annual_rate = rng.uniform(0.16, 0.42)
        total = round(rng.uniform(500, 45_000), 2)
        face = round(total / count, 2)
        seller, originator, debtor = (
            rng.choice(self.sellers),
            rng.choice(self.originators),
            rng.choice(self.debtors),
        )
        receivable_type = rng.choice(RECEIVABLE_TYPES)
        issue = br_date(cal.add_business_days(day, -rng.randint(0, 10)))

        rows = []
        for number in range(1, count + 1):
            self.installment_counter += 1
            term_bd = first_term_bd + BUSINESS_DAYS_BETWEEN_INSTALLMENTS * (number - 1)
            due = cal.add_business_days(day, term_bd)
            price = round(face / (1 + annual_rate) ** (term_bd / 252), 2)
            row = {
                "contract_id": contract_id,
                "installment_id": f"{self.installment_counter:011d}",
                "installment_number": str(number),
                "receivable_type": receivable_type,
                "seller_name": seller[0], "seller_doc": seller[1],
                "originator_name": originator[0], "originator_doc": originator[1],
                "debtor_name": debtor[0], "debtor_doc": debtor[1],
                "issue_date": issue,
                "due_date": br_date(due),
                "acquisition_date": br_date(day),
                "face_value": br_decimal(face),
                "purchase_price": br_decimal(price),
            }  # fmt: skip
            self._schedule_settlements(row["installment_id"], day, due, face)
            rows.append(row)
        return rows

    def _schedule_settlements(self, installment_id: str, day: date, due: date, face: float):
        rng, cal = self.rng, self.cal
        profile = rng.choices(PROFILES, PROFILE_WEIGHTS)[0]

        def pay(when: date, kind: str, amount: float) -> None:
            self.transaction_counter += 1
            self.schedule[cal.next_business_day(when)].append(
                {
                    "installment_id": installment_id,
                    "transaction_id": f"TX-{self.fund.code}-{self.transaction_counter:09d}",
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
        kinds = [
            "bad_doc", "negative", "missing", "price_above_face", "dup",
            "missing_installment_id", "conflicting_dup",
        ]  # fmt: skip
        kind = self.rng.choice(kinds)
        if kind in ("dup", "conflicting_dup") and not previous:
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
        elif kind == "missing_installment_id":
            row["installment_id"] = ""
        elif kind == "conflicting_dup":
            # Same installment ID as an earlier row, but different contents.
            row = dict(previous[-1])
            row["face_value"] = br_decimal(float(row["face_value"].replace(",", ".")) + 1)
        else:  # dup: same installment sent twice in one file
            row = dict(previous[-1])
        return row

    def _corrupt_settlements(self, settlements: list[dict]) -> tuple[list[dict], int]:
        """Resend some payments and blank some installment IDs, to exercise the gate."""
        out, errors = [], 0
        for row in settlements:
            if self.rng.random() >= ERROR_RATE:
                out.append(row)
                continue
            errors += 1
            if self.rng.random() < 0.5:
                out.extend([row, dict(row)])  # the same payment sent twice
            else:
                out.append({**row, "installment_id": ""})
        return out, errors

    def day(self, d: date) -> tuple[list[dict], list[dict], int]:
        acquisitions, errors = [], 0
        for _ in range(self.rng.randint(5, 15)):
            for row in self._new_contract(d):
                if self.rng.random() < ERROR_RATE:
                    row = self._corrupt(row, acquisitions)
                    errors += 1
                acquisitions.append(row)
        settlements, settlement_errors = self._corrupt_settlements(self.schedule.pop(d, []))
        return acquisitions, settlements, errors + settlement_errors


def _write(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, delimiter=";", extrasaction="ignore")
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
                       sim.settlement_columns, sett)  # fmt: skip
                files += 1
            n_acq, n_set, n_err = n_acq + len(acq), n_set + len(sett), n_err + errs
    return GenerationSummary(files, n_acq, n_set, n_err)
