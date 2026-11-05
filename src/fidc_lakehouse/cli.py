"""Command-line entry point: `fidc <command>`."""

from __future__ import annotations

import argparse
from datetime import date

from fidc_lakehouse import config
from fidc_lakehouse.business_calendar import DEFAULT_HOLIDAYS, write_holiday_seed
from fidc_lakehouse.generate import generate
from fidc_lakehouse.ingest import ingest
from fidc_lakehouse.pipeline import backfill, run_day, summarize

DEMO_START, DEMO_END = date(2026, 1, 2), date(2026, 3, 31)


def _date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="fidc", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write synthetic landing files")
    g.add_argument("--start", type=_date, default=DEMO_START)
    g.add_argument("--end", type=_date, default=DEMO_END)
    g.add_argument("--seed", type=int, default=42)

    i = sub.add_parser("ingest", help="load pending landing files into the raw layer")
    i.add_argument("--up-to", type=_date)

    r = sub.add_parser("run", help="ingest + dbt build for one reference date")
    r.add_argument("--as-of", type=_date, required=True)

    b = sub.add_parser("backfill", help="rebuild the daily snapshot for every business day")
    b.add_argument("--start", type=_date, required=True)
    b.add_argument("--end", type=_date, required=True)

    d = sub.add_parser("demo", help="generate data and backfill the whole demo period")
    d.add_argument("--start", type=_date, default=DEMO_START)
    d.add_argument("--end", type=_date, default=DEMO_END)

    sub.add_parser("holidays", help="regenerate dbt/seeds/b3_holidays.csv")

    args = p.parse_args(argv)
    funds = config.load_funds()

    if args.cmd == "generate":
        s = generate(funds, args.start, args.end, config.landing_dir(), seed=args.seed)
        print(f"{s.files} files, {s.acquisitions} acquisitions, {s.settlements} settlements, "
              f"{s.injected_errors} rows deliberately corrupted")  # fmt: skip
    elif args.cmd == "ingest":
        res = ingest(config.warehouse_path(), config.landing_dir(), funds, up_to=args.up_to)
        print(summarize(res))
    elif args.cmd == "run":
        run_day(args.as_of)
    elif args.cmd == "backfill":
        backfill(args.start, args.end)
    elif args.cmd == "demo":
        s = generate(funds, args.start, args.end, config.landing_dir())
        print(f"generated {s.files} files")
        backfill(args.start, args.end)
    elif args.cmd == "holidays":
        print(f"{write_holiday_seed(DEFAULT_HOLIDAYS)} holidays written to {DEFAULT_HOLIDAYS}")


if __name__ == "__main__":
    main()
