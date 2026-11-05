from datetime import date

from fidc_lakehouse.business_calendar import BusinessCalendar, b3_holidays, easter_sunday

cal = BusinessCalendar.from_csv()


def test_easter_known_dates():
    assert easter_sunday(2024) == date(2024, 3, 31)
    assert easter_sunday(2026) == date(2026, 4, 5)


def test_carnival_and_good_friday_2026():
    h = b3_holidays(2026)
    assert date(2026, 2, 16) in h and date(2026, 2, 17) in h  # Carnival
    assert date(2026, 4, 3) in h  # Good Friday


def test_black_consciousness_day_only_from_2024():
    assert date(2023, 11, 20) not in b3_holidays(2023)
    assert date(2024, 11, 20) in b3_holidays(2024)


def test_weekends_and_holidays_are_not_business_days():
    assert not cal.is_business_day(date(2026, 1, 3))  # Saturday
    assert not cal.is_business_day(date(2026, 1, 1))  # New Year
    assert cal.is_business_day(date(2026, 1, 2))


def test_business_days_between_uses_open_closed_interval():
    # Fri 2026-01-02 -> Fri 2026-01-09: Mon..Fri = 5 business days
    assert cal.business_days_between(date(2026, 1, 2), date(2026, 1, 9)) == 5
    assert cal.business_days_between(date(2026, 1, 9), date(2026, 1, 2)) == 0


def test_add_business_days_skips_carnival():
    # Fri 2026-02-13 + 1 business day skips the weekend and Carnival Mon/Tue
    assert cal.add_business_days(date(2026, 2, 13), 1) == date(2026, 2, 18)


def test_add_and_count_are_consistent():
    start = date(2026, 1, 2)
    for n in (1, 10, 63, 252):
        assert cal.business_days_between(start, cal.add_business_days(start, n)) == n
