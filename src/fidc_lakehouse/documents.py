"""Validation of Brazilian taxpayer IDs (CPF for individuals, CNPJ for companies).

Both use mod-11 check digits; a row whose debtor/seller ID fails the checksum is
rejected at ingestion instead of silently flowing into the warehouse.
"""

from __future__ import annotations

import re

_NON_DIGITS = re.compile(r"\D")


def only_digits(value: str) -> str:
    return _NON_DIGITS.sub("", value or "")


def _check_digit(digits: list[int], weights: list[int]) -> int:
    remainder = sum(d * w for d, w in zip(digits, weights, strict=True)) % 11
    return 0 if remainder < 2 else 11 - remainder


def is_valid_cpf(value: str) -> bool:
    digits = [int(c) for c in only_digits(value)]
    if len(digits) != 11 or len(set(digits)) == 1:
        return False
    d1 = _check_digit(digits[:9], list(range(10, 1, -1)))
    d2 = _check_digit(digits[:10], list(range(11, 1, -1)))
    return digits[9] == d1 and digits[10] == d2


_CNPJ_W1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
_CNPJ_W2 = [6, *_CNPJ_W1]


def is_valid_cnpj(value: str) -> bool:
    digits = [int(c) for c in only_digits(value)]
    if len(digits) != 14 or len(set(digits)) == 1:
        return False
    d1 = _check_digit(digits[:12], _CNPJ_W1)
    d2 = _check_digit(digits[:13], _CNPJ_W2)
    return digits[12] == d1 and digits[13] == d2


def is_valid_document(value: str) -> bool:
    """True if `value` is a valid CPF (11 digits) or CNPJ (14 digits)."""
    n = len(only_digits(value))
    if n == 11:
        return is_valid_cpf(value)
    if n == 14:
        return is_valid_cnpj(value)
    return False
