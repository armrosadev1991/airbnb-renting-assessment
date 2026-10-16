"""Tests for raw-string parsing. Every input below is a shape seen in the real files."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from airbnb_renting.parsing import (
    normalise_null,
    parse_area_sqm,
    parse_decimal,
    parse_eur,
    parse_pc4,
    parse_pc6,
    utilities_included,
)

if TYPE_CHECKING:  # fixtures are injected by pytest; only the type is imported
    from tests.conftest import Evaluate


def test_normalise_null(evaluate: Evaluate) -> None:
    """Kamernet spells 'missing' five ways, some wrapped in newlines; all must become NULL."""
    inputs = [
        "NA",
        "-",
        "",
        "   ",
        "\n                            -\n                    ",
        None,
        " 42 ",
    ]
    assert evaluate(normalise_null, inputs) == [None, None, None, None, None, None, "42"]


def test_parse_pc4(evaluate: Evaluate) -> None:
    """Protects AT-001..005: a wrong PC4 silently moves a listing to another postcode."""
    cases = {
        "1053": 1053,
        "1018  DW": 1018,
        "1079 HH Amsterdam": 1079,
        "Nederland 1091 TS": 1091,
        "`1055tk": 1055,
        "342HUIS": None,  # only 3 digits before letters: not a postcode
        "12345": None,  # 5 digits: lookarounds stop it matching as 1234
        "0123": None,  # NL postcodes never start with 0
        "....": None,
        "b": None,
        "0": None,
        " ": None,
        None: None,
    }
    assert evaluate(parse_pc4, list(cases)) == list(cases.values())


def test_parse_pc6(evaluate: Evaluate) -> None:
    """Full postcodes are normalised (upper case, no spaces) or NULL if not a valid PC6."""
    inputs = ["1024SJ", "1018  dw", "1053", "NA", None]
    assert evaluate(parse_pc6, inputs) == ["1024SJ", "1018DW", None, None, None]


def test_parse_eur(evaluate: Evaluate) -> None:
    """Protects AT-006/008: rent and costs strings to numbers; unparseable becomes NULL, not 0."""
    inputs = [
        "€ 600,-  Utilities incl.",
        "€ 1250,-",
        "€ 1,-",
        "€ 1.250,-",  # defensive: thousands separator never seen, but must not become 1.25
        "\n                            € 50\n                    ",
        "€ 0",
        "NA",
        "-",
        None,
    ]
    expected = [
        Decimal(600),
        Decimal(1250),
        Decimal(1),
        Decimal(1250),
        Decimal(50),
        Decimal(0),
        None,
        None,
        None,
    ]
    assert evaluate(parse_eur, inputs) == expected


def test_utilities_included(evaluate: Evaluate) -> None:
    """The flag is never NULL: a missing marker means 'not included'."""
    inputs = ["€ 600,-  Utilities incl.", "€ 1250,-", None]
    assert evaluate(utilities_included, inputs) == [True, False, False]


def test_parse_area_sqm(evaluate: Evaluate) -> None:
    """Protects AT-007."""
    assert evaluate(parse_area_sqm, ["14 m2", "280 m2", "NA", None]) == [14, 280, None, None]


def test_parse_decimal(evaluate: Evaluate) -> None:
    """Airbnb prices are plain numbers; ANSI mode must not crash on garbage, it must yield NULL."""
    assert evaluate(parse_decimal, ["130", "59.5", "abc", "", None]) == [
        Decimal(130),
        Decimal("59.5"),
        None,
        None,
        None,
    ]
