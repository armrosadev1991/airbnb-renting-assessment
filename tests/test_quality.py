"""Tests for the silver quality rules and the valid/quarantine split."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import functions as F

from airbnb_renting.quality import (
    AIRBNB_RULES,
    AIRBNB_WARN_RULES,
    RENTALS_RULES,
    RENTALS_WARN_RULES,
    all_rules_pass,
    failed_rules,
    null_safe,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

AIRBNB_SCHEMA = (
    "case string, latitude double, longitude double, pc4 int, pc4_is_amsterdam boolean, "
    "price_per_night_eur decimal(10,2), segment string, bedrooms int, review_scores_value double"
)
GOOD = (52.37, 4.89, 1017, True, Decimal(100), "whole_unit", 1, 90.0)


@pytest.fixture(scope="module")
def airbnb_cases(spark: SparkSession) -> DataFrame:
    """One good row and one row per failure mode, named by the rule(s) it should fail."""
    rows = [
        ("good", *GOOD),
        ("no_pc4", 52.37, 4.89, None, None, Decimal(100), "whole_unit", 1, 90.0),
        ("amstelveen", 52.30, 4.86, 1183, False, Decimal(100), "whole_unit", 1, 90.0),
        ("price_null", 52.37, 4.89, 1017, True, None, "whole_unit", 1, 90.0),
        ("price_9000", 52.37, 4.89, 1017, True, Decimal(9000), "whole_unit", 1, 90.0),
        ("hotel_room", 52.37, 4.89, 1017, True, Decimal(100), None, 1, 90.0),
        ("two_failures", None, None, None, None, Decimal(9000), "room", 1, 90.0),
    ]
    return spark.createDataFrame(rows, AIRBNB_SCHEMA)


def test_rule_dicts_are_valid_sql(airbnb_cases: DataFrame, spark: SparkSession) -> None:
    """A typo in a rule string would only surface when the pipeline runs; catch it here."""
    airbnb_cases.select([F.expr(c) for c in null_safe(AIRBNB_RULES | AIRBNB_WARN_RULES).values()])
    rentals = spark.createDataFrame(
        [(1017, True, Decimal(800), "room", 14)],
        "pc4 int, pc4_is_amsterdam boolean, rent_eur decimal(10,2), segment string, area_sqm int",
    )
    rentals.select([F.expr(c) for c in null_safe(RENTALS_RULES | RENTALS_WARN_RULES).values()])


def test_null_never_passes(spark: SparkSession) -> None:
    """Protects against silent row loss: a NULL comparison must count as FAILED, never NULL."""
    df = spark.createDataFrame([(None,)], "price decimal(10,2)")
    row = df.select(all_rules_pass({"in_bounds": "price BETWEEN 10 AND 5000"})).first()
    assert row is not None
    assert row[0] is False


def test_failed_rules_names_each_cause(airbnb_cases: DataFrame) -> None:
    """Quarantined rows must say WHY, with one reason per cause (AT-018, AT-026)."""
    got = {
        row["case"]: row["failed"]
        for row in airbnb_cases.select("case", failed_rules(AIRBNB_RULES).alias("failed")).collect()
    }
    assert got == {
        "good": [],
        "no_pc4": ["pc4_not_null"],
        "amstelveen": ["pc4_in_amsterdam"],
        "price_null": ["price_parsed"],
        "price_9000": ["price_in_bounds"],
        "hotel_room": ["segment_known"],
        "two_failures": ["coordinates_parsed", "pc4_not_null", "price_in_bounds"],
    }


def test_every_row_lands_in_exactly_one_table(airbnb_cases: DataFrame) -> None:
    """The partition property behind row conservation (SC-02): valid XOR quarantined."""
    split = airbnb_cases.select(
        all_rules_pass(AIRBNB_RULES).alias("valid"),
        (F.size(failed_rules(AIRBNB_RULES)) > 0).alias("quarantined"),
    ).collect()
    assert all(row["valid"] != row["quarantined"] for row in split)
