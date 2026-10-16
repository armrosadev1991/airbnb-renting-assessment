"""Tests for the revenue model (AT-011..016). Expected values are computed by hand."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import functions as F

from airbnb_renting.config import parse_scenarios
from airbnb_renting.revenue import (
    airbnb_monthly_revenue,
    breakeven_occupancy,
    investment_comparison,
    revenue_per_postcode,
    revenue_per_property,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

SCENARIOS = parse_scenarios(
    '[{"name":"legal_cap_30n","nights_per_year":30,"segments":["whole_unit"]},'
    '{"name":"occ_50","occupancy":0.5}]'
)


def test_airbnb_monthly_revenue(spark: SparkSession) -> None:
    """AT-011: 150 x 30 x 0.5 = 2,250. AT-012: cap = 150 x 30 nights / 12 = 375."""
    df = spark.createDataFrame(
        [(150.0, 0.5, None), (150.0, None, 30)], "price double, occ double, nights int"
    )
    got = [
        r[0]
        for r in df.select(
            airbnb_monthly_revenue(F.col("price"), F.col("occ"), F.col("nights"), 30)
        ).collect()
    ]
    assert got == [2250.0, 375.0]


@pytest.fixture(scope="module")
def per_property(spark: SparkSession) -> DataFrame:
    """Two whole units and one room on Airbnb; one whole unit on Kamernet; all in PC4 1017."""
    airbnb = spark.createDataFrame(
        [
            ("a1", 1017, "whole_unit", 100.0),
            ("a2", 1017, "whole_unit", 200.0),
            ("a3", 1017, "room", 60.0),
        ],
        "listing_id string, pc4 int, segment string, price_per_night_eur double",
    )
    rentals = spark.createDataFrame(
        [("k1", 1017, "whole_unit", 1400.0)],
        "listing_id string, pc4 int, segment string, rent_eur double",
    )
    return revenue_per_property(airbnb, rentals, SCENARIOS, nights_per_month=30)


def test_cap_not_applied_to_rooms(per_property: DataFrame) -> None:
    """A-004: a room gets occ_50 only; a whole unit gets both scenarios; Kamernet gets long_term."""
    rows = {(r["listing_id"], r["scenario"]) for r in per_property.collect()}
    assert rows == {
        ("a1", "legal_cap_30n"),
        ("a1", "occ_50"),
        ("a2", "legal_cap_30n"),
        ("a2", "occ_50"),
        ("a3", "occ_50"),
        ("k1", "long_term"),
    }


def test_revenue_per_postcode_uses_median(per_property: DataFrame) -> None:
    """Whole units at €100 and €200 a night: median price 150, occ_50 revenue median 2,250."""
    row = (
        revenue_per_postcode(per_property)
        .where("source = 'airbnb' AND segment = 'whole_unit' AND scenario = 'occ_50'")
        .first()
    )
    assert row is not None
    assert (
        row["median_price_per_night_eur"],
        row["median_monthly_revenue_eur"],
        row["n_listings"],
    ) == (
        150.0,
        2250.0,
        2,
    )


def test_breakeven_occupancy(spark: SparkSession) -> None:
    """AT-013: 1400 / (150 x 30) = 0.311. AT-014: 5000 / (100 x 30) = 1.667. AT-016: NULL side -> NULL."""
    df = spark.createDataFrame(
        [(1400.0, 150.0), (5000.0, 100.0), (None, 150.0), (1400.0, None)],
        "rent double, price double",
    )
    got = [
        r[0] for r in df.select(breakeven_occupancy(F.col("rent"), F.col("price"), 30)).collect()
    ]
    assert got == [0.311, 1.667, None, None]


def test_investment_comparison(per_property: DataFrame, postcodes: DataFrame) -> None:
    """Winner, breakeven and confidence per PC4 x segment x scenario (AT-015, AT-016)."""
    comparison = investment_comparison(
        revenue_per_postcode(per_property), postcodes, nights_per_month=30, low_sample_threshold=5
    )
    rows = {(r["segment"], r["scenario"]): r for r in comparison.collect()}

    whole_cap = rows[("whole_unit", "legal_cap_30n")]
    assert whole_cap["winner"] == "kamernet"  # 150 x 30 / 12 = 375 < 1,400
    assert whole_cap["breakeven_occupancy"] == 0.311
    assert whole_cap["low_sample"] is True  # 1 Kamernet, 2 Airbnb listings < 5
    assert whole_cap["centroid_lat"] == 52.365

    assert rows[("whole_unit", "occ_50")]["winner"] == "airbnb"  # 2,250 > 1,400

    room = rows[("room", "occ_50")]  # Airbnb only: kept visible, nothing to compare
    assert room["winner"] is None
    assert room["n_kamernet"] == 0
    assert room["breakeven_occupancy"] is None
