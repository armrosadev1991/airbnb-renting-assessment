"""Component tests: bronze-shaped fixtures through the parsed views and the quality split.

Each fixture row is designed to land in a known place, so these tests check row conservation
(SC-02) on a small scale: bronze = valid + quarantine (+ removed duplicates).
"""

from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import functions as F

from airbnb_renting.quality import AIRBNB_RULES, RENTALS_RULES, all_rules_pass, failed_rules
from airbnb_renting.transforms import (
    AIRBNB_SOURCE_COLUMNS,
    bronze_airbnb_to_parsed,
    bronze_rentals_to_parsed,
    dq_uniqueness,
)
from tests.conftest import FIXTURES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

RENTALS_BRONZE_SCHEMA = (
    "_id array<string>, city string, postalCode string, propertyType string, rent string, "
    "areaSqm string, additionalCostsRaw string, deposit string, furnish string, "
    "matchCapacity string, crawledAt array<string>"
)


def _with_lineage(df: DataFrame, source_file: str) -> DataFrame:
    return df.withColumns(
        {"_ingested_at": F.current_timestamp(), "_source_file": F.lit(source_file)}
    )


@pytest.fixture(scope="module")
def airbnb_bronze(spark: SparkSession) -> DataFrame:
    """The CSV fixture as Auto Loader would land it: all strings, empty fields as NULL."""
    with (FIXTURES / "airbnb_sample.csv").open() as f:
        rows = [tuple(v or None for v in r.values()) for r in csv.DictReader(f)]
    schema = ", ".join(f"{c} string" for c in AIRBNB_SOURCE_COLUMNS)
    return _with_lineage(spark.createDataFrame(rows, schema), "airbnb_sample.csv")


@pytest.fixture(scope="module")
def rentals_bronze(spark: SparkSession) -> DataFrame:
    """The JSON fixture with the bronze schema (Mongo-style arrays preserved)."""
    records = json.loads((FIXTURES / "rentals_sample.json").read_text())
    return _with_lineage(
        spark.createDataFrame(records, RENTALS_BRONZE_SCHEMA), "rentals_sample.json"
    )


def _split(parsed: DataFrame, rules: dict[str, str]) -> tuple[list, list]:
    valid = parsed.where(all_rules_pass(rules)).collect()
    quarantine = (
        parsed.where(~all_rules_pass(rules))
        .withColumn("failed_rules", failed_rules(rules))
        .collect()
    )
    return valid, quarantine


def test_airbnb_conservation_and_reasons(airbnb_bronze: DataFrame, postcodes: DataFrame) -> None:
    """Every bronze row ends in exactly one place, with the expected reason (AT-017/018/025/026)."""
    parsed = bronze_airbnb_to_parsed(airbnb_bronze, postcodes)
    valid, quarantine = _split(parsed, AIRBNB_RULES)
    duplicates = airbnb_bronze.count() - parsed.count()

    assert duplicates == 1  # the exact copy of row 1
    assert len(valid) + len(quarantine) + duplicates == airbnb_bronze.count()
    assert sorted(r["failed_rules"][0] for r in quarantine) == [
        "pc4_in_amsterdam",  # '....' backfilled into Amstelveen
        "pc4_in_amsterdam",  # raw 1183 AP: valid zip, but not Amsterdam
        "pc4_not_null",  # point in the sea, no zip
        "price_in_bounds",  # €9,000
        "segment_known",  # 'Hotel room'
    ]
    by_zip = {r["zipcode_raw"]: r for r in valid}
    assert by_zip["1017 AB"]["pc4_source"] == "raw"
    assert by_zip["1017 AB"]["pc4_geo_mismatch"] is True  # point lies in 1018: raw wins
    assert by_zip[None]["pc4"] == 1018
    assert by_zip[None]["pc4_source"] == "geo_backfill"


def test_airbnb_types(airbnb_bronze: DataFrame, postcodes: DataFrame) -> None:
    """'2.0' bedrooms must become 2, not NULL (a direct int cast fails under ANSI mode)."""
    row = (
        bronze_airbnb_to_parsed(airbnb_bronze, postcodes)
        .where("zipcode_raw = '1017 AB'")
        .select("bedrooms", "accommodates", "price_per_night_eur", "segment")
        .first()
    )
    assert row is not None
    assert (row["bedrooms"], row["accommodates"], float(row["price_per_night_eur"])) == (1, 2, 95.0)
    assert row["segment"] == "whole_unit"


def test_rentals_scope_and_reasons(rentals_bronze: DataFrame, postcodes: DataFrame) -> None:
    """Rotterdam is out of scope (not quarantined); lowercase 'amsterdam' IS in scope (AT-020)."""
    parsed = bronze_rentals_to_parsed(rentals_bronze, postcodes)
    valid, quarantine = _split(parsed, RENTALS_RULES)

    assert parsed.count() == 6  # 7 minus Rotterdam
    assert len(valid) + len(quarantine) == 6
    reasons = {r["listing_id"]: r["failed_rules"] for r in quarantine}
    assert reasons == {
        "id3": ["rent_in_bounds"],  # € 1,-
        "id4": ["pc4_in_amsterdam"],  # 'amsterdam' but PC4 1183 (Amstelveen)
        "id5": ["segment_known"],  # Anti-squat
    }


def test_rentals_parsing(rentals_bronze: DataFrame, postcodes: DataFrame) -> None:
    """Money, flags, area and timestamps parse from the real string shapes (AT-006..010)."""
    rows = {
        r["listing_id"]: r for r in bronze_rentals_to_parsed(rentals_bronze, postcodes).collect()
    }
    assert (float(rows["id1"]["rent_eur"]), rows["id1"]["utilities_included"]) == (600.0, True)
    assert rows["id2"]["additional_costs_eur"] is None  # '\n   -\n ' is a NULL spelling
    assert float(rows["id1"]["deposit_eur"]) == 500.0  # newline-wrapped '€ 500'
    assert rows["id1"]["pc6"] == "1017AB"
    assert rows["id7"]["area_sqm"] is None
    assert rows["id1"]["crawled_at"] is not None


def test_dq_uniqueness(spark: SparkSession) -> None:
    """The table-level contract behind `expect_or_fail` (AT-022)."""
    unique = spark.createDataFrame([("a",), ("b",)], "listing_id string")
    duplicated = spark.createDataFrame([("a",), ("a",), ("b",)], "listing_id string")
    got = {
        r["table_name"]: r["n_duplicates"]
        for r in dq_uniqueness({"unique": unique, "duplicated": duplicated}).collect()
    }
    assert got == {"unique": 0, "duplicated": 1}
