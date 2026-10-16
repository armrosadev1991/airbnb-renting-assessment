"""Tests for postcode polygons and the PC4 backfill policy (AT-017..019, AT-026)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from airbnb_renting.geo import assign_geo_pc4, flag_amsterdam, resolve_pc4

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def test_explode_postcodes_keeps_multipolygons(postcodes: DataFrame) -> None:
    """Every polygon must parse, MultiPolygons included: schema inference used to break them."""
    rows = {
        r["pc4"]: r
        for r in postcodes.select(
            "pc4",
            "is_amsterdam",
            F.expr("ST_GeomFromGeoJSON(geometry_json) IS NOT NULL").alias("ok"),
        ).collect()
    }
    assert sorted(rows) == [1017, 1018, 1183]
    assert all(r["ok"] for r in rows.values())
    assert [rows[pc4]["is_amsterdam"] for pc4 in (1017, 1018, 1183)] == [True, True, False]


def test_assign_geo_pc4(spark: SparkSession, postcodes: DataFrame) -> None:
    """Protects the backfill: longitude-first point order and the left join (no point is lost)."""
    points = spark.createDataFrame(
        [
            ("in_1017", 4.885, 52.365),
            ("in_1018", 4.91, 52.365),
            ("in_1183", 4.86, 52.30),
            ("sea", 5.5, 52.0),
        ],
        "id string, longitude double, latitude double",
    )
    got = {r["id"]: r["geo_pc4"] for r in assign_geo_pc4(points, postcodes).collect()}
    assert got == {"in_1017": 1017, "in_1018": 1018, "in_1183": 1183, "sea": None}


def test_flag_amsterdam(spark: SparkSession, postcodes: DataFrame) -> None:
    """The flag is never NULL, so it can be used directly in a quality rule."""
    df = spark.createDataFrame([(1, 1017), (2, 1183), (3, 9999), (4, None)], "id int, pc4 int")
    got = {r["id"]: r["ok"] for r in flag_amsterdam(df, "pc4", postcodes, "ok").collect()}
    assert got == {1: True, 2: False, 3: False, 4: False}


def test_resolve_pc4_policy(spark: SparkSession) -> None:
    """Raw wins when it's a valid Amsterdam PC4 (fuzzing, scratch/01); otherwise geo backfills."""
    cases = spark.createDataFrame(
        [
            ("raw_wins_mismatch", 1017, True, 1018, True),
            ("raw_agrees", 1017, True, 1017, True),
            ("backfill", None, False, 1018, True),
            ("raw_outside_ams_geo_ams", 1111, False, 1017, True),
            ("both_outside_ams", 1183, False, 1183, False),
            ("nothing", None, False, None, None),
        ],
        "case string, raw_pc4 int, raw_is_ams boolean, geo_pc4 int, geo_is_ams boolean",
    )
    cols = resolve_pc4(F.col("raw_pc4"), F.col("raw_is_ams"), F.col("geo_pc4"), F.col("geo_is_ams"))
    got = {
        r["case"]: (r["pc4"], r["pc4_source"], r["pc4_is_amsterdam"], r["pc4_geo_mismatch"])
        for r in cases.select("case", *[c.alias(n) for n, c in cols.items()]).collect()
    }
    assert got == {
        "raw_wins_mismatch": (1017, "raw", True, True),
        "raw_agrees": (1017, "raw", True, False),
        "backfill": (1018, "geo_backfill", True, False),
        "raw_outside_ams_geo_ams": (1017, "geo_backfill", True, True),
        "both_outside_ams": (1183, "geo_backfill", False, False),
        "nothing": (None, None, False, False),
    }
