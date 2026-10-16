"""Bronze-to-silver transformations: compose parsing, segments and geo into one parsed view.

These functions take and return DataFrames and never read or write tables, so the same code runs
in the SDP pipeline (streaming) and in unit tests (batch). The pipeline notebooks only wire inputs
to these functions.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from airbnb_renting.geo import assign_geo_pc4, flag_amsterdam, resolve_pc4
from airbnb_renting.parsing import (
    normalise_null,
    parse_area_sqm,
    parse_decimal,
    parse_eur,
    parse_pc4,
    parse_pc6,
    utilities_included,
)
from airbnb_renting.segments import airbnb_segment, kamernet_segment

AIRBNB_SOURCE_COLUMNS = [
    "zipcode",
    "latitude",
    "longitude",
    "room_type",
    "accommodates",
    "bedrooms",
    "price",
    "review_scores_value",
]
LINEAGE_COLUMNS = ["_ingested_at", "_source_file"]
_NULL_MARKER = "∅"  # 'empty set': concat_ws skips NULLs, which would shift positions


def row_hash(columns: list[str]) -> Column:
    """Deterministic id for sources without one. NULLs are marked so ('a', NULL) != (NULL, 'a')."""
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c), F.lit(_NULL_MARKER)) for c in columns]), 256
    )


def _to_double(col: Column) -> Column:
    return normalise_null(col).try_cast("double")


def _to_int(col: Column) -> Column:
    # '1.0' -> 1: bedrooms are written as floats in the CSV; a direct int cast would fail.
    return _to_double(col).try_cast("int")


def bronze_airbnb_to_parsed(bronze: DataFrame, postcodes: DataFrame) -> DataFrame:
    """Typed, segmented Airbnb listings with a resolved PC4, exact duplicates removed.

    Dedup happens AFTER the geo join so it also absorbs a point on a shared polygon border
    (two matches, same listing_id), which would otherwise break listing_id uniqueness.
    """
    typed = bronze.select(
        row_hash(AIRBNB_SOURCE_COLUMNS).alias("listing_id"),
        F.col("zipcode").alias("zipcode_raw"),
        parse_pc4(F.col("zipcode")).alias("raw_pc4"),
        _to_double(F.col("latitude")).alias("latitude"),
        _to_double(F.col("longitude")).alias("longitude"),
        F.col("room_type"),
        airbnb_segment(F.col("room_type")).alias("segment"),
        _to_int(F.col("accommodates")).alias("accommodates"),
        _to_int(F.col("bedrooms")).alias("bedrooms"),
        parse_decimal(F.col("price")).alias("price_per_night_eur"),
        _to_double(F.col("review_scores_value")).alias("review_scores_value"),
        *LINEAGE_COLUMNS,
    )
    located = flag_amsterdam(
        assign_geo_pc4(typed, postcodes), "raw_pc4", postcodes, out_col="raw_is_amsterdam"
    )
    pc4_columns = resolve_pc4(
        F.col("raw_pc4"), F.col("raw_is_amsterdam"), F.col("geo_pc4"), F.col("geo_is_amsterdam")
    )
    return (
        located.withColumns(pc4_columns)
        .drop("raw_is_amsterdam", "geo_is_amsterdam")
        .dropDuplicates(["listing_id"])
    )


def is_amsterdam_city(city: Column) -> Column:
    """Rentals scope: the dataset covers the whole country (one row is spelled 'amsterdam')."""
    return F.coalesce(F.lower(normalise_null(city)) == "amsterdam", F.lit(False))


def bronze_rentals_to_parsed(bronze: DataFrame, postcodes: DataFrame) -> DataFrame:
    """Typed, segmented Amsterdam rentals with PC4 and the Amsterdam flag.

    The city filter is SCOPE, not quality: other cities are out of scope, so they are excluded
    before row conservation is measured and never reach quarantine.
    """
    in_scope = bronze.where(is_amsterdam_city(F.col("city")))
    typed = in_scope.select(
        F.element_at("_id", 1).alias("listing_id"),
        F.col("postalCode").alias("postal_code_raw"),
        parse_pc6(F.col("postalCode")).alias("pc6"),
        parse_pc4(F.col("postalCode")).alias("pc4"),
        F.col("propertyType").alias("property_type"),
        kamernet_segment(F.col("propertyType")).alias("segment"),
        F.col("rent").alias("rent_raw"),
        parse_eur(F.col("rent")).alias("rent_eur"),
        utilities_included(F.col("rent")).alias("utilities_included"),
        parse_eur(F.col("additionalCostsRaw")).alias("additional_costs_eur"),
        parse_eur(F.col("deposit")).alias("deposit_eur"),
        parse_area_sqm(F.col("areaSqm")).alias("area_sqm"),
        normalise_null(F.col("furnish")).alias("furnish"),
        normalise_null(F.col("matchCapacity")).alias("match_capacity"),
        F.try_to_timestamp(F.element_at("crawledAt", 1), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSZ")).alias(
            "crawled_at"
        ),
        *LINEAGE_COLUMNS,
    )
    return flag_amsterdam(typed, "pc4", postcodes, out_col="pc4_is_amsterdam")


def dq_uniqueness(tables: dict[str, DataFrame], key: str = "listing_id") -> DataFrame:
    """One row per table with its duplicate-key count, for a table-level `expect_or_fail`."""
    counts = [
        df.agg(
            F.lit(name).alias("table_name"),
            F.count("*").alias("n_rows"),
            F.countDistinct(key).alias("n_distinct_ids"),
        )
        for name, df in tables.items()
    ]
    union = counts[0]
    for other in counts[1:]:
        union = union.unionByName(other)
    return union.withColumn("n_duplicates", F.col("n_rows") - F.col("n_distinct_ids"))
