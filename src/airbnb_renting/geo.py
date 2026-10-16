"""Postcode polygons and the PC4 backfill (DESIGN Decisions 9 and 10).

Policy, validated in `scratch/01`: a raw zip that is a valid Amsterdam PC4 wins (raw/geo
mismatches are Airbnb's ~150 m location fuzzing: median 66 m); otherwise the PC4 polygon that
contains the point is used. "Amsterdam" is whatever the reference data says it is.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# The GeoJSON mixes Polygon and MultiPolygon. Schema inference can't reconcile their nesting
# depths and degrades coordinates to strings; declaring `geometry` as STRING keeps the raw JSON,
# which ST_GeomFromGeoJSON parses.
GEOJSON_SCHEMA = (
    "features ARRAY<STRUCT<"
    "properties: STRUCT<pc4_code: STRING, gem_name: STRING, "
    "geo_point_2d: STRUCT<lon: DOUBLE, lat: DOUBLE>>, "
    "geometry: STRING>>"
)
AMSTERDAM = "Amsterdam"


def explode_postcodes(geojson: DataFrame) -> DataFrame:
    """One row per PC4 polygon from a DataFrame read with GEOJSON_SCHEMA."""
    return geojson.select(F.explode("features").alias("f")).select(
        F.col("f.properties.pc4_code").try_cast("int").alias("pc4"),
        F.col("f.properties.gem_name").alias("municipality"),
        (F.col("f.properties.gem_name") == AMSTERDAM).alias("is_amsterdam"),
        F.col("f.geometry").alias("geometry_json"),
        F.col("f.properties.geo_point_2d.lat").alias("centroid_lat"),
        F.col("f.properties.geo_point_2d.lon").alias("centroid_lon"),
    )


def flag_amsterdam(df: DataFrame, pc4_col: str, postcodes: DataFrame, out_col: str) -> DataFrame:
    """Add a non-NULL boolean: is `pc4_col` one of the reference data's Amsterdam PC4s."""
    lookup = F.broadcast(
        postcodes.where("is_amsterdam").select(F.col("pc4").alias("_ams_pc4")).distinct()
    )
    return (
        df.join(lookup, F.col(pc4_col) == F.col("_ams_pc4"), "left")
        .withColumn(out_col, F.col("_ams_pc4").isNotNull())
        .drop("_ams_pc4")
    )


def assign_geo_pc4(points: DataFrame, postcodes: DataFrame) -> DataFrame:
    """Add `geo_pc4` / `geo_is_amsterdam`: the polygon containing (longitude, latitude).

    A broadcast non-equi join: 474 polygons x ~10k points is trivial, and the polygon side is
    static, so it also works as a stream-static join inside the pipeline. CRS84 means
    ST_Point takes LONGITUDE first.
    """
    polygons = F.broadcast(
        postcodes.select(
            F.col("pc4").alias("geo_pc4"),
            F.col("is_amsterdam").alias("geo_is_amsterdam"),
            F.expr("ST_GeomFromGeoJSON(geometry_json)").alias("_geom"),
        )
    )
    return (
        points.withColumn("_point", F.expr("ST_Point(longitude, latitude, 4326)"))
        .join(polygons, F.expr("ST_Contains(_geom, _point)"), "left")
        .drop("_geom", "_point")
    )


def resolve_pc4(
    raw_pc4: Column, raw_is_amsterdam: Column, geo_pc4: Column, geo_is_amsterdam: Column
) -> dict[str, Column]:
    """Final PC4 columns under the 'valid raw zip first, geo second' policy."""
    return {
        "pc4": F.when(raw_is_amsterdam, raw_pc4).otherwise(geo_pc4),
        "pc4_source": F.when(raw_is_amsterdam, F.lit("raw")).when(
            geo_pc4.isNotNull(), F.lit("geo_backfill")
        ),
        "pc4_is_amsterdam": raw_is_amsterdam | F.coalesce(geo_is_amsterdam, F.lit(False)),
        # Measured, not used for routing: how often the listed zip disagrees with the map.
        "pc4_geo_mismatch": F.coalesce(raw_pc4 != geo_pc4, F.lit(False)),
    }
