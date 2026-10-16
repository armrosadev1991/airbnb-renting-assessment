# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: faithful copies of the landing files
# MAGIC
# MAGIC Everything stays a **string** (Auto Loader's default for CSV/JSON), so a malformed value can
# MAGIC never break ingestion; typing happens in silver, where failures are quarantined with a reason.
# MAGIC Lineage columns `_ingested_at` and `_source_file` travel with every row.

# COMMAND ----------

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from airbnb_renting.config import require_conf
from airbnb_renting.geo import GEOJSON_SCHEMA, explode_postcodes

landing = require_conf("landing_path", spark.conf.get("landing_path"))
bronze = require_conf("bronze_schema", spark.conf.get("bronze_schema"))

# Mongo-style one-element arrays: pinned so they stay arrays, not JSON text in a string column.
RENTALS_SCHEMA_HINTS = (
    "_id ARRAY<STRING>, crawledAt ARRAY<STRING>, detailsCrawledAt ARRAY<STRING>, "
    "firstSeenAt ARRAY<STRING>, lastSeenAt ARRAY<STRING>"
)


def with_lineage(df: DataFrame) -> DataFrame:
    """Add when and from which file each row was ingested."""
    return df.select(
        "*",
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )


# COMMAND ----------


@dp.table(name=f"{bronze}.airbnb_raw", comment="airbnb.csv as uploaded: all columns string.")
def airbnb_raw() -> DataFrame:
    return with_lineage(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{landing}/airbnb/")
    )


# COMMAND ----------


@dp.table(
    name=f"{bronze}.rentals_raw",
    comment="rentals.json as uploaded (all Dutch cities): one JSON array, hence multiLine.",
)
def rentals_raw() -> DataFrame:
    return with_lineage(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.schemaHints", RENTALS_SCHEMA_HINTS)
        .load(f"{landing}/rentals/")
    )


# COMMAND ----------


@dp.materialized_view(
    name=f"{bronze}.postcodes_geo",
    comment="One row per PC4 polygon (474, 82 in Amsterdam). Static reference data: batch read.",
)
def postcodes_geo() -> DataFrame:
    geojson = (
        spark.read.schema(GEOJSON_SCHEMA)
        .option("multiLine", "true")
        .json(f"{landing}/geo/post_codes.geojson")
    )
    return explode_postcodes(geojson)
