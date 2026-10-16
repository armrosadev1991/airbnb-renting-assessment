# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Airbnb listings
# MAGIC
# MAGIC `airbnb_parsed` (temporary view) types and dedups the rows and resolves PC4 (valid raw zip
# MAGIC first, point-in-polygon second). Both tables read it and share ONE rules dict:
# MAGIC - `airbnb_listings` keeps rows passing every rule (`expect_all_or_drop`),
# MAGIC - `airbnb_quarantine` keeps the rest, with `failed_rules` saying why.
# MAGIC
# MAGIC Nothing is dropped silently: bronze = listings + quarantine + exact duplicates.

# COMMAND ----------

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from airbnb_renting.config import require_conf
from airbnb_renting.quality import (
    AIRBNB_RULES,
    AIRBNB_WARN_RULES,
    all_rules_pass,
    failed_rules,
    null_safe,
)
from airbnb_renting.transforms import bronze_airbnb_to_parsed

bronze = require_conf("bronze_schema", spark.conf.get("bronze_schema"))
silver = require_conf("silver_schema", spark.conf.get("silver_schema"))

# COMMAND ----------


@dp.temporary_view(name="airbnb_parsed")
def airbnb_parsed() -> DataFrame:
    return bronze_airbnb_to_parsed(
        spark.readStream.table(f"{bronze}.airbnb_raw"),
        postcodes=spark.read.table(f"{bronze}.postcodes_geo"),  # static side of the join
    )


# COMMAND ----------


@dp.table(name=f"{silver}.airbnb_listings", comment="Clean Airbnb listings with a resolved PC4.")
@dp.expect_all_or_drop(null_safe(AIRBNB_RULES))
@dp.expect_all(AIRBNB_WARN_RULES)
def airbnb_listings() -> DataFrame:
    return spark.readStream.table("airbnb_parsed")


# COMMAND ----------


@dp.table(
    name=f"{silver}.airbnb_quarantine",
    comment="Airbnb rows failing AIRBNB_RULES; `failed_rules` lists the reasons.",
)
def airbnb_quarantine() -> DataFrame:
    return (
        spark.readStream.table("airbnb_parsed")
        .where(~all_rules_pass(AIRBNB_RULES))
        .withColumn("failed_rules", failed_rules(AIRBNB_RULES))
    )
