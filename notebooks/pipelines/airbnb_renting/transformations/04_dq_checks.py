# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: table-level contracts
# MAGIC
# MAGIC Expectations are row-level: one row can't know it is a duplicate. So uniqueness is computed
# MAGIC as an aggregate first, and the expectation is put on the aggregate. A duplicate key means the
# MAGIC logic is broken (not one bad row), so the update fails loudly instead of quarantining.

# COMMAND ----------

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from airbnb_renting.config import require_conf
from airbnb_renting.transforms import dq_uniqueness

silver = require_conf("silver_schema", spark.conf.get("silver_schema"))

# COMMAND ----------


@dp.materialized_view(
    name=f"{silver}.dq_uniqueness",
    comment="One row per silver table: n_rows, n_distinct_ids, n_duplicates.",
)
@dp.expect_or_fail("unique_listing_id", "n_duplicates = 0")
def dq_uniqueness_check() -> DataFrame:
    return dq_uniqueness(
        {
            "airbnb_listings": spark.read.table(f"{silver}.airbnb_listings"),
            "rentals_listings": spark.read.table(f"{silver}.rentals_listings"),
        }
    )
