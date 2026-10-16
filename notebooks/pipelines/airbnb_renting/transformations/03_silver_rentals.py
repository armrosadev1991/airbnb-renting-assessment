# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Kamernet rentals (Amsterdam)
# MAGIC
# MAGIC Same valid/quarantine pattern as Airbnb. The `city = amsterdam` filter is **scope**, not
# MAGIC quality: other cities never reach quarantine, so conservation is measured on Amsterdam rows.

# COMMAND ----------

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from airbnb_renting.config import require_conf
from airbnb_renting.quality import (
    RENTALS_RULES,
    RENTALS_WARN_RULES,
    all_rules_pass,
    failed_rules,
    null_safe,
)
from airbnb_renting.transforms import bronze_rentals_to_parsed

bronze = require_conf("bronze_schema", spark.conf.get("bronze_schema"))
silver = require_conf("silver_schema", spark.conf.get("silver_schema"))

# COMMAND ----------


@dp.temporary_view(name="rentals_parsed")
def rentals_parsed() -> DataFrame:
    return bronze_rentals_to_parsed(
        spark.readStream.table(f"{bronze}.rentals_raw"),
        postcodes=spark.read.table(f"{bronze}.postcodes_geo"),
    )


# COMMAND ----------


@dp.table(name=f"{silver}.rentals_listings", comment="Clean Amsterdam Kamernet listings.")
@dp.expect_all_or_drop(null_safe(RENTALS_RULES))
@dp.expect_all(RENTALS_WARN_RULES)
def rentals_listings() -> DataFrame:
    return spark.readStream.table("rentals_parsed")


# COMMAND ----------


@dp.table(
    name=f"{silver}.rentals_quarantine",
    comment="Amsterdam rentals failing RENTALS_RULES; `failed_rules` lists the reasons.",
)
def rentals_quarantine() -> DataFrame:
    return (
        spark.readStream.table("rentals_parsed")
        .where(~all_rules_pass(RENTALS_RULES))
        .withColumn("failed_rules", failed_rules(RENTALS_RULES))
    )
