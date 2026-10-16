# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: revenue per property, per postcode, and the comparison
# MAGIC
# MAGIC Materialized views (batch reads of silver): aggregates must be recomputed when inputs change,
# MAGIC which append-only streaming tables can't do. Scenarios and thresholds come from the pipeline
# MAGIC configuration, so changing an assumption is a config change, not a code change.

# COMMAND ----------

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from airbnb_renting.config import parse_scenarios, require_conf
from airbnb_renting.revenue import investment_comparison, revenue_per_postcode, revenue_per_property

bronze = require_conf("bronze_schema", spark.conf.get("bronze_schema"))
silver = require_conf("silver_schema", spark.conf.get("silver_schema"))
gold = require_conf("gold_schema", spark.conf.get("gold_schema"))
scenarios = parse_scenarios(require_conf("scenarios", spark.conf.get("scenarios")))
nights_per_month = int(require_conf("nights_per_month", spark.conf.get("nights_per_month")))
low_sample_threshold = int(
    require_conf("low_sample_threshold", spark.conf.get("low_sample_threshold"))
)

# COMMAND ----------


@dp.materialized_view(
    name=f"{gold}.revenue_per_property",
    comment="Gross monthly revenue per listing: Airbnb per scenario, Kamernet long-term rent.",
)
def gold_revenue_per_property() -> DataFrame:
    return revenue_per_property(
        spark.read.table(f"{silver}.airbnb_listings"),
        spark.read.table(f"{silver}.rentals_listings"),
        scenarios,
        nights_per_month,
    )


# COMMAND ----------


@dp.materialized_view(
    name=f"{gold}.revenue_per_postcode",
    comment="Median/avg monthly revenue per source, PC4, segment and scenario.",
)
def gold_revenue_per_postcode() -> DataFrame:
    return revenue_per_postcode(spark.read.table(f"{gold}.revenue_per_property"))


# COMMAND ----------


@dp.materialized_view(
    name=f"{gold}.investment_comparison",
    comment="Kamernet vs Airbnb per PC4 x segment x scenario: winner, breakeven occupancy, confidence.",
)
def gold_investment_comparison() -> DataFrame:
    return investment_comparison(
        spark.read.table(f"{gold}.revenue_per_postcode"),
        spark.read.table(f"{bronze}.postcodes_geo"),
        nights_per_month,
        low_sample_threshold,
    )
