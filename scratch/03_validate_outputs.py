# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Validate pipeline outputs
# MAGIC
# MAGIC Checks the DEFINE success criteria against the tables the deployed pipeline produced.
# MAGIC Every check is an `assert`, so this notebook fails loudly if a run regresses.
# MAGIC
# MAGIC Run in the workspace, or locally: `just scratch 03_validate_outputs`
# MAGIC (set `TARGET = "prod"` to validate the prod schemas).

# COMMAND ----------

import re

from pyspark.sql import functions as F

from airbnb_renting.transforms import AIRBNB_SOURCE_COLUMNS, is_amsterdam_city, row_hash

try:
    spark  # noqa: B018  (provided by the Databricks notebook runtime)
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()

TARGET = "dev"
CATALOG = "airbnb_renting"
# `mode: development` prefixes schemas with dev_<user short name>_ (see databricks.yml).
user = spark.sql("SELECT current_user()").first()[0]
prefix = f"dev_{re.sub(r'[^a-z0-9]', '_', user.split('@')[0].lower())}_" if TARGET == "dev" else ""
BRONZE, SILVER, GOLD = (f"{CATALOG}.{prefix}{layer}" for layer in ("bronze", "silver", "gold"))
print(BRONZE, SILVER, GOLD)


def count(table: str) -> int:
    """Row count of a table."""
    return spark.read.table(table).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## SC-02 · Row conservation: every in-scope bronze row lands in exactly one place

# COMMAND ----------

airbnb_bronze = spark.read.table(f"{BRONZE}.airbnb_raw")
n_bronze = airbnb_bronze.count()
n_distinct = airbnb_bronze.select(row_hash(AIRBNB_SOURCE_COLUMNS)).distinct().count()
n_listings, n_quarantine = count(f"{SILVER}.airbnb_listings"), count(f"{SILVER}.airbnb_quarantine")
n_duplicates = n_bronze - n_distinct
print(
    f"Airbnb : bronze {n_bronze} = listings {n_listings} + quarantine {n_quarantine} + exact duplicates {n_duplicates}"
)
assert n_bronze == n_listings + n_quarantine + n_duplicates

rentals_bronze = spark.read.table(f"{BRONZE}.rentals_raw")
n_rentals_all = rentals_bronze.count()
n_rentals_ams = rentals_bronze.where(is_amsterdam_city(F.col("city"))).count()
n_r_listings, n_r_quarantine = (
    count(f"{SILVER}.rentals_listings"),
    count(f"{SILVER}.rentals_quarantine"),
)
print(
    f"Rentals: bronze {n_rentals_all} (Amsterdam scope {n_rentals_ams}) = listings {n_r_listings} + quarantine {n_r_quarantine}"
)
assert n_rentals_ams == n_r_listings + n_r_quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine reasons

# COMMAND ----------

for table in ("airbnb_quarantine", "rentals_quarantine"):
    print(table)
    (
        spark.read.table(f"{SILVER}.{table}")
        .select(F.explode("failed_rules").alias("rule"))
        .groupBy("rule")
        .count()
        .orderBy(F.desc("count"))
        .show()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SC-03 · Airbnb PC4 coverage and the raw-vs-geo story

# COMMAND ----------

airbnb_all = spark.read.table(f"{SILVER}.airbnb_listings").unionByName(
    spark.read.table(f"{SILVER}.airbnb_quarantine").drop("failed_rules")
)
coverage = airbnb_all.selectExpr(
    "count(*) AS rows",
    "count_if(pc4 IS NOT NULL AND pc4_is_amsterdam) AS amsterdam_pc4",
    "count_if(pc4_source = 'raw') AS from_raw",
    "count_if(pc4_source = 'geo_backfill') AS from_geo",
    "count_if(pc4_geo_mismatch) AS raw_geo_mismatch",
).first()
print(coverage)
pct = coverage["amsterdam_pc4"] / coverage["rows"]
print(f"Airbnb rows with an Amsterdam PC4: {pct:.2%}")
assert pct >= 0.99

# COMMAND ----------

# MAGIC %md
# MAGIC ## SC-04 / SC-05 · Silver contracts

# COMMAND ----------

rentals = spark.read.table(f"{SILVER}.rentals_listings")
assert rentals.where("rent_eur IS NULL").count() == 0, "SC-04: every valid rental has a rent"
for table in ("airbnb_listings", "rentals_listings"):
    bad = spark.read.table(f"{SILVER}.{table}").where(
        "NOT pc4_is_amsterdam OR segment NOT IN ('whole_unit', 'room')"
    )
    assert bad.count() == 0, f"SC-05 violated in {table}"
spark.read.table(f"{SILVER}.dq_uniqueness").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectation metrics from the SDP event log

# COMMAND ----------

expectations = spark.sql(
    f"""
    SELECT
      origin.flow_name AS flow,
      e.name AS expectation,
      SUM(e.passed_records) AS passed,
      SUM(e.failed_records) AS failed
    FROM event_log(TABLE({SILVER}.airbnb_listings))
    LATERAL VIEW explode(
      from_json(
        details:flow_progress.data_quality.expectations,
        'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
      )
    ) AS e
    WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
    GROUP BY ALL
    ORDER BY flow, expectation
    """
)
expectations.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SC-06 · Gold sanity

# COMMAND ----------

comparison = spark.read.table(f"{GOLD}.investment_comparison")
dupes = comparison.groupBy("pc4", "segment", "scenario").count().where("count > 1").count()
assert dupes == 0, "SC-06: one row per (pc4, segment, scenario)"

print("Winner per segment and scenario (reliable cells only, i.e. low_sample = false):")
(
    comparison.where("NOT low_sample")
    .groupBy("segment", "scenario")
    .agg(
        F.count("*").alias("pc4s"),
        F.expr("count_if(winner = 'airbnb')").alias("airbnb_wins"),
        F.expr("count_if(winner = 'kamernet')").alias("kamernet_wins"),
        F.percentile_approx("breakeven_occupancy", 0.5).alias("median_breakeven"),
    )
    .orderBy("segment", "scenario")
    .show()
)
comparison.selectExpr(
    "count(DISTINCT pc4) AS pc4s",
    "count_if(low_sample) AS low_sample_rows",
    "count_if(winner IS NULL) AS one_sided_rows",
    "count_if(airbnb_never_wins) AS airbnb_never_wins_rows",
).show()
print("✅ all checks passed")
