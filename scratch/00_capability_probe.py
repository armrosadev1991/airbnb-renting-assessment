# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Capability probe (Free Edition)
# MAGIC
# MAGIC The design rests on three platform assumptions that Databricks docs don't confirm for
# MAGIC **Free Edition**. This notebook checks them before any pipeline code is written, so a
# MAGIC wrong assumption costs minutes, not a redesign.
# MAGIC
# MAGIC | # | Assumption | Result (2026-10-16) |
# MAGIC |---|------------|---------------------|
# MAGIC | A-003 | We can create our own catalog | ✅ via **SQL** `CREATE CATALOG` (picks up Default Storage). ❌ via REST `catalogs create`: *"Metastore storage root URL does not exist"* |
# MAGIC | A-002 | `ST_*` spatial functions work on serverless | ✅ SQL warehouse (DBSQL 2026.39) and serverless compute (DBR 19.9) |
# MAGIC | Decision 12 | `databricks-connect` version for tests | Serverless runs **Spark 4.2 / DBR 19.9**; pinned `databricks-connect==19.2.*` (newest client) |
# MAGIC
# MAGIC Runs in the workspace (where `spark` exists) or locally: `uv run --env-file .env python scratch/00_capability_probe.py`
# MAGIC (`--env-file` supplies `DATABRICKS_CONFIG_PROFILE`; `just` loads `.env` automatically, `uv run` does not).

# COMMAND ----------

try:
    spark  # noqa: B018  (provided by the Databricks notebook runtime)
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Runtime version
# MAGIC `databricks-connect` should track the serverless runtime: an older client can miss newer
# MAGIC server features or behave subtly differently.

# COMMAND ----------

print("Spark:", spark.version)
print(spark.sql("SELECT current_version().dbr_version AS dbr").first()["dbr"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Spatial SQL (point-in-polygon backfill, Decision 9)
# MAGIC A square around central Amsterdam: one point inside, one outside. GeoJSON is CRS84
# MAGIC (**longitude first**), so `ST_Point(lon, lat, 4326)`; swapping them would silently put
# MAGIC every listing in the sea off Somalia.

# COMMAND ----------

square = (
    '{"type":"Polygon","coordinates":[[[4.88,52.37],[4.88,52.38],'
    "[4.89,52.38],[4.89,52.37],[4.88,52.37]]]}"
)
result = spark.sql(
    f"""
    SELECT
      ST_Contains(ST_GeomFromGeoJSON('{square}'), ST_Point(4.885, 52.375, 4326)) AS inside,
      ST_Contains(ST_GeomFromGeoJSON('{square}'), ST_Point(5.000, 52.000, 4326)) AS outside
    """
).first()
assert result["inside"] and not result["outside"], result
print("ST_* functions work:", result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Landing zone
# MAGIC Created by `just bootstrap`. Listing it confirms the catalog/volume exist and shows
# MAGIC whether the raw files have been uploaded yet.

# COMMAND ----------

for row in spark.sql("LIST '/Volumes/airbnb_renting/landing/raw/'").collect():
    print(row["path"])
