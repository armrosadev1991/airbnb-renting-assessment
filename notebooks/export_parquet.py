# Databricks notebook source
# MAGIC %md
# MAGIC # Export gold tables to Parquet
# MAGIC
# MAGIC Job task 2, after the pipeline refresh. Writing files is a side effect outside the
# MAGIC declarative pipeline, so it lives in the job, not in SDP.
# MAGIC
# MAGIC Gold is small (tens of thousands of rows), so each table becomes **one** readable
# MAGIC `<table>.parquet` via pandas instead of Spark's directory of part files and commit markers.
# MAGIC At scale this would switch back to `df.write.parquet(...)`.

# COMMAND ----------

from pathlib import Path

from databricks.sdk.runtime import dbutils, spark

dbutils.widgets.text("catalog", "airbnb_renting")
dbutils.widgets.text("gold_schema", "gold")
catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")

GOLD_TABLES = ["revenue_per_property", "revenue_per_postcode", "investment_comparison"]
export_dir = Path(f"/Volumes/{catalog}/{gold_schema}/exports")

# COMMAND ----------

for table in GOLD_TABLES:
    pdf = spark.read.table(f"{catalog}.{gold_schema}.{table}").toPandas()
    # Spark Connect's toPandas() stores query plan metrics in `pdf.attrs`; pandas.to_parquet
    # serialises attrs into the file metadata as JSON and fails on them. Not needed: drop them.
    pdf.attrs = {}
    target = export_dir / f"{table}.parquet"
    pdf.to_parquet(target, index=False)
    print(f"{table}: {len(pdf):,} rows -> {target}")
