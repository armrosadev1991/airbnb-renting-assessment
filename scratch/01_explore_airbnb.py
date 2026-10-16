# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Explore Airbnb listings
# MAGIC
# MAGIC Goal: understand `airbnb.csv` well enough to freeze the silver rules **before** writing them.
# MAGIC Each section ends with the decision it supports. Results are summarised at the bottom.
# MAGIC
# MAGIC Run in the workspace, or locally: `uv run --env-file .env python scratch/01_explore_airbnb.py`.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    spark  # noqa: B018  (provided by the Databricks notebook runtime)
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()

LANDING = "/Volumes/airbnb_renting/landing/raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Load as bronze will: every column a string
# MAGIC Reading with inferred types would turn a bad price into a NULL *at read time*, hiding the
# MAGIC problem. Strings keep the raw evidence; silver does the typing.

# COMMAND ----------

airbnb: DataFrame = spark.read.option("header", True).csv(f"{LANDING}/airbnb/")
print("rows:", airbnb.count())
airbnb.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Zipcode shapes → `parse_pc4` rule
# MAGIC Classify every raw value and show what the planned regex extracts. NL postcodes never start
# MAGIC with 0, and the lookarounds stop `12345` from yielding `1234`.

# COMMAND ----------

PC4_PATTERN = r"(?<![0-9])([1-9][0-9]{3})(?![0-9])"

zip_shapes = airbnb.select(
    "zipcode",
    F.when(F.trim("zipcode").isNull() | (F.trim("zipcode") == ""), "empty")
    .when(F.trim("zipcode").rlike(r"^[1-9][0-9]{3}$"), "pc4")
    .when(F.upper(F.trim("zipcode")).rlike(r"^[1-9][0-9]{3}\s*[A-Z]{2}$"), "pc6")
    .otherwise("messy")
    .alias("shape"),
    F.regexp_extract("zipcode", PC4_PATTERN, 1).alias("parsed_pc4"),
)
zip_shapes.groupBy("shape").count().orderBy(F.desc("count")).show()
print("Every 'messy' value and what the regex makes of it:")
zip_shapes.where("shape = 'messy'").groupBy("zipcode", "parsed_pc4").count().orderBy(
    F.desc("count")
).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Duplicates → Decision 8
# MAGIC Exact duplicates (all 8 columns equal) vs same coordinates only.

# COMMAND ----------

n_rows = airbnb.count()
n_exact_distinct = airbnb.distinct().count()
n_coord_distinct = airbnb.select("latitude", "longitude").distinct().count()
print(f"exact duplicate rows      : {n_rows - n_exact_distinct}")
print(f"same lat/long (any fields): {n_rows - n_coord_distinct}")

print("Same coordinates but different attributes (NOT exact duplicates), a sample:")
(
    airbnb.groupBy("latitude", "longitude")
    .agg(F.count("*").alias("n"), F.countDistinct("price", "room_type").alias("variants"))
    .where("n > 1 AND variants > 1")
    .orderBy(F.desc("n"))
    .show(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Price distribution → bounds (A-005)

# COMMAND ----------

price = airbnb.select(F.col("price").cast("double").alias("price"), "room_type")
print("unparseable prices:", price.where("price IS NULL").count())
price.select(
    F.percentile_approx("price", [0.0, 0.01, 0.5, 0.99, 0.999, 1.0]).alias("p0_p1_p50_p99_p999_max")
).show(truncate=False)
price.selectExpr(
    "count_if(price < 10) AS lt_10",
    "count_if(price > 1000) AS gt_1000",
    "count_if(price > 2000) AS gt_2000",
    "count_if(price > 5000) AS gt_5000",
).show()
price.groupBy("room_type").agg(
    F.count("*").alias("n"),
    F.percentile_approx("price", 0.5).alias("median"),
    F.max("price").alias("max"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Other columns: room type, bedrooms, reviews, coordinates

# COMMAND ----------

airbnb.groupBy("room_type").count().orderBy(F.desc("count")).show()
airbnb.selectExpr(
    "count_if(bedrooms IS NULL) AS null_bedrooms",
    "count_if(review_scores_value IS NULL) AS null_review",
    "count_if(accommodates IS NULL OR accommodates = '0') AS bad_accommodates",
    "min(CAST(latitude AS DOUBLE)) AS min_lat",
    "max(CAST(latitude AS DOUBLE)) AS max_lat",
    "min(CAST(longitude AS DOUBLE)) AS min_lon",
    "max(CAST(longitude AS DOUBLE)) AS max_lon",
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Point-in-polygon backfill → Decisions 9 and 10
# MAGIC GeoJSON mixes `Polygon` (3-deep arrays) and `MultiPolygon` (4-deep). Spark's schema
# MAGIC inference can't reconcile them and degrades coordinates to strings, so we declare
# MAGIC `geometry` as **STRING**: Spark keeps the raw JSON text, which `ST_GeomFromGeoJSON` parses.

# COMMAND ----------

GEO_SCHEMA = (
    "features ARRAY<STRUCT<properties: STRUCT<pc4_code: STRING, gem_name: STRING>, "
    "geometry: STRING>>"
)
postcodes = (
    spark.read.schema(GEO_SCHEMA)
    .option("multiLine", True)
    .json(f"{LANDING}/geo/post_codes.geojson")
    .select(F.explode("features").alias("f"))
    .select(
        F.col("f.properties.pc4_code").cast("int").alias("geo_pc4"),
        F.col("f.properties.gem_name").alias("gem_name"),
        F.expr("ST_GeomFromGeoJSON(f.geometry)").alias("geom"),
    )
)

points = airbnb.select(
    "zipcode",
    # Spark 4 = ANSI mode: CAST('' AS INT) raises, so no-match ("") needs try_cast -> NULL.
    F.regexp_extract("zipcode", PC4_PATTERN, 1).try_cast("int").alias("raw_pc4"),
    F.expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE), 4326)").alias("pt"),
    F.monotonically_increasing_id().alias("row_id"),
)
geo = points.join(F.broadcast(postcodes), F.expr("ST_Contains(geom, pt)"), "left").drop("geom")

amsterdam = postcodes.where("gem_name = 'Amsterdam'").select(F.col("geo_pc4").alias("ams_pc4"))
resolved = geo.join(F.broadcast(amsterdam), geo.raw_pc4 == amsterdam.ams_pc4, "left").select(
    "*",
    F.col("ams_pc4").isNotNull().alias("raw_is_amsterdam"),
)

print("Points matching >1 polygon (border cases):")
resolved.groupBy("row_id").count().where("count > 1").groupBy().count().show()

resolved.selectExpr(
    "count(*) AS rows",
    "count_if(geo_pc4 IS NOT NULL) AS geo_found",
    "count_if(gem_name = 'Amsterdam') AS geo_in_amsterdam",
    "count_if(raw_pc4 IS NULL) AS raw_missing_or_bad",
    "count_if(raw_pc4 IS NULL AND gem_name = 'Amsterdam') AS backfilled_into_amsterdam",
    "count_if(raw_is_amsterdam) AS raw_valid_amsterdam",
    "count_if(raw_is_amsterdam AND geo_pc4 IS NOT NULL AND raw_pc4 <> geo_pc4) AS raw_geo_mismatch",
    "count_if(NOT raw_is_amsterdam AND raw_pc4 IS NOT NULL) AS raw_not_amsterdam",
).show(truncate=False)

print("Where do non-Amsterdam points land?")
resolved.where("gem_name <> 'Amsterdam' OR gem_name IS NULL").groupBy("gem_name").count().orderBy(
    F.desc("count")
).show()

print("Raw PC4s that are not Amsterdam postcodes:")
resolved.where("NOT raw_is_amsterdam AND raw_pc4 IS NOT NULL").groupBy(
    "zipcode", "raw_pc4", "geo_pc4", "gem_name"
).count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Why do raw zip and geo PC4 disagree? → AT-019 policy
# MAGIC 21% of valid raw PC4s differ from the polygon the point falls in. Two hypotheses:
# MAGIC - **A · location fuzzing:** Airbnb shifts public coordinates by up to ~150 m, so points
# MAGIC   land *just across* a border → distance to the raw PC4's polygon is small → trust raw.
# MAGIC - **B · wrong zips:** hosts typed a wrong postcode → points are *far* from it → trust geo.
# MAGIC
# MAGIC `ST_Distance` on lon/lat geometries returns degrees, so we transform to the Dutch
# MAGIC national grid (EPSG:28992, metres) first.

# COMMAND ----------

raw_polys = postcodes.select(
    F.col("geo_pc4").alias("raw_poly_pc4"), F.col("geom").alias("raw_geom")
)
mismatches = (
    resolved.where("raw_is_amsterdam AND geo_pc4 IS NOT NULL AND raw_pc4 <> geo_pc4")
    .join(F.broadcast(raw_polys), F.col("raw_pc4") == F.col("raw_poly_pc4"))
    .select(
        "row_id",
        "raw_pc4",
        "geo_pc4",
        F.expr("ST_Distance(ST_Transform(pt, 28992), ST_Transform(raw_geom, 28992))").alias(
            "metres_to_raw_pc4"
        ),
    )
)
mismatches.select(
    F.count("*").alias("n"),
    F.percentile_approx("metres_to_raw_pc4", [0.5, 0.75, 0.9, 0.95, 0.99]).alias(
        "p50_p75_p90_p95_p99_m"
    ),
    F.expr("count_if(metres_to_raw_pc4 <= 200)").alias("within_200m"),
    F.expr("count_if(metres_to_raw_pc4 > 1000)").alias("over_1km"),
).show(truncate=False)

print("Final PC4 under the policy 'raw if valid Amsterdam PC4, else geo':")
resolved.select(
    F.when(F.col("raw_is_amsterdam"), F.col("raw_pc4")).otherwise(F.col("geo_pc4")).alias("pc4"),
    F.when(F.col("raw_is_amsterdam"), "raw").otherwise("geo_backfill").alias("pc4_source"),
    "gem_name",
    "raw_is_amsterdam",
).selectExpr(
    "count_if(pc4_source = 'raw') AS from_raw",
    "count_if(pc4_source = 'geo_backfill' AND gem_name = 'Amsterdam') AS from_geo_amsterdam",
    "count_if(pc4_source = 'geo_backfill' AND (gem_name <> 'Amsterdam' OR gem_name IS NULL)) "
    "AS not_amsterdam_or_no_polygon",
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Near-duplicates (not exact)
# MAGIC Same coordinates + room type + guests + bedrooms, price within 5% (e.g. €2,772 / €2,771):
# MAGIC most likely the same listing scraped twice after a price change.

# COMMAND ----------

listing_key = ["latitude", "longitude", "room_type", "accommodates", "bedrooms"]
(
    airbnb.distinct()
    .groupBy(*listing_key)
    .agg(
        F.count("*").alias("n"),
        F.min(F.col("price").cast("double")).alias("lo"),
        F.max(F.col("price").cast("double")).alias("hi"),
    )
    .where("n > 1 AND (hi - lo) / hi <= 0.05")
    .selectExpr("count(*) AS groups", "sum(n - 1) AS extra_rows")
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results and decisions (run 2026-10-16)
# MAGIC
# MAGIC | Finding | Number | Decision |
# MAGIC |---|---|---|
# MAGIC | Rows | 9,913 | |
# MAGIC | Zip shapes | PC6 4,531 · PC4 3,119 · empty 2,254 · messy 9 | `parse_pc4` regex handles every messy value seen (`1079 HH Amsterdam`→1079, `` `1055tk``→1055, `342HUIS`/`b`/`0`/`....`→NULL) |
# MAGIC | Exact duplicates | 382 | Dedup on row hash (Decision 8) |
# MAGIC | Near-duplicates | 130 extra rows | **Keep**, count and document: can't prove identity (apart-hotels have identical units), ~1.4% of rows, negligible effect on medians |
# MAGIC | Price | min €15 · median €125 · p99 €1,260 · p99.9 €2,556 · max €9,000; 0 unparseable | Bounds **€10–€5,000**: quarantines 3 rows (€9,000 for 2 guests/no reviews, €8,131, €7,783). €2k–€4k rows are 3–4-bedroom, 95–100-rated homes → plausible luxury, kept |
# MAGIC | Missing bedrooms / review score | 14 / 1,711 | Warn-only expectations |
# MAGIC | Geo match | 9,906 of 9,913 points inside a PC4 polygon; 0 border double-matches | 7 points outside every polygon → quarantine |
# MAGIC | Raw zip missing/unparseable | 2,258 | 2,256 backfilled into an Amsterdam PC4 by point-in-polygon |
# MAGIC | Raw PC4 vs geo PC4 mismatch | 1,616 of 7,629 valid (21%) | **Fuzzing, not wrong zips:** median 66 m, 85% within 200 m of the raw PC4 polygon, 21 over 1 km → **trust raw** (AT-019) |
# MAGIC | Raw PC4 outside Amsterdam | 26 (Diemen, Amstelveen, …) | Policy uses geo for these; those that land outside Amsterdam are quarantined (`pc4_in_amsterdam`) |
# MAGIC | **Final PC4 (before dedup)** | raw 7,629 + geo 2,260 = **9,889 in Amsterdam (99.76%)**; 24 → quarantine | SC-03 (≥ 99%) met |
