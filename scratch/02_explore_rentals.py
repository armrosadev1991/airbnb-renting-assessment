# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Explore Kamernet rentals
# MAGIC
# MAGIC Goal: freeze the parsing rules, the Amsterdam scope filter, the rent bounds and the
# MAGIC low-sample threshold before writing silver. Results are summarised at the bottom.
# MAGIC
# MAGIC Run in the workspace, or locally: `uv run --env-file .env python scratch/02_explore_rentals.py`.

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
# MAGIC ## 1 · Load: one 69 MB JSON array → `multiLine`
# MAGIC Without `multiLine`, Spark expects one JSON object per line (JSON Lines) and would put the
# MAGIC whole array into `_corrupt_record`.

# COMMAND ----------

rentals: DataFrame = spark.read.option("multiLine", True).json(f"{LANDING}/rentals/")
print("rows:", rentals.count())
rentals.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Mongo-style arrays and the primary key

# COMMAND ----------

rentals.selectExpr(
    "count_if(size(_id) <> 1) AS id_not_single",
    "count(DISTINCT element_at(_id, 1)) AS distinct_ids",
    "count(*) AS rows",
    "min(element_at(crawledAt, 1)) AS first_crawl",
    "max(element_at(crawledAt, 1)) AS last_crawl",
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Scope: is `city = 'Amsterdam'` enough? (Decision 10)
# MAGIC Compare the free-text city with the authoritative PC4 polygons, in both directions.

# COMMAND ----------

GEO_SCHEMA = "features ARRAY<STRUCT<properties: STRUCT<pc4_code: STRING, gem_name: STRING>>>"
amsterdam_pc4 = (
    spark.read.schema(GEO_SCHEMA)
    .option("multiLine", True)
    .json(f"{LANDING}/geo/post_codes.geojson")
    .select(F.explode("features").alias("f"))
    .where("f.properties.gem_name = 'Amsterdam'")
    .select(F.col("f.properties.pc4_code").cast("int").alias("ams_pc4"))
)

with_pc4 = rentals.select(
    "*",
    F.upper(F.regexp_replace("postalCode", r"\s", "")).alias("pc6"),
    F.regexp_extract("postalCode", r"^\s*([1-9][0-9]{3})", 1).try_cast("int").alias("pc4"),
).join(F.broadcast(amsterdam_pc4), F.col("pc4") == F.col("ams_pc4"), "left")

print("City spellings that mention Amsterdam:")
with_pc4.where(F.lower("city").contains("amsterdam")).groupBy("city").count().show(truncate=False)

with_pc4.selectExpr(
    "count_if(city = 'Amsterdam') AS city_amsterdam",
    "count_if(city = 'Amsterdam' AND ams_pc4 IS NULL) AS city_ams_but_pc4_not",
    "count_if(city <> 'Amsterdam' AND ams_pc4 IS NOT NULL) AS pc4_ams_but_city_not",
    "count_if(postalCode IS NULL OR trim(postalCode) = '') AS missing_postcode",
).show()

print("Cities whose postcode is in Amsterdam:")
with_pc4.where("city <> 'Amsterdam' AND ams_pc4 IS NOT NULL").groupBy("city").count().show()

ams = with_pc4.where("city = 'Amsterdam'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Rent strings → `parse_rent`
# MAGIC Replace digits with `9` to see the *shapes* rather than thousands of distinct values.

# COMMAND ----------

ams.groupBy(F.regexp_replace(F.trim("rent"), r"[0-9]", "9").alias("rent_shape")).count().orderBy(
    F.desc("count")
).show(truncate=False)

rent = ams.select(
    F.regexp_extract("rent", r"€\s*([0-9.]+)", 1).alias("amount_txt"),
    F.lower("rent").contains("utilities incl").alias("utilities_included"),
).select(
    F.regexp_replace("amount_txt", r"\.", "").try_cast("decimal(10,2)").alias("rent_eur"),
    "utilities_included",
)
rent.select(
    F.expr("count_if(rent_eur IS NULL)").alias("unparsed"),
    F.percentile_approx("rent_eur", [0.0, 0.001, 0.01, 0.5, 0.99, 1.0]).alias(
        "p0_p01_p1_p50_p99_max"
    ),
    F.expr("count_if(rent_eur < 100)").alias("lt_100"),
    F.expr("count_if(rent_eur > 5000)").alias("gt_5000"),
    F.expr("count_if(utilities_included)").alias("utilities_incl"),
).show(truncate=False)
print("Rents below €100:")
ams.where(F.regexp_extract("rent", r"€\s*([0-9.]+)", 1).try_cast("int") < 100).select(
    "rent", "propertyType", "areaSqm", "title"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Other money/area fields and their null spellings

# COMMAND ----------

for column in ["additionalCostsRaw", "deposit", "registrationCost", "areaSqm"]:
    print(f"--- {column} shapes")
    ams.groupBy(F.regexp_replace(F.trim(column), r"[0-9]+", "9").alias("shape")).count().orderBy(
        F.desc("count")
    ).show(10, truncate=False)

ams.select(F.regexp_extract("areaSqm", r"([0-9]+)", 1).try_cast("int").alias("area")).select(
    F.percentile_approx("area", [0.0, 0.01, 0.5, 0.99, 1.0]).alias("area_p0_p1_p50_p99_max"),
    F.expr("count_if(area IS NULL OR area = 0)").alias("missing_or_zero"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Property types → segment mapping

# COMMAND ----------

ams.groupBy("propertyType").agg(
    F.count("*").alias("n"),
    F.percentile_approx(F.regexp_extract("rent", r"€\s*([0-9.]+)", 1).try_cast("int"), 0.5).alias(
        "median_rent"
    ),
    F.percentile_approx(F.regexp_extract("areaSqm", r"([0-9]+)", 1).try_cast("int"), 0.5).alias(
        "median_m2"
    ),
).orderBy(F.desc("n")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Listings per PC4 × segment → low-sample threshold (A-007)
# MAGIC How many postcode/segment cells would a threshold of 3, 5 or 10 listings flag?

# COMMAND ----------

segment = F.when(F.col("propertyType").isin("Apartment", "Studio"), "whole_unit").when(
    F.col("propertyType").isin("Room", "Student residence"), "room"
)
cells = (
    ams.select("pc4", segment.alias("segment"))
    .where("segment IS NOT NULL")
    .groupBy("pc4", "segment")
    .count()
)
cells.groupBy("segment").agg(
    F.count("*").alias("pc4_cells"),
    F.percentile_approx("count", 0.5).alias("median_listings"),
    F.expr("count_if(count < 3)").alias("lt_3"),
    F.expr("count_if(count < 5)").alias("lt_5"),
    F.expr("count_if(count < 10)").alias("lt_10"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results and decisions (run 2026-10-16)
# MAGIC
# MAGIC | Finding | Number | Decision |
# MAGIC |---|---|---|
# MAGIC | Rows / distinct `_id` | 46,722 / 46,722; `_id` always a 1-element array | `listing_id = element_at(_id, 1)`; uniqueness still enforced by `dq_uniqueness` |
# MAGIC | Crawl window | 2019-07-14 → 2020-03-03 | Documented as the comparison period (pre-COVID) |
# MAGIC | City spellings | `Amsterdam` 8,094 · `amsterdam` 1 | Scope filter `lower(trim(city)) = 'amsterdam'` (8,095 rows) |
# MAGIC | City vs geo | 0 `Amsterdam` rows outside the Amsterdam PC4 set; the lowercase row is PC4 1181 (Amstelveen) | That row is quarantined by `pc4_in_amsterdam`: a visible reason, not a silent filter |
# MAGIC | `Diemen` rows with an "Amsterdam" PC4 | 123, all **PC4 1112** (streets are in Diemen) | The reference data attributes 1112 to Amsterdam; keep city as the rentals scope; document as a reference-data limitation (Airbnb listings in 1112 count as Amsterdam) |
# MAGIC | Rent shapes | only `€ N,-` and `€ N,-  Utilities incl.`; 0 unparsed | `parse_rent` as designed |
# MAGIC | Rent distribution | min €1 · p1 €350 · median €795 · p99 €2,550 · max €5,000 | Bounds **€100–€10,000**: quarantines 4 rows (three €1, one €50 for 7 m²) |
# MAGIC | Utilities included | 5,704 of 8,094 | Kept as a flag; revenue stays gross (A-011) |
# MAGIC | Null spellings | `NA`, `-`, SQL NULL, **and values wrapped in newlines** (`\n      -\n`) | `normalise_null` must strip **all whitespace** (`\s`), not just spaces: Spark's `trim` only removes spaces |
# MAGIC | Area | always `N m2`, 6–280 m², none missing | `parse_area_sqm` simple regex |
# MAGIC | Property types | Room 5,203 · Apartment 2,290 · Studio 597 · Anti-squat 3 · Student residence 1 | Segment map as designed; Anti-squat excluded (not a market rent) |
# MAGIC | PC4 × segment cells | room: 71 cells, median 69 listings, 6 with < 5 · whole_unit: 70 cells, median 40, 3 with < 5 | Low-sample threshold **n < 5** (A-007) |
