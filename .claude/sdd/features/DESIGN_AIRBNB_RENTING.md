# DESIGN: Airbnb vs Kamernet Investment Pipeline

> Technical design for a Lakeflow SDP medallion pipeline on Databricks Free Edition, deployed with a revo-dabs-based bundle and GitHub Actions.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AIRBNB_RENTING |
| **Date** | 2026-10-16 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_AIRBNB_RENTING.md](./DEFINE_AIRBNB_RENTING.md) (v1.1; deviations applied) |
| **Status** | Ready for Build |

---

## Verification Results (DEFINE open questions)

| # | Question | Result | Source | Confidence |
|---|----------|--------|--------|------------|
| 1 | SDP Python API | `from pyspark import pipelines as dp`; `import dlt` is legacy. Reads use `spark.read.table` / `spark.readStream.table` | databricks-pipelines skill; revo-dabs example | ✅ Verified |
| 2 | Catalog creation | **Build step 1:** REST `catalogs create` fails on Free Edition ("Metastore storage root URL does not exist"); **SQL `CREATE CATALOG` works** (uses Default Storage). `just bootstrap` therefore runs idempotent SQL through the Statement Execution API (`.just/run_sql.sh`) | `scratch/00_capability_probe.py` | ✅ Verified |
| 3 | Spatial SQL | **Build step 1:** `ST_GeomFromGeoJSON` / `ST_Point` / `ST_Contains` work on the SQL warehouse (DBSQL 2026.39) **and** on serverless compute (DBR 19.9, Spark 4.2). Shapely fallback not needed | `scratch/00_capability_probe.py` | ✅ Verified (pipeline runtime confirmed at first deploy) |
| 4 | Multi-schema publishing | One pipeline can write to several schemas with fully qualified `name=` | databricks-pipelines skill (pipeline-configuration) | ✅ Verified |
| 5 | Wheel on serverless SDP | Bundle `artifacts` (`uv build`) + pipeline `environment.dependencies: [${var.wheel_path}]`, the revo-dabs pattern. Alternative: `--editable ${workspace.file_path}` | revo-dabs template; databricks-pipelines skill | ✅ Pattern verified; ⚠️ glob path on Free Edition confirmed at first deploy |
| 6 | revo-dabs vs Free Edition | Template pipeline uses **classic clusters** (`serverless: false`, `i3.xlarge`, `edition: ADVANCED`), `spark_version 16.4`, a `test` target and semantic-release-triggered CD. Must be adapted (Decision 1) | Cloned template source | ✅ Verified |
| 7 | Amsterdam 30-night rule | Applies to renting a **whole home** (permit + notification). Renting a room in your own home = B&B, **no night cap**. Third-party guides report 15 nights in some central areas from 1 Apr 2026; the 2019 data predates that | Airbnb help + rental guides (to cite in docs) | ✅ Whole-home cap; ⚠️ 2026 central rule unofficial |
| 8 | AI/BI point map | Not verified; still a COULD | n/a | ⚠️ Build step 7 |
| 9 | Local tests | **Databricks Connect (serverless)**, user choice, matching revo-dabs. Serverless = DBR 19.9 / Spark 4.2 → pinned `databricks-connect==19.2.*` (newest client; template's `16.4.*` was 3 majors behind) | User decision; probe | ✅ Verified |
| 10 | Raw vs geo PC4 conflict | Trust a raw zip when it's a valid Amsterdam PC4; otherwise use geo; always record `pc4_geo_mismatch` | Decision 9 | ✅ Decided |

### New profiling facts found during design

| Fact | Evidence | Design impact |
|------|----------|---------------|
| Airbnb has **382 exact duplicate rows** | Full-row duplicate count | Dedup in silver on row hash (Decision 8); conservation check includes duplicates |
| Postcode GeoJSON covers **474 PC4s in 47 municipalities**; Amsterdam = **82 PC4s (1011–1112)** | `gem_name` property | "Is Amsterdam" comes from geo data, not a hardcoded range (Decision 10) |
| Every Kamernet `city = Amsterdam` row has a PC4 in the Amsterdam polygon set | 0 mismatches | `pc4_in_amsterdam` rule is a safeguard, expected to pass |
| Rent strings: only `€ N,-` and `€ N,-  Utilities incl.`; **no thousands separator**; min €1, 4 rows < €100, max €5,000 | Pattern histogram | Simpler parser; bounds €100–€10,000 quarantine 4 rows |
| Airbnb price: min €15, median €125, p99 €1,260, max €9,000; 209 rows > €1,000 | Distribution | Upper bound €2,000 (calibrate in scratch); medians make outliers harmless |
| GeoJSON CRS = CRS84 (lon, lat) | `crs` member | `ST_Point(longitude, latitude, 4326)`: order matters |

### Build step 3 findings (`scratch/01`, `scratch/02`): frozen inputs for step 4

| Finding | Evidence | Frozen decision |
|---------|----------|-----------------|
| GeoJSON mixes Polygon/MultiPolygon; inference degrades coordinates to strings | Inferred `array<array<array<string>>>` | Read with explicit schema, **`geometry STRING`** (raw JSON) → `ST_GeomFromGeoJSON`; all 474 valid |
| Spark 4 is ANSI by default | `CAST('' AS INT)` raised `CAST_INVALID_INPUT` | Parsing uses `try_cast` / `when(x != '')`, never a bare cast |
| Raw vs geo PC4 mismatch is **fuzzing** | 1,616 mismatches: median 66 m, 85% ≤ 200 m, 21 > 1 km | AT-019 policy (trust valid raw) **validated by data** |
| PC4 coverage | raw 7,629 + geo 2,260 = 9,889/9,913 (99.76%); 0 border double-matches; 24 → quarantine | SC-03 met before dedup |
| Airbnb price outliers | p99.9 €2,556; > €5,000: 3 rows (€9,000 for 2 guests/no reviews) | **Bounds €10–€5,000** (user decision; was €2,000) |
| Airbnb near-duplicates | 130 extra rows: same coords/type/guests/bedrooms, price within 5% | **Keep, count, document** (user decision): identity unprovable, ~1.4% of rows |
| Rentals city spelling | `amsterdam` ×1 (PC4 1181 = Amstelveen) | Scope `lower(trim(city)) = 'amsterdam'`; that row is quarantined by `pc4_in_amsterdam` |
| Reference data quirk | 123 `Diemen` rentals in PC4 1112, which the GeoJSON labels Amsterdam | Rentals scope stays city-based; document 1112 as a reference-data limitation |
| Null spellings wrapped in newlines | `"\n      -\n"` in costs/deposit | `normalise_null` strips `\s` (Spark `trim` only removes spaces) |
| Rent bounds | 4 rows < €100 (three €1, one €50); max €5,000 | **Bounds €100–€10,000** confirmed |
| Low-sample cells | Rentals: 6 room + 3 whole_unit PC4 cells with n < 5 | **Threshold n < 5** confirmed |
| Crawl window | Kamernet 2019-07-14 → 2020-03-03 | Documented comparison period (pre-COVID) |

### Build steps 4–7 deviations (as built)

| Area | Design said | As built | Why |
|------|-------------|----------|-----|
| Schema isolation | `var.schema_prefix` (`dev_`) | **Removed**; `mode: development` already prefixes schemas `dev_<user>_` (validate showed `dev_<user>_dev_bronze`) | Per-developer isolation for free; prod uses plain `bronze/silver/gold` |
| Parsing API | `parse_rent` returning a tuple | `parse_eur` + `utilities_included` | Single-purpose, reused for deposit/costs, testable separately |
| Rules | `price BETWEEN …` | `price IS NULL OR price BETWEEN …` (same for pc4/rent) | One cause → one `failed_rules` reason |
| Notebook globals | implicit `spark` | `from databricks.sdk.runtime import spark` (revo-dabs pattern) | Type-checkable; works in pipelines |
| Pipeline config | `spark.conf.get(...)` | `require_conf(key, spark.conf.get(key))` | `conf.get` returns `str | None`; a missing key would silently build `None.airbnb_raw` |
| Parquet export | `coalesce(1).write.parquet` | pandas `to_parquet` → one `<table>.parquet` per table; `pdf.attrs = {}` first | Clean single files for reviewers (gold is small); Spark Connect's `toPandas()` puts PlanMetrics in `attrs`, which `to_parquet` can't JSON-serialise |
| Bundle sync | (not designed) | `sync.exclude: data/**, .claude/**, CLAUDE.md` | First deploy uploaded 144 files incl. 13 MB of zips |
| Dashboard key | `airbnb_renting` | `airbnb_renting_dashboard` | Resource keys must be unique across *all* resource types |
| Local scripts | `uv run python scratch/…` | `just scratch <name>` (`uv run --no-editable`) | uv marks `.venv` hidden on macOS; Python 3.12.4 skips hidden `.pth` files, so editable installs aren't importable |
| `gold won't refresh on bad data` (Decision 6) | claimed | **Not guaranteed**: gold MVs read silver in parallel with `dq_uniqueness`; the *update* fails, but MVs already refreshed in it stay | Documented honestly; enforcing order would need gold to depend on `dq_uniqueness` |
| Branch protection (Decision 13, AT-024) | Ruleset requiring the `ci` check on `main` | Ruleset created but **not enforced**: GitHub Free doesn't enforce rulesets/branch protection on private repos. Technical control instead: CD calls CI (`workflow_call`) and `deploy_prod` `needs: ci`, so nothing deploys to prod unless CI passes on that commit | Platform limitation; the deploy is the gate. Enforced merge-blocking needs GitHub Pro/Team or a public repo |
| CI lint step | separate ruff/ty/pydoclint commands | `uv run prek run --all-files` | CI's `ruff format --check .` also formatted Markdown code blocks, which the local hook didn't: one definition of clean |
| CI identity | SP (fallback PAT) | SP `github-actions-airbnb` (OAuth M2M), least-privilege grants; prod `root_path` in the SP's folder + `run_as` the SP | `/Workspace/Shared` would be writable by all users |
| Kamernet-only PC4s in dashboard | (not designed) | 4 rows without an Airbnb scenario are excluded by the scenario filter; stated in the subtitle | Dashboard filters drop NULL-scenario rows |

---

## Architecture Overview

```text
┌──────────────────────────── Databricks Free Edition (serverless, one workspace) ─────────────────────────────┐
│                                                                                                              │
│  SHARED, created once by `just bootstrap` (not bundle-owned: survives `bundle destroy`)                      │
│  ┌──────────────────────────────────────────────────────────────┐                                            │
│  │ catalog airbnb_renting ─ schema landing ─ volume raw          │◀── user uploads unzipped files manually   │
│  │   /Volumes/airbnb_renting/landing/raw/{airbnb,rentals,geo}/   │                                            │
│  └───────────────────────────────┬──────────────────────────────┘                                            │
│                                  │ Auto Loader / batch read                                                  │
│  BUNDLE-OWNED, per target (dev → dev_* schemas, prod → plain schemas)                                        │
│  ┌───────────────────────────────▼─────────────────────────────────────────────────────────────────────┐     │
│  │ SDP pipeline `airbnb_renting` (serverless, triggered, PREVIEW channel, wheel via environment)        │     │
│  │                                                                                                       │     │
│  │ bronze.airbnb_raw (ST)   bronze.rentals_raw (ST)   bronze.postcodes_geo (MV)                          │     │
│  │        │                        │                         │ (static side of joins)                    │     │
│  │        ▼                        ▼                         │                                           │     │
│  │ airbnb_parsed (temp view) rentals_parsed (temp view) ◀────┘                                           │     │
│  │  dedup · parse · resolve PC4 (raw ▸ geo)   parse · Amsterdam scope filter                             │     │
│  │        │  same rules dict                    │  same rules dict                                       │     │
│  │   ┌────┴──────────┐                    ┌─────┴─────────┐                                              │     │
│  │   ▼               ▼                    ▼               ▼                                              │     │
│  │ silver.airbnb_  silver.airbnb_    silver.rentals_ silver.rentals_     silver.dq_uniqueness (MV,       │     │
│  │ listings (ST)   quarantine (ST)   listings (ST)   quarantine (ST)      expect_or_fail)                │     │
│  │        └──────────────┬────────────────┘                                                              │     │
│  │                       ▼  batch reads (MVs)                                                            │     │
│  │ gold.revenue_per_property ─▶ gold.revenue_per_postcode ─▶ gold.investment_comparison                  │     │
│  └───────────────────────────────────────────────────────┬───────────────────────────────────────────────┘     │
│                                                          │                                                     │
│  Job `airbnb_renting_job`: [pipeline_task] ─▶ [export_parquet notebook] ─▶ gold.exports volume (parquet)      │
│  AI/BI dashboard `Airbnb vs Kamernet` ◀── queries gold (Serverless Starter Warehouse)                          │
└───────────────────────────────────────────────────────────┬──────────────────────────────────────────────────┘
                                                            │ `just export` (databricks fs cp)
                                                            ▼
                                                     ./data/output/*.parquet

GitHub ── PR ──▶ CI: prek/ruff · ty · pydoclint · uv build · pytest (Databricks Connect) · bundle validate
       └─ main ─▶ CD: bundle deploy -t prod
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Bootstrap recipe | Create shared catalog, `landing` schema and `raw` volume idempotently | `just bootstrap` → `databricks catalogs/schemas/volumes create` |
| `airbnb_renting` package | All parsing, PC4 resolution, quality rules and revenue logic as pure DataFrame/Column functions | Python 3.12, PySpark API, built as a wheel by `uv build` |
| SDP pipeline | Bronze/silver/gold tables, expectations, quarantine | Lakeflow SDP (Python notebooks), serverless |
| Job | Orchestrates pipeline refresh and parquet export | Lakeflow Jobs (serverless) |
| Export notebook | Writes gold tables as parquet to a volume | Notebook task |
| Dashboard | Investor view of revenue per postcode | AI/BI (Lakeview) `.lvdash.json` in bundle |
| Bundle | IaC for schemas, export volume, pipeline, job, dashboard; dev/prod targets | Declarative Automation Bundles (from revo-dabs) |
| CI/CD | Quality gate on PR, deploy on main | GitHub Actions |
| Dev tooling | Hooks, linters, task runner | prek (pre-commit), ruff, ty, pydoclint, just, uv |
| Scratch notebooks | Exploration, capability probe, output validation | Databricks notebooks |
| Docs + diagrams | The *why*, ADRs, data issues, revenue model | Markdown + Excalidraw (`/agentspec:excalidraw-diagram`) |

---

## Key Decisions

### Decision 1: Start from revo-dabs, adapted to serverless Free Edition

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** The brief recommends revo-dabs, the reviewers' own template. Its defaults assume classic clusters, Azure, a `test` target and semantic-release-driven CD.

**Choice:** Initialise with `databricks bundle init https://github.com/revodatanl/revo-dabs` (tailored: GitHub CI/CD, `cloud_provider=aws`, no example jobs, no Windows support, DAB recipes + VS Code on), then adapt:
- pipeline: `serverless: true`, remove `clusters`, `photon`, `edition`, `spark_version` variable;
- targets: keep `dev` (development mode) and `prod`; **drop `test`** (one workspace; CI validates `prod`);
- CD: trigger on `push` to `main` instead of after semantic-release (semantic-release kept only if it works unchanged: G12);
- delete Azure files, the example job/pipeline, template docs that don't apply (`wsl.md`, images).

**Rationale:** Reviewers recognise the layout and tooling at a glance; adapting it shows we understand it rather than copying it.

**Alternatives Rejected:**
1. Hand-rolled bundle: loses the recognisable conventions the brief points to.
2. `databricks pipelines init` template: Databricks' generic layout, not RevoData's.

**Consequences:**
- We must be able to explain every template file we keep (ty, pydoclint, prek, commitizen).
- Deleting `test` diverges from the template; documented in `docs/decisions.md`.

---

### Decision 2: Shared landing zone outside the bundle; layer schemas inside it

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** The user uploads raw files manually. Bundles *own* their resources: `bundle destroy` deletes them, and two targets defining the same catalog would fight over it.

**Choice:**
- `just bootstrap` (idempotent CLI calls) creates catalog `airbnb_renting`, schema `landing`, volume `landing.raw`. Fallback: if catalog creation is refused, `var.catalog = workspace`.
- The bundle owns per-target schemas `${var.schema_prefix}bronze|silver|gold` (`dev_` for dev, empty for prod) and a managed volume `${var.schema_prefix}gold.exports`.

**Rationale:** Raw data outlives deployments; a `bundle destroy` must never delete the files the user uploaded. Environment-specific *outputs* are bundle-owned, so dev and prod never collide.

**Alternatives Rejected:**
1. Catalog + landing volume as bundle resources: destroy deletes uploads, and both targets claim the same catalog.
2. Catalog per target (`airbnb_renting_dev`): the user would have to upload the raw files twice.

**Consequences:**
- One manual-but-scripted step (`just bootstrap`) before the first deploy, documented in README.
- Interview talking point: *who owns which data* in IaC.

---

### Decision 3: SDP source = Databricks notebooks in `notebooks/pipelines/airbnb_renting/transformations/`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** The brief asks for pipeline notebooks in `./notebooks`; Level 3 asks for SDP.

**Choice:** Follow the revo-dabs convention: `.py` files in **Databricks notebook source format** (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC %md`) under `notebooks/pipelines/airbnb_renting/transformations/`, picked up by `libraries: - glob: include: .../transformations/**`. One file per concern (bronze, silver Airbnb, silver rentals, DQ, gold).

**Rationale:** These are notebooks in the workspace, diff cleanly in Git (unlike `.ipynb`), and match the reviewers' template exactly.

**Alternatives Rejected:**
1. `.ipynb` notebooks: noisy diffs, outputs leak into Git.
2. Plain `.py` modules under `src/`: contradicts the brief's "notebooks".

**Consequences:**
- Notebooks can't be run cell-by-cell outside a pipeline update. All logic lives in the wheel (Decision 4) and is tested there.

---

### Decision 4: "Thin notebooks, fat library": logic in `src/airbnb_renting`, wheel via bundle artifacts

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** SC-08 (≥ 80% coverage) and SC-12 (no logic in notebooks).

**Choice:** Every transformation is a pure function `(DataFrame | Column, ...) -> DataFrame | Column` in the package. Notebooks only read config and inputs, call one function and return. The wheel is built by `artifacts.platform.build: uv build` and installed through the pipeline's `environment.dependencies: [${var.wheel_path}]`.

**Rationale:** Testable without a pipeline; one artifact is tested in CI and runs in Databricks; it matches the template.

**Alternatives Rejected:**
1. Logic in notebooks: untestable, fails SC-12.
2. `--editable ${workspace.file_path}`: kept as the **fallback** if the wheel glob doesn't resolve on Free Edition, but it skips the "buildable package" story.

**Consequences:**
- Package version bump or redeploy needed for logic changes (fine: `bundle deploy` rebuilds).

---

### Decision 5: Parse once in a temporary view; valid and quarantine tables share one rules dict

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** We want both `expect_all_or_drop` (Level 3 expectations) *and* no silent drops (G2).

**Choice:**
- `@dp.temporary_view` `airbnb_parsed` / `rentals_parsed`: streaming read of bronze → parse → resolve.
- `silver.*_listings`: `@dp.expect_all_or_drop(RULES)` over the view.
- `silver.*_quarantine`: the same view filtered with `NOT all_rules_pass(RULES)`, plus `failed_rules ARRAY<STRING>`.
- `RULES` lives in `airbnb_renting/quality.py`. Each rule is wrapped null-safe (`coalesce((cond), false)`), so a NULL never escapes both tables.
- Soft checks (missing bedrooms/review score) use `@dp.expect_all` (warn) on the valid table.

**Rationale:** Expectations give event-log metrics and UI visibility; the inverse filter keeps every rejected row inspectable with *why* it failed. One dict means the two tables can't drift apart.

**Alternatives Rejected:**
1. Only `expect_or_drop`: bad rows vanish (counted, but not inspectable).
2. Hand-rolled filters without expectations: loses the Level 3 requirement and event-log metrics.
3. `_rescued_data` quarantine only: covers schema/parse problems, not value rules (we read all-string, so rescue is rare).

**Consequences:**
- The view is computed twice (once per consumer). Negligible at this size; noted in docs.
- **Gotcha:** SQL three-valued logic. Without `coalesce(..., false)` a NULL comparison would be dropped by the expectation *and* not match `NOT(...)`, so the row would disappear silently.

---

### Decision 6: Uniqueness checked by an aggregated DQ materialized view with `expect_or_fail`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** AT-022 wanted `expect_or_fail` on duplicate `_id`, but expectations are **row-level**: a single row can't know it's a duplicate.

**Choice:** `silver.dq_uniqueness` MV: one row per silver table with `n_rows`, `n_distinct_ids`, `n_duplicates`; decorated `@dp.expect_or_fail("unique_listing_id", "n_duplicates = 0")`.

**Rationale:** Uses the expectations framework for a table-level contract; fails the update loudly with a named rule.

**Alternatives Rejected:**
1. Row-level expectation: impossible to express.
2. Check only in the scratch notebook: not enforced by the pipeline.

**Consequences:**
- A contract break stops the pipeline after silver is written; gold won't refresh on bad data.

---

### Decision 7: Bronze is a faithful, all-string copy + lineage columns

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Choice:** Auto Loader with `cloudFiles.inferColumnTypes=false` (CSV) / default JSON inference (arrays preserved), `multiLine=true` for the JSON array. Add `_ingested_at = current_timestamp()` and `_source_file = _metadata.file_path`. `postcodes_geo` is an MV (batch read of a small static reference file) exploding `features` to `pc4`, `gem_name`, `geometry_json`, `centroid_lat/lon`.

**Rationale:** Bronze must never fail on bad values. All typing and cleaning happens in silver, where it's tested and quarantined.

**Alternatives Rejected:** typed schema in bronze: a malformed price would break ingestion instead of being quarantined.

**Consequences:** `rentals.json` is a single 69 MB array: not splittable, one task reads it. Fine at this size; documented.

---

### Decision 8: Exact-duplicate Airbnb rows are deduplicated in silver on a row hash

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** 382 exact duplicate rows; the CSV has no id.

**Choice:** `listing_id = sha2(concat_ws('||', <all source columns>), 256)`; `dropDuplicates(["listing_id"])` in `airbnb_parsed`. Counted in validation as `bronze − (valid + quarantine)`.

**Rationale:** Identical rows (same coordinates to 8 decimals, same price, same everything) are scrape duplicates. Counting them twice would bias medians in dense postcodes.

**Alternatives Rejected:**
1. Keep duplicates: biases aggregates and breaks `listing_id` uniqueness.
2. Dedup on lat/long only (738 dups): could merge genuinely distinct listings in the same building.

**Consequences:**
- `dropDuplicates` on a stream is **stateful** (keeps seen keys forever). That's fine for a bounded static source; for Level 4 it would need `dropDuplicatesWithinWatermark`. A documented trade-off.
- SC-02 becomes `bronze = valid + quarantine + duplicates_removed`.

---

### Decision 9: PC4 resolution: valid raw zip first, geo point-in-polygon second, mismatch recorded

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Context:** 23% of Airbnb zips are missing and many are malformed. Airbnb shifts coordinates slightly for privacy, so geo is approximate near borders.

**Choice:**
1. `raw_pc4 = regexp_extract(zipcode, '(?<![0-9])([1-9][0-9]{3})(?![0-9])', 1)` → INT or NULL.
2. `geo_pc4` = PC4 of the polygon containing `ST_Point(longitude, latitude, 4326)`, via a **stream-static broadcast join** on `ST_Contains(ST_GeomFromGeoJSON(geometry_json), point)`, computed for **every** row.
3. `pc4 = raw_pc4 if raw_pc4 ∈ Amsterdam PC4s else geo_pc4`; `pc4_source ∈ {raw, geo_backfill}`; `pc4_geo_mismatch = raw_pc4 IS NOT NULL AND geo_pc4 IS NOT NULL AND raw_pc4 <> geo_pc4`.
4. Pipeline channel: start on **`CURRENT`**, since the probe showed `ST_*` already works on default serverless (DBR 19.9). Switch to `PREVIEW` only if the pipeline runtime rejects the functions at first deploy (less preview risk).

**Fallback (if the build-step-1 probe shows ST functions unavailable):** pandas UDF using `shapely` (`STRtree` over the 474 polygons, read once per executor from the volume path) with `shapely` in `environment.dependencies`. Same output columns, so silver, gold and tests don't change.

**Rationale:** Authoritative polygons beat nearest-neighbour; computing geo for all rows lets us *measure* how trustworthy raw zips are (an interview-ready number).

**Alternatives Rejected:**
1. Geo always wins: Airbnb's location fuzzing would move correct zips across borders.
2. H3 polyfill equi-join: approximate at polygon edges; more concepts for no gain at 474 × 10k rows.
3. External API (Level 5): network dependency, rate limits.

**Consequences:** PREVIEW channel = newer runtime with less stability; acceptable for an assessment, noted for production.

---

### Decision 10: "Amsterdam" is defined by the postcode reference data

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Choice:** `amsterdam_pc4 = postcodes_geo WHERE gem_name = 'Amsterdam'` (82 PC4s, 1011–1112). Rentals: `city = 'Amsterdam'` is the **scope filter** (out of scope ≠ bad data; excluded *before* conservation). The quality rule `pc4_in_amsterdam` then catches inconsistent rows. Airbnb: the same rule; listings that resolve to a neighbouring municipality (Amstelveen, Diemen…) are quarantined with that reason.

**Alternatives Rejected:** hardcoded `1000–1109` (wrong: Amsterdam goes up to 1112 and starts at 1011).

---

### Decision 11: Revenue model: long format, config-driven scenarios, gross monthly EUR

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Choice:**
- Scenarios come from pipeline configuration (JSON string) → `list[Scenario]` via `config.parse_scenarios`:
  `[{"name":"legal_cap_30n","nights_per_year":30,"segments":["whole_unit"]}, {"name":"occ_50","occupancy":0.5}, {"name":"occ_70","occupancy":0.7}]`
- Airbnb monthly revenue = `price × nights_per_month × occupancy`, or `price × nights_per_year / 12` for capped scenarios. The cap applies **only to `whole_unit`** (rooms in your own home = B&B, no night cap).
- Kamernet monthly revenue = `rent_eur` (scenario `long_term`).
- `gold.revenue_per_property`: long format `(source, listing_id, pc4, segment, scenario, monthly_revenue_eur)`.
- `gold.revenue_per_postcode`: `(source, pc4, segment, scenario)` → `median`, `avg`, `n_listings`.
- `gold.investment_comparison`: `(pc4, segment, scenario)` → Kamernet median rent, Airbnb median price and revenue, `winner`, `breakeven_occupancy = kamernet_median / (airbnb_median_price × nights_per_month)`, `airbnb_never_wins = breakeven > 1`, `n_kamernet`, `n_airbnb`, `low_sample`, `centroid_lat/lon`. **Full outer join** on `(pc4, segment)`, so one-sided postcodes stay visible (AT-016).

**Rationale:** Long format makes adding a scenario a config change, not a schema change; breakeven is scenario-independent.

**Alternatives Rejected:** wide columns per scenario (schema changes per scenario); mean instead of median (skew: p99 €1,260, max €9,000).

---

### Decision 12: Tests on serverless via Databricks Connect

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted (user choice) |
| **Date** | 2026-10-16 |

**Choice:** Keep the revo-dabs `conftest.py` (`DatabricksSession.builder.serverless(True)`); pin `databricks-connect` to the Free Edition serverless client version (checked in build step 1); local auth via `DATABRICKS_CONFIG_PROFILE`, CI via GitHub secrets.

**Rationale:** Same engine as production (Spark Connect on serverless), so the `ST_*` functions are testable and serverless-only gaps (e.g. no RDDs) surface early.

**Alternatives Rejected:** local PySpark: offline and fast, but can't test spatial logic, and it diverges from the template.

**Consequences:**
- Tests need network and credentials and use Free Edition quota. Fixtures stay tiny, and the session is created once (`scope="session"`).
- **Gotcha:** `databricks-connect` and `pyspark` can't coexist in one venv.

---

### Decision 13: CI/CD on GitHub Actions: gate on PR, deploy prod on main

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Choice:**
- `ci.yml` (pull_request): `uv sync --frozen` → ruff → ty → pydoclint → `uv build` + package validation → pytest with coverage (`--cov-fail-under=80`) → `databricks bundle validate -t prod`.
- `cd.yml` (push to `main`): composite action → `databricks bundle deploy -t prod`. No auto-run (a run consumes quota; triggered manually or via `just run-prod`).
- Auth: service principal OAuth M2M (`DATABRICKS_CLIENT_ID/SECRET`) as in the template; **fallback** to a PAT (`DATABRICKS_TOKEN`) if Free Edition doesn't allow SP secrets (checked in build step 8).
- Branch protection on `main` requiring CI (AT-024).
- *(v1.3, user-confirmed)* **semantic-release removed** (`.github/workflows/semantic-release.yml`, `release.config.mjs`). `cd.yml` triggers on `push` to `main`; `ci.yml` validates `-t prod` (the template's `test` target/environment no longer exists). Conventional commits stay enforced by the commitizen hook; `uv run cz bump` is available for manual releases.
- *(v1.3)* Template hygiene: drop `prek autoupdate` from `just setup` (non-reproducible hook versions); remove `.github/dependabot.yml` (dependency-PR noise during the assessment).

**Alternatives Rejected:**
1. semantic-release-triggered CD (template default): it pushes a release commit to `main` with `GITHUB_TOKEN`, which **conflicts with branch protection** unless the bot gets a bypass exception. And it versions a *published library*, while our wheel is an internal deployment artifact. Release ceremony without value here.
2. Keeping both via a ruleset bypass / PAT for the release bot: an extra security exception to justify, for no assessment benefit.

---

### Decision 14: Parquet export via a job notebook, pulled locally with `just export`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Choice:** Job task 2 (`notebooks/export_parquet.py`, serverless) writes each gold table with `coalesce(1).write.mode("overwrite").parquet(<exports volume>/<table>)`. `just export` runs `databricks fs cp -r dbfs:/Volumes/<cat>/<prefix>gold/exports ./data/output --overwrite`.

**Rationale:** Export is a side effect *outside* SDP, so it belongs in the job, not the pipeline. `coalesce(1)` produces readable single files for reviewers (data is tiny).

**Alternatives Rejected:** an SDP sink (overkill and streaming-oriented); exporting from local Databricks Connect (bypasses the orchestrated job).

---

### Decision 15: Dashboard as a bundle resource on the Serverless Starter Warehouse

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-10-16 |

**Choice:** `resources/dashboards/airbnb_renting.lvdash.json` + `airbnb_renting.dashboard.yml` with `warehouse_id: ${var.warehouse_id}` (bundle variable `lookup: warehouse: "Serverless Starter Warehouse"`), `dataset_catalog/schema` per target. Pages: (1) KPIs + scenario selector, (2) PC4 table/bar chart of Kamernet vs Airbnb, (3) map of PC4 centroids coloured by breakeven (COULD, G11). SQL queries are tested before deployment (databricks-aibi-dashboards skill rule).

---

## File Manifest

> 🧩 = generated by revo-dabs, then adapted. ✳️ = new. 🗑 = template file deleted.

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| **Scaffold & tooling** ||||||
| 1 | `databricks.yml` | 🧩 Modify | Bundle, artifacts, variables (`catalog`, `schema_prefix`, `landing_path`, `warehouse_id`, `wheel_path`), targets dev/prod | @ci-cd-specialist | — |
| 2 | `pyproject.toml` | 🧩 Modify | Package `airbnb_renting`, pin databricks-connect, `--cov-fail-under=80`, ruff/ty config | @python-developer | — |
| 3 | `.pre-commit-config.yaml` | 🧩 Keep | ruff, ty, pydoclint, uv-lock, commitizen hooks | (general) | 2 |
| 4 | `.justfile`, `.just/dab.justfile` | 🧩 Modify | Add `bootstrap`, `deploy`, `run`, `export`, `validate` recipes | @shell-script-specialist | 1 |
| 5 | `.gitignore`, `.gitattributes`, `.python-version` | 🧩 Modify | Ignore unzipped raw data, `.venv`, `dist` | (general) | — |
| 6 | Azure files, example job/pipeline/notebook, `src/.../main.py`, template docs (`wsl.md`, images, `coding_standard.md`…) | 🗑 Delete | Repo hygiene (R3) | (general) | — |
| **Resources (IaC)** ||||||
| 7 | `resources/airbnb_renting.schemas.yml` | ✳️ Create | `bronze/silver/gold` schemas per target + `gold.exports` volume | @ci-cd-specialist | 1 |
| 8 | `resources/airbnb_renting.pipeline.yml` | ✳️ Create | Serverless SDP pipeline, PREVIEW channel, configuration, wheel env | @lakeflow-pipeline-builder | 1, 7 |
| 9 | `resources/airbnb_renting.job.yml` | ✳️ Create | pipeline_task → export notebook; failure email to current user | @lakeflow-pipeline-builder | 8, 27 |
| 10 | `resources/airbnb_renting.dashboard.yml` | ✳️ Create | Dashboard resource | @ci-cd-specialist | 7, 11 |
| 11 | `resources/dashboards/airbnb_renting.lvdash.json` | ✳️ Create | Dashboard definition | (general, databricks-aibi-dashboards skill) | 25 |
| **Package `src/airbnb_renting/`** ||||||
| 12 | `__init__.py` | ✳️ Create | Package marker + version | @python-developer | 2 |
| 13 | `config.py` | ✳️ Create | `Scenario` dataclass, `parse_scenarios(json)`, constants (`NIGHTS_PER_MONTH`, bounds, low-sample n) | @python-developer | 12 |
| 14 | `parsing.py` | ✳️ Create | `normalise_null`, `parse_pc4`, `parse_eur`, `parse_rent` (amount + utilities flag), `parse_area_sqm`, `first_element` | @python-developer | 12 |
| 15 | `segments.py` | ✳️ Create | `airbnb_segment`, `kamernet_segment` | @python-developer | 12 |
| 16 | `quality.py` | ✳️ Create | `AIRBNB_RULES`, `RENTALS_RULES`, `*_WARN_RULES`, `null_safe`, `all_rules_pass`, `failed_rules` | @python-developer | 12 |
| 17 | `geo.py` | ✳️ Create | `explode_postcodes(geojson_df)`, `amsterdam_pc4(postcodes_df)`, `assign_geo_pc4(points_df, postcodes_df)`, `resolve_pc4(...)` | @spark-engineer | 12 |
| 18 | `transforms.py` | ✳️ Create | `bronze_airbnb_to_parsed`, `bronze_rentals_to_parsed`, `dq_uniqueness` (compose 13–17) | @spark-engineer | 13–17 |
| 19 | `revenue.py` | ✳️ Create | `airbnb_monthly_revenue`, `revenue_per_property`, `revenue_per_postcode`, `breakeven_occupancy`, `investment_comparison` | @spark-engineer | 13, 15 |
| **Pipeline notebooks** ||||||
| 20 | `notebooks/pipelines/airbnb_renting/transformations/01_bronze.py` | ✳️ Create | `airbnb_raw`, `rentals_raw` (ST, Auto Loader), `postcodes_geo` (MV) | @lakeflow-pipeline-builder | 8 |
| 21 | `.../02_silver_airbnb.py` | ✳️ Create | `airbnb_parsed` view, `airbnb_listings`, `airbnb_quarantine` | @lakeflow-pipeline-builder | 16–18, 20 |
| 22 | `.../03_silver_rentals.py` | ✳️ Create | `rentals_parsed` view, `rentals_listings`, `rentals_quarantine` | @lakeflow-pipeline-builder | 16, 18, 20 |
| 23 | `.../04_dq_checks.py` | ✳️ Create | `dq_uniqueness` MV with `expect_or_fail` | @lakeflow-pipeline-builder | 18, 21, 22 |
| 24 | `.../05_gold.py` | ✳️ Create | 3 gold MVs | @lakeflow-pipeline-builder | 19, 21, 22 |
| 25 | (gold tables exist after first run) | — | Input for dashboard SQL testing | — | 24 |
| 26 | `notebooks/pipelines/airbnb_renting/README.md` | 🧩 Modify | What each file defines | (general) | 20–24 |
| 27 | `notebooks/export_parquet.py` | ✳️ Create | Export gold → parquet in volume | @spark-engineer | 7 |
| **Scratch** ||||||
| 28 | `scratch/00_capability_probe.py` | ✳️ Create | Runtime/connect version, `ST_*` availability, catalog creation check | (general) | — |
| 29 | `scratch/01_explore_airbnb.py` | ✳️ Create | Profile + calibrate bounds (A-005) | @spark-engineer | bootstrap + upload |
| 30 | `scratch/02_explore_rentals.py` | ✳️ Create | Profile + rent patterns | @spark-engineer | bootstrap + upload |
| 31 | `scratch/03_validate_outputs.py` | ✳️ Create | Conservation, backfill hit-rate, mismatch rate, quarantine reasons, event-log expectation metrics, gold sanity | @data-quality-analyst | 24 |
| **Tests** ||||||
| 32 | `tests/conftest.py` | 🧩 Modify | Serverless `spark` fixture + fixture loaders | @test-generator | 2 |
| 33 | `tests/fixtures/airbnb_sample.csv` | ✳️ Create | ~20 rows: every malformed zip, a duplicate, outlier prices, room types | @test-generator | — |
| 34 | `tests/fixtures/rentals_sample.json` | ✳️ Create | ~15 records: rent variants, `NA`/`-`, non-Amsterdam, anti-squat | @test-generator | — |
| 35 | `tests/fixtures/postcodes_sample.geojson` | ✳️ Create | 3 simplified PC4 polygons (2 Amsterdam, 1 Amstelveen) | @test-generator | — |
| 36 | `tests/test_parsing.py` | ✳️ Create | AT-001…008, AT-010 | @test-generator | 14, 32 |
| 37 | `tests/test_segments.py` | ✳️ Create | AT-009 | @test-generator | 15 |
| 38 | `tests/test_quality.py` | ✳️ Create | Null-safety, failed_rules contents, rules ↔ quarantine partition | @test-generator | 16 |
| 39 | `tests/test_geo.py` | ✳️ Create | AT-017, AT-018, AT-019 on fixture polygons | @test-generator | 17, 35 |
| 40 | `tests/test_revenue.py` | ✳️ Create | AT-011…016, config parsing | @test-generator | 13, 19 |
| 41 | `tests/test_transforms.py` | ✳️ Create | End-to-end bronze-shaped fixture → parsed → conservation holds; dedup | @test-generator | 18, 33–35 |
| 🗑 | `tests/default_test.py`, `tests/default_spark_test.py` | 🗑 Delete | Template examples | (general) | — |
| **CI/CD** ||||||
| 42 | `.github/workflows/ci.yml` | 🧩 Modify | PR gate (Decision 13); validate `-t prod`, drop `environment: test` | @ci-cd-specialist | 1, 2 |
| 43 | `.github/workflows/cd.yml` | 🧩 Modify | Deploy prod on **push to main** (not after semantic-release) | @ci-cd-specialist | 1 |
| 🗑 | `.github/workflows/semantic-release.yml`, `release.config.mjs`, `.github/dependabot.yml` | 🗑 Delete | Decision 13 v1.3 | (general) | — |
| 44 | `.github/actions/cd/action.yml` | 🧩 Keep/Modify | Composite deploy; PAT fallback | @ci-cd-specialist | 43 |
| **Docs & deliverables** ||||||
| 45 | `README.md` | 🧩 Rewrite | What, quick start (profile → bootstrap → upload → deploy → run → export), results summary, links | @code-documenter | all |
| 46 | `docs/README.md` | 🧩 Rewrite | Docs index | @code-documenter | — |
| 47 | `docs/tooling.md` | ✳️ Create | Why Databricks Free Edition + SDP + DABs + revo-dabs (R1) | @code-documenter | — |
| 48 | `docs/decisions.md` | ✳️ Create | ADR log (these 15 decisions + brainstorm decisions, ≥ 15 with rejected alternatives) | @code-documenter | — |
| 49 | `docs/data_quality.md` | ✳️ Create | Issues found, rules, quarantine, dedup, PC4 backfill strategy + measured results | @data-quality-analyst | 31 |
| 50 | `docs/revenue_model.md` | ✳️ Create | Scenarios, regulation (with sources), breakeven, limitations (gross revenue, 2019 data, segment comparability) | @code-documenter | — |
| 51 | `docs/development.md`, `docs/bundle_deployment.md` | 🧩 Modify | Keep template content that is still true; adapt to Free Edition | @code-documenter | 1 |
| 52 | `docs/diagrams/data_flow.excalidraw` + `.png` | ✳️ Create | Data-flow diagram with concrete examples | `/agentspec:excalidraw-diagram` | 20–24 |
| 53 | `docs/diagrams/cicd_pipeline.excalidraw` + `.png` | ✳️ Create | CI/CD diagram | `/agentspec:excalidraw-diagram` | 42–44 |
| 54 | `docs/ai-sessions/*.md` | ✳️ Create | Exported AI sessions (R6) | user (export) | — |
| 55 | `data/output/*.parquet` | ✳️ Generate | Exported gold (O6) | `just export` | 27 |
| 56 | `LEARNINGS.md` | ✳️ Create/Append | Personal learning log (CLAUDE.md) | (general) | — |

**Total files:** ≈ 56 created or modified, plus template deletions.

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|----------------|----------------|
| @lakeflow-pipeline-builder | 8, 9, 20–24 | SDP tables, expectations, pipeline/job YAML |
| @spark-engineer | 17–19, 27, 29, 30 | PySpark transformations, spatial join, aggregations |
| @python-developer | 2, 12–16 | Typed, pure functions, dataclasses, package layout |
| @test-generator | 32–41 | pytest fixtures and edge-case tests |
| @ci-cd-specialist | 1, 7, 10, 42–44 | DABs targets/variables, GitHub Actions |
| @data-quality-analyst | 31, 49 | Validation notebook, DQ documentation |
| @code-documenter | 45–51 | README and the *why* docs |
| @shell-script-specialist | 4 | just recipes (bootstrap idempotency) |
| `/agentspec:excalidraw-diagram` | 52, 53 | User's explicit choice |
| (general) | 3, 5, 6, 11, 26, 28, 56 | Small edits; dashboard via the databricks-aibi-dashboards skill |

> **Learning-mode override (CLAUDE.md):** build happens **in small increments with a stop after each step**. Agents may draft, but every file is presented with an explanation before moving on. **"My turn" candidate:** `quality.failed_rules()` (build step 4), which is interesting, self-contained and low-risk.

---

## Build Order (small, reviewable steps)

| Step | Scope | Who | Exit check |
|------|-------|-----|------------|
| 0 | Create Free Edition workspace; `databricks auth login --host <url> --profile dbx-free` | **User** | `databricks current-user me --profile dbx-free` works |
| 1 | `bundle init revo-dabs` into the repo + adaptations (Decision 1); `just bootstrap`; `scratch/00_capability_probe` | Claude + user runs | Probe answers: ST funcs ✅/❌, catalog ✅/❌, serverless client version |
| 2 | **User uploads** files to `/Volumes/airbnb_renting/landing/raw/{airbnb,rentals,geo}/` | **User** | `databricks fs ls dbfs:/Volumes/...` |
| 3 | `scratch/01`, `02`: exploration + calibrate bounds | Claude | Bounds and rules frozen in `config.py` |
| 4 | Package: `config`, `parsing`, `segments`, `quality` + tests (TDD) | Claude + **my turn** | pytest green |
| 5 | `geo`, `transforms`, `revenue` + tests | Claude | pytest green, coverage ≥ 80% |
| 6 | Pipeline notebooks + schemas/pipeline/job YAML; `bundle deploy -t dev`; run | Claude | SC-01; `scratch/03` passes SC-02…06 |
| 7 | Gold export + `just export`; dashboard | Claude | SC-07; dashboard renders |
| 8 | CI/CD + GitHub repo + secrets + branch protection | Claude + user (secrets) | AT-024 demo: red PR, green PR, prod deploy |
| 9 | Docs, Excalidraw diagrams, README, cleanup, LEARNINGS.md | Claude | SC-13…15 |

---

## Code Patterns

### Pattern 1: Pure Column function (package) + its test

```python
# src/airbnb_renting/parsing.py
from pyspark.sql import Column
from pyspark.sql import functions as F

_NULL_TOKENS = ["", "NA", "-", "N/A"]
# NL postcodes never start with 0; lookarounds stop "12345" matching as "1234".
_PC4_PATTERN = r"(?<![0-9])([1-9][0-9]{3})(?![0-9])"


def normalise_null(col: Column) -> Column:
    """Map the source's many 'empty' spellings to a real NULL."""
    trimmed = F.trim(col)
    return F.when(trimmed.isin(_NULL_TOKENS) | trimmed.isNull(), None).otherwise(trimmed)


def parse_pc4(zipcode: Column) -> Column:
    """Extract a 4-digit PC4 from free text ('1079 HH Amsterdam' -> 1079), else NULL."""
    extracted = F.regexp_extract(normalise_null(zipcode), _PC4_PATTERN, 1)
    return F.when(extracted != "", extracted.cast("int"))


def parse_rent(rent: Column) -> tuple[Column, Column]:
    """'€ 600,-  Utilities incl.' -> (600.00, True). Thousands dots stripped defensively."""
    amount = F.regexp_extract(rent, r"€\s*([0-9.]+)", 1)
    amount_eur = F.when(amount != "", F.regexp_replace(amount, r"\.", "").cast("decimal(10,2)"))
    utilities_included = F.coalesce(F.lower(rent).contains("utilities incl"), F.lit(False))
    return amount_eur, utilities_included
```

```python
# tests/test_parsing.py
import pytest
from pyspark.sql import SparkSession
from airbnb_renting.parsing import parse_pc4


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1053", 1053), ("1018  DW", 1018), ("1079 HH Amsterdam", 1079), ("Nederland 1091 TS", 1091),
     ("`1055tk", 1055), ("342HUIS", None), ("....", None), ("0", None), (" ", None), (None, None)],
)
def test_parse_pc4(spark: SparkSession, raw: str | None, expected: int | None) -> None:
    """Protects AT-001..005: every zip shape seen in the real file."""
    df = spark.createDataFrame([(raw,)], "zipcode string")
    assert df.select(parse_pc4("zipcode").alias("pc4")).first()["pc4"] == expected
```

> Performance note for build: on Databricks Connect each parametrised case is a round-trip. Prefer **one DataFrame with all cases + an expected column** per test function to keep CI fast.

### Pattern 2: Shared rules → valid + quarantine pair (notebook, thin)

```python
# src/airbnb_renting/quality.py
from pyspark.sql import Column
from pyspark.sql import functions as F

AIRBNB_RULES: dict[str, str] = {
    "pc4_not_null": "pc4 IS NOT NULL",
    "pc4_in_amsterdam": "pc4_is_amsterdam",
    "price_parsed": "price_per_night_eur IS NOT NULL",
    "price_in_bounds": "price_per_night_eur BETWEEN 10 AND 5000",
    "segment_known": "segment IS NOT NULL",
}


def null_safe(rules: dict[str, str]) -> dict[str, str]:
    """NULL must count as a failure, or a row escapes both valid and quarantine tables."""
    return {name: f"coalesce(({cond}), false)" for name, cond in rules.items()}


def all_rules_pass(rules: dict[str, str]) -> Column:
    return F.expr(" AND ".join(f"({c})" for c in null_safe(rules).values()))


def failed_rules(rules: dict[str, str]) -> Column:
    # TODO(me): return ARRAY<STRING> with the names of the rules this row fails (empty if none).
    # Hint: build one `when(NOT cond, lit(name))` per rule, then think about how to drop the NULLs.
    ...
```

```python
# notebooks/pipelines/airbnb_renting/transformations/02_silver_airbnb.py  (excerpt)
from pyspark import pipelines as dp
from airbnb_renting.quality import AIRBNB_RULES, AIRBNB_WARN_RULES, all_rules_pass, failed_rules, null_safe
from airbnb_renting.transforms import bronze_airbnb_to_parsed

bronze, silver = spark.conf.get("bronze_schema"), spark.conf.get("silver_schema")

@dp.temporary_view(name="airbnb_parsed")
def airbnb_parsed():
    return bronze_airbnb_to_parsed(
        spark.readStream.table(f"{bronze}.airbnb_raw"),
        postcodes=spark.read.table(f"{bronze}.postcodes_geo"),  # static side of the join
    )

@dp.table(name=f"{silver}.airbnb_listings", comment="Clean Airbnb listings with resolved PC4")
@dp.expect_all_or_drop(null_safe(AIRBNB_RULES))
@dp.expect_all(AIRBNB_WARN_RULES)
def airbnb_listings():
    return spark.readStream.table("airbnb_parsed")

@dp.table(name=f"{silver}.airbnb_quarantine", comment="Rows failing AIRBNB_RULES, with reasons")
def airbnb_quarantine():
    return (spark.readStream.table("airbnb_parsed")
            .where(~all_rules_pass(AIRBNB_RULES))
            .withColumn("failed_rules", failed_rules(AIRBNB_RULES)))
```

### Pattern 3: Stream-static spatial join (geo.py)

```python
def assign_geo_pc4(points: DataFrame, postcodes: DataFrame) -> DataFrame:
    """Left-join each listing to the PC4 polygon containing it (CRS84: lon, lat)."""
    polys = F.broadcast(postcodes.select("pc4", "geometry_json"))
    joined = points.alias("p").join(
        polys.alias("g"),
        F.expr("ST_Contains(ST_GeomFromGeoJSON(g.geometry_json), "
               "ST_Point(p.longitude, p.latitude, 4326))"),
        "left",
    )
    return joined.select("p.*", F.col("g.pc4").alias("geo_pc4"))
```

> Edge case: a point exactly on a shared border matches two polygons. Dedup by `listing_id` after the join (keep min `geo_pc4`); tested in `test_geo.py`.

### Pattern 4: Revenue and breakeven (revenue.py)

```python
def airbnb_monthly_revenue(price: Column, scenario: Scenario, nights_per_month: int) -> Column:
    if scenario.nights_per_year is not None:            # legal-cap scenario
        return price * F.lit(scenario.nights_per_year) / F.lit(12)
    return price * F.lit(nights_per_month) * F.lit(scenario.occupancy)


def breakeven_occupancy(kamernet_median_rent: Column, airbnb_median_price: Column, nights_per_month: int) -> Column:
    """Occupancy at which Airbnb gross revenue equals long-term rent; >1 means Airbnb never wins."""
    return F.round(kamernet_median_rent / (airbnb_median_price * F.lit(nights_per_month)), 3)
```

### Pattern 5: Pipeline resource (serverless)

```yaml
# resources/airbnb_renting.pipeline.yml
resources:
  pipelines:
    airbnb_renting:
      name: airbnb_renting
      catalog: ${var.catalog}
      schema: ${var.schema_prefix}bronze            # default; silver/gold via fully qualified names
      serverless: true
      continuous: false
      channel: CURRENT                              # ST_* verified on serverless; PREVIEW only if needed (Decision 9)
      root_path: ../notebooks/pipelines/airbnb_renting
      libraries:
        - glob:
            include: ../notebooks/pipelines/airbnb_renting/transformations/**
      environment:
        dependencies:
          - ${var.wheel_path}
      configuration:
        landing_path: ${var.landing_path}
        bronze_schema: ${var.schema_prefix}bronze
        silver_schema: ${var.schema_prefix}silver
        gold_schema: ${var.schema_prefix}gold
        nights_per_month: "30"
        low_sample_threshold: "5"
        scenarios: '[{"name":"legal_cap_30n","nights_per_year":30,"segments":["whole_unit"]},{"name":"occ_50","occupancy":0.5},{"name":"occ_70","occupancy":0.7}]'
```

### Pattern 6: Bundle targets and variables

```yaml
# databricks.yml (excerpt)
variables:
  catalog:        { default: airbnb_renting }
  schema_prefix:  { default: "" }
  landing_path:   { default: /Volumes/airbnb_renting/landing/raw }
  wheel_path:     { default: "${workspace.root_path}/artifacts/.internal/*.whl" }
  warehouse_id:
    lookup: { warehouse: "Serverless Starter Warehouse" }

targets:
  dev:
    mode: development
    default: true
    variables: { schema_prefix: dev_ }
  prod:
    mode: production
```

### Pattern 7: Table-level contract (dq_uniqueness)

```python
@dp.materialized_view(name=f"{silver}.dq_uniqueness")
@dp.expect_or_fail("unique_listing_id", "n_duplicates = 0")
def dq_uniqueness():
    return dq_uniqueness_df({
        "airbnb_listings": spark.read.table(f"{silver}.airbnb_listings"),
        "rentals_listings": spark.read.table(f"{silver}.rentals_listings"),
    })  # -> one row per table: table_name, n_rows, n_distinct_ids, n_duplicates
```

---

## Data Flow

```text
1. User uploads airbnb.csv, rentals.json, post_codes.geojson → landing.raw volume (once)
   │
2. Job run → pipeline update (triggered)
   │  bronze: Auto Loader picks up new files only (checkpointed) + postcodes MV
   ▼
3. *_parsed temp views: dedup (Airbnb) · normalise nulls · parse money/area/ids
   · segment · PC4 resolve (raw ▸ geo) · Amsterdam scope (rentals)
   │
   ├──▶ silver.*_listings    (expect_all_or_drop)   ┐
   └──▶ silver.*_quarantine  (inverse + failed_rules) ┘→ silver.dq_uniqueness (expect_or_fail)
   ▼
4. gold MVs recompute: per property (long, scenarios) → per postcode (median) → comparison (breakeven)
   │
5. Job task 2: export gold → gold.exports volume (parquet)
   │
6. Dashboard reads gold live · `just export` copies parquet to ./data/output
```

---

## Integration Points

| External System | Integration Type | Authentication |
|-----------------|-----------------|----------------|
| Databricks Free Edition workspace | CLI / bundle / Databricks Connect | Local: OAuth U2M profile `dbx-free`; CI: SP OAuth M2M secrets (fallback PAT) |
| GitHub | Actions, branch protection | Repo secrets `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (or `DATABRICKS_TOKEN`) |
| revo-dabs template | One-time `bundle init` | Public repo |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|---------------|
| Unit | Every public function in `airbnb_renting` | `tests/test_*.py` | pytest + Databricks Connect (serverless) | ≥ 80% lines (`--cov-fail-under=80`) |
| Component | Bronze-shaped fixtures through `transforms` (conservation, dedup, PC4 resolution) | `tests/test_transforms.py` + fixtures | pytest | AT-017…019 + conservation |
| Pipeline (integration) | Deployed pipeline on real data | `scratch/03_validate_outputs.py` | Databricks notebook + event log | SC-02…06, AT-020…022 |
| Deployment | Bundle config | CI `bundle validate -t prod`; AT-023 manual first deploy | Databricks CLI | All resources |
| CI gate | PR workflow | `.github/workflows/ci.yml` | GitHub Actions | AT-024 demonstrated once |

**What each unit-test file protects against** (written into each test's docstring in /build): mis-parsed zips moving listings to the wrong postcode; rent strings becoming NULL and silently dropping; NULL-unsafe rules losing rows; the cap scenario wrongly applied to rooms; breakeven division errors; one-sided postcodes disappearing from gold.

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| Malformed value (price, rent, zip) | Parsed to NULL → fails a rule → quarantine with `failed_rules` | No |
| Schema drift in source file | Auto Loader `_rescued_data` column kept in bronze; documented | No |
| Duplicate ids in silver | `dq_uniqueness` `expect_or_fail` stops the update | No (fix data/logic) |
| Point on polygon border (2 matches) | Deterministic dedup (min PC4) | No |
| Missing landing files | Bronze produces 0 rows; `scratch/03` conservation flags it; README upload contract | No |
| Transient serverless failure | Prod pipeline (non-development mode) retries; job `max_retries: 1` on pipeline task | Yes |
| ST functions unavailable | Detected in probe (build step 1) → switch to shapely UDF fallback before step 6 | n/a |
| Incompatible schema change in a streaming table | Full refresh (explicit, user-approved; destroys checkpoints) | Manual |

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `var.catalog` | string | `airbnb_renting` | UC catalog (fallback `workspace`) |
| `var.schema_prefix` | string | `dev_` (dev) / `""` (prod) | Isolates targets in one workspace |
| `var.landing_path` | string | `/Volumes/airbnb_renting/landing/raw` | Upload location (shared) |
| `var.warehouse_id` | lookup | Serverless Starter Warehouse | Dashboard warehouse |
| `var.wheel_path` | string | `${workspace.root_path}/artifacts/.internal/*.whl` | Wheel installed on the pipeline |
| `scenarios` (pipeline conf) | JSON string | cap 30 n/yr (whole_unit), 50%, 70% | Airbnb occupancy scenarios |
| `nights_per_month` | int | 30 | Nightly → monthly conversion |
| `low_sample_threshold` | int | 5 | `low_sample` flag when n < threshold on either side |
| Bounds (`config.py`) | decimal | Airbnb 10–5000 €/night; rent 100–10000 €/month | Quarantine thresholds, calibrated in scratch (step 3) |

---

## Security Considerations

- No secrets in the repo: local OAuth profile in `~/.databrickscfg`; CI uses GitHub encrypted secrets; least privilege SP (workspace user, `USE`/`MODIFY` on the catalog).
- Raw data is public listing data; free-text descriptions stay in bronze and aren't propagated (data minimisation).
- Job failure notifications go to `${workspace.current_user.userName}`, so no email address is hardcoded in the repo.
- Branch protection: `main` deploys to prod only through reviewed PRs.

---

## Observability

| Aspect | Implementation |
|--------|----------------|
| Logging | SDP event log (per-update flow progress, errors) |
| Metrics | Expectation pass/fail counts from the event log (`flow_progress.data_quality.expectations`), read in `scratch/03` |
| Data health | Quarantine row counts by rule; `pc4_source` split; `pc4_geo_mismatch` rate; conservation check |
| Alerts | Job `email_notifications.on_failure` to the deploying user |

---

## Pipeline Architecture

### DAG Diagram

```text
landing/raw/airbnb ──AL──▶ bronze.airbnb_raw ──▶ airbnb_parsed ──▶ silver.airbnb_listings ───┐
landing/raw/geo    ──────▶ bronze.postcodes_geo ─┘ (static)    └─▶ silver.airbnb_quarantine  │
landing/raw/rentals──AL──▶ bronze.rentals_raw ──▶ rentals_parsed ─▶ silver.rentals_listings ──┤
                                                               └─▶ silver.rentals_quarantine  │
                                         silver.dq_uniqueness ◀─(both listings)               │
gold.revenue_per_property ◀──────────────────────────────────────────────────────────────────┘
        └─▶ gold.revenue_per_postcode ─▶ gold.investment_comparison ◀─ postcodes_geo (centroids)
```

### Partition Strategy

| Table | Partition Key | Granularity | Rationale |
|-------|---------------|-------------|-----------|
| All | None | n/a | < 100 MB total; partitioning or clustering would add files, not speed. Mention liquid clustering on `pc4` as the at-scale answer |

### Incremental Strategy

| Model | Strategy | Key Column | Lookback |
|-------|----------|------------|----------|
| Bronze | Auto Loader (file-level checkpoint) | file path | n/a |
| Silver | Streaming append from bronze; Airbnb `dropDuplicates(listing_id)` (stateful) | `listing_id` | n/a |
| Gold | Materialized views (serverless incremental refresh when possible, else full recompute) | n/a | n/a |

### Schema Evolution Plan

| Change Type | Handling | Rollback |
|-------------|----------|----------|
| New source column | Auto Loader adds it to bronze (schema evolution); silver ignores it until mapped | n/a |
| Type change in silver | Requires a full refresh of the streaming table (documented) | Redeploy previous bundle version + full refresh |
| Rule/bound change | Config change → new rows only, unless full refresh (explicit) | Revert config |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|-------------------|
| Value rules (PC4, price/rent bounds, segment) | SDP `expect_all_or_drop` + quarantine | n/a | Row → quarantine |
| Soft completeness (bedrooms, review score) | SDP `expect_all` | n/a | Warn (metric only) |
| PK uniqueness | `dq_uniqueness` `expect_or_fail` | 0 duplicates | Fail update |
| Row conservation | `scratch/03` | difference = 0 | Investigate before sign-off |
| Backfill coverage | `scratch/03` | ≥ 99% Airbnb rows with PC4 | Investigate polygons/CRS |
| Unit tests + coverage | CI | ≥ 80% | Block PR |

---

## Deviations from DEFINE (✅ applied in DEFINE v1.1)

| DEFINE item | Change | Reason |
|-------------|--------|--------|
| AT-006 | Replace the `"€ 1.250,-"` example with real patterns (`"€ 1250,-"`, `"€ 1,-"` → below bound) | Thousands separator doesn't occur in the data |
| AT-022 | Duplicate check via `dq_uniqueness` MV + `expect_or_fail`, not a row expectation | Expectations are row-level (Decision 6) |
| SC-01 | Tables: 3 bronze, 5 silver (incl. 2 quarantine + `dq_uniqueness`), 3 gold | Decision 6 |
| SC-02 | Airbnb: `bronze = valid + quarantine + duplicates_removed` | 382 exact duplicates (Decision 8) |
| SC-05 + schema contract | Amsterdam = 82 PC4s from geo data (1011–1112), not 1000–1109 | Decision 10 |
| A-002 / A-004 / A-005 | Spatial: probe + shapely fallback; cap applies only to `whole_unit` (verified); bounds partly calibrated | Verification results |
| New AT-025 | Exact duplicate Airbnb rows → one silver row; duplicates counted | Decision 8 |
| New AT-026 | Valid raw zip outside Amsterdam (e.g. Amstelveen) → quarantined with `pc4_in_amsterdam` | Decision 10 |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-10-16 | design-agent | Initial version from DEFINE v1.0; verification of open items; 15 decisions |
| 1.1 | 2026-10-16 | iterate-agent | Bookkeeping only: DEFINE v1.1 now contains the deviations; links and status updated |
| 1.5 | 2026-10-16 | build (steps 4–7) | As-built deviations table (schema prefix removed, require_conf, pandas export, sync exclude, Decision 6 caveat, …) |
| 1.4 | 2026-10-16 | build (step 3) | Exploration findings frozen: GeoJSON as STRING, ANSI try_cast, AT-019 validated (fuzzing), Airbnb bound €5,000, near-dups kept, case-insensitive city scope, whitespace-aware null normalisation, PC4 1112 quirk |
| 1.3 | 2026-10-16 | iterate (user-confirmed) | Decision 13: branch protection over semantic-release (G12 cut); CD on push to main; CI validates prod; remove `prek autoupdate` and Dependabot |
| 1.2 | 2026-10-16 | build (step 1) | Probe results: catalog via SQL (REST fails on Free Edition), ST_* verified on serverless, databricks-connect 19.2; pipeline channel CURRENT first; `.python-version` relaxed to `3.12`; `.gitignore` no longer ignores `scratch/` |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_AIRBNB_RENTING.md`
