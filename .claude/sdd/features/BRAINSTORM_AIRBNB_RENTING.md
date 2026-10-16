# BRAINSTORM: Airbnb vs Kamernet Investment Pipeline

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AIRBNB_RENTING |
| **Date** | 2026-10-16 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define (v1.3) |

---

## Initial Idea

**Raw Input:** Technical assessment for a Data Engineer role (https://github.com/revodatanl/assessment-rent-airbnb).
Deliver everything up to and including **Level 3 / Future-Proof**. Data is in `./data/`; it may go into a
Unity Catalog volume (to be created by Claude).

**Challenge summary:** As an investor buying Amsterdam property, determine which postcodes have investment
potential and whether long-term renting (Kamernet) or short-term renting (Airbnb) is more profitable.

**Scope = Core + Level 1 + Level 2 + Level 3:**

| Level | Items |
|-------|-------|
| Core | Ingest `rentals.json` + `airbnb.csv`; clean; identify missing/improper data + backfill strategy; revenue per property and per postcode for both sources; Medallion architecture |
| Core outputs | `./scratch` (exploration + validation), `./notebooks`, `./src/<pkg>`, `./tests`, README + `./docs` (the *why*), parquet in `./data/output`, job config in `./resources` |
| L1 Engineer | CI/CD that deploys the pipeline; tests in CI; pre-commit hooks |
| L2 Artist | Dashboard of revenue per postcode (rental + Airbnb); diagrams of data flow + CI/CD |
| L3 Future-Proof | Lakeflow Spark Declarative Pipelines (SDP); expectations for data quality; Declarative Automation Bundles (DABs) |

**Context Gathered:**
- Repo is empty apart from the initial commit, `CLAUDE.md` (learning mode) and `data/` (zipped).
- `data/airbnb.zip` → `airbnb.csv` (9,913 rows, 575 KB); `data/rentals.zip` → `rentals.json` (46,722 records, 69 MB, a single JSON array).
- `data/geo/post_codes.zip` → `post_codes.geojson` (22 MB); `data/geo/amsterdam_areas.zip` → `amsterdam_areas.geojson`.
- Databricks CLI v1.21.0 installed; existing profiles are employer/personal → a **new Free Edition profile** will be created.
- Reviewers explicitly expect data issues and creative workarounds, and will ask the candidate to defend every choice.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `src/airbnb_renting/` (library), `src/pipelines/` or `resources/` (SDP + job YAML), `notebooks/`, `scratch/`, `tests/` | Follow the assessment's prescribed layout + the RevoData DAB template |
| Relevant KB Domains | Lakeflow SDP, DABs, Unity Catalog, Auto Loader, Databricks spatial SQL, AI/BI dashboards | Consult the `databricks-pipelines`, `databricks-dabs` and `databricks-unity-catalog` skills at design time |
| IaC Patterns | DABs (`databricks.yml` + `resources/*.yml`); GitHub Actions | The bundle owns schemas/volume/pipeline/job/dashboard |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Which Databricks workspace should the bundle target? | **Free Edition (new profile)** | Serverless-only compute; SDP must be serverless; single workspace → dev/prod are *targets* not workspaces; a new CLI profile is needed |
| 2 | How should Airbnb revenue be modelled against Kamernet monthly rent? | **Occupancy scenarios** (parameterised, e.g. 30-night legal cap / 50% / 70%) | Occupancy lives in config; gold exposes one row per scenario + a breakeven occupancy |
| 3 | How should the comparison be made fair? | **Segment by property type** | Common `segment` column: `whole_unit` (Airbnb Entire home/apt ↔ Kamernet Apartment/Studio) and `room` (Airbnb Private/Shared room ↔ Kamernet Room). Comparisons happen within the segment, per PC4 |
| 4 | Backfill strategy for missing/invalid Airbnb postcodes? | **Point-in-polygon** against `post_codes.geojson` | Spatial join on lat/long; also repairs malformed zips; covers a Level 5 item for free. Spatial support on Free Edition must be verified |
| 5 | Ground truth or sample outputs available? | **None, inputs only** | Test fixtures are hand-picked slices of the real files with hand-calculated expected values |
| 6 | (Validation 1 change) Who handles infra vs data upload? | **Claude creates catalog + schemas + volume; the user unzips and uploads files** | Bundle/setup script owns infra; landing layout must be documented precisely (see below) |

**Decided without asking (defaults, open to challenge):** the join grain is **PC4** (4-digit postcode). It is the only grain both sources share: Airbnb is mostly 4-digit, Kamernet is 6-character.

---

## Deliverables Traceability (added v1.1)

> Every deliverable in the brief, mapped to where it is satisfied. /define must turn each row into an acceptance criterion.

### Core pipeline (brief: "Engineering Deliverables")

| # | Brief requirement | How / where it is met |
|---|-------------------|-----------------------|
| D1 | Ingest Kamernet `rentals.json` | Bronze `rentals_raw` (Auto Loader, `multiLine` JSON) |
| D2 | Ingest Airbnb `airbnb.csv` | Bronze `airbnb_raw` (Auto Loader CSV, all columns as string) |
| D3 | Clean both datasets | Silver `rentals_listings` and `airbnb_listings` via `src/` parsing functions |
| D4 | Identify missing/improper data | Silver expectations + `*_quarantine` tables + profiling in `./scratch` |
| D5 | Backfill strategy (implement if time permits) | Strategy in `docs/`; **implemented** as point-in-polygon PC4 backfill (`pc4_source` flag) |
| D6 | Potential revenue **per property, both sources** | Gold `revenue_per_property`: Kamernet = monthly `rent_eur`; Airbnb = `price × 30 × occupancy` per scenario |
| D7 | Potential revenue **per postal code, both sources** | Gold `revenue_per_postcode` + `investment_comparison` |
| D8 | Medallion architecture | Bronze / silver / gold schemas in one SDP pipeline |

### Expected repository outputs

| # | Brief wording | Path | Content |
|---|---------------|------|---------|
| O1 | "Exploratory notebooks with data validation checks" | `./scratch/` | `01_explore_airbnb`, `02_explore_rentals`, `03_validate_outputs` (row counts per layer, quarantine reasons, backfill hit-rate, sanity checks on gold) |
| O2 | "Notebooks containing your pipeline implementation" | `./notebooks/` | **SDP source as Databricks notebooks** (`bronze`, `silver`, `gold`) + `export_parquet` job notebook. Notebooks stay thin and import the package |
| O3 | "Libraries or buildable packages" | `./src/airbnb_renting/` | Package built as a **wheel** (`uv build`, bundle `artifacts`) and installed on the pipeline/job |
| O4 | "Unit tests for your pipeline" | `./tests/` | pytest on every `src/` function, with fixtures from the profiled edge cases |
| O5 | "Documentation… explain the why, not the how" | `README.md` + `./docs/` | README = what + build/run; `docs/` = decisions (ADR-style), data issues and backfill, revenue model and assumptions, limitations, **tooling choice rationale**, diagrams |
| O6 | "Export of datasets… as Parquet" | `./data/output/` | Gold (+ silver) tables exported by a job task to the volume, then `databricks fs cp` into the repo (scripted in `justfile`) |
| O7 | "Pipeline job configurations" | `./resources/` | Bundle YAML: schemas, volume, pipeline, job, dashboard |

### Repository guidance + review

| # | Brief wording | How it is met |
|---|---------------|---------------|
| R1 | "Explain your choice" of tooling | `docs/` section: why Databricks Free Edition + SDP + DABs (vs local PySpark) |
| R2 | Private Git repo shared with reviewers | GitHub private repo; reviewers invited at ship time |
| R3 | "Clean repository: remove any redundant files, replace our README" | Own README; template leftovers removed; decision on `.claude/` + `CLAUDE.md` made at /ship (see open items) |
| R4 | "Clear instructions for building and running" | README quick-start: profile setup → upload files → `just deploy` → `just run` → `just export` |
| R5 | Start from RevoData's template (recommended) | **Initialise from `revo-dabs`** (`databricks bundle init https://github.com/revodatanl/revo-dabs`), tailored: GitHub CI/CD, no example jobs |
| R6 | Markdown export of AI session(s) | `docs/ai-sessions/*.md` |
| R7 | Be able to explain and defend all code | Learning mode (CLAUDE.md) + `LEARNINGS.md` |

### Stretch goals in scope

| # | Level | Item | How |
|---|-------|------|-----|
| S1 | L1 | CI/CD deploys the pipeline | GitHub Actions: `bundle validate` on PR, `bundle deploy -t prod` on main |
| S2 | L1 | Tests run in CI/CD | pytest job on every PR |
| S3 | L1 | Pre-commit hooks | From revo-dabs (ruff, ty, …) |
| S4 | L2 | Dashboard of revenue per postcode (rental **and** Airbnb) | AI/BI dashboard in the bundle |
| S5 | L2 | Diagrams: data flow **and** CI/CD | Generated with the **`/agentspec:excalidraw-diagram`** skill into `docs/diagrams/`: `data_flow.excalidraw` + `.png`, `cicd_pipeline.excalidraw` + `.png`. PNGs embedded in README/docs (GitHub doesn't render `.excalidraw`). Created in /build **after** /design locks the architecture |
| S6 | L3 | Pipelines built with SDP | Python SDP notebooks |
| S7 | L3 | Expectations for data quality | `@dp.expect*` on silver + quarantine pattern |
| S8 | L3 | Deploy with DABs | `databricks.yml` + `./resources` |

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | `data/airbnb.zip` → `airbnb.csv` | 9,913 rows | Columns: zipcode, latitude, longitude, room_type, accommodates, bedrooms, price, review_scores_value |
| Input files | `data/rentals.zip` → `rentals.json` | 46,722 records (8,094 Amsterdam) | 36 keys, all strings; Mongo-style `_id`/timestamps as single-element arrays |
| Input files | `data/geo/post_codes.zip` → `post_codes.geojson` | 22 MB | Postcode polygons for the backfill |
| Input files | `data/geo/amsterdam_areas.zip` → `amsterdam_areas.geojson` | 244 KB | Area polygons (optional in viz; cut by YAGNI) |
| Output examples | N/A | 0 | None provided |
| Ground truth | N/A | 0 | None provided |
| Related code | N/A | 0 | Greenfield repo |

### Profiling findings (drive cleaning + expectations)

**Airbnb**

| Issue | Evidence | Planned handling |
|-------|----------|------------------|
| Missing zipcode | 2,253 / 9,913 empty (~23%) | Point-in-polygon backfill; `pc4_source` = `raw` / `geo_backfill` |
| Malformed zipcode | `....`, `b`, `0`, `' '`, `` `1055tk ``, `342HUIS`, `Nederland 1091 TS`, `1079 HH Amsterdam`, `1018  DW`, 17-char strings | Regex-extract `\d{4}`; if invalid → treat as missing → geo backfill |
| Mixed granularity | 4-char (3,120), 6/7-char PC6 (4,532) | Normalise to PC4 |
| Missing bedrooms | 14 | Nullable; not needed for revenue |
| Missing review score | 1,711 | Nullable; informational only |
| Price is nightly | integer EUR/night | Monthly revenue = price × 30 × occupancy scenario |

**Kamernet (rentals)**

| Issue | Evidence | Planned handling |
|-------|----------|------------------|
| Whole-country data | Only 8,094 / 46,722 are `city = Amsterdam` | Keep all in bronze; filter in silver |
| Rent is a formatted string | `"€ 600,-  Utilities incl."` | Parse to `rent_eur` (Dutch thousands separator `.`); keep `utilities_included` flag |
| Area is a string | `"14 m2"` | Parse to `area_sqm` int |
| Inconsistent nulls | `NA`, `-`, `''`, `null`, whitespace-padded `€ 0` | Normalise to NULL / numeric |
| Arrays for scalars | `_id`, `crawledAt`, `firstSeenAt`, ... are 1-element arrays | `element_at(...,1)` |
| Property types | Room 5,203 / Apartment 2,290 / Studio 597 / Anti-squat 3 / Student residence 1 | Map to segment; anti-squat → excluded (not a market rent) |
| Duplicates | 0 duplicate `_id` | Still enforce uniqueness expectation |
| Time period | Crawled July–Aug 2019; Airbnb snapshot likely older | Document as a known limitation |

**How samples will be used:**
- Test fixtures: tiny CSV/JSON slices containing every malformed zip and rent format above.
- Expectations: thresholds derived from the profile (e.g. price > 0, rent between sane bounds, PC4 in 1000–1109 range for Amsterdam).
- `scratch/` notebook reproduces this profile as the documented "validation checks" deliverable.

---

## Approaches Explored

### Approach A: Python SDP pipeline + tested library ("thin pipeline, fat library") ⭐ Recommended → SELECTED

**Description:** A single Lakeflow SDP pipeline (Python). Bronze = Auto Loader streaming tables reading a UC volume.
Silver = cleaned tables with expectations and quarantine tables for failing rows. Gold = materialized views.
All parsing/business logic lives in pure PySpark functions in `src/airbnb_renting/`, unit tested with pytest
and a local SparkSession. The DAB deploys schemas, volume, pipeline, a job (pipeline → parquet export → dashboard) and an AI/BI dashboard.
GitHub Actions: lint + tests on PR, `bundle validate/deploy` on main.

**Pros:**
- Natively covers Levels 1–3 (SDP, expectations, DABs, CI/CD).
- Logic is testable without a workspace; the clear separation is easy to defend in review.
- Auto Loader gives incremental ingestion and a natural path to Level 4 later.

**Cons:**
- SDP decorated functions can't be run locally → requires discipline to keep them thin.
- Free Edition constraints (serverless-only, quotas) must be verified.

**Why Recommended:** It scores best against the rubric the reviewers implied (pipeline logic, handling data issues, defensibility) while hitting every Level 1–3 item with native tooling.

---

### Approach B: Notebooks + Lakeflow Job + custom DQ (e.g. DQX)

**Pros:** Familiar imperative PySpark; full control.
**Cons:** Does not satisfy Level 3 "build with SDP"; checkpointing/incremental logic must be hand-written.

---

### Approach C: SQL-only SDP

**Pros:** Very readable, especially gold.
**Cons:** String parsing (rent, messy zips) is awkward and hard to unit test in SQL; the brief asks for `src/` libraries + `tests/`.
*Compromise considered:* SQL for gold MVs only. Rejected in favour of a single language/test story.

---

## Data Engineering Context

### Source Systems
| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| Kamernet rentals | JSON array file (UC volume) | 46,722 records / 69 MB | Static snapshot (2019) |
| Airbnb listings | CSV file (UC volume) | 9,913 rows / 575 KB | Static snapshot (date unknown, likely ≤ 2019) |
| Postcode polygons | GeoJSON (UC volume) | 22 MB | Static reference |

### Unity Catalog layout (created by Claude: bundle and/or idempotent setup script)

```
catalog:  airbnb_renting            (fallback: `workspace` if Free Edition disallows new catalogs)
schemas:  landing  · bronze · silver · gold
volume:   airbnb_renting.landing.raw
```

**Upload contract for the user** (Auto Loader reads directories, so one folder per source):

```
/Volumes/airbnb_renting/landing/raw/
├── airbnb/airbnb.csv
├── rentals/rentals.json
└── geo/post_codes.geojson
    geo/amsterdam_areas.geojson
```

### Data Flow Sketch

```text
landing volume ──Auto Loader──▶ BRONZE (as-is + _ingested_at, _source_file)
  airbnb_raw · rentals_raw (all NL) · postcodes_geo
        │  parse + expectations (failures → *_quarantine)
        ▼
SILVER
  airbnb_listings  : pc4 (clean zip ∪ point-in-polygon backfill), pc4_source, segment, price_per_night
  rentals_listings : Amsterdam only, rent_eur, utilities_included, area_sqm, segment, pc4, pc6
        ▼
GOLD (materialized views)
  revenue_per_property  : both sources: Kamernet rent_eur, Airbnb price × 30 × occupancy (per scenario)
  revenue_per_postcode  : pc4 × segment × source × scenario → median, avg, n_listings
  investment_comparison : pc4 × segment → kamernet vs airbnb per scenario, winner,
                          breakeven_occupancy, low_sample flag
        ├─▶ job task: export gold → parquet in volume → user `databricks fs cp` → ./data/output
        └─▶ AI/BI dashboard (revenue per postcode, both sources)
```

### Key Data Questions Explored
| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Expected data volume? | < 100 MB total, static | Serverless is more than enough; no tuning needed. Optimise for clarity |
| 2 | Freshness SLA? | None (batch snapshots) | Triggered pipeline; Auto Loader for incrementality, not latency |
| 3 | Who consumes the output? | Investor (dashboard) + reviewers (parquet, code) | Gold modelled per PC4 × segment × scenario; headline = breakeven occupancy |

---

## Lakeflow SDP Pipeline (added v1.2)

> Level 3 core. One SDP pipeline owns every bronze/silver/gold table. Plain Spark code never writes to these tables.

### Pipeline resource (`./resources/airbnb_renting.pipeline.yml`)

| Setting | Value | Why |
|---------|-------|-----|
| Source code | Notebooks in `./notebooks/pipeline/` (`bronze`, `silver`, `gold`) | Brief wants pipeline implementation in notebooks; SDP accepts notebooks as source |
| Compute | `serverless: true` | Free Edition is serverless-only |
| Mode | Triggered (not continuous) | Static snapshots; continuous would burn quota for nothing |
| Catalog / schemas | `airbnb_renting`; tables fully qualified into `bronze` / `silver` / `gold` | One pipeline, medallion schemas (multi-schema publishing to verify) |
| `configuration` | `landing_path`, `occupancy_scenarios` (e.g. `cap_30_nights,0.5,0.7`), `nights_per_month=30` | Assumptions are config, not code; changeable per target without redeploying code |
| Dependencies | The `airbnb_renting` wheel | Logic lives in the tested package (decision 15) |
| `development` | `true` on dev target, `false` on prod | Dev mode: faster restarts, no retries; prod: retries on failure |

### Tables and their SDP type

| Layer | Table | SDP type | Reason |
|-------|-------|----------|--------|
| Bronze | `airbnb_raw` | Streaming table (Auto Loader CSV, all strings) | Append-only file source; incremental and idempotent; Level-4-ready |
| Bronze | `rentals_raw` | Streaming table (Auto Loader JSON, `multiLine`) | Same; keeps all cities |
| Bronze | `postcodes_geo` | Materialized view (batch read of GeoJSON, explode `features`) | Reference data; full recompute if the file changes |
| Silver | `airbnb_listings` (+ `airbnb_quarantine`) | Streaming table; stream-static join to postcodes for the backfill | Row-level cleaning of an append-only stream |
| Silver | `rentals_listings` (+ `rentals_quarantine`) | Streaming table | Same |
| Gold | `revenue_per_property`, `revenue_per_postcode`, `investment_comparison` | Materialized views | Aggregations must be recomputed correctly when inputs change, which an append-only streaming table can't do |

### Data quality with expectations

| Expectation level | SDP decorator | Used for |
|-------------------|---------------|----------|
| Warn (keep row, log metric) | `@dp.expect` | Soft signals: missing `review_scores_value`, missing `bedrooms` |
| Drop + quarantine | `@dp.expect_all_or_drop(rules)` on the valid table; **same `rules` dict inverted** in the quarantine table, with a `failed_rules` column | Unparseable price/rent, PC4 still null after backfill, rent outside sane bounds, non-Amsterdam PC4 |
| Fail pipeline | `@dp.expect_or_fail` | Contract breaks: missing primary key, duplicate `_id` |

- Rules live in **one Python dict per table** in the wheel (`airbnb_renting/quality.py`), so the valid and quarantine tables can't drift apart and the rules are unit-testable.
- Expectation pass/fail metrics are read from the **pipeline event log** in `./scratch/03_validate_outputs` (deliverable O1).

### Job (`./resources/airbnb_renting.job.yml`)

```text
pipeline_task (refresh SDP pipeline) ──▶ notebook_task: notebooks/export_parquet ──▶ dashboard refresh
```

### SDP gotchas to remember
- SDP notebooks **can't be run cell-by-cell** like normal notebooks: `@dp.table` only works inside a pipeline update. That's another reason logic goes in the wheel.
- Don't call `.collect()`/`.count()` or write with `df.write` inside a pipeline; table functions must just *return* a DataFrame.
- Auto Loader on a **single JSON array file** needs `multiLine=true`. A 69 MB single file isn't splittable, which is fine at this size and worth mentioning in the interview.
- Python API naming: `from pyspark import pipelines as dp` (new) vs `import dlt` (legacy). Verify in /design (open item 1).

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A: Python SDP + tested library, all Python |
| **User Confirmation** | 2026-10-16 (Validation 1), with the change: Claude creates catalog/schemas/volume, user unzips + uploads files |
| **Reasoning** | Native coverage of Levels 1–3; testable logic; most defensible in review |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Databricks Free Edition with a new CLI profile | Personal, shareable, no employer-workspace risk | Existing `dev_AR` / employer profiles |
| 2 | Lakeflow SDP (Python) + DABs | Level 3 requirement; declarative DQ via expectations | Notebooks + Jobs (B); SQL-only (C) |
| 3 | Logic in `src/airbnb_renting/` as pure Column/DataFrame functions | Unit-testable locally; pipeline files stay thin | Logic inside `@table` functions |
| 4 | PC4 as the comparison grain | The only grain both sources share | PC6 (Airbnb mostly lacks it) |
| 5 | Segment by property type (`whole_unit` / `room`) | Avoids comparing Kamernet rooms with Airbnb entire homes | Per-m²/bedroom normalisation (missing fields on each side); naïve averages |
| 6 | Occupancy as config-driven scenarios (incl. the Amsterdam 30-night cap for whole homes) | Conclusion depends on occupancy; scenarios + breakeven make it explicit | Single fixed occupancy; 100% occupancy |
| 7 | Breakeven occupancy as headline metric | Scenario-independent answer to "when does Airbnb win?" | Only winner per fixed scenario |
| 8 | Median (with n_listings + low-sample flag) for postcode aggregates | Skewed prices/outliers; small-n postcodes would mislead | Mean only |
| 9 | Point-in-polygon backfill of Airbnb PC4 from lat/long | Authoritative, also fixes malformed zips; ~23% of rows recovered | Nearest neighbour; dropping rows |
| 10 | Expectations + quarantine tables (not silent drops) | Bad data stays inspectable; good talking point | `expect_or_drop` everywhere |
| 11 | Claude provisions catalog/schemas/volume; the user uploads unzipped files | The user's explicit choice | Scripted upload of zips |
| 12 | dev + prod as bundle *targets* in one workspace | Free Edition = one workspace; still demonstrates promotion | Separate workspaces |
| 13 | *(v1.1)* Initialise the repo from **revo-dabs** (tailored: GitHub CI/CD, no example jobs) | Recommended by the brief; matches reviewers' own conventions (uv, ruff, ty, pre-commit, just) | Hand-rolled bundle layout |
| 14 | *(v1.1)* SDP source code as **Databricks notebooks in `./notebooks/`** | Brief asks for "notebooks containing your pipeline implementation"; notebooks stay thin, logic stays in the wheel | Plain `.py` pipeline files (contradicted the brief) |
| 15 | *(v1.1)* `src/airbnb_renting` built as a **wheel** and installed on pipeline + job | Brief asks for a "buildable package"; one artifact tested in CI and run in Databricks | Importing `src/` via sys.path / workspace files |
| 16 | *(v1.1)* `revenue_per_property` covers **both sources** | Brief: revenue "per property… for both sources" | Airbnb-only per-property table |
| 17 | *(v1.3)* Diagrams (data flow + CI/CD) made with the `/agentspec:excalidraw-diagram` skill, stored in `docs/diagrams/` as `.excalidraw` source + rendered `.png` | User's choice; editable source plus a GitHub-viewable image. Data-flow diagram should show concrete evidence (real messy zips/rent strings → parsed values, expectation outcomes), not just labelled boxes | Mermaid in markdown (v1.1 proposal) |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| Level 4: stream `rentals.json` record-by-record, near-real-time gold | Beyond the agreed Level-3 target | Yes (Auto Loader + MVs make this incremental) |
| External postcode API UDF (opendatasoft) | GeoJSON is authoritative and offline | Yes |
| PC4 polygon choropleth with `amsterdam_areas.geojson` | AI/BI support for custom polygons is uncertain; a centroid point map is sufficient | Yes |
| SCD / history tracking | Static snapshots, no history to track | Yes |
| Separate prod workspace | Free Edition has one workspace | Yes |
| Non-Amsterdam Kamernet data beyond bronze | Out of the investment question's scope | Yes (bronze keeps it) |
| Per-m² / per-bedroom normalisation | Fields missing on one side each; heavy assumptions | Yes, as a secondary metric |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Architecture concept (Approach A vs B vs C) | ✅ | Approve A; Claude creates catalog + volume, user uploads files | Yes: added upload contract + catalog fallback |
| Data flow, layers, gold metrics, YAGNI cuts | ✅ | Approved as proposed | No |

---

## Open Items to Verify in /design (uncertain, do not assume)

1. **SDP Python API:** `from pyspark import pipelines as dp` (new) vs `import dlt` (legacy). Which is current on Free Edition serverless?
2. **Catalog creation:** can Free Edition create a new catalog, and is there a DAB `catalogs` resource? Otherwise use a setup script or the `workspace` catalog.
3. **Spatial SQL:** are native `ST_*` functions (e.g. `ST_Contains`, `ST_GeomFromGeoJSON`) available on Free Edition serverless? Fallback: H3 functions, or shapely in a pandas UDF.
4. **Multi-schema publishing:** can one SDP pipeline write to bronze/silver/gold schemas via fully qualified names?
5. **Parquet export:** where it runs (job task writing to the volume) and how it lands in `./data/output` (`databricks fs cp`).
6. **Amsterdam 30-night cap:** confirm the current rule before citing it in docs/interview.
7. **AI/BI dashboard maps:** point map support for PC4 centroids.
8. **Local testing:** pytest with local PySpark vs Databricks Connect (serverless) for the library tests.
9. *(v1.1)* **revo-dabs vs Free Edition:** the template targets DBR 16.4 LTS and asks for `cloud_provider` (azure/aws). Check that the generated bundle works serverless-only, and strip what doesn't.
10. *(v1.1)* **Wheel on serverless SDP:** how a serverless pipeline installs a bundle-built wheel (pipeline `environment.dependencies` vs `%pip install` in the notebook).
11. *(v1.1)* **Repo hygiene:** keep or exclude `.claude/` (SDD docs = process evidence) and `CLAUDE.md` in the submitted repo. Decide at /ship.

---

## Suggested Requirements for /define

### Problem Statement (Draft)
An Amsterdam property investor needs to know, per postcode and property type, whether long-term renting (Kamernet) or short-term renting (Airbnb) yields more monthly revenue, and at what Airbnb occupancy the answer flips. The answer must come from messy, incomplete public listing data.

### Target Users (Draft)
| User | Pain Point |
|------|------------|
| Investor (business) | Can't compare nightly Airbnb prices with monthly rents; doesn't know which postcodes are worth buying in |
| Assessment reviewers (RevoData) | Need to judge pipeline logic, data-issue handling, engineering practices and defensibility |
| The candidate (António) | Must understand and defend every line without AI help during the interview |

### Success Criteria (Draft)
> Every row of the **Deliverables Traceability** section (D1–D8, O1–O7, R1–R7, S1–S8) is a criterion. The list below highlights the key ones.

- [ ] Repo initialised from revo-dabs; layout has `scratch/`, `notebooks/`, `src/airbnb_renting/`, `tests/`, `docs/`, `data/output/`, `resources/`
- [ ] Pipeline implementation lives in **notebooks** under `./notebooks/`; `src/airbnb_renting` builds into a wheel used by the pipeline
- [ ] `./scratch` notebooks contain exploration **and** data validation checks
- [ ] `revenue_per_property` and `revenue_per_postcode` cover **both** Kamernet and Airbnb
- [ ] `docs/` explains the tooling choice and every key decision (the why, not the how)
- [ ] AI session export present in `docs/ai-sessions/`
- [ ] Bronze/silver/gold tables built by one SDP pipeline deployed via `databricks bundle deploy` on Free Edition
- [ ] Every malformed/missing Airbnb postcode is either backfilled (point-in-polygon) or quarantined with a reason, and both counts are reported
- [ ] Expectations defined on silver; quarantine tables populated for failing rows
- [ ] Gold `investment_comparison` gives, per PC4 × segment, Kamernet median rent, Airbnb median revenue per scenario, winner, breakeven occupancy, n_listings, low-sample flag
- [ ] Parquet exports of gold present in `./data/output`
- [ ] Unit tests for every parsing/business function (zip parsing, rent parsing, segment mapping, revenue scenarios, breakeven), run in CI
- [ ] Pre-commit hooks (ruff format + lint, at minimum) and GitHub Actions CI (test on PR, deploy on main)
- [ ] AI/BI dashboard of revenue per postcode for both sources
- [ ] Diagrams: data flow + CI/CD, produced with `/agentspec:excalidraw-diagram`, saved in `docs/diagrams/` as `.excalidraw` + `.png`, and embedded in README/docs
- [ ] README with build/run instructions; `./docs` explains the *why* (decisions above, limitations)

### Constraints Identified
- Databricks Free Edition: serverless-only, single workspace, quotas.
- Expected effort is ~3–4 h by the brief; the candidate must be able to explain everything (learning mode).
- Private Git repo; clean repo (remove redundant files, own README).
- Must submit a Markdown export of the AI session(s).
- Raw files are uploaded manually by the user to the documented volume layout.

### Out of Scope (Confirmed)
- Levels 4 and 5 (except the point-in-polygon backfill, which is part of the core backfill strategy)
- External APIs, SCD/history, multi-workspace deployment, non-Amsterdam analysis

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 5 discovery + 2 validations |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 7 |
| Validations Completed | 2 |
| Duration | ~1 session |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-10-16 | brainstorm-agent | Initial version |
| 1.1 | 2026-10-16 | iterate-agent | User flagged missing deliverables. Added Deliverables Traceability matrix; pipeline moved to `./notebooks` (was `.py` files, which contradicted the brief); `src/` built as a wheel; per-property revenue covers both sources; revo-dabs template adopted; tooling rationale, AI-session export and repo-hygiene items added; open items 9–11 |
| 1.2 | 2026-10-16 | iterate-agent | User flagged SDP as under-specified. Added **Lakeflow SDP Pipeline** section: pipeline resource settings, table-by-table SDP type (streaming table vs MV), expectations levels + shared-rules quarantine pattern, event-log DQ metrics, job DAG, SDP gotchas |
| 1.3 | 2026-10-16 | iterate-agent | Diagrams (S5) to be produced with `/agentspec:excalidraw-diagram` into `docs/diagrams/` (`.excalidraw` + `.png`); replaces the Mermaid proposal; decision 17 added |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_AIRBNB_RENTING.md`
