# DEFINE: Airbnb vs Kamernet Investment Pipeline

> A Lakeflow SDP medallion pipeline on Databricks Free Edition, deployed with a bundle. It turns messy Kamernet and Airbnb listings into per-postcode revenue comparisons that show where, and at what occupancy, short-term renting beats long-term renting in Amsterdam.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AIRBNB_RENTING |
| **Date** | 2026-10-16 |
| **Author** | define-agent |
| **Status** | Ready for Design |
| **Clarity Score** | 14/15 |
| **Source** | `.claude/sdd/features/BRAINSTORM_AIRBNB_RENTING.md` (v1.3) |
| **Brief** | https://github.com/revodatanl/assessment-rent-airbnb (Data Engineer, scope: Core + Levels 1–3) |

---

## Problem Statement

An investor buying property in Amsterdam can't tell, per postcode and property type, whether long-term renting (Kamernet, monthly rent) or short-term renting (Airbnb, nightly price) earns more. The only evidence is two inconsistent public datasets: 23% of Airbnb rows have no postcode, rents are free-text strings, and Kamernet covers the whole country. The pipeline must make them comparable, show where the answer flips with Airbnb occupancy, and do this in a way the candidate can defend line by line in a technical review.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Investor | Business decision-maker (dashboard consumer) | Can't compare nightly vs monthly income; doesn't know which postcodes are worth buying in, or how sensitive the answer is to occupancy |
| RevoData reviewers | Assessors of the submission | Need to see sound pipeline logic, explicit handling of data issues, and engineering practice (tests, CI/CD, IaC) in a clean, runnable repo |
| António (candidate) | Builder and presenter | Must understand and defend every design choice and line of code without AI help during the interview |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | G1: Ingest both sources into bronze, clean into silver, aggregate into gold. One Lakeflow SDP pipeline (Python, notebook source) following the medallion pattern |
| **MUST** | G2: Detect and route every bad record: expectations on silver, quarantine tables with the failed rule names; nothing is dropped silently |
| **MUST** | G3: Backfill missing or malformed Airbnb postcodes to PC4 via point-in-polygon on lat/long |
| **MUST** | G4: Revenue per property and per PC4 for **both** sources, segmented (`whole_unit` / `room`), with Airbnb occupancy scenarios and a breakeven occupancy |
| **MUST** | G5: Repo layout and outputs exactly as the brief lists: `scratch/`, `notebooks/`, `src/airbnb_renting/` (wheel), `tests/`, `docs/` + README, `data/output/` (parquet), `resources/` |
| **MUST** | G6: Everything deployed with a bundle (DABs) started from the revo-dabs template: catalog/schemas/volume, pipeline, job, dashboard |
| **MUST** | G7 (L1): GitHub Actions CI/CD (lint + tests on PR, deploy on main) and pre-commit hooks |
| **MUST** | G8 (L2): AI/BI dashboard of revenue per postcode for both sources; Excalidraw diagrams of the data flow and the CI/CD pipeline in `docs/diagrams/` |
| **SHOULD** | G9: `./scratch/03_validate_outputs` reads expectation metrics from the SDP event log and reconciles row counts across layers |
| **SHOULD** | G10: `prod` target deployed by CI from `main` (in addition to the developer's `dev` target) |
| **COULD** | G11: Dashboard map of PC4 centroids (only if AI/BI point maps work on Free Edition) |
| ~~COULD~~ | ~~G12: Keep template extras (semantic-release, …)~~ **Cut (v1.2):** semantic-release conflicts with branch protection on `main` (DESIGN Decision 13). ty, pydoclint and commitizen stay as part of G7 |

**Cut order if time runs short:** G12 → G11 → G10 → G9. G1–G8 make up the agreed scope.

---

## Success Criteria

**Pipeline correctness**
- [ ] SC-01: One `databricks bundle deploy -t dev` followed by a job run builds all tables (3 bronze; 5 silver: 2 listings, 2 quarantine, 1 `dq_uniqueness`; 3 gold) with **0 pipeline errors**.
- [ ] SC-02: **Row conservation** (difference = 0): Kamernet `count(bronze city=Amsterdam) = count(silver valid) + count(silver quarantine)`; Airbnb `count(bronze) = count(silver valid) + count(silver quarantine) + count(exact duplicates removed)`.
- [ ] SC-03: **≥ 99%** of Airbnb rows end with a non-null PC4 in silver (raw or `geo_backfill`); the rest are in `airbnb_quarantine` with a reason. Counts by `pc4_source` are reported in `scratch/03_validate_outputs`.
- [ ] SC-04: **100%** of Kamernet silver rows have a numeric `rent_eur`; rows that can't be parsed are in `rentals_quarantine`, none are dropped silently.
- [ ] SC-05: Silver `rentals_listings` and `airbnb_listings` contain only Amsterdam PC4s, defined by the postcode reference data (`gem_name = 'Amsterdam'`: 82 PC4s, 1011–1112), and only `segment ∈ {whole_unit, room}`.
- [ ] SC-06: Gold `investment_comparison` has exactly one row per (PC4, segment, scenario) with: Kamernet median rent, Airbnb median monthly revenue, `winner`, `breakeven_occupancy`, `n_kamernet`, `n_airbnb`, `low_sample`.
- [ ] SC-07: Parquet files for every gold table are present in `./data/output/` and readable with `pandas.read_parquet`.

**Engineering quality**
- [ ] SC-08: Unit tests cover every public function in `src/airbnb_renting/`, with **≥ 80% line coverage**, all passing locally and in CI.
- [ ] SC-09: Every expectation rule is defined once (`quality.py`) and is used by both the valid table and its quarantine table.
- [ ] SC-10: The CI workflow runs on every PR (pre-commit/lint + pytest + `bundle validate`) and fails the PR on any error; a merge to `main` deploys the `prod` target.
- [ ] SC-11: Pre-commit hooks (ruff format + ruff lint at minimum) pass on the whole repo.
- [ ] SC-12: The SDP notebooks contain **no parsing or business logic**: each table function only reads inputs, calls `airbnb_renting.*` functions and returns a DataFrame.

**Deliverables and documentation**
- [ ] SC-13: Each of the 31 traceability rows (D1–D8, O1–O7, R1–R7, S1–S8 in the brainstorm) maps to a concrete file or table. Checked at /ship.
- [ ] SC-14: A new reader can go from `git clone` to a populated dashboard following the README alone (profile → upload → deploy → run → export).
- [ ] SC-15: `docs/` contains: tooling rationale, decision log (≥ 15 decisions with rejected alternatives), data-issue and backfill strategy, revenue model and assumptions, limitations, diagrams (2 × `.excalidraw` + `.png`), AI session export.

---

## Acceptance Tests

### Parsing and cleaning (unit tests, `tests/`)

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Clean 4-digit zip | Airbnb `zipcode = "1053"` | `parse_pc4` | `1053` |
| AT-002 | PC6 with spaces / suffix | `"1018  DW"`, `"1079 HH Amsterdam"`, `"Nederland 1091 TS"` | `parse_pc4` | `1018`, `1079`, `1091` |
| AT-003 | Garbage zip | `"...."`, `"b"`, `"0"`, `" "`, `""`, `NULL` | `parse_pc4` | `NULL` (sent to backfill) |
| AT-004 | Typo-prefixed zip | `` "`1055tk" `` | `parse_pc4` | `1055` |
| AT-005 | Non-Amsterdam-looking zip | `"342HUIS"` | `parse_pc4` | `NULL` (not a 4-digit NL postcode); backfill decides |
| AT-006 | Rent string variants (the real patterns in the data) | `"€ 600,-  Utilities incl."`, `"€ 1250,-"`, `"€ 1,-"` | `parse_rent` | `600` + `utilities_included=true`; `1250` + `false`; `1` + `false` (later quarantined by `rent_in_bounds`). A defensive `"€ 1.250,-"` → `1250` case is kept even though the format doesn't occur |
| AT-007 | Area string | `"14 m2"`, `NULL`, `"NA"` | `parse_area_sqm` | `14`, `NULL`, `NULL` |
| AT-008 | Null variants | `"NA"`, `"-"`, `""`, `"   € 0  "` | `normalise_null` / `parse_eur` | `NULL`, `NULL`, `NULL`, `0` |
| AT-009 | Segment mapping | Airbnb `Entire home/apt`, `Private room`, `Shared room`; Kamernet `Apartment`, `Studio`, `Room`, `Anti-squat`, `Student residence` | `map_segment` | `whole_unit`, `room`, `room`; `whole_unit`, `whole_unit`, `room`, `NULL` (excluded), `room` |
| AT-010 | Mongo-style array id | `_id = ["5d2b…"]` | bronze → silver | scalar `listing_id = "5d2b…"` |

### Revenue model (unit tests)

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-011 | Scenario revenue | Price €150/night, `nights_per_month = 30`, occupancy 0.5 | `airbnb_monthly_revenue` | €2,250 |
| AT-012 | Legal-cap scenario | Price €150, whole unit, cap 30 nights/year | `airbnb_monthly_revenue` (cap scenario) | €150 × 30 / 12 = **€375** |
| AT-013 | Breakeven | Kamernet median €1,400; Airbnb median €150/night | `breakeven_occupancy` | 1400 / (150 × 30) = **0.311** |
| AT-014 | Breakeven impossible | Kamernet median €5,000; Airbnb median €100/night | `breakeven_occupancy` | 1.667 → flagged `airbnb_never_wins = true` |
| AT-015 | Low sample | PC4 with 3 Airbnb and 12 Kamernet listings in a segment | gold aggregation | `low_sample = true` (threshold n < 5 on either side) |
| AT-016 | One side missing | PC4 has Airbnb listings but no Kamernet in the segment | gold aggregation | Row present, Kamernet columns NULL, `winner = NULL` (not compared) |

### Pipeline / integration (validated on Databricks via `scratch/03_validate_outputs`)

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-017 | Geo backfill | Airbnb row with empty zip and lat/long inside a PC4 polygon | Pipeline update | Silver row has that PC4 and `pc4_source = 'geo_backfill'` |
| AT-018 | Backfill miss | Airbnb row with empty zip and lat/long outside every polygon | Pipeline update | Row in `airbnb_quarantine` with `failed_rules` containing `pc4_not_null` |
| AT-019 | Raw zip disagrees with geo | Raw zip `1053` but point lies in `1054` | Pipeline update | Raw zip kept (`pc4_source = 'raw'`); mismatch counted in validation notebook (decision for /design: trust raw or geo) |
| AT-020 | Non-Amsterdam rental | Kamernet row `city = Rotterdam` | Pipeline update | Present in bronze, absent from silver and quarantine (filtered, not a quality failure) |
| AT-021 | Re-run is idempotent | Pipeline already ran, no new files | Job re-run | Bronze/silver row counts unchanged (Auto Loader checkpoint) |
| AT-022 | Contract break | A rentals row with duplicate `_id` | Pipeline update | `silver.dq_uniqueness` (one row per silver table: `n_rows`, `n_distinct_ids`, `n_duplicates`) fails `expect_or_fail("unique_listing_id", "n_duplicates = 0")` and stops the update. Uniqueness is table-level, so it can't be a row expectation |
| AT-025 | Exact duplicate Airbnb rows | Two identical CSV rows (382 such duplicates exist in the real file) | Pipeline update / `transforms` unit test | One silver row with that `listing_id` (row hash); duplicates counted in `scratch/03_validate_outputs` |
| AT-026 | Valid zip outside Amsterdam | Airbnb row whose PC4 resolves to a neighbouring municipality (e.g. Amstelveen) | Pipeline update | Row in `airbnb_quarantine` with `failed_rules` containing `pc4_in_amsterdam` |
| AT-023 | Deploy from scratch | Empty Free Edition workspace + profile | `just deploy` (or `databricks bundle deploy -t dev`) | Catalog/schemas/volume, pipeline, job and dashboard exist |
| AT-024 | CI gate | PR with a failing unit test | GitHub Actions | Workflow red, merge blocked |

---

## Out of Scope

- **Level 4:** record-by-record streaming of `rentals.json` and near-real-time gold.
- **Level 5:** external postcode API UDF; `amsterdam_areas.geojson` choropleth. The point-in-polygon backfill *is* in scope as the core backfill strategy.
- Analysis of non-Amsterdam Kamernet data (kept in bronze only).
- Net yield / profitability: purchase price, mortgage, taxes, platform fees, cleaning and utility costs. Revenue is **gross**.
- SCD / history tracking, multi-workspace deployment, a separate prod workspace.
- Per-m² or per-bedroom normalisation.
- Non-engineering deliverables (business case, roadmap, storyline).

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Databricks **Free Edition**: serverless-only, single workspace, usage quotas | SDP `serverless: true`; dev/prod are targets in one workspace; no classic clusters (revo-dabs defaults may need stripping) |
| Technical | Must use **Lakeflow SDP + expectations + DABs** (Level 3) | No imperative `df.write` into managed tables; DQ via expectations |
| Technical | Pipeline implementation must live in **notebooks** in `./notebooks`; libraries in `./src/<package>` as a buildable package | Notebook source for SDP; wheel built by bundle artifacts |
| Technical | Raw files are uploaded **manually by the user** to `/Volumes/<catalog>/landing/raw/{airbnb,rentals,geo}/` | Upload contract documented in README; pipeline paths come from config |
| Timeline | Brief expects ~3–4 h; learning mode (CLAUDE.md) adds explanation time | Strict scope; cut order defined in Goals |
| Process | Learning mode: small steps, plan before code, a "my turn" gap per session | /build delivers in reviewable increments |
| Submission | Private GitHub repo, clean (own README, no redundant files), Markdown export of AI sessions | Repo-hygiene pass at /ship |
| Interview | No AI assistance during the interview; candidate must defend all code | Decision log + LEARNINGS.md are first-class deliverables |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | Repo root = bundle root (revo-dabs). `src/airbnb_renting/` (wheel), `notebooks/pipeline/{bronze,silver,gold}`, `notebooks/export_parquet`, `resources/*.yml`, `scratch/`, `tests/`, `docs/`, `data/output/`, `.github/workflows/` | Brief prescribes the folders; revo-dabs supplies tooling |
| **KB Domains** | Lakeflow SDP, Auto Loader, expectations; DABs (artifacts, pipelines, jobs, dashboards, schemas, volumes); Unity Catalog; Databricks spatial SQL / H3; AI/BI dashboards; GitHub Actions | Consult the `databricks-pipelines`, `databricks-dabs`, `databricks-unity-catalog`, `databricks-dbsql`, `databricks-aibi-dashboards` skills in /design |
| **IaC Impact** | **New resources:** UC catalog (or fallback), schemas `landing/bronze/silver/gold`, volume `landing.raw`, SDP pipeline, job, AI/BI dashboard; GitHub Actions secrets for a Free Edition token/SP | All in the bundle except the catalog, if DABs can't create it (open item) |

---

## Data Contract

### Source Inventory
| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| Kamernet rentals | JSON array file in UC volume | 46,722 records (8,094 Amsterdam), 69 MB | Static snapshot, crawled Jul–Aug 2019 | RevoData (assessment) |
| Airbnb listings | CSV file in UC volume | 9,913 rows, 575 KB | Static snapshot, date unknown (likely ≤ 2019) | RevoData (assessment) |
| Postcode polygons | GeoJSON in UC volume | 22 MB | Static reference | RevoData (assessment) |

### Schema Contract (silver, the stable interface for gold)

**`silver.airbnb_listings`**

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| `listing_id` | STRING | NOT NULL, UNIQUE (surrogate: sha2 hash of all source columns, since the CSV has no id; exact duplicates removed) | No |
| `pc4` | INT | NOT NULL, ∈ Amsterdam PC4 set from geo data | No |
| `pc4_source` | STRING | `raw` \| `geo_backfill` | No |
| `pc4_geo_mismatch` | BOOLEAN | NOT NULL (true when a valid raw zip and the geo PC4 disagree) | No |
| `latitude`, `longitude` | DOUBLE | NOT NULL, within Amsterdam bbox | No (listing location, not a person) |
| `room_type` | STRING | NOT NULL | No |
| `segment` | STRING | `whole_unit` \| `room` | No |
| `accommodates` | INT | > 0 | No |
| `bedrooms` | INT | nullable (warn) | No |
| `price_per_night_eur` | DECIMAL(10,2) | NOT NULL, within sane bounds (A-005) | No |
| `review_scores_value` | DOUBLE | nullable (warn) | No |
| `_ingested_at`, `_source_file` | TIMESTAMP, STRING | NOT NULL | No |

**`silver.rentals_listings`**

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| `listing_id` | STRING | NOT NULL, UNIQUE (from `_id[0]`) | No |
| `pc6`, `pc4` | STRING, INT | NOT NULL; pc4 ∈ Amsterdam PC4 set from geo data | No |
| `property_type` | STRING | NOT NULL | No |
| `segment` | STRING | `whole_unit` \| `room` | No |
| `rent_eur` | DECIMAL(10,2) | NOT NULL, within sane bounds (A-005) | No |
| `utilities_included` | BOOLEAN | NOT NULL | No |
| `additional_costs_eur`, `deposit_eur` | DECIMAL(10,2) | nullable | No |
| `area_sqm` | INT | nullable, > 0 | No |
| `furnish`, `match_capacity` | STRING | nullable | No |
| `crawled_at` | TIMESTAMP | NOT NULL | No |
| `_ingested_at`, `_source_file` | TIMESTAMP, STRING | NOT NULL | No |

**Quarantine tables** (`*_quarantine`): same columns as the bronze source plus `failed_rules ARRAY<STRING>`.

Descriptive free-text fields (`descriptionTranslated`, `pageTitle`, …) stay in bronze only. They contain no PII the analysis needs, and leaving them out keeps silver lean.

### Freshness SLAs
| Layer | Target | Measurement |
|-------|--------|-------------|
| Bronze → Gold | On demand (triggered job); a full run completes in **< 15 min** on serverless | Job run duration |
| Gold → `./data/output` | Manual `just export` after a run | File timestamps |

### Completeness Metrics
- Zero null primary keys (`listing_id`) in silver: enforced by `expect_or_fail`.
- Row conservation bronze(Amsterdam) = silver + quarantine (SC-02).
- ≥ 99% Airbnb PC4 coverage after backfill (SC-03).

### Lineage Requirements
- Table-level lineage through Unity Catalog (automatic for SDP tables). `_source_file` and `_ingested_at` carried from bronze to silver for row-level traceability.

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Free Edition supports serverless SDP with notebook sources, Auto Loader on UC volumes and multi-schema publishing | Fall back to a single schema with layer prefixes (`bronze_*`) | [ ] /design |
| A-002 | Native spatial SQL (`ST_*`, Public Preview, DBR ≥ 17.1) is available on Free Edition serverless with the pipeline on the PREVIEW channel | Fallback: shapely point-in-polygon in a pandas UDF (STRtree over the 474 polygons); same output columns | [~] /design: designed; probe in build step 1 |
| A-003 | Free Edition allows creating a catalog (via bundle or SQL) | Use the `workspace` catalog with the same schemas | [ ] /design |
| A-004 | Amsterdam limits whole-home holiday rental to 30 nights/year. The cap scenario applies to **`whole_unit` only**: renting a room in your own home is B&B with no night cap. Third-party guides report 15 nights in some central areas from 1 Apr 2026, after our 2019 data | Cap scenario relabelled "regulated low-occupancy" with the verified number | [x] /design (sources cited in DESIGN; to be cited in `docs/revenue_model.md`) |
| A-005 | Sane bounds: Airbnb €10–€5,000/night (user decision after step 3; was €2,000) (observed min €15, median €125, p99 €1,260, max €9,000); Kamernet rent €100–€10,000/month (observed min €1, 4 rows < €100, max €5,000) | Bounds recalibrated in `scratch/01`/`02` before building silver | [x] Calibrated in `scratch/01`/`02`: quarantines 3 Airbnb + 4 rental rows |
| A-006 | `nights_per_month = 30` and scenarios {30-night cap → 30/12 nights per month, 50%, 70%} are reasonable defaults | Only config changes; breakeven metric is scenario-independent anyway | [x] Agreed in brainstorm |
| A-007 | Low-sample threshold n < 5 listings per side, per PC4 × segment | Config value; changes flags, not logic | [x] `scratch/02`: flags 9 of 141 rental cells |
| A-008 | Airbnb lat/long are accurate enough for PC4 assignment (Airbnb randomises locations by up to ~150 m) | Some border listings land in a neighbouring PC4. Documented limitation; raw zip preferred when valid (AT-019) | [x] `scratch/01`: mismatches median 66 m, 85% ≤ 200 m → fuzzing confirmed |
| A-009 | Kamernet "Room" rent and Airbnb "Private room" price describe comparable units | Comparison within the `room` segment is weaker. Documented limitation | [x] Accepted in brainstorm |
| A-010 | The revo-dabs template can be adapted to serverless-only Free Edition within ~30 min | Hand-roll a minimal bundle following its conventions | [ ] /design |
| A-011 | Gross revenue is an acceptable proxy for "potential revenue" (brief wording) | Add a cost model; out of scope for now | [x] Brief says "potential revenue" |

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Specific user, decision, data issues and output |
| Users | 3 | Three personas with distinct needs (investor, reviewers, candidate) |
| Goals | 3 | MoSCoW-prioritised, mapped to levels, explicit cut order |
| Success | 3 | Numeric, testable criteria (row conservation, ≥ 99% backfill, ≥ 80% coverage) + 24 acceptance tests with hand-computed values |
| Scope | 2 | Boundaries explicit, but several platform capabilities (A-001–A-003, A-010) are unverified and could force scope changes |
| **Total** | **14/15** | |

---

## Open Questions

None block Design. These are **verification tasks for /design** (carried over from brainstorm open items):

1. SDP Python API: `from pyspark import pipelines as dp` vs `import dlt` on Free Edition (A-001).
2. Catalog creation and the DAB `catalogs` resource (A-003).
3. Spatial support: `ST_*` vs H3 vs shapely UDF (A-002).
4. Multi-schema publishing from one pipeline (A-001).
5. How a serverless pipeline installs the bundle-built wheel.
6. revo-dabs compatibility with serverless-only Free Edition (A-010).
7. Verify the Amsterdam 30-night rule and its applicability to rooms (A-004).
8. AI/BI point-map support (G11).
9. Local tests: pure local PySpark vs Databricks Connect.
10. AT-019 policy: when the raw zip and the geo-derived PC4 disagree, which wins? (Proposed default: trust a valid raw zip; record the mismatch.)

Deferred to /ship: keep or exclude `.claude/` + `CLAUDE.md` in the submitted repo.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-10-16 | define-agent | Initial version from BRAINSTORM v1.3 |
| 1.2 | 2026-10-16 | iterate-agent | G12 cut: branch protection chosen over semantic-release (user decision) |
| 1.1 | 2026-10-16 | iterate-agent | Applied DESIGN v1.0 "Deviations from DEFINE": AT-006 uses real rent patterns; AT-022 via `dq_uniqueness` MV; SC-01 table count; SC-02 counts exact Airbnb duplicates; SC-05 + schema contract use the geo-derived Amsterdam PC4 set (82, 1011–1112); `pc4_geo_mismatch` column; A-002/A-004/A-005 updated with verification results; new AT-025 (duplicates) and AT-026 (non-Amsterdam PC4) |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_AIRBNB_RENTING.md`
