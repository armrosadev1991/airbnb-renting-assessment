# Decision log

Each decision lists what was chosen, why, and what was rejected. The full working documents
(brainstorm, requirements, design) are in [`.claude/sdd/features/`](../.claude/sdd/features/).

## Data and modelling

| # | Decision | Why | Rejected |
|---|----------|-----|----------|
| M1 | **Compare at PC4** (4-digit postcode) | The only grain both sources share: Airbnb zips are mostly 4-digit or missing, Kamernet's are 6-character | PC6: most Airbnb rows don't have one |
| M2 | **Segment by property type** (`whole_unit` / `room`) | Kamernet is mostly rooms, Airbnb mostly entire homes; mixing them compares a room's rent with a whole flat's revenue | Normalising per m² or bedroom: each source lacks one of the two fields |
| M3 | **Occupancy as config-driven scenarios** (legal 30-night cap, 50%, 70%) | The answer depends on occupancy; scenarios make that explicit | One fixed occupancy; 100% occupancy |
| M4 | **Breakeven occupancy** as the headline metric | Answers "when does Airbnb win?" without picking a scenario | Only a winner per fixed scenario |
| M5 | **Medians** + `n_listings` + `low_sample` flag (n < 5) | Prices are skewed (p99 €1,260, max €5,000 after cleaning); 2 listings shouldn't decide a postcode | Means only |
| M6 | **30-night cap applies to whole units only** | Renting a room in your own home is B&B, with no night cap | Applying the cap to rooms |
| M7 | **Gross revenue** | The brief asks for "potential revenue"; costs, fees and taxes are out of scope | A cost model |
| M8 | **Full outer join** in the comparison | A postcode with listings on only one side stays visible (winner NULL) instead of silently disappearing | Inner join |
| M9 | **"Amsterdam" = the reference data's PC4s** (82) | Authoritative and testable | Hard-coded 1000–1109, which was wrong: Amsterdam is 1011–1112 |

## Data quality

| # | Decision | Why | Rejected |
|---|----------|-----|----------|
| Q1 | **Bronze is all strings** | A malformed value can never break ingestion; typing happens in silver, where it's tested | Typed bronze |
| Q2 | **Valid + quarantine tables share one rules dict** | `expect_all_or_drop(RULES)` for the clean table, the same rules inverted for quarantine, with `failed_rules` saying why. They can't drift apart | Only `expect_or_drop` (rows vanish); filters without expectations (no metrics) |
| Q3 | **Every rule is NULL-safe** (`coalesce(rule, false)`) | Otherwise a NULL comparison is dropped by the expectation *and* not selected by `NOT rule`, so the row is lost silently | — |
| Q4 | **One rule checks one thing** (`price IS NULL OR price BETWEEN …`) | One cause gives one reason in `failed_rules` | Rules that overlap |
| Q5 | **Uniqueness via an aggregate table** (`dq_uniqueness` + `expect_or_fail`) | Expectations are row-level: one row can't know it's a duplicate | Checking only in a notebook |
| Q6 | **Exact duplicates removed** (382), **near-duplicates kept** (130) | Exact copies are scrape duplicates. Near-duplicates (same flat, price ±€1) can't be proven to be one listing: apart-hotels have identical units | Dedup on coordinates (738): could merge distinct units |
| Q7 | **Quality bounds are about errors, not outliers**: Airbnb €10–€5,000, rent €100–€10,000 | Chosen after looking at the rows outside them (€9,000 for 2 guests, €1 rents). €2k–€4k listings are real luxury homes | Trimming at p99: would remove real listings, biased against Airbnb |
| Q8 | **PC4: valid raw zip first, polygon second** | Raw and polygon disagree for 21% of rows, but by a median of 66 m (85% within 200 m): Airbnb's location fuzzing, not wrong zips | Polygon always wins; nearest neighbour; external API |
| Q9 | **City filter is scope, not quality** | Rotterdam isn't bad data; out-of-scope rows never enter conservation or quarantine | Quarantining 38,627 non-Amsterdam rows |

## Platform and engineering

| # | Decision | Why | Rejected |
|---|----------|-----|----------|
| D1 | **Start from revo-dabs, adapted** to serverless | Reviewers' conventions; adapted classic clusters → serverless, dropped the `test` target and semantic-release | Hand-rolled layout |
| D2 | **Landing zone outside the bundle** (`just bootstrap`) | `bundle destroy` must never delete hand-uploaded raw files; dev and prod share one upload | Catalog/volume as bundle resources |
| D3 | **SDP source = notebooks** in `notebooks/pipelines/…/transformations/` | The brief asks for pipeline notebooks; the revo-dabs layout | `.py` modules; `.ipynb` (noisy diffs) |
| D4 | **Thin notebooks, logic in a tested wheel** | 37 unit tests, 100% coverage; notebooks only wire inputs to functions | Logic in notebooks (untestable) |
| D5 | **Dev isolation from `mode: development`** (`dev_<user>_` schemas) | Free per-developer isolation | My own `schema_prefix` variable, which doubled the prefix |
| D6 | **Silver streaming, gold materialized views** | Row-level cleaning is append-only; aggregates must be recomputed | Gold as streaming tables (wrong totals) |
| D7 | **Pipeline defines data, job defines workflow** | The parquet export is a side effect, so it's job task 2, outside SDP. When the export broke, the data update was unaffected | Export inside the pipeline |
| D8 | **Single-file parquet via pandas** | Readable files for reviewers; gold is small | Spark's directory of part files (fine at scale) |
| D9 | **Tests on serverless (Databricks Connect)** | Same engine as production, so `ST_*` is testable | Local PySpark |
| D10 | **CI runs the pre-commit hooks** (`prek run --all-files`) | One definition of clean; separate command lists drifted twice | Separate lint commands in CI |
| D11 | **CI identity = service principal** with least-privilege grants; prod `root_path` in its folder; `run_as` it | Prod never runs as a person; only CI can write prod | Personal token; `/Workspace/Shared` (writable by everyone) |
| D12 | **The deploy is the gate**: CD calls CI and deploys only if it passes | GitHub Free doesn't enforce branch protection on private repos | Relying on an unenforced ruleset |
