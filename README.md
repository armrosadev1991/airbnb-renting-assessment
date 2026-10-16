# Amsterdam: rent long-term (Kamernet) or short-term (Airbnb)?

![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition%20%C2%B7%20serverless-%231B3139)
[![python](https://img.shields.io/badge/python-3.12-g)](https://www.python.org)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with ty](https://img.shields.io/badge/type%20checked-ty-261230.svg)](https://github.com/astral-sh/ty)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

A Lakeflow medallion pipeline on Databricks that turns two messy public datasets (Kamernet
rentals and Airbnb listings) into a per-postcode answer: **which pays more, and at what Airbnb
occupancy does the answer flip?** Built for the RevoData data-engineer assessment, up to Level 3.

## The answer

Gross monthly revenue, per 4-digit postcode, compared within the same property type. Only
postcodes with at least 5 listings on each side are counted:

| Property type | Airbnb scenario | Airbnb wins | Kamernet wins | Occupancy Airbnb needs (median) |
|---------------|-----------------|-------------|---------------|---------------------------------|
| Whole home | **Legal cap: 30 nights/year** | 1 | **61** | 39.5% |
| Whole home | 50% occupancy | 51 | 11 | 39.5% |
| Room | 50% occupancy | 64 | 0 | 25.1% |

A whole home on Airbnb needs about **40%** occupancy to match long-term rent; Amsterdam's cap
allows about **8%**. Within the law, **long-term renting wins almost everywhere for whole
homes**, while rooms favour Airbnb. Assumptions and limitations (2019 data, gross revenue, room
comparability) are in [docs/revenue_model.md](docs/revenue_model.md).

## How it works

![Data flow](docs/diagrams/data_flow.png)

- **Bronze:** Auto Loader copies the files as-is (all strings), plus lineage columns.
- **Silver:** typed and cleaned. 23% of Airbnb zips are missing or unusable; those listings get
  their postcode from their coordinates (`ST_Contains`), which brings coverage to 99.76%. Every bad row lands in a **quarantine table with the reason**, and row
  conservation is checked: `9,913 = 9,505 clean + 26 quarantined + 382 duplicates`.
- **Gold:** revenue per listing → per postcode (medians) → Kamernet vs Airbnb with breakeven
  occupancy.
- **Outputs:** a job exports gold to [`data/output/`](data/output/) as parquet; an AI/BI
  dashboard shows the result per postcode on a map.

The reasoning behind each choice: [docs/decisions.md](docs/decisions.md) ·
data issues and backfill: [docs/data_quality.md](docs/data_quality.md) ·
why this stack: [docs/tooling.md](docs/tooling.md).

## Quick start

**Prerequisites:** a [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
workspace, the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html),
[uv](https://docs.astral.sh/uv/) and [just](https://just.systems).

```bash
# 1. Authenticate and configure the profile the recipes use
databricks auth login --host <workspace-url> --profile <name>
printf 'PROFILE_NAME=<name>\nDATABRICKS_CONFIG_PROFILE=<name>\n' > .env

# 2. Environment and git hooks
just setup

# 3. Landing zone (catalog, schema, volume), created once, outside the bundle
just bootstrap

# 4. Upload the raw files, one folder per source
cd data && unzip -o airbnb.zip && unzip -o rentals.zip && unzip -o geo/post_codes.zip -d geo && cd ..
databricks fs cp data/airbnb.csv             dbfs:/Volumes/airbnb_renting/landing/raw/airbnb/  --profile <name>
databricks fs cp data/rentals.json           dbfs:/Volumes/airbnb_renting/landing/raw/rentals/ --profile <name>
databricks fs cp data/geo/post_codes.geojson dbfs:/Volumes/airbnb_renting/landing/raw/geo/     --profile <name>

# 5. Deploy your dev copy, run it, pull the exports
just deploy      # pipeline, job, schemas, dashboard (dev target)
just run         # pipeline refresh, then parquet export (~3 min, serverless)
just export      # copies the parquet files into data/output/

# 6. Check the results and open the dashboard
just scratch 03_validate_outputs
just open
```

Tests: `just test` (runs on serverless through Databricks Connect, coverage gate 80%).

## Deliverables

| Brief | Where |
|-------|-------|
| Exploratory notebooks with validation checks | [`scratch/`](scratch/): `00` capability probe, `01`/`02` exploration, `03` output validation |
| Pipeline notebooks | [`notebooks/pipelines/airbnb_renting/transformations/`](notebooks/pipelines/airbnb_renting/transformations/), [`notebooks/export_parquet.py`](notebooks/export_parquet.py) |
| Buildable package | [`src/airbnb_renting/`](src/airbnb_renting/) (wheel built by the bundle with `uv build`) |
| Unit tests | [`tests/`](tests/): 37 tests, 100% line coverage, on serverless |
| Documentation (the why) | this README and [`docs/`](docs/) |
| Parquet exports | [`data/output/`](data/output/) |
| Job configuration | [`resources/`](resources/) |
| **L1** CI/CD, tests in CI, pre-commit | [`.github/workflows/`](.github/workflows/), [`.pre-commit-config.yaml`](.pre-commit-config.yaml) |
| **L2** dashboard, diagrams | [`resources/dashboards/`](resources/dashboards/), [`docs/diagrams/`](docs/diagrams/) |
| **L3** SDP, expectations, bundles | Lakeflow pipeline with `expect_all_or_drop` / `expect_or_fail`; [`databricks.yml`](databricks.yml) |

## CI/CD

![CI/CD](docs/diagrams/cicd_pipeline.png)

- **CI** on every pull request: the pre-commit hooks, the package build check, tests on
  serverless, and `bundle validate -t prod`.
- **CD** on `main`: runs CI again, then deploys `prod` as the service principal
  `github-actions-airbnb`.

GitHub Free doesn't enforce branch protection on private repositories, so **the deploy is the
gate**: broken code never reaches prod, even if it reaches `main`.

## Tooling choice, in one line

Databricks Free Edition, because Levels 1–3 ask for SDP, expectations and bundles, and serverless
spatial SQL made the postcode backfill a single join. The project started from RevoData's
[revo-dabs](https://github.com/revodatanl/revo-dabs) template. Details in
[docs/tooling.md](docs/tooling.md).
