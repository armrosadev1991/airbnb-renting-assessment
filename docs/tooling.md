# Tooling: why this stack

The brief allows any tool, so this page explains why I chose each one. It's about choices,
not set-up steps (see [development.md](development.md) for those).

## Databricks Free Edition, not local PySpark

| Need | Why Databricks |
|------|----------------|
| Levels 1–3 ask for SDP, expectations and Declarative Automation Bundles | These are Databricks features; local PySpark could only imitate them |
| Postcode backfill needs point-in-polygon | Native `ST_*` spatial SQL on serverless (DBR 19.9) replaces a shapely UDF |
| A dashboard for the investor | AI/BI dashboards read the gold tables directly; no extra BI tool |
| Something reviewers can open | One URL to a workspace, instead of "run this on your laptop" |

Free Edition costs nothing and runs **serverless only**. That shaped several decisions: no
cluster configuration anywhere, `serverless: true` on the pipeline, and dev and prod as two
*targets* in one workspace (see [decisions.md](decisions.md), D2 and D12).

**Trade-off accepted:** tests and pipelines need a network connection and a workspace, and
use Free Edition quota.

## Lakeflow Spark Declarative Pipelines (SDP), not notebooks plus jobs

- **Declarative:** each table is a function that returns a DataFrame; SDP works out the order,
  the checkpoints and incremental processing. A hand-written job would have to manage all of
  that.
- **Expectations built in:** data-quality rules give per-rule pass/fail counts in the event
  log for free. `scratch/03_validate_outputs.py` reads them back.
- **Streaming tables + materialized views** match the data: bronze and silver are appended
  once per file (Auto Loader), while gold aggregates have to be recomputed when inputs change.

## Declarative Automation Bundles (DABs), started from revo-dabs

The bundle is the only definition of what's deployed: schemas, export volume, pipeline, job,
dashboard. `dev` and `prod` are targets in it, and CI/CD deploys it as a service principal.
I started from [revo-dabs](https://github.com/revodatanl/revo-dabs), RevoData's own template,
so the layout and tooling (uv, ruff, ty, pydoclint, prek, just) will look familiar to
reviewers. I changed it where Free Edition needs something different; the changes are in
[decisions.md](decisions.md) (D1).

## Developer tooling (from revo-dabs)

| Tool | Role | Why it's worth having |
|------|------|-----------------------|
| **uv** | Dependencies, lock file, `uv build` of the wheel | One fast tool; `uv.lock` makes CI and laptops identical |
| **ruff** | Lint (`select = ["ALL"]`) + format | Strict by default; every rule ignored has a written reason in `pyproject.toml` |
| **ty** | Type checking | Caught real bugs: `spark.conf.get()` returns `str \| None` |
| **pydoclint** | Docstrings match signatures | Docstrings don't go stale |
| **prek** | Runs the pre-commit hooks | Fast reimplementation of pre-commit; **CI runs the same hooks** |
| **commitizen** | Conventional Commit messages | Readable history; `cz bump` available if releases are ever needed |
| **just** | Task runner | `just bootstrap / deploy / run / export / scratch` |
| **Databricks Connect** | Unit tests on serverless | Tests run on the same engine as production, including `ST_*` |
