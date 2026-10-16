# BUILD REPORT: Airbnb vs Kamernet Investment Pipeline

| Attribute | Value |
|-----------|-------|
| **Feature** | AIRBNB_RENTING |
| **Date** | 2026-10-16 |
| **DESIGN** | [DESIGN_AIRBNB_RENTING.md](../features/DESIGN_AIRBNB_RENTING.md) (v1.5 + as-built tables) |
| **Status** | Built, deployed (dev + prod), verified |

## Build steps

| Step | Scope | Result |
|------|-------|--------|
| 0 | Free Edition workspace + CLI profile | ✅ |
| 1 | revo-dabs scaffold adapted; `just bootstrap`; capability probe | ✅ Catalog only via SQL; `ST_*` works on serverless; Connect 19.2 |
| 2 | Raw files uploaded (user) | ✅ |
| 3 | Exploration (`scratch/01`, `02`), bounds frozen | ✅ New findings: 130 near-dups, PC4 1112 quirk, newline-wrapped nulls |
| 4 | `config`, `parsing`, `segments`, `quality` + tests | ✅ ("my turn" skipped by the user) |
| 5 | `geo`, `transforms`, `revenue` + tests | ✅ ("my turn" skipped by the user) |
| 6 | Pipeline notebooks, bundle resources, first deploy + run, `scratch/03` | ✅ All checks pass |
| 7 | Export task, `just export`, AI/BI dashboard | ✅ |
| 8 | CI/CD, service principal, hooks, GitHub | ✅ PRs #1–#2; CD gated on CI; prod deployed and run as the service principal |
| 9 | Docs, diagrams, README, clean-up, learnings | ✅ |

## Quality gate

| Check | Result |
|-------|--------|
| Unit tests | 37 passed on serverless |
| Coverage | 100% of `src/` (gate: 80%) |
| ruff / ruff-format / ty / pydoclint / hygiene hooks | All pass (`prek run --all-files`, also in CI) |
| `bundle validate` dev + prod | OK, no warnings |
| Row conservation (SC-02) | Airbnb 9,913 = 9,505 + 26 + 382; rentals 8,095 = 8,087 + 8 |
| PC4 coverage (SC-03) | 99.76% |
| Uniqueness (`dq_uniqueness`) | 0 duplicates |
| Parquet readable (SC-07) | 3 files, read with pandas |
| CI gate (AT-024) | Not enforced by GitHub on a free private repo; deploy gated with `needs: ci` instead |

## Deviations from DESIGN

See "Build steps 4–7 deviations (as built)" in the DESIGN document. The main ones:
- `schema_prefix` removed (dev mode already prefixes per user);
- pandas single-file export;
- CI runs the hooks;
- the deploy is the gate;
- prod `root_path` + `run_as` the service principal.

## Open items (handed to the user)

- Export the AI session(s) into `docs/ai-sessions/` (R6).
- Delete the local secret file and archive or delete the old `airbnb-renting` GitHub repo.
- Visually check the dashboard widgets (SQL was tested; rendering wasn't).
