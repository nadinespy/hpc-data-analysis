# Code Improvements & Technical Debt

This document records improvements worth making to the codebase and workflow — performance, correctness, testing, structure, and tooling. It is the developer-side companion to [`../analysis/open_questions.md`](../analysis/open_questions.md): where that file tracks unresolved questions about the *data and analysis*, this one tracks things we know how to improve about the *code*.

Nothing here is a correctness bug that invalidates results; these are quality, speed, and maintainability improvements. Items are candidates for GitHub issues.

## Priority & effort

| Field | Values | Meaning |
|-------|--------|---------|
| **Priority** | High / Medium / Low | High = big payoff or removes a real pain point; Low = nice-to-have |
| **Effort** | S / M / L | Relative size only — Small / Medium / Large. |
| **Type** | Performance / Correctness / Testing / Structure / Tooling | Rough category |

## Summary

| # | Item | Type | Priority | Effort |
|---|------|------|----------|--------|
| 1 | Fetch once, not twice — single-pass pipeline | Performance / Structure | High | M |
| 2 | Query does a full table scan (no usable `time_submit` index) | Performance | High | M |
| 3 | `discover_special_steps()` re-runs (and re-scans) every time | Performance | Medium | S |
| 4 | No tests for the parsing / encoding logic | Testing | High | M |
| 5 | `parse_date_range` is timezone-dependent | Correctness | Medium | S |
| 6 | `print`-to-stderr progress instead of `logging` | Structure | Low | S |
| 7 | Smoother dev/deploy workflow | Tooling | Medium | M |
| 8 | Dev scripts duplicate boilerplate (connection, `out()`, TRES parsing) | Structure | Medium | M |
| 9 | Hardcoded and inconsistent time ranges in dev scripts | Correctness | Low | S |

---

## Performance

### 1. Fetch once, not twice — single-pass pipeline

- **Priority / Effort:** High / M
- **What / why:** `aggregate_stats.py` and `job_stats.py` each call `fetch_job_data()` independently over the same date range, so the standard workflow (running both tools) scans the large `create_job_table` ⋈ `create_step_table` join **twice** and runs `calculate_job_metrics()` twice per job. They differ only in what they do per row: the aggregate tool calls `update_stats()` for *all* jobs (it needs every state for the counts); the job-level tool calls `write_csv_row()` for the `INCLUDED_STATES` subset.
- **Proposed approach:** A single `hpc-stats` command that fetches once and, per row, updates the aggregate *and* — if `state in INCLUDED_STATES` — writes the per-job row. Roughly halves the database work for the combined run and removes the duplicated `main()` scaffolding (see #below and item overlaps with **Structure**). The aggregate could alternatively be derived from the per-job CSV in pandas, but a single-pass CLI is the clean minimum.
- **References:** `slurm_utils.fetch_job_data`; `aggregate_stats.main`; `job_stats.main`.

### 2. The query full-scans the table (missing `time_submit` index)

- **Priority / Effort:** High / M
- **What / why:** `fetch_job_data()` filters on `j.time_submit`, but the schema (`dev_scripts/output/output_table_defs.txt`) shows no index led by `time_submit` — the leading time indices are on `time_end` / `time_eligible`. So a one-day query and a six-month query scan the same (whole) table, which is why runtime is flat at ~40–55 min regardless of date range. This is the developer-side of [open_questions.md](../analysis/open_questions.md) #12.
- **Proposed approach:**
  - Cheapest: filter on `time_end` (which *is* indexed) instead of / in addition to `time_submit`. **Note the semantic shift** — this selects jobs by when they *finished* rather than when they were *submitted*; decide whether that's acceptable for the analysis window, or add both bounds.
  - Or ask infra to add a `time_submit` index.
  - Confirm with `EXPLAIN` on a one-day vs six-month range (identical row estimates ⇒ full scan).
- **References:** `slurm_utils.fetch_job_data`; [open_questions.md](../analysis/open_questions.md) #12.

### 3. `discover_special_steps()` re-runs and re-scans every time

- **Priority / Effort:** Medium / S
- **What / why:** Every run calls `discover_special_steps()`, whose `SELECT DISTINCT id_step, step_name ... WHERE id_step < 0` scans the (large, unindexed on `id_step`) step table just to rediscover values that are stable per cluster (`batch = -5`, `interactive = -6`, from Slurm's `slurm.h`).
- **Proposed approach:** Cache the discovered map (a small JSON file, or a block in `config.yaml`) and reuse it, re-discovering only behind a `--refresh-steps` flag; and/or accept `--batch-step-id` / `--interactive-step-id` overrides with discovery as the fallback.
- **References:** `slurm_utils.discover_special_steps`.

---

## Testing

### 4. No tests for the parsing / encoding logic

- **Priority / Effort:** High / M
- **What / why:** The subtlest, most easily-broken logic has zero coverage — and it's exactly the kind of pure-function logic that's cheap to test and a real safety net for anyone who later edits the SQL.
- **Proposed approach:** A `tests/` suite (pytest) covering: `parse_tres_value` (normal / missing id / empty / malformed); the `mem_req` bit-63 per-cpu vs per-node decode; `parse_date_range` (valid + `since >= until` error); the four `submit_line` parsers (`--ntasks`, `--cpus-per-task`, `--pty`, `sbatch`); and the CPU step-aggregation fallback (regular steps present vs batch-only). Add `pytest` as a dev dependency.
- **References:** `slurm_utils.py` (parsers, `calculate_job_metrics`).

---

## Correctness / robustness

### 5. `parse_date_range` is timezone-dependent

- **Priority / Effort:** Medium / S
- **What / why:** `datetime.strptime(...).timestamp()` uses the *local* timezone of whatever machine runs it, so `2025-07-01` maps to a different Unix boundary on a UTC cloud box than on a machine in BST — shifting which jobs fall in range by a few hours at each end.
- **Proposed approach:** Build the boundaries in explicit UTC (`datetime(..., tzinfo=timezone.utc)`), or document that dates are interpreted in the runner's local time.
- **References:** `slurm_utils.parse_date_range`.

---

## Structure / maintainability

### 6. `print`-to-stderr progress instead of `logging`

- **Priority / Effort:** Low / S
- **What / why:** Progress and LDAP diagnostics use `print(..., file=sys.stderr)`. The `logging` module would give levels and a `-v`/`--verbose` switch for free.
- **References:** `slurm_utils.py`, `aggregate_stats.py`, `job_stats.py`.

### Note on the two `main()`s

Beyond the single-pass merge (#1), the two CLIs duplicate their setup — arg parsing, LDAP setup, MySQL connect, `discover_special_steps`, the fetch loop, per-job faculty lookup. Even if two commands are kept, factoring a shared `run_pipeline(...)` helper into `slurm_utils.py` removes the copy-paste. This is largely subsumed by #1.

---

## Tooling / workflow

### 7. Smoother dev/deploy loop

- **Priority / Effort:** Medium / M
- **What / why:** The database is reachable **only from the cluster/cloud**, but the visualisation notebooks run **locally** — so every iteration shuttles files between the two machines: edit code (local) → get it to the cloud → run there (CSVs land on the cloud) → copy the CSVs back to local → run notebooks (local). The round-trip is inherent, but each hop can be smoothed. (`scp` = "secure copy" and `rsync` are command-line tools that copy files to/from a remote machine over SSH; `rsync` additionally transfers only what changed and can sync whole directories.)
- **Proposed approach** *(all of these are helpers to build — none exists yet)*:
  - A **one-line driver** (Makefile target or `run_analysis.sh`) that runs the data generation with the standard date range and output paths on the cluster.
  - A **`fetch-results` helper** — a small script wrapping `rsync`/`scp` — that copies `results/data/*.csv` from the cloud back to local in one command, instead of copying each file by hand.
  - For tight iteration, **`rsync` the working tree** (or just `src/`) to the cloud each edit, instead of the `git push` → `git pull` dance just to test a change.
  - *(Optional)* The notebooks already expose their parameters in an "Edit per run" cell (`DATE_FILTER`, `RESULTS_DIR`), and the `visualisation_*` ones auto-select the most recent CSVs when `DATE_FILTER = None`. `papermill` could inject those parameters and run the notebooks **headless** — e.g. exporting rendered HTML from the cloud — without opening Jupyter. (This is the only notebook-parametrisation gap: the parameter cells themselves already exist.)
- **References:** README "Usage"; `results/data/`; `notebooks/`.

---

## Dev scripts

### 8. Dev scripts duplicate boilerplate

- **Priority / Effort:** Medium / M
- **What / why:** The diagnostic scripts repeat a lot of the same code. Verified across `dev_scripts/`: **13** scripts each build their own `mysql.connector.connect(...)` instead of using `slurm_utils.connect_mysql`; **14** define their own `out()` (print-to-stdout-and-file) helper; and **2** (`query_memory.py`, `query_tres_usage_vs_rusage.py`) re-define `parse_tres_value` even though `slurm_utils` already has it. Only two scripts import anything from the package (`discover_special_steps`), and even they still duplicate the connection and `out()`. The path-resolution boilerplate (`SCRIPT_DIR` / `PROJECT_ROOT` / `CONFIG_FILE` / `OUTPUT_DIR`) is copied too, in two slightly different variants (`SCRIPT_DIR.parent` vs. an upward search for `config.yaml`).
- **Proposed approach:** A small **`dev_scripts/_common.py`** sibling module holding the shared pieces (config load + connect, `out()`, `parse_tres_value`, path/output setup) that each script imports locally. Note the deliberate design tension recorded during the handover: the scripts were kept standalone partly so they run with nothing but `config.yaml` (no package install / no `PYTHONPATH` — which bit us on the cloud), and partly as a frozen archival record of how the DB was reverse-engineered. A **sibling** helper resolves this cleanly — it removes the duplication **without** reintroducing the installed-package dependency, because it's imported from the same directory rather than from `hpc_data_analysis`. (A lighter alternative: freeze the already-run scripts and use `_common.py` only for new ones.)
- **References:** `dev_scripts/*.py`; `slurm_utils.connect_mysql`, `slurm_utils.parse_tres_value`.

### 9. Hardcoded and inconsistent time ranges in dev scripts

- **Priority / Effort:** Low / S
- **What / why:** Several scripts bake a date window straight into their SQL, and the windows disagree: `faculty_stats.py` (`START_DATE`/`END_DATE` constants), `query_job_states.py`, `query_job_flags.py`, and `query_submit_line.py` use Jan 2025 (`2025-01-01`…`2025-02-01`); `query_memory.py` uses a single day (`2025-01-01`…`2025-01-02`); `query_sacct_steps.py` uses `2025-01-15`…`2025-01-16`. (Others — `query_cpu_mem_diagnostics.py`, `query_step_diagnostics.py`, `query_tres.py`, `query_tres_usage_vs_rusage.py`, `query_schema.py`, `query_table_defs.py` — apply no date filter at all.) Re-running a diagnostic for a different period means editing SQL by hand, and the inconsistency makes cross-script comparisons apples-to-oranges.
- **Proposed approach:** Pull the window into a shared default (a `SINCE`/`UNTIL` constant in `_common.py`, item #8) and/or accept `--since` / `--until` arguments, so every script uses the same period unless told otherwise. These are diagnostics, so a shared constant is probably enough; full argparse is optional.
- **References:** `faculty_stats.py`, `query_job_states.py`, `query_job_flags.py`, `query_submit_line.py`, `query_memory.py`, `query_sacct_steps.py`.

---

## Related Documentation

- [Open Questions](../analysis/open_questions.md) - Unresolved questions about the data and analysis (the analysis-side companion to this file)
- [Dev Scripts Guide](dev_scripts_guide.md) - The investigative scripts behind these findings
