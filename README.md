# HPC Data Analysis

Analyses HPC cluster usage by faculty, with a focus on resource efficiency (CPU, memory, time). Queries the Slurm accounting database (MySQL) directly and maps users to faculties via LDAP.

> **About this repository**
>
> I am the sole developer of this project: I wrote all of the code and shaped the
> analytical approach myself, working in regular consultation with stakeholders in
> the **e-Research team at King's College London** (soon to be renamed **Advanced
> Research Computing**), who guided the direction, priorities, and focus of the work.
>
> This is my personal copy, kept as a snapshot of the work as of **September 2026**.
> The actively maintained version lives at
> [kcl-eresearch/hpc-data-analysis](https://github.com/kcl-eresearch/hpc-data-analysis),
> where the team continues development after my handover. Released under the MIT
> License (see [LICENSE](LICENSE)).

## Structure

```
hpc-data-analysis/
├── src/
│   └── hpc_data_analysis/
│       ├── __init__.py
│       ├── slurm_utils.py                  # Shared utilities: MySQL, LDAP, TRES parsing
│       ├── aggregate_stats.py              # Aggregate statistics per faculty (→ CSV)
│       └── job_stats.py                    # Per-job efficiency metrics (→ CSV)
├── docs/
│   ├── analysis/                           # Documentation for analysts
│   └── development/                        # Documentation for developers
├── notebooks/
│   ├── 2026-02_sustainability_month_blog_post.ipynb  # Blog-post plots (user perspective)
│   ├── visualisation_users.ipynb                     # User-focused: average efficiency metrics
│   └── visualisation_infrastructure.ipynb            # Infrastructure-focused: weighted efficiency metrics
├── dev_scripts/                            # Diagnostic query scripts
│   └── output/                             # ...and their saved output
├── results/
│   ├── data/                               # Generated CSV output (gitignored)
│   └── plots/                              # Generated figures (committed)
├── pyproject.toml                          # Package metadata and dependency ranges
├── requirements.txt                        # Pinned dependency versions (reproducible env)
├── config.yaml.example                     # Template for config.yaml
├── config.yaml                             # MySQL credentials (not committed)
├── LICENSE
└── README.md
```

## Installation

This project runs in two stages, typically on two machines:

1. **Generate the data** (core analysis) — the CLI tools (`hpc-aggregate-stats`, `hpc-job-stats`) query the Slurm accounting database and write CSVs with per-job and per-faculty efficiency stats. The database is reachable only from the **cluster**, so this stage runs there, and needs only the core install (`pip install .`).
2. **Explore the results** (further analysis) — the notebooks read those CSVs and produce the visualisations. They need no database access, so this stage runs wherever you work (usually **locally**), and needs the notebook dependencies (`pip install ".[notebook]"`). The CSVs are generated on the cluster in stage 1, so copy them into `results/data/` on your local machine first.

Creating the virtual environment is the same on both machines; only the install command differs by stage (shown below).

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install the package. Choose based on the stage you are setting up:

```bash
# Stage 1 — core CLI tools only (query the database, write CSVs).
# Run on the cluster, where the database is reachable:
pip install .

# Stage 2 — also the notebook dependencies (pandas, matplotlib, seaborn,
# scipy, etc.). Run locally, where you explore the generated CSVs:
pip install ".[notebook]"

# For development on either machine — an "editable" install that links the
# package to this source tree, so code edits take effect without reinstalling:
pip install -e ".[notebook]"
```

`[notebook]` is an optional dependency group — the core tools don't need pandas/seaborn/etc., so omitting it keeps the environment light for anyone who only runs the CLI tools (and not the visualisation notebooks).

To reproduce the exact dependency versions this project was developed and tested with, install the pinned set first, then the package itself:

```bash
pip install -r requirements.txt
pip install .            # or  pip install -e .  for development
```

`requirements.txt` pins every dependency (a reproducible snapshot); `pyproject.toml` declares the looser compatible ranges used when you install the package directly.

### Verify your installation

Confirm the package and its dependencies import in your *active* environment:

```bash
hpc-aggregate-stats --help     # package + entry point + core deps (mysql.connector, ldap, yaml)
python3 -c "import pandas, numpy, matplotlib, seaborn, scipy, statsmodels; print('notebook deps OK')"
```

This only checks that everything is installed and importable — not that the database is reachable or your credentials work (that needs the cluster).

## Configuration

The tools rely on two configuration files. Both hold credentials, but they are managed differently: one you create and keep locally in the repo tree (gitignored, never committed); the other is a system file on the cluster that the infrastructure team maintains and you never touch.

### `config.yaml` — MySQL (you create this)

Connection details for the Slurm accounting database. Copy the template and fill in the values provided by the infrastructure team:

```bash
cp config.yaml.example config.yaml
```

It lives in the project root and is gitignored, so it is never committed. The accounting database is reachable only from the cluster, so run the tools there. The two CLI commands default to `config.yaml` in the directory you run them from; if it lives elsewhere, point them at it with `--config <path>`.

### `/etc/hpc_export_stats.yaml` — LDAP (infra-managed)

Only needed when mapping users to faculties (`--collate_by st=faculty` or `--include-faculty`). This file is provisioned and maintained by the infrastructure team on the cluster — you do not create or edit it, but it must exist for faculty lookups to work. The two CLI commands default to `/etc/hpc_export_stats.yaml`; if this file lives elsewhere, override the path with `--ad_config <path>`. It contains the following keys:

| Key | Meaning |
|-----|---------|
| `ldap_host` | AD/LDAP server hostname (connected to via `ldaps://`) |
| `ldap_ca_file` | path to the CA certificate file used to verify the TLS connection |
| `ldap_users_ou` | base OU searched for user accounts |
| `ldap_binddn` | bind DN used to authenticate |
| `ldap_password` | bind account password |

Since the infrastructure team owns this file, confirm the exact key names and values with them if faculty lookups are not resolving.

## Usage

Run from the repository root. The examples use the installed console commands (`hpc-aggregate-stats`, `hpc-job-stats`), available after `pip install`. The accounting database is reachable only from the cluster, so run these there.

### 1. Generate aggregate faculty statistics

```bash
hpc-aggregate-stats \
    --since 2025-01-01 --until 2025-02-01 \
    --collate_by st=faculty --collate_by none \
    --output results/data/hpc_stats_output.csv
```

- `--collate_by st=faculty`: Groups stats by faculty (LDAP attribute)
- `--collate_by none`: Adds a global summary row (`faculty="all"`)

### 2. Generate per-job metrics

```bash
hpc-job-stats \
    --since 2025-01-01 --until 2025-02-01 \
    --output results/data/job_level_metrics.csv --include-faculty
```

> **All output filenames are auto-prefixed with the date range.** The command above actually writes `results/data/2025-01-01_2025-02-01_job_level_metrics.csv`, not the bare name you passed. The prefix stops runs over different date ranges from overwriting each other and gives every output a consistent, self-describing name. Generated CSVs contain usernames, so they are gitignored and never committed.

These two commands are the data-generation step (stage 1): they produce the CSVs in results/data/ that the notebooks read. The CSVs are gitignored (not in the repo), so they must be regenerated before any notebook can run, and the date range you pass determines which jobs the analysis covers. Running them needs cluster database access (and LDAP access for the `--collate_by` / `--include-faculty` faculty lookups).

> **If a command is "not found"** (common on shared/cloud systems where the package didn't install into the active Python environment), run the module directly with the source on the path instead — the arguments are identical:
> ```bash
> PYTHONPATH=src python3 -m hpc_data_analysis.aggregate_stats ...
> ```

### 3. Run the notebooks

All three notebooks read the CSVs produced in steps 1–2. To reproduce the analysis exactly as it appears in this repo, generate the data over the period the committed notebooks use — `--since 2025-07-01 --until 2025-12-31`, with both commands above — and the notebooks will reproduce the committed results. To analyse a different period instead, regenerate with a different range and update the notebooks' input paths to match.

The notebooks differ mainly in whether they report **average** or **weighted** efficiency (see [Weighted vs average efficiency](#weighted-vs-average-efficiency)). Each one — its perspective, purpose, and which CSVs it reads — is described in [`docs/notebooks.md`](docs/notebooks.md).

## What this analysis computes

### Efficiency metrics

`hpc-job-stats` outputs three types of efficiencies **per job** — the resource one job used divided by what it reserved (e.g. CPU efficiency = CPU time used / (elapsed × CPUs requested)):

- **CPU efficiency**: total CPU time / (elapsed × requested CPUs). Can exceed 100% if jobs use more threads than requested CPUs.
- **Memory efficiency**: peak memory used / requested memory. Can exceed 100% if memory limits are not enforced.
- **Time efficiency**: elapsed wall-clock time / requested time limit.

### Weighted vs average efficiency

To summarise a **group** of jobs (a faculty, or the whole cluster), `hpc-aggregate-stats` combines those per-job efficiencies in two ways and reports *both* per faculty. Both use the same per-job `used` and `allocated` values; they differ only in how much each job counts:

| Method | Formula | Best for |
|--------|---------|----------|
| **Simple average** | mean over jobs of `(used / allocated) × 100` — each job counts **once** | User education — the typical job's efficiency |
| **Weighted average** | `(Σ used / Σ allocated) × 100`, summed over all jobs — equivalently a mean weighted by `allocated`, so each job counts in proportion to what it reserved | Infrastructure view — a more representative efficiency figure, since each job is weighted by the resources it used, so large jobs (the ones with the biggest impact on the cluster) count for most |

The average treats a 1-CPU job and a 512-CPU job equally, while the weighted figure lets the big job count for more.

### Job states

Efficiency is computed only for jobs that ran and have meaningful usage — **COMPLETED**, **TIMEOUT**, and **OUT_OF_MEMORY**. *Success* (COMPLETED only) and *failure* (FAILED, TIMEOUT, NODE_FAIL, PREEMPTED, OUT_OF_MEMORY — not CANCELLED, which is intentional) are tracked separately. See [`docs/analysis/data_caveats.md`](docs/analysis/data_caveats.md) for which states are included and why.

## Documentation

- **For analysts** interpreting results: see [`docs/analysis/`](docs/analysis/index.md)
  - How efficiency metrics are calculated
  - What good/bad efficiency looks like
  - Known limitations and caveats

- **Open questions & known issues:** [`docs/analysis/open_questions.md`](docs/analysis/open_questions.md) — the durable record of unresolved puzzles, assumptions, and methodological decisions behind specific results (e.g. why some efficiencies exceed 100%, the multi-node CPU undercount).

- **For developers** working on the codebase: see [`docs/development/`](docs/development/index.md)
  - Slurm database schema
  - CPU and memory accounting details
  - Dev scripts guide

- **Notebook catalog:** [`docs/notebooks.md`](docs/notebooks.md) — what each notebook does
