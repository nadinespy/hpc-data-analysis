# HPC Data Analysis

Analyses HPC cluster usage by faculty, with a focus on resource efficiency (CPU, memory, time). Queries the Slurm accounting database (MySQL) directly and maps users to faculties via LDAP.

## Structure

```
hpc-data-analysis/
├── src/
│   └── hpc_data_analysis/
│       ├── __init__.py
│       ├── slurm_utils.py          # Shared utilities: MySQL, LDAP, TRES parsing
│       ├── aggregate_stats.py      # Aggregate statistics per faculty (→ CSV)
│       └── job_stats.py            # Per-job efficiency metrics (→ CSV)
├── notebooks/
│   └── visualisation.ipynb         # Jupyter notebook with all plots and documentation
├── dev_scripts/                    # Diagnostic queries and their output
├── results/                        # Generated CSV output (gitignored)
├── pyproject.toml                  # Package metadata and dependencies
├── config.yaml                     # MySQL credentials (not committed)
└── README.md
```

## Installation

Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Create the results directory:

```bash
mkdir results
```

Install the package:

```bash
pip install .
```

For notebook support:

```bash
pip install ".[notebook]"
```

For development (editable install):

```bash
pip install -e ".[notebook]"
```

## Configuration

Create a `config.yaml` with MySQL connection details:

```yaml
mysql:
    host: <hostname>
    user: <user>
    password: <password>
    database: <slurm_acct_db>
```

## Usage

### 1. Generate aggregate faculty statistics

```bash
hpc-aggregate-stats --since 2025-01-01 --until 2025-02-01 \
    --collate_by st=faculty --output results/hpc_stats_output.csv
```

### 2. Generate per-job metrics

```bash
hpc-job-stats --since 2025-01-01 --until 2025-02-01 \
    --output results/job_level_metrics.csv --include-faculty
```

### 3. Run the notebook

Open `notebooks/visualisation.ipynb` in Jupyter. It reads the CSV files generated above. The notebook includes a Technical Appendix documenting the methodology, efficiency formulas, and open questions.

## What it computes

- **CPU efficiency**: total CPU time / (elapsed x requested CPUs). Can exceed 100% when programs spawn threads internally without Slurm knowing.
- **Memory efficiency**: peak memory used / requested memory. Can exceed 100% if memory limits are not enforced on the cluster.
- **Time efficiency**: elapsed wall-clock time / requested time limit.
- **Weighted vs average**: weighted efficiency is dominated by large jobs (sum of used / sum of allocated); average efficiency treats each job equally.

See the Technical Appendix in the notebook for full details.
