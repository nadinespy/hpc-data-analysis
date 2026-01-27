# HPC Data Analysis

Analyses HPC cluster usage by faculty, with a focus on resource efficiency (CPU, memory, time). Queries the Slurm accounting database (MySQL) directly and maps users to faculties via LDAP.

## Structure

```
hpc_stats.py                  # Aggregate statistics per faculty (→ CSV)
job_level_metrics.py           # Per-job efficiency metrics (→ CSV)
slurm_utils.py                 # Shared utilities: MySQL, LDAP, TRES parsing
hpc_stats_visualisation.ipynb  # Jupyter notebook with all plots and documentation
config.yaml                   # MySQL credentials (not committed)
original_code/                 # Previous version of the analysis scripts
dev_scripts/                   # Diagnostic queries and their output
```

## Setup

### Config

Create a `config.yaml` with MySQL connection details:

```yaml
mysql_host: <hostname>
mysql_port: 3306
mysql_user: <user>
mysql_password: <password>
mysql_database: <slurm_acct_db>
cluster_name: <cluster>
```

### Dependencies

```
pip install mysql-connector-python python-ldap PyYAML
pip install pandas numpy matplotlib seaborn scipy ipywidgets  # for the notebook
```

## Usage

### 1. Generate aggregate faculty statistics

```bash
python3 hpc_stats.py --since 2025-01-01 --until 2025-02-01
```

Outputs `hpc_stats_output.csv` and `hpc_stats_output_global.csv`.

### 2. Generate per-job metrics

```bash
python3 job_level_metrics.py --since 2025-01-01 --until 2025-02-01 \
    --output job_level_metrics.csv --include-faculty
```

### 3. Run the notebook

Open `hpc_stats_visualisation.ipynb` in Jupyter. It reads the CSV files generated above. The notebook includes a Technical Appendix documenting the methodology, efficiency formulas, and open questions.

## What it computes

- **CPU efficiency**: total CPU time / (elapsed x requested CPUs). Can exceed 100% when programs spawn threads internally without Slurm knowing.
- **Memory efficiency**: peak memory used / requested memory. Can exceed 100% if memory limits are not enforced on the cluster.
- **Time efficiency**: elapsed wall-clock time / requested time limit.
- **Weighted vs average**: weighted efficiency is dominated by large jobs (sum of used / sum of allocated); average efficiency treats each job equally.

See the Technical Appendix in the notebook for full details.
