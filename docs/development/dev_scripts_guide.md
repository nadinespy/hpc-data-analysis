# Dev Scripts Guide

The `dev_scripts/` directory contains investigative scripts used to understand the Slurm accounting database. Each script explores a specific aspect of the data and writes its output to `dev_scripts/output/`.

## Script Overview

Grouped roughly from "map the database" to "validate the pipeline":

| Script | Purpose | Output (in `output/`) |
|--------|---------|-----------------------|
| `query_schema.py` | Dumps every table and its columns (`SHOW TABLES` / `SHOW COLUMNS`) | `output_schema.txt` |
| `query_table_defs.py` | Dumps Slurm's own `CREATE TABLE` definitions (shows the indexes) | `output_table_defs.txt` |
| `query_tres.py` | Dumps `tres_table` (ID → type/name) and sample TRES strings | `output_tres.txt` |
| `query_job_states.py` | Counts jobs by state code | `output_job_states.txt` |
| `query_job_flags.py` | Explores the `flags` bitmask | `output_job_flags.txt` |
| `query_cpu_time.py` | Shows the rusage CPU-time columns per step | `output_cpu_time.txt` |
| `query_memory.py` | Determines memory unit encoding (MB vs bytes) | `output_memory.txt` |
| `query_cpu_mem_diagnostics.py` | Main CPU/memory diagnostic (req vs alloc CPUs, `mem_req` bit 63, mem eff >100%) | `output_cpu_mem_diagnostics.txt` |
| `query_step_diagnostics.py` | Step structure and CPU distribution across steps (incl. multi-node) | `output_step_diagnostics.txt` |
| `query_tres_usage_vs_rusage.py` | Compares rusage vs TRES CPU accounting | `output_tres_usage_vs_rusage.txt` |
| `query_submit_line.py` | Submission type + `--ntasks`/`--cpus-per-task` parsed from `submit_line` | `output_submit_line.txt` |
| `query_sacct_steps.py` | **Investigates** how `sacct` structures its data — job-level vs step-level, and how the batch/srun/extern step patterns map onto the MySQL step table. Exploratory; needs cluster + `sacct` | `output_sacct_steps.txt` |
| `verify_sacct_mysql_compatibility.py` | **Validates** the final numbers: compares our pipeline's per-job output CSV against a manual `sacct` export, field by field (Elapsed, TotalCPU, AllocCPUs, ReqMem, MaxRSS) | `output_verify_sacct_mysql_compatibility.txt` |
| `faculty_stats.py` | Legacy faculty statistics (use the CLI tools instead) | `output_faculty_stats.csv` |

Most outputs are committed as a record of what was found; a few (`query_cpu_time.py`, `query_memory.py`, `faculty_stats.py`) are re-runnable but not checked in.

## Running Scripts

Scripts can be run from any directory — they resolve paths relative to their own location (`Path(__file__).resolve()`), so `config.yaml`, the output folder, and any data files are found regardless of your working directory:

```bash
python3 query_cpu_mem_diagnostics.py
```

Scripts require:
- MySQL access to the Slurm accounting database
- `config.yaml` in the project root with MySQL credentials

## Detailed Descriptions

### `query_cpu_mem_diagnostics.py`

**Purpose**: The main diagnostic for CPU and memory data.

**Sections**:
1. CPU requests — `cpus_req` vs allocated CPUs (`tres_alloc` ID 1), single- vs multi-CPU, and `cpus_req` vs `tres_req` CPU
2. Memory request type — `mem_req` bit 63 (`--mem` vs `--mem-per-cpu`) distribution
3. `mem_req` value vs `tres_req` memory (checked separately for per-cpu and per-node jobs)
4. Jobs with apparent memory efficiency >100%, broken down by memory type
5. Requested vs allocated memory (`tres_req` vs `tres_alloc`)

**Key findings**: `tres_req` is the canonical request; `cpus_req` matches the `tres_req` CPU value for 99.999% of jobs, but *allocated* CPUs exceed requested for ~5.5% (whole-core rounding on hyperthreaded nodes); bit 63 of `mem_req` marks `--mem-per-cpu`.

### `query_step_diagnostics.py`

**Purpose**: Understand job step structure and how CPU time is spread across steps.

**Sections**:
1. Sample jobs with their step rows
2. Step-count distribution (~99% of jobs are single-step)
3. Step-type breakdown (special vs regular; common step names)
4. Batch vs srun CPU comparison (double-count check)
5. Batch CPU distribution in multi-step jobs
6. Batch-only and interactive-only jobs
7. Multi-node jobs — CPU via `tres_usage_in_max` vs rusage

**Key findings**: most jobs are single-step (batch only), where the batch step holds all the CPU time; where srun steps exist, batch CPU is usually negligible overhead — but a few multi-step jobs have large batch CPU that isn't fully explained (open_questions #5). Multi-node srun steps have `user_sec = 0` yet non-zero TRES CPU (open_questions #4).

### `query_tres_usage_vs_rusage.py`

**Purpose**: Compare the two CPU-time sources in step records.

**Data sources**:
- `user_sec`/`sys_sec`: from getrusage/wait4 (local process only, in seconds)
- `tres_usage_in_max` (TRES ID 1): from cgroup accounting (can include remote nodes)

**Key findings**: TRES CPU time is in **milliseconds** — the ratio to `user_sec+sys_sec` is consistently ~1000 (not seconds, as some earlier notes assumed). Regular srun steps on remote nodes often have rusage = 0 but valid TRES data. See open_questions #4.

### `query_memory.py`

**Purpose**: Determine the memory units in different fields, by sampling jobs and testing unit assumptions.

**Key findings**: `tres_req` (TRES ID 2) is in **MB**; `tres_usage_in_max` (TRES ID 2) is in **bytes**. So memory efficiency = `maxrss_bytes / (reqmem_MB × 1024²) × 100`.

### `query_submit_line.py`

**Purpose**: Parse the raw `submit_line` for submission type and CPU-request flags.

**Key findings**: interactive jobs are detected by `--pty` (~0.4% of jobs), not by the id_step = -6 step (essentially never created on CREATE); ~99% of jobs are submitted with `sbatch`. Also characterises how `--ntasks` / `--cpus-per-task` relate to `cpus_req`. See open_questions #9, #10.

### `query_job_flags.py`

**Purpose**: Explore the `flags` bitmask column.

**Key findings**: decoded the common flag bits from Slurm source; crucially, the flags do **not** reliably distinguish interactive jobs, which is why the pipeline detects submission type from `submit_line` instead.

### `query_sacct_steps.py` / `verify_sacct_mysql_compatibility.py`

**Purpose**: Validate the MySQL/TRES pipeline against `sacct`. Both need cluster access; `query_sacct_steps.py` also needs the `sacct` command.

**Key findings**: elapsed time, AllocCPUs, ReqMem, and MaxRSS match `sacct` essentially 100%. TotalCPU matches too, except for a handful of jobs where `sacct` reports `00:00:00` but the DB pipeline — summing the step table directly — finds the real CPU time (so the pipeline is, if anything, more complete than sacct's job-level field).

## Adding New Scripts

When creating new investigative scripts:

1. Name with `query_` prefix for consistency
2. Add comprehensive docstring explaining purpose and findings
3. Use path resolution pattern for config:
   ```python
   SCRIPT_DIR = Path(__file__).resolve().parent   # .resolve() so it works from any cwd
   PROJECT_ROOT = SCRIPT_DIR.parent
   CONFIG_FILE = PROJECT_ROOT / "config.yaml"
   OUTPUT_DIR = SCRIPT_DIR / "output"
   OUTPUT_DIR.mkdir(exist_ok=True)
   OUTPUT_FILE = OUTPUT_DIR / "output_scriptname.txt"
   ```
4. Output to both stdout and file
5. Document data sources (tables, columns) in docstring
6. Record key findings in docstring after running
