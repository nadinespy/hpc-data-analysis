# CPU Accounting

This document explains how CPU time is recorded in the Slurm accounting database and how to correctly calculate CPU efficiency.

## Data Sources

CPU time data comes from two sources in `create_step_table`:

### 1. rusage columns (user_sec, sys_sec, user_usec, sys_usec)

- Source: the OS **rusage** ("resource usage") struct, filled by the `getrusage()` / `wait4()` system calls when a process exits — Slurm records its user/system CPU-time fields into these columns
- Captures: CPU time for **local processes only**
- Limitation: For distributed steps (srun across nodes), only captures head node activity

```
Total CPU (seconds) = user_sec + sys_sec + (user_usec + sys_usec) / 1,000,000
```

### 2. TRES usage (tres_usage_in_max)

- Source: cgroup accounting
- Captures: CPU time including processes on **remote nodes** (unlike rusage)
- Format: TRES string like `1=500,2=12345` where ID 1 = CPU time in **milliseconds** — not seconds; the ratio to `user_sec+sys_sec` is consistently ~1000 (see [open_questions.md](../analysis/open_questions.md) #4)
- Limitation: Not always populated for all step types

## Step Types and CPU Time

| id_step | step_name | rusage data? | TRES data? | Notes |
|---------|-----------|--------------|------------|-------|
| -5 | batch | Yes | Yes | Script execution on head node |
| -6 | interactive | Yes | Yes | Interactive session |
| >= 0 | (srun steps) | Often 0 (work ran on remote nodes) | Yes | Real work; often distributed across nodes |

On CREATE the only special steps observed are `batch` (-5) and `interactive` (-6); the standard Slurm `extern` step (-4) does not appear in the data (`query_step_diagnostics.py`, `discover_special_steps()`).

**Key insight (and an open question)**: regular srun steps often have `user_sec = 0` while their TRES CPU value is non-zero — the work ran on remote nodes, which the head-node rusage columns don't see but cgroup/TRES accounting does. The pipeline currently reads CPU time **only from the rusage columns**, so genuinely multi-node jobs are undercounted; using TRES for those jobs is a possible fix but is not done. See [open_questions.md](../analysis/open_questions.md) #4.

## Calculating Total CPU Time

### The Problem

A job might have:
- A batch step (id_step=-5) with CPU time for running the batch script
- One or more srun steps (id_step>=0) with CPU time for the actual work

If you sum ALL steps, you may **double-count** - the batch step runs the script that launches srun, so there's overlap.

### The Solution

Use this priority order:

1. **If regular steps exist (id_step >= 0)**: Sum CPU time from regular steps only
2. **If no regular steps**: Use batch/interactive step CPU time

This is implemented in `fetch_job_data()`:

```sql
COALESCE(
    NULLIF(SUM(CASE WHEN s.id_step NOT IN (-5, -6, -4)
                    THEN s.user_sec ELSE 0 END), 0),
    MAX(CASE WHEN s.id_step = -5 THEN s.user_sec END),
    0
) AS total_user_sec
```

Logic:
1. Sum regular steps (excluding special step IDs)
2. If that sum is 0 or NULL, fall back to batch step
3. If still NULL, default to 0

**Remaining uncertainties**: this avoids the obvious double-count (never adding batch + regular), but two things are not fully settled — what the batch step's CPU represents when it is large in a multi-step job (genuine separate work vs. an overlap with the srun steps), and the multi-node undercount described above. Both are tracked in [open_questions.md](../analysis/open_questions.md) #4, #5.

The CPU-time value computed here is the numerator of CPU efficiency; the denominator is `elapsed × requested CPUs`, with requested CPUs taken from `tres_req` TRES ID 1 (falling back to the `cpus_req` column). The efficiency formula itself is an analysis concern — see [Efficiency Metrics](../analysis/efficiency_metrics.md).

## Related Documentation

- [TRES Encoding](tres_encoding.md) - How TRES strings are formatted
- [Slurm Database Schema](slurm_database_schema.md) - Table structure
- [Dev Scripts Guide](dev_scripts_guide.md) - Scripts that investigate CPU accounting
