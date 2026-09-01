# Job States and Flags

This document describes job state codes and flag bitmasks in the Slurm accounting database.

## Job States

The `state` column in `create_job_table` contains a numeric code:

| Code | Name | Description |
|------|------|-------------|
| 0 | PENDING | Job is waiting in queue |
| 1 | RUNNING | Job is currently running |
| 2 | SUSPENDED | Job has been suspended |
| 3 | COMPLETED | Job finished successfully |
| 4 | CANCELLED | Job was cancelled by user or admin |
| 5 | FAILED | Job terminated with non-zero exit code |
| 6 | TIMEOUT | Job reached its time limit |
| 7 | NODE_FAIL | Job terminated due to node failure |
| 8 | PREEMPTED | Job was preempted by higher priority job |
| 9 | BOOT_FAIL | Job terminated due to boot failure |
| 10 | DEADLINE | Job missed its deadline |
| 11 | OUT_OF_MEMORY | Job killed for exceeding memory limit |

The numeric codes are not in Slurm's public documentation — they come from Slurm's C source (`slurm.h`) and were confirmed against the data (`query_job_states.py`). `slurm_utils.py` exposes them as named constants (`JOB_STATE_COMPLETED`, etc.) together with the `INCLUDED_STATES`, `SUCCESS_STATES`, and `FAILED_STATES` sets. *Which* states are included in efficiency analysis, and why, is an analysis decision — see [Data Caveats](../analysis/data_caveats.md).

## Job Flags

The `flags` column is a **bitmask**: one integer whose individual bits each act as an independent yes/no switch for a job attribute (e.g. "ntasks was set explicitly", "using the default partition"). Many attributes are packed into a single number, and you read them back by testing individual bits. Slurm defines which bit means what in its C source — nothing in the database documents it — so we decoded the values empirically with `query_job_flags.py`.

> **Not to be confused with the `mem_req` bit-63 flag.** That is a separate column and a separate bitmask: its top bit distinguishes `--mem` from `--mem-per-cpu`, and the pipeline *does* use it (see [Memory Accounting](memory_accounting.md)). The `flags` column below is unrelated — even its `JOB_MEM_SET` bit only records *that* memory was set, not which mode was used.

**The analysis pipeline does not use the `flags` column.** We investigated it as a possible way to tell interactive jobs from batch jobs. The method (in `query_job_flags.py`) was: (1) find the most common `flags` values in the data, (2) run each through `decode_flags` to see *which named bits* were set, checking whether any bit looked like an "interactive" marker, and (3) cross-check by grouping the `flags` of jobs that actually had an interactive step (`id_step = -6`) to see if they shared a distinctive pattern. The result was negative: the set bits are all generic scheduler/state attributes (`JOB_NTASKS_SET`, `USE_DEFAULT_PART`, and so on) — none tracks how the job was submitted — and step (3) had almost no `-6` jobs to correlate against in the first place. So the `flags` column does not carry that signal reliably, and interactive detection is done from `submit_line` instead (see [CPU Accounting](cpu_accounting.md)). The decoder below is kept as a utility for anyone exploring the column.

### Decoding Flags

```python
def decode_flags(flags_val):
    """Decode flag bits into names."""
    flag_names = {
        0x0001: "KILL_INV_DEP",
        0x0002: "NO_KILL_INV_DEP",
        0x1000: "JOB_NTASKS_SET",
        0x2000: "JOB_CPUS_SET",
        0x200000: "JOB_MEM_SET",
        # ... add others as needed
    }
    bits = []
    for bit, name in flag_names.items():
        if flags_val & bit:
            bits.append(name)
    return bits

# Example
flags = 0x203200  # JOB_MEM_SET | JOB_CPUS_SET | TRES_STR_CALC
print(decode_flags(flags))  # ['TRES_STR_CALC', 'JOB_CPUS_SET', 'JOB_MEM_SET']
```

## Investigation Scripts

- `query_job_flags.py` - Analyses flag distributions and decodes common values (and shows flags don't reliably mark interactive jobs)
- `query_submit_line.py` - Detects submission type (batch vs interactive) from the raw `submit_line`

## Related Documentation

- [Slurm Database Schema](slurm_database_schema.md) - Table structure
- [CPU Accounting](cpu_accounting.md) - Step types and their meanings
