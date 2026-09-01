# Developer Documentation

This documentation is for developers working on the hpc-data-analysis codebase. It explains the Slurm accounting database structure, key insights discovered during development, and how the analysis tools work.

## Contents

1. [Slurm Database Schema](slurm_database_schema.md) - Tables, columns, and relationships
2. [Dev Scripts Guide](dev_scripts_guide.md) - What each query script investigates
3. [CPU Accounting](cpu_accounting.md) - Step types, CPU time sources, avoiding double-counting
4. [Memory Accounting](memory_accounting.md) - Units, --mem vs --mem-per-cpu encoding
5. [Job States and Flags](job_states_and_flags.md) - State codes, flag bitmasks
6. [TRES Encoding](tres_encoding.md) - TRES string format and common IDs
7. [Code Improvements](code_improvements.md) - Performance, correctness, testing, and workflow TODOs

## Recommended Reading Order

If you're new to this codebase:

1. Start with **TRES Encoding** to understand how Slurm stores resource data
2. Read **Slurm Database Schema** for the table structure
3. Review **CPU Accounting** and **Memory Accounting** for the main analysis challenges
4. Browse **Dev Scripts Guide** to see how insights were discovered

## Key Insights Summary

The hard-won database and coding gotchas from reverse-engineering the accounting schema:

- **Memory unit mismatch**: `tres_req` stores memory in **MB**, but `tres_usage_in_max` stores it in **bytes**. The efficiency calculation must convert (× 1024²), or it is wrong by a factor of ~1,000,000. See [Memory Accounting](memory_accounting.md).

- **`mem_req` packs a flag into bit 63**: the top bit of the 64-bit `mem_req` marks `--mem-per-cpu` (set) vs `--mem`/per-node (unset); the memory value is in the lower 63 bits. Prefer `tres_req` (TRES ID 2), which already stores the *total* memory and needs no decoding. See [Memory Accounting](memory_accounting.md).

- **CPU-time step aggregation**: sum the regular srun steps and fall back to the batch step only when there are no regular steps (never add both), to avoid double-counting. What the batch step's CPU represents in large multi-step jobs, and how multi-node jobs should be summed, is not fully settled — see [open_questions.md](../analysis/open_questions.md) #4, #5 and [CPU Accounting](cpu_accounting.md).

- **TRES CPU time is in milliseconds**, whereas the rusage `user_sec`/`sys_sec` columns are in seconds — the ratio between them is consistently ~1000. See [CPU Accounting](cpu_accounting.md) and [open_questions.md](../analysis/open_questions.md) #4.

- **Interactive detection uses `submit_line`, not the step type**: the interactive step (id_step = -6) is essentially never created on CREATE, so interactive jobs are found by the `--pty` flag in `submit_line`. See [open_questions.md](../analysis/open_questions.md) #9.
