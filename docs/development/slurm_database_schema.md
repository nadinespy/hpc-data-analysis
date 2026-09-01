# Slurm Database Schema

This document describes the key tables in the Slurm accounting database used by this project.

## Overview

The Slurm accounting database stores historical job data. The main tables we use are:

| Table | Purpose |
|-------|---------|
| `create_job_table` | One row per job - submission time, resources requested, state, etc. |
| `create_step_table` | One row per step - CPU time used, memory used, step type |
| `create_assoc_table` | User/account associations - maps `id_assoc` to usernames |
| `tres_table` | TRES type definitions - maps TRES IDs to names (cpu, mem, etc.) |

## create_job_table

Main job information table. Key columns:

| Column | Type | Description |
|--------|------|-------------|
| `job_db_inx` | INT | Primary key, internal database index |
| `id_job` | INT | Slurm job ID (user-visible) |
| `id_assoc` | INT | Foreign key to `create_assoc_table` |
| `state` | INT | Job state code (see [Job States](job_states_and_flags.md)) |
| `exit_code` | INT | Exit code from job |
| `time_submit` | INT | Unix timestamp when job was submitted |
| `time_start` | INT | Unix timestamp when job started running |
| `time_end` | INT | Unix timestamp when job finished |
| `timelimit` | INT | Requested time limit in **minutes** |
| `cpus_req` | INT | Number of CPUs requested |
| `mem_req` | BIGINT | Memory requested (see [Memory Accounting](memory_accounting.md)) |
| `nodes_alloc` | INT | Number of nodes allocated |
| `tres_req` | VARCHAR | TRES string of requested resources (see [TRES Encoding](tres_encoding.md)) |
| `flags` | INT | Job flags bitmask (see [Job States and Flags](job_states_and_flags.md)) |
| `partition` | VARCHAR | Partition name |

### Notes

- `timelimit` is in minutes, not seconds. Multiply by 60 for comparisons with elapsed time.
- `mem_req` has a special encoding - bit 63 indicates `--mem-per-cpu` vs `--mem`. See [Memory Accounting](memory_accounting.md).
- `tres_req` contains the canonical resource request. Prefer this over individual columns like `cpus_req`.

## create_step_table

Step-level information. Each job can have multiple steps.

| Column | Type | Description |
|--------|------|-------------|
| `job_db_inx` | INT | Foreign key to `create_job_table` |
| `id_step` | INT | Step ID (-5=batch, -6=interactive, -4=extern, >=0=srun steps) |
| `step_name` | VARCHAR | Step name ('batch', 'interactive', 'extern', or user-defined) |
| `user_sec` | INT | User CPU time in seconds |
| `sys_sec` | INT | System CPU time in seconds |
| `user_usec` | INT | User CPU time microseconds (fractional part) |
| `sys_usec` | INT | System CPU time microseconds (fractional part) |
| `tres_usage_in_max` | VARCHAR | TRES string of peak resource usage |

### Step Types

| id_step | step_name | Description |
|---------|-----------|-------------|
| -6 | interactive | Jobs submitted via `srun --pty` |
| -5 | batch | Jobs submitted via `sbatch` |
| -4 | extern | External step for job-level tracking |
| >= 0 | (varies) | Regular steps from `srun` commands (numbered 0, 1, 2, ...) |

See [CPU Accounting](cpu_accounting.md) for how to correctly sum CPU time across steps.

## create_assoc_table

Maps association IDs to users and accounts.

| Column | Type | Description |
|--------|------|-------------|
| `id_assoc` | INT | Primary key |
| `user` | VARCHAR | Username |
| `acct` | VARCHAR | Account name |

## tres_table

Defines TRES (Trackable RESources) types.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT | TRES ID used in TRES strings |
| `type` | VARCHAR | Resource type ('cpu', 'mem', 'node', etc.) |
| `name` | VARCHAR | Resource name (for GRES, e.g., 'gpu') |

### Common TRES IDs

| ID | Type | Notes |
|----|------|-------|
| 1 | cpu | CPU count (`tres_req`) or CPU time in **milliseconds** (`tres_usage_in_max`) |
| 2 | mem | Memory in MB (`tres_req`) or bytes (`tres_usage_in_max`) |
| 3 | energy | Energy in joules |
| 4 | node | Node count |
| 5 | billing | Billing units |
| 1001+ | gres | GPUs and other generic resources |

## Relationships

**Primary key (PK)** — the column that uniquely identifies each row in its own table; no two rows share the same value (e.g. `create_assoc_table.id_assoc`, `create_job_table.job_db_inx`).

**Foreign key (FK)** — a column in another table that stores a PK value in order to point back at that row. It is *not* unique in its own table: many jobs can carry the same `id_assoc` (FK) pointing to one association (PK), and many steps can carry the same `job_db_inx` (FK) pointing to one job. In the diagram, `FK -> PK` labels that pointing direction.

```text
                            +--------------------+
          id_assoc  ------> | create_assoc_table |   resolve a job to its user
         (who ran it)       |  id_assoc (PK)     |   (LDAP then maps user ->
                            |  user, acct        |    faculty)
                            +--------------------+
                                      ^
                                      | id_assoc  (FK -> PK)
                                      |
    +----------------------+   job_db_inx      +-----------------------+
    | create_job_table     | --------------->  | create_step_table     |
    |  job_db_inx (PK)     |  one job ->       |  job_db_inx (FK)      |
    |  id_job (Slurm ID)   |  many steps       |  id_step (-5/-6/>=0)  |
    |  id_assoc (FK)       |                   |  user_sec, sys_sec    |
    |  state, timelimit    |                   |  tres_usage_in_max    |
    |  cpus_req, mem_req   |                   +-----------+-----------+
    |  tres_req/tres_alloc |                               |
    +----------+-----------+                               |
               |                                           |
               |   numeric TRES IDs inside every           |
               |   tres_* string (1=cpu, 2=mem, 4=node)    |
               +---------------------+---------------------+
                                     v
                           +---------------------+
                           | tres_table          |
                           |  id -> type, name   |
                           +---------------------+
```

- **`create_job_table.id_assoc` -> `create_assoc_table.id_assoc`**: resolves a job to a username (LDAP then maps username -> faculty).
- **`create_step_table.job_db_inx` -> `create_job_table.job_db_inx`**: one job links to one or more steps.
- **`tres_table`**: a lookup for the numeric IDs used inside every TRES string (`tres_req`, `tres_alloc`, `tres_usage_in_max`). Slurm does not declare these as foreign keys — the links are by convention, maintained by Slurm's own code, not enforced by the database. You can confirm this by running `SHOW CREATE TABLE create_job_table`: the output lists the columns and indexes but contains no `CONSTRAINT ... FOREIGN KEY ... REFERENCES ...` clauses, which is what an enforced FK would look like.

## Example Query

Get jobs with their usernames and step data:

```sql
SELECT
    j.id_job,
    a.user,
    j.state,
    j.cpus_req,
    s.id_step,
    s.user_sec,
    s.sys_sec
FROM create_job_table j
JOIN create_assoc_table a ON j.id_assoc = a.id_assoc
LEFT JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
WHERE j.time_submit >= UNIX_TIMESTAMP('2025-01-01')
  AND j.time_submit < UNIX_TIMESTAMP('2025-02-01')
```
