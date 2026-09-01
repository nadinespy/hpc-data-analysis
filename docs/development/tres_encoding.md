# TRES Encoding

TRES (Trackable RESources) is Slurm's system for tracking resource requests and usage. This document explains the TRES string format used in the accounting database.

## TRES String Format

TRES data is stored as comma-separated `ID=value` pairs:

```
1=8,2=16384,4=1
```

This means:
- TRES ID 1 (CPU) = 8
- TRES ID 2 (Memory) = 16384
- TRES ID 4 (Node) = 1

## Common TRES IDs

| ID | Type | Unit (tres_req) | Unit (tres_usage_in_max) | Notes |
|----|------|-----------------|-------------------|-------|
| 1 | cpu | count | CPU-**milliseconds** | request = CPU count; usage = CPU time in ms (see [CPU Accounting](cpu_accounting.md)) |
| 2 | mem | MB | **bytes** | unit differs between request and usage — see [Memory Accounting](memory_accounting.md) |
| 3 | energy | joules | joules | Energy consumed |
| 4 | node | count | count | Nodes |
| 5 | billing | units | units | Billing/fairshare units |
| 6 | fs/disk | varies | bytes | Filesystem usage |
| 7 | vmem | MB | bytes | Virtual memory |
| 1001+ | gres/* | count | varies | GPUs, other GRES |

Two of these units are the ones that most easily cause bugs: memory (ID 2) is **MB in `tres_req` but bytes in `tres_usage_in_max`**, and CPU (ID 1) usage is in **milliseconds**. The memory conversion (`× 1024²`) is covered in [Memory Accounting](memory_accounting.md).

## Parsing TRES Strings

### Python Implementation

```python
def parse_tres_value(tres_string, tres_id):
    """
    Parse a TRES string and extract value for given ID.

    Args:
        tres_string: TRES format string like "1=8,2=16384"
        tres_id: The TRES ID to extract (e.g., 2 for memory)

    Returns:
        Integer value, or 0 if not found.
    """
    if not tres_string:
        return 0
    try:
        for pair in tres_string.split(','):
            if '=' in pair:
                tid, value = pair.split('=', 1)
                if int(tid) == tres_id:
                    return int(value)
    except (ValueError, AttributeError):
        pass
    return 0

# Usage
tres_req = "1=8,2=16384,4=1"
cpus = parse_tres_value(tres_req, 1)      # 8
memory_mb = parse_tres_value(tres_req, 2)  # 16384
nodes = parse_tres_value(tres_req, 4)      # 1
```

### SQL Implementation

Extract memory (TRES ID 2) from a TRES string in MySQL:

```sql
CAST(
    SUBSTRING_INDEX(
        SUBSTRING_INDEX(CONCAT(',', tres_usage_in_max), ',2=', -1),
        ',', 1
    ) AS UNSIGNED
) AS max_mem_bytes
```

How it works:
1. `CONCAT(',', tres_string)` - Ensures string starts with comma for consistent parsing
2. `SUBSTRING_INDEX(..., ',2=', -1)` - Gets everything after `,2=`
3. `SUBSTRING_INDEX(..., ',', 1)` - Gets just the value (before next comma)
4. `CAST(... AS UNSIGNED)` - Converts to integer

### Which one the code uses

Both — for different purposes:

- The **SQL** form runs inside `fetch_job_data()` to pull the peak memory out of `tres_usage_in_max` while aggregating steps. It matters that the extraction is numeric: `tres_usage_in_max` is a string, so a plain SQL `MAX()` would compare **lexicographically** and could return the wrong step's value. Extracting the number with `SUBSTRING_INDEX` + `CAST` and taking `MAX` of *that* gives the true numeric peak.
- The **Python** `parse_tres_value()` (in `slurm_utils.py`) runs afterwards in `calculate_job_metrics()` to read scalar values (CPUs, memory) out of the job-level `tres_req` / `tres_alloc` strings the query returns.

## TRES Fields in Database

| Table | Column | Contains |
|-------|--------|----------|
| create_job_table | tres_req | Requested resources |
| create_job_table | tres_alloc | Allocated resources |
| create_step_table | tres_usage_in_max | Peak usage per step |
| create_step_table | tres_usage_in_min | Minimum usage per step |
| create_step_table | tres_usage_in_tot | Total usage per step |
| create_step_table | tres_usage_out_max | Max output (e.g., disk writes) |

## GPU TRES

GPUs are tracked as GRES (Generic RESources) with IDs >= 1001. This is observed directly in `tres_table` on CREATE (`query_tres.py`), which lists `1001 = gres/gpu`, `1004 = gres/gpuutil`, and `1007 = gres/gpumem`; a GPU request then appears in the TRES string as, e.g., `1001=2`:

```
1=8,2=16384,4=1,1001=2
```

The exact IDs depend on cluster configuration, so query `tres_table` to confirm them:

```sql
SELECT id, type, name FROM tres_table WHERE type = 'gres' AND name LIKE '%gpu%';
```

**Note:** this project only *identified* the GPU TRES IDs; it does not compute GPU efficiency. GPU analysis is flagged as future work in the blog post.

## Investigation Scripts

- `query_tres.py` - Dumps `tres_table` (and sample TRES strings) to show all TRES IDs and their meanings
- `query_memory.py` - Investigates memory unit encoding
- `query_cpu_mem_diagnostics.py` - Compares TRES values with dedicated columns

## Related Documentation

- [Memory Accounting](memory_accounting.md) - Memory-specific TRES handling
- [CPU Accounting](cpu_accounting.md) - CPU-specific TRES handling
- [Slurm Database Schema](slurm_database_schema.md) - Where TRES columns appear
