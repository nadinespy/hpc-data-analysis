#!/usr/bin/env python3
"""Diagnostic script to investigate CPU and memory efficiency edge cases.

Tests:
1.  Does allocated CPUs differ from requested CPUs? (Could explain CPU eff > 100%)
    Also reports single-CPU vs multi-CPU job breakdown.
1b. Does cpus_req column match the CPU value in tres_req? (Verify column vs TRES string consistency)
2.  Does mem_req encode --mem vs --mem-per-cpu via flag bit?
3.  Does tres_req memory match mem_req for both --mem and --mem-per-cpu jobs?
4.  Are jobs with mem eff > 100% correlated with --mem-per-cpu?

All data comes from the Slurm accounting database tables:
- create_job_table: job-level records (cpus_req, mem_req, tres_req, tres_alloc, etc.)
- create_step_table: step-level records (tres_usage_in_max for actual memory usage)
- tres_table: TRES ID mappings (1=CPU, 2=memory in MB, 4=node, etc.)

Saves output to output_cpu_mem_diagnostics.txt
"""

import sys
from pathlib import Path
import mysql.connector
import yaml

# Paths relative to script location (allows running from any directory)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "output_cpu_mem_diagnostics.txt"
with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)
mysql_conf = config["mysql"]
conn = mysql.connector.connect(
    host=mysql_conf["host"],
    user=mysql_conf["user"],
    password=mysql_conf["password"],
    database=mysql_conf["database"],
)
cur = conn.cursor()

with open(OUTPUT_FILE, 'w') as f:
    def out(text=""):
        print(text)
        print(text, file=f)

    # =========================================================================
    # Part 1: CPU requests — requested vs allocated, single vs multi-CPU
    # =========================================================================
    out("=" * 80)
    out("PART 1: CPU requests — requested vs allocated, single vs multi-CPU")
    out("=" * 80)
    out()
    out("Columns (all from create_job_table):")
    out("  - cpus_req: direct column storing requested CPUs")
    out("  - tres_alloc: TRES-encoded string; TRES ID 1 = allocated CPUs")
    out("    Format: '1=N,2=M,...' where 1=CPU, 2=memory(MB), 4=node, etc.")

    # Compare cpus_req vs tres_alloc CPU value
    try:
        cur.execute("""
            SELECT
                j.cpus_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS alloc_cpus,
                COUNT(*) as cnt
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
            GROUP BY cpus_req, alloc_cpus
            ORDER BY cnt DESC
            LIMIT 30
        """)
        out()
        out("cpus_req vs tres_alloc CPU (top 30 combos by frequency):")
        out(f"  {'cpus_req':>10}  {'tres_alloc':>12}  {'count':>12}  {'%':>8}  {'comparison':>15}")
        out(f"  {'(column)':>10}  {'(TRES ID 1)':>12}")
        rows = list(cur)
        total_shown = sum(r[2] for r in rows)
        for cpus_req, alloc_cpus, cnt in rows:
            if cpus_req == alloc_cpus:
                cmp = "EQUAL"
            elif alloc_cpus > cpus_req:
                cmp = "alloc > req"
            else:
                cmp = "alloc < req"
            pct = (cnt / total_shown * 100) if total_shown else 0
            out(f"  {cpus_req:>10}  {alloc_cpus:>12}  {cnt:>12}  {pct:>7.2f}%  {cmp:>15}")
    except Exception as e:
        out(f"Error comparing: {e}")

    # Count jobs where they differ + single vs multi-CPU breakdown
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN j.cpus_req = CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) THEN 1 ELSE 0 END) as equal_count,
                SUM(CASE WHEN j.cpus_req != CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) THEN 1 ELSE 0 END) as differ_count,
                SUM(CASE WHEN j.cpus_req = 1 THEN 1 ELSE 0 END) as single_cpu,
                SUM(CASE WHEN j.cpus_req > 1 THEN 1 ELSE 0 END) as multi_cpu
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
        """)
        row = cur.fetchone()
        total, equal, differ, single_cpu, multi_cpu = row
        equal_pct = (equal / total * 100) if total else 0
        differ_pct = (differ / total * 100) if total else 0
        single_pct = (single_cpu / total * 100) if total else 0
        multi_pct = (multi_cpu / total * 100) if total else 0
        out()
        out(f"Summary (cpus_req vs tres_alloc):")
        out(f"  Total jobs:  {total:>12}")
        out(f"  Equal:       {equal:>12}  ({equal_pct:>6.2f}%)")
        out(f"  Differ:      {differ:>12}  ({differ_pct:>6.2f}%)")
        out()
        out(f"Summary (single vs multi-CPU jobs):")
        out(f"  Single-CPU (cpus_req = 1):  {single_cpu:>12}  ({single_pct:>6.2f}%)")
        out(f"  Multi-CPU (cpus_req > 1):   {multi_cpu:>12}  ({multi_pct:>6.2f}%)")
    except Exception as e:
        out(f"Error counting: {e}")

    # Show sample jobs where they differ
    try:
        cur.execute("""
            SELECT j.id_job, j.cpus_req,
                   CAST(
                       SUBSTRING_INDEX(
                           SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                           ',', 1
                       ) AS UNSIGNED
                   ) AS alloc_cpus,
                   j.nodes_alloc, j.tres_req, j.tres_alloc
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.cpus_req != CAST(
                  SUBSTRING_INDEX(
                      SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                      ',', 1
                  ) AS UNSIGNED
              )
            ORDER BY j.id_job DESC
            LIMIT 10
        """)
        out()
        out("Sample jobs where cpus_req (column) != tres_alloc CPU (TRES ID 1):")
        out(f"  {'id_job':>10}  {'cpus_req':>10}  {'tres_alloc':>12}  {'nodes':>6}  tres_req / tres_alloc")
        out(f"  {'':>10}  {'(column)':>10}  {'(TRES ID 1)':>12}  {'_alloc':>6}")
        for row in cur:
            out(f"  {row[0]:>10}  {row[1]:>10}  {row[2]:>12}  {row[3]:>6}  {row[4]} / {row[5]}")
    except Exception as e:
        out(f"Error fetching differing jobs: {e}")

    # =========================================================================
    # Part 1b: cpus_req vs tres_req CPU (column vs TRES string consistency)
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 1b: cpus_req vs tres_req CPU — column vs TRES string consistency")
    out("=" * 80)
    out()
    out("Columns (all from create_job_table):")
    out("  - cpus_req: direct column storing requested CPUs")
    out("  - tres_req: TRES-encoded string; TRES ID 1 = requested CPUs")
    out("Checking if these two sources of 'requested CPUs' are consistent.")

    # Compare cpus_req vs tres_req CPU value
    try:
        cur.execute("""
            SELECT
                j.cpus_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS req_cpus_from_tres,
                COUNT(*) as cnt
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
            GROUP BY cpus_req, req_cpus_from_tres
            ORDER BY cnt DESC
            LIMIT 30
        """)
        out()
        out("cpus_req vs tres_req CPU (top 30 combos by frequency):")
        out(f"  {'cpus_req':>10}  {'tres_req':>12}  {'count':>12}  {'%':>8}  {'comparison':>15}")
        out(f"  {'(column)':>10}  {'(TRES ID 1)':>12}")
        rows = list(cur)
        total_shown = sum(r[2] for r in rows)
        for cpus_req, tres_req_cpu, cnt in rows:
            if cpus_req == tres_req_cpu:
                cmp = "EQUAL"
            elif tres_req_cpu > cpus_req:
                cmp = "tres > col"
            else:
                cmp = "tres < col"
            pct = (cnt / total_shown * 100) if total_shown else 0
            out(f"  {cpus_req:>10}  {tres_req_cpu:>12}  {cnt:>12}  {pct:>7.2f}%  {cmp:>15}")
    except Exception as e:
        out(f"Error comparing: {e}")

    # Count jobs where cpus_req differs from tres_req CPU
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN j.cpus_req = CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) THEN 1 ELSE 0 END) as equal_count,
                SUM(CASE WHEN j.cpus_req != CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) THEN 1 ELSE 0 END) as differ_count
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
        """)
        row = cur.fetchone()
        total, equal, differ = row
        equal_pct = (equal / total * 100) if total else 0
        differ_pct = (differ / total * 100) if total else 0
        out()
        out(f"Summary:")
        out(f"  Total jobs:  {total:>12}")
        out(f"  Equal:       {equal:>12}  ({equal_pct:>6.2f}%)")
        out(f"  Differ:      {differ:>12}  ({differ_pct:>6.2f}%)")
    except Exception as e:
        out(f"Error counting: {e}")

    # Show sample jobs where cpus_req differs from tres_req CPU
    try:
        cur.execute("""
            SELECT j.id_job, j.cpus_req,
                   CAST(
                       SUBSTRING_INDEX(
                           SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',1=', -1),
                           ',', 1
                       ) AS UNSIGNED
                   ) AS tres_req_cpu,
                   j.tres_req
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.cpus_req != CAST(
                  SUBSTRING_INDEX(
                      SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',1=', -1),
                      ',', 1
                  ) AS UNSIGNED
              )
            ORDER BY j.id_job DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        out()
        if rows:
            out("Sample jobs where cpus_req (column) != tres_req (TRES ID 1):")
            out(f"  {'id_job':>10}  {'cpus_req':>10}  {'tres_req':>12}  tres_req (full string)")
            out(f"  {'':>10}  {'(column)':>10}  {'(TRES ID 1)':>12}")
            for row in rows:
                out(f"  {row[0]:>10}  {row[1]:>10}  {row[2]:>12}  {row[3]}")
        else:
            out("No jobs found where cpus_req != tres_req CPU (they always match)")
    except Exception as e:
        out(f"Error fetching differing jobs: {e}")

    # =========================================================================
    # Part 2: mem_req flag bit (--mem vs --mem-per-cpu)
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 2: mem_req flag bit — --mem vs --mem-per-cpu distribution")
    out("=" * 80)
    out()
    out("Column (from create_job_table):")
    out("  - mem_req: bigint(20) unsigned, stores requested memory")
    out("    Bit 63 (MSB) encodes memory type: 0 = --mem (per-node), 1 = --mem-per-cpu")
    out("    Lower 63 bits store the actual memory value in MB")

    # All memory-related columns
    out()
    out("All memory-related columns in create_job_table:")
    cur.execute("SHOW COLUMNS FROM create_job_table LIKE '%mem%'")
    for row in cur:
        out(f"  {row}")

    # Check flag bit distribution - fixed query using subquery to avoid GROUP BY alias issue
    out()
    out("mem_req flag bit distribution:")
    try:
        cur.execute("""
            SELECT mem_type, COUNT(*) as cnt
            FROM (
                SELECT
                    CASE WHEN mem_req & 0x8000000000000000 != 0 THEN '--mem-per-cpu (bit 63 set)'
                         ELSE '--mem (per-node, no flag)' END as mem_type
                FROM create_job_table
                WHERE time_start > 0 AND time_end > 0
            ) sub
            GROUP BY mem_type
        """)
        rows = list(cur)
        total = sum(r[1] for r in rows)
        out(f"  {'Memory type':>30}  {'Count':>12}  {'%':>8}")
        for mem_type, cnt in rows:
            pct = (cnt / total * 100) if total else 0
            out(f"  {mem_type:>30}  {cnt:>12}  {pct:>7.2f}%")
        out()
        out(f"  Total: {total:>12}")
    except Exception as e:
        out(f"Error checking flag bit: {e}")

    # =========================================================================
    # Part 3: Compare mem_req value with tres_req memory value
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 3: mem_req vs tres_req memory — comparing column with TRES string")
    out("=" * 80)
    out()
    out("Columns (all from create_job_table):")
    out("  - mem_req: bigint storing memory request (bit 63 = per-cpu flag, lower 63 bits = value in MB)")
    out("  - tres_req: TRES-encoded string; TRES ID 2 = total requested memory in MB")
    out()
    out("Expected relationship:")
    out("  - --mem-per-cpu jobs: (mem_req value) × cpus_req = tres_req memory")
    out("  - --mem (per-node) jobs: mem_req value = tres_req memory directly")

    # -------------------------------------------------------------------------
    # Part 3a: --mem-per-cpu jobs
    # -------------------------------------------------------------------------
    out()
    out("-" * 40)
    out("Part 3a: --mem-per-cpu jobs (bit 63 set)")
    out("-" * 40)

    # Sample --mem-per-cpu jobs (flag bit set) with multi-CPU
    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req,
                j.mem_req & 0x8000000000000000 AS is_per_cpu,
                j.mem_req & 0x7FFFFFFFFFFFFFFF AS mem_value_raw,
                j.tres_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS tres_req_mem
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 != 0
              AND j.cpus_req > 1
            ORDER BY j.id_job DESC
            LIMIT 15
        """)
        out()
        out("Sample --mem-per-cpu jobs with cpus_req > 1:")
        out(f"  {'id_job':>10}  {'cpus_req':>8}  {'mem_req':>20}  {'mem_value':>12}  {'tres_req':>10}  {'tres/cpus':>10}  tres_req (string)")
        out(f"  {'':>10}  {'(column)':>8}  {'(column, raw)':>20}  {'(lower 63b)':>12}  {'(TRES ID 2)':>10}  {'':>10}")
        for row in cur:
            id_job, cpus, mem_req_raw, is_per_cpu, mem_value, tres_req, tres_mem = row
            tres_per_cpu = tres_mem // cpus if cpus > 0 and tres_mem else 0
            out(f"  {id_job:>10}  {cpus:>8}  {mem_req_raw:>20}  {mem_value:>12}  {tres_mem:>10}  {tres_per_cpu:>10}  {tres_req}")
    except Exception as e:
        out(f"Error: {e}")

    # Verify: for --mem-per-cpu jobs, does mem_value × cpus_req = tres_mem?
    out()
    out("Verification: for --mem-per-cpu jobs, does (mem_value × cpus_req) = tres_req memory?")
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN (j.mem_req & 0x7FFFFFFFFFFFFFFF) * j.cpus_req =
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) THEN 1 ELSE 0 END) as match_count,
                SUM(CASE WHEN (j.mem_req & 0x7FFFFFFFFFFFFFFF) * j.cpus_req !=
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) THEN 1 ELSE 0 END) as differ_count
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 != 0
        """)
        row = cur.fetchone()
        total, match, differ = row
        match_pct = (match / total * 100) if total else 0
        differ_pct = (differ / total * 100) if total else 0
        out(f"  Total --mem-per-cpu jobs:  {total:>12}")
        out(f"  Match (formula holds):     {match:>12}  ({match_pct:>6.2f}%)")
        out(f"  Differ (formula fails):    {differ:>12}  ({differ_pct:>6.2f}%)")
    except Exception as e:
        out(f"Error: {e}")

    # Sample --mem-per-cpu jobs showing the calculation
    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req & 0x7FFFFFFFFFFFFFFF AS mem_value,
                (j.mem_req & 0x7FFFFFFFFFFFFFFF) * j.cpus_req AS calculated_total,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS tres_mem,
                j.tres_req
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 != 0
            ORDER BY j.id_job DESC
            LIMIT 10
        """)
        out()
        out("Sample --mem-per-cpu jobs (showing mem_value × cpus = calculated vs tres_req memory):")
        out(f"  {'id_job':>10}  {'cpus_req':>8}  {'mem_value':>10}  {'calculated':>12}  {'tres_req':>10}  {'match?':>7}")
        out(f"  {'':>10}  {'(column)':>8}  {'(lower 63b)':>10}  {'(val×cpus)':>12}  {'(TRES ID 2)':>10}")
        for row in cur:
            id_job, cpus, mem_value, calculated, tres_mem, tres_req = row
            match = "YES" if calculated == tres_mem else "NO"
            out(f"  {id_job:>10}  {cpus:>8}  {mem_value:>10}  {calculated:>12}  {tres_mem:>10}  {match:>7}")
    except Exception as e:
        out(f"Error: {e}")

    # -------------------------------------------------------------------------
    # Part 3b: --mem (per-node) jobs
    # -------------------------------------------------------------------------
    out()
    out("-" * 40)
    out("Part 3b: --mem (per-node) jobs (bit 63 not set)")
    out("-" * 40)
    out()
    out("For --mem (per-node) jobs, mem_value should equal tres_req memory directly:")
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN (j.mem_req & 0x7FFFFFFFFFFFFFFF) =
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) THEN 1 ELSE 0 END) as match_count,
                SUM(CASE WHEN (j.mem_req & 0x7FFFFFFFFFFFFFFF) !=
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) THEN 1 ELSE 0 END) as differ_count
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 = 0
              AND j.mem_req > 0
        """)
        row = cur.fetchone()
        total, match, differ = row
        match_pct = (match / total * 100) if total else 0
        differ_pct = (differ / total * 100) if total else 0
        out(f"  Total --mem (per-node) jobs:  {total:>12}")
        out(f"  Match (mem_value = tres):     {match:>12}  ({match_pct:>6.2f}%)")
        out(f"  Differ:                       {differ:>12}  ({differ_pct:>6.2f}%)")
    except Exception as e:
        out(f"Error: {e}")

    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req & 0x7FFFFFFFFFFFFFFF AS mem_value,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS tres_mem,
                j.tres_req
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 = 0
              AND j.cpus_req > 1
              AND j.mem_req > 0
            ORDER BY j.id_job DESC
            LIMIT 10
        """)
        out()
        out("Sample --mem (per-node) jobs:")
        out(f"  {'id_job':>10}  {'cpus_req':>8}  {'mem_value':>12}  {'tres_req':>10}  {'match?':>8}")
        out(f"  {'':>10}  {'(column)':>8}  {'(lower 63b)':>12}  {'(TRES ID 2)':>10}")
        for row in cur:
            id_job, cpus, mem_value, tres_mem, tres_req = row
            match = "YES" if mem_value == tres_mem else "NO"
            out(f"  {id_job:>10}  {cpus:>8}  {mem_value:>12}  {tres_mem:>10}  {match:>8}")
    except Exception as e:
        out(f"Error: {e}")

    # =========================================================================
    # Part 4: Jobs with memory efficiency > 100% — what mem type?
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 4: Jobs with apparent memory efficiency > 100% — breakdown by mem type")
    out("=" * 80)
    out()
    out("Data sources:")
    out("  - tres_req (TRES ID 2) from create_job_table: requested memory in MB")
    out("  - tres_usage_in_max (TRES ID 2) from create_step_table: peak memory usage in bytes")
    out("  - mem_req (bit 63) from create_job_table: distinguishes --mem vs --mem-per-cpu")
    out()
    out("Memory efficiency = (maxrss bytes) / (tres_req MB × 1024²) × 100%")

    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req,
                CASE WHEN j.mem_req & 0x8000000000000000 != 0 THEN 'per-cpu' ELSE 'per-node' END as mem_type,
                j.mem_req & 0x7FFFFFFFFFFFFFFF AS mem_value,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS tres_req_mem,
                MAX(
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', s.tres_usage_in_max), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    )
                ) AS maxrss
            FROM create_job_table j
            JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.state = 3
            GROUP BY j.job_db_inx
            HAVING maxrss > 0 AND tres_req_mem > 0
                   AND maxrss > tres_req_mem * 1024 * 1024
            ORDER BY j.id_job DESC
            LIMIT 20
        """)
        out()
        out("Sample completed jobs where maxrss > tres_req × 1M (mem eff > 100%):")
        out(f"  {'id_job':>10}  {'cpus_req':>8}  {'mem_type':>10}  {'mem_value':>10}  {'tres_req':>10}  {'maxrss':>15}  {'eff%':>8}")
        out(f"  {'':>10}  {'(column)':>8}  {'(bit 63)':>10}  {'(lower 63b)':>10}  {'(TRES ID 2)':>10}  {'(step TRES)':>15}")
        for row in cur:
            id_job, cpus, mem_req, mem_type, mem_value, tres_mem, maxrss = row
            reqmem_bytes = tres_mem * 1024 * 1024
            eff = (maxrss / reqmem_bytes * 100) if reqmem_bytes > 0 else 0
            out(f"  {id_job:>10}  {cpus:>8}  {mem_type:>10}  {mem_value:>10}  {tres_mem:>10}  {maxrss:>15}  {eff:>7.1f}%")
    except Exception as e:
        out(f"Error: {e}")

    # Count by mem_type for jobs with eff > 100% (using subquery)
    try:
        cur.execute("""
            SELECT mem_type, COUNT(*) as cnt FROM (
                SELECT
                    j.job_db_inx,
                    CASE WHEN j.mem_req & 0x8000000000000000 != 0 THEN 'per-cpu' ELSE 'per-node' END as mem_type,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS tres_req_mem,
                    MAX(
                        CAST(
                            SUBSTRING_INDEX(
                                SUBSTRING_INDEX(CONCAT(',', s.tres_usage_in_max), ',2=', -1),
                                ',', 1
                            ) AS UNSIGNED
                        )
                    ) AS maxrss
                FROM create_job_table j
                JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
                WHERE j.time_start > 0 AND j.time_end > 0 AND j.state = 3
                GROUP BY j.job_db_inx
                HAVING maxrss > tres_req_mem * 1024 * 1024
            ) sub
            GROUP BY mem_type
        """)
        out()
        out("Breakdown of mem eff > 100% jobs by memory request type:")
        rows = list(cur)
        total = sum(r[1] for r in rows)
        out(f"  {'Memory type':>15}  {'Count':>12}  {'%':>8}")
        for mem_type, cnt in rows:
            pct = (cnt / total * 100) if total else 0
            out(f"  {mem_type:>15}  {cnt:>12}  {pct:>7.2f}%")
        out()
        out(f"  Total: {total:>12}")
    except Exception as e:
        out(f"Error in breakdown query: {e}")

    # =========================================================================
    # Part 5: Requested vs Allocated Memory (tres_req vs tres_alloc)
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 5: Requested vs Allocated Memory (tres_req vs tres_alloc)")
    out("=" * 80)
    out()
    out("Data sources (all from create_job_table):")
    out("  - tres_req (TRES ID 2): total requested memory in MB")
    out("  - tres_alloc (TRES ID 2): total allocated memory in MB")
    out("  - mem_req (bit 63): distinguishes --mem vs --mem-per-cpu")
    out("  - cpus_req vs alloc_cpus (TRES ID 1 in tres_alloc): requested vs allocated CPUs")
    out()
    out("Hypothesis: For --mem-per-cpu jobs where alloc_cpus > cpus_req,")
    out("Slurm allocates proportionally more memory than requested.")
    out("This causes memory efficiency > 100% even with ConstrainRAMSpace enabled,")
    out("because the cgroup enforces the *allocated* amount, not the *requested* amount.")

    # -------------------------------------------------------------------------
    # Part 5a: Overall req_mem vs alloc_mem comparison by memory type
    # -------------------------------------------------------------------------
    out()
    out("-" * 40)
    out("Part 5a: req_mem vs alloc_mem by memory type")
    out("-" * 40)

    try:
        cur.execute("""
            SELECT
                mem_type, comparison, COUNT(*) as cnt
            FROM (
                SELECT
                    CASE WHEN j.mem_req & 0x8000000000000000 != 0 THEN 'per-cpu' ELSE 'per-node' END as mem_type,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS req_mem,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS alloc_mem,
                    CASE
                        WHEN CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1), ',', 1) AS UNSIGNED)
                           = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1), ',', 1) AS UNSIGNED)
                        THEN 'req == alloc'
                        ELSE 'req != alloc'
                    END as comparison
                FROM create_job_table j
                WHERE j.time_start > 0 AND j.time_end > 0
            ) sub
            GROUP BY mem_type, comparison
            ORDER BY mem_type, comparison
        """)
        out()
        out(f"  {'mem_type':>10}  {'comparison':>15}  {'count':>12}  {'%':>8}")
        rows = list(cur)
        total = sum(r[2] for r in rows)
        for mem_type, comparison, cnt in rows:
            pct = (cnt / total * 100) if total else 0
            out(f"  {mem_type:>10}  {comparison:>15}  {cnt:>12}  {pct:>7.2f}%")
        out(f"  {'':>10}  {'Total':>15}  {total:>12}")
    except Exception as e:
        out(f"Error: {e}")

    # -------------------------------------------------------------------------
    # Part 5b: Focus on jobs where alloc_cpus != cpus_req
    # -------------------------------------------------------------------------
    out()
    out("-" * 40)
    out("Part 5b: Jobs where alloc_cpus != cpus_req — memory comparison")
    out("-" * 40)

    try:
        cur.execute("""
            SELECT
                mem_type, comparison, COUNT(*) as cnt
            FROM (
                SELECT
                    CASE WHEN j.mem_req & 0x8000000000000000 != 0 THEN 'per-cpu' ELSE 'per-node' END as mem_type,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS req_mem,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS alloc_mem,
                    CASE
                        WHEN CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1), ',', 1) AS UNSIGNED)
                           = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1), ',', 1) AS UNSIGNED)
                        THEN 'req == alloc'
                        ELSE 'req != alloc'
                    END as comparison
                FROM create_job_table j
                WHERE j.time_start > 0 AND j.time_end > 0
                  AND j.cpus_req != CAST(
                      SUBSTRING_INDEX(
                          SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                          ',', 1
                      ) AS UNSIGNED
                  )
            ) sub
            GROUP BY mem_type, comparison
            ORDER BY mem_type, comparison
        """)
        out()
        out(f"  {'mem_type':>10}  {'comparison':>15}  {'count':>12}  {'%':>8}")
        rows = list(cur)
        total = sum(r[2] for r in rows)
        for mem_type, comparison, cnt in rows:
            pct = (cnt / total * 100) if total else 0
            out(f"  {mem_type:>10}  {comparison:>15}  {cnt:>12}  {pct:>7.2f}%")
        out(f"  {'':>10}  {'Total':>15}  {total:>12}")
    except Exception as e:
        out(f"Error: {e}")

    # -------------------------------------------------------------------------
    # Part 5c: Sample rows with actual values
    # -------------------------------------------------------------------------
    out()
    out("-" * 40)
    out("Part 5c: Sample --mem-per-cpu jobs where alloc_cpus > cpus_req")
    out("-" * 40)

    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS alloc_cpus,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS req_mem,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS alloc_mem
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 != 0
              AND j.cpus_req != CAST(
                  SUBSTRING_INDEX(
                      SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                      ',', 1
                  ) AS UNSIGNED
              )
            ORDER BY j.id_job DESC
            LIMIT 15
        """)
        out()
        out(f"  {'id_job':>10}  {'cpus_req':>8}  {'alloc_cpus':>10}  {'req_mem':>10}  {'alloc_mem':>10}  {'alloc/req':>10}  {'cpus ratio':>10}")
        out(f"  {'':>10}  {'':>8}  {'(TRES)':>10}  {'(MB)':>10}  {'(MB)':>10}  {'(mem)':>10}  {'(cpus)':>10}")
        for row in cur:
            id_job, cpus_req, alloc_cpus, req_mem, alloc_mem = row
            mem_ratio = (alloc_mem / req_mem) if req_mem > 0 else 0
            cpu_ratio = (alloc_cpus / cpus_req) if cpus_req > 0 else 0
            out(f"  {id_job:>10}  {cpus_req:>8}  {alloc_cpus:>10}  {req_mem:>10}  {alloc_mem:>10}  {mem_ratio:>10.2f}  {cpu_ratio:>10.2f}")
    except Exception as e:
        out(f"Error: {e}")

    # -------------------------------------------------------------------------
    # Part 5d: Verify proportional scaling for --mem-per-cpu jobs
    # -------------------------------------------------------------------------
    out()
    out("-" * 40)
    out("Part 5d: Does alloc_mem / req_mem == alloc_cpus / cpus_req for --mem-per-cpu jobs?")
    out("-" * 40)
    out()
    out("For --mem-per-cpu jobs where alloc_cpus > cpus_req, if memory is scaled")
    out("proportionally, then alloc_mem / req_mem should equal alloc_cpus / cpus_req.")

    try:
        cur.execute("""
            SELECT
                proportional, COUNT(*) as cnt
            FROM (
                SELECT
                    j.id_job,
                    j.cpus_req,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS alloc_cpus,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS req_mem,
                    CAST(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1),
                            ',', 1
                        ) AS UNSIGNED
                    ) AS alloc_mem,
                    CASE
                        WHEN j.cpus_req > 0
                         AND CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1), ',', 1) AS UNSIGNED) > 0
                         AND CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1), ',', 1) AS UNSIGNED) * j.cpus_req
                           = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1), ',', 1) AS UNSIGNED)
                           * CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1), ',', 1) AS UNSIGNED)
                        THEN 'proportional'
                        ELSE 'not proportional'
                    END as proportional
                FROM create_job_table j
                WHERE j.time_start > 0 AND j.time_end > 0
                  AND j.mem_req & 0x8000000000000000 != 0
                  AND j.cpus_req != CAST(
                      SUBSTRING_INDEX(
                          SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                          ',', 1
                      ) AS UNSIGNED
                  )
            ) sub
            GROUP BY proportional
        """)
        out()
        rows = list(cur)
        total = sum(r[1] for r in rows)
        out(f"  {'Result':>20}  {'Count':>12}  {'%':>8}")
        for result, cnt in rows:
            pct = (cnt / total * 100) if total else 0
            out(f"  {result:>20}  {cnt:>12}  {pct:>7.2f}%")
        out(f"  {'Total':>20}  {total:>12}")
    except Exception as e:
        out(f"Error: {e}")

    # Sample --mem (per-node) jobs where alloc_cpus != cpus_req
    out()
    out("-" * 40)
    out("Part 5e: Sample --mem (per-node) jobs where alloc_cpus > cpus_req")
    out("-" * 40)
    out()
    out("For --mem (per-node) jobs, alloc_mem should equal req_mem regardless of CPU count.")

    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS alloc_cpus,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS req_mem,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS alloc_mem
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x8000000000000000 = 0
              AND j.mem_req > 0
              AND j.cpus_req != CAST(
                  SUBSTRING_INDEX(
                      SUBSTRING_INDEX(CONCAT(',', j.tres_alloc), ',1=', -1),
                      ',', 1
                  ) AS UNSIGNED
              )
            ORDER BY j.id_job DESC
            LIMIT 15
        """)
        out()
        out(f"  {'id_job':>10}  {'cpus_req':>8}  {'alloc_cpus':>10}  {'req_mem':>10}  {'alloc_mem':>10}  {'match?':>8}")
        out(f"  {'':>10}  {'':>8}  {'(TRES)':>10}  {'(MB)':>10}  {'(MB)':>10}")
        for row in cur:
            id_job, cpus_req, alloc_cpus, req_mem, alloc_mem = row
            match = "YES" if req_mem == alloc_mem else "NO"
            out(f"  {id_job:>10}  {cpus_req:>8}  {alloc_cpus:>10}  {req_mem:>10}  {alloc_mem:>10}  {match:>8}")
    except Exception as e:
        out(f"Error: {e}")

conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
