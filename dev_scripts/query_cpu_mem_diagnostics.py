#!/usr/bin/env python3
"""Diagnostic script to investigate CPU and memory efficiency edge cases.

Tests:
1. Does AllocCPUS (from tres_alloc) differ from cpus_req? (Could explain CPU eff > 100%)
2. Does mem_req encode --mem vs --mem-per-cpu via flag bit?
3. Does tres_req memory match mem_req, or is it per-cpu vs total?
4. Are jobs with mem eff > 100% correlated with --mem-per-cpu?

Saves output to cpu_mem_diagnostics_output.txt
"""

import sys
import mysql.connector
import yaml

OUTPUT_FILE = "cpu_mem_diagnostics_output.txt"

with open("config.yaml", "r") as f:
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
    # Part 1: cpus_req vs AllocCPUS (from tres_alloc TRES ID 1)
    # =========================================================================
    out("=" * 80)
    out("PART 1: cpus_req vs AllocCPUS (TRES ID 1 from tres_alloc)")
    out("=" * 80)
    out()
    out("Note: sacct's AllocCPUS is not a column — it's parsed from tres_alloc.")
    out("tres_alloc has format '1=N,...' where TRES ID 1 = CPU.")

    # Check that tres_alloc column exists
    cur.execute("SHOW COLUMNS FROM create_job_table LIKE 'tres_alloc'")
    col = cur.fetchone()
    out(f"tres_alloc column: {col}")

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
        out("cpus_req vs AllocCPUS (top 30 combos by frequency):")
        out(f"  {'cpus_req':>10}  {'alloc_cpus':>12}  {'count':>10}  {'comparison':>15}")
        for row in cur:
            cpus_req, alloc_cpus, cnt = row
            if cpus_req == alloc_cpus:
                cmp = "EQUAL"
            elif alloc_cpus > cpus_req:
                cmp = "alloc > req"
            else:
                cmp = "alloc < req"
            out(f"  {cpus_req:>10}  {alloc_cpus:>12}  {cnt:>10}  {cmp:>15}")
    except Exception as e:
        out(f"Error comparing: {e}")

    # Count jobs where they differ
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
                ) THEN 1 ELSE 0 END) as differ_count
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
        """)
        row = cur.fetchone()
        out()
        out(f"Summary: total={row[0]}, equal={row[1]}, differ={row[2]}")
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
        out("Sample jobs where cpus_req != AllocCPUS:")
        out(f"  {'id_job':>10}  {'cpus_req':>10}  {'alloc_cpus':>12}  {'nodes':>6}  tres_req  /  tres_alloc")
        for row in cur:
            out(f"  {row[0]:>10}  {row[1]:>10}  {row[2]:>12}  {row[3]:>6}  {row[4]}  /  {row[5]}")
    except Exception as e:
        out(f"Error fetching differing jobs: {e}")

    # =========================================================================
    # Part 2: mem_req flag bit (--mem vs --mem-per-cpu)
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 2: mem_req flag bit (--mem vs --mem-per-cpu)")
    out("=" * 80)

    # Check mem_req column exists
    cur.execute("SHOW COLUMNS FROM create_job_table LIKE 'mem_req'")
    col = cur.fetchone()
    out(f"mem_req column: {col}")

    # All memory-related columns
    out()
    out("All memory-related columns in create_job_table:")
    cur.execute("SHOW COLUMNS FROM create_job_table LIKE '%mem%'")
    for row in cur:
        out(f"  {row}")

    # Check flag bit distribution
    out()
    out("mem_req flag bit distribution (bit 31 = --mem-per-cpu):")
    try:
        cur.execute("""
            SELECT
                CASE WHEN mem_req & 0x80000000 != 0 THEN 'per-cpu (flag set)'
                     ELSE 'per-node (no flag)' END as mem_type,
                COUNT(*) as cnt
            FROM create_job_table
            WHERE time_start > 0 AND time_end > 0
            GROUP BY mem_type
        """)
        for row in cur:
            out(f"  {row[0]:>25}: {row[1]:>10} jobs")
    except Exception as e:
        out(f"Error checking flag bit: {e}")

    # =========================================================================
    # Part 3: Compare mem_req value with tres_req memory value
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 3: mem_req vs tres_req memory for --mem-per-cpu jobs")
    out("=" * 80)

    # Sample --mem-per-cpu jobs (flag bit set) with multi-CPU
    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req,
                j.mem_req & 0x80000000 AS is_per_cpu,
                j.mem_req & 0x7FFFFFFF AS mem_value_raw,
                j.tres_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS tres_req_mem
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x80000000 != 0
              AND j.cpus_req > 1
            ORDER BY j.id_job DESC
            LIMIT 15
        """)
        out()
        out("--mem-per-cpu jobs with cpus_req > 1:")
        out(f"  {'id_job':>10}  {'cpus':>5}  {'mem_req_raw':>12}  {'mem_value':>12}  {'tres_mem':>10}  {'tres_mem/cpus':>14}  tres_req")
        for row in cur:
            id_job, cpus, mem_req_raw, is_per_cpu, mem_value, tres_req, tres_mem = row
            tres_per_cpu = tres_mem // cpus if cpus > 0 and tres_mem else 0
            out(f"  {id_job:>10}  {cpus:>5}  {mem_req_raw:>12}  {mem_value:>12}  {tres_mem:>10}  {tres_per_cpu:>14}  {tres_req}")
    except Exception as e:
        out(f"Error: {e}")

    # Same for --mem (per-node) jobs for comparison
    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req,
                j.mem_req & 0x7FFFFFFF AS mem_value_raw,
                j.tres_req,
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', j.tres_req), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                ) AS tres_req_mem
            FROM create_job_table j
            WHERE j.time_start > 0 AND j.time_end > 0
              AND j.mem_req & 0x80000000 = 0
              AND j.cpus_req > 1
              AND j.mem_req > 0
            ORDER BY j.id_job DESC
            LIMIT 15
        """)
        out()
        out("--mem (per-node) jobs with cpus_req > 1, for comparison:")
        out(f"  {'id_job':>10}  {'cpus':>5}  {'mem_req':>12}  {'tres_mem':>10}  {'match?':>8}  tres_req")
        for row in cur:
            id_job, cpus, mem_req, mem_value, tres_req, tres_mem = row
            match = "YES" if mem_value == tres_mem else "NO"
            out(f"  {id_job:>10}  {cpus:>5}  {mem_req:>12}  {tres_mem:>10}  {match:>8}  {tres_req}")
    except Exception as e:
        out(f"Error: {e}")

    # =========================================================================
    # Part 4: Jobs with memory efficiency > 100% — what mem type?
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 4: Jobs with apparent memory efficiency > 100% — which mem type?")
    out("=" * 80)

    try:
        cur.execute("""
            SELECT
                j.id_job,
                j.cpus_req,
                j.mem_req,
                CASE WHEN j.mem_req & 0x80000000 != 0 THEN 'per-cpu' ELSE 'per-node' END as mem_type,
                j.mem_req & 0x7FFFFFFF AS mem_value,
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
        out("Completed jobs where maxrss > tres_req_mem * 1M (apparent mem eff > 100%):")
        out(f"  {'id_job':>10}  {'cpus':>5}  {'mem_type':>8}  {'mem_val':>10}  {'tres_mem':>10}  {'maxrss':>15}  {'eff%':>8}")
        for row in cur:
            id_job, cpus, mem_req, mem_type, mem_value, tres_mem, maxrss = row
            reqmem_bytes = tres_mem * 1024 * 1024
            eff = (maxrss / reqmem_bytes * 100) if reqmem_bytes > 0 else 0
            out(f"  {id_job:>10}  {cpus:>5}  {mem_type:>8}  {mem_value:>10}  {tres_mem:>10}  {maxrss:>15}  {eff:>7.1f}%")
    except Exception as e:
        out(f"Error: {e}")

    # Count by mem_type for jobs with eff > 100% (using subquery)
    try:
        cur.execute("""
            SELECT mem_type, COUNT(*) as cnt FROM (
                SELECT
                    j.job_db_inx,
                    CASE WHEN j.mem_req & 0x80000000 != 0 THEN 'per-cpu' ELSE 'per-node' END as mem_type,
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
        for row in cur:
            out(f"  {row[0]:>10}: {row[1]:>8} jobs")
    except Exception as e:
        out(f"Error in breakdown query: {e}")

conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
