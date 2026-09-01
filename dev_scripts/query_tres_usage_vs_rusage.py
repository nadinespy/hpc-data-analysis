#!/usr/bin/env python3
"""Investigate the relationship between rusage and TRES CPU accounting.

Slurm has two sources of CPU time data:
1. user_sec/sys_sec columns: From getrusage/wait4 system calls (local process only)
2. tres_usage_in_max (TRES ID 1): From cgroup accounting (can include remote nodes)

This matters for multi-node jobs where the batch script runs on the head node
but srun steps execute on remote nodes. The rusage data only captures local
CPU time, while TRES data may capture distributed usage.

Tests performed:
1. Compare user_sec+sys_sec vs tres_usage_in_max CPU for batch steps
   (to determine TRES CPU units - should be ~1:1 ratio if both in seconds)
2. Find steps where rusage=0 but TRES has data (distributed/remote steps)
3. Count steps by data source availability (rusage only, TRES only, both, neither)
4. Compare single-node vs multi-node jobs (do they differ in data availability?)
5. For jobs with both batch and regular steps, compare CPU totals
   (is batch CPU additional overhead or duplicate counting?)

Key findings:
- TRES CPU values are in CPU-seconds (same unit as rusage)
- Regular srun steps often have rusage=0 but TRES data (distributed execution)
- Batch steps usually have both rusage and TRES data
- For accurate CPU efficiency, sum regular steps if available, else use batch

Data sources:
- create_step_table: user_sec, sys_sec, user_usec, sys_usec, tres_usage_in_max
- create_job_table: nodes_alloc, cpus_req

Saves output to output_tres_usage_vs_rusage.txt
"""

import sys
from pathlib import Path
import yaml
import mysql.connector

# Paths relative to script location (allows running from any directory)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "output_tres_usage_vs_rusage.txt"
TRES_CPU_ID = 1


def connect_mysql(config_path=None):
    """Connect to MySQL using config file."""
    if config_path is None:
        config_path = CONFIG_FILE
    with open(config_path) as f:
        config = yaml.safe_load(f)
    mysql_cfg = config.get("mysql", config)
    conn = mysql.connector.connect(
        host=mysql_cfg["host"],
        port=mysql_cfg.get("port", 3306),
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"],
    )
    return conn, conn.cursor()


def parse_tres_value(tres_str, tres_id):
    """Parse a TRES string like '1=8,2=8000' and extract value for given ID."""
    if not tres_str:
        return 0
    for part in tres_str.split(","):
        if "=" in part:
            tid, val = part.split("=", 1)
            if int(tid) == tres_id:
                return int(val)
    return 0

def parse_tres_cpu(tres_str):
    """Extract CPU value from tres_usage_in_max string."""
    if not tres_str:
        return None
    return parse_tres_value(tres_str, TRES_CPU_ID)

def main():
    conn, cursor = connect_mysql()

    with open(OUTPUT_FILE, 'w') as f:
        def out(text=""):
            print(text)
            print(text, file=f)

        out("=" * 80)
        out("PART 1: Compare user_sec/sys_sec vs tres_usage_in_max for BATCH steps")
        out("        (where user_sec > 0, to see the relationship)")
        out("=" * 80)

        query = """
            SELECT
                s.job_db_inx,
                j.id_job,
                j.nodes_alloc,
                s.id_step,
                s.step_name,
                s.user_sec,
                s.sys_sec,
                s.user_usec,
                s.sys_usec,
                s.tres_usage_in_max
            FROM create_step_table s
            JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
            WHERE s.id_step = -5  -- batch step
              AND s.user_sec > 100  -- substantial CPU time
              AND s.tres_usage_in_max IS NOT NULL
              AND s.tres_usage_in_max != ''
            ORDER BY RAND()
            LIMIT 20
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        out(f"\n{'job_id':>12} {'nodes':>6} {'user_sec':>12} {'sys_sec':>10} "
              f"{'total_sec':>12} {'tres_cpu':>14} {'ratio':>10}")
        out("-" * 90)

        ratios = []
        for row in rows:
            job_db_inx, id_job, nodes, id_step, step_name, user_sec, sys_sec, user_usec, sys_usec, tres_usage = row
            total_sec = (user_sec or 0) + (sys_sec or 0) + ((user_usec or 0) + (sys_usec or 0)) / 1_000_000
            tres_cpu = parse_tres_cpu(tres_usage)

            if tres_cpu and total_sec > 0:
                ratio = tres_cpu / total_sec
                ratios.append(ratio)
                out(f"{id_job:>12} {nodes:>6} {user_sec:>12} {sys_sec:>10} "
                      f"{total_sec:>12.2f} {tres_cpu:>14} {ratio:>10.2f}")

        if ratios:
            avg_ratio = sum(ratios) / len(ratios)
            out(f"\nAverage ratio (tres_cpu / total_sec): {avg_ratio:.2f}")
            out(f"If ratio ~1: tres_usage_in_max is in CPU-seconds")
            out(f"If ratio ~1000: tres_usage_in_max is in CPU-milliseconds")

        out("\n" + "=" * 80)
        out("PART 2: Steps where user_sec=0 but tres_usage_in_max has CPU data")
        out("        (these are likely distributed/remote steps)")
        out("=" * 80)

        query = """
            SELECT
                s.job_db_inx,
                j.id_job,
                j.nodes_alloc,
                s.id_step,
                s.step_name,
                s.user_sec,
                s.sys_sec,
                s.tres_usage_in_max
            FROM create_step_table s
            JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
            WHERE s.id_step >= 0  -- regular steps only
              AND (s.user_sec = 0 OR s.user_sec IS NULL)
              AND (s.sys_sec = 0 OR s.sys_sec IS NULL)
              AND s.tres_usage_in_max IS NOT NULL
              AND s.tres_usage_in_max != ''
              AND s.tres_usage_in_max LIKE '1=%'  -- has CPU data
            ORDER BY RAND()
            LIMIT 20
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        out(f"\n{'job_id':>12} {'nodes':>6} {'id_step':>8} {'step_name':>25} "
              f"{'user_sec':>10} {'tres_cpu':>14}")
        out("-" * 90)

        for row in rows:
            job_db_inx, id_job, nodes, id_step, step_name, user_sec, sys_sec, tres_usage = row
            tres_cpu = parse_tres_cpu(tres_usage)
            out(f"{id_job:>12} {nodes:>6} {id_step:>8} {step_name:>25} "
                  f"{user_sec or 0:>10} {tres_cpu or 0:>14}")

        out("\n" + "=" * 80)
        out("PART 3: Count of steps by accounting method availability")
        out("=" * 80)

        query = """
            SELECT
                CASE
                    WHEN id_step < 0 THEN 'special'
                    ELSE 'regular'
                END as step_type,
                CASE
                    WHEN (user_sec > 0 OR sys_sec > 0) AND tres_usage_in_max LIKE '1=%' THEN 'both'
                    WHEN (user_sec > 0 OR sys_sec > 0) THEN 'rusage_only'
                    WHEN tres_usage_in_max LIKE '1=%' THEN 'tres_only'
                    ELSE 'neither'
                END as data_source,
                COUNT(*) as count
            FROM create_step_table
            GROUP BY step_type, data_source
            ORDER BY step_type, data_source
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        out(f"\n{'step_type':>10} {'data_source':>15} {'count':>15}")
        out("-" * 45)
        for row in rows:
            step_type, data_source, count = row
            out(f"{step_type:>10} {data_source:>15} {count:>15,}")

        out("\n" + "=" * 80)
        out("PART 4: Single-node vs multi-node job comparison")
        out("        (do single-node jobs have rusage data in regular steps?)")
        out("=" * 80)

        # Single-node jobs with regular steps
        query = """
            SELECT
                j.nodes_alloc,
                s.id_step,
                s.step_name,
                s.user_sec,
                s.sys_sec,
                s.tres_usage_in_max
            FROM create_step_table s
            JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
            WHERE j.nodes_alloc = 1
              AND s.id_step >= 0
              AND s.tres_usage_in_max LIKE '1=%'
            ORDER BY RAND()
            LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        out(f"\nSingle-node jobs (nodes=1) with regular steps:")
        out(f"{'nodes':>6} {'id_step':>8} {'step_name':>20} {'user_sec':>10} {'sys_sec':>10} {'tres_cpu':>14}")
        out("-" * 75)
        for row in rows:
            nodes, id_step, step_name, user_sec, sys_sec, tres_usage = row
            tres_cpu = parse_tres_cpu(tres_usage)
            out(f"{nodes:>6} {id_step:>8} {step_name:>20} {user_sec or 0:>10} {sys_sec or 0:>10} {tres_cpu or 0:>14}")

        # Multi-node jobs with regular steps
        query = """
            SELECT
                j.nodes_alloc,
                s.id_step,
                s.step_name,
                s.user_sec,
                s.sys_sec,
                s.tres_usage_in_max
            FROM create_step_table s
            JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
            WHERE j.nodes_alloc > 1
              AND s.id_step >= 0
              AND s.tres_usage_in_max LIKE '1=%'
            ORDER BY RAND()
            LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        out(f"\nMulti-node jobs (nodes>1) with regular steps:")
        out(f"{'nodes':>6} {'id_step':>8} {'step_name':>20} {'user_sec':>10} {'sys_sec':>10} {'tres_cpu':>14}")
        out("-" * 75)
        for row in rows:
            nodes, id_step, step_name, user_sec, sys_sec, tres_usage = row
            tres_cpu = parse_tres_cpu(tres_usage)
            out(f"{nodes:>6} {id_step:>8} {step_name:>20} {user_sec or 0:>10} {sys_sec or 0:>10} {tres_cpu or 0:>14}")

        out("\n" + "=" * 80)
        out("PART 5: For jobs with BOTH batch and regular steps, compare totals")
        out("        (is batch CPU a duplicate of regular steps, or additional?)")
        out("=" * 80)

        query = """
            SELECT
                j.id_job,
                j.nodes_alloc,
                j.time_end - j.time_start as elapsed,
                j.cpus_req,
                -- Batch step data
                MAX(CASE WHEN s.id_step = -5 THEN s.user_sec + s.sys_sec END) as batch_rusage,
                MAX(CASE WHEN s.id_step = -5 THEN s.tres_usage_in_max END) as batch_tres,
                -- Regular steps data (summed)
                SUM(CASE WHEN s.id_step >= 0 THEN s.user_sec + s.sys_sec ELSE 0 END) as regular_rusage,
                GROUP_CONCAT(CASE WHEN s.id_step >= 0 THEN s.tres_usage_in_max END) as regular_tres_list,
                COUNT(CASE WHEN s.id_step >= 0 THEN 1 END) as regular_step_count
            FROM create_job_table j
            JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
            WHERE j.time_end > j.time_start
              AND j.cpus_req > 1
            GROUP BY j.job_db_inx
            HAVING regular_step_count > 0 AND batch_rusage > 100
            ORDER BY RAND()
            LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        out(f"\n{'job_id':>12} {'nodes':>6} {'elapsed':>8} {'cpus':>6} "
              f"{'batch_ru':>12} {'regular_ru':>12} {'batch_tres_cpu':>15}")
        out("-" * 90)

        for row in rows:
            id_job, nodes, elapsed, cpus, batch_rusage, batch_tres, regular_rusage, regular_tres_list, step_count = row
            batch_tres_cpu = parse_tres_cpu(batch_tres) if batch_tres else 0

            # Sum tres_cpu from regular steps
            regular_tres_cpu = 0
            if regular_tres_list:
                for tres_str in regular_tres_list.split(',1='):
                    if tres_str:
                        cpu_val = parse_tres_cpu('1=' + tres_str if not tres_str.startswith('1=') else tres_str)
                        if cpu_val:
                            regular_tres_cpu += cpu_val

            out(f"{id_job:>12} {nodes:>6} {elapsed:>8} {cpus:>6} "
                  f"{batch_rusage or 0:>12.0f} {regular_rusage or 0:>12.0f} {batch_tres_cpu:>15}")

        cursor.close()
        conn.close()

        out("\n" + "=" * 80)
        out("SUMMARY")
        out("=" * 80)
        out("""
Key questions this script answers:
1. What is the unit of tres_usage_in_max CPU values? (ratio to user_sec+sys_sec)
2. Are regular steps missing rusage data but have tres data?
3. Is this pattern different for single-node vs multi-node jobs?
4. Is batch step CPU additional to regular steps, or a duplicate?
""")

    print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)

if __name__ == "__main__":
    main()
