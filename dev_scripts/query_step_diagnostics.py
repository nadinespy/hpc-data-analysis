#!/usr/bin/env python3
"""Diagnostic script to inspect Slurm step IDs and test CPU accounting.

Saves output to step_diagnostics_output.txt
"""

import sys
import mysql.connector
import yaml

OUTPUT_FILE = "output_step_diagnostics.txt"

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

    # Summary of step names by count (top 20 + all with count > 1)
    out("=== Step names by frequency (count > 1) ===")
    cur.execute("""
        SELECT step_name, MIN(id_step), MAX(id_step), COUNT(*) as cnt
        FROM create_step_table
        GROUP BY step_name
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    out(f"  {'step_name':>30}  {'min_id':>8}  {'max_id':>8}  {'count':>12}")
    for row in cur:
        out(f"  {str(row[0]):>30}  {row[1]:>8}  {row[2]:>8}  {row[3]:>12}")

    out()
    out("=== Total unique step names ===")
    cur.execute("SELECT COUNT(DISTINCT step_name) FROM create_step_table")
    out(f"  {cur.fetchone()[0]} unique step names")

    out()
    out("=== Special vs regular steps summary ===")
    cur.execute("""
        SELECT
            SUM(CASE WHEN id_step < 0 THEN 1 ELSE 0 END) as special_steps,
            SUM(CASE WHEN id_step >= 0 THEN 1 ELSE 0 END) as regular_steps,
            COUNT(*) as total_steps
        FROM create_step_table
    """)
    row = cur.fetchone()
    out(f"  Special steps (id_step < 0):  {row[0]}")
    out(f"  Regular steps (id_step >= 0): {row[1]}")
    out(f"  Total steps:                  {row[2]}")

    out()
    out("=== Special steps detail (id_step < 0) ===")
    cur.execute("""
        SELECT step_name, id_step, COUNT(*) as cnt
        FROM create_step_table
        WHERE id_step < 0
        GROUP BY step_name, id_step
        ORDER BY cnt DESC
    """)
    out(f"  {'step_name':>15}  {'id_step':>8}  {'count':>12}")
    for row in cur:
        out(f"  {str(row[0]):>15}  {row[1]:>8}  {row[2]:>12}")

    # Batch vs srun CPU comparison
    out()
    out("=== Batch vs other steps: CPU comparison (10 sample jobs with both) ===")
    out("  Jobs that have a step named 'batch' AND at least one other step:")
    cur.execute("""
        SELECT
            s.job_db_inx,
            MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
            SUM(CASE WHEN s.step_name NOT IN ('batch', 'interactive') THEN s.user_sec + s.sys_sec ELSE 0 END) AS other_cpu,
            SUM(s.user_sec + s.sys_sec) AS total_all_steps,
            COUNT(*) AS num_steps
        FROM create_step_table s
        GROUP BY s.job_db_inx
        HAVING batch_cpu IS NOT NULL AND other_cpu > 0
        ORDER BY s.job_db_inx DESC
        LIMIT 10
    """)
    out(f"  {'job_db_inx':>25}  {'batch_cpu':>10}  {'other_cpu':>10}  {'all_steps':>10}  {'steps':>6}  {'batch=all?':>10}")
    for row in cur:
        match = "YES" if row[1] == row[3] else "NO"
        out(f"  {row[0]:>25}  {row[1]:>10}  {row[2]:>10}  {row[3]:>10}  {row[4]:>6}  {match:>10}")

    # Multi-step jobs where batch_cpu is substantial
    out()
    out("=== Multi-step jobs where batch_cpu is SUBSTANTIAL ===")
    out("  Looking for jobs with batch + regular steps where batch_cpu > 100 sec:")
    cur.execute("""
        SELECT
            s.job_db_inx,
            MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
            SUM(CASE WHEN s.step_name NOT IN ('batch', 'interactive', 'extern') THEN s.user_sec + s.sys_sec ELSE 0 END) AS other_cpu,
            SUM(s.user_sec + s.sys_sec) AS total_all_steps,
            COUNT(*) AS num_steps
        FROM create_step_table s
        GROUP BY s.job_db_inx
        HAVING batch_cpu IS NOT NULL AND batch_cpu > 100 AND other_cpu > 0
        ORDER BY batch_cpu DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    if rows:
        out(f"  {'job_db_inx':>25}  {'batch_cpu':>10}  {'other_cpu':>10}  {'all_steps':>10}  {'steps':>6}  {'batch/other':>12}")
        for row in rows:
            ratio = row[1] / row[2] if row[2] > 0 else 0
            out(f"  {row[0]:>25}  {row[1]:>10}  {row[2]:>10}  {row[3]:>10}  {row[4]:>6}  {ratio:>11.2f}x")
        out()
        out("  If batch/other ≈ 1.0, batch might be double-counting the same work as other steps.")
        out("  If batch/other << 1.0, batch is likely separate overhead work.")
    else:
        out("  No multi-step jobs found where batch_cpu > 100 sec")

    # Count how many multi-step jobs have substantial batch_cpu
    out()
    out("=== Distribution of batch_cpu in multi-step jobs ===")
    cur.execute("""
        SELECT
            CASE
                WHEN batch_cpu = 0 THEN '0 (zero)'
                WHEN batch_cpu BETWEEN 1 AND 10 THEN '1-10 sec'
                WHEN batch_cpu BETWEEN 11 AND 100 THEN '11-100 sec'
                WHEN batch_cpu BETWEEN 101 AND 1000 THEN '101-1000 sec'
                WHEN batch_cpu > 1000 THEN '>1000 sec'
            END as batch_cpu_range,
            COUNT(*) as job_count
        FROM (
            SELECT
                s.job_db_inx,
                MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
                SUM(CASE WHEN s.step_name NOT IN ('batch', 'interactive', 'extern') THEN s.user_sec + s.sys_sec ELSE 0 END) AS other_cpu
            FROM create_step_table s
            GROUP BY s.job_db_inx
            HAVING batch_cpu IS NOT NULL AND other_cpu > 0
        ) sub
        GROUP BY batch_cpu_range
        ORDER BY MIN(batch_cpu)
    """)
    out(f"  {'batch_cpu_range':>15}  {'job_count':>12}")
    for row in cur:
        out(f"  {str(row[0]):>15}  {row[1]:>12}")

    # Batch-only jobs (no other steps)
    out()
    out("=== Batch-only jobs: CPU check (10 samples) ===")
    out("  Jobs that have ONLY a 'batch' step (no srun steps):")
    cur.execute("""
        SELECT
            s.job_db_inx,
            MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
            COUNT(*) AS num_steps
        FROM create_step_table s
        GROUP BY s.job_db_inx
        HAVING num_steps = 1 AND batch_cpu IS NOT NULL
        ORDER BY batch_cpu DESC
        LIMIT 10
    """)
    out(f"  {'job_db_inx':>25}  {'batch_cpu':>10}  {'steps':>6}")
    for row in cur:
        out(f"  {row[0]:>25}  {row[1]:>10}  {row[2]:>6}")

conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
