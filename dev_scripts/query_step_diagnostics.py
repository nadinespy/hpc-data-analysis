#!/usr/bin/env python3
"""Diagnostic script to inspect Slurm step IDs and test CPU accounting.

Saves output to step_diagnostics_output.txt
"""

import sys
import mysql.connector

OUTPUT_FILE = "step_diagnostics_output.txt"

conn = mysql.connector.connect(
    host='ermysqlr2.er.kcl.ac.uk',
    user='slurm_create_ro',
    password='qu4c46wQookqTWdq',
    database='slurm_create'
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
