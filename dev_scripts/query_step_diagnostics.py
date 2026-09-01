#!/usr/bin/env python3
"""Diagnostic script to investigate Slurm job steps and CPU accounting.

Slurm stores job data at two levels:
- create_job_table: One row per job with requested resources, timestamps, etc.
- create_step_table: One row per step within a job (linked via job_db_inx)

A "step" is a unit of execution within a job. Step types:
- batch (id_step=-5): The batch script itself
- interactive (id_step=-6): Interactive shell sessions
- extern (id_step=-4): External/prolog/epilog work
- Regular steps (id_step >= 0): srun commands within the job

Key questions addressed:
1. How are jobs and steps related? (sample data with column explanations)
2. How many steps do jobs typically have? (distribution)
3. What step types exist? (special vs regular, with frequencies)
4. How is CPU time distributed across steps? (batch vs srun)
5. Multi-node jobs: CPU time in tres_usage_in_max vs user_sec/sys_sec
6. Batch-only and interactive-only jobs

Output sections:
1. Sample jobs with step data (showing schema and relationships)
2. Step count distribution per job
3. Step type breakdown (special vs regular, frequencies)
4. Batch vs srun CPU comparison (to check for double-counting)
5. Distribution of batch CPU time in multi-step jobs
6. Batch-only and interactive-only jobs
7. Multi-node jobs — CPU accounting via tres_usage_in_max

Data sources:
- create_job_table: job_db_inx (links to steps), id_job, cpus_req, tres_req, timestamps
- create_step_table: job_db_inx (FK), id_step, step_name, user_sec, sys_sec, tres_usage_in_max

Saves output to output_step_diagnostics.txt
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
OUTPUT_FILE = OUTPUT_DIR / "output_step_diagnostics.txt"
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
    # Background: Job submission types and step relationships
    # =========================================================================
    out("=" * 80)
    out("BACKGROUND: Job submission types and step relationships")
    out("=" * 80)
    out()
    out("Batch vs Interactive Jobs:")
    out("  - BATCH job: Submitted with 'sbatch script.sh'. The script runs as the")
    out("    'batch' step (id_step=-5). Most common job type.")
    out("  - INTERACTIVE job: Submitted with 'salloc' or 'srun --pty bash'. Creates")
    out("    an 'interactive' step (id_step=-6). User gets a shell on compute node.")
    out("  - A job is either batch OR interactive based on submission method —")
    out("    they cannot occur together in the same job.")
    out()
    out("srun steps (regular steps, id_step >= 0):")
    out("  - Created when user runs 'srun <command>' inside a job")
    out("  - Can occur inside EITHER batch or interactive jobs")
    out("  - Each srun command creates a new step numbered 0, 1, 2, ...")
    out("  - step_name records the command/executable that was run")
    out()
    out("Typical job structures:")
    out("  - Batch-only: Just batch step (-5), no srun commands in script")
    out("  - Batch + srun: Batch step (-5) + regular steps (0, 1, 2...)")
    out("  - Interactive-only: Just interactive step (-6), user got shell but")
    out("    didn't run any srun commands")
    out("  - Interactive + srun: Interactive step (-6) + regular steps (0, 1, 2...)")

    # =========================================================================
    # Part 1: Sample jobs with step data (schema illustration)
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 1: Sample jobs with step data — illustrating schema and relationships")
    out("=" * 80)
    out()
    out("Data sources:")
    out("  create_job_table columns:")
    out("    - job_db_inx: internal DB key (links to steps)")
    out("    - id_job: Slurm job ID (what users see)")
    out("    - cpus_req: requested CPUs")
    out("    - nodes_alloc: number of nodes allocated")
    out("    - tres_req/tres_alloc: TRES-encoded resource strings")
    out("    - time_submit/time_start/time_end: Unix timestamps")
    out("    - timelimit: requested time limit (minutes)")
    out()
    out("  create_step_table columns:")
    out("    - job_db_inx: FK to job table")
    out("    - id_step: step ID (<0 = special, >=0 = srun steps)")
    out("    - step_name: human-readable name (command/executable for srun steps)")
    out("    - user_sec/user_usec: user-mode CPU time (seconds + microseconds)")
    out("    - sys_sec/sys_usec: system-mode CPU time (seconds + microseconds)")
    out("    - tres_usage_in_max: peak resource usage (TRES-encoded)")

    # Sample 1: Single-step batch job
    out()
    out("-" * 40)
    out("Sample 1: Single-step batch job (batch step only, no srun)")
    out("-" * 40)
    cur.execute("""
        SELECT j.job_db_inx, j.id_job, j.cpus_req, j.nodes_alloc, j.tres_req,
               j.time_start, j.time_end, j.timelimit
        FROM create_job_table j
        JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.state = 3 AND j.time_start > 0 AND j.time_end > 0
        GROUP BY j.job_db_inx
        HAVING COUNT(*) = 1
           AND SUM(CASE WHEN s.step_name = 'batch' THEN 1 ELSE 0 END) = 1
        ORDER BY j.id_job DESC
        LIMIT 1
    """)
    job = cur.fetchone()
    if job:
        job_db_inx, id_job, cpus_req, nodes_alloc, tres_req, time_start, time_end, timelimit = job
        out()
        out(f"Job id_job={id_job}:")
        out(f"  cpus_req:     {cpus_req}")
        out(f"  nodes_alloc:  {nodes_alloc}")
        out(f"  tres_req:     {tres_req}")
        out(f"  timelimit:    {timelimit} min")
        out(f"  elapsed:      {time_end - time_start} sec")

        cur.execute("""
            SELECT id_step, step_name, user_sec, user_usec, sys_sec, sys_usec,
                   tres_usage_in_max
            FROM create_step_table
            WHERE job_db_inx = %s
            ORDER BY id_step
        """, (job_db_inx,))
        out()
        out(f"  Steps:")
        out(f"    {'id_step':>8}  {'step_name':<15}  {'user_sec':>10}  {'sys_sec':>8}  tres_usage_in_max")
        out(f"    {'-'*8}  {'-'*15}  {'-'*10}  {'-'*8}  {'-'*40}")
        for step in cur:
            name = str(step[1])[:15] if step[1] else ''
            tres = str(step[6])[:40] if step[6] else ''
            out(f"    {step[0]:>8}  {name:<15}  {step[2]:>10}  {step[4]:>8}  {tres}")

    # Sample 2: Multi-step batch job (batch + srun steps)
    out()
    out("-" * 40)
    out("Sample 2: Multi-step batch job (batch + srun steps)")
    out("-" * 40)
    cur.execute("""
        SELECT j.job_db_inx, j.id_job, j.cpus_req, j.nodes_alloc, j.tres_req,
               j.time_start, j.time_end, j.timelimit
        FROM create_job_table j
        JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.state = 3 AND j.time_start > 0 AND j.time_end > 0
        GROUP BY j.job_db_inx
        HAVING COUNT(*) BETWEEN 3 AND 6
           AND SUM(CASE WHEN s.step_name = 'batch' THEN 1 ELSE 0 END) = 1
           AND SUM(CASE WHEN s.id_step >= 0 THEN 1 ELSE 0 END) >= 2
        ORDER BY j.id_job DESC
        LIMIT 1
    """)
    job = cur.fetchone()
    if job:
        job_db_inx, id_job, cpus_req, nodes_alloc, tres_req, time_start, time_end, timelimit = job
        out()
        out(f"Job id_job={id_job}:")
        out(f"  cpus_req:     {cpus_req}")
        out(f"  nodes_alloc:  {nodes_alloc}")
        out(f"  tres_req:     {tres_req}")
        out(f"  timelimit:    {timelimit} min")
        out(f"  elapsed:      {time_end - time_start} sec")

        cur.execute("""
            SELECT id_step, step_name, user_sec, user_usec, sys_sec, sys_usec,
                   tres_usage_in_max
            FROM create_step_table
            WHERE job_db_inx = %s
            ORDER BY id_step
        """, (job_db_inx,))
        out()
        out(f"  Steps:")
        out(f"    {'id_step':>8}  {'step_name':<25}  {'user_sec':>10}  {'sys_sec':>8}  tres_usage_in_max")
        out(f"    {'-'*8}  {'-'*25}  {'-'*10}  {'-'*8}  {'-'*40}")
        for step in cur:
            name = str(step[1])[:25] if step[1] else ''
            tres = str(step[6])[:40] if step[6] else ''
            out(f"    {step[0]:>8}  {name:<25}  {step[2]:>10}  {step[4]:>8}  {tres}")

    # Sample 3: Job with many srun steps
    out()
    out("-" * 40)
    out("Sample 3: Job with many srun steps (10+ steps)")
    out("-" * 40)
    cur.execute("""
        SELECT j.job_db_inx, j.id_job, j.cpus_req, j.nodes_alloc, j.tres_req,
               j.time_start, j.time_end, j.timelimit, COUNT(*) as step_count
        FROM create_job_table j
        JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.state = 3 AND j.time_start > 0 AND j.time_end > 0
        GROUP BY j.job_db_inx
        HAVING COUNT(*) >= 10
        ORDER BY j.id_job DESC
        LIMIT 1
    """)
    job = cur.fetchone()
    if job:
        job_db_inx, id_job, cpus_req, nodes_alloc, tres_req, time_start, time_end, timelimit, step_count = job
        out()
        out(f"Job id_job={id_job} ({step_count} steps):")
        out(f"  cpus_req:     {cpus_req}")
        out(f"  nodes_alloc:  {nodes_alloc}")
        out(f"  tres_req:     {tres_req}")
        out(f"  timelimit:    {timelimit} min")
        out(f"  elapsed:      {time_end - time_start} sec")

        cur.execute("""
            SELECT id_step, step_name, user_sec, user_usec, sys_sec, sys_usec,
                   tres_usage_in_max
            FROM create_step_table
            WHERE job_db_inx = %s
            ORDER BY id_step
            LIMIT 15
        """, (job_db_inx,))
        out()
        out(f"  Steps (first 15 of {step_count}):")
        out(f"    {'id_step':>8}  {'step_name':<25}  {'user_sec':>10}  {'sys_sec':>8}  tres_usage_in_max")
        out(f"    {'-'*8}  {'-'*25}  {'-'*10}  {'-'*8}  {'-'*40}")
        for step in cur:
            name = str(step[1])[:25] if step[1] else ''
            tres = str(step[6])[:40] if step[6] else ''
            out(f"    {step[0]:>8}  {name:<25}  {step[2]:>10}  {step[4]:>8}  {tres}")

    # =========================================================================
    # Part 2: Step count distribution per job
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 2: Step count distribution — how many steps do jobs typically have?")
    out("=" * 80)
    out()
    out("Data source: create_step_table, grouped by job_db_inx")

    cur.execute("""
        SELECT step_count, COUNT(*) as job_count
        FROM (
            SELECT job_db_inx, COUNT(*) as step_count
            FROM create_step_table
            GROUP BY job_db_inx
        ) counts
        GROUP BY step_count
        ORDER BY step_count
        LIMIT 20
    """)
    rows = list(cur)
    total_jobs = sum(r[1] for r in rows)

    out()
    out(f"  {'steps':>6}  {'jobs':>12}  {'%':>8}  {'cumulative %':>14}")
    out(f"  {'-'*6}  {'-'*12}  {'-'*8}  {'-'*14}")
    cumulative = 0
    for step_count, job_count in rows:
        pct = (job_count / total_jobs * 100) if total_jobs else 0
        cumulative += pct
        out(f"  {step_count:>6}  {job_count:>12}  {pct:>7.2f}%  {cumulative:>13.2f}%")

    out()
    out(f"  Total jobs shown: {total_jobs:,}")

    # Step ID range
    cur.execute("""
        SELECT MIN(id_step), MAX(id_step), COUNT(*), COUNT(DISTINCT id_step)
        FROM create_step_table
    """)
    min_step, max_step, total_steps, distinct_steps = cur.fetchone()
    out()
    out(f"  Step ID range: {min_step} to {max_step}")
    out(f"  Total step rows: {total_steps:,}")
    out(f"  Distinct step IDs: {distinct_steps:,}")

    # =========================================================================
    # Part 3: Step type breakdown (special vs regular)
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 3: Step type breakdown — special vs regular steps")
    out("=" * 80)
    out()
    out("Data source: create_step_table (id_step, step_name)")
    out()
    out("Step ID conventions:")
    out("  - id_step < 0:  Special steps (batch=-5, interactive=-6, extern=-4)")
    out("  - id_step >= 0: Regular steps (srun commands, numbered 0, 1, 2, ...)")

    # Overall breakdown
    cur.execute("""
        SELECT
            SUM(CASE WHEN id_step < 0 THEN 1 ELSE 0 END) as special_steps,
            SUM(CASE WHEN id_step >= 0 THEN 1 ELSE 0 END) as regular_steps,
            COUNT(*) as total_steps
        FROM create_step_table
    """)
    row = cur.fetchone()
    special, regular, total = row
    special_pct = (special / total * 100) if total else 0
    regular_pct = (regular / total * 100) if total else 0

    out()
    out(f"Overall breakdown:")
    out(f"  Special steps (id_step < 0):   {special:>12}  ({special_pct:>6.2f}%)")
    out(f"  Regular steps (id_step >= 0):  {regular:>12}  ({regular_pct:>6.2f}%)")
    out(f"  Total steps:                   {total:>12}")

    # Special steps detail
    out()
    out("Special steps detail (id_step < 0):")
    cur.execute("""
        SELECT step_name, id_step, COUNT(*) as cnt,
               AVG(user_sec) as avg_user, AVG(sys_sec) as avg_sys
        FROM create_step_table
        WHERE id_step < 0
        GROUP BY step_name, id_step
        ORDER BY cnt DESC
    """)
    rows = list(cur)
    special_total = sum(r[2] for r in rows)
    out(f"  {'step_name':>15}  {'id_step':>8}  {'count':>12}  {'%':>8}  {'avg_user_sec':>12}  {'avg_sys_sec':>12}")
    out(f"  {'-'*15}  {'-'*8}  {'-'*12}  {'-'*8}  {'-'*12}  {'-'*12}")
    for name, id_step, cnt, avg_user, avg_sys in rows:
        pct = (cnt / special_total * 100) if special_total else 0
        out(f"  {str(name):>15}  {id_step:>8}  {cnt:>12}  {pct:>7.2f}%  {avg_user:>12.1f}  {avg_sys:>12.1f}")

    # Regular steps — summarize instead of listing all unique names
    out()
    out("Regular steps (id_step >= 0):")
    cur.execute("""
        SELECT COUNT(DISTINCT step_name) as unique_names,
               COUNT(*) as total_steps,
               MIN(id_step) as min_id,
               MAX(id_step) as max_id
        FROM create_step_table
        WHERE id_step >= 0
    """)
    row = cur.fetchone()
    unique_names, total_regular, min_id, max_id = row

    out(f"  Total regular steps:     {total_regular:>12}")
    out(f"  Unique step names:       {unique_names:>12}")
    out(f"  Step ID range:           {min_id} to {max_id}")
    out()
    out("  Note: step_name for regular steps records the command/executable run with srun.")
    out("  Many unique names are user script paths (e.g., '/path/to/myscript.sh').")
    out("  These are not listed individually as they reflect individual job scripts.")

    # Top non-path step names (common patterns)
    out()
    out("Top regular step names (names appearing >100 times — likely common executables):")
    cur.execute("""
        SELECT step_name, COUNT(*) as cnt
        FROM create_step_table
        WHERE id_step >= 0
        GROUP BY step_name
        HAVING cnt > 100
        ORDER BY cnt DESC
        LIMIT 15
    """)
    rows = list(cur)
    if rows:
        out(f"  {'step_name':>40}  {'count':>12}  {'%':>8}")
        out(f"  {'-'*40}  {'-'*12}  {'-'*8}")
        for name, cnt in rows:
            pct = (cnt / total_regular * 100) if total_regular else 0
            name_str = str(name)[:40] if name else '(null)'
            out(f"  {name_str:>40}  {cnt:>12}  {pct:>7.2f}%")
    else:
        out("  No regular step names with count > 100")

    # =========================================================================
    # Part 4: Batch vs srun CPU comparison
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 4: Batch vs srun CPU comparison — checking for double-counting")
    out("=" * 80)
    out()
    out("Data source: create_step_table (step_name, user_sec, sys_sec)")
    out()
    out("For jobs with both batch and srun steps, we compare CPU time to understand")
    out("whether batch step CPU is separate overhead or duplicates srun CPU.")

    # Sample jobs with both batch and other steps
    cur.execute("""
        SELECT
            s.job_db_inx,
            MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
            SUM(CASE WHEN s.step_name NOT IN ('batch', 'interactive', 'extern')
                THEN s.user_sec + s.sys_sec ELSE 0 END) AS srun_cpu,
            SUM(s.user_sec + s.sys_sec) AS total_all_steps,
            COUNT(*) AS num_steps
        FROM create_step_table s
        GROUP BY s.job_db_inx
        HAVING batch_cpu IS NOT NULL AND srun_cpu > 0
        ORDER BY s.job_db_inx DESC
        LIMIT 10
    """)
    out()
    out("Sample jobs with both batch and srun steps:")
    out(f"  {'job_db_inx':>15}  {'batch_cpu':>10}  {'srun_cpu':>10}  {'all_steps':>10}  {'steps':>6}  {'batch_only?':>12}")
    out(f"  {'-'*15}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*6}  {'-'*12}")
    for row in cur:
        job_db_inx, batch_cpu, srun_cpu, total_all, num_steps = row
        # If batch_cpu ≈ total_all, batch might be double-counting
        batch_only = "YES" if batch_cpu and batch_cpu == total_all else "NO"
        out(f"  {job_db_inx:>15}  {batch_cpu:>10}  {srun_cpu:>10}  {total_all:>10}  {num_steps:>6}  {batch_only:>12}")

    out()
    out("  batch_only = YES means batch CPU equals total (no separate srun work tracked)")
    out("  batch_only = NO means srun steps contain the actual computation")

    # =========================================================================
    # Part 5: Distribution of batch CPU time in multi-step jobs
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 5: Distribution of batch CPU time in multi-step jobs")
    out("=" * 80)
    out()
    out("For jobs with both batch and srun steps, how much CPU does batch use?")
    out("This helps determine if batch is overhead or real work.")

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
                SUM(CASE WHEN s.step_name NOT IN ('batch', 'interactive', 'extern')
                    THEN s.user_sec + s.sys_sec ELSE 0 END) AS srun_cpu
            FROM create_step_table s
            GROUP BY s.job_db_inx
            HAVING batch_cpu IS NOT NULL AND srun_cpu > 0
        ) sub
        GROUP BY batch_cpu_range
        ORDER BY MIN(batch_cpu)
    """)
    rows = list(cur)
    total_multi = sum(r[1] for r in rows)

    out()
    out(f"  {'batch_cpu_range':>15}  {'job_count':>12}  {'%':>8}")
    out(f"  {'-'*15}  {'-'*12}  {'-'*8}")
    for batch_range, job_count in rows:
        pct = (job_count / total_multi * 100) if total_multi else 0
        out(f"  {str(batch_range):>15}  {job_count:>12}  {pct:>7.2f}%")
    out()
    out(f"  Total multi-step jobs: {total_multi:,}")
    out()
    out("Interpretation:")
    out("  - If most jobs show batch_cpu in 0-10 sec range, batch is just shell overhead")
    out("  - If batch_cpu is substantial, the job may be doing work in the batch script")
    out("    rather than through srun (which is fine, but affects how we sum CPU time)")

    # Multi-step jobs where batch_cpu is substantial
    out()
    out("Sample multi-step jobs where batch_cpu > 100 sec:")
    cur.execute("""
        SELECT
            s.job_db_inx,
            MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
            SUM(CASE WHEN s.step_name NOT IN ('batch', 'interactive', 'extern')
                THEN s.user_sec + s.sys_sec ELSE 0 END) AS srun_cpu,
            COUNT(*) AS num_steps
        FROM create_step_table s
        GROUP BY s.job_db_inx
        HAVING batch_cpu IS NOT NULL AND batch_cpu > 100 AND srun_cpu > 0
        ORDER BY batch_cpu DESC
        LIMIT 10
    """)
    rows = list(cur)
    if rows:
        out(f"  {'job_db_inx':>15}  {'batch_cpu':>10}  {'srun_cpu':>10}  {'steps':>6}  {'batch/srun':>12}")
        out(f"  {'-'*15}  {'-'*10}  {'-'*10}  {'-'*6}  {'-'*12}")
        for job_db_inx, batch_cpu, srun_cpu, num_steps in rows:
            ratio = batch_cpu / srun_cpu if srun_cpu > 0 else 0
            out(f"  {job_db_inx:>15}  {batch_cpu:>10}  {srun_cpu:>10}  {num_steps:>6}  {ratio:>11.2f}x")
        out()
        out("  batch/srun ≈ 1.0 suggests batch might duplicate srun work")
        out("  batch/srun << 1.0 suggests batch is separate overhead")
    else:
        out("  No multi-step jobs found where batch_cpu > 100 sec")

    # =========================================================================
    # Part 6: Batch-only and interactive-only jobs
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 6: Batch-only and interactive-only jobs")
    out("=" * 80)
    out()
    out("Jobs that have only the submission step (no srun commands).")

    # Batch-only jobs
    out()
    out("Batch-only jobs (batch step, no srun steps):")
    out("  These run everything in the batch script directly (no srun commands).")
    out("  For these, batch step CPU is the total job CPU.")

    cur.execute("""
        SELECT COUNT(*) as batch_only_count
        FROM (
            SELECT job_db_inx
            FROM create_step_table
            GROUP BY job_db_inx
            HAVING SUM(CASE WHEN step_name = 'batch' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN id_step >= 0 THEN 1 ELSE 0 END) = 0
        ) sub
    """)
    batch_only_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT job_db_inx) FROM create_step_table")
    total_jobs_with_steps = cur.fetchone()[0]

    batch_only_pct = (batch_only_count / total_jobs_with_steps * 100) if total_jobs_with_steps else 0

    out()
    out(f"  Batch-only jobs:            {batch_only_count:>12}  ({batch_only_pct:>6.2f}%)")
    out(f"  Total jobs with steps:      {total_jobs_with_steps:>12}")

    # Sample batch-only jobs
    cur.execute("""
        SELECT
            s.job_db_inx,
            MAX(CASE WHEN s.step_name = 'batch' THEN s.user_sec + s.sys_sec END) AS batch_cpu,
            COUNT(*) AS num_steps
        FROM create_step_table s
        GROUP BY s.job_db_inx
        HAVING SUM(CASE WHEN s.step_name = 'batch' THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN s.id_step >= 0 THEN 1 ELSE 0 END) = 0
        ORDER BY batch_cpu DESC
        LIMIT 10
    """)
    out()
    out("  Sample batch-only jobs (highest CPU):")
    out(f"    {'job_db_inx':>15}  {'batch_cpu (sec)':>15}  {'steps':>6}")
    out(f"    {'-'*15}  {'-'*15}  {'-'*6}")
    for row in cur:
        out(f"    {row[0]:>15}  {row[1]:>15}  {row[2]:>6}")

    # Interactive-only jobs
    out()
    out("Interactive-only jobs (interactive step, no srun steps):")
    out("  User got an interactive shell but didn't run any srun commands.")

    cur.execute("""
        SELECT COUNT(*) as interactive_only_count
        FROM (
            SELECT job_db_inx
            FROM create_step_table
            GROUP BY job_db_inx
            HAVING SUM(CASE WHEN step_name = 'interactive' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN id_step >= 0 THEN 1 ELSE 0 END) = 0
        ) sub
    """)
    interactive_only_count = cur.fetchone()[0]
    interactive_only_pct = (interactive_only_count / total_jobs_with_steps * 100) if total_jobs_with_steps else 0

    out()
    out(f"  Interactive-only jobs:      {interactive_only_count:>12}  ({interactive_only_pct:>6.2f}%)")
    out(f"  Total jobs with steps:      {total_jobs_with_steps:>12}")

    if interactive_only_count > 0:
        cur.execute("""
            SELECT
                s.job_db_inx,
                MAX(CASE WHEN s.step_name = 'interactive' THEN s.user_sec + s.sys_sec END) AS interactive_cpu,
                COUNT(*) AS num_steps
            FROM create_step_table s
            GROUP BY s.job_db_inx
            HAVING SUM(CASE WHEN s.step_name = 'interactive' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN s.id_step >= 0 THEN 1 ELSE 0 END) = 0
            ORDER BY interactive_cpu DESC
            LIMIT 10
        """)
        out()
        out("  Sample interactive-only jobs (highest CPU):")
        out(f"    {'job_db_inx':>15}  {'interactive_cpu (sec)':>20}  {'steps':>6}")
        out(f"    {'-'*15}  {'-'*20}  {'-'*6}")
        for row in cur:
            out(f"    {row[0]:>15}  {row[1]:>20}  {row[2]:>6}")

    # =========================================================================
    # Part 7: Multi-node jobs — CPU time in tres_usage_in_max vs user_sec/sys_sec
    # =========================================================================
    out()
    out("=" * 80)
    out("PART 7: Multi-node jobs — CPU accounting via tres_usage_in_max")
    out("=" * 80)
    out()
    out("For multi-node jobs with srun steps, CPU time is often stored differently:")
    out("  - user_sec/sys_sec may be 0 for regular steps")
    out("  - tres_usage_in_max contains the actual CPU time (TRES ID 1 = CPU)")
    out()
    out("This affects efficiency calculations — we need to use tres_usage_in_max")
    out("for multi-node jobs to get accurate CPU time.")

    # Find multi-node jobs where srun steps have user_sec=0 but tres_usage_in_max has values
    cur.execute("""
        SELECT j.job_db_inx, j.id_job, j.cpus_req, j.nodes_alloc,
               j.time_end - j.time_start as elapsed
        FROM create_job_table j
        JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.state = 3
          AND j.nodes_alloc > 1
          AND s.id_step >= 0
          AND s.user_sec = 0
          AND s.sys_sec = 0
          AND s.tres_usage_in_max LIKE '%%1=%%'
        GROUP BY j.job_db_inx
        HAVING COUNT(*) >= 2
        ORDER BY j.nodes_alloc DESC, j.cpus_req DESC
        LIMIT 5
    """)
    jobs = list(cur)

    if jobs:
        out()
        out("Sample multi-node jobs where srun steps have user_sec=0 but tres_usage_in_max has CPU values:")
        for job_db_inx, id_job, cpus_req, nodes_alloc, elapsed in jobs:
            out()
            out(f"  Job {id_job} ({cpus_req} CPUs across {nodes_alloc} nodes, {elapsed} sec elapsed):")

            cur.execute("""
                SELECT id_step, step_name, user_sec, sys_sec, tres_usage_in_max
                FROM create_step_table
                WHERE job_db_inx = %s
                ORDER BY id_step
                LIMIT 10
            """, (job_db_inx,))
            out(f"    {'id_step':>8}  {'step_name':<35}  {'user_sec':>10}  {'sys_sec':>8}  tres_usage_in_max (CPU)")
            out(f"    {'-'*8}  {'-'*35}  {'-'*10}  {'-'*8}  {'-'*30}")
            for step in cur:
                name = str(step[1])[:35] if step[1] else ''
                # Extract CPU value from tres_usage_in_max (format: "1=<cpu_usec>,2=...")
                tres = step[4] or ''
                cpu_val = ''
                if tres and '1=' in tres:
                    for part in tres.split(','):
                        if part.startswith('1='):
                            cpu_val = part[2:]
                            # Format large numbers with commas
                            try:
                                cpu_val = f"{int(cpu_val):,}"
                            except ValueError:
                                pass
                            break
                out(f"    {step[0]:>8}  {name:<35}  {step[2]:>10}  {step[3]:>8}  1={cpu_val}")
    else:
        out()
        out("  No multi-node jobs found with the expected pattern.")

    # Summary of multi-node vs single-node CPU accounting
    out()
    out("Summary: Multi-node vs single-node CPU accounting:")
    cur.execute("""
        SELECT
            CASE WHEN j.nodes_alloc > 1 THEN 'multi-node' ELSE 'single-node' END as node_type,
            COUNT(DISTINCT j.job_db_inx) as job_count,
            SUM(CASE WHEN s.id_step >= 0 AND s.user_sec = 0 AND s.sys_sec = 0
                     AND s.tres_usage_in_max LIKE '%%1=%%' THEN 1 ELSE 0 END) as steps_with_zero_usersec_but_tres,
            SUM(CASE WHEN s.id_step >= 0 THEN 1 ELSE 0 END) as total_regular_steps
        FROM create_job_table j
        JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.state = 3
        GROUP BY node_type
    """)
    out()
    out(f"  {'node_type':>12}  {'jobs':>12}  {'steps_zero_usec_with_tres':>26}  {'total_regular_steps':>20}")
    out(f"  {'-'*12}  {'-'*12}  {'-'*26}  {'-'*20}")
    for row in cur:
        node_type, job_count, zero_steps, total_steps = row
        out(f"  {node_type:>12}  {job_count:>12}  {zero_steps:>26}  {total_steps:>20}")

    out()
    out("Implication: For multi-node jobs, our CPU efficiency calculation may need to")
    out("use tres_usage_in_max instead of user_sec+sys_sec to get accurate values.")

conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
