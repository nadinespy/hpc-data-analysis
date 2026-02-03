#!/usr/bin/env python3
"""
Sample how jobs and steps are stored in the database.

Shows:
1. Job-level data (create_job_table)
2. Step-level data (create_step_table)
3. Jobs with single vs multiple steps
4. Variables relevant for efficiency calculations
"""

import mysql.connector
import yaml

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

# =============================================================================
# Part 1: Sample a single job with all relevant columns
# =============================================================================
print("=" * 80)
print("PART 1: Sample job with all relevant columns (create_job_table)")
print("=" * 80)

cur.execute("""
    SELECT
        job_db_inx,
        id_job,
        state,
        time_submit,
        time_start,
        time_end,
        timelimit,
        cpus_req,
        mem_req,
        nodes_alloc,
        tres_req,
        tres_alloc
    FROM create_job_table
    WHERE time_start > 0 AND time_end > 0
      AND state = 3  -- completed
    ORDER BY id_job DESC
    LIMIT 1
""")
row = cur.fetchone()
if row:
    print(f"""
Job-level data:
  job_db_inx:   {row[0]}  (internal DB key, links to steps)
  id_job:       {row[1]}  (Slurm job ID users see)
  state:        {row[2]}  (3 = completed)
  time_submit:  {row[3]}  (Unix timestamp)
  time_start:   {row[4]}  (Unix timestamp)
  time_end:     {row[5]}  (Unix timestamp)
  timelimit:    {row[6]}  (in minutes)
  cpus_req:     {row[7]}  (requested CPUs - direct column)
  mem_req:      {row[8]}  (requested memory - encoded, may have flag bit)
  nodes_alloc:  {row[9]}  (nodes allocated)
  tres_req:     {row[10]} (requested resources - TRES encoded)
  tres_alloc:   {row[11]} (allocated resources - TRES encoded)
""")
    sample_job_db_inx = row[0]
else:
    print("No completed jobs found")
    sample_job_db_inx = None

# =============================================================================
# Part 2: Show steps for that job
# =============================================================================
print("=" * 80)
print("PART 2: Steps for the sample job (create_step_table)")
print("=" * 80)

if sample_job_db_inx:
    cur.execute("""
        SELECT
            id_step,
            step_name,
            state,
            time_start,
            time_end,
            user_sec,
            user_usec,
            sys_sec,
            sys_usec,
            tres_alloc,
            tres_usage_in_max,
            tres_usage_in_tot
        FROM create_step_table
        WHERE job_db_inx = %s
        ORDER BY id_step
    """, (sample_job_db_inx,))

    rows = cur.fetchall()
    print(f"\nJob {sample_job_db_inx} has {len(rows)} step(s):\n")

    for row in rows:
        print(f"""  Step id_step={row[0]}, name='{row[1]}':
    state:             {row[2]}
    time_start:        {row[3]}
    time_end:          {row[4]}
    user_sec:          {row[5]}
    user_usec:         {row[6]}
    sys_sec:           {row[7]}
    sys_usec:          {row[8]}
    tres_alloc:        {row[9]}
    tres_usage_in_max: {row[10]}
    tres_usage_in_tot: {row[11]}
""")

# =============================================================================
# Part 3: Distribution of step counts per job
# =============================================================================
print("=" * 80)
print("PART 3: How many steps do jobs typically have?")
print("=" * 80)

cur.execute("""
    SELECT
        step_count,
        COUNT(*) as job_count
    FROM (
        SELECT job_db_inx, COUNT(*) as step_count
        FROM create_step_table
        GROUP BY job_db_inx
    ) counts
    GROUP BY step_count
    ORDER BY step_count
    LIMIT 20
""")

print(f"\n  {'steps':>6}  {'jobs':>12}")
print(f"  {'-'*6}  {'-'*12}")
for row in cur:
    print(f"  {row[0]:>6}  {row[1]:>12}")

# =============================================================================
# Part 4: Sample job with SINGLE step
# =============================================================================
print("\n" + "=" * 80)
print("PART 4: Sample job with SINGLE step")
print("=" * 80)

cur.execute("""
    SELECT j.job_db_inx, j.id_job, j.cpus_req, j.tres_req, j.tres_alloc,
           j.time_start, j.time_end,
           s.id_step, s.step_name, s.user_sec, s.sys_sec, s.user_usec, s.sys_usec,
           s.tres_usage_in_max
    FROM create_job_table j
    JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
    WHERE j.state = 3
      AND j.time_start > 0 AND j.time_end > 0
    GROUP BY j.job_db_inx
    HAVING COUNT(*) = 1
    ORDER BY j.id_job DESC
    LIMIT 1
""")
row = cur.fetchone()
if row:
    print(f"""
Job {row[1]} (single step):
  Job-level:
    cpus_req:       {row[2]}
    tres_req:       {row[3]}
    tres_alloc:     {row[4]}
    time_start:     {row[5]}
    time_end:       {row[6]}

  Step: id={row[7]}, name='{row[8]}'
    user_sec:          {row[9]}
    sys_sec:           {row[10]}
    user_usec:         {row[11]}
    sys_usec:          {row[12]}
    tres_usage_in_max: {row[13]}
""")

# =============================================================================
# Part 5: Sample job with MULTIPLE steps
# =============================================================================
print("=" * 80)
print("PART 5: Sample job with MULTIPLE steps")
print("=" * 80)

cur.execute("""
    SELECT j.job_db_inx, j.id_job, j.cpus_req, j.tres_req, j.tres_alloc,
           j.time_start, j.time_end
    FROM create_job_table j
    JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
    WHERE j.state = 3
      AND j.time_start > 0 AND j.time_end > 0
    GROUP BY j.job_db_inx
    HAVING COUNT(*) > 2
    ORDER BY j.id_job DESC
    LIMIT 1
""")
job = cur.fetchone()
if job:
    job_db_inx, id_job, cpus_req, tres_req, tres_alloc, time_start, time_end = job

    print(f"""
Job {id_job} (multiple steps):
  Job-level:
    cpus_req:    {cpus_req}
    tres_req:    {tres_req}
    tres_alloc:  {tres_alloc}
    time_start:  {time_start}
    time_end:    {time_end}

  Steps:""")

    cur.execute("""
        SELECT id_step, step_name, user_sec, sys_sec, user_usec, sys_usec, tres_usage_in_max
        FROM create_step_table
        WHERE job_db_inx = %s
        ORDER BY id_step
    """, (job_db_inx,))

    print(f"    {'id_step':>8}  {'step_name':<12}  {'user_sec':>10}  {'sys_sec':>10}  {'user_usec':>10}  {'sys_usec':>10}  tres_usage_in_max")
    print(f"    {'-'*8}  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*20}")
    for step in cur:
        print(f"    {step[0]:>8}  {str(step[1]):<12}  {step[2]:>10}  {step[3]:>10}  {step[4]:>10}  {step[5]:>10}  {step[6]}")

# =============================================================================
# Part 6: What step names exist?
# =============================================================================
print("\n" + "=" * 80)
print("PART 6: Step names and their frequencies")
print("=" * 80)

cur.execute("""
    SELECT step_name, id_step, COUNT(*) as cnt
    FROM create_step_table
    GROUP BY step_name, id_step
    ORDER BY cnt DESC
    LIMIT 20
""")

print(f"\n  {'step_name':<20}  {'id_step':>8}  {'count':>12}")
print(f"  {'-'*20}  {'-'*8}  {'-'*12}")
for row in cur:
    print(f"  {str(row[0]):<20}  {row[1]:>8}  {row[2]:>12}")

cur.close()
conn.close()
