#!/usr/bin/env python3
"""Query TRES table and sample all TRES-related variables from job and step tables."""

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
cursor = conn.cursor()

# =============================================================================
# Part 1: TRES ID mappings
# =============================================================================
print("=" * 80)
print("TRES TABLE (ID → type/name mapping)")
print("=" * 80)
print("Columns: creation_time, deleted, id, type, name")
print()
cursor.execute("SELECT creation_time, deleted, id, type, name FROM tres_table ORDER BY id")
print(f"  {'creation_time':>14}  {'deleted':>7}  {'id':>4}  {'type':<12}  {'name':<20}")
print(f"  {'-'*14}  {'-'*7}  {'-'*4}  {'-'*12}  {'-'*20}")
for row in cursor:
    ctime, deleted, tid, ttype, tname = row
    print(f"  {ctime:>14}  {deleted:>7}  {tid:>4}  {ttype or '':<12}  {tname or '':<20}")

# =============================================================================
# Part 2: TRES variables in create_job_table
# =============================================================================
print()
print("=" * 80)
print("TRES VARIABLES IN create_job_table (job-level)")
print("=" * 80)
print("Columns: tres_req (requested), tres_alloc (allocated)")
print()
cursor.execute("""
    SELECT id_job, tres_req, tres_alloc
    FROM create_job_table
    WHERE tres_req IS NOT NULL AND tres_req != ''
      AND tres_alloc IS NOT NULL AND tres_alloc != ''
    ORDER BY id_job DESC
    LIMIT 5
""")
print("Sample jobs (5 most recent with both tres_req and tres_alloc):")
print()
for row in cursor:
    id_job, tres_req, tres_alloc = row
    print(f"  Job {id_job}:")
    print(f"    tres_req:   {tres_req}")
    print(f"    tres_alloc: {tres_alloc}")
    print()

# =============================================================================
# Part 3: TRES variables in create_step_table
# =============================================================================
print("=" * 80)
print("TRES VARIABLES IN create_step_table (step-level)")
print("=" * 80)
print("Columns: tres_alloc, tres_usage_in_max, tres_usage_in_ave, tres_usage_in_tot,")
print("         tres_usage_in_min, tres_usage_out_max, tres_usage_out_ave, etc.")
print()
cursor.execute("""
    SELECT
        job_db_inx,
        id_step,
        step_name,
        tres_alloc,
        tres_usage_in_max,
        tres_usage_in_ave,
        tres_usage_in_tot,
        tres_usage_in_min
    FROM create_step_table
    WHERE tres_usage_in_max IS NOT NULL AND tres_usage_in_max != ''
    ORDER BY job_db_inx DESC
    LIMIT 5
""")
print("Sample steps (5 most recent with tres_usage_in_max):")
print()
for row in cursor:
    job_db_inx, id_step, step_name, tres_alloc, tres_max, tres_ave, tres_tot, tres_min = row
    print(f"  Step {id_step} ('{step_name}') of job_db_inx {job_db_inx}:")
    print(f"    tres_alloc:        {tres_alloc}")
    print(f"    tres_usage_in_max: {tres_max}")
    print(f"    tres_usage_in_ave: {tres_ave}")
    print(f"    tres_usage_in_tot: {tres_tot}")
    print(f"    tres_usage_in_min: {tres_min}")
    print()

cursor.close()
conn.close()
