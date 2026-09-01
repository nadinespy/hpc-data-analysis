#!/usr/bin/env python3
"""Explore TRES (Trackable RESources) encoding in the Slurm database.

TRES is Slurm's system for tracking multiple resource types. Resources are encoded
as comma-separated "id=value" pairs in string columns like tres_req, tres_alloc,
and tres_usage_in_max.

This script shows:
1. The TRES ID mappings from tres_table (what each ID means)
2. Sample tres_req/tres_alloc values from create_job_table (job-level)
3. Sample tres_usage_* values from create_step_table (step-level)

Common TRES IDs (from tres_table):
- 1: CPU count
- 2: Memory in MB
- 3: Energy
- 4: Node count
- 5: Billing
- 1001+: GRES (GPUs, etc.)

Example TRES string: "1=8,2=16000,4=1" means 8 CPUs, 16000 MB memory, 1 node.

Data sources:
- tres_table: TRES ID to type/name mappings
- create_job_table: tres_req (requested), tres_alloc (allocated)
- create_step_table: tres_usage_in_max/ave/tot/min (actual usage)

Saves output to output_tres.txt
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
OUTPUT_FILE = OUTPUT_DIR / "output_tres.txt"
with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)
mysql_conf = config["mysql"]
conn = mysql.connector.connect(
    host=mysql_conf["host"],
    user=mysql_conf["user"],
    password=mysql_conf["password"],
    database=mysql_conf["database"],
)
cursor = conn.cursor()

with open(OUTPUT_FILE, 'w') as f:
    def out(text=""):
        print(text)
        print(text, file=f)

    # =============================================================================
    # Part 1: TRES ID mappings
    # =============================================================================
    out("=" * 80)
    out("TRES TABLE (ID -> type/name mapping)")
    out("=" * 80)
    out("Columns: creation_time, deleted, id, type, name")
    out()
    cursor.execute("SELECT creation_time, deleted, id, type, name FROM tres_table ORDER BY id")
    out(f"  {'creation_time':>14}  {'deleted':>7}  {'id':>4}  {'type':<12}  {'name':<20}")
    out(f"  {'-'*14}  {'-'*7}  {'-'*4}  {'-'*12}  {'-'*20}")
    for row in cursor:
        ctime, deleted, tid, ttype, tname = row
        out(f"  {ctime:>14}  {deleted:>7}  {tid:>4}  {ttype or '':<12}  {tname or '':<20}")

    # =============================================================================
    # Part 2: TRES variables in create_job_table
    # =============================================================================
    out()
    out("=" * 80)
    out("TRES VARIABLES IN create_job_table (job-level)")
    out("=" * 80)
    out("Columns: tres_req (requested), tres_alloc (allocated)")
    out()
    cursor.execute("""
        SELECT id_job, tres_req, tres_alloc
        FROM create_job_table
        WHERE tres_req IS NOT NULL AND tres_req != ''
          AND tres_alloc IS NOT NULL AND tres_alloc != ''
        ORDER BY id_job DESC
        LIMIT 5
    """)
    out("Sample jobs (5 most recent with both tres_req and tres_alloc):")
    out()
    for row in cursor:
        id_job, tres_req, tres_alloc = row
        out(f"  Job {id_job}:")
        out(f"    tres_req:   {tres_req}")
        out(f"    tres_alloc: {tres_alloc}")
        out()

    # =============================================================================
    # Part 3: TRES variables in create_step_table
    # =============================================================================
    out("=" * 80)
    out("TRES VARIABLES IN create_step_table (step-level)")
    out("=" * 80)
    out("Columns: tres_alloc, tres_usage_in_max, tres_usage_in_ave, tres_usage_in_tot,")
    out("         tres_usage_in_min, tres_usage_out_max, tres_usage_out_ave, etc.")
    out()
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
    out("Sample steps (5 most recent with tres_usage_in_max):")
    out()
    for row in cursor:
        job_db_inx, id_step, step_name, tres_alloc, tres_max, tres_ave, tres_tot, tres_min = row
        out(f"  Step {id_step} ('{step_name}') of job_db_inx {job_db_inx}:")
        out(f"    tres_alloc:        {tres_alloc}")
        out(f"    tres_usage_in_max: {tres_max}")
        out(f"    tres_usage_in_ave: {tres_ave}")
        out(f"    tres_usage_in_tot: {tres_tot}")
        out(f"    tres_usage_in_min: {tres_min}")
        out()

cursor.close()
conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
