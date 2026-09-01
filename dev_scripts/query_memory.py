#!/usr/bin/env python3
"""Investigate memory unit encoding in Slurm's TRES fields.

This script helps determine the units used for memory values in different fields:
- tres_req (TRES ID 2): Requested memory - stored in MB
- tres_usage_in_max (TRES ID 2): Actual peak memory - stored in BYTES

The script samples a few jobs and calculates memory efficiency under different
unit assumptions to verify which interpretation is correct:
- If maxrss is in bytes and reqmem is in MB: efficiency = maxrss / (reqmem * 1024²)
- If both are in same units: efficiency = maxrss / reqmem

Typical finding: tres_req memory is in MB, tres_usage_in_max memory is in bytes.
This means memory efficiency should be calculated as:
  mem_eff = (maxrss_bytes) / (reqmem_MB * 1024 * 1024) * 100%

Data sources:
- create_job_table: tres_req (TRES ID 2 = requested memory in MB)
- create_step_table: tres_usage_in_max (TRES ID 2 = peak memory in bytes)

Saves output to output_memory.txt
"""

import sys
from pathlib import Path
import mysql.connector
import re
import yaml

# Paths relative to script location (allows running from any directory)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "output_memory.txt"
def parse_tres_value(tres_string, tres_id):
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

    # Get a few jobs with their steps
    cursor.execute("""
        SELECT j.id_job, j.job_db_inx, j.tres_req, a.user
        FROM create_job_table j
        JOIN create_assoc_table a ON j.id_assoc = a.id_assoc
        WHERE j.time_submit >= UNIX_TIMESTAMP('2025-01-01')
          AND j.time_submit < UNIX_TIMESTAMP('2025-01-02')
          AND j.state = 3
        LIMIT 5
    """)

    jobs = list(cursor)

    out("=== JOB MEMORY ANALYSIS ===\n")

    for id_job, job_db_inx, tres_req, user in jobs:
        out(f"Job {id_job} (user: {user})")
        out(f"  tres_req: {tres_req}")

        reqmem_raw = parse_tres_value(tres_req, 2)
        out(f"  Requested memory (raw value from tres_req): {reqmem_raw}")
        out(f"  If MB: {reqmem_raw} MB = {reqmem_raw / 1024:.2f} GB")

        # Get steps for this job
        cursor.execute("""
            SELECT id_step, tres_usage_in_max
            FROM create_step_table
            WHERE job_db_inx = %s
        """, (job_db_inx,))

        steps = list(cursor)
        out(f"  Steps ({len(steps)}):")

        max_mem = 0
        for step_id, tres_usage in steps:
            mem_raw = parse_tres_value(tres_usage, 2)
            if mem_raw > max_mem:
                max_mem = mem_raw
            out(f"    Step {step_id}: tres_usage_in_max = {tres_usage}")
            out(f"      Memory (raw): {mem_raw}")
            if mem_raw > 0:
                out(f"      If bytes: {mem_raw / 1024 / 1024:.2f} MB")
                out(f"      If KB: {mem_raw / 1024:.2f} MB = {mem_raw / 1024 / 1024:.2f} GB")

        out(f"  Max memory across steps (raw): {max_mem}")
        if max_mem > 0 and reqmem_raw > 0:
            # Try different unit interpretations
            out(f"  Efficiency if maxrss=bytes, reqmem=MB: {max_mem / (reqmem_raw * 1024 * 1024) * 100:.2f}%")
            out(f"  Efficiency if maxrss=KB, reqmem=MB: {(max_mem * 1024) / (reqmem_raw * 1024 * 1024) * 100:.2f}%")
            out(f"  Efficiency if both in same units: {max_mem / reqmem_raw * 100:.2f}%")
        out()

cursor.close()
conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
