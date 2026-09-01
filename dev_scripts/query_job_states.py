#!/usr/bin/env python3
"""Query job state distribution in the Slurm database.

Shows the count of jobs by state code for a recent time period (Jan 2025).
This helps understand what job outcomes exist and their relative frequency.

Slurm state codes (from slurm.h):
- 0: PENDING      - Job is awaiting resource allocation
- 1: RUNNING      - Job currently has an allocation
- 2: SUSPENDED    - Job has an allocation, but execution has been suspended
- 3: COMPLETED    - Job has terminated with exit code of zero
- 4: CANCELLED    - Job was explicitly cancelled by user or admin
- 5: FAILED       - Job terminated with non-zero exit code or other failure
- 6: TIMEOUT      - Job terminated upon reaching its time limit
- 7: NODE_FAIL    - Job terminated due to failure of one or more allocated nodes
- 8: PREEMPTED    - Job terminated due to preemption
- 9: BOOT_FAIL    - Job terminated due to node boot failure
- 10: DEADLINE    - Job terminated on deadline
- 11: OOM         - Job experienced out of memory error (OUT_OF_MEMORY)

For efficiency analysis, we typically include: COMPLETED (3), TIMEOUT (6), OOM (11)
as these represent jobs that actually ran and used resources.

Saves output to output_job_states.txt
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
OUTPUT_FILE = OUTPUT_DIR / "output_job_states.txt"
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

    out("=== JOB STATES IN DATABASE (Jan 2025) ===")
    out("state_code, count")
    cursor.execute("""
        SELECT state, COUNT(*) as cnt
        FROM create_job_table
        WHERE time_submit >= UNIX_TIMESTAMP('2025-01-01')
          AND time_submit < UNIX_TIMESTAMP('2025-02-01')
        GROUP BY state
        ORDER BY cnt DESC
    """)
    for row in cursor:
        out(f"{row[0]}, {row[1]}")

    out("\nTo find what each code means, run:")
    out("  sacct -j <job_id> -o JobID,State")
    out("for a job with that state, then compare with the code above.")

cursor.close()
conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
