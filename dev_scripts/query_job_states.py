#!/usr/bin/env python3
"""Query to see what job states exist in the database (recent jobs only for speed)."""

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

print("=== JOB STATES IN DATABASE (Jan 2025) ===")
print("state_code, count")
cursor.execute("""
    SELECT state, COUNT(*) as cnt
    FROM create_job_table
    WHERE time_submit >= UNIX_TIMESTAMP('2025-01-01')
      AND time_submit < UNIX_TIMESTAMP('2025-02-01')
    GROUP BY state
    ORDER BY cnt DESC
""")
for row in cursor:
    print(f"{row[0]}, {row[1]}")

print("\nTo find what each code means, run:")
print("  sacct -j <job_id> -o JobID,State")
print("for a job with that state, then compare with the code above.")

cursor.close()
conn.close()
