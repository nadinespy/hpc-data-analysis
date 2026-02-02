#!/usr/bin/env python3
"""Sample CPU time variables from create_step_table.

CPU time is stored directly in columns (not TRES-encoded):
- user_sec, user_usec: user-space CPU time
- sys_sec, sys_usec: kernel/system CPU time
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
cursor = conn.cursor()

print("=" * 80)
print("CPU TIME VARIABLES IN create_step_table")
print("=" * 80)
print("Columns: user_sec, user_usec, sys_sec, sys_usec")
print("  - user_sec/usec: CPU time spent in user space")
print("  - sys_sec/usec: CPU time spent in kernel/system calls")
print("  - Total CPU time = user_sec + sys_sec + (user_usec + sys_usec) / 1,000,000")
print()

cursor.execute("""
    SELECT
        job_db_inx,
        id_step,
        step_name,
        user_sec,
        user_usec,
        sys_sec,
        sys_usec
    FROM create_step_table
    WHERE user_sec > 0 OR sys_sec > 0
    ORDER BY job_db_inx DESC
    LIMIT 10
""")

print("Sample steps (10 most recent with CPU time > 0):")
print()
print(f"  {'job_db_inx':>12}  {'id_step':>8}  {'step_name':>12}  {'user_sec':>10}  {'user_usec':>10}  {'sys_sec':>10}  {'sys_usec':>10}  {'total_cpu':>12}")
print(f"  {'-'*12}  {'-'*8}  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*12}")

for row in cursor:
    job_db_inx, id_step, step_name, user_sec, user_usec, sys_sec, sys_usec = row
    total_cpu = user_sec + sys_sec + (user_usec + sys_usec) / 1_000_000
    print(f"  {job_db_inx:>12}  {id_step:>8}  {str(step_name):>12}  {user_sec:>10}  {user_usec:>10}  {sys_sec:>10}  {sys_usec:>10}  {total_cpu:>12.2f}")

cursor.close()
conn.close()
