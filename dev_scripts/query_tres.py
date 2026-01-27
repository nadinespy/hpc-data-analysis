#!/usr/bin/env python3
"""Quick script to check TRES table and sample tres_usage_in_max format."""

import mysql.connector

conn = mysql.connector.connect(
    host="ermysqlr2.er.kcl.ac.uk",
    user="slurm_create_ro",
    password="qu4c46wQookqTWdq",
    database="slurm_create"
)
cursor = conn.cursor()

# Query 1: See what resource types exist
print("=== TRES TABLE ===")
cursor.execute("SELECT * FROM tres_table")
for row in cursor:
    print(row)

# Query 2: See a sample of tres_usage_in_max format
print("\n=== SAMPLE tres_usage_in_max ===")
cursor.execute("""
    SELECT tres_usage_in_max
    FROM create_step_table
    WHERE tres_usage_in_max IS NOT NULL AND tres_usage_in_max != ''
    LIMIT 5
""")
for row in cursor:
    print(row)

# Query 3: Also check user_sec/sys_sec to confirm CPU time is there
print("\n=== SAMPLE CPU TIME (user_sec, sys_sec) ===")
cursor.execute("""
    SELECT user_sec, sys_sec, user_usec, sys_usec
    FROM create_step_table
    WHERE user_sec > 0 OR sys_sec > 0
    LIMIT 5
""")
for row in cursor:
    print(row)

cursor.close()
conn.close()
