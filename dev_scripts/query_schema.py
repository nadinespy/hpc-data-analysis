#!/usr/bin/env python3
"""Dump the MySQL Slurm database schema (tables and columns).

This is a basic exploration script to see what tables and columns exist in the
Slurm accounting database. Useful as a starting point for understanding the
database structure.

Output:
- List of all tables in the database
- For each table: all column names and their data types

Key tables for job accounting:
- create_job_table: job-level records
- create_step_table: step-level records (linked via job_db_inx)
- create_assoc_table: user/account associations
- tres_table: TRES ID to resource type mappings

Saves output to output_schema.txt
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
OUTPUT_FILE = OUTPUT_DIR / "output_schema.txt"
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

    # Get all tables
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    out("=== TABLES ===")
    for table in tables:
        out(f"  {table}")

    # Get columns for each table
    for table in tables:
        out(f"\n  === {table} ===")
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        for row in cursor.fetchall():
            col_name = row[0]
            col_type = row[1]
            out(f"  {col_name:<30} {col_type}")

cursor.close()
conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
