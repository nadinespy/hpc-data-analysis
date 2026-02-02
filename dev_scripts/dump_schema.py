#!/usr/bin/env python3
"""Dump the MySQL Slurm database schema (tables and columns)."""

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

# Get all tables
cursor.execute("SHOW TABLES")
tables = [row[0] for row in cursor.fetchall()]

print("=== TABLES ===")
for table in tables:
    print(f"  {table}")

# Get columns for each table
for table in tables:
    print(f"\n  === {table} ===")
    cursor.execute(f"SHOW COLUMNS FROM {table}")
    for row in cursor.fetchall():
        col_name = row[0]
        col_type = row[1]
        print(f"  {col_name:<30} {col_type}")

cursor.close()
conn.close()
