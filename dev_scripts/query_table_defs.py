#!/usr/bin/env python3
"""Query table_defs_table to see if it contains schema definitions."""

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
print("TABLE_DEFS_TABLE - Schema definitions stored by Slurm")
print("=" * 80)
print()

cursor.execute("SELECT table_name, definition FROM table_defs_table")
for row in cursor:
    table_name, definition = row
    print(f"=== {table_name} ===")
    print(definition)
    print()

cursor.close()
conn.close()
