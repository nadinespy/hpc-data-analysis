#!/usr/bin/env python3
"""Query Slurm's internal table_defs_table for schema definitions.

Slurm stores its own table definitions in a special table called table_defs_table.
This contains the CREATE TABLE statements used by Slurm, which can be useful for
understanding the intended schema and any constraints.

This is complementary to dump_schema.py - that script shows what MySQL reports
about the schema, while this shows what Slurm internally defines.

Saves output to output_table_defs.txt
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
OUTPUT_FILE = OUTPUT_DIR / "output_table_defs.txt"
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

    out("=" * 80)
    out("TABLE_DEFS_TABLE - Schema definitions stored by Slurm")
    out("=" * 80)
    out()

    cursor.execute("SELECT table_name, definition FROM table_defs_table")
    for row in cursor:
        table_name, definition = row
        out(f"=== {table_name} ===")
        out(definition)
        out()

cursor.close()
conn.close()
print(f"\nOutput saved to {OUTPUT_FILE}", file=sys.stderr)
