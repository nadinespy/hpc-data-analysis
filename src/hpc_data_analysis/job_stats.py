#!/usr/bin/env python3
"""
Job-Level Metrics Export Tool

Exports per-job efficiency metrics for distribution analysis and visualisation.
Output is suitable for violin plots, scatter plots, and correlation analysis.

Usage:
    python3 job_level_metrics.py --since 2025-01-01 --until 2025-02-01 --output job_level_metrics.csv
    # produces: 2025-01-01_2025-02-01_job_level_metrics.csv
"""

import argparse
import csv
import os
import sys

from hpc_data_analysis.slurm_utils import (
    connect_mysql, discover_special_steps,
    fetch_job_data, calculate_job_metrics,
    LdapClient, load_ad_config, get_user_attribute,
    parse_date_range, format_value,
    INCLUDED_STATES,
)


def write_csv_header(writer, include_faculty=False):
    """Write CSV header row."""
    headers = [
        "job_id", "username"
    ]
    if include_faculty:
        headers.append("faculty")
    headers.extend([
        "submission_type", "step_count",
        "state", "exit_code", "is_success",
        "elapsed_sec", "wait_sec", "timelimit_sec",
        "cpu_eff_req", "cpu_eff_alloc", "mem_eff", "mem_eff_alloc", "time_eff",
        "total_cpu_sec", "user_cpu_sec", "sys_cpu_sec", "user_cpu_pct",
        "maxrss_bytes", "reqmem_bytes", "allocmem_bytes", "mem_type",
        "req_cpus", "alloc_cpus", "n_nodes", "n_tasks",
        "submit_line_ntasks", "submit_line_cpus_per_task", "submit_line_interactive"
    ])
    writer.writerow(headers)


def write_csv_row(job, writer, include_faculty=False):
    """Write a single job as a CSV row."""
    row = [
        str(job["id_job"]),
        job["username"],
    ]
    if include_faculty:
        row.append(job.get("faculty", "unknown"))
    row.extend([
        job.get("submission_type", "unknown"),
        str(job.get("step_count", 0)),
        str(job["state"]),
        str(job["exit_code"]),
        "1" if job["is_success"] else "0",
        format_value(job["elapsed_sec"]),
        format_value(job["wait_sec"]),
        format_value(job["timelimit_sec"]),
        format_value(job["cpu_eff_req"]),
        format_value(job["cpu_eff_alloc"]),
        format_value(job["mem_eff"]),
        format_value(job["mem_eff_alloc"]),
        format_value(job["time_eff"]),
        format_value(job["total_cpu_sec"]),
        format_value(job["user_cpu_sec"]),
        format_value(job["sys_cpu_sec"]),
        format_value(job["user_cpu_pct"]),
        format_value(job["maxrss_bytes"]),
        format_value(job["reqmem_bytes"]),
        format_value(job.get("allocmem_bytes", 0)),
        job.get("mem_type", "unknown"),
        str(job["req_cpus"]),
        str(job.get("alloc_cpus", job["req_cpus"])),
        str(job["n_nodes"]),
        str(job.get("n_tasks", 0)),
        format_value(job.get("submit_line_ntasks")),
        format_value(job.get("submit_line_cpus_per_task")),
        "1" if job.get("submit_line_interactive") else "0",
    ])
    writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Export per-job efficiency metrics for distribution analysis"
    )
    parser.add_argument("--config", default="config.yaml",
                        help="Path to config YAML with MySQL credentials")
    parser.add_argument("--ad_config", default="/etc/hpc_export_stats.yaml",
                        help="Path to AD config YAML file (for faculty lookup)")
    parser.add_argument("--since", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--until", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    parser.add_argument("--include-faculty", action="store_true",
                        help="Include faculty column (requires LDAP lookup)")
    parser.add_argument("--faculty-attr", default="st",
                        help="LDAP attribute for faculty (default: st)")

    args = parser.parse_args()
    since_ts, until_ts = parse_date_range(args.since, args.until)

    # Build output path with date range prefix
    output_dir = os.path.dirname(args.output)
    output_base = os.path.basename(args.output)
    output_path = os.path.join(output_dir, f"{args.since}_{args.until}_{output_base}")

    # Setup LDAP if needed (connection is lazy — made on first lookup)
    ldap_client = None
    ad_config = None
    if args.include_faculty:
        ad_config = load_ad_config(args.ad_config)
        ldap_client = LdapClient(ad_config)

    # Connect to MySQL and discover step IDs
    print("Connecting to MySQL...", file=sys.stderr)
    conn, cursor = connect_mysql(args.config)
    special_steps = discover_special_steps(cursor)

    # Process jobs — stream rows directly to CSV
    print("Querying jobs...", file=sys.stderr)
    user_cache = {}
    ldap_errors = []
    job_count = 0
    included_count = 0

    with open(output_path, 'w') as f:
        print(f"# date_range: {args.since} to {args.until}", file=f)
        writer = csv.writer(f, lineterminator="\n")
        write_csv_header(writer, include_faculty=args.include_faculty)

        for row in fetch_job_data(cursor, since_ts, until_ts, special_steps):
            job_count += 1
            state = row[3]  # state is at index 3 (after job_db_inx, id_job, user)

            if state not in INCLUDED_STATES:
                continue

            job = calculate_job_metrics(row)

            # Add faculty if requested
            if args.include_faculty and ldap_client and ad_config:
                faculty = get_user_attribute(
                    ldap_client, ad_config, job["username"],
                    args.faculty_attr, user_cache, ldap_errors
                )
                job["faculty"] = faculty

            write_csv_row(job, writer, include_faculty=args.include_faculty)
            included_count += 1

    cursor.close()
    conn.close()

    print(f"Processed {job_count} jobs, included {included_count} finished jobs", file=sys.stderr)
    print(f"Output saved to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
