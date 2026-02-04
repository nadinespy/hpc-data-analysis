#!/usr/bin/env python3
"""
Job-Level Metrics Export Tool

Exports per-job efficiency metrics for distribution analysis and visualisation.
Output is suitable for violin plots, scatter plots, and correlation analysis.

Usage:
    python3 job_level_metrics.py --since 2025-01-01 --until 2025-02-01 --output jobs.csv
    python3 job_level_metrics.py --since 2025-01-01 --until 2025-02-01 --output jobs.csv --include-faculty
"""

import argparse
import sys

from hpc_data_analysis.slurm_utils import (
    connect_mysql, discover_special_steps,
    fetch_job_data, calculate_job_metrics,
    LdapClient, load_ad_config, get_user_attribute,
    parse_date_range, format_value,
    INCLUDED_STATES,
)


def output_csv(jobs, outfile, include_faculty=False):
    """Write jobs to CSV file."""
    headers = [
        "job_id", "username"
    ]
    if include_faculty:
        headers.append("faculty")
    headers.extend([
        "state", "exit_code", "is_success",
        "elapsed_sec", "wait_sec", "timelimit_sec",
        "cpu_eff_pct", "mem_eff_pct", "time_eff_pct",
        "total_cpu_sec", "user_cpu_sec", "sys_cpu_sec", "user_cpu_pct",
        "maxrss_bytes", "reqmem_bytes",
        "reqcpus", "nodes"
    ])

    print(",".join(headers), file=outfile)

    for job in jobs:
        row = [
            str(job["id_job"]),
            job["username"],
        ]
        if include_faculty:
            row.append(f'"{job.get("faculty", "unknown")}"')
        row.extend([
            str(job["state"]),
            str(job["exit_code"]),
            "1" if job["is_success"] else "0",
            format_value(job["elapsed"]),
            format_value(job["wait_time"]),
            format_value(job["timelimit_sec"]),
            format_value(job["cpu_eff"]),
            format_value(job["mem_eff"]),
            format_value(job["time_eff"]),
            format_value(job["total_cpu"]),
            format_value(job["total_user"]),
            format_value(job["total_sys"]),
            format_value(job["user_cpu_pct"]),
            format_value(job["maxrss"]),
            format_value(job["reqmem"]),
            str(job["req_cpus"]),
            str(job["nodes_alloc"]),
        ])
        print(",".join(row), file=outfile)


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

    # Process jobs
    print("Querying jobs...", file=sys.stderr)
    jobs = []
    user_cache = {}
    ldap_errors = []
    job_count = 0
    included_count = 0

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

        jobs.append(job)
        included_count += 1

    cursor.close()
    conn.close()

    print(f"Processed {job_count} jobs, included {included_count} finished jobs", file=sys.stderr)

    # Write output
    with open(args.output, 'w') as f:
        output_csv(jobs, f, include_faculty=args.include_faculty)

    print(f"Output saved to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
