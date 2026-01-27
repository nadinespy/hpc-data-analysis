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
    LdapClient, load_ad_config, get_user_attribute,
    parse_tres_value, parse_date_range, format_value,
    TRES_MEM_ID, FINISHED_STATES, SUCCESS_STATES,
)


def fetch_job_data(cursor, since_ts, until_ts, special_steps):
    """Fetch per-job data from MySQL with step aggregation.

    CPU time: sums regular steps only; falls back to batch step for jobs
    without srun steps (avoids double-counting batch + srun steps).
    Memory: extracts numeric memory value from TRES string per step,
    then takes the numeric MAX (not string MAX).

    Args:
        special_steps: dict from discover_special_steps(), e.g. {'batch': -5}
    """
    # Build exclusion list from all discovered special steps
    exclude_ids = ", ".join(str(sid) for sid in special_steps.values())
    batch_id = special_steps.get("batch")

    query = f"""
        SELECT
            j.id_job,
            a.user,
            j.state,
            j.exit_code,
            j.time_submit,
            j.time_start,
            j.time_end,
            j.cpus_req,
            j.tres_req,
            j.timelimit,
            j.nodes_alloc,
            COALESCE(
                NULLIF(SUM(CASE WHEN s.id_step NOT IN ({exclude_ids})
                                THEN s.user_sec ELSE 0 END), 0),
                MAX(CASE WHEN s.id_step = {batch_id} THEN s.user_sec END),
                0
            ) AS total_user_sec,
            COALESCE(
                NULLIF(SUM(CASE WHEN s.id_step NOT IN ({exclude_ids})
                                THEN s.sys_sec ELSE 0 END), 0),
                MAX(CASE WHEN s.id_step = {batch_id} THEN s.sys_sec END),
                0
            ) AS total_sys_sec,
            COALESCE(
                NULLIF(SUM(CASE WHEN s.id_step NOT IN ({exclude_ids})
                                THEN s.user_usec + s.sys_usec ELSE 0 END), 0),
                MAX(CASE WHEN s.id_step = {batch_id} THEN s.user_usec + s.sys_usec END),
                0
            ) AS total_cpu_usec,
            MAX(
                CAST(
                    SUBSTRING_INDEX(
                        SUBSTRING_INDEX(CONCAT(',', s.tres_usage_in_max), ',2=', -1),
                        ',', 1
                    ) AS UNSIGNED
                )
            ) AS max_mem_bytes
        FROM create_job_table j
        JOIN create_assoc_table a ON j.id_assoc = a.id_assoc
        LEFT JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.time_submit >= %s
          AND j.time_submit < %s
          AND j.time_start > 0
          AND j.time_end > 0
          AND j.time_end >= j.time_start
        GROUP BY j.job_db_inx
    """
    cursor.execute(query, (since_ts, until_ts))
    return cursor


def process_job(row):
    """Process a single job row and return metrics dict."""
    (id_job, username, state, exit_code, time_submit, time_start, time_end,
     alloc_cpus, tres_req, timelimit, nodes_alloc,
     total_user_sec, total_sys_sec, total_cpu_usec, max_mem_bytes) = row

    # Convert types
    time_submit = float(time_submit) if time_submit else 0
    time_start = float(time_start) if time_start else 0
    time_end = float(time_end) if time_end else 0
    total_user_sec = float(total_user_sec) if total_user_sec else 0
    total_sys_sec = float(total_sys_sec) if total_sys_sec else 0
    total_cpu_usec = float(total_cpu_usec) if total_cpu_usec else 0
    nodes_alloc = int(nodes_alloc) if nodes_alloc else 0
    alloc_cpus = alloc_cpus or 0
    timelimit = timelimit or 0

    # Derived values
    elapsed = time_end - time_start if time_end and time_start else 0
    wait_time = time_start - time_submit if time_start and time_submit else 0
    total_cpu = total_user_sec + total_sys_sec + (total_cpu_usec / 1_000_000)
    timelimit_sec = timelimit * 60

    # Memory values
    maxrss = int(max_mem_bytes) if max_mem_bytes else 0
    reqmem_mb = parse_tres_value(tres_req, TRES_MEM_ID)
    reqmem = reqmem_mb * 1024 * 1024

    # Per-job efficiencies
    cpu_allocated = elapsed * alloc_cpus
    cpu_eff = (total_cpu / cpu_allocated * 100) if cpu_allocated > 0 else None
    mem_eff = (maxrss / reqmem * 100) if reqmem > 0 else None
    time_eff = (elapsed / timelimit_sec * 100) if timelimit_sec > 0 else None

    # User vs system CPU ratio
    total_cpu_time = total_user_sec + total_sys_sec
    user_cpu_pct = (total_user_sec / total_cpu_time * 100) if total_cpu_time > 0 else None

    is_success = state in SUCCESS_STATES

    return {
        "job_id": id_job,
        "username": username,
        "state": state,
        "exit_code": exit_code or 0,
        "is_success": is_success,
        "elapsed_sec": elapsed,
        "wait_sec": wait_time,
        "timelimit_sec": timelimit_sec,
        "cpu_eff_pct": cpu_eff,
        "mem_eff_pct": mem_eff,
        "time_eff_pct": time_eff,
        "total_cpu_sec": total_cpu,
        "user_cpu_sec": total_user_sec,
        "sys_cpu_sec": total_sys_sec,
        "user_cpu_pct": user_cpu_pct,
        "maxrss_bytes": maxrss,
        "reqmem_bytes": reqmem,
        "alloccpus": alloc_cpus,
        "nodes": nodes_alloc,
    }


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
        "alloccpus", "nodes"
    ])

    print(",".join(headers), file=outfile)

    for job in jobs:
        row = [
            str(job["job_id"]),
            job["username"],
        ]
        if include_faculty:
            row.append(f'"{job.get("faculty", "unknown")}"')
        row.extend([
            str(job["state"]),
            str(job["exit_code"]),
            "1" if job["is_success"] else "0",
            format_value(job["elapsed_sec"]),
            format_value(job["wait_sec"]),
            format_value(job["timelimit_sec"]),
            format_value(job["cpu_eff_pct"]),
            format_value(job["mem_eff_pct"]),
            format_value(job["time_eff_pct"]),
            format_value(job["total_cpu_sec"]),
            format_value(job["user_cpu_sec"]),
            format_value(job["sys_cpu_sec"]),
            format_value(job["user_cpu_pct"]),
            format_value(job["maxrss_bytes"]),
            format_value(job["reqmem_bytes"]),
            str(job["alloccpus"]),
            str(job["nodes"]),
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
        state = row[2]  # state is at index 2 in this query

        if state not in FINISHED_STATES:
            continue

        job = process_job(row)

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
