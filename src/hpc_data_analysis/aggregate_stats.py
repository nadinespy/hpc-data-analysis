#!/usr/bin/env python3
"""
HPC Aggregate Statistics Export Tool

Produces aggregated statistics by faculty (or other LDAP attributes),
including resource efficiency metrics.

Usage:
    python3 hpc_stats.py --collate_by st=faculty --collate_by none --since 2025-01-01 --until 2025-02-01 --output hpc_stats_output.csv
    # produces: 2025-01-01_2025-02-01_hpc_stats_output.csv
"""

import argparse
import csv
import os
import re
import sys

from hpc_data_analysis.slurm_utils import (
    connect_mysql, discover_special_steps,
    fetch_job_data, calculate_job_metrics,
    LdapClient, load_ad_config, get_user_attribute,
    parse_date_range, format_value,
    INCLUDED_STATES, SUCCESS_STATES, FAILED_STATES, STATE_NAMES,
)


# =============================================================================
# Argument Parsing
# =============================================================================

def parse_collate_by_arg(value):
    """Parse --collate_by argument like 'st=faculty' into (attr, label) tuple."""
    if value.lower() == 'none':
        return None
    match = re.match(r"^([a-zA-Z0-9]+)=(.+)$", value)
    if not match:
        raise argparse.ArgumentTypeError(
            f"Invalid collate_by format: {value}. Use 'attr=label' or 'none'"
        )
    return match.groups()


# =============================================================================
# Statistics Tracking
# =============================================================================

def init_stats_dict():
    """Initialize a statistics dictionary for a group."""
    return {
        # Counts (across all job states)
        "job_count": 0,
        "job_count_success": 0,  # COMPLETED only
        "job_count_failed": 0,   # All FAILED_STATES
        # State counts for non-success efficiency-relevant states
        "count_timeout": 0,
        "count_out_of_memory": 0,
        "count_interactive": 0,
        "count_with_ntasks": 0,
        "count_with_cpus_per_task": 0,

        # Resource totals (only for INCLUDED_STATES jobs)
        "total_elapsed": 0,
        "total_cpu": 0,
        "total_user_cpu": 0,
        "total_sys_cpu": 0,
        "total_maxrss": 0,
        "total_reqmem": 0,
        "total_reqcpus": 0,
        "total_timelimit": 0,
        "total_nodes": 0,
        "total_wait": 0,

        # For efficiency calculations - requested CPUs
        "sum_cpu_requested": 0,
        "sum_job_cpu_eff_req": 0,
        "count_cpu_eff_req": 0,
        # For efficiency calculations - allocated CPUs
        "sum_cpu_allocated": 0,
        "sum_job_cpu_eff_alloc": 0,
        "count_cpu_eff_alloc": 0,
        # For efficiency calculations - memory (requested) and time
        "sum_job_mem_eff": 0,
        "sum_job_time_eff": 0,
        "count_mem_eff": 0,
        "count_time_eff": 0,
        # For efficiency calculations - memory (allocated)
        "total_allocmem": 0,
        "sum_job_mem_eff_alloc": 0,
        "count_mem_eff_alloc": 0,
    }


# =============================================================================
# Job Processing
# =============================================================================

def update_stats(metrics, stats, collate_key):
    """Update stats dictionary with job metrics.

    All jobs contribute to counts (job_count, success/failed).
    Only INCLUDED_STATES jobs contribute to efficiency metrics and state counts.
    """
    s = stats[collate_key]
    state = metrics["state"]
    is_success = metrics["is_success"]
    is_failed = state in FAILED_STATES
    include_in_efficiency = state in INCLUDED_STATES

    # All jobs: basic counts
    s["job_count"] += 1
    if is_success:
        s["job_count_success"] += 1
    elif is_failed:
        s["job_count_failed"] += 1
    # Note: CANCELLED jobs count in job_count but not in success or failed

    # Submit-line derived counts (apply to all jobs)
    if metrics.get("submit_line_interactive"):
        s["count_interactive"] += 1
    if metrics.get("submit_line_ntasks") is not None:
        s["count_with_ntasks"] += 1
    if metrics.get("submit_line_cpus_per_task") is not None:
        s["count_with_cpus_per_task"] += 1

    # Only INCLUDED_STATES jobs: state counts, resource totals, efficiency metrics
    if not include_in_efficiency:
        return

    # State counts for non-success efficiency-relevant states
    state_name = STATE_NAMES.get(state)
    if state_name in ("timeout", "out_of_memory"):
        s[f"count_{state_name}"] += 1

    s["total_elapsed"] += metrics["elapsed_sec"]
    s["total_cpu"] += metrics["total_cpu_sec"]
    s["total_user_cpu"] += metrics["user_cpu_sec"]
    s["total_sys_cpu"] += metrics["sys_cpu_sec"]
    s["total_maxrss"] += metrics["maxrss_bytes"]
    s["total_reqmem"] += metrics["reqmem_bytes"]
    s["total_reqcpus"] += metrics["req_cpus"]
    s["total_timelimit"] += metrics["timelimit_sec"]
    s["total_nodes"] += metrics["n_nodes"]
    s["total_wait"] += metrics["wait_sec"]
    s["sum_cpu_requested"] += metrics["cpu_requested"]
    s["sum_cpu_allocated"] += metrics["cpu_allocated"]

    s["total_allocmem"] += metrics.get("allocmem_bytes", 0)

    cpu_eff_req = metrics["cpu_eff_req"]
    cpu_eff_alloc = metrics["cpu_eff_alloc"]
    mem_eff = metrics["mem_eff"]
    mem_eff_alloc = metrics.get("mem_eff_alloc")
    time_eff = metrics["time_eff"]

    if cpu_eff_req is not None:
        s["sum_job_cpu_eff_req"] += cpu_eff_req
        s["count_cpu_eff_req"] += 1
    if cpu_eff_alloc is not None:
        s["sum_job_cpu_eff_alloc"] += cpu_eff_alloc
        s["count_cpu_eff_alloc"] += 1
    if mem_eff is not None:
        s["sum_job_mem_eff"] += mem_eff
        s["count_mem_eff"] += 1
    if mem_eff_alloc is not None:
        s["sum_job_mem_eff_alloc"] += mem_eff_alloc
        s["count_mem_eff_alloc"] += 1
    if time_eff is not None:
        s["sum_job_time_eff"] += time_eff
        s["count_time_eff"] += 1


def calculate_final_efficiencies(s):
    """Calculate final weighted and average efficiencies."""
    # Count of jobs included in efficiency stats (INCLUDED_STATES)
    eff_job_count = s["job_count_success"] + s["count_timeout"] + s["count_out_of_memory"]

    # Efficiency metrics - CPU based on requested CPUs
    s["weighted_cpu_eff_req"] = (s["total_cpu"] / s["sum_cpu_requested"] * 100) if s["sum_cpu_requested"] > 0 else None
    s["avg_cpu_eff_req"] = (s["sum_job_cpu_eff_req"] / s["count_cpu_eff_req"]) if s["count_cpu_eff_req"] > 0 else None
    # Efficiency metrics - CPU based on allocated CPUs
    s["weighted_cpu_eff_alloc"] = (s["total_cpu"] / s["sum_cpu_allocated"] * 100) if s["sum_cpu_allocated"] > 0 else None
    s["avg_cpu_eff_alloc"] = (s["sum_job_cpu_eff_alloc"] / s["count_cpu_eff_alloc"]) if s["count_cpu_eff_alloc"] > 0 else None
    # Efficiency metrics - memory (requested) and time
    s["weighted_mem_eff"] = (s["total_maxrss"] / s["total_reqmem"] * 100) if s["total_reqmem"] > 0 else None
    s["weighted_time_eff"] = (s["total_elapsed"] / s["total_timelimit"] * 100) if s["total_timelimit"] > 0 else None
    s["avg_mem_eff"] = (s["sum_job_mem_eff"] / s["count_mem_eff"]) if s["count_mem_eff"] > 0 else None
    s["avg_time_eff"] = (s["sum_job_time_eff"] / s["count_time_eff"]) if s["count_time_eff"] > 0 else None
    # Efficiency metrics - memory (allocated)
    s["weighted_mem_eff_alloc"] = (s["total_maxrss"] / s["total_allocmem"] * 100) if s["total_allocmem"] > 0 else None
    s["avg_mem_eff_alloc"] = (s["sum_job_mem_eff_alloc"] / s["count_mem_eff_alloc"]) if s["count_mem_eff_alloc"] > 0 else None

    # Resource averages (based on INCLUDED_STATES jobs only)
    s["avg_elapsed"] = (s["total_elapsed"] / eff_job_count) if eff_job_count > 0 else None
    s["avg_cpu"] = (s["total_cpu"] / eff_job_count) if eff_job_count > 0 else None
    s["avg_reqcpus"] = (s["total_reqcpus"] / eff_job_count) if eff_job_count > 0 else None
    s["avg_reqmem"] = (s["total_reqmem"] / eff_job_count) if eff_job_count > 0 else None
    s["avg_maxrss"] = (s["total_maxrss"] / eff_job_count) if eff_job_count > 0 else None
    s["avg_wait"] = (s["total_wait"] / eff_job_count) if eff_job_count > 0 else None

    # User/System CPU ratio
    total_cpu_time = s["total_user_cpu"] + s["total_sys_cpu"]
    s["user_cpu_pct"] = (s["total_user_cpu"] / total_cpu_time * 100) if total_cpu_time > 0 else None
    s["sys_cpu_pct"] = (s["total_sys_cpu"] / total_cpu_time * 100) if total_cpu_time > 0 else None


# =============================================================================
# Output
# =============================================================================

def output_csv(stats, collate_label, outfile=None, include_header=True):
    """Output statistics as CSV."""
    headers = [
        collate_label if collate_label else "global",
        # Job counts
        "job_count", "job_count_success", "job_count_failed",
        "count_timeout", "count_out_of_memory",
        "count_interactive", "count_with_ntasks", "count_with_cpus_per_task",
        # Resource totals and averages
        "total_elapsed_sec", "avg_elapsed_sec",
        "total_cpu_sec", "avg_cpu_sec",
        "total_user_cpu_sec", "total_sys_cpu_sec", "user_cpu_pct", "sys_cpu_pct",
        "total_maxrss_bytes", "avg_maxrss_bytes",
        "total_reqmem_bytes", "avg_reqmem_bytes",
        "total_allocmem_bytes",
        "total_reqcpus", "avg_reqcpus",
        "total_nodes",
        "total_wait_sec", "avg_wait_sec",
        # Efficiency metrics
        "weighted_cpu_eff_req", "avg_cpu_eff_req",
        "weighted_cpu_eff_alloc", "avg_cpu_eff_alloc",
        "weighted_mem_eff", "avg_mem_eff",
        "weighted_mem_eff_alloc", "avg_mem_eff_alloc",
        "weighted_time_eff", "avg_time_eff",
    ]

    out = outfile if outfile else sys.stdout
    writer = csv.writer(out, lineterminator="\n")
    if include_header:
        writer.writerow(headers)

    for key, s in sorted(stats.items(), key=lambda x: -x[1]["job_count"]):
        row = [
            key,
            # Job counts
            format_value(s["job_count"]),
            format_value(s["job_count_success"]),
            format_value(s["job_count_failed"]),
            format_value(s["count_timeout"]),
            format_value(s["count_out_of_memory"]),
            format_value(s["count_interactive"]),
            format_value(s["count_with_ntasks"]),
            format_value(s["count_with_cpus_per_task"]),
            # Resource totals and averages
            format_value(s["total_elapsed"]),
            format_value(s["avg_elapsed"]),
            format_value(s["total_cpu"]),
            format_value(s["avg_cpu"]),
            format_value(s["total_user_cpu"]),
            format_value(s["total_sys_cpu"]),
            format_value(s["user_cpu_pct"]),
            format_value(s["sys_cpu_pct"]),
            format_value(s["total_maxrss"]),
            format_value(s["avg_maxrss"]),
            format_value(s["total_reqmem"]),
            format_value(s["avg_reqmem"]),
            format_value(s["total_allocmem"]),
            format_value(s["total_reqcpus"]),
            format_value(s["avg_reqcpus"]),
            format_value(s["total_nodes"]),
            format_value(s["total_wait"]),
            format_value(s["avg_wait"]),
            # Efficiency metrics
            format_value(s["weighted_cpu_eff_req"]),
            format_value(s["avg_cpu_eff_req"]),
            format_value(s["weighted_cpu_eff_alloc"]),
            format_value(s["avg_cpu_eff_alloc"]),
            format_value(s["weighted_mem_eff"]),
            format_value(s["avg_mem_eff"]),
            format_value(s.get("weighted_mem_eff_alloc")),
            format_value(s.get("avg_mem_eff_alloc")),
            format_value(s["weighted_time_eff"]),
            format_value(s["avg_time_eff"]),
        ]
        writer.writerow(row)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Export HPC aggregate statistics with efficiency metrics"
    )
    parser.add_argument("--config", default="config.yaml",
                        help="Path to config YAML with MySQL credentials")
    parser.add_argument("--ad_config", default="/etc/hpc_export_stats.yaml",
                        help="Path to AD config YAML file")
    parser.add_argument("--collate_by", action="append", required=True,
                        help="LDAP attr=label to collate by, or 'none' for global stats")
    parser.add_argument("--since", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--until", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", help="Output CSV file path")

    args = parser.parse_args()

    # Parse collate_by arguments
    collate_by = {}
    use_global = False
    for cb in args.collate_by:
        parsed = parse_collate_by_arg(cb)
        if parsed is None:
            use_global = True
        else:
            attr, label = parsed
            collate_by[attr] = label

    collate_by_keys = list(collate_by.keys())
    since_ts, until_ts = parse_date_range(args.since, args.until)

    # Setup LDAP if needed (connection is lazy — made on first lookup)
    ldap_client = None
    ad_config = None
    if collate_by_keys:
        ad_config = load_ad_config(args.ad_config)
        ldap_client = LdapClient(ad_config)

    # Connect to MySQL and discover step IDs
    print("Connecting to MySQL...", file=sys.stderr)
    conn, cursor = connect_mysql(args.config)
    special_steps = discover_special_steps(cursor)

    # Initialize stats
    stats_by_attr = {attr: {} for attr in collate_by_keys}
    global_stats = {} if use_global else None
    user_cache = {}
    ldap_errors = []

    # Process jobs
    print("Querying jobs...", file=sys.stderr)
    job_count = 0

    for row in fetch_job_data(cursor, since_ts, until_ts, special_steps):
        job_count += 1
        metrics = calculate_job_metrics(row)
        username = metrics["username"]

        # Process for each collate_by attribute
        if collate_by_keys and ldap_client and ad_config:
            for attr in collate_by_keys:
                value = get_user_attribute(ldap_client, ad_config, username, attr, user_cache, ldap_errors)
                if value not in stats_by_attr[attr]:
                    stats_by_attr[attr][value] = init_stats_dict()
                update_stats(metrics, stats_by_attr[attr], value)

        # Process for global stats
        if use_global:
            if "all" not in global_stats:
                global_stats["all"] = init_stats_dict()
            update_stats(metrics, global_stats, "all")

    cursor.close()
    conn.close()
    print(f"Processed {job_count} jobs", file=sys.stderr)

    # Report LDAP issues
    if ldap_errors:
        print(f"\nLDAP errors ({len(ldap_errors)} logged, showing first 3):", file=sys.stderr)
        for err in ldap_errors:
            print(f"  {err}", file=sys.stderr)

    unknown_count = sum(1 for k in user_cache if user_cache[k] == "unknown")
    total_cached = len(user_cache)
    print(f"LDAP lookups: {total_cached} unique users, {unknown_count} resolved to 'unknown'",
          file=sys.stderr)

    for attr in collate_by_keys:
        groups = list(stats_by_attr[attr].keys())
        print(f"Groups found for '{attr}': {groups}", file=sys.stderr)

    # Output all stats to single file
    outfile = None
    if args.output:
        output_dir = os.path.dirname(args.output)
        output_base = os.path.basename(args.output)
        output_path = os.path.join(output_dir, f"{args.since}_{args.until}_{output_base}")
        outfile = open(output_path, 'w')
        print(f"# date_range: {args.since} to {args.until}", file=outfile)
        print(f"Writing stats to {output_path}", file=sys.stderr)

    # Output faculty stats
    for attr, label in collate_by.items():
        print(f"\nStatistics collated by {label}:", file=sys.stderr)
        for stats_dict in stats_by_attr[attr].values():
            calculate_final_efficiencies(stats_dict)
        output_csv(stats_by_attr[attr], label, outfile)

    # Append global stats to same file
    if use_global:
        print("\nGlobal statistics:", file=sys.stderr)
        for stats_dict in global_stats.values():
            calculate_final_efficiencies(stats_dict)
        # Use same label as faculty stats, append without header
        label = list(collate_by.values())[0] if collate_by else "faculty"
        output_csv(global_stats, label, outfile, include_header=False)

    if outfile:
        outfile.close()


if __name__ == "__main__":
    main()
