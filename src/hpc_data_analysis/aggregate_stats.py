#!/usr/bin/env python3
"""
HPC Aggregate Statistics Export Tool

Produces aggregated statistics by faculty (or other LDAP attributes),
including resource efficiency metrics.

Usage:
    python3 hpc_stats.py --collate_by st=faculty --since 2025-01-01 --until 2025-02-01
    python3 hpc_stats.py --collate_by none --since 2025-01-01 --until 2025-02-01  # global stats only
    python3 hpc_stats.py --collate_by st=faculty --collate_by none --output stats.csv ...  # both
"""

import argparse
import re
import sys

from hpc_data_analysis.slurm_utils import (
    connect_mysql, discover_special_steps,
    fetch_job_data, calculate_job_metrics,
    LdapClient, load_ad_config, get_user_attribute,
    parse_date_range, format_value,
    FINISHED_STATES, SUCCESS_STATES, STATE_NAMES,
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
        # Counts
        "job_count": 0,
        "job_count_success": 0,
        "job_count_failed": 0,
        "count_by_state": {name: 0 for name in STATE_NAMES.values()},
        "exit_codes": {},

        # Resource totals
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

        # For efficiency calculations (all jobs)
        "sum_cpu_requested": 0,
        "sum_job_cpu_eff": 0,
        "sum_job_mem_eff": 0,
        "sum_job_time_eff": 0,
        "count_cpu_eff": 0,
        "count_mem_eff": 0,
        "count_time_eff": 0,

        # For efficiency calculations (successful jobs only)
        "success_total_elapsed": 0,
        "success_total_cpu": 0,
        "success_total_maxrss": 0,
        "success_total_reqmem": 0,
        "success_sum_cpu_requested": 0,
        "success_sum_job_cpu_eff": 0,
        "success_sum_job_mem_eff": 0,
        "success_sum_job_time_eff": 0,
        "success_count_cpu_eff": 0,
        "success_count_mem_eff": 0,
        "success_count_time_eff": 0,
        "success_total_timelimit": 0,
    }


# =============================================================================
# Job Processing
# =============================================================================

def update_stats(metrics, stats, collate_key):
    """Update stats dictionary with job metrics."""
    s = stats[collate_key]
    is_success = metrics["is_success"]

    s["job_count"] += 1
    s["job_count_success" if is_success else "job_count_failed"] += 1
    state_name = STATE_NAMES.get(metrics["state"])
    if state_name:
        s["count_by_state"][state_name] += 1

    ec = metrics["exit_code"]
    s["exit_codes"][ec] = s["exit_codes"].get(ec, 0) + 1

    s["total_elapsed"] += metrics["elapsed"]
    s["total_cpu"] += metrics["total_cpu"]
    s["total_user_cpu"] += metrics["total_user"]
    s["total_sys_cpu"] += metrics["total_sys"]
    s["total_maxrss"] += metrics["maxrss"]
    s["total_reqmem"] += metrics["reqmem"]
    s["total_reqcpus"] += metrics["req_cpus"]
    s["total_timelimit"] += metrics["timelimit_sec"]
    s["total_nodes"] += metrics["nodes_alloc"]
    s["total_wait"] += metrics["wait_time"]
    s["sum_cpu_requested"] += metrics["cpu_requested"]

    cpu_eff = metrics["cpu_eff"]
    mem_eff = metrics["mem_eff"]
    time_eff = metrics["time_eff"]

    if cpu_eff is not None:
        s["sum_job_cpu_eff"] += cpu_eff
        s["count_cpu_eff"] += 1
    if mem_eff is not None:
        s["sum_job_mem_eff"] += mem_eff
        s["count_mem_eff"] += 1
    if time_eff is not None:
        s["sum_job_time_eff"] += time_eff
        s["count_time_eff"] += 1

    if is_success:
        s["success_total_elapsed"] += metrics["elapsed"]
        s["success_total_cpu"] += metrics["total_cpu"]
        s["success_total_maxrss"] += metrics["maxrss"]
        s["success_total_reqmem"] += metrics["reqmem"]
        s["success_sum_cpu_requested"] += metrics["cpu_requested"]
        s["success_total_timelimit"] += metrics["timelimit_sec"]
        if cpu_eff is not None:
            s["success_sum_job_cpu_eff"] += cpu_eff
            s["success_count_cpu_eff"] += 1
        if mem_eff is not None:
            s["success_sum_job_mem_eff"] += mem_eff
            s["success_count_mem_eff"] += 1
        if time_eff is not None:
            s["success_sum_job_time_eff"] += time_eff
            s["success_count_time_eff"] += 1


def calculate_final_efficiencies(s):
    """Calculate final weighted and average efficiencies."""
    # All jobs
    s["weighted_cpu_eff"] = (s["total_cpu"] / s["sum_cpu_requested"] * 100) if s["sum_cpu_requested"] > 0 else None
    s["weighted_mem_eff"] = (s["total_maxrss"] / s["total_reqmem"] * 100) if s["total_reqmem"] > 0 else None
    s["weighted_time_eff"] = (s["total_elapsed"] / s["total_timelimit"] * 100) if s["total_timelimit"] > 0 else None
    s["avg_cpu_eff"] = (s["sum_job_cpu_eff"] / s["count_cpu_eff"]) if s["count_cpu_eff"] > 0 else None
    s["avg_mem_eff"] = (s["sum_job_mem_eff"] / s["count_mem_eff"]) if s["count_mem_eff"] > 0 else None
    s["avg_time_eff"] = (s["sum_job_time_eff"] / s["count_time_eff"]) if s["count_time_eff"] > 0 else None
    s["avg_wait"] = (s["total_wait"] / s["job_count"]) if s["job_count"] > 0 else None

    # Additional averages
    s["avg_elapsed"] = (s["total_elapsed"] / s["job_count"]) if s["job_count"] > 0 else None
    s["avg_cpu"] = (s["total_cpu"] / s["job_count"]) if s["job_count"] > 0 else None
    s["avg_reqcpus"] = (s["total_reqcpus"] / s["job_count"]) if s["job_count"] > 0 else None
    s["avg_reqmem"] = (s["total_reqmem"] / s["job_count"]) if s["job_count"] > 0 else None
    s["avg_maxrss"] = (s["total_maxrss"] / s["job_count"]) if s["job_count"] > 0 else None

    # User/System CPU ratio
    total_cpu_time = s["total_user_cpu"] + s["total_sys_cpu"]
    s["user_cpu_pct"] = (s["total_user_cpu"] / total_cpu_time * 100) if total_cpu_time > 0 else None
    s["sys_cpu_pct"] = (s["total_sys_cpu"] / total_cpu_time * 100) if total_cpu_time > 0 else None

    # Successful jobs only
    s["success_weighted_cpu_eff"] = (s["success_total_cpu"] / s["success_sum_cpu_requested"] * 100) if s["success_sum_cpu_requested"] > 0 else None
    s["success_weighted_mem_eff"] = (s["success_total_maxrss"] / s["success_total_reqmem"] * 100) if s["success_total_reqmem"] > 0 else None
    s["success_weighted_time_eff"] = (s["success_total_elapsed"] / s["success_total_timelimit"] * 100) if s["success_total_timelimit"] > 0 else None
    s["success_avg_cpu_eff"] = (s["success_sum_job_cpu_eff"] / s["success_count_cpu_eff"]) if s["success_count_cpu_eff"] > 0 else None
    s["success_avg_mem_eff"] = (s["success_sum_job_mem_eff"] / s["success_count_mem_eff"]) if s["success_count_mem_eff"] > 0 else None
    s["success_avg_time_eff"] = (s["success_sum_job_time_eff"] / s["success_count_time_eff"]) if s["success_count_time_eff"] > 0 else None


# =============================================================================
# Output
# =============================================================================

def output_csv(stats, collate_label, outfile=None, include_header=True):
    """Output statistics as CSV."""
    headers = [
        collate_label if collate_label else "global",
        "job_count", "job_count_success", "job_count_failed",
        "count_completed", "count_cancelled", "count_failed",
        "count_timeout", "count_node_fail", "count_preempted",
        "total_elapsed_sec", "avg_elapsed_sec",
        "total_cpu_sec", "avg_cpu_sec",
        "total_user_cpu_sec", "total_sys_cpu_sec", "user_cpu_pct", "sys_cpu_pct",
        "total_maxrss_bytes", "avg_maxrss_bytes",
        "total_reqmem_bytes", "avg_reqmem_bytes",
        "total_reqcpus", "avg_reqcpus",
        "total_nodes",
        "total_wait_sec", "avg_wait_sec",
        "weighted_cpu_eff_pct", "avg_cpu_eff_pct",
        "weighted_mem_eff_pct", "avg_mem_eff_pct",
        "weighted_time_eff_pct", "avg_time_eff_pct",
        "success_weighted_cpu_eff_pct", "success_avg_cpu_eff_pct",
        "success_weighted_mem_eff_pct", "success_avg_mem_eff_pct",
        "success_weighted_time_eff_pct", "success_avg_time_eff_pct",
        "exit_codes",
    ]

    out = outfile if outfile else sys.stdout
    if include_header:
        print(",".join(headers), file=out)

    for key, s in sorted(stats.items(), key=lambda x: -x[1]["job_count"]):
        exit_codes_str = ";".join(f"{k}:{v}" for k, v in sorted(s["exit_codes"].items()))
        row = [
            f'"{key}"',
            format_value(s["job_count"]),
            format_value(s["job_count_success"]),
            format_value(s["job_count_failed"]),
            format_value(s["count_by_state"]["completed"]),
            format_value(s["count_by_state"]["cancelled"]),
            format_value(s["count_by_state"]["failed"]),
            format_value(s["count_by_state"]["timeout"]),
            format_value(s["count_by_state"]["node_fail"]),
            format_value(s["count_by_state"]["preempted"]),
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
            format_value(s["total_reqcpus"]),
            format_value(s["avg_reqcpus"]),
            format_value(s["total_nodes"]),
            format_value(s["total_wait"]),
            format_value(s["avg_wait"]),
            format_value(s["weighted_cpu_eff"]),
            format_value(s["avg_cpu_eff"]),
            format_value(s["weighted_mem_eff"]),
            format_value(s["avg_mem_eff"]),
            format_value(s["weighted_time_eff"]),
            format_value(s["avg_time_eff"]),
            format_value(s["success_weighted_cpu_eff"]),
            format_value(s["success_avg_cpu_eff"]),
            format_value(s["success_weighted_mem_eff"]),
            format_value(s["success_avg_mem_eff"]),
            format_value(s["success_weighted_time_eff"]),
            format_value(s["success_avg_time_eff"]),
            f'"{exit_codes_str}"',
        ]
        print(",".join(row), file=out)


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
        state = row[3]  # state is at index 3 (after job_db_inx, id_job, user)

        if state not in FINISHED_STATES:
            continue

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

    # Output faculty stats
    outfile = None
    if args.output:
        outfile = open(args.output, 'w')
        print(f"Writing faculty stats to {args.output}", file=sys.stderr)

    for attr, label in collate_by.items():
        print(f"\nStatistics collated by {label}:", file=sys.stderr)
        for stats_dict in stats_by_attr[attr].values():
            calculate_final_efficiencies(stats_dict)
        output_csv(stats_by_attr[attr], label, outfile)

    if outfile:
        outfile.close()

    # Output global stats to separate file
    if use_global:
        print("\nGlobal statistics:", file=sys.stderr)
        for stats_dict in global_stats.values():
            calculate_final_efficiencies(stats_dict)

        if args.output:
            global_output = args.output.replace('.csv', '_global.csv')
            if global_output == args.output:
                global_output = args.output + '_global'
            with open(global_output, 'w') as f:
                output_csv(global_stats, None, f)
            print(f"Global stats saved to {global_output}", file=sys.stderr)
        else:
            output_csv(global_stats, None)


if __name__ == "__main__":
    main()
