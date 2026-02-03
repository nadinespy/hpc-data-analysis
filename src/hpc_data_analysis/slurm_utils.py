#!/usr/bin/env python3
"""
Shared utilities for HPC statistics tools.

Contains common functions for:
- Configuration loading
- MySQL database connection
- LDAP user lookups
- TRES string parsing
- Slurm job state constants
"""

import sys
import yaml
import mysql.connector
import ldap
from ldap import filter as ldap_filter


# =============================================================================
# Constants
# =============================================================================

# TRES IDs (from tres_table)
TRES_CPU_ID = 1
TRES_MEM_ID = 2
TRES_ENERGY_ID = 3

# Slurm job states (numeric codes)
# See: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
JOB_STATE_COMPLETED = 3
JOB_STATE_CANCELLED = 4
JOB_STATE_FAILED = 5
JOB_STATE_TIMEOUT = 6
JOB_STATE_NODE_FAIL = 7
JOB_STATE_PREEMPTED = 8

# States considered "finished" (job ran and ended)
FINISHED_STATES = {
    JOB_STATE_COMPLETED, JOB_STATE_FAILED, JOB_STATE_TIMEOUT,
    JOB_STATE_CANCELLED, JOB_STATE_NODE_FAIL, JOB_STATE_PREEMPTED
}

# States considered "successful"
SUCCESS_STATES = {JOB_STATE_COMPLETED}

# Human-readable state names
STATE_NAMES = {
    JOB_STATE_COMPLETED: "completed",
    JOB_STATE_CANCELLED: "cancelled",
    JOB_STATE_FAILED: "failed",
    JOB_STATE_TIMEOUT: "timeout",
    JOB_STATE_NODE_FAIL: "node_fail",
    JOB_STATE_PREEMPTED: "preempted",
}


# =============================================================================
# Configuration
# =============================================================================

def load_config(config_path):
    """
    Load configuration from YAML file.

    Expected format:
        mysql:
            host: "hostname"
            user: "username"
            password: "password"
            database: "dbname"

    Returns dict with 'mysql' key containing connection params.
    """
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        sys.exit(f"Error: Config file not found: {config_path}\n"
                 f"Create a config.yaml with MySQL credentials.")
    except Exception as e:
        sys.exit(f"Error loading config file: {e}")


def get_mysql_config(config):
    """Extract MySQL connection parameters from config dict."""
    mysql_conf = config.get("mysql", {})
    return {
        "host": mysql_conf.get("host"),
        "user": mysql_conf.get("user"),
        "password": mysql_conf.get("password"),
        "database": mysql_conf.get("database"),
    }


# =============================================================================
# TRES Parsing
# =============================================================================

def parse_tres_value(tres_string, tres_id):
    """
    Parse a TRES string like "1=500,2=12345,7=999" and extract value for given ID.

    Args:
        tres_string: TRES format string from Slurm DB
        tres_id: The TRES ID to extract (e.g., 2 for memory)

    Returns:
        Integer value for the TRES ID, or 0 if not found/parse error.
    """
    if not tres_string:
        return 0
    try:
        for pair in tres_string.split(','):
            if '=' in pair:
                tid, value = pair.split('=', 1)
                if int(tid) == tres_id:
                    return int(value)
    except (ValueError, AttributeError):
        pass
    return 0


# =============================================================================
# Database Connection
# =============================================================================

def connect_mysql(config_path="config.yaml"):
    """
    Connect to MySQL database using config file.

    Returns:
        tuple: (connection, cursor)
    """
    config = load_config(config_path)
    mysql_config = get_mysql_config(config)

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        sys.exit(f"Error connecting to MySQL: {e}")


def discover_special_steps(cursor):
    """Query the step table for special (non-regular) step IDs.

    Special steps (batch, interactive, extern, etc.) have negative id_step
    values. These must be excluded from CPU time summation to avoid
    double-counting with regular srun steps.

    Returns:
        dict mapping step_name to id_step, e.g. {'batch': -5, 'interactive': -6}
    """
    cursor.execute("""
        SELECT DISTINCT id_step, step_name
        FROM create_step_table
        WHERE id_step < 0
        ORDER BY id_step
    """)
    steps = {row[1]: row[0] for row in cursor.fetchall()}

    print(f"Special step IDs: {steps}", file=sys.stderr)
    if 'batch' not in steps:
        print("WARNING: No 'batch' step found in step table", file=sys.stderr)

    return steps


def fetch_job_data(cursor, since_ts, until_ts, special_steps):
    """Fetch job data from MySQL with step aggregation.

    CPU time: sums regular steps only; falls back to batch step for jobs
    without srun steps (avoids double-counting batch + srun steps).
    Memory: extracts numeric memory value from TRES string per step,
    then takes the numeric MAX (not string MAX).

    Args:
        cursor: MySQL cursor
        since_ts: Start timestamp (Unix)
        until_ts: End timestamp (Unix)
        special_steps: dict from discover_special_steps(), e.g. {'batch': -5}

    Returns:
        cursor with query results. Each row contains:
        (job_db_inx, id_job, username, state, exit_code, time_submit,
         time_start, time_end, cpus_req, tres_req, timelimit, nodes_alloc,
         total_user_sec, total_sys_sec, total_user_usec, total_sys_usec,
         max_mem_bytes)
    """
    # Build exclusion list from all discovered special steps
    exclude_ids = ", ".join(str(sid) for sid in special_steps.values())
    batch_id = special_steps.get("batch")

    query = f"""
        SELECT
            j.job_db_inx,
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
                                THEN s.user_usec ELSE 0 END), 0),
                MAX(CASE WHEN s.id_step = {batch_id} THEN s.user_usec END),
                0
            ) AS total_user_usec,
            COALESCE(
                NULLIF(SUM(CASE WHEN s.id_step NOT IN ({exclude_ids})
                                THEN s.sys_usec ELSE 0 END), 0),
                MAX(CASE WHEN s.id_step = {batch_id} THEN s.sys_usec END),
                0
            ) AS total_sys_usec,
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


def calculate_job_metrics(row):
    """Calculate common job metrics from a database row.

    Args:
        row: Database row from fetch_job_data query

    Returns:
        dict with calculated metrics:
        - id_job, username, state, exit_code
        - time_submit, time_start, time_end
        - elapsed, wait_time, timelimit_sec
        - total_user, total_sys, total_cpu (all in seconds, including usec)
        - req_cpus, maxrss, reqmem (in bytes)
        - cpu_eff, mem_eff, time_eff (percentages or None)
        - cpu_requested (elapsed * req_cpus)
        - user_cpu_pct (percentage or None)
        - nodes_alloc
        - is_success (bool)
    """
    (job_db_inx, id_job, username, state, exit_code, time_submit, time_start,
     time_end, cpus_req_col, tres_req, timelimit, nodes_alloc,
     total_user_sec, total_sys_sec, total_user_usec, total_sys_usec,
     max_mem_bytes) = row

    # Convert types
    time_submit = float(time_submit) if time_submit else 0
    time_start = float(time_start) if time_start else 0
    time_end = float(time_end) if time_end else 0
    total_user_sec = float(total_user_sec) if total_user_sec else 0
    total_sys_sec = float(total_sys_sec) if total_sys_sec else 0
    total_user_usec = float(total_user_usec) if total_user_usec else 0
    total_sys_usec = float(total_sys_usec) if total_sys_usec else 0
    nodes_alloc = int(nodes_alloc) if nodes_alloc else 0
    timelimit = timelimit or 0

    # Derived values - include microseconds in user/sys totals
    total_user = total_user_sec + (total_user_usec / 1_000_000)
    total_sys = total_sys_sec + (total_sys_usec / 1_000_000)
    elapsed = time_end - time_start if time_end and time_start else 0
    wait_time = time_start - time_submit if time_start and time_submit else 0
    total_cpu = total_user + total_sys
    timelimit_sec = timelimit * 60

    # CPU values - use tres_req for consistency
    req_cpus = parse_tres_value(tres_req, TRES_CPU_ID)
    if req_cpus == 0:
        req_cpus = cpus_req_col or 0  # fallback to direct column

    # Memory values
    maxrss = int(max_mem_bytes) if max_mem_bytes else 0
    reqmem_mb = parse_tres_value(tres_req, TRES_MEM_ID)
    reqmem = reqmem_mb * 1024 * 1024

    # Per-job efficiencies
    cpu_requested = elapsed * req_cpus
    cpu_eff = (total_cpu / cpu_requested * 100) if cpu_requested > 0 else None
    mem_eff = (maxrss / reqmem * 100) if reqmem > 0 else None
    time_eff = (elapsed / timelimit_sec * 100) if timelimit_sec > 0 else None

    # User vs system CPU ratio (now includes microseconds)
    user_cpu_pct = (total_user / total_cpu * 100) if total_cpu > 0 else None

    is_success = state in SUCCESS_STATES

    return {
        "job_db_inx": job_db_inx,
        "id_job": id_job,
        "username": username,
        "state": state,
        "exit_code": exit_code or 0,
        "time_submit": time_submit,
        "time_start": time_start,
        "time_end": time_end,
        "elapsed": elapsed,
        "wait_time": wait_time,
        "timelimit_sec": timelimit_sec,
        "total_user": total_user,
        "total_sys": total_sys,
        "total_cpu": total_cpu,
        "req_cpus": req_cpus,
        "maxrss": maxrss,
        "reqmem": reqmem,
        "cpu_requested": cpu_requested,
        "cpu_eff": cpu_eff,
        "mem_eff": mem_eff,
        "time_eff": time_eff,
        "user_cpu_pct": user_cpu_pct,
        "nodes_alloc": nodes_alloc,
        "is_success": is_success,
    }


# =============================================================================
# LDAP Functions
# =============================================================================

def load_ad_config(ad_config_path="/etc/hpc_export_stats.yaml"):
    """Load AD/LDAP configuration from YAML file."""
    try:
        with open(ad_config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        sys.exit(f"Error loading AD config: {e}")


class LdapClient:
    """Lazy-connecting, auto-reconnecting LDAP client for Active Directory."""

    def __init__(self, ad_config):
        self.ad_config = ad_config
        self._conn = None

    def _connect(self):
        ldap.set_option(ldap.OPT_NETWORK_TIMEOUT, 10)
        ldap.set_option(ldap.OPT_X_TLS_CACERTFILE, self.ad_config["ldap_ca_file"])
        conn = ldap.initialize(f"ldaps://{self.ad_config['ldap_host']}")
        conn.set_option(ldap.OPT_REFERRALS, 0)
        conn.simple_bind_s(self.ad_config["ldap_binddn"], self.ad_config["ldap_password"])
        self._conn = conn
        print("LDAP connected.", file=sys.stderr)

    def search(self, base, scope, filter_str, attrs):
        """Search with lazy connect and auto-reconnect on SERVER_DOWN."""
        if self._conn is None:
            self._connect()

        for attempt in range(2):
            try:
                return self._conn.search_s(base, scope, filter_str, attrs)
            except ldap.SERVER_DOWN:
                if attempt == 0:
                    print("LDAP connection lost, reconnecting...", file=sys.stderr)
                    self._connect()
                else:
                    raise


def get_user_attribute(ldap_client, ad_config, username, attribute, cache, error_log):
    """
    Look up a single LDAP attribute for a user.

    Args:
        ldap_client: LdapClient instance (lazy-connecting, auto-reconnecting)
        ad_config: AD configuration dict
        username: Username to look up
        attribute: LDAP attribute name (e.g., 'st' for faculty)
        cache: Dict to cache lookups
        error_log: List to append errors (max 3 logged)

    Returns:
        Attribute value string, or "unknown" if not found.
    """
    cache_key = (username, attribute)
    if cache_key in cache:
        return cache[cache_key]

    try:
        results = ldap_client.search(
            ad_config["ldap_users_ou"],
            ldap.SCOPE_SUBTREE,
            f"(&(objectClass=user)(sAMAccountName={ldap_filter.escape_filter_chars(username)}))",
            [attribute]
        )
        # Filter out referral entries (dn is None for referrals)
        entries = [(dn, attrs) for dn, attrs in results if dn is not None]

        # Log raw result details for the first few lookups
        if len(cache) < 3:
            print(f"  LDAP debug for '{username}': "
                  f"raw entries={len(results)}, real entries={len(entries)}",
                  file=sys.stderr)
            if entries:
                print(f"    DN: {entries[0][0]}", file=sys.stderr)
                print(f"    Attrs: {entries[0][1]}", file=sys.stderr)

        if len(entries) == 0:
            cache[cache_key] = "unknown"
            if len(error_log) < 3:
                error_log.append(f"User {username}: not found in LDAP "
                                 f"(raw results: {len(results)}, referrals filtered)")
        else:
            entry = entries[0][1]
            cache[cache_key] = entry.get(attribute, [b"unknown"])[0].decode("utf-8")
    except Exception as e:
        cache[cache_key] = "unknown"
        if len(error_log) < 3:
            error_log.append(f"User {username}: LDAP error - {type(e).__name__}: {e}")

    return cache[cache_key]


# =============================================================================
# Utility Functions
# =============================================================================

def format_value(val):
    """Format a value for CSV output."""
    if val is None:
        return "NULL"
    elif isinstance(val, float):
        return f"{val:.2f}"
    else:
        return str(val)


def parse_date_range(since_str, until_str):
    """
    Parse date strings to Unix timestamps.

    Args:
        since_str: Start date as YYYY-MM-DD
        until_str: End date as YYYY-MM-DD

    Returns:
        tuple: (since_timestamp, until_timestamp)
    """
    from datetime import datetime

    try:
        since_dt = datetime.strptime(since_str, "%Y-%m-%d")
        until_dt = datetime.strptime(until_str, "%Y-%m-%d")
        since_ts = int(since_dt.timestamp())
        until_ts = int(until_dt.timestamp())
    except ValueError as e:
        sys.exit(f"Error parsing dates: {e}")

    if since_ts >= until_ts:
        sys.exit("Error: 'since' date must be before 'until' date")

    return since_ts, until_ts
