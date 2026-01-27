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
