import mysql.connector
import ldap
from ldap import filter as ldap_filter
import yaml
import sys

# Config
START_DATE = '2025-01-01'
END_DATE = '2025-02-01'

# Load LDAP config
with open("/etc/hpc_export_stats.yaml", "r") as f:
    ad_config = yaml.safe_load(f)

# Connect to MySQL
print("Connecting to MySQL...", file=sys.stderr)
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

# Connect to LDAP
print("Connecting to LDAP...", file=sys.stderr)
ldap.set_option(ldap.OPT_NETWORK_TIMEOUT, 10)
ldap.set_option(ldap.OPT_X_TLS_CACERTFILE, ad_config["ldap_ca_file"])
ldap_conn = ldap.initialize(f"ldaps://{ad_config['ldap_host']}")
ldap_conn.simple_bind_s(ad_config["ldap_binddn"], ad_config["ldap_password"])

# Cache for LDAP lookups
faculty_cache = {}

def get_faculty(username):
    if username in faculty_cache:
        return faculty_cache[username]
    try:
        result = ldap_conn.result(
            ldap_conn.search(
                ad_config["ldap_users_ou"],
                ldap.SCOPE_SUBTREE,
                f"(&(objectClass=user)(sAMAccountName={ldap_filter.escape_filter_chars(username)}))",
                ["st"]
            )
        )
        if len(result[1]) == 0 or "st" not in result[1][0][1]:
            faculty_cache[username] = "unknown"
        else:
            faculty_cache[username] = result[1][0][1]["st"][0].decode("utf-8")
    except Exception as e:
        faculty_cache[username] = "unknown"
    return faculty_cache[username]

# Get jobs with usernames - fix the elapsed time calculation
print("Querying jobs...", file=sys.stderr)
cursor.execute(f"""
    SELECT a.user, j.cpus_req, j.mem_req, 
           j.time_start, j.time_end,
           j.nodes_alloc, j.state
    FROM create_job_table j
    JOIN create_assoc_table a ON j.id_assoc = a.id_assoc
    WHERE j.time_submit >= UNIX_TIMESTAMP('{START_DATE}')
      AND j.time_submit < UNIX_TIMESTAMP('{END_DATE}')
      AND j.time_start > 0
      AND j.time_end > 0
      AND j.time_end >= j.time_start
""")

# Aggregate by faculty
stats = {}
total_jobs = 0
users_seen = set()

print("Processing jobs...", file=sys.stderr)
for row in cursor:
    username, cpus, mem, time_start, time_end, nodes, state = row
    total_jobs += 1
    users_seen.add(username)

    elapsed = time_end - time_start

    faculty = get_faculty(username)

    if faculty not in stats:
        stats[faculty] = {
            "job_count": 0,
            "total_elapsed": 0,
            "total_cpus": 0,
            "total_mem": 0,
            "total_nodes": 0,
        }

    stats[faculty]["job_count"] += 1
    stats[faculty]["total_elapsed"] += elapsed if elapsed else 0
    stats[faculty]["total_cpus"] += cpus if cpus else 0
    stats[faculty]["total_mem"] += mem if mem else 0
    stats[faculty]["total_nodes"] += nodes if nodes else 0

cursor.close()
conn.close()

print(f"Processed {total_jobs} jobs from {len(users_seen)} users", file=sys.stderr)

# Output CSV
print("faculty,job_count,total_elapsed_seconds,total_cpus_requested,total_mem_bytes,total_nodes")
for faculty, s in sorted(stats.items(), key=lambda x: -x[1]["job_count"]):
    print(f'"{faculty}",{s["job_count"]},{s["total_elapsed"]},{s["total_cpus"]},{s["total_mem"]},{s["total_nodes"]}')
                                                                                                                                                                      108,1         Bot




