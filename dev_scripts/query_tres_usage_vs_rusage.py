#!/usr/bin/env python3
"""
Diagnostic script to investigate the relationship between:
- user_sec/sys_sec columns (from getrusage/wait4)
- tres_usage_in_max field (from cgroup accounting)

Goal: Understand units and when each is populated.
"""

import yaml
import mysql.connector

TRES_CPU_ID = 1


def connect_mysql(config_path="config.yaml"):
    """Connect to MySQL using config file."""
    with open(config_path) as f:
        config = yaml.safe_load(f)
    mysql_cfg = config.get("mysql", config)
    conn = mysql.connector.connect(
        host=mysql_cfg["host"],
        port=mysql_cfg.get("port", 3306),
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"],
    )
    return conn, conn.cursor()


def parse_tres_value(tres_str, tres_id):
    """Parse a TRES string like '1=8,2=8000' and extract value for given ID."""
    if not tres_str:
        return 0
    for part in tres_str.split(","):
        if "=" in part:
            tid, val = part.split("=", 1)
            if int(tid) == tres_id:
                return int(val)
    return 0

def parse_tres_cpu(tres_str):
    """Extract CPU value from tres_usage_in_max string."""
    if not tres_str:
        return None
    return parse_tres_value(tres_str, TRES_CPU_ID)

def main():
    conn, cursor = connect_mysql("config.yaml")

    print("=" * 80)
    print("PART 1: Compare user_sec/sys_sec vs tres_usage_in_max for BATCH steps")
    print("        (where user_sec > 0, to see the relationship)")
    print("=" * 80)

    query = """
        SELECT
            s.job_db_inx,
            j.id_job,
            j.nodes_alloc,
            s.id_step,
            s.step_name,
            s.user_sec,
            s.sys_sec,
            s.user_usec,
            s.sys_usec,
            s.tres_usage_in_max
        FROM create_step_table s
        JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
        WHERE s.id_step = -5  -- batch step
          AND s.user_sec > 100  -- substantial CPU time
          AND s.tres_usage_in_max IS NOT NULL
          AND s.tres_usage_in_max != ''
        ORDER BY RAND()
        LIMIT 20
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"\n{'job_id':>12} {'nodes':>6} {'user_sec':>12} {'sys_sec':>10} "
          f"{'total_sec':>12} {'tres_cpu':>14} {'ratio':>10}")
    print("-" * 90)

    ratios = []
    for row in rows:
        job_db_inx, id_job, nodes, id_step, step_name, user_sec, sys_sec, user_usec, sys_usec, tres_usage = row
        total_sec = (user_sec or 0) + (sys_sec or 0) + ((user_usec or 0) + (sys_usec or 0)) / 1_000_000
        tres_cpu = parse_tres_cpu(tres_usage)

        if tres_cpu and total_sec > 0:
            ratio = tres_cpu / total_sec
            ratios.append(ratio)
            print(f"{id_job:>12} {nodes:>6} {user_sec:>12} {sys_sec:>10} "
                  f"{total_sec:>12.2f} {tres_cpu:>14} {ratio:>10.2f}")

    if ratios:
        avg_ratio = sum(ratios) / len(ratios)
        print(f"\nAverage ratio (tres_cpu / total_sec): {avg_ratio:.2f}")
        print(f"If ratio ~1: tres_usage_in_max is in CPU-seconds")
        print(f"If ratio ~1000: tres_usage_in_max is in CPU-milliseconds")

    print("\n" + "=" * 80)
    print("PART 2: Steps where user_sec=0 but tres_usage_in_max has CPU data")
    print("        (these are likely distributed/remote steps)")
    print("=" * 80)

    query = """
        SELECT
            s.job_db_inx,
            j.id_job,
            j.nodes_alloc,
            s.id_step,
            s.step_name,
            s.user_sec,
            s.sys_sec,
            s.tres_usage_in_max
        FROM create_step_table s
        JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
        WHERE s.id_step >= 0  -- regular steps only
          AND (s.user_sec = 0 OR s.user_sec IS NULL)
          AND (s.sys_sec = 0 OR s.sys_sec IS NULL)
          AND s.tres_usage_in_max IS NOT NULL
          AND s.tres_usage_in_max != ''
          AND s.tres_usage_in_max LIKE '1=%'  -- has CPU data
        ORDER BY RAND()
        LIMIT 20
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"\n{'job_id':>12} {'nodes':>6} {'id_step':>8} {'step_name':>25} "
          f"{'user_sec':>10} {'tres_cpu':>14}")
    print("-" * 90)

    for row in rows:
        job_db_inx, id_job, nodes, id_step, step_name, user_sec, sys_sec, tres_usage = row
        tres_cpu = parse_tres_cpu(tres_usage)
        print(f"{id_job:>12} {nodes:>6} {id_step:>8} {step_name:>25} "
              f"{user_sec or 0:>10} {tres_cpu or 0:>14}")

    print("\n" + "=" * 80)
    print("PART 3: Count of steps by accounting method availability")
    print("=" * 80)

    query = """
        SELECT
            CASE
                WHEN id_step < 0 THEN 'special'
                ELSE 'regular'
            END as step_type,
            CASE
                WHEN (user_sec > 0 OR sys_sec > 0) AND tres_usage_in_max LIKE '1=%' THEN 'both'
                WHEN (user_sec > 0 OR sys_sec > 0) THEN 'rusage_only'
                WHEN tres_usage_in_max LIKE '1=%' THEN 'tres_only'
                ELSE 'neither'
            END as data_source,
            COUNT(*) as count
        FROM create_step_table
        GROUP BY step_type, data_source
        ORDER BY step_type, data_source
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"\n{'step_type':>10} {'data_source':>15} {'count':>15}")
    print("-" * 45)
    for row in rows:
        step_type, data_source, count = row
        print(f"{step_type:>10} {data_source:>15} {count:>15,}")

    print("\n" + "=" * 80)
    print("PART 4: Single-node vs multi-node job comparison")
    print("        (do single-node jobs have rusage data in regular steps?)")
    print("=" * 80)

    # Single-node jobs with regular steps
    query = """
        SELECT
            j.nodes_alloc,
            s.id_step,
            s.step_name,
            s.user_sec,
            s.sys_sec,
            s.tres_usage_in_max
        FROM create_step_table s
        JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
        WHERE j.nodes_alloc = 1
          AND s.id_step >= 0
          AND s.tres_usage_in_max LIKE '1=%'
        ORDER BY RAND()
        LIMIT 10
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"\nSingle-node jobs (nodes=1) with regular steps:")
    print(f"{'nodes':>6} {'id_step':>8} {'step_name':>20} {'user_sec':>10} {'sys_sec':>10} {'tres_cpu':>14}")
    print("-" * 75)
    for row in rows:
        nodes, id_step, step_name, user_sec, sys_sec, tres_usage = row
        tres_cpu = parse_tres_cpu(tres_usage)
        print(f"{nodes:>6} {id_step:>8} {step_name:>20} {user_sec or 0:>10} {sys_sec or 0:>10} {tres_cpu or 0:>14}")

    # Multi-node jobs with regular steps
    query = """
        SELECT
            j.nodes_alloc,
            s.id_step,
            s.step_name,
            s.user_sec,
            s.sys_sec,
            s.tres_usage_in_max
        FROM create_step_table s
        JOIN create_job_table j ON s.job_db_inx = j.job_db_inx
        WHERE j.nodes_alloc > 1
          AND s.id_step >= 0
          AND s.tres_usage_in_max LIKE '1=%'
        ORDER BY RAND()
        LIMIT 10
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"\nMulti-node jobs (nodes>1) with regular steps:")
    print(f"{'nodes':>6} {'id_step':>8} {'step_name':>20} {'user_sec':>10} {'sys_sec':>10} {'tres_cpu':>14}")
    print("-" * 75)
    for row in rows:
        nodes, id_step, step_name, user_sec, sys_sec, tres_usage = row
        tres_cpu = parse_tres_cpu(tres_usage)
        print(f"{nodes:>6} {id_step:>8} {step_name:>20} {user_sec or 0:>10} {sys_sec or 0:>10} {tres_cpu or 0:>14}")

    print("\n" + "=" * 80)
    print("PART 5: For jobs with BOTH batch and regular steps, compare totals")
    print("        (is batch CPU a duplicate of regular steps, or additional?)")
    print("=" * 80)

    query = """
        SELECT
            j.id_job,
            j.nodes_alloc,
            j.time_end - j.time_start as elapsed,
            j.cpus_req,
            -- Batch step data
            MAX(CASE WHEN s.id_step = -5 THEN s.user_sec + s.sys_sec END) as batch_rusage,
            MAX(CASE WHEN s.id_step = -5 THEN s.tres_usage_in_max END) as batch_tres,
            -- Regular steps data (summed)
            SUM(CASE WHEN s.id_step >= 0 THEN s.user_sec + s.sys_sec ELSE 0 END) as regular_rusage,
            GROUP_CONCAT(CASE WHEN s.id_step >= 0 THEN s.tres_usage_in_max END) as regular_tres_list,
            COUNT(CASE WHEN s.id_step >= 0 THEN 1 END) as regular_step_count
        FROM create_job_table j
        JOIN create_step_table s ON j.job_db_inx = s.job_db_inx
        WHERE j.time_end > j.time_start
          AND j.cpus_req > 1
        GROUP BY j.job_db_inx
        HAVING regular_step_count > 0 AND batch_rusage > 100
        ORDER BY RAND()
        LIMIT 10
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"\n{'job_id':>12} {'nodes':>6} {'elapsed':>8} {'cpus':>6} "
          f"{'batch_ru':>12} {'regular_ru':>12} {'batch_tres_cpu':>15}")
    print("-" * 90)

    for row in rows:
        id_job, nodes, elapsed, cpus, batch_rusage, batch_tres, regular_rusage, regular_tres_list, step_count = row
        batch_tres_cpu = parse_tres_cpu(batch_tres) if batch_tres else 0

        # Sum tres_cpu from regular steps
        regular_tres_cpu = 0
        if regular_tres_list:
            for tres_str in regular_tres_list.split(',1='):
                if tres_str:
                    cpu_val = parse_tres_cpu('1=' + tres_str if not tres_str.startswith('1=') else tres_str)
                    if cpu_val:
                        regular_tres_cpu += cpu_val

        print(f"{id_job:>12} {nodes:>6} {elapsed:>8} {cpus:>6} "
              f"{batch_rusage or 0:>12.0f} {regular_rusage or 0:>12.0f} {batch_tres_cpu:>15}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
Key questions this script answers:
1. What is the unit of tres_usage_in_max CPU values? (ratio to user_sec+sys_sec)
2. Are regular steps missing rusage data but have tres data?
3. Is this pattern different for single-node vs multi-node jobs?
4. Is batch step CPU additional to regular steps, or a duplicate?
""")

if __name__ == "__main__":
    main()
