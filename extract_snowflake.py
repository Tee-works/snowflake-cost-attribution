"""Extract SNOWFLAKE.ACCOUNT_USAGE into the working DuckDB database, in the same shape the models expect.

Connection: a named connection in ~/.snowflake/connections.toml (default name "finops", override with
SNOWFLAKE_CONNECTION). The role needs IMPORTED PRIVILEGES on the SNOWFLAKE database. No credentials live in this repo.

Choices that matter for cost attribution:
  - CREDITS_USED_COMPUTE, not CREDITS_USED: cloud services credits are billed separately and are not warehouse time.
  - Only whole hours that ended at least 3 hours ago, because ACCOUNT_USAGE can lag by up to about 3 hours.
  - A query's warehouse time is its EXECUTION_TIME, ending at END_TIME. Compilation (cloud services) and time spent
    queued or waiting for the warehouse to resume are not warehouse time, so start_time here is execution start.
    Failed queries still used the warehouse and are kept; result-cache and metadata queries (execution 0) are not.
  - Warehouse sizes are normalised (X-Small -> XSMALL) to match the credit rates in the models. QUERY_HISTORY leaves
    the size empty on some queries (for example while the warehouse resumes); the models take the known size.
  - Snowflake-managed serverless pools (COMPUTE_SERVICE_WH_*, user SYSTEM) show up in QUERY_HISTORY but are billed
    through serverless metering, not WAREHOUSE_METERING_HISTORY, so they are out of scope here.
  - Metered hours with zero compute credits are kept: queries in them are costed at zero instead of going missing.
    DDL and metadata statements (CREATE USER, GRANT, SHOW) report the session's warehouse but run in cloud services.
  - Users with no team in config/user_team.csv are kept and shown as "unmapped", never dropped.
"""
from __future__ import annotations

import csv
import os
from pathlib import Path

import duckdb

LOOKBACK_DAYS = int(os.environ.get("FINOPS_LOOKBACK_DAYS", "28"))
LAG_HOURS = int(os.environ.get("FINOPS_LAG_HOURS", "3"))
TEAM_MAP = Path(__file__).parent / "config" / "user_team.csv"

QUERY_HISTORY = f"""
select query_id, user_name, role_name, warehouse_name,
       upper(replace(warehouse_size, '-', '')) as warehouse_size,
       dateadd(millisecond, -execution_time, convert_timezone('UTC', end_time))::timestamp_ntz as start_time,
       convert_timezone('UTC', end_time)::timestamp_ntz as end_time,
       execution_time, query_tag, bytes_scanned
from snowflake.account_usage.query_history
where warehouse_name is not null
  and warehouse_name not like 'COMPUTE_SERVICE_WH_%'
  and execution_time > 0
  and execution_status in ('SUCCESS', 'FAIL')
  and start_time >= dateadd(day, -{LOOKBACK_DAYS}, date_trunc(hour, current_timestamp()))
  and end_time < dateadd(hour, -{LAG_HOURS}, date_trunc(hour, current_timestamp()))
"""

METERING = f"""
select warehouse_name,
       convert_timezone('UTC', start_time)::timestamp_ntz as start_time,
       convert_timezone('UTC', end_time)::timestamp_ntz as end_time,
       credits_used_compute as credits_used
from snowflake.account_usage.warehouse_metering_history
where warehouse_name != 'CLOUD_SERVICES_ONLY'
  and start_time >= dateadd(day, -{LOOKBACK_DAYS}, date_trunc(hour, current_timestamp()))
  and end_time <= dateadd(hour, -{LAG_HOURS}, date_trunc(hour, current_timestamp()))
"""


def _connect():
    import snowflake.connector
    return snowflake.connector.connect(connection_name=os.environ.get("SNOWFLAKE_CONNECTION", "finops"))


def extract(db: str) -> dict:
    sf = _connect()
    try:
        cur = sf.cursor()
        queries = cur.execute(QUERY_HISTORY).fetchall()
        metering = cur.execute(METERING).fetchall()
    finally:
        sf.close()

    teams = {}
    if TEAM_MAP.exists():
        with TEAM_MAP.open() as f:
            teams = {r["user_name"].upper(): r["team"] for r in csv.DictReader(f)}
    users = sorted({q[1] for q in queries})

    con = duckdb.connect(db)
    con.execute("""create or replace table query_history(query_id varchar, user_name varchar, role_name varchar,
        warehouse_name varchar, warehouse_size varchar, start_time timestamp, end_time timestamp,
        execution_time bigint, query_tag varchar, bytes_scanned bigint)""")
    if queries:
        con.executemany("insert into query_history values (?,?,?,?,?,?,?,?,?,?)", queries)
    con.execute("create or replace table warehouse_metering_history(warehouse_name varchar, start_time timestamp, "
                "end_time timestamp, credits_used double)")
    if metering:
        con.executemany("insert into warehouse_metering_history values (?,?,?,?)",
                        [(w, s, e, float(c)) for w, s, e, c in metering])
    con.execute("create or replace table user_team(user_name varchar, team varchar)")
    if users:
        con.executemany("insert into user_team values (?,?)", [(u, teams.get(u.upper(), "unmapped")) for u in users])
    con.close()
    return {"queries": len(queries), "metered_hours": len(metering),
            "credits": round(sum(float(m[3]) for m in metering), 3),
            "unmapped_users": sum(1 for u in users if u.upper() not in teams)}


if __name__ == "__main__":
    print(extract("data/finops_snowflake.duckdb"))
