"""Synthetic Snowflake ACCOUNT_USAGE data for a decentralised analytics platform (no real Grover data).

Tables mirror the column names of SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY and WAREHOUSE_METERING_HISTORY,
so the SQL in models/ runs on Snowflake with only the source names changed.

Planted cost problems, the kind a platform team finds once it starts attributing credits:
  - a Looker dashboard refreshing every 15 minutes, all night, on a LARGE warehouse
  - one data champion re-running an unfiltered scan of a big table every hour of the working day
  - a warehouse with a long auto-suspend, burning credits while idle
  - a week-over-week cost spike for one team
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

import duckdb

START = datetime(2026, 10, 5)          # four weeks, Monday 5 Oct to Sunday 1 Nov
DAYS = 28
SIZE_CREDITS = {"XSMALL": 1, "SMALL": 2, "MEDIUM": 4, "LARGE": 8}
WAREHOUSES = {"TRANSFORM_WH": "MEDIUM", "LOAD_WH": "SMALL", "BI_WH": "LARGE", "ADHOC_WH": "SMALL"}
TEAMS = ["finance", "risk", "marketing", "operations", "product", "b2b", "logistics", "customer"]


def generate(path: str, seed: int = 3) -> dict:
    rnd = random.Random(seed)
    users = [(f"CHAMPION_{i:02d}", TEAMS[i % len(TEAMS)]) for i in range(1, 31)]
    users += [("AIRFLOW_SVC", "platform"), ("AIRBYTE_SVC", "platform"), ("LOOKER_SVC", "bi")]
    queries, n = [], 0

    def q(user, wh, start, secs, tag, gb):
        nonlocal n
        n += 1
        queries.append((f"q{n:07d}", user, f"{user}_ROLE", wh, WAREHOUSES[wh], start, start + timedelta(seconds=secs),
                        int(secs * 1000), tag, int(gb * 1e9)))

    for d in range(DAYS):
        day = START + timedelta(days=d)
        weekday = day.weekday() < 5
        for h in range(2, 5):                                       # nightly dbt + loads
            for m in range(40):
                q("AIRFLOW_SVC", "TRANSFORM_WH", day + timedelta(hours=h, minutes=m), rnd.randint(20, 160), f"dbt:model_{m:02d}", rnd.uniform(1, 40))
            q("AIRBYTE_SVC", "LOAD_WH", day + timedelta(hours=h), rnd.randint(300, 900), "airbyte:sync", rnd.uniform(5, 60))
        for minute in range(0, 24 * 60, 15):                        # planted: dashboard refresh all night on LARGE
            if weekday or minute % 60 == 0:
                q("LOOKER_SVC", "BI_WH", day + timedelta(minutes=minute), rnd.randint(25, 70), "looker:exec_dashboard_7", rnd.uniform(2, 9))
        if weekday:
            for user, team in users[:30]:
                for _ in range(rnd.randint(2, 9)):
                    t = day + timedelta(hours=rnd.randint(8, 17), minutes=rnd.randint(0, 59))
                    wh = "BI_WH" if rnd.random() < 0.5 else "ADHOC_WH"
                    q(user, wh, t, rnd.randint(2, 90), "looker:explore" if wh == "BI_WH" else "adhoc", rnd.uniform(0.1, 5))
            for h in range(9, 18):                                  # planted: hourly unfiltered scan by one champion
                q("CHAMPION_07", "ADHOC_WH", day + timedelta(hours=h, minutes=5), rnd.randint(400, 700), "adhoc", rnd.uniform(300, 420))
            if d >= 21:                                             # planted: marketing spike in week 4
                for user, team in users[:30]:
                    if team == "marketing":
                        for _ in range(25):
                            t = day + timedelta(hours=rnd.randint(9, 17), minutes=rnd.randint(0, 59))
                            q(user, "BI_WH", t, rnd.randint(60, 240), "looker:explore", rnd.uniform(5, 30))

    # metering: a warehouse bills while running; it suspends `auto_suspend` seconds after its last query
    auto_suspend = {"TRANSFORM_WH": 60, "LOAD_WH": 60, "BI_WH": 600, "ADHOC_WH": 3600}   # planted: ADHOC_WH 1 hour
    running: dict[tuple[str, datetime], float] = {}
    by_wh: dict[str, list] = {}
    for r in queries:
        by_wh.setdefault(r[3], []).append((r[5], r[6]))
    for wh, spans in by_wh.items():
        spans.sort()
        merged = []
        for s, e in spans:
            e = e + timedelta(seconds=auto_suspend[wh])
            if merged and s <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        for s, e in merged:
            t = s
            while t < e:
                hour = t.replace(minute=0, second=0, microsecond=0)
                nxt = min(e, hour + timedelta(hours=1))
                running[(wh, hour)] = running.get((wh, hour), 0) + (nxt - t).total_seconds()
                t = nxt
    metering = [(wh, h, h + timedelta(hours=1), round(SIZE_CREDITS[WAREHOUSES[wh]] * secs / 3600, 9))
                for (wh, h), secs in running.items()]

    con = duckdb.connect(path)
    con.execute("""create or replace table query_history(query_id varchar, user_name varchar, role_name varchar,
        warehouse_name varchar, warehouse_size varchar, start_time timestamp, end_time timestamp,
        execution_time bigint, query_tag varchar, bytes_scanned bigint)""")
    con.executemany("insert into query_history values (?,?,?,?,?,?,?,?,?,?)", queries)
    con.execute("create or replace table warehouse_metering_history(warehouse_name varchar, start_time timestamp, "
                "end_time timestamp, credits_used double)")
    con.executemany("insert into warehouse_metering_history values (?,?,?,?)", metering)
    con.execute("create or replace table user_team(user_name varchar, team varchar)")
    con.executemany("insert into user_team values (?,?)", users)
    con.close()
    return {"queries": len(queries), "metered_hours": len(metering), "credits": round(sum(m[3] for m in metering), 1)}


if __name__ == "__main__":
    print(generate("finops_demo.duckdb"))
