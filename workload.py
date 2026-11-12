"""Scripted multi-team workload on the live Snowflake account, so ACCOUNT_USAGE has real queries and real metering.

The users, roles and warehouses come from terraform/. Each user signs in with key-pair auth and tags
its queries, the way a real platform tags dbt models and dashboards. In one session (default 30 minutes):

  TRANSFORM_SVC   rebuilds a daily aggregate on TRANSFORM_WH (auto-suspend 60 s)        tag dbt:fct_daily_orders
  LOOKER_SVC      refreshes an executive dashboard every 5 minutes on BI_WH (600 s)     tag looker:exec_dashboard
  CHAMPION_01-08  ad hoc questions on ADHOC_WH at random intervals                      tag adhoc:<team>
  CHAMPION_05     re-runs an unfiltered scan of the whole orders table                  tag adhoc:unfiltered_scan

Usage: python workload.py setup      # create the ANALYTICS.MARTS tables (once)
       python workload.py [minutes]  # run one session
"""
from __future__ import annotations

import os
import random
import sys
import threading
import time
from pathlib import Path

import snowflake.connector

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "AJPYTLM-HQ40527")
KEY = os.path.expanduser(os.environ.get("WORKLOAD_PRIVATE_KEY", "~/.snowflake/keys/finops_svc.p8"))

SETUP = [
    """create or replace table analytics.marts.orders as
       select seq8() as order_id,
              uniform(1, 200000, random()) as customer_id,
              dateadd(minute, -uniform(0, 525600, random()), current_timestamp())::timestamp_ntz as ordered_at,
              array_construct('DE','AT','NL','ES','US')[uniform(0, 4, random())]::varchar as country,
              array_construct('phone','laptop','tablet','wearable','console','camera')[uniform(0, 5, random())]::varchar as category,
              uniform(1, 36, random()) as rental_months,
              round(uniform(5, 150, random()) + uniform(0, 99, random()) / 100, 2) as monthly_price
       from table(generator(rowcount => 20000000))""",
    """create or replace table analytics.marts.customers as
       select seq4() + 1 as customer_id,
              array_construct('consumer','business')[uniform(0, 1, random())]::varchar as segment,
              array_construct('organic','paid_search','paid_social','referral','partner')[uniform(0, 4, random())]::varchar as channel
       from table(generator(rowcount => 200000))""",
]

DASHBOARD = [
    "select date_trunc(day, ordered_at) d, category, count(*), sum(monthly_price * rental_months) from analytics.marts.orders where ordered_at >= dateadd(day, -30, current_timestamp()) group by 1, 2",
    "select country, count(distinct customer_id), avg(monthly_price) from analytics.marts.orders where ordered_at >= dateadd(day, -7, current_timestamp()) group by 1",
]
TRANSFORM = ["create or replace table analytics.marts.fct_daily_orders as select date_trunc(day, ordered_at) as order_date, country, category, count(*) as orders, sum(monthly_price) as mrr from analytics.marts.orders group by 1, 2, 3"]
ADHOC = [
    "select category, avg(rental_months) from analytics.marts.orders where country = '{c}' and ordered_at >= dateadd(day, -{n}, current_timestamp()) group by 1",
    "select c.channel, count(*), sum(o.monthly_price) from analytics.marts.orders o join analytics.marts.customers c using (customer_id) where o.ordered_at >= dateadd(day, -{n}, current_timestamp()) group by 1",
    "select segment, count(*) from analytics.marts.customers group by 1",
]
UNFILTERED = ["select * from analytics.marts.orders order by monthly_price desc limit 100000",
              "select customer_id, listagg(category, ',') from analytics.marts.orders group by 1 order by 1 limit 50000"]

TEAMS = {f"CHAMPION_{i:02d}": t for i, t in zip(range(1, 9), ["finance"] * 2 + ["risk"] * 2 + ["marketing"] * 2 + ["operations"] * 2)}


def connect(user: str, warehouse: str, tag: str):
    return snowflake.connector.connect(account=ACCOUNT, user=user, authenticator="SNOWFLAKE_JWT",
                                       private_key_file=KEY, warehouse=warehouse,
                                       session_parameters={"QUERY_TAG": tag, "USE_CACHED_RESULT": False})


def actor(user, warehouse, tag, statements, every, until, jitter=0.0):
    con = connect(user, warehouse, tag)
    cur, rnd, n = con.cursor(), random.Random(user), 0
    try:
        while time.time() < until:
            sql = rnd.choice(statements).format(c=rnd.choice(["DE", "AT", "NL", "ES", "US"]), n=rnd.choice([7, 30, 90]))
            try:
                cur.execute(sql)
                cur.fetchmany(10)
                n += 1
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"{user}: {e.msg[:120]}", flush=True)
            time.sleep(max(5.0, every + rnd.uniform(-jitter, jitter)))
    finally:
        con.close()
    print(f"{user:14s} {tag:28s} {n} queries", flush=True)


def session(minutes: float) -> None:
    until = time.time() + minutes * 60
    actors = [("TRANSFORM_SVC", "TRANSFORM_WH", "dbt:fct_daily_orders", TRANSFORM, 600, 0),
              ("LOOKER_SVC", "BI_WH", "looker:exec_dashboard", DASHBOARD, 300, 0)]
    for user, team in TEAMS.items():
        if user == "CHAMPION_05":
            actors.append((user, "ADHOC_WH", "adhoc:unfiltered_scan", UNFILTERED, 120, 30))
        else:
            actors.append((user, "ADHOC_WH", f"adhoc:{team}", ADHOC, 420, 300))
    threads = [threading.Thread(target=actor, args=(*a[:5], until, a[5])) for a in actors]
    for t in threads:
        t.start()
        time.sleep(2)
    for t in threads:
        t.join()


if __name__ == "__main__":
    if sys.argv[1:2] == ["setup"]:
        con = connect("TRANSFORM_SVC", "TRANSFORM_WH", "setup:seed_tables")
        for sql in SETUP:
            con.cursor().execute(sql)
        con.close()
        print("tables created")
    else:
        session(float(sys.argv[1]) if len(sys.argv) > 1 else 30)
