"""Build the cost-attribution models in order, run the reconciliation checks and print the showback."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
CREDIT_PRICE_USD = 3.0     # assumption for the demo; use your contract rate

CHECKS = {
    "reconciles: attributed + idle credits = metered credits (to 1e-6)":
        """select abs((select sum(credits) from query_cost) + (select sum(idle_credits) from warehouse_idle)
                      - (select sum(credits_used) from warehouse_metering_history)) > 1e-6""",
    "grain: one row per query in query_cost":
        "select count(*) - count(distinct query_id) from query_cost",
    "every query with a warehouse is costed":
        "select count(*) from query_history where warehouse_name is not null and query_id not in (select query_id from query_cost)",
    "no negative or null costs":
        "select count(*) from query_cost where credits is null or credits < 0",
    "busy time never exceeds running time by more than rounding":
        "select count(*) from warehouse_hour_usage where busy_secs > running_secs + 1",
}


def build(db: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db)
    for sql in sorted((HERE / "models").glob("*.sql")):
        con.execute(sql.read_text().replace("$credit_price", str(CREDIT_PRICE_USD)))
    return con


def check(con) -> dict:
    return {k: int(con.execute(v).fetchone()[0]) for k, v in CHECKS.items()}


if __name__ == "__main__":
    con = build(sys.argv[1] if len(sys.argv) > 1 else "finops_demo.duckdb")
    for k, v in check(con).items():
        print(("PASS " if v == 0 else "FAIL ") + k)
    print("\nmetered credits", round(con.execute("select sum(credits_used) from warehouse_metering_history").fetchone()[0], 1))
    print(con.execute("select warehouse_name, round(metered_credits,1) metered, round(idle_credits,1) idle, idle_share from warehouse_idle order by 2 desc").fetchdf().to_string(index=False))
    print("\nteam showback (4 weeks):")
    print(con.execute("select team, round(sum(credits),1) credits, round(sum(cost_usd)) usd from user_week_cost group by 1 order by 2 desc").fetchdf().to_string(index=False))
    print("\ntop workloads:")
    print(con.execute("select query_tag, user_name, warehouse_name, runs, credits from workload_cost order by credits desc limit 5").fetchdf().to_string(index=False))
    print("\nalerts:")
    print(con.execute("select * from cost_alerts order by alert, value desc").fetchdf().to_string(index=False))
