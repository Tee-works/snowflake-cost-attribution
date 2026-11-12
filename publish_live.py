"""Render live/README.md from the latest DAG run: run time, reconciliation result, idle share and team showback."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import duckdb

import run

DB = os.environ.get("FINOPS_DB", "data/finops.duckdb")


def table(con, sql: str) -> str:
    df = con.execute(sql).fetchdf()
    head = "| " + " | ".join(df.columns) + " |\n|" + "---|" * len(df.columns) + "\n"
    return head + "".join("| " + " | ".join(str(v) for v in row) + " |\n" for row in df.itertuples(index=False))


def main() -> str:
    con = duckdb.connect(DB, read_only=True)
    checks = run.check(con)
    metered, idle, first, last = con.execute(
        """select (select sum(credits_used) from warehouse_metering_history),
                  (select sum(idle_credits) from warehouse_idle),
                  (select min(start_time) from warehouse_metering_history),
                  (select max(end_time) from warehouse_metering_history)""").fetchone()
    attributed = con.execute("select sum(credits) from query_cost").fetchone()[0]
    out = [
        "# Live showback",
        "",
        f"Last run: {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC, by the `finops_showback` Airflow DAG, "
        "from SNOWFLAKE.ACCOUNT_USAGE on a live Snowflake account. The workload is scripted (8 analyst users, a BI and a transform service account); the metering is real.",
        "",
        f"Window: {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC. "
        f"Metered compute credits: {metered:.3f}. Attributed to queries: {attributed:.3f}. Idle: {idle:.3f} ({idle / metered:.0%}).",
        "",
        "## Checks",
        "",
        *[f"- {'PASS' if v == 0 else 'FAIL'}: {k}" for k, v in checks.items()],
        "",
        "## Warehouses",
        "",
        table(con, "select warehouse_name, round(metered_credits, 3) metered, round(idle_credits, 3) idle, idle_share from warehouse_idle order by 2 desc"),
        "## Team showback",
        "",
        table(con, "select team, round(sum(credits), 3) credits, round(sum(cost_usd), 2) usd, sum(queries)::int queries from user_week_cost group by 1 order by 2 desc"),
        "## Alerts",
        "",
        table(con, "select * from cost_alerts order by alert, value desc"),
    ]
    return "\n".join(out)


if __name__ == "__main__":
    print(main())
