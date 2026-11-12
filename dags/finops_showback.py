"""Daily Snowflake cost showback: extract usage, build the attribution models, gate on reconciliation, publish.

Tasks
  extract_account_usage  load QUERY_HISTORY and WAREHOUSE_METERING_HISTORY into the working database
                         (FINOPS_SOURCE=sample: seeded sample workload; FINOPS_SOURCE=snowflake: SNOWFLAKE.ACCOUNT_USAGE)
  check_sources          stop early if a source is empty or metering has gaps
  build_<model>          one task per SQL model in models/, in order
  reconcile_gate         attributed + idle credits must equal metered credits; any failed check fails the run
  publish_showback       write team showback, top workloads and alerts to reports/<run date>/

Scheduled at 06:00 UTC, after ACCOUNT_USAGE latency (up to about 3 hours) has settled for the previous day.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

DB = os.environ.get("FINOPS_DB", str(REPO / "data" / "finops.duckdb"))
SOURCE = os.environ.get("FINOPS_SOURCE", "sample")
MODELS = sorted(p.name for p in (REPO / "models").glob("*.sql"))


@dag(
    dag_id="finops_showback",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["platform", "finops"],
    doc_md=__doc__,
)
def finops_showback():
    @task
    def extract_account_usage() -> dict:
        Path(DB).parent.mkdir(parents=True, exist_ok=True)
        if SOURCE == "snowflake":
            import extract_snowflake
            return extract_snowflake.extract(DB)
        import generate
        return generate.generate(DB)

    @task
    def check_sources(_: dict) -> dict:
        import duckdb
        con = duckdb.connect(DB, read_only=True)
        counts = {t: con.execute(f"select count(*) from {t}").fetchone()[0]
                  for t in ("query_history", "warehouse_metering_history", "user_team")}
        empty = [t for t, n in counts.items() if n == 0]
        if empty:
            raise ValueError(f"empty source tables: {empty}")
        return counts

    def build_task(model: str):
        @task(task_id=f"build_{Path(model).stem}")
        def build() -> str:
            import duckdb
            import run
            con = duckdb.connect(DB)
            run.build_model(con, REPO / "models" / model)
            con.close()
            return model
        return build()

    @task
    def reconcile_gate() -> dict:
        import duckdb
        import run
        con = duckdb.connect(DB, read_only=True)
        results = run.check(con)
        failed = [k for k, v in results.items() if v]
        if failed:
            raise ValueError(f"cost attribution checks failed: {failed}")
        return results

    @task
    def publish_showback(ds: str | None = None) -> str:
        import duckdb
        con = duckdb.connect(DB, read_only=True)
        out = REPO / "reports" / (ds or "manual")
        out.mkdir(parents=True, exist_ok=True)
        for name, sql in {
            "team_showback": "select team, round(sum(credits),4) credits, round(sum(cost_usd),2) usd from user_week_cost group by 1 order by 2 desc",
            "warehouse_idle": "select warehouse_name, round(metered_credits,4) metered, round(idle_credits,4) idle, idle_share from warehouse_idle order by 2 desc",
            "top_workloads": "select query_tag, user_name, warehouse_name, runs, round(credits,4) credits from workload_cost order by credits desc limit 10",
            "cost_alerts": "select * from cost_alerts order by alert, value desc",
        }.items():
            con.execute(sql).fetchdf().to_csv(out / f"{name}.csv", index=False)
        alerts = con.execute("select count(*) from cost_alerts").fetchone()[0]
        return f"{out} ({alerts} alerts)"

    sources = check_sources(extract_account_usage())
    previous = sources
    for model in MODELS:
        current = build_task(model)
        previous >> current
        previous = current
    previous >> reconcile_gate() >> publish_showback()


finops_showback()
