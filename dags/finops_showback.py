"""Example Airflow DAG: refresh the cost showback daily after ACCOUNT_USAGE latency (about 3 hours).

Illustrative only: not run in this repo's tests. On Snowflake, point the models at SNOWFLAKE.ACCOUNT_USAGE
and post cost_alerts to the platform channel.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def refresh_showback():
    import run
    con = run.build("finops.duckdb")
    failed = [k for k, v in run.check(con).items() if v]
    if failed:
        raise ValueError(f"cost attribution checks failed: {failed}")


with DAG("finops_showback", start_date=datetime(2026, 11, 1), schedule="0 6 * * *", catchup=False,
         default_args={"retries": 2}, tags=["platform", "finops"]) as dag:
    PythonOperator(task_id="refresh_showback", python_callable=refresh_showback)
