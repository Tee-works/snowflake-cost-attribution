"""Fargate job: fetch the Snowflake key from Secrets Manager, run the job, publish to S3, alert on failure via SNS."""
from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import boto3

BUCKET = os.environ["REPORTS_BUCKET"]
TOPIC = os.environ["ALERT_TOPIC_ARN"]
SECRET = os.environ["SNOWFLAKE_KEY_SECRET"]
JOB = os.environ.get("JOB", "showback")
ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
AIRFLOW_VERSION, PY = "3.3.2", f"{sys.version_info.major}.{sys.version_info.minor}"
TODAY = dt.datetime.now(dt.timezone.utc).date().isoformat()


def sh(*cmd: str, env: dict | None = None) -> str:
    r = subprocess.run(cmd, env={**os.environ, **(env or {})}, capture_output=True, text=True)
    print(r.stdout[-4000:], r.stderr[-4000:], sep="\n", flush=True)
    if r.returncode:
        raise RuntimeError(f"{' '.join(cmd[:4])} exited {r.returncode}\n{(r.stdout + r.stderr)[-2500:]}")
    return r.stdout


def snowflake_key() -> str:
    key = boto3.client("secretsmanager").get_secret_value(SecretId=SECRET)["SecretString"]
    home = Path.home() / ".snowflake"
    home.mkdir(mode=0o700, exist_ok=True)
    path = home / "finops_svc.p8"
    path.write_text(key)
    path.chmod(0o600)
    (home / "connections.toml").write_text(
        f'[finops]\naccount = "{ACCOUNT}"\nuser = "FINOPS_SVC"\nauthenticator = "SNOWFLAKE_JWT"\n'
        f'private_key_file = "{path}"\nrole = "FINOPS_ADMIN"\nwarehouse = "PLATFORM_WH"\n')
    (home / "connections.toml").chmod(0o600)
    return str(path)


def showback() -> str:
    sh(sys.executable, "-m", "pip", "install", "-q", f"apache-airflow=={AIRFLOW_VERSION}", "-r", "requirements.txt",
       "snowflake-connector-python", "--constraint",
       f"https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-{PY}.txt")
    env = {"AIRFLOW_HOME": "/tmp/airflow", "AIRFLOW__CORE__DAGS_FOLDER": "/app/dags",
           "AIRFLOW__CORE__LOAD_EXAMPLES": "False", "FINOPS_SOURCE": "snowflake"}
    sh("airflow", "db", "migrate", env=env)
    sh("airflow", "dags", "test", "finops_showback", TODAY, env=env)
    live = sh(sys.executable, "publish_live.py")
    s3 = boto3.client("s3")
    for f in Path("reports").glob("*/*.csv"):
        for prefix in (f"reports/{TODAY}", "reports/latest"):
            s3.upload_file(str(f), BUCKET, f"{prefix}/{f.name}")
    for prefix in (f"reports/{TODAY}", "reports/latest"):
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/README.md", Body=live.encode(), ContentType="text/markdown")
    return live.splitlines()[4] if len(live.splitlines()) > 4 else "showback published"


def workload() -> str:
    os.environ["WORKLOAD_PRIVATE_KEY"] = snowflake_key()
    sh(sys.executable, "-m", "pip", "install", "-q", "snowflake-connector-python")
    sh(sys.executable, "workload.py", os.environ.get("WORKLOAD_MINUTES", "20"))
    return "workload session finished"


if __name__ == "__main__":
    try:
        snowflake_key()
        print(showback() if JOB == "showback" else workload(), flush=True)
    except Exception as e:  # any failure, including the reconciliation gate, pages the platform owner
        boto3.client("sns").publish(TopicArn=TOPIC, Subject=f"finops {JOB} failed {TODAY}"[:100], Message=str(e)[-3500:])
        raise
