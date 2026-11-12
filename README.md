# Snowflake cost attribution for decentralised analytics

![dag-run](https://github.com/Tee-works/snowflake-cost-attribution/actions/workflows/dag-run.yml/badge.svg)

When dozens of people and service accounts query shared Snowflake warehouses, the bill arrives per warehouse, not per person. This project attributes every metered credit to the query, user and team that caused it, keeps idle warehouse time as its own line so the totals reconcile to the bill, and publishes a daily showback with alerts.

It runs in production every day: **AWS (EventBridge Scheduler, ECS Fargate) runs the Airflow DAG against a live Snowflake account**, and Terraform manages both clouds. The account is driven by a scripted multi-team workload (`workload.py`): 8 analyst users across 4 teams, a BI service account and a transform service account, each signing in with its own key and tagging its queries. The metering, the bill and every number below are real.

## Architecture

```
EventBridge Scheduler (06:00 UTC daily)
  -> ECS Fargate task (Python 3.12, ARM64)
       -> Airflow 3.3 DAG finops_showback
            extract_account_usage   SNOWFLAKE.ACCOUNT_USAGE query + metering history, key-pair auth
            check_sources           stop if a source is empty
            build_01..04            SQL models (DuckDB) below
            reconcile_gate          attributed + idle credits must equal metered credits, or the run fails
            publish_showback        team showback, top workloads, alerts
       -> S3 reports/<date>/ and reports/latest/
       -> SNS email if anything fails (including the reconciliation gate)
Secrets Manager holds the Snowflake key. CloudWatch keeps the logs.
```

| Layer | Where | What it creates |
|---|---|---|
| `snowflake/bootstrap.sql` | run once by ACCOUNTADMIN | account-wide resource monitor (40 credits a month), `FINOPS_ADMIN` role, key-pair service user |
| `terraform/` | Snowflake provider | 3 workload warehouses, a role per team, 10 key-pair service users, database, grants (67 resources) |
| `terraform/aws/` | AWS provider | S3, Secrets Manager, ECS cluster and task, IAM (least privilege), EventBridge schedules, SNS, logs (21 resources) |
| `dags/`, `models/`, `extract_snowflake.py` | the pipeline | the DAG, the four SQL models and the Snowflake extract |
| `.github/workflows/dag-run.yml` | CI | unit tests and a full DAG run on every push |

## The models

| Model | Grain | Point |
|---|---|---|
| `01_query_hour_slices` | query x metered hour | Long queries are split across the hours they ran in |
| `02_warehouse_hour_usage` | warehouse x hour | Busy time is the **union** of query time (concurrent queries aren't double counted). Idle = billed running time minus busy time |
| `03_query_cost` | query | Each hour's *active* credits are shared by its queries in proportion to execution seconds. *Idle* credits stay with the warehouse |
| `04_showback` | user x week, team, workload | Showback in credits and USD, plus alerts: team week-over-week spike, warehouse idle share over 25%, any workload over 10% of spend |

Decisions that matter on real Snowflake data (see `extract_snowflake.py`):
- `CREDITS_USED_COMPUTE`, not `CREDITS_USED`: cloud services credits are not warehouse time.
- A query's warehouse time is its `EXECUTION_TIME`, ending at `END_TIME`. Compilation and time waiting for a warehouse to resume are not.
- Only hours older than `ACCOUNT_USAGE` latency (about 3 hours) are read, so a run never reconciles against half-loaded metering.
- Users with no team mapping are shown as `unmapped`, never dropped.
- Serverless pools and the `CLOUD_SERVICES_ONLY` row are out of scope: they are not warehouse compute.

## Live results

First live run (11 Nov 2026, a 45-minute workload session, read back by the DAG once `ACCOUNT_USAGE` caught up):

| Warehouse | Auto-suspend | Metered credits | Idle credits | Idle share |
|---|---|---|---|---|
| BI_WH | 600 s | 0.839 | 0.837 | **99.8%** |
| ADHOC_WH | 300 s | 0.832 | 0.820 | 98.5% |
| PLATFORM_WH | 60 s | 0.211 | 0.207 | 97.9% |
| TRANSFORM_WH | 60 s | 0.091 | 0.085 | 93.4% |

1.973 credits metered, 0.024 attributed to 325 queries, 1.949 idle. All five checks pass, and attributed + idle equals the bill to 1e-6.

What it says: on this account, almost the whole bill is warehouses running with nothing to do. The dashboard refreshes every 5 minutes, but BI_WH waits 600 seconds before suspending, so it never suspends and bills a full session for under 6 seconds of query time. A per-query cost view (including Snowflake's own `QUERY_ATTRIBUTION_HISTORY`) would show about 1% of this bill and hide the rest. The levers are auto-suspend per warehouse, consolidating sporadic ad hoc users onto fewer warehouses, and the one workload the alert flags: an unfiltered scan that is 41% of attributed spend.

The latest daily report is written to S3 (`reports/latest/`); `publish_live.py` renders it as markdown.

### What real data broke

The sample data passed every check. The first live run failed the reconciliation gate, which is the gate doing its job. Three things in real `ACCOUNT_USAGE` that the sample never had:

1. `QUERY_HISTORY.WAREHOUSE_SIZE` is empty on some queries (for example while the warehouse resumes). Joining sizes on it duplicated warehouse-hours and broke reconciliation. Fix: take the known size per warehouse.
2. Snowflake's serverless pools (`COMPUTE_SERVICE_WH_*`, user `SYSTEM`) appear in query history but bill through serverless metering, so they can never match warehouse metering. Fix: scope them out explicitly.
3. DDL and metadata statements (`CREATE USER`, `GRANT`, `SHOW`) report the session's warehouse but run in cloud services, in hours with zero compute credits. Fix: keep zero-credit metered hours so those queries are costed at zero, not dropped, and apply the busy-time check only to hours that billed compute.

## Checks and tests

Five checks gate every run: the reconciliation above; one row per query; every query costed; no negative or null costs; busy time never more than running time. Four tests check the checks on a seeded sample workload (`generate.py`), including that concurrency is de-duplicated and that planted problems are flagged. CI runs the tests and the whole DAG in Airflow on every push.

## Run it

```bash
pip install -r requirements.txt
python generate.py && python run.py && pytest -q          # sample workload, no accounts needed
```

On your own accounts: run `snowflake/bootstrap.sql`, then `terraform apply` in `terraform/` and `terraform/aws/`, then put the Snowflake key in Secrets Manager:

```bash
aws secretsmanager put-secret-value --secret-id finops-showback/snowflake-private-key --secret-string file://path/to/key.p8
```
