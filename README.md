# Snowflake cost attribution for decentralised analytics

## What it does

`generate.py` simulates four weeks of `ACCOUNT_USAGE`: 9,451 queries from 30 data champions plus Airflow, Airbyte and Looker service users, across four warehouses. It also produces the hourly `WAREHOUSE_METERING_HISTORY` those queries would bill, including each warehouse's auto-suspend tail. The column names match Snowflake's, so the SQL runs there with only the source names changed.

It plants four cost problems:
- a Looker dashboard refreshing every 15 minutes, all night, on a LARGE warehouse;
- one champion re-running an unfiltered scan every working hour;
- an ad-hoc warehouse with a one-hour auto-suspend;
- a 20x week-over-week spike for one team.

| Model | Grain | Point |
|---|---|---|
| `01_query_hour_slices` | query x clock hour | Long queries are split across the hours they ran in |
| `02_warehouse_hour_usage` | warehouse x hour | Busy time is the **union** of query time (concurrent queries aren't double counted). Idle = billed running time minus busy time |
| `03_query_cost` | query | Each hour's *active* credits are shared by its queries in proportion to execution seconds. *Idle* credits stay with the warehouse |
| `04_showback` | user x week, team, workload | Showback in credits and USD, plus alerts: team week-over-week spike, warehouse idle share over 25%, any workload over 10% of spend |

## What it shows

- **Attributed credits plus idle credits equal metered credits exactly** (checked to 1e-6). Showback people can't reconcile to the bill gets argued with, not acted on.
- **80% of the metered credits (3,285 of 4,103) are idle warehouse time.** Snowflake's own `QUERY_ATTRIBUTION_HISTORY` leaves idle time out, so per-user attribution alone would hide most of the bill. Here the overnight dashboard refresh keeps `BI_WH` awake (86% idle), and `ADHOC_WH`'s one-hour auto-suspend does the same (79% idle). The fixes are a shorter auto-suspend and a less frequent refresh, not asking 30 people to write cheaper SQL.
- All three planted user-level problems are caught: the dashboard (26% of query spend), the hourly unfiltered scan, and the marketing spike.

## Run it

```
pip install -r requirements.txt
python generate.py && python run.py && pytest -q
```

Five checks run on every build:
- the reconciliation above;
- one row per query;
- every query costed;
- no negative or null costs;
- busy time never more than running time.

Four tests check the checks:
- all of them pass;
- concurrency really is de-duplicated;
- the planted problems are flagged;
- a rebuild gives the same numbers.

`dags/finops_showback.py` sketches the daily Airflow job. It's illustrative and not run in the tests.

## On Snowflake and dbt

- Swap the sources for `SNOWFLAKE.ACCOUNT_USAGE` and a date spine.
- Make `query_cost` incremental on `start_time`, with a lookback of a few hours to cover the view's latency.
- Turn the checks into dbt tests and post `cost_alerts` to Slack.
- Map users to teams through roles, or `QUERY_TAG` for service users.
- `ORGANIZATION_USAGE` adds the currency, so USD becomes EUR at the contract rate.
