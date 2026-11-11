-- Showback for ~30 data champions: weekly cost per user and team, top workloads, and alerts.
create or replace table user_week_cost as
select date_trunc('week', c.start_time) as week, c.user_name, t.team,
       round(sum(c.credits), 4) as credits, round(sum(c.credits) * $credit_price, 2) as cost_usd, count(*) as queries
from query_cost c join user_team t using (user_name)
group by all;

create or replace table workload_cost as
select query_tag, user_name, warehouse_name, count(*) as runs, round(sum(credits), 2) as credits,
       round(sum(credits) * $credit_price, 2) as cost_usd
from query_cost group by all;

-- alerts a platform team acts on
create or replace table cost_alerts as
with team_week as (
  select team, week, sum(credits) as credits from user_week_cost group by team, week
),
wow as (
  select team, week, credits, lag(credits) over (partition by team order by week) as prev_credits from team_week
)
select 'team_week_over_week_spike' as alert, team as subject, cast(week as varchar) as detail,
       round(credits / prev_credits, 2) as value
from wow where prev_credits > 0 and credits > 2 * prev_credits and credits - prev_credits > 5
union all
select 'warehouse_idle_over_25pct', warehouse_name, 'idle share of metered credits', idle_share
from warehouse_idle where idle_share > 0.25
union all
select 'top_workload_over_10pct', query_tag || ' / ' || user_name, warehouse_name,
       round(credits / (select sum(credits) from query_cost), 4)
from workload_cost where credits > 0.10 * (select sum(credits) from query_cost);
