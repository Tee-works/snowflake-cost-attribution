-- Grain: one row per query. Active credits in each warehouse-hour are shared by the queries that ran in it,
-- in proportion to their execution seconds. Idle credits stay with the warehouse (see warehouse_idle).
-- Snowflake's own QUERY_ATTRIBUTION_HISTORY also leaves idle time out. This model makes it visible instead of losing it.
create or replace table query_cost as
select s.query_id, any_value(s.user_name) as user_name, any_value(s.warehouse_name) as warehouse_name,
       any_value(s.query_tag) as query_tag,
       min(s.slice_start) as start_time,
       sum(u.active_credits * s.slice_secs / nullif(u.query_secs, 0)) as credits
from query_hour_slices s
join warehouse_hour_usage u using (warehouse_name, hour_start)
group by s.query_id;

create or replace table warehouse_idle as
select warehouse_name, sum(idle_credits) as idle_credits, sum(credits_used) as metered_credits,
       round(sum(idle_credits) / sum(credits_used), 4) as idle_share
from warehouse_hour_usage group by all;
