-- Grain: one row per warehouse per metered hour.
-- running_secs comes from metered credits (credits = size rate x running hours).
-- busy_secs is the union of query time in the hour (concurrent queries are not double counted),
-- so idle = running - busy: time the warehouse billed while nothing ran (auto-suspend tail).
create or replace table warehouse_hour_usage as
with sized as (
  select m.warehouse_name, m.start_time as hour_start, m.credits_used,
         m.credits_used * 3600.0 / case w.warehouse_size when 'XSMALL' then 1 when 'SMALL' then 2
                                   when 'MEDIUM' then 4 when 'LARGE' then 8 when 'XLARGE' then 16 end as running_secs
  from warehouse_metering_history m
  -- QUERY_HISTORY leaves the size empty on some queries, so take the known size per warehouse
  left join (select warehouse_name, max(warehouse_size) as warehouse_size from query_history
             where warehouse_size is not null group by 1) w using (warehouse_name)
),
ordered as (
  select warehouse_name, hour_start, slice_start, slice_end,
         max(slice_end) over (partition by warehouse_name, hour_start order by slice_start, slice_end
                              rows between unbounded preceding and 1 preceding) as prev_max_end
  from query_hour_slices
),
islands as (
  select *, sum(case when prev_max_end is null or slice_start > prev_max_end then 1 else 0 end)
            over (partition by warehouse_name, hour_start order by slice_start, slice_end) as island
  from ordered
),
busy as (
  select warehouse_name, hour_start, sum(epoch(island_end) - epoch(island_start)) as busy_secs
  from (select warehouse_name, hour_start, island, min(slice_start) as island_start, max(slice_end) as island_end
        from islands group by all)
  group by all
),
query_secs as (
  select warehouse_name, hour_start, sum(slice_secs) as query_secs from query_hour_slices group by all
)
select s.warehouse_name, s.hour_start, s.credits_used, s.running_secs,
       coalesce(b.busy_secs, 0) as busy_secs, coalesce(qs.query_secs, 0) as query_secs,
       idle_credits, s.credits_used - idle_credits as active_credits
from sized s
left join busy b using (warehouse_name, hour_start)
cross join lateral (select case
    when s.running_secs > 0 then s.credits_used * greatest(0, 1 - least(coalesce(b.busy_secs, 0), s.running_secs) / s.running_secs)
    when coalesce(b.busy_secs, 0) = 0 then s.credits_used  -- billed with nothing running
    else 0 end as idle_credits) i
left join query_secs qs using (warehouse_name, hour_start);
