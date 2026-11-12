-- Grain: one row per query per clock hour it ran in (long queries span hours).
-- Hours come from metering, per warehouse: a warehouse is billed for every hour it runs, so every query falls in a metered hour.
-- (The "every query is costed" check catches any that do not.)
create or replace table hours as
select distinct warehouse_name, start_time as hour_start from warehouse_metering_history;

create or replace table query_hour_slices as
select q.query_id, q.user_name, q.warehouse_name, q.query_tag, h.hour_start,
       greatest(q.start_time, h.hour_start) as slice_start,
       least(q.end_time, h.hour_start + interval 1 hour) as slice_end,
       epoch(least(q.end_time, h.hour_start + interval 1 hour)) - epoch(greatest(q.start_time, h.hour_start)) as slice_secs
from query_history q
join hours h on h.warehouse_name = q.warehouse_name and q.start_time < h.hour_start + interval 1 hour and q.end_time > h.hour_start
where q.warehouse_name is not null;
