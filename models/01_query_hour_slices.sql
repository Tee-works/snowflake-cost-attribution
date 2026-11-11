-- Grain: one row per query per clock hour it ran in (long queries span hours).
-- Uses a calendar of hours. On Snowflake, replace hours with your calendar/date spine table.
create or replace table hours as
select h as hour_start from range(timestamp '2026-10-05', timestamp '2026-11-03', interval 1 hour) t(h);

create or replace table query_hour_slices as
select q.query_id, q.user_name, q.warehouse_name, q.query_tag, h.hour_start,
       greatest(q.start_time, h.hour_start) as slice_start,
       least(q.end_time, h.hour_start + interval 1 hour) as slice_end,
       epoch(least(q.end_time, h.hour_start + interval 1 hour)) - epoch(greatest(q.start_time, h.hour_start)) as slice_secs
from query_history q
join hours h on q.start_time < h.hour_start + interval 1 hour and q.end_time > h.hour_start
where q.warehouse_name is not null;
