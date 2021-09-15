with base as (
SELECT  *
    from weather
    where station = :airport_icao
    and posted_timestamp between (:start_time) and (:end_time)
),
cleaned as
 (
select
    posted_timestamp  as "timestamp",
    obs_timestamp as observed_timestamp,
    temperature,
    wind_direction,
    wind_speed,
    wind_gust,
    sky_condition,
    visibility,
    dewpoint,
    special_weather

from base
)
select *
from cleaned
where "timestamp" between :start_time and :end_time