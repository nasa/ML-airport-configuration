with base as (
SELECT  *
    from lamp_weather
    where station = :airport_icao
    and posted_timestamp between (:start_time) and (:end_time)
),
cleaned as
 (
select
    posted_timestamp as "timestamp",
    utc_timestamp  as forecast_timestamp,
    "tmp" as temperature,
    "wdr" as wind_direction,
    "wsp" as wind_speed,
    "wgs" as wind_gust,
    "cig" as cloud_ceiling,
    "vis" as visibility,
    "cld" as cloud,
    "lc1" as lightning_prob,
    ("pco" = 'Y') as precip
from base
)
select *
from cleaned
where "timestamp" between :start_time and :end_time
