with base as (
SELECT  *
    from weather
    where station = :airport_icao
    and posted_timestamp between (:start_time) and (:end_time)
),
cleaned as
 (
select
    posted_timestamp as "timestamp",
    obs_timestamp  as forecast_timestamp,
    "TMP" as temperature,
    "WDR" as wind_direction,
    "WSP" as wind_speed,
    "WGS" as wind_gust,
    "CIG" as cloud_ceiling,
    "VIS" as visibility,
    "CLD" as cloud,
    "LC1" as lightening_prob,
    ("PCO" = 'Y') as precip
from base
)
select *
from cleaned
where "timestamp" between :start_time and :end_time