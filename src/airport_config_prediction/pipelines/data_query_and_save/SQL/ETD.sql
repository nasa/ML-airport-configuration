select
    gufi,
    "timestamp",
    departure_runway_estimated_time
from matm_flight
where "timestamp" between :start_time and :end_time
and departure_runway_estimated_time is not null
and departure_aerodrome_icao_name = :airport_icao