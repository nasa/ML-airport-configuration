select
    gufi,
    arrival_runway_actual_time,
    arrival_runway_actual,
    departure_runway_actual_time,
    departure_runway_actual
from runway_actuals
where (airport_id = :airport_icao and arrival_runway_actual_time between :start_time and :end_time )or
(airport_id = :airport_icao and departure_runway_actual_time between :start_time and :end_time)
order by arrival_runway_actual_time, departure_runway_actual_time