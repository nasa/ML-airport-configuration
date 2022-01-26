select
    gufi,
    arrival_runway_actual_time,
    arrival_runway_actual,
    departure_runway_actual_time,
    departure_runway_actual
from runways
where (arrival_aerodrome_iata_name = :airport_iata and arrival_runway_actual_time between :start_time and :end_time ) or
(departure_aerodrome_iata_name = :airport_iata and departure_runway_actual_time between :start_time and :end_time) and
(points_on_runway = :surf_surv_avail)
order by arrival_runway_actual_time, departure_runway_actual_time
